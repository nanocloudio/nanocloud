//! Store source — subscription-driven entry into a Chronicle graph.
//!
//! See `manifest.toml` for the frame layout and the rationale.

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::contracts::storage::object::{write_answer, WriteAnswer};
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/jsonpatch.rs");
include!("../_shared/views.rs");
include!("../_shared/docschema.rs");
include!("../_shared/head_rev.rs");
include!("../_shared/jsonq.rs");

// Chronicle's stage executor, from its published cores — not restated. With
// `ir_stages` the watch connector runs a Chronicle PIPELINE on each candidate
// record (a selector as params: map stages over labels, a verdict), so which
// changes and which list items a client sees is data, and the connector only
// counts what passed.
mod chron {
    include!("../../../target/fluxor/chronicle-common/vm_core.rs");
    include!("../../../target/fluxor/chronicle-common/pipeline_core.rs");
    include!("../../../target/fluxor/chronicle-common/hex_core.rs");
    include!("../../../target/fluxor/chronicle-common/lower_core.rs");
}
/// The filter pipeline: its IR hex as it arrives, and lowered.
const FILT_HEX: usize = 4096;
const FILT_BUF: usize = 2048;
/// Stages a filter may hold (the pipeline module's own bound).
const FILT_STAGES: usize = 8;
/// A filter's working frames: a candidate is projections only, never the
/// object, so a Chronicle record's ceiling bounds it.
const FILT_FRAME: usize = 4096;

const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
/// A nested record frame (chronicle `TY_MSG`): a repeated field's element.
const TY_MSG: u8 = 3;

/// Matches store_effect: a key derived from an identity (an mTLS peer is
/// `peer:` + 64 hex) plus the rows it indexes runs past 160, and a source
/// SKIPS a key it cannot hold — the row would simply never be seen.
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 4096;
/// One LIST PAGE. Not a ceiling: fluxor's store provider pages, filling this
/// buffer and handing back a cursor, and `list_page` walks to the end across
/// steps. Deliberately modest — a bigger buffer buys fewer syscalls, not more
/// reachable objects.
const LIST_BUF: usize = 2048;
const MAX_PREFIX: usize = 96;
/// `paths` param: `spec.host,spec.to.name,...` — dotted, comma-separated.
const MAX_PATHS_SPEC: usize = 256;
/// The `table` column spec a graph may state.
const TABLE_SPEC: usize = 2048;
/// The document schema (`schema`, _shared/docschema.rs).
const DOC_SCHEMA_MAX: usize = 2048;
/// `paths` fields land at 4..=18; 19 on are the record's own.
const MAX_PATHS: usize = 15;
/// `ints` land at 50..=56; 57 is the key remainder and 58 the clock.
const MAX_INTS: usize = 7;
const FRAME_BUF: usize = 8192;
/// A collected list (`a[].b`), joined.
const COLLECT_BUF: usize = 512;

/// Decimal digits of `v` into `out`; the length.
fn dec_into(mut v: u64, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    loop {
        i -= 1;
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}
/// Opaque LIST continuation cursor (contracts/storage/namespace.rs).
/// The contract encodes the trailing cursor's length as a u8: 255 is the most
/// a provider can hand back, so that is the size — not what any one provider
/// happens to emit.
const MAX_CURSOR: usize = 255;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    sink: i32,
    record_out: i32,
    resolved: u8,
    subscribed: u8,
    dirty: u8,
    seq: i64,
    /// Scan state. A prefix can span many LIST pages, and a step must return —
    /// so the scan is resumable: `paging` says one is in flight and `cursor`
    /// is where it resumes. Draining a whole prefix inside one step would stall
    /// the cooperative scheduler, which is what an unbounded loop here did.
    paging: u8,
    cursor: [u8; MAX_CURSOR],
    clen: u16,
    /// Non-zero when the current page is part-delivered: backpressure resumes
    /// at `resume_key` rather than at the page start, because repeating from
    /// the top makes no forward progress when a page is larger than the
    /// downstream channel — a livelock, not a slow path. By KEY, not by
    /// position: the page is re-listed on the next step, and a consumer that
    /// deleted what it was handed (a sweep) has shifted every position after
    /// it, so an index would skip exactly as many live entries as it deleted.
    page_off: u32,
    resume_key: [u8; MAX_KEY],
    resume_len: u16,
    /// A record the downstream byte stream took only part of: `frame[pend_at
    /// ..pend_len]` is still owed. A frame is never torn — the rest goes out
    /// before anything else does. (A mailbox takes a record whole or not at
    /// all, so this is only ever set on a stream edge.)
    pend_at: u16,
    pend_len: u16,
    /// The EXPANSION index already delivered for the entry `page_off` points
    /// at. The same argument as `page_off`, one level down: an object that
    /// expands into more records than the downstream channel can hold makes no
    /// forward progress if the expansion restarts at 0 — it re-emits the same
    /// prefix of indices forever and the tail is never reached. That is a
    /// livelock that LOOKS like convergence, because every index it does emit
    /// converges correctly and only the tail is missing.
    exp_off: i64,
    /// Array expansion: the LENGTHS of every list of `expand =
    /// "a[]*b[]*c[]*d[]"` for the object being expanded (up to four; 0 past
    /// the last). Index `m` is their mixed-radix number, the last list varying
    /// fastest, and a path's `#k` resolves to its k-th digit — for two lists,
    /// `#0 = m / |b|` and `#1 = m % |b|`, as before.
    exp_dims: [i64; 4],
    /// `count = 1`: a scan is TWO passes. The first walks the listing and only
    /// counts; the second emits, stamping each record with its position (field
    /// 20) and the total (field 22). A cap — "keep the newest N" — is a decision
    /// over exactly those two numbers, and a per-record decision cannot know
    /// the total any other way: the listing pages, so the last page is not in
    /// hand when the first is emitted. Two LIST walks per scan is the price,
    /// paid only by a source that asks for it.
    count: u32,
    /// `hash = <path>`: field 27 carries an 8-hex-digit FNV-1a of the bytes at
    /// that path — the pod-template-hash convention, computed where the bytes
    /// already are. The VM has no digest, and an identity for a subtree that
    /// is short enough to be a label can only come from one; same argument as
    /// the expansion trailer, that this is a pure function of the record.
    hash: [u8; 48],
    hash_len: u8,
    /// `join = <prefix>`: one record per (object, entry under that prefix) —
    /// a cross product, the SET-shaped sibling of `expand`'s count. The joined
    /// entry's last segment lands at 21, and the same joined keys `expand`
    /// builds land at 24/26 with that segment in place of the index. A
    /// DaemonSet is one Pod per ready Node: the cardinality is another
    /// prefix's, and it lives in the source for the same reason a count does.
    join: [u8; MAX_PATHS_SPEC],
    join_len: u16,
    /// `join_scoped = 1`: each join prefix is scoped to THIS object — the
    /// listing is `<prefix><tail>/…`, one record per object under the
    /// namespace — and field 24 carries the joined entry's FULL key, so a
    /// connector with no key_prefix can act on it wherever it lives. `join`
    /// may then be a comma list of prefixes: a namespace's contents are
    /// spread across every namespaced tree, and one chain sweeps them all.
    /// `2..=9` scopes to the tail's first (k - 1) segments (2: its namespace,
    /// 3: `<ns>/<name>`, …); `10` to its LAST segment.
    join_scoped: u32,
    /// `join_match = "<joined path>:<object path>"`: field 65 is "1" when the
    /// JSON object at the joined entry's path is a SUPERSET of the object's
    /// — a label selector — else "". Every joined entry is still emitted, so
    /// a decision can act on the ones that stopped matching too.
    join_match: [u8; 96],
    join_match_len: u8,
    /// `join_paths = <path,…>`: the joined entry's own fields at 60..64.
    join_paths: [u8; MAX_PATHS_SPEC],
    join_paths_len: u16,
    /// `each = "<n>=<path>[]:<m>,…;<n>=<path>{}"`: a JSON array as a REPEATED
    /// field `n` — one nested frame per element, its members `m` at 1.. (a
    /// member `x[]` is that scalar array comma-joined) — or a JSON object as
    /// one frame per member, `{1: key, 2: value}`. What a map stage iterates.
    each: [u8; MAX_PATHS_SPEC],
    each_len: u16,
    /// `join_each`: the same, over the JOINED entry; an `@<path>` item looks up
    /// in the OBJECT (see `join_terms` for the converse).
    join_each: [u8; MAX_PATHS_SPEC],
    join_each_len: u16,
    /// `join_terms`: `each` over the OBJECT, where an item ending `@<path>`
    /// also carries, per element, the JOINED entry's value at `<path>.<key>`
    /// (the element's `key`, or its member name) — absent when the joined
    /// entry has none. A selector's terms arrive paired with the labels they
    /// test, and the joined entry's own list never rides in the record.
    join_terms: [u8; MAX_PATHS_SPEC],
    join_terms_len: u16,
    /// `table`: the server-side Table's columns per kind —
    /// `Kind=Header[/format]:<cell>,…;…;*=…` (see table.rs). A LIST, GET
    /// or watch whose registration asks for a Table (`tb`) is answered as one.
    table: [u8; TABLE_SPEC],
    table_len: u16,
    /// `schema` (_shared/docschema.rs): the revision's path (`rv`), what a
    /// GET of nothing answers (`missing`), and how a view reads (`view.*`).
    schema: [u8; DOC_SCHEMA_MAX],
    schema_len: u16,
    rv: [u8; 48],
    rv_len: u8,
    missing: [u8; 256],
    missing_len: u16,
    /// `terms_max`: elements `join_terms` emits per item (0 = no cap); past it
    /// the list is cut and field 67 says so (1), so a decision can FAIL CLOSED
    /// instead of the record failing to decode.
    terms_max: u32,
    /// `list_item = <bytes>`: written before EVERY child, and
    /// `list_join = <bytes>`: written BETWEEN them (default `,`). Together they
    /// turn a set of children into whatever record shape the consumer already
    /// reads — `a=<ip>;a=<ip>` for a DNS zone — without a per-element transform
    /// the VM cannot express and without changing the consumer to suit the
    /// producer.
    list_item: [u8; 16],
    list_item_len: u8,
    list_join: [u8; 8],
    list_join_len: u8,
    /// `list_values = 1`: field 56 carries the children's VALUES only, without
    /// the `<name>=` each is normally labelled with. A backend set is a list of
    /// values; a decision cannot strip the labels afterwards.
    list_values: u32,
    /// `list_children = <prefix>`: field 56 is the object's children under
    /// `<prefix><tail><child_sep>`, materialised as `<name>=<value>,…` in key
    /// order — a view of a set, for a document that has to state one.
    list_children: [u8; MAX_PREFIX],
    list_children_len: u8,
    /// `where = "<path>=<value>"`: emit only objects whose path reads exactly
    /// that value; `"<path>="` means absent or empty. A filter, so the count
    /// mode's index is a position among the objects that QUALIFY — "the first
    /// unbound pod" is index 0 under `where = "spec.nodeName="`.
    where_: [u8; 96],
    where_len: u8,
    /// `join_pick = "min:<path>"`: the join reduces to the ONE joined entry
    /// whose integer at <path> is least (ties: lowest key). An argmin is a fold
    /// over the joined set; it lives in the source for the same reason a count
    /// does, and the decision sees one record with the winner in it.
    join_pick: [u8; 64],
    join_pick_len: u8,
    /// `now = 1`: field 58 is the monotonic clock in ms, read once per scan,
    /// on every record — so a decision compares "now >= deadline" as two
    /// integers. Reading the clock makes this instance `wall_clock` class.
    now: u32,
    /// `due = <path>`: that path holds a deadline in the same ms clock. At
    /// the end of each scan the EARLIEST deadline still ahead arms a kernel
    /// timer fd; when it fires the kernel steps this module (a timer is an
    /// fd), the scan re-arms, and the record whose time has come is emitted
    /// with `now` past its deadline. Deadlines live in the store; the timer
    /// is the alarm clock; nothing polls. `<path>+<ms>` is the deadline that
    /// long after the time the path holds — a pause a decision measures from
    /// a recorded moment.
    due: [u8; 96],
    due_len: u8,
    /// `related = "<prefix>:<path>,<path>;<prefix>:<path>"` — the SAME KEY
    /// under another prefix, projected alongside the record. This control
    /// plane keys every one of its planes by the same id (`/pod-specs/<uid>`,
    /// `/sandbox-status/<uid>`, `/probe-status/<uid>`), so "the other facts
    /// about this object" is a lookup, not a join, and a chain that needed
    /// five read stages to gather them needs none. Fields land at 70.. in
    /// blocks of ten: the first related prefix at 70..79, the second at
    /// 80..89, and so on. Absent resolves to empty, like every projection.
    related: [u8; MAX_PATHS_SPEC],
    related_len: u16,
    /// `unless = <prefix>`: emit only objects whose SAME KEY under that
    /// prefix is ABSENT — "a request with no lease", "a claim whose owner is
    /// gone". A filter like `where`, so count mode indexes among the objects
    /// that qualify, and "the first request still waiting" is index 0.
    unless: [u8; MAX_PREFIX],
    unless_len: u8,
    /// `empty = 1`: a scan that finds NOTHING qualifying emits one record
    /// anyway — tail empty, 20 = 0, 22 = 0 — so a chain can act on "the set
    /// is empty", which no per-object record can say. Set only with `count`.
    empty: u32,
    /// Objects that qualified in the current scan (for `empty`).
    scan_hits: u32,
    timer_fd: i32,
    scan_now: i64,
    /// Earliest deadline ahead of `scan_now` seen this scan; i64::MAX = none.
    next_due: i64,
    /// `child_sep`: the byte between the tail and a child's name for
    /// `count_children` and `expand_over` — `-` for ordinals (default), `/`
    /// for a namespace's objects.
    child_sep: u8,
    counting: u8,
    total: i64,
    /// Position in the listing across pages, for field 20 in count mode.
    list_idx: i64,
    emitted: u32,
    /// Frames the downstream channel refused; the pass re-arms and retries.
    deferred: u32,
    /// Passes that hit a bound (an over-long frame, or a truncated LIST).
    truncated: u32,
    prefix: [u8; MAX_PREFIX],
    prefix_len: u8,
    paths: [u8; MAX_PATHS_SPEC],
    paths_len: u16,
    /// 0 = the projected value is nested JSON and `paths` are dotted JSON
    /// paths; 1 = it is a flat `k=v;k=v` record and each `paths` entry is a
    /// field name. Both formats are in the control plane — k8s objects are
    /// JSON, the internal projections are flat — and a source that only spoke
    /// one could not feed a decision from the other.
    flat: u32,
    /// Dotted path to a COUNT. When set, the source emits one record per index
    /// `0..count+expand_tail` for each object instead of one record per object.
    /// This is where a set-shaped reconcile belongs: the cardinality is stream
    /// cardinality, which the design already permits, rather than a connector
    /// op that would decide the whole set and leave the decision node deciding
    /// nothing.
    expand: [u8; 48],
    expand_len: u8,
    /// Extra prefixes to SUBSCRIBE to but not list. A change under one of them
    /// re-arms the scan without becoming a record of its own.
    ///
    /// A source that watches only what it lists cannot self-heal: delete a Pod
    /// a ReplicaSet owns and nothing wakes the ReplicaSet, so the Pod stays
    /// gone. The module this mirrors subscribes to `/pods/` for exactly that
    /// reason, and a rollout makes it load-bearing rather than merely
    /// desirable — the roll deletes a Pod and the recreate is a later pass.
    watch: [u8; MAX_PATHS_SPEC],
    watch_len: u16,
    /// Indices emitted BEYOND the count, so scale-down has something to probe.
    /// Bounded: a converged set costs `count + tail` probes per pass.
    expand_tail: u32,
    /// `rev = 1`: field 2 carries the object's store revision (one HEAD per
    /// record), so a verdict can be made CONDITIONAL on the object being as
    /// it was judged — a prune's delete (store_effect field 28) then cannot
    /// remove a row rewritten between the read and the write.
    rev: u32,
    /// `expand_over = <prefix>`: the CHILDREN's prefix. The expansion then
    /// covers every existing child as well as the desired count — the
    /// listing of `<prefix><tail>-` is scanned for its highest ordinal, and
    /// the range extends past it. A fixed tail cannot find a surplus far
    /// above the count: scale 10 → 1 with a tail of 4 leaves ordinals 5..9
    /// never probed, orphaned for good. The desired count is the object's;
    /// the actual set is the children's; the range must cover both.
    expand_over: [u8; MAX_PREFIX],
    expand_over_len: u8,
    /// `count_children = <prefix,…>`: on a plain (unexpanded) record, field 22
    /// is how many children `<prefix><tail><child_sep>*` exist across every
    /// listed prefix, and 21 the same as digits — a per-object fold ("how many of mine are done") that a
    /// per-record decision cannot perform and a status must state.
    count_children: [u8; MAX_PREFIX],
    count_children_len: u8,
    /// `desired = <path>`: field 20 is that path parsed as an integer, so the
    /// decision can compare the fold above against the object's own target
    /// as numbers. `expand` reads the same kind of path but expands on it.
    desired: [u8; 48],
    desired_len: u8,
    /// `ints = <path,path,…>`: those paths as INTEGERS at 50..56 (absent → 0).
    ints: [u8; MAX_PATHS_SPEC],
    ints_len: u16,
    /// Bytes offered to one LIST call. Smaller = more pages, more syscalls,
    /// less state held per step; it does not bound how many objects are
    /// reachable, because `list_page` walks the cursor to the end.
    page_bytes: u32,
    list_buf: [u8; LIST_BUF],
    val_buf: [u8; MAX_VALUE],
    frame: [u8; FRAME_BUF],
    /// `events = 1`: the WATCH connector (see `events_step`).
    events: u32,
    /// `bookmark_ms`: the BOOKMARK cadence for a registration that asks for
    /// them (default `BOOKMARK_MS`, upstream's minute).
    bookmark_ms: u32,
    /// `stale_at_open = 1`: the registrations found at this runtime's
    /// first sync are a previous runtime's (no connection outlives its
    /// runtime) and are deleted unserved — else a connection that reuses
    /// a stale one's id would be answered its watch. Done once, at the
    /// first step: construction precedes every step, and a registration
    /// of this runtime needs a request carried through the whole request
    /// chain first.
    stale_at_open: u32,
    stale_purged: u8,
    /// The purge in progress: the revision its first listing was consistent
    /// at (0 until listed) — a registration newer than that is this
    /// runtime's — the page it lists next, whether a delete of this pass is
    /// still undecided, and how many registrations it has removed.
    purge_rev: u64,
    purge_cur: [u8; MAX_CURSOR],
    purge_cur_len: u8,
    purge_pending: u8,
    purge_gone: u64,
    /// `tomb = <prefix>`: where store_effect leaves a deleted object's last
    /// state, which a DELETED event carries.
    tomb: [u8; 48],
    tomb_len: u8,
    /// `label = <text>`: stamped at 48 on every record, the empty one too —
    /// what the graph is to its decisions (a node's own name).
    label: [u8; 64],
    label_len: u8,
    watchers: [Watcher; MAX_WATCHERS],
    /// The watcher a pass is serving (`EV_IDLE` between passes), and its
    /// batch of changes: `ev_buf[ev_at..ev_len]` still to emit.
    ev_w: u8,
    ev_len: u32,
    ev_at: u32,
    ev_fence: u64,
    ev_buf: [u8; EV_BUF],
    /// The LIST registration being served: its current page, the read
    /// position in it, whether it was the last, and whether `limit` cut the
    /// listing (the tail then carries `continue`).
    /// A wire line longer than one record: the heap bytes still to go as
    /// `MORE` records, and the revision and connection they belong to.
    wire: *mut u8,
    wire_len: usize,
    wire_at: usize,
    wire_rev: u64,
    wire_cid: i64,
    /// A change-feed batch that outgrew `ev_buf`, on the heap; `ev_heap` is
    /// set while the current batch lives there.
    evh: *mut u8,
    evh_cap: usize,
    ev_heap: u8,
    lp_page: [u8; LIST_BUF],
    lp_n: usize,
    lp_rp: usize,
    lp_end: u8,
    lp_cut: u8,
    /// The provider's cursor for the page after the one being walked.
    lp_next: [u8; MAX_CURSOR],
    lp_next_len: u8,
    /// `ir_stages`/`stage_kinds`/`keep`: the filter (see `chron`). A record
    /// passes when the pipeline's output field `keep` is true; a filter that
    /// does not load fails CLOSED — nothing passes, and it is said so.
    filt_hex: [u8; FILT_HEX],
    filt_hex_len: u16,
    filt_kinds_hex: [u8; 16],
    filt_kinds_hex_len: u8,
    keep: u32,
    filt: [u8; FILT_BUF],
    filt_len: u16,
    filt_kinds: [u8; FILT_STAGES],
    filt_fault: u8,
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::SCHEMA_MAX;
    use super::{LIST_BUF, MAX_PATHS_SPEC, MAX_PREFIX};

    define_params! {
        State;

        1, prefix, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.prefix_len = n as u8;
                if n > 0 { ptr_copy(s.prefix.as_mut_ptr(), d, n); }
            };

        2, paths, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.paths_len = n as u16;
                if n > 0 { ptr_copy(s.paths.as_mut_ptr(), d, n); }
            };

        3, page_bytes, u32, LIST_BUF as u32
            => |s, d, len| { s.page_bytes = p_u32(d, len, 0, LIST_BUF as u32); };

        4, flat, u32, 0
            => |s, d, len| { s.flat = p_u32(d, len, 0, 0); };

        5, expand, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.expand_len = n as u8;
                if n > 0 { ptr_copy(s.expand.as_mut_ptr(), d, n); }
            };

        6, expand_tail, u32, 4
            => |s, d, len| { s.expand_tail = p_u32(d, len, 0, 4); };

        7, watch, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.watch_len = n as u16;
                if n > 0 { ptr_copy(s.watch.as_mut_ptr(), d, n); }
            };

        8, count, u32, 0
            => |s, d, len| { s.count = p_u32(d, len, 0, 0); };

        9, hash, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.hash_len = n as u8;
                if n > 0 { ptr_copy(s.hash.as_mut_ptr(), d, n); }
            };

        11, expand_over, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.expand_over_len = n as u8;
                if n > 0 { ptr_copy(s.expand_over.as_mut_ptr(), d, n); }
            };

        12, count_children, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.count_children_len = n as u8;
                if n > 0 { ptr_copy(s.count_children.as_mut_ptr(), d, n); }
            };

        13, desired, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.desired_len = n as u8;
                if n > 0 { ptr_copy(s.desired.as_mut_ptr(), d, n); }
            };

        14, ints, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.ints_len = n as u16;
                if n > 0 { ptr_copy(s.ints.as_mut_ptr(), d, n); }
            };

        10, join, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.join_len = n as u16;
                if n > 0 { ptr_copy(s.join.as_mut_ptr(), d, n); }
            };

        15, join_scoped, u32, 0
            => |s, d, len| { s.join_scoped = p_u32(d, len, 0, 0); };

        16, child_sep, str, 0
            => |s, d, len| { s.child_sep = if len > 0 { *d } else { b'-' }; };

        17, join_match, str, 0
            => |s, d, len| {
                let n = if len > 96 { 96 } else { len };
                s.join_match_len = n as u8;
                if n > 0 { ptr_copy(s.join_match.as_mut_ptr(), d, n); }
            };

        18, join_paths, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.join_paths_len = n as u16;
                if n > 0 { ptr_copy(s.join_paths.as_mut_ptr(), d, n); }
            };

        31, each, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.each_len = n as u16;
                if n > 0 { ptr_copy(s.each.as_mut_ptr(), d, n); }
            };

        32, join_each, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.join_each_len = n as u16;
                if n > 0 { ptr_copy(s.join_each.as_mut_ptr(), d, n); }
            };

        39, join_terms, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.join_terms_len = n as u16;
                if n > 0 { ptr_copy(s.join_terms.as_mut_ptr(), d, n); }
            };

        // Chunked; past its cap the schema is dropped whole (a cut directive
        // names a different path).
        43, schema, str_chunked, 0
            => |s, d, len| {
                let at = s.schema_len as usize;
                if at + len > super::DOC_SCHEMA_MAX {
                    s.schema_len = u16::MAX;
                } else if s.schema_len != u16::MAX {
                    ptr_copy(s.schema.as_mut_ptr().add(at), d, len);
                    s.schema_len = (at + len) as u16;
                }
            };

        42, table, str_chunked, 0
            => |s, d, len| {
                let mut i = 0usize;
                while i < len && (s.table_len as usize) < super::TABLE_SPEC {
                    s.table[s.table_len as usize] = *d.add(i);
                    s.table_len += 1;
                    i += 1;
                }
            };

        40, terms_max, u32, 0
            => |s, d, len| { s.terms_max = super::p_u32(d, len, 0, 0); };

        20, where, str, 0
            => |s, d, len| {
                let n = if len > 96 { 96 } else { len };
                s.where_len = n as u8;
                if n > 0 { ptr_copy(s.where_.as_mut_ptr(), d, n); }
            };

        21, join_pick, str, 0
            => |s, d, len| {
                let n = if len > 64 { 64 } else { len };
                s.join_pick_len = n as u8;
                if n > 0 { ptr_copy(s.join_pick.as_mut_ptr(), d, n); }
            };

        26, list_item, str, 0
            => |s, d, len| {
                let n = if len > 16 { 16 } else { len };
                s.list_item_len = n as u8;
                if n > 0 { ptr_copy(s.list_item.as_mut_ptr(), d, n); }
            };

        27, list_join, str, 0
            => |s, d, len| {
                let n = if len > 8 { 8 } else { len };
                s.list_join_len = n as u8;
                if n > 0 { ptr_copy(s.list_join.as_mut_ptr(), d, n); }
            };

        25, list_values, u32, 0
            => |s, d, len| { s.list_values = p_u32(d, len, 0, 0); };

        22, now, u32, 0
            => |s, d, len| { s.now = p_u32(d, len, 0, 0); };

        23, due, str, 0
            => |s, d, len| {
                let n = if len > 96 { 96 } else { len };
                s.due_len = n as u8;
                if n > 0 { ptr_copy(s.due.as_mut_ptr(), d, n); }
            };

        24, related, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.related_len = n as u16;
                if n > 0 { ptr_copy(s.related.as_mut_ptr(), d, n); }
            };

        19, list_children, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.list_children_len = n as u8;
                if n > 0 { ptr_copy(s.list_children.as_mut_ptr(), d, n); }
            };

        28, unless, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.unless_len = n as u8;
                if n > 0 { ptr_copy(s.unless.as_mut_ptr(), d, n); }
            };

        29, empty, u32, 0
            => |s, d, len| { s.empty = super::p_u32(d, len, 0, 0); };

        30, rev, u32, 0
            => |s, d, len| { s.rev = super::p_u32(d, len, 0, 0); };

        33, events, u32, 0
            => |s, d, len| { s.events = super::p_u32(d, len, 0, 0); };

        35, bookmark_ms, u32, 60000
            => |s, d, len| { s.bookmark_ms = super::p_u32(d, len, 0, 60000); };

        41, stale_at_open, u32, 0
            => |s, d, len| { s.stale_at_open = super::p_u32(d, len, 0, 0); };

        36, ir_stages, str_chunked, 0
            => |s, d, len| {
                let mut i = 0usize;
                while i < len && (s.filt_hex_len as usize) < super::FILT_HEX {
                    s.filt_hex[s.filt_hex_len as usize] = *d.add(i);
                    s.filt_hex_len += 1;
                    i += 1;
                }
                if i < len { s.filt_fault = 1; }
            };

        37, stage_kinds, str, 0
            => |s, d, len| {
                let n = if len > 16 { 16 } else { len };
                s.filt_kinds_hex_len = n as u8;
                if n > 0 { ptr_copy(s.filt_kinds_hex.as_mut_ptr(), d, n); }
            };

        38, keep, u32, 99
            => |s, d, len| { s.keep = super::p_u32(d, len, 0, 99); };

        34, tomb, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.tomb_len = n as u8;
                if n > 0 { ptr_copy(s.tomb.as_mut_ptr(), d, n); }
            };

        44, label, str, 0
            => |s, d, len| {
                let n = if len > 64 { 64 } else { len };
                s.label_len = n as u8;
                if n > 0 { ptr_copy(s.label.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

unsafe fn get_value(sys: &SyscallTable, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return None;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return None;
    }
    let mut rarg = [0u8; 20];
    rarg[8..12].copy_from_slice(&(dst.len() as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
    // A value the buffer cannot hold is REFUSED, never read as a prefix of
    // itself: a projection over a cut document reads whatever the cut left
    // (a label set missing its tail, a spec without its last container) as
    // the object. `RANGE_GET` reports bytes copied, so a full buffer is
    // ambiguous; one byte past it settles it (as `_shared/store.rs` does).
    let mut cut = false;
    if n >= 0 && n as usize == dst.len() {
        let mut probe = [0u8; 1];
        let mut parg = [0u8; 20];
        parg[0..8].copy_from_slice(&(dst.len() as u64).to_le_bytes());
        parg[8..12].copy_from_slice(&1u32.to_le_bytes());
        parg[12..20].copy_from_slice(&(probe.as_mut_ptr() as u64).to_le_bytes());
        cut = (sys.provider_call)(h, OBJ_RANGE_GET, parg.as_mut_ptr(), 20) > 0;
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    if cut {
        let m = b"[store_source] value larger than the buffer - REFUSED, not read cut";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return None;
    }
    if n < 0 {
        None
    } else {
        Some(n as usize)
    }
}

/// Entries in a comma-separated spec.
fn entries(spec: &[u8]) -> usize {
    if spec.is_empty() {
        0
    } else {
        spec.iter().filter(|&&b| b == b',').count() + 1
    }
}

unsafe fn store_subscribe(sys: &SyscallTable, prefix: &[u8], sink: i32) -> i32 {
    let mut arg = [0u8; MAX_KEY + 16];
    if prefix.len() + 7 > arg.len() {
        return -1;
    }
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    arg[p..p + 4].copy_from_slice(&(sink as u32).to_le_bytes());
    p += 4;
    arg[p] = 0;
    p += 1;
    let rc = (sys.provider_call)(-1, NS_SUBSCRIBE, arg.as_mut_ptr(), p);
    if rc < 0 {
        // The store's subscription table is full (or the prefix is refused):
        // this source still scans once, but a change here will never wake it
        // again — a controller that looks converged and is not.
        let m = b"[store_source] SUBSCRIBE refused - changes under this prefix will NOT wake this source";
        dev_log(sys, 1, m.as_ptr(), m.len());
    }
    rc
}

unsafe fn drain_changes(sys: &SyscallTable, sink: i32) -> u32 {
    let mut n = 0u32;
    let mut buf = [0u8; 512];
    loop {
        let poll = (sys.channel_poll)(sink, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            break;
        }
        let r = (sys.channel_read)(sink, buf.as_mut_ptr(), buf.len());
        if r <= 0 {
            break;
        }
        n += 1;
    }
    n
}

/// One page of a storage.namespace LIST, RAW: `out` is handed to the provider
/// and comes back as `[name_len:u8][kind:u8][name]…` entries followed by the
/// trailing `[0xFF][0xFF][cursor_len:u8][cursor]` record. Nothing is repacked, so
/// there is no second buffer for a page to overflow, and the trailing record
/// is always reached: "listing complete" can only ever come from the provider
/// saying so with a zero-length cursor.
///
/// Returns (page length in bytes, cursor length written to `cursor_out`); a
/// zero cursor length means the listing is complete. A page the provider
/// refuses is reported at error level and returned as empty AND complete —
/// which is wrong, but loud, and the next change re-arms the scan.
unsafe fn list_page(
    sys: &SyscallTable,
    prefix: &[u8],
    cursor_in: &[u8],
    cap: usize,
    out: &mut [u8],
    cursor_out: &mut [u8],
) -> (usize, usize) {
    let mut fence = 0u64;
    list_page_fenced(sys, prefix, cursor_in, cap, out, cursor_out, &mut fence)
}

/// [`list_page`], with the revision the page is consistent at
/// (`fence_revision` of its `ViewConsistent` fence).
unsafe fn list_page_fenced(
    sys: &SyscallTable,
    prefix: &[u8],
    cursor_in: &[u8],
    cap: usize,
    out: &mut [u8],
    cursor_out: &mut [u8],
    fence_out: &mut u64,
) -> (usize, usize) {
    let mut larg = [0u8; MAX_KEY + MAX_CURSOR + 32];
    if 2 + prefix.len() + 2 + cursor_in.len() + 8 + 4 + 8 + 2 > larg.len() {
        return (0, 0);
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    larg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    larg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    larg[p..p + 2].copy_from_slice(&(cursor_in.len() as u16).to_le_bytes());
    p += 2;
    if !cursor_in.is_empty() {
        larg[p..p + cursor_in.len()].copy_from_slice(cursor_in);
        p += cursor_in.len();
    }
    larg[p..p + 8].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    let cap = cap.clamp(64, out.len());
    larg[p..p + 4].copy_from_slice(&(cap as u32).to_le_bytes());
    p += 4;
    larg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    larg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, NS_LIST, larg.as_mut_ptr(), p);
    *fence_out = fence_revision(&fence);
    if n < 0 {
        let m = b"[store_source] LIST refused a page - NOTHING emitted this pass";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return (0, 0);
    }
    let n = (n as usize).min(cap);
    // Find the trailing record to lift the cursor out; entries are consumed
    // by the caller, in place.
    let mut rp = 0usize;
    let mut clen = 0usize;
    while rp < n {
        let name_len = out[rp] as usize;
        // The trailer is BOTH sentinel bytes. A name of exactly 255 bytes
        // makes `name_len` 0xFF too, and stopping on that alone drops the
        // entry and every one after it; `kind` never takes the value 0xFF.
        if name_len == 0xFF && rp + 1 < n && out[rp + 1] == 0xFF {
            if rp + 3 <= n {
                let cl = out[rp + 2] as usize;
                if cl > 0 && rp + 3 + cl <= n && cl <= cursor_out.len() {
                    cursor_out[..cl].copy_from_slice(&out[rp + 3..rp + 3 + cl]);
                    clen = cl;
                } else if cl > 0 {
                    let m = b"[store_source] LIST cursor does not fit - scan INCOMPLETE";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                }
            }
            break;
        }
        if rp + 2 + name_len > n {
            let m = b"[store_source] LIST entry overruns its page - scan INCOMPLETE";
            dev_log(sys, 1, m.as_ptr(), m.len());
            break;
        }
        rp += 2 + name_len;
    }
    (n, clen)
}

/// The next raw entry at `rp`, or None at the trailing record / end of page.
/// Returns (name, next rp).
fn raw_entry(page: &[u8], rp: usize) -> Option<(&[u8], usize)> {
    if rp >= page.len() {
        return None;
    }
    let name_len = page[rp] as usize;
    // Trailer = both sentinel bytes; `name_len` alone would also match a
    // 255-byte name and end the page one entry early.
    if name_len == 0xFF && rp + 1 < page.len() && page[rp + 1] == 0xFF {
        return None;
    }
    if rp + 2 + name_len > page.len() {
        return None;
    }
    Some((&page[rp + 2..rp + 2 + name_len], rp + 2 + name_len))
}

fn put_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    if payload.len() > u16::MAX as usize || at + 4 + payload.len() > out.len() {
        return None;
    }
    out[at] = number;
    out[at + 1] = ty;
    out[at + 2..at + 4].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    out[at + 4..at + 4 + payload.len()].copy_from_slice(payload);
    Some(at + 4 + payload.len())
}

/// `;`-separated `name=value` lookup, for the control plane's flat records.
fn flat_field<'a>(value: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0usize;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';')
            .map(|i| start + i)
            .unwrap_or(value.len());
        let seg = &value[start..end];
        if seg.len() > name.len() && seg[..name.len()] == *name && seg[name.len()] == b'=' {
            return Some(&seg[name.len() + 1..]);
        }
        if end >= value.len() {
            break;
        }
        start = end + 1;
    }
    None
}

/// Split one dotted path (`spec.to.name`) into segments for `j_path`.
fn split_dots<'a>(spec: &'a [u8], segs: &mut [&'a [u8]; 6]) -> usize {
    let mut n = 0;
    let mut start = 0;
    while start <= spec.len() && n < segs.len() {
        let end = spec[start..]
            .iter()
            .position(|&b| b == b'.')
            .map(|i| start + i)
            .unwrap_or(spec.len());
        segs[n] = &spec[start..end];
        n += 1;
        if end >= spec.len() {
            break;
        }
        start = end + 1;
    }
    n
}

/// Project one object into a frame. Returns the frame length, or 0 if it did
/// not fit — a truncated frame is never emitted.
/// Resolve the index segments of an array expansion in place: `#` is the
/// element index, and for a cross product `#0`/`#1` are its two coordinates.
/// A record that is not an expansion leaves the segments untouched.
/// The k-th digit of expansion index `i` over the list lengths `dims` (see
/// `exp_dims`): the last list varies fastest. With one list (or none) every
/// digit is the index itself, which is what `#` means.
fn exp_digit(i: i64, k: usize, dims: &[i64; 4]) -> i64 {
    let n = dims.iter().take_while(|&&d| d > 0).count();
    if n < 2 || k >= n {
        return i;
    }
    let mut below = 1i64;
    for d in dims.iter().take(n).skip(k + 1) {
        below = below.saturating_mul(*d);
    }
    (i / below.max(1)) % dims[k].max(1)
}

fn subst_index<'a>(
    segs: &mut [&'a [u8]; 6],
    ns: usize,
    idx: Option<i64>,
    dims: &[i64; 4],
    bufs: &'a mut [[u8; 20]; 5],
) {
    let Some(i) = idx else {
        return;
    };
    let [b0, b1, b2, b3, b4] = bufs;
    let n0 = write_dec(b0, i.max(0) as u64);
    let n1 = write_dec(b1, exp_digit(i, 0, dims).max(0) as u64);
    let n2 = write_dec(b2, exp_digit(i, 1, dims).max(0) as u64);
    let n3 = write_dec(b3, exp_digit(i, 2, dims).max(0) as u64);
    let n4 = write_dec(b4, exp_digit(i, 3, dims).max(0) as u64);
    let (s0, s1, s2, s3, s4): (&'a [u8], &'a [u8], &'a [u8], &'a [u8], &'a [u8]) =
        (&b0[..n0], &b1[..n1], &b2[..n2], &b3[..n3], &b4[..n4]);
    for seg in segs.iter_mut().take(ns) {
        match *seg {
            b"#" => *seg = s0,
            b"#0" => *seg = s1,
            b"#1" => *seg = s2,
            b"#2" => *seg = s3,
            b"#3" => *seg = s4,
            _ => {}
        }
    }
}

unsafe fn project(
    s: &State,
    key_tail: &[u8],
    value: &[u8],
    expand_idx: Option<(i64, i64)>,
    list_pos: Option<(i64, i64)>,
    join: Option<(i64, i64, &[u8])>,
    out: &mut [u8],
) -> usize {
    let mut p = 1usize;
    let mut n = 0u8;
    let Some(q) = put_field(out, p, 1, TY_I64, &s.seq.to_le_bytes()) else {
        return 0;
    };
    p = q;
    n += 1;
    // The object's revision when `rev = 1` (a HEAD); otherwise 0, "unknown",
    // which a consumer must treat as "re-read before committing" rather than
    // as a usable precondition.
    let mut revision = 0i64;
    if s.rev != 0 {
        let plen = s.prefix_len as usize;
        let mut k = [0u8; MAX_KEY + MAX_PREFIX];
        let a = append_bytes(&mut k, 0, &s.prefix[..plen]);
        let kl = append_bytes(&mut k, a, key_tail);
        if kl <= MAX_KEY {
            revision = head_rev(&*s.syscalls, &k[..kl]).unwrap_or(0) as i64;
        }
    }
    let Some(q) = put_field(out, p, 2, TY_I64, &revision.to_le_bytes()) else {
        return 0;
    };
    p = q;
    n += 1;
    let Some(q) = put_field(out, p, 3, TY_BYTES, key_tail) else {
        return 0;
    };
    p = q;
    n += 1;
    if s.label_len > 0 {
        let Some(q) = put_field(out, p, 48, TY_BYTES, &s.label[..s.label_len as usize]) else {
            return 0;
        };
        p = q;
        n += 1;
    }

    // One field per `paths` entry, in order. Absent resolves to empty rather
    // than being omitted, so field numbers stay stable across objects — a
    // decision indexes by number, and a shifting layout would silently
    // misread.
    let spec = &s.paths[..s.paths_len as usize];
    let mut start = 0usize;
    let mut idx = 0usize;
    while start <= spec.len() && idx < MAX_PATHS {
        let end = spec[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(spec.len());
        let one = &spec[start..end];
        if !one.is_empty() {
            let whole = one;
            let (one, modifier) = split_mod(one);
            // A query decodes each ELEMENT with the modifier, not the joined
            // list, so it keeps the modifier and the list is used as is.
            #[allow(
                clippy::manual_contains,
                reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
            )]
            let query = one.iter().any(|&c| c == b'[');
            let modifier = if query { None } else { modifier };
            let one = if query { whole } else { one };
            let mut cbuf = [0u8; COLLECT_BUF];
            let got = if one == b"." {
                // The WHOLE VALUE, unparsed. A projection reads FIELDS out of
                // an object, which is right for a reconciler and wrong for
                // anything bridging the store to something that wants opaque
                // bytes — a kagi key frame, an endpoint address, a document a
                // decision only forwards. Without it, reaching a raw value
                // takes a `store_effect` GET — a second round trip to read
                // what this walk already has in hand.
                value
            } else if s.flat != 0 && one.ends_with(b".#") && expand_idx.is_some() {
                // A flat list, expanded (`expand = <field>[]`): this record's
                // element of `<field>=a,b,c`.
                let i = expand_idx.map(|(i, _)| i).unwrap_or(0).max(0) as usize;
                flat_field(value, &one[..one.len() - 2])
                    .and_then(|l| l.split(|&c| c == b',').nth(i))
                    .unwrap_or(&[])
            } else if s.flat != 0 {
                flat_field(value, one).unwrap_or(&[])
            } else if query {
                // `a[].b` collects, `a[k=v].b` selects (see `_shared/jsonq.rs`).
                // `#`, `#0`, `#1` segments are this record's expansion index,
                // substituted first, as in any path.
                let mut qbuf = [0u8; 160];
                let mut ql = 0usize;
                let i0 = expand_idx.map(|(i, _)| i).unwrap_or(0);
                for (k, seg) in one.split(|&c| c == b'.').enumerate() {
                    let mut digits = [0u8; 20];
                    let rep: &[u8] = match seg {
                        b"#" | b"#0" | b"#1" | b"#2" | b"#3" if expand_idx.is_some() => {
                            let v = match seg {
                                b"#0" => exp_digit(i0, 0, &s.exp_dims),
                                b"#1" => exp_digit(i0, 1, &s.exp_dims),
                                b"#2" => exp_digit(i0, 2, &s.exp_dims),
                                b"#3" => exp_digit(i0, 3, &s.exp_dims),
                                _ => i0,
                            };
                            let n = dec_into(v as u64, &mut digits);
                            &digits[..n]
                        }
                        _ => seg,
                    };
                    let need = rep.len() + (k > 0) as usize;
                    if ql + need > qbuf.len() {
                        ql = 0;
                        break;
                    }
                    if k > 0 {
                        qbuf[ql] = b'.';
                        ql += 1;
                    }
                    qbuf[ql..ql + rep.len()].copy_from_slice(rep);
                    ql += rep.len();
                }
                let len = match jq(value, &qbuf[..ql], &mut cbuf) {
                    Some(n) => n,
                    None => {
                        // Too long is a different list. NOT empty — an empty
                        // list reads as "every peer" — but the one entry
                        // nothing can name, so a consumer refuses it.
                        cbuf[0] = b'-';
                        1
                    }
                };
                &cbuf[..len]
            } else if one.len() > 2 && one.ends_with(b".*") {
                // `path.*`: the object's MEMBERS, braces stripped, so a
                // decision can splice one more member in with literals —
                // `{"nodeName":"n",` + members + `}`. The VM cannot open a
                // brace; the source can, once. Empty for an empty object, so
                // the decision can choose the comma-less form.
                let mut segs: [&[u8]; 6] = [&[]; 6];
                let ns = split_dots(&one[..one.len() - 2], &mut segs);
                match j_sub(value, &segs[..ns]) {
                    Some(sub) => object_members(sub),
                    None => &[],
                }
            } else {
                let mut ibufs = [[0u8; 20]; 5];
                let mut segs: [&[u8]; 6] = [&[]; 6];
                let ns = split_dots(one, &mut segs);
                subst_index(
                    &mut segs,
                    ns,
                    expand_idx.map(|(i, _)| i),
                    &s.exp_dims,
                    &mut ibufs,
                );
                // A path may name a SUBTREE (`spec.template.spec`) as well as a
                // scalar. `j_path` reads scalars only, so try the subtree first
                // and fall back — a pod template has to cross as bytes.
                j_sub(value, &segs[..ns])
                    .or_else(|| j_path(value, &segs[..ns]))
                    .unwrap_or(&[])
            };
            let mut modbuf = [0u8; 512];
            let got = match modifier {
                Some(m) => {
                    let n = apply_mod(m, got, &mut modbuf);
                    &modbuf[..n]
                }
                None => got,
            };
            let Some(q) = put_field(out, p, 4 + idx as u8, TY_BYTES, got) else {
                return 0;
            };
            p = q;
            n += 1;
            idx += 1;
        }
        if end >= spec.len() {
            break;
        }
        start = end + 1;
    }
    // The key's own segments, always: 28 = the tail's FIRST segment (the
    // namespace, under every namespaced prefix) and 29 = its LAST (the name).
    // A document may not carry metadata.namespace at all — the CLI's do not —
    // and the compiled reconcilers always took both from the key. A chain that
    // read them from the document instead once probed `/deployments.apps//web`
    // and collected a live ReplicaSet. The key is the truth; the VM cannot
    // split it; so the source does, once.
    {
        let first = match key_tail.iter().position(|&b| b == b'/') {
            Some(i) => &key_tail[..i],
            None => key_tail,
        };
        let last = match key_tail.iter().rposition(|&b| b == b'/') {
            Some(i) => &key_tail[i + 1..],
            None => key_tail,
        };
        let Some(q) = put_field(out, p, 28, TY_BYTES, first) else {
            return 0;
        };
        p = q;
        n += 1;
        let Some(q) = put_field(out, p, 29, TY_BYTES, last) else {
            return 0;
        };
        p = q;
        n += 1;
        // 57: the tail AFTER its first segment — "<ns>/<pod>" out of
        // "<node>/<ns>/<pod>" — for a record that lives under one key and
        // names an object under another.
        let rest = match key_tail.iter().position(|&b| b == b'/') {
            Some(i) => &key_tail[i + 1..],
            None => &key_tail[..0],
        };
        let Some(q) = put_field(out, p, 57, TY_BYTES, rest) else {
            return 0;
        };
        p = q;
        n += 1;
    }
    // Related prefixes: the same key elsewhere, ten fields each from 70.
    if s.related_len > 0 {
        let sys = &*s.syscalls;
        let spec = &s.related[..s.related_len as usize];
        let mut start = 0usize;
        let mut blk = 0u8;
        while start <= spec.len() && blk < 6 {
            let end = spec[start..]
                .iter()
                .position(|&b| b == b';')
                .map(|i| start + i)
                .unwrap_or(spec.len());
            let one = &spec[start..end];
            if !one.is_empty() {
                let colon = one.iter().position(|&b| b == b':').unwrap_or(one.len());
                let mut prefix = &one[..colon];
                // `<prefix>!flat` reads the RELATED record as `k=v;k=v` rather
                // than as JSON. The two shapes both live in the control plane —
                // a Route is JSON, its `/route-status/` is flat — and a source
                // only knows the shape of its OWN objects. Without this the
                // projection silently reads nothing: `j_path` finds no path in
                // a flat record, and an empty field looks exactly like an
                // absent one.
                let mut rel_flat = s.flat != 0;
                if prefix.len() > 5 && prefix.ends_with(b"!flat") {
                    rel_flat = true;
                    prefix = &prefix[..prefix.len() - 5];
                } else if prefix.len() > 5 && prefix.ends_with(b"!json") {
                    // And the converse: a flat source asking after a JSON
                    // object (a row asking after its Service).
                    rel_flat = false;
                    prefix = &prefix[..prefix.len() - 5];
                }
                // `<prefix>^<n>`: the key's n-th PARENT under that prefix —
                // the tail with its last n segments dropped. A row keyed
                // `<ns>/<svc>/<pod>` asks after its Service at `<ns>/<svc>`.
                // `<prefix>^<n>+<leaf>`: that parent's `<leaf>` child — a
                // session's `status` beside its `ctl`.
                let mut leaf: &[u8] = &[];
                if let Some(c) = prefix.iter().rposition(|&b| b == b'^') {
                    if let Some(plus) = prefix[c..].iter().position(|&b| b == b'+') {
                        leaf = &prefix[c + plus + 1..];
                        prefix = &prefix[..c + plus];
                    }
                }
                let mut up = 0usize;
                if prefix.len() > 2
                    && prefix[prefix.len() - 2] == b'^'
                    && prefix[prefix.len() - 1].is_ascii_digit()
                {
                    up = (prefix[prefix.len() - 1] - b'0') as usize;
                    prefix = &prefix[..prefix.len() - 2];
                }
                let mut rel_tail = key_tail;
                for _ in 0..up {
                    rel_tail = match rel_tail.iter().rposition(|&b| b == b'/') {
                        Some(i) => &rel_tail[..i],
                        None => &rel_tail[..0],
                    };
                }
                let paths = if colon < one.len() {
                    &one[colon + 1..]
                } else {
                    &one[..0]
                };
                let mut rk = [0u8; MAX_KEY + MAX_PREFIX];
                let rl = {
                    let a = append_bytes(&mut rk, 0, prefix);
                    let b = append_bytes(&mut rk, a, rel_tail);
                    if leaf.is_empty() {
                        b
                    } else {
                        let c = append_bytes(&mut rk, b, b"/");
                        append_bytes(&mut rk, c, leaf)
                    }
                };
                let mut rv = [0u8; MAX_VALUE];
                let rvl = if rl <= MAX_KEY {
                    get_value(sys, &rk[..rl], &mut rv).unwrap_or(0)
                } else {
                    0
                };
                let rval = &rv[..rvl];
                let base = 70u8 + blk * 10;
                let mut ps = 0usize;
                let mut fno = 0u8;
                while ps <= paths.len() && fno < 10 {
                    let pe = paths[ps..]
                        .iter()
                        .position(|&b| b == b',')
                        .map(|i| ps + i)
                        .unwrap_or(paths.len());
                    let path = &paths[ps..pe];
                    if !path.is_empty() {
                        // A trailing `#` asks for the field as an INTEGER: a
                        // decision that must add a grace period to the clock
                        // needs a number, not the digits of one.
                        let as_int = path.last() == Some(&b'#');
                        let path = if as_int {
                            &path[..path.len() - 1]
                        } else {
                            path
                        };
                        let (path, modifier) = split_mod(path);
                        let got = if path == b"." {
                            rval
                        } else if rel_flat {
                            flat_field(rval, path).unwrap_or(&[])
                        } else {
                            let mut segs: [&[u8]; 6] = [&[]; 6];
                            let ns = split_dots(path, &mut segs);
                            j_sub(rval, &segs[..ns])
                                .or_else(|| j_path(rval, &segs[..ns]))
                                .unwrap_or(&[])
                        };
                        let mut modbuf = [0u8; 64];
                        let got = match modifier {
                            Some(m) => {
                                let n = apply_mod(m, got, &mut modbuf);
                                &modbuf[..n]
                            }
                            None => got,
                        };
                        let iv = (parse_dec_u64(got) as i64).to_le_bytes();
                        let (ty, bytes): (u8, &[u8]) = if as_int {
                            (TY_I64, &iv)
                        } else {
                            (TY_BYTES, got)
                        };
                        let Some(q) = put_field(out, p, base + fno, ty, bytes) else {
                            return 0;
                        };
                        p = q;
                        n += 1;
                        fno += 1;
                    }
                    if pe >= paths.len() {
                        break;
                    }
                    ps = pe + 1;
                }
                blk += 1;
            }
            if end >= spec.len() {
                break;
            }
            start = end + 1;
        }
    }
    // 58: the clock, when asked for — once per scan, so every record of a
    // scan agrees on what time it is.
    if s.now != 0 || s.due_len > 0 {
        let Some(q) = put_field(out, p, 58, TY_I64, &s.scan_now.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
    }
    // Integer projections at 50..54, always emitted when configured.
    if s.ints_len > 0 {
        let spec = &s.ints[..s.ints_len as usize];
        let mut start = 0usize;
        let mut fno = 50u8;
        // 50..56: seven at most — 57 is the key remainder and 58 the clock, so an
        // eighth int would collide with a field a decision already reads.
        while start <= spec.len() && fno < 57 {
            let end = spec[start..]
                .iter()
                .position(|&b| b == b',')
                .map(|i| start + i)
                .unwrap_or(spec.len());
            let one = &spec[start..end];
            if !one.is_empty() {
                // `.` is the whole value, and `<path>!<mod>` decodes first —
                // `.!ip4` reads a raw lease address as the number it spells.
                let (one, modifier) = split_mod(one);
                let raw = if one == b"." {
                    value
                } else if s.flat != 0 {
                    flat_field(value, one).unwrap_or(&[])
                } else {
                    let mut ibufs = [[0u8; 20]; 5];
                    let mut segs: [&[u8]; 6] = [&[]; 6];
                    let ns = split_dots(one, &mut segs);
                    subst_index(
                        &mut segs,
                        ns,
                        expand_idx.map(|(i, _)| i),
                        &s.exp_dims,
                        &mut ibufs,
                    );
                    j_path(value, &segs[..ns]).unwrap_or(&[])
                };
                let mut modbuf = [0u8; 64];
                let raw = match modifier {
                    Some(m) => {
                        let n = apply_mod(m, raw, &mut modbuf);
                        &modbuf[..n]
                    }
                    None => raw,
                };
                let v = parse_dec_u64(raw) as i64;
                let Some(q) = put_field(out, p, fno, TY_I64, &v.to_le_bytes()) else {
                    return 0;
                };
                p = q;
                n += 1;
                fno += 1;
            }
            if end >= spec.len() {
                break;
            }
            start = end + 1;
        }
    }
    // Repeated fields: one nested frame per array element / object member.
    if s.each_len > 0 {
        let spec = core::slice::from_raw_parts(s.each.as_ptr(), s.each_len as usize);
        let idx = expand_idx.map(|(i, _)| i);
        let Some((q, m, _)) = each_fields(out, p, value, spec, idx, &s.exp_dims, &[], 0) else {
            return 0;
        };
        p = q;
        n += m;
    }
    // The content digest, at a fixed number like the trailers below. Absent
    // path → digest of nothing, still emitted: a decision comparing hashes
    // must never find the field missing.
    if s.hash_len > 0 {
        let hp = &s.hash[..s.hash_len as usize];
        let mut segs: [&[u8]; 6] = [&[]; 6];
        let ns = split_dots(hp, &mut segs);
        // `.` digests the WHOLE value: an identity for "this object as it is
        // now", which rows derived from it carry so a stale one can be found.
        let bytes = if hp == b"." {
            value
        } else {
            j_sub(value, &segs[..ns])
                .or_else(|| j_path(value, &segs[..ns]))
                .unwrap_or(&[])
        };
        let v = fnv1a(bytes);
        let hexd = b"0123456789abcdef";
        let mut h = [0u8; 8];
        let mut x = v;
        for i in (0..8).rev() {
            h[i] = hexd[(x & 0xf) as usize];
            x >>= 4;
        }
        let Some(q) = put_field(out, p, 27, TY_BYTES, &h) else {
            return 0;
        };
        p = q;
        n += 1;
    }
    // Expansion trailer, at FIXED field numbers so a decision can index them
    // however many paths precede. The index appears twice on purpose: as an
    // i64 to compare against the count, and as decimal bytes because the key
    // needs `<name>-<ordinal>` and the VM cannot render an int as a string.
    if let Some((i, count)) = expand_idx {
        let Some(q) = put_field(out, p, 20, TY_I64, &i.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
        let mut dec = [0u8; 12];
        let dl = write_dec(&mut dec, i as u64);
        let Some(q) = put_field(out, p, 21, TY_BYTES, &dec[..dl]) else {
            return 0;
        };
        p = q;
        n += 1;
        let Some(q) = put_field(out, p, 22, TY_I64, &count.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
        // The PREDECESSOR index, as digits. An ordered rollout needs to ask
        // about `i - 1`, and the VM can neither subtract into a string nor
        // render an int — so the arithmetic happens here, once, where the index
        // is already known. Empty at index 0: there is no predecessor, and the
        // decision reads that as "nothing gates me".
        let mut prev = [0u8; 12];
        let pl = if i > 0 {
            write_dec(&mut prev, (i - 1) as u64)
        } else {
            0
        };
        let Some(q) = put_field(out, p, 23, TY_BYTES, &prev[..pl]) else {
            return 0;
        };
        p = q;
        n += 1;
        // The EXPANDED KEYS, built once here: `<tail>-<i>` and `<tail>-<i-1>`.
        // A decision could assemble them from parts, but every part costs a
        // field and a constructed message is capped at 16 — building the key
        // here is both cheaper and the same argument as everywhere else, that
        // joining bytes is not the VM's job.
        let mut ek = [0u8; MAX_KEY + 16];
        let mut e = append_bytes(&mut ek, 0, key_tail);
        e = append_bytes(&mut ek, e, b"-");
        let mut d2 = [0u8; 12];
        let d2l = write_dec(&mut d2, i as u64);
        e = append_bytes(&mut ek, e, &d2[..d2l]);
        let Some(q) = put_field(out, p, 24, TY_BYTES, &ek[..e]) else {
            return 0;
        };
        p = q;
        n += 1;
        let plen2 = if pl > 0 {
            let mut pk = append_bytes(&mut ek, 0, key_tail);
            pk = append_bytes(&mut ek, pk, b"-");
            append_bytes(&mut ek, pk, &prev[..pl])
        } else {
            0
        };
        let Some(q) = put_field(out, p, 25, TY_BYTES, &ek[..plen2]) else {
            return 0;
        };
        p = q;
        n += 1;
        // The SUCCESSOR's key, `<tail>-<i+1>`: an ordered teardown removes the
        // highest ordinal first, and "my successor is absent" is how a
        // per-record decision knows it is the highest.
        let mut d3 = [0u8; 12];
        let d3l = write_dec(&mut d3, (i + 1) as u64);
        let mut sk = append_bytes(&mut ek, 0, key_tail);
        sk = append_bytes(&mut ek, sk, b"-");
        sk = append_bytes(&mut ek, sk, &d3[..d3l]);
        let Some(q) = put_field(out, p, 19, TY_BYTES, &ek[..sk]) else {
            return 0;
        };
        p = q;
        n += 1;
        // The expanded key's LAST SEGMENT: `<name>-<i>` without the namespace.
        // Field 24 is the store key, which carries `<ns>/` because that is what
        // addresses the object; a document that NAMES the object wants the bare
        // name. Both are the same join, done once here for the same reason.
        let mut nk = [0u8; MAX_KEY + 16];
        // The tail's last segment, inline: this module includes `json.rs` and
        // nothing else, and pulling in the whole store-helper set for one split
        // would cost more than the split does.
        let seg = match key_tail.iter().rposition(|&b| b == b'/') {
            Some(i) => &key_tail[i + 1..],
            None => key_tail,
        };
        let mut g = append_bytes(&mut nk, 0, seg);
        g = append_bytes(&mut nk, g, b"-");
        g = append_bytes(&mut nk, g, &d2[..d2l]);
        let Some(q) = put_field(out, p, 26, TY_BYTES, &nk[..g]) else {
            return 0;
        };
        p = q;
        n += 1;
    } else if let Some((i0, count, seg)) = join {
        let i = list_pos.map(|(li, _)| li).unwrap_or(i0);
        if s.join_scoped != 0 {
            // Scoped join: 24 is the joined entry's FULL key, 21 its last
            // segment, 22 the count. Nothing is built from the tail here —
            // the joined object is where it is.
            let Some(q) = put_field(out, p, 20, TY_I64, &i.to_le_bytes()) else {
                return 0;
            };
            p = q;
            n += 1;
            let last = match seg.iter().rposition(|&b| b == b'/') {
                Some(x) => &seg[x + 1..],
                None => seg,
            };
            let Some(q) = put_field(out, p, 21, TY_BYTES, last) else {
                return 0;
            };
            p = q;
            n += 1;
            let Some(q) = put_field(out, p, 22, TY_I64, &count.to_le_bytes()) else {
                return 0;
            };
            p = q;
            n += 1;
            let Some(q) = put_field(out, p, 24, TY_BYTES, seg) else {
                return 0;
            };
            p = q;
            n += 1;
            let Some((q, m)) = joined_fields(s, out, p, n, value, seg) else {
                return 0;
            };
            p = q;
            n = m;
            out[0] = n;
            return p;
        }
        // Join trailer: the same numbers as the expansion, with the joined
        // entry's last segment where the index digits would be — 21 the
        // segment, 24 `<tail>-<seg>`, 26 `<lastseg>-<seg>` — so one decision
        // shape serves a count-expanded record and a set-expanded one alike.
        let Some(q) = put_field(out, p, 20, TY_I64, &i.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
        let Some(q) = put_field(out, p, 21, TY_BYTES, seg) else {
            return 0;
        };
        p = q;
        n += 1;
        let Some(q) = put_field(out, p, 22, TY_I64, &count.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
        let mut ek = [0u8; MAX_KEY + 16];
        let mut e = append_bytes(&mut ek, 0, key_tail);
        e = append_bytes(&mut ek, e, b"-");
        e = append_bytes(&mut ek, e, seg);
        let Some(q) = put_field(out, p, 24, TY_BYTES, &ek[..e]) else {
            return 0;
        };
        p = q;
        n += 1;
        let last = match key_tail.iter().rposition(|&b| b == b'/') {
            Some(i) => &key_tail[i + 1..],
            None => key_tail,
        };
        let mut nk = [0u8; MAX_KEY + 16];
        let mut g = append_bytes(&mut nk, 0, last);
        g = append_bytes(&mut nk, g, b"-");
        g = append_bytes(&mut nk, g, seg);
        let Some(q) = put_field(out, p, 26, TY_BYTES, &nk[..g]) else {
            return 0;
        };
        p = q;
        n += 1;
        // The joined object's own projections, as a scoped join has them: the
        // entry's full key is its prefix and its name.
        if s.join_paths_len > 0
            || s.join_match_len > 0
            || s.join_each_len > 0
            || s.join_terms_len > 0
        {
            let mut jk = [0u8; MAX_PREFIX + MAX_KEY];
            let mut l = append_bytes(&mut jk, 0, first_join_prefix(s));
            l = append_bytes(&mut jk, l, seg);
            let Some((q, m)) = joined_fields(s, out, p, n, value, &jk[..l]) else {
                return 0;
            };
            p = q;
            n = m;
        }
    } else if s.count_children_len > 0 || s.desired_len > 0 || s.list_children_len > 0 {
        // The per-object fold and the target, as numbers, on a plain record.
        let sys = &*s.syscalls;
        if s.list_children_len > 0 {
            let mut lc = [0u8; 2048];
            let view = ChildView {
                sep: s.child_sep,
                values_only: s.list_values != 0,
                item_prefix: &s.list_item[..s.list_item_len as usize],
                join: if s.list_join_len > 0 {
                    &s.list_join[..s.list_join_len as usize]
                } else {
                    b","
                },
            };
            let ll = list_children(
                sys,
                &s.list_children[..s.list_children_len as usize],
                key_tail,
                &view,
                &mut lc,
            );
            let Some(q) = put_field(out, p, 56, TY_BYTES, &lc[..ll]) else {
                return 0;
            };
            p = q;
            n += 1;
        }
        if s.desired_len > 0 {
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(&s.desired[..s.desired_len as usize], &mut segs);
            let want = j_path(value, &segs[..ns]).map(parse_dec_bytes).unwrap_or(0) as i64;
            let Some(q) = put_field(out, p, 20, TY_I64, &want.to_le_bytes()) else {
                return 0;
            };
            p = q;
            n += 1;
        }
        if s.count_children_len > 0 {
            let cc = child_count(
                sys,
                &s.count_children[..s.count_children_len as usize],
                key_tail,
                s.child_sep,
            );
            let mut dec = [0u8; 12];
            let dl = write_dec(&mut dec, cc as u64);
            let Some(q) = put_field(out, p, 21, TY_BYTES, &dec[..dl]) else {
                return 0;
            };
            p = q;
            n += 1;
            let Some(q) = put_field(out, p, 22, TY_I64, &cc.to_le_bytes()) else {
                return 0;
            };
            p = q;
            n += 1;
        }
    } else if let Some((i, total)) = list_pos {
        // Count mode, at the SAME field numbers the expansion uses for the same
        // two ideas — this index, that count — so a decision reads either
        // shape identically. Never both: an expanded record's index is its
        // ordinal, not its place in the listing.
        let Some(q) = put_field(out, p, 20, TY_I64, &i.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
        let Some(q) = put_field(out, p, 22, TY_I64, &total.to_le_bytes()) else {
            return 0;
        };
        p = q;
        n += 1;
    }
    out[0] = n;
    p
}

/// Decimal render of `v` into `dst`; returns the length.
/// Append one REPEATED field per `each` spec item (see the `each` param):
/// `n=<path>[]:<m>,…` walks a JSON array, `n=<path>{}` a JSON object, and
/// every element becomes one nested record frame under field `n`. Returns the
/// advanced cursor and the number of fields written; `None` when the record
/// cannot hold them — the record is then not emitted, never emitted short.
///
/// An item ending `@<path>` (with `look`, the joined entry) also carries, as
/// the element's next field, `look`'s value at `<path>.<key>` — the key being
/// the element's `key` member, or an object member's name — and nothing when
/// `look` has none. `cap` (0 = none) bounds the elements per item; the third
/// result says whether any list was cut.
#[allow(
    clippy::too_many_arguments,
    reason = "one walk's inputs: the record cursor, the document, the spec, the expansion, and the lookup side"
)]
fn each_fields(
    out: &mut [u8],
    mut p: usize,
    value: &[u8],
    spec: &[u8],
    idx: Option<i64>,
    dims: &[i64; 4],
    look: &[u8],
    cap: usize,
) -> Option<(usize, u8, bool)> {
    let mut n = 0u8;
    let mut over = false;
    for item in spec.split(|&b| b == b';') {
        if item.is_empty() {
            continue;
        }
        let (item, lpath) = match item.iter().position(|&b| b == b'@') {
            Some(at) => (&item[..at], Some(&item[at + 1..])),
            None => (item, None),
        };
        let mut emitted = 0usize;
        let eq = item.iter().position(|&b| b == b'=')?;
        let num = parse_dec_u64(&item[..eq]) as u8;
        let rest = &item[eq + 1..];
        let (path, members, object) = if let Some(k) = rest.windows(2).position(|w| w == b"[]") {
            let m = rest.get(k + 2..).unwrap_or(&[]);
            (&rest[..k], m.strip_prefix(b":").unwrap_or(m), false)
        } else if let Some(k) = rest.windows(2).position(|w| w == b"{}") {
            (&rest[..k], &[][..], true)
        } else {
            return None;
        };
        let mut segs: [&[u8]; 6] = [&[]; 6];
        let ns = split_dots(path, &mut segs);
        // `#`, `#0`…: this record's expansion index, as in any path — the
        // selectors of the k-th element of an expanded list.
        let mut ibufs = [[0u8; 20]; 5];
        subst_index(&mut segs, ns, idx, dims, &mut ibufs);
        let Some(cont) = j_sub(value, &segs[..ns]) else {
            continue; // absent: no elements
        };
        let mut i = j_ws(cont, 1);
        loop {
            i = j_ws(cont, i);
            if i >= cont.len() || cont[i] == b']' || cont[i] == b'}' {
                break;
            }
            if cap > 0 && emitted >= cap {
                over = true;
                break;
            }
            emitted += 1;
            let mut frame = [0u8; 1024];
            // The element's key, for an `@` lookup into `look`.
            let mut lkey: &[u8] = &[];
            let mut fp = 1usize;
            let mut fc = 0u8;
            if object {
                // "key": value
                if cont[i] != b'"' {
                    return None;
                }
                let key = j_scalar(cont, i);
                let mut j = j_ws(cont, j_skip(cont, i));
                if cont.get(j) != Some(&b':') {
                    return None;
                }
                j = j_ws(cont, j + 1);
                let vend = j_skip(cont, j);
                let val = if matches!(cont.get(j), Some(b'{') | Some(b'[')) {
                    &cont[j..vend]
                } else {
                    j_scalar(cont, j)
                };
                fp = put_field(&mut frame, fp, 1, TY_BYTES, key)?;
                fp = put_field(&mut frame, fp, 2, TY_BYTES, val)?;
                fc = 2;
                lkey = key;
                i = vend;
            } else {
                let end = j_skip(cont, i);
                let elem = &cont[i..end];
                for (k, m) in members.split(|&b| b == b',').enumerate() {
                    if m.is_empty() {
                        continue;
                    }
                    let fno = (k + 1) as u8;
                    if let Some(base) = m.strip_suffix(b"[]") {
                        // a scalar array, comma-joined
                        let mut bs: [&[u8]; 6] = [&[]; 6];
                        let nb = split_dots(base, &mut bs);
                        let Some(arr) = j_sub(elem, &bs[..nb]) else {
                            continue;
                        };
                        let mut joined = [0u8; 512];
                        let mut jl = 0usize;
                        let mut a = j_ws(arr, 1);
                        loop {
                            a = j_ws(arr, a);
                            if a >= arr.len() || arr[a] == b']' {
                                break;
                            }
                            let v = j_scalar(arr, a);
                            if jl > 0 {
                                *joined.get_mut(jl)? = b',';
                                jl += 1;
                            }
                            joined.get_mut(jl..jl + v.len())?.copy_from_slice(v);
                            jl += v.len();
                            a = j_ws(arr, j_skip(arr, a));
                            if arr.get(a) == Some(&b',') {
                                a += 1;
                            }
                        }
                        fp = put_field(&mut frame, fp, fno, TY_BYTES, &joined[..jl])?;
                        fc += 1;
                    } else {
                        let mut ms: [&[u8]; 6] = [&[]; 6];
                        let nm = split_dots(m, &mut ms);
                        // absent member: left out, so `has()` sees it absent
                        if let Some(v) = j_path(elem, &ms[..nm]) {
                            fp = put_field(&mut frame, fp, fno, TY_BYTES, v)?;
                            fc += 1;
                            if k == 0 {
                                lkey = v;
                            }
                        }
                    }
                }
                i = end;
            }
            // The paired value: the joined entry's `<lpath>.<key>`, at the
            // element's next field number; left out when there is none.
            if let Some(lp) = lpath {
                let lf = if object {
                    3u8
                } else {
                    members.split(|&b| b == b',').count() as u8 + 1
                };
                let mut ls: [&[u8]; 6] = [&[]; 6];
                let nl = split_dots(lp, &mut ls);
                if !lkey.is_empty() && nl < ls.len() {
                    if let Some(map) = j_sub(look, &ls[..nl]) {
                        if let Some(at) = j_obj_get(map, 0, lkey) {
                            let v = j_scalar(map, at);
                            fp = put_field(&mut frame, fp, lf, TY_BYTES, v)?;
                            fc += 1;
                        }
                    }
                }
            }
            frame[0] = fc;
            p = put_field(out, p, num, TY_MSG, &frame[..fp])?;
            n = n.checked_add(1)?;
            i = j_ws(cont, i);
            if cont.get(i) == Some(&b',') {
                i += 1;
            }
        }
    }
    Some((p, n, over))
}

/// The joined object's own projections — `join_paths` at 60..64, the
/// `join_match` verdict at 65, and `join_each`'s repeated fields — from the
/// joined entry at `jkey` (its full store key). Shared by the scoped and the
/// unscoped join: a joined object is read the same way whichever listing found
/// it. `None` when the record cannot hold them.
unsafe fn joined_fields(
    s: &State,
    out: &mut [u8],
    mut p: usize,
    mut n: u8,
    value: &[u8],
    jkey: &[u8],
) -> Option<(usize, u8)> {
    // The joined object itself: its fields at 60..64 and, at 65, the
    // selector verdict — "1" when the joined object's `<a>` is a
    // superset of this object's `<b>` for `join_match = "a:b"`.
    if s.join_paths_len > 0 || s.join_match_len > 0 || s.join_each_len > 0 || s.join_terms_len > 0 {
        let sys = &*s.syscalls;
        let mut jv = [0u8; MAX_VALUE];
        let jl = get_value(sys, jkey, &mut jv).unwrap_or(0);
        let jval = &jv[..jl];
        let spec = &s.join_paths[..s.join_paths_len as usize];
        let mut start = 0usize;
        let mut fno = 60u8;
        while start <= spec.len() && fno < 65 {
            let end = spec[start..]
                .iter()
                .position(|&b| b == b',')
                .map(|i| start + i)
                .unwrap_or(spec.len());
            let one = &spec[start..end];
            if !one.is_empty() {
                // `.` is the joined entry's WHOLE VALUE — same reason
                // as `paths`: a joined record whose value is not JSON
                // (an endpoint address, a key frame) has no path to
                // read, and the walk already holds the bytes.
                let got = if one == b"." {
                    jval
                } else {
                    let mut segs: [&[u8]; 6] = [&[]; 6];
                    let ns = split_dots(one, &mut segs);
                    j_sub(jval, &segs[..ns])
                        .or_else(|| j_path(jval, &segs[..ns]))
                        .unwrap_or(&[])
                };
                let q = put_field(out, p, fno, TY_BYTES, got)?;
                p = q;
                n += 1;
                fno += 1;
            }
            if end >= spec.len() {
                break;
            }
            start = end + 1;
        }
        if s.join_match_len > 0 {
            let jm = &s.join_match[..s.join_match_len as usize];
            let colon = jm.iter().position(|&b| b == b':').unwrap_or(jm.len());
            let mut sa: [&[u8]; 6] = [&[]; 6];
            let na = split_dots(&jm[..colon], &mut sa);
            let mut sb: [&[u8]; 6] = [&[]; 6];
            let nb = if colon < jm.len() {
                split_dots(&jm[colon + 1..], &mut sb)
            } else {
                0
            };
            let sup = j_sub(jval, &sa[..na]).unwrap_or(b"{}");
            let sub = if nb > 0 {
                j_sub(value, &sb[..nb]).unwrap_or(b"{}")
            } else {
                b"{}"
            };
            let hit: &[u8] = if j_obj_subset(sub, sup) { b"1" } else { b"" };
            let q = put_field(out, p, 65, TY_BYTES, hit)?;
            p = q;
            n += 1;
        }
        if s.join_each_len > 0 {
            let spec = core::slice::from_raw_parts(s.join_each.as_ptr(), s.join_each_len as usize);
            // `@` items look up in the OBJECT: the joined entry holds the terms
            // (a watch registration's selector), the object the labels.
            let (q, m, _) = each_fields(out, p, jval, spec, None, &[0; 4], value, 0)?;
            p = q;
            n += m;
        }
        if s.join_terms_len > 0 {
            let spec =
                core::slice::from_raw_parts(s.join_terms.as_ptr(), s.join_terms_len as usize);
            let (q, m, over) = each_fields(
                out,
                p,
                value,
                spec,
                None,
                &[0; 4],
                jval,
                s.terms_max as usize,
            )?;
            p = q;
            n += m;
            p = put_field(out, p, 67, TY_I64, &(over as i64).to_le_bytes())?;
            n += 1;
        }
    }
    Some((p, n))
}

fn write_dec(dst: &mut [u8], mut v: u64) -> usize {
    if v == 0 {
        dst[0] = b'0';
        return 1;
    }
    let mut tmp = [0u8; 20];
    let mut n = 0;
    while v > 0 && n < tmp.len() {
        tmp[n] = b'0' + (v % 10) as u8;
        v /= 10;
        n += 1;
    }
    for i in 0..n {
        dst[i] = tmp[n - 1 - i];
    }
    n
}

/// The highest numeric ordinal among the children `<expand_over><tail>-<n>`,
/// or -1 when there are none. A child whose suffix is not a number is not an
/// ordinal and is ignored.
unsafe fn highest_child(s: &State, sys: &SyscallTable, tail: &[u8]) -> i64 {
    let ol = s.expand_over_len as usize;
    let mut pfx = [0u8; MAX_PREFIX + MAX_KEY + 2];
    let mut pl = append_bytes(&mut pfx, 0, &s.expand_over[..ol]);
    pl = append_bytes(&mut pfx, pl, tail);
    pl = append_bytes(&mut pfx, pl, &[s.child_sep]);
    if pl > MAX_KEY {
        return -1;
    }
    let mut cur = [0u8; MAX_CURSOR];
    let mut clen = 0usize;
    let mut top = -1i64;
    loop {
        let mut page = [0u8; LIST_BUF];
        let mut next = [0u8; MAX_CURSOR];
        let (pn, nlen) = list_page(
            sys,
            &pfx[..pl],
            &cur[..clen],
            LIST_BUF,
            &mut page,
            &mut next,
        );
        let mut rp = 0usize;
        while let Some((name, q)) = raw_entry(&page[..pn], rp) {
            rp = q;
            if name.len() <= pl {
                continue;
            }
            let suffix = &name[pl..];
            if suffix.is_empty() || !suffix.iter().all(|b| b.is_ascii_digit()) {
                continue;
            }
            let v = parse_dec_bytes(suffix) as i64;
            if v > top {
                top = v;
            }
        }
        if nlen == 0 {
            return top;
        }
        cur[..nlen].copy_from_slice(&next[..nlen]);
        clen = nlen;
    }
}

/// How many children `<prefix><tail><sep>*` exist, summed over every
/// comma-separated prefix in `prefixes`.
unsafe fn child_count(sys: &SyscallTable, prefixes: &[u8], tail: &[u8], sep: u8) -> i64 {
    let mut total = 0i64;
    let mut start = 0usize;
    while start <= prefixes.len() {
        let end = prefixes[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(prefixes.len());
        if end > start {
            total += child_count_one(sys, &prefixes[start..end], tail, sep);
        }
        if end >= prefixes.len() {
            break;
        }
        start = end + 1;
    }
    total
}

unsafe fn child_count_one(sys: &SyscallTable, prefix: &[u8], tail: &[u8], sep: u8) -> i64 {
    let mut pfx = [0u8; MAX_PREFIX + MAX_KEY + 2];
    let mut pl = append_bytes(&mut pfx, 0, prefix);
    pl = append_bytes(&mut pfx, pl, tail);
    pl = append_bytes(&mut pfx, pl, &[sep]);
    if pl > MAX_KEY {
        return 0;
    }
    let mut cur = [0u8; MAX_CURSOR];
    let mut clen = 0usize;
    let mut total = 0i64;
    loop {
        let mut page = [0u8; LIST_BUF];
        let mut next = [0u8; MAX_CURSOR];
        let (pn, nlen) = list_page(
            sys,
            &pfx[..pl],
            &cur[..clen],
            LIST_BUF,
            &mut page,
            &mut next,
        );
        let mut rp = 0usize;
        while let Some((_, q)) = raw_entry(&page[..pn], rp) {
            rp = q;
            total += 1;
        }
        if nlen == 0 {
            return total;
        }
        cur[..nlen].copy_from_slice(&next[..nlen]);
        clen = nlen;
    }
}

/// Walk every join listing in order and return the `n`-th key — its tail
/// below the join prefix, or the FULL key when the join is scoped — or None
/// past the end. Whole walks per record, deliberately: the joined set is
/// small by nature, and a resumable inner cursor would buy little for a lot
/// of state.
unsafe fn join_nth(
    s: &State,
    sys: &SyscallTable,
    tail: &[u8],
    n: i64,
    out: &mut [u8],
) -> Option<usize> {
    let spec = &s.join[..s.join_len as usize];
    let mut seen = 0i64;
    let mut start = 0usize;
    while start <= spec.len() {
        let end = spec[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(spec.len());
        if end > start {
            let one = &spec[start..end];
            let mut jp = [0u8; MAX_PREFIX + MAX_KEY + 2];
            let mut jl = append_bytes(&mut jp, 0, one);
            if s.join_scoped != 0 {
                jl = append_bytes(&mut jp, jl, join_scope(s, tail));
                jl = append_bytes(&mut jp, jl, b"/");
            }
            if jl <= MAX_KEY {
                let mut cur = [0u8; MAX_CURSOR];
                let mut clen = 0usize;
                loop {
                    let mut page = [0u8; LIST_BUF];
                    let mut next = [0u8; MAX_CURSOR];
                    let (pn, nlen) =
                        list_page(sys, &jp[..jl], &cur[..clen], LIST_BUF, &mut page, &mut next);
                    let mut rp = 0usize;
                    while let Some((name, q)) = raw_entry(&page[..pn], rp) {
                        rp = q;
                        if seen == n {
                            let got = if s.join_scoped != 0 {
                                name
                            } else if name.len() > one.len() {
                                &name[one.len()..]
                            } else {
                                &name[..0]
                            };
                            let l = got.len().min(out.len());
                            out[..l].copy_from_slice(&got[..l]);
                            return Some(l);
                        }
                        seen += 1;
                    }
                    if nlen == 0 {
                        break;
                    }
                    cur[..nlen].copy_from_slice(&next[..nlen]);
                    clen = nlen;
                }
            }
        }
        if end >= spec.len() {
            break;
        }
        start = end + 1;
    }
    None
}

/// The `where` filter: `<path>=<value>` (exact) or `<path>=` (absent/empty).
/// `unless`: true when the object's key tail has NO record under the
/// `unless` prefix — one existence probe, the same one a decision would need
/// a whole store round trip for.
unsafe fn unless_ok(s: &State, sys: &SyscallTable, tail: &[u8]) -> bool {
    if s.unless_len == 0 {
        return true;
    }
    let ul = s.unless_len as usize;
    let mut k = [0u8; MAX_KEY + MAX_PREFIX];
    let a = append_bytes(&mut k, 0, &s.unless[..ul]);
    let kl = append_bytes(&mut k, a, tail);
    if kl > MAX_KEY {
        return true;
    }
    let mut probe = [0u8; MAX_VALUE];
    get_value(sys, &k[..kl], &mut probe).is_none()
}

fn where_ok(s: &State, value: &[u8]) -> bool {
    if s.where_len == 0 {
        return true;
    }
    let w = &s.where_[..s.where_len as usize];
    let eq = match w.iter().position(|&b| b == b'=') {
        Some(i) => i,
        None => return true,
    };
    let mut segs: [&[u8]; 6] = [&[]; 6];
    let ns = split_dots(&w[..eq], &mut segs);
    let got = if s.flat != 0 {
        flat_field(value, &w[..eq]).unwrap_or(&[])
    } else {
        j_path(value, &segs[..ns]).unwrap_or(&[])
    };
    got == &w[eq + 1..]
}

/// What a scoped join is scoped TO: the object's whole tail (`join_scoped =
/// 1`) or its namespace segment alone (`join_scoped = 2` — "the pods of this
/// service's namespace").
fn join_scope<'a>(s: &State, tail: &'a [u8]) -> &'a [u8] {
    match s.join_scoped {
        // 2..=9: the tail's first (k - 1) segments — 2 the namespace, 3 the
        // namespace and the next one (`<ns>/<name>`), and so on.
        k @ 2..=9 => {
            let want = (k - 1) as usize;
            let mut seen = 0usize;
            for (i, &b) in tail.iter().enumerate() {
                if b == b'/' {
                    seen += 1;
                    if seen == want {
                        return &tail[..i];
                    }
                }
            }
            tail
        }
        // 10: the tail's LAST segment — a row keyed `…/<ns>` joined with that
        // namespace's objects.
        10 => match tail.iter().rposition(|&b| b == b'/') {
            Some(i) => &tail[i + 1..],
            None => tail,
        },
        _ => tail,
    }
}

/// How a children view renders: the separator below the object's key, whether
/// entries carry their names, the literal each entry opens with, and the one
/// placed between entries.
///
/// One parameter rather than four, because the four always travel together —
/// they are read from this module's params once and describe a single view.
#[derive(Clone, Copy)]
struct ChildView<'a> {
    sep: u8,
    values_only: bool,
    item_prefix: &'a [u8],
    join: &'a [u8],
}

/// The object's children under `<prefix><tail><sep>`, as `<name>=<value>,…`
/// in key order. Bounded by `out`; a set that does not fit is cut at a whole
/// entry and reported, never corrupted.
unsafe fn list_children(
    sys: &SyscallTable,
    prefix: &[u8],
    tail: &[u8],
    view: &ChildView,
    out: &mut [u8],
) -> usize {
    let ChildView {
        sep,
        values_only,
        item_prefix,
        join,
    } = *view;
    let mut pfx = [0u8; MAX_PREFIX + MAX_KEY + 2];
    let mut pl = append_bytes(&mut pfx, 0, prefix);
    pl = append_bytes(&mut pfx, pl, tail);
    pl = append_bytes(&mut pfx, pl, &[sep]);
    if pl > MAX_KEY {
        return 0;
    }
    let mut cur = [0u8; MAX_CURSOR];
    let mut clen = 0usize;
    let mut o = 0usize;
    loop {
        let mut page = [0u8; LIST_BUF];
        let mut next = [0u8; MAX_CURSOR];
        let (pn, nlen) = list_page(
            sys,
            &pfx[..pl],
            &cur[..clen],
            LIST_BUF,
            &mut page,
            &mut next,
        );
        let mut rp = 0usize;
        while let Some((name, q)) = raw_entry(&page[..pn], rp) {
            rp = q;
            if name.len() <= pl {
                continue;
            }
            let child = &name[pl..];
            let mut v = [0u8; 256];
            let Some(vl) = get_value(sys, name, &mut v) else {
                continue;
            };
            let need = if values_only {
                vl
            } else {
                child.len() + 1 + vl
            } + usize::from(o > 0);
            if o + need > out.len() {
                let m = b"[store_source] list_children does not fit - view CUT at a whole entry";
                dev_log(sys, 1, m.as_ptr(), m.len());
                return o;
            }
            if o > 0 {
                o = append_bytes(out, o, join);
            }
            o = append_bytes(out, o, item_prefix);
            // `<name>=<value>` is what a projection keyed BY child wants — the
            // endpoints document names its pods. A consumer that wants the
            // values as a list (an edge's backend set) wants them without the
            // names, and cannot strip them afterwards: the VM has no iteration.
            if !values_only {
                o = append_bytes(out, o, child);
                out[o] = b'=';
                o += 1;
            }
            o = append_bytes(out, o, &v[..vl]);
        }
        if nlen == 0 {
            return o;
        }
        cur[..nlen].copy_from_slice(&next[..nlen]);
        clen = nlen;
    }
}

/// `join_pick`: reduce the join to ONE entry.
///   `min:<path>`               least integer at <path> in the joined object
///   `mincount:<prefix>:<path>` least number of objects under <prefix> whose
///                              <path> equals the joined entry's name — a
///                              load measured from the facts themselves,
///                              not from a projection that lags them
/// Ties go to the lowest key. None when the join is empty.
unsafe fn join_argmin(s: &State, sys: &SyscallTable, tail: &[u8], out: &mut [u8]) -> Option<usize> {
    let jp = &s.join_pick[..s.join_pick_len as usize];
    let c1 = jp
        .iter()
        .position(|&b| b == b':')
        .map(|i| i + 1)
        .unwrap_or(jp.len());
    let mode = &jp[..c1.saturating_sub(1)];
    let rest = &jp[c1..];
    let total = join_count(s, sys, tail);
    let mut best: Option<i64> = None;
    let mut bestkey = [0u8; MAX_KEY];
    let mut bestlen = 0usize;
    let mut i = 0i64;
    while i < total {
        let mut k = [0u8; MAX_KEY];
        let Some(kl) = join_nth(s, sys, tail, i, &mut k) else {
            break;
        };
        let measure = if mode == b"mincount" {
            // rest = "<prefix>:<path>"; the name is the entry's last segment.
            let c2 = rest.iter().position(|&b| b == b':').unwrap_or(rest.len());
            let prefix = &rest[..c2];
            let path = if c2 < rest.len() {
                &rest[c2 + 1..]
            } else {
                &rest[..0]
            };
            let name = match k[..kl].iter().rposition(|&b| b == b'/') {
                Some(x) => &k[x + 1..kl],
                None => &k[..kl],
            };
            count_where(sys, prefix, path, name)
        } else {
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(rest, &mut segs);
            let mut full = [0u8; MAX_KEY + MAX_PREFIX];
            let fl = if s.join_scoped != 0 {
                append_bytes(&mut full, 0, &k[..kl])
            } else {
                let one = first_join_prefix(s);
                let a = append_bytes(&mut full, 0, one);
                append_bytes(&mut full, a, &k[..kl])
            };
            let mut v = [0u8; MAX_VALUE];
            match get_value(sys, &full[..fl], &mut v) {
                Some(vl) => j_path(&v[..vl], &segs[..ns])
                    .map(parse_dec_bytes)
                    .unwrap_or(0) as i64,
                None => i64::MAX,
            }
        };
        if best.map(|b| measure < b).unwrap_or(true) {
            best = Some(measure);
            bestkey[..kl].copy_from_slice(&k[..kl]);
            bestlen = kl;
        }
        i += 1;
    }
    best?;
    let l = bestlen.min(out.len());
    out[..l].copy_from_slice(&bestkey[..l]);
    Some(l)
}

/// How many objects under `prefix` have `path` equal to `want`.
unsafe fn count_where(sys: &SyscallTable, prefix: &[u8], path: &[u8], want: &[u8]) -> i64 {
    let mut segs: [&[u8]; 6] = [&[]; 6];
    let ns = split_dots(path, &mut segs);
    let mut cur = [0u8; MAX_CURSOR];
    let mut clen = 0usize;
    let mut total = 0i64;
    loop {
        let mut page = [0u8; LIST_BUF];
        let mut next = [0u8; MAX_CURSOR];
        let (pn, nlen) = list_page(sys, prefix, &cur[..clen], LIST_BUF, &mut page, &mut next);
        let mut rp = 0usize;
        while let Some((name, q)) = raw_entry(&page[..pn], rp) {
            rp = q;
            let mut v = [0u8; MAX_VALUE];
            if let Some(vl) = get_value(sys, name, &mut v) {
                if j_path(&v[..vl], &segs[..ns]) == Some(want) {
                    total += 1;
                }
            }
        }
        if nlen == 0 {
            return total;
        }
        cur[..nlen].copy_from_slice(&next[..nlen]);
        clen = nlen;
    }
}

/// The first prefix of a comma-separated `join` list.
fn first_join_prefix(s: &State) -> &[u8] {
    let spec = &s.join[..s.join_len as usize];
    match spec.iter().position(|&b| b == b',') {
        Some(i) => &spec[..i],
        None => spec,
    }
}

/// How many entries the join listings hold, in total.
unsafe fn join_count(s: &State, sys: &SyscallTable, tail: &[u8]) -> i64 {
    let spec = &s.join[..s.join_len as usize];
    let mut total = 0i64;
    let mut start = 0usize;
    while start <= spec.len() {
        let end = spec[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(spec.len());
        if end > start {
            let one = &spec[start..end];
            let mut jp = [0u8; MAX_PREFIX + MAX_KEY + 2];
            let mut jl = append_bytes(&mut jp, 0, one);
            if s.join_scoped != 0 {
                jl = append_bytes(&mut jp, jl, join_scope(s, tail));
                jl = append_bytes(&mut jp, jl, b"/");
            }
            if jl <= MAX_KEY {
                let mut cur = [0u8; MAX_CURSOR];
                let mut clen = 0usize;
                loop {
                    let mut page = [0u8; LIST_BUF];
                    let mut next = [0u8; MAX_CURSOR];
                    let (pn, nlen) =
                        list_page(sys, &jp[..jl], &cur[..clen], LIST_BUF, &mut page, &mut next);
                    let mut rp = 0usize;
                    while let Some((_, q)) = raw_entry(&page[..pn], rp) {
                        rp = q;
                        total += 1;
                    }
                    if nlen == 0 {
                        break;
                    }
                    cur[..nlen].copy_from_slice(&next[..nlen]);
                    clen = nlen;
                }
            }
        }
        if end >= spec.len() {
            break;
        }
        start = end + 1;
    }
    total
}

/// Count ONE page of the current scan — the first pass of count mode. Nothing
/// is emitted; only `total` advances. Returns true when the listing is done.
unsafe fn count_page(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let plen = s.prefix_len as usize;
    let mut pfx = [0u8; MAX_PREFIX];
    pfx[..plen].copy_from_slice(&s.prefix[..plen]);
    let mut cur = [0u8; MAX_CURSOR];
    let clen = s.clen as usize;
    cur[..clen].copy_from_slice(&s.cursor[..clen]);
    let mut lout = [0u8; LIST_BUF];
    let mut next = [0u8; MAX_CURSOR];
    let (n, nlen) = list_page(
        sys,
        &pfx[..plen],
        &cur[..clen],
        s.page_bytes as usize,
        &mut lout,
        &mut next,
    );
    let mut rp = 0usize;
    while let Some((name, q)) = raw_entry(&lout[..n], rp) {
        rp = q;
        if s.unless_len > 0 {
            let pl = s.prefix_len as usize;
            let tail = if name.len() > pl {
                &name[pl..]
            } else {
                &name[..0]
            };
            if !unless_ok(s, sys, tail) {
                continue;
            }
        }
        if s.where_len > 0 {
            // The filter applies to the count too, or the index is a position
            // in a set the emit walk does not emit.
            let mut v = [0u8; MAX_VALUE];
            let Some(vl) = get_value(sys, name, &mut v) else {
                continue;
            };
            if !where_ok(s, &v[..vl]) {
                continue;
            }
        }
        s.total += 1;
    }
    if nlen == 0 {
        s.clen = 0;
        return true;
    }
    s.cursor[..nlen].copy_from_slice(&next[..nlen]);
    s.clen = nlen as u16;
    false
}

/// Emit ONE page of the current scan. Returns frames written.
///
/// Level-triggered and resumable: a change re-arms `dirty`, which starts a scan;
/// each step advances it by one page until the provider reports the listing
/// complete. Re-emitting a page is harmless — consumers are idempotent — so
/// backpressure simply repeats the page rather than losing it.
enum Sent {
    /// The whole record went out.
    Whole,
    /// Part of it went out; the rest is staged (`pend_*`) and owed first.
    Staged,
    /// Nothing went out: downstream is full.
    Blocked,
}

/// Write `frame[..flen]` downstream.
unsafe fn send(s: &mut State, flen: usize) -> Sent {
    let sys = &*s.syscalls;
    let w = (sys.channel_write)(s.record_out, s.frame.as_ptr(), flen);
    if w <= 0 {
        Sent::Blocked
    } else if w as usize >= flen {
        Sent::Whole
    } else {
        s.pend_at = w as u16;
        s.pend_len = flen as u16;
        Sent::Staged
    }
}

/// Deliver what a partial write still owes. True when nothing is owed.
unsafe fn flush(s: &mut State) -> bool {
    if s.pend_len == 0 {
        return true;
    }
    let sys = &*s.syscalls;
    let (at, len) = (s.pend_at as usize, s.pend_len as usize);
    let w = (sys.channel_write)(s.record_out, s.frame.as_ptr().add(at), len - at);
    if w > 0 {
        s.pend_at += w as u16;
    }
    if s.pend_at as usize >= len {
        s.pend_at = 0;
        s.pend_len = 0;
        true
    } else {
        false
    }
}

unsafe fn emit_page(s: &mut State) -> u32 {
    let sys = &*s.syscalls;
    let plen = s.prefix_len as usize;
    let mut pfx = [0u8; MAX_PREFIX];
    pfx[..plen].copy_from_slice(&s.prefix[..plen]);

    let mut cur = [0u8; MAX_CURSOR];
    let clen = s.clen as usize;
    cur[..clen].copy_from_slice(&s.cursor[..clen]);

    let mut lout = [0u8; LIST_BUF];
    let mut next = [0u8; MAX_CURSOR];
    let (n, nlen) = list_page(
        sys,
        &pfx[..plen],
        &cur[..clen],
        s.page_bytes as usize,
        &mut lout,
        &mut next,
    );

    let mut wrote = 0u32;
    let mut rp = 0usize;
    while let Some((name, q)) = raw_entry(&lout[..n], rp) {
        rp = q;
        let klen = name.len();
        if klen > MAX_KEY {
            continue;
        }
        // Skip what a previous step already delivered from this page. LIST
        // pages by cursor, so its keys ascend: everything below the resume key
        // was delivered. The resume key itself keeps its expansion offset; if
        // it is gone, its successor starts its own expansion at 0.
        if s.page_off != 0 {
            let r = &s.resume_key[..s.resume_len as usize];
            if name < r {
                continue;
            }
            if name != r {
                s.exp_off = 0;
            }
            s.page_off = 0;
        }
        // LIST yields whole keys — the reconcilers pass them straight to GET.
        let mut key = [0u8; MAX_KEY];
        key[..klen].copy_from_slice(name);

        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &key[..klen], &mut val) else {
            continue;
        };
        if !where_ok(s, &val[..vlen]) {
            continue;
        }
        if s.unless_len > 0 {
            let tl = if klen > plen {
                &key[plen..klen]
            } else {
                &key[..0]
            };
            if !unless_ok(s, sys, tl) {
                continue;
            }
        }
        s.scan_hits = s.scan_hits.wrapping_add(1);
        if s.due_len > 0 {
            // A comma list: an object may be waiting on several clocks (a kill
            // grace AND a restart backoff), and the timer must be armed for
            // whichever comes first. A zero or absent field is not a deadline.
            let spec = &s.due[..s.due_len as usize];
            let mut ds = 0usize;
            while ds <= spec.len() {
                let de = spec[ds..]
                    .iter()
                    .position(|&b| b == b',')
                    .map(|i| ds + i)
                    .unwrap_or(spec.len());
                let one = &spec[ds..de];
                // `<field>+<ms>`: a deadline that far past a recorded time.
                let (one, after) = match one.iter().position(|&b| b == b'+') {
                    Some(i) => (&one[..i], parse_dec_u64(&one[i + 1..]) as i64),
                    None => (one, 0),
                };
                if !one.is_empty() {
                    let raw = if s.flat != 0 {
                        flat_field(&val[..vlen], one).unwrap_or(&[])
                    } else {
                        let mut segs: [&[u8]; 6] = [&[]; 6];
                        let ns = split_dots(one, &mut segs);
                        j_path(&val[..vlen], &segs[..ns]).unwrap_or(&[])
                    };
                    let t = parse_dec_u64(raw) as i64;
                    if t != 0 {
                        let d = t.saturating_add(after);
                        if d > s.scan_now && d < s.next_due {
                            s.next_due = d;
                        }
                    }
                }
                if de >= spec.len() {
                    break;
                }
                ds = de + 1;
            }
        }
        // Field 3 is the key TAIL below the prefix: a decision keyed on
        // `default/web` should not have to re-strip a prefix it configured.
        let tail = if klen > plen {
            &key[plen..klen]
        } else {
            &key[..0]
        };

        // EXPANSION: one record per index rather than one per object. The count
        // comes from the object itself, so the cardinality is the data's — and
        // each index is an INDEPENDENT decision, which is what makes a
        // set-shaped reconcile expressible without a set-shaped connector op.
        // `a[]` counts an ARRAY instead of reading a number: one record per
        // element, and `#` in a path is that element's index. `a[]*b[]` is the
        // cross product of two arrays (`#0`, `#1`). `a[]+` is one array plus
        // ONE record past its last element (index = count), for what belongs
        // to the object as a whole rather than to any element — a policy's
        // isolation after its rules. (Not `expand_tail`: that defaults to 4,
        // for probing past a numeric count, and every array expansion would
        // grow four records.)
        s.exp_dims = [0; 4];
        let mut specbuf = [0u8; 48];
        let spec_len = (s.expand_len as usize).min(specbuf.len());
        specbuf[..spec_len].copy_from_slice(&s.expand[..spec_len]);
        let span = if s.expand_len > 0 {
            let mut spec = &specbuf[..spec_len];
            let plus = if spec.ends_with(b"[]+") {
                spec = &spec[..spec.len() - 1];
                1i64
            } else {
                0
            };
            let (c, tail_n) = if spec.ends_with(b"[]") {
                // `a[]*b[]*…`: up to four lists, crossed.
                let mut parts: [&[u8]; 4] = [&[]; 4];
                let mut np = 0usize;
                for part in spec.split(|&x| x == b'*') {
                    if np < 4 {
                        parts[np] = part;
                        np += 1;
                    }
                }
                let arr_len = |p: &[u8]| -> i64 {
                    let p = if p.ends_with(b"[]") {
                        &p[..p.len() - 2]
                    } else {
                        p
                    };
                    if s.flat != 0 {
                        // A flat record's list is `<field>=a,b,c`.
                        return match flat_field(&val[..vlen], p) {
                            Some(l) if !l.is_empty() => {
                                l.iter().filter(|&&c| c == b',').count() as i64 + 1
                            }
                            _ => 0,
                        };
                    }
                    let mut segs: [&[u8]; 6] = [&[]; 6];
                    let ns = split_dots(p, &mut segs);
                    j_arr_len(&val[..vlen], &segs[..ns]) as i64
                };
                if np < 2 {
                    (arr_len(parts[0]), plus)
                } else {
                    let mut total = 1i64;
                    for (k, part) in parts.iter().take(np).enumerate() {
                        let l = arr_len(part);
                        s.exp_dims[k] = l;
                        total = total.saturating_mul(l);
                    }
                    (total, 0)
                }
            } else {
                let mut segs: [&[u8]; 6] = [&[]; 6];
                let ns = split_dots(spec, &mut segs);
                (
                    j_path(&val[..vlen], &segs[..ns])
                        .map(parse_dec_bytes)
                        .unwrap_or(0) as i64,
                    s.expand_tail as i64,
                )
            };
            let mut hi = c + tail_n;
            if s.expand_over_len > 0 {
                let top = highest_child(s, sys, tail);
                if top + 1 > hi {
                    hi = top + 1;
                }
            }
            Some((c, hi))
        } else {
            None
        };
        let joined = if s.join_len > 0 && span.is_none() {
            if s.join_pick_len > 0 {
                // A pick reduces the join to one record — or none, when the
                // joined set is empty: there is nothing to choose.
                Some(if join_count(s, sys, tail) > 0 { 1 } else { 0 })
            } else {
                Some(join_count(s, sys, tail))
            }
        } else {
            None
        };
        let (count, hi) = match (span, joined) {
            (Some((c, h)), _) => (c, h),
            (None, Some(j)) => (j, j),
            (None, None) => (0, 1),
        };

        let mut ord = s.exp_off;
        let mut blocked = false;
        while ord < hi {
            let expand_idx = span.map(|(c, _)| (ord, c));
            s.seq = s.seq.wrapping_add(1);
            let mut frame = [0u8; FRAME_BUF];
            // Count mode stamps the LISTING position at 20 — also on a joined
            // record, where it replaces the join ordinal: "the first unbound
            // pod" is a position among pods, and with a pick the join ordinal
            // is always 0 anyway.
            let list_pos = if s.count != 0 && span.is_none() {
                Some((s.list_idx, s.total))
            } else {
                None
            };
            let mut segbuf = [0u8; MAX_KEY];
            let join = match joined {
                Some(j) => match if s.join_pick_len > 0 {
                    join_argmin(s, sys, tail, &mut segbuf)
                } else {
                    join_nth(s, sys, tail, ord, &mut segbuf)
                } {
                    Some(l) => {
                        // Scoped: the whole joined key travels (field 24 below).
                        // Unscoped: the joined entry's LAST segment names it.
                        let t = &segbuf[..l];
                        if s.join_scoped != 0 {
                            Some((ord, j, t))
                        } else {
                            let last = match t.iter().rposition(|&b| b == b'/') {
                                Some(i) => &t[i + 1..],
                                None => t,
                            };
                            Some((ord, j, last))
                        }
                    }
                    None => {
                        ord += 1;
                        continue;
                    }
                },
                None => None,
            };
            let flen = project(
                s,
                tail,
                &val[..vlen],
                expand_idx,
                list_pos,
                join,
                &mut frame,
            );
            if flen == 0 {
                s.truncated = s.truncated.wrapping_add(1);
                ord += 1;
                continue;
            }
            s.frame[..flen].copy_from_slice(&frame[..flen]);
            match send(s, flen) {
                Sent::Whole => wrote += 1,
                Sent::Staged => {
                    // Delivered as far as this scan is concerned; the stream
                    // owes the rest, so stop here and resume past it.
                    wrote += 1;
                    ord += 1;
                    blocked = true;
                    break;
                }
                Sent::Blocked => {
                    blocked = true;
                    break;
                }
            }
            ord += 1;
        }
        let _ = count;
        if blocked {
            // Downstream is full. RETAIN the work: leave the cursor where it is
            // so the next step repeats this page. Dropping would be a leak, not
            // a delay — nothing re-reads an object that never changes again.
            // Accepted work is retained until delivered or explicitly
            // rejected.
            s.deferred = s.deferred.wrapping_add(1);
            s.page_off = 1; // retry THIS entry next step, not the page
            s.resume_key[..klen].copy_from_slice(&key[..klen]);
            s.resume_len = klen as u16;
            s.exp_off = ord; // ...and resume the expansion HERE, not at 0
            s.emitted = s.emitted.wrapping_add(wrote);
            return wrote;
        }
        // Entry expanded whole; the next one starts its own expansion at 0.
        s.exp_off = 0;
        s.list_idx += 1;
    }

    s.page_off = 0; // page delivered whole
    s.exp_off = 0;
    if nlen == 0 && s.empty != 0 && s.scan_hits == 0 {
        // Nothing qualified: say so, once. The same field set a counted
        // record carries (20 = 0, 22 = 0), with an empty tail and value, so a
        // decision reads "the set is empty" by the numbers it already reads.
        s.seq = s.seq.wrapping_add(1);
        let mut frame = [0u8; FRAME_BUF];
        let flen = project(s, &[], &[], None, Some((0, 0)), None, &mut frame);
        if flen > 0 {
            s.frame[..flen].copy_from_slice(&frame[..flen]);
            if let Sent::Blocked = send(s, flen) {
                // Downstream full: stay mid-scan, and the next step re-lists
                // the (empty) prefix and tries again.
                s.deferred = s.deferred.wrapping_add(1);
                s.clen = 0;
                return wrote;
            }
            wrote += 1;
        }
    }
    if nlen == 0 {
        s.paging = 0; // listing complete
        s.clen = 0;
        arm_due(s, sys);
    } else {
        s.cursor[..nlen].copy_from_slice(&next[..nlen]);
        s.clen = nlen as u16;
    }
    s.emitted = s.emitted.wrapping_add(wrote);
    wrote
}

// ---- module ABI ----

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

/// # Safety
/// Kernel module-ABI entry point: `state`/`syscalls` are the loader-owned
/// instance arena and syscall table; `params` is the config TLV blob (may be
/// null when `params_len == 0`).
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
    state: *mut u8,
    state_size: usize,
    syscalls: *const c_void,
) -> i32 {
    unsafe {
        if syscalls.is_null() || state.is_null() {
            return -1;
        }
        if state_size < core::mem::size_of::<State>() {
            return -2;
        }
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        // Port 0 pair: `changes` in (the store's push sink) and `record_out`
        // out. Handed over directly rather than resolved by index.
        s.sink = in_chan;
        s.record_out = out_chan;
        s.resolved = 0;
        s.subscribed = 0;
        s.dirty = 0;
        s.seq = 0;
        s.paging = 0;
        s.clen = 0;
        s.page_off = 0;
        s.pend_at = 0;
        s.pend_len = 0;
        s.exp_off = 0;
        s.count = 0;
        s.hash_len = 0;
        s.join_len = 0;
        s.join_scoped = 0;
        s.join_match_len = 0;
        s.join_paths_len = 0;
        s.each_len = 0;
        s.join_each_len = 0;
        s.join_terms_len = 0;
        s.table_len = 0;
        s.schema_len = 0;
        s.rv_len = 0;
        s.missing_len = 0;
        s.terms_max = 0;
        s.list_children_len = 0;
        s.where_len = 0;
        s.join_pick_len = 0;
        s.now = 0;
        s.due_len = 0;
        s.related_len = 0;
        s.timer_fd = -1;
        s.scan_now = 0;
        s.next_due = i64::MAX;
        s.child_sep = b'-';
        s.expand_over_len = 0;
        s.count_children_len = 0;
        s.desired_len = 0;
        s.ints_len = 0;
        s.counting = 0;
        s.total = 0;
        s.list_idx = 0;
        s.emitted = 0;
        s.deferred = 0;
        s.truncated = 0;
        s.events = 0;
        s.bookmark_ms = BOOKMARK_MS as u32;
        s.tomb_len = 0;
        s.watchers = [WATCHER_EMPTY; MAX_WATCHERS];
        s.ev_w = EV_IDLE;
        s.ev_len = 0;
        s.ev_at = 0;
        s.ev_fence = 0;
        s.lp_n = 0;
        s.lp_rp = 0;
        s.lp_end = 0;
        s.lp_cut = 0;
        s.wire = core::ptr::null_mut();
        s.wire_len = 0;
        s.wire_at = 0;
        s.wire_rev = 0;
        s.wire_cid = 0;
        s.evh = core::ptr::null_mut();
        s.evh_cap = 0;
        s.ev_heap = 0;
        s.filt_hex_len = 0;
        s.filt_kinds_hex_len = 0;
        s.keep = 99;
        s.filt_len = 0;
        s.filt_fault = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.schema_len == u16::MAX {
            s.schema_len = 0;
            let sys = &*(syscalls as *const SyscallTable);
            let m = b"[store_source] schema past its cap - DROPPED";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        {
            let sch = &s.schema[..s.schema_len as usize];
            let rv = sch_get(sch, b"rv");
            if rv.len() <= s.rv.len() {
                s.rv[..rv.len()].copy_from_slice(rv);
                s.rv_len = rv.len() as u8;
            }
            let missing = sch_get(sch, b"missing");
            if missing.len() <= s.missing.len() {
                s.missing[..missing.len()].copy_from_slice(missing);
                s.missing_len = missing.len() as u16;
            }
        }
        load_filter(s);
        if s.page_bytes == 0 {
            s.page_bytes = LIST_BUF as u32;
        }
        0
    }
}

#[no_mangle]
#[link_section = ".text.module_step"]
pub extern "C" fn module_step(state: *mut u8) -> i32 {
    unsafe {
        if state.is_null() {
            return 0;
        }
        let s = &mut *(state as *mut State);
        if s.syscalls.is_null() {
            return 0;
        }
        let sys = &*s.syscalls;

        if s.resolved == 0 {
            // A field past the last number a record carries would be missing
            // from every record, and a decision reading it fails closed on
            // each one: refused here, where it is a configuration fault.
            if entries(&s.paths[..s.paths_len as usize]) > MAX_PATHS
                || entries(&s.ints[..s.ints_len as usize]) > MAX_INTS
            {
                let m = b"[store_source] FAULT: more than 15 paths or 7 ints - REFUSED, a record has no field for the rest";
                dev_log(sys, 1, m.as_ptr(), m.len());
                return -22;
            }
            if s.sink >= 0 && s.prefix_len > 0 {
                let plen = s.prefix_len as usize;
                let mut pfx = [0u8; MAX_PREFIX];
                pfx[..plen].copy_from_slice(&s.prefix[..plen]);
                store_subscribe(sys, &pfx[..plen], s.sink);
                // Extra watches: changes here re-arm the scan but are never
                // listed, so a deleted child wakes its parent.
                let w = s.watch_len as usize;
                let mut start = 0usize;
                while start < w {
                    let end = s.watch[start..w]
                        .iter()
                        .position(|&b| b == b',')
                        .map(|i| start + i)
                        .unwrap_or(w);
                    if end > start {
                        let mut wb = [0u8; MAX_PREFIX];
                        let n = (end - start).min(MAX_PREFIX);
                        wb[..n].copy_from_slice(&s.watch[start..start + n]);
                        store_subscribe(sys, &wb[..n], s.sink);
                    }
                    if end >= w {
                        break;
                    }
                    start = end + 1;
                }
                s.subscribed = 1;
            }
            s.resolved = 1;
            s.dirty = 1; // cold start: emit the current set once
        }

        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.dirty = 1;
        }
        // A deadline came due: the kernel stepped us for it. Cancel the
        // fire (an edge — the scan below re-arms for the next one) and scan.
        if s.timer_fd >= 0 {
            let poll = dev_fd_poll(sys, s.timer_fd, POLL_IN);
            if poll > 0 && (poll as u32 & POLL_IN) != 0 {
                let mut none = [0u8; 4];
                (sys.provider_call)(s.timer_fd, TIMER_CANCEL, none.as_mut_ptr(), 0);
                s.dirty = 1;
            }
        }
        // A change starts a scan; a scan in flight advances by ONE page per
        // step. Bounded work per step is the contract a cooperative scheduler
        // needs — an unbounded drain here stalls the whole graph.
        if s.record_out >= 0 {
            if !flush(s) {
                return 0;
            }
            if s.events != 0 {
                events_step(s);
                return 0;
            }
            if s.paging == 0 && s.dirty == 1 {
                s.dirty = 0;
                s.paging = 1;
                s.clen = 0;
                s.page_off = 0;
                s.exp_off = 0;
                s.list_idx = 0;
                s.total = 0;
                s.scan_hits = 0;
                s.counting = if s.count != 0 { 1 } else { 0 };
                s.scan_now = dev_millis(sys) as i64;
                s.next_due = i64::MAX;
            }
            if s.paging == 1 && s.counting == 1 {
                // One page per step here too: the count walk is bounded work
                // like the emit walk, and a large prefix must not stall a step.
                if count_page(s) {
                    s.counting = 0;
                    s.clen = 0;
                }
            } else if s.paging == 1 {
                emit_page(s);
            }
        }
        0
    }
}

/// The members of a JSON object: everything between its outer braces, with
/// surrounding whitespace trimmed. Empty for `{}` or for anything that is not
/// an object.
fn object_members(sub: &[u8]) -> &[u8] {
    let mut a = 0usize;
    let mut b = sub.len();
    while a < b && sub[a].is_ascii_whitespace() {
        a += 1;
    }
    while b > a && sub[b - 1].is_ascii_whitespace() {
        b -= 1;
    }
    if b - a < 2 || sub[a] != b'{' || sub[b - 1] != b'}' {
        return &[];
    }
    a += 1;
    b -= 1;
    while a < b && sub[a].is_ascii_whitespace() {
        a += 1;
    }
    while b > a && sub[b - 1].is_ascii_whitespace() {
        b -= 1;
    }
    &sub[a..b]
}

/// Decimal bytes → u64 (a millisecond clock outgrows u32 in 49 days).
fn parse_dec_u64(b: &[u8]) -> u64 {
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    v
}

// ── `events = 1`: the WATCH connector ───────────────────────────────────────
//
// A listing answers "what is there"; a watch answers "what happened since".
// In this mode the source's `prefix` holds watch REGISTRATIONS (one key per
// watching connection, written by the request chain) and every record it
// emits is ONE CHANGE for ONE registration, read from the store's CHANGES
// feed strictly after that registration's cursor:
//
//   registration  {"p": <watched prefix>, "rv": <resume revision>, "ts":
//                  <timeoutSeconds>, "bm": <1 = bookmarks>, "k": <kind>,
//                  "av": <apiVersion>, …}
//   record        the change's object projected by `paths`/`each` as any
//                 listed object is, the registration projected by
//                 `join_paths`/`join_each` (60.., 9x), and
//                   70 type    ADDED | MODIFIED | DELETED | BOOKMARK | ERROR | CLOSE
//                   71 rev     the change's revision (i64)
//                   72 line    the Kubernetes watch event, one JSON line
//                   73 cid     the registration's connection (i64)
//                   74 st      the status the answer opens with (i64)
//
// The cursor per registration is the connector's own state: a bounded table
// (`MAX_WATCHERS`), because which revision each connection has been served up
// to is exactly what a consumer of a change feed must remember and a store
// key rewritten per event would cost a write per event per watcher. A
// registration past the table is refused loudly, not dropped.
//
// Fail-closed: a cursor older than the store's history (LOST), or a burst of
// changes larger than `EV_BUF`, answers 410 Expired on that watch and nothing
// after it — the client relists, which every Kubernetes client does on 410.

const EV_BUF: usize = 16384;
/// Wire bytes per record: a record also carries the object's projections and
/// the registration's, under a Chronicle record's 4 KiB ceiling. A longer
/// line (a large object) goes across several records (`MORE`).
const WIRE_CHUNK: usize = 2048;
/// The largest object the watch connector reads whole (a request body may be
/// 3 MiB; Kubernetes stores up to ~1.5 MiB).
const DOC_MAX: usize = 3 << 20;
/// A change-feed batch grown on the heap stops here: past it, 410.
const EVH_MAX: usize = 16 << 20;
/// `ENOMEM`: the provider's answer when a CHANGES batch outgrows the buffer.
const ERR_ENOMEM: i32 = -12;
const MAX_WATCHERS: usize = 32;
const WNAME: usize = 96;
/// Bookmark cadence for a registration that asked for them — upstream sends
/// one about every minute.
const BOOKMARK_MS: i64 = 60_000;
/// A watch that names no `timeoutSeconds`. Upstream picks 30–60 min at random;
/// one fixed bound here.
const DEFAULT_WATCH_MS: i64 = 1_800_000;
/// `ev_w` when no pass is in progress.
const EV_IDLE: u8 = 0xFF;
const NS_CHANGES: u32 = 0x1307;

#[derive(Clone, Copy)]
struct Watcher {
    live: u8,
    seen: u8,
    /// An ERROR or CLOSE went out: nothing more until the registration goes.
    done: u8,
    name: [u8; WNAME],
    name_len: u8,
    /// The watched prefix — or, for a GET, the object's whole key.
    prefix: [u8; MAX_KEY],
    prefix_len: u16,
    cursor: u64,
    /// The connection the registration belongs to (its `cid`), emitted at 73.
    cid: i64,
    /// The status its answer opens with (`st`, emitted at 74): 200, or the
    /// 201 of a GET that answers a create with the object it made.
    st: i64,
    deadline: i64,
    /// `i64::MAX` when the registration did not ask for bookmarks.
    next_bm: i64,
    /// A LIST registration (`"l":"1"`): served once, as a stream of items.
    /// A GET (`"g":"1"`) is the same with one key and no envelope.
    list: u8,
    get: u8,
    /// `limit` (0 = none), items sent so far, and the revision the listing
    /// is read at.
    lim: u32,
    sent: u32,
    lrev: u64,
    /// Where the listing stands: the provider's cursor for the page being
    /// walked (opaque — the provider's own, empty for the first page) and
    /// how many of that page's entries are walked. A `continue` is these
    /// two, so a page resumes at the entry after the last one sent, over
    /// any store.
    pcur: [u8; MAX_CURSOR],
    pcur_len: u8,
    pskip: u32,
    /// The List envelope's head (`{"kind":"PodList",…,"items":[`), sent
    /// before the first item — or before the tail when nothing passes.
    head: [u8; 160],
    head_len: u8,
    /// Answer as a server-side Table (the registration's `tb`), of this kind.
    table: u8,
    kind: [u8; 40],
    kind_len: u8,
    /// A log read's `tailLines` and `limitBytes` (0 = not asked).
    tail: u32,
    lbytes: u32,
}

const WATCHER_EMPTY: Watcher = Watcher {
    live: 0,
    seen: 0,
    done: 0,
    name: [0; WNAME],
    name_len: 0,
    prefix: [0; MAX_KEY],
    prefix_len: 0,
    cursor: 0,
    cid: 0,
    st: 200,
    deadline: 0,
    next_bm: i64::MAX,
    list: 0,
    get: 0,
    lim: 0,
    sent: 0,
    lrev: 0,
    pcur: [0; MAX_CURSOR],
    pcur_len: 0,
    pskip: 0,
    head: [0; 160],
    head_len: 0,
    table: 0,
    kind: [0; 40],
    kind_len: 0,
    tail: 0,
    lbytes: 0,
};

/// `CHANGES(prefix, since)` into `out`: the byte count written and the fence
/// revision, or the negative errno.
unsafe fn store_changes(
    sys: &SyscallTable,
    prefix: &[u8],
    since: u64,
    out: &mut [u8],
    fence_rev: &mut u64,
) -> i32 {
    let mut arg = [0u8; MAX_KEY + 40];
    if prefix.len() + 34 > arg.len() {
        return -22;
    }
    let mut fence = [0u8; 64];
    let mut p = 0usize;
    arg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    arg[p..p + 8].copy_from_slice(&since.to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 4].copy_from_slice(&(out.len() as u32).to_le_bytes());
    p += 4;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&(fence.len() as u16).to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, NS_CHANGES, arg.as_mut_ptr(), p);
    *fence_rev = fence_revision(&fence);
    n
}

/// The revision of an encoded `ViewConsistent` fence (`fluxor fence.rs`:
/// `[tag 5][source 16][revision u64 LE]`); 0 for any other kind.
fn fence_revision(f: &[u8]) -> u64 {
    match (f.first(), f.get(17..25)) {
        (Some(&5), Some(&[a, b, c, d, e, g, h, i])) => u64::from_le_bytes([a, b, c, d, e, g, h, i]),
        _ => 0,
    }
}

/// A registration's JSON scalar at a top-level key, as bytes (a string's
/// content, a number as written); empty when absent.
fn reg_field<'a>(v: &'a [u8], k: &[u8]) -> &'a [u8] {
    j_path(v, &[k]).unwrap_or(&[])
}

/// A JSON string's content with `\"` and `\\` unescaped, into `out`; its
/// length (0 when it does not fit — nothing half-written is used).
fn json_unescape(v: &[u8], out: &mut [u8]) -> usize {
    let (mut r, mut w) = (0usize, 0usize);
    while let Some(&c) = v.get(r) {
        let b = if c == b'\\' {
            r += 1;
            match v.get(r) {
                Some(&e) => e,
                None => break,
            }
        } else {
            c
        };
        match out.get_mut(w) {
            Some(d) => *d = b,
            None => return 0,
        }
        w += 1;
        r += 1;
    }
    w
}

/// Hex digits → u32 (a `continue` token this connector issued); 0 when
/// absent or not hex.
fn parse_hex_u32(b: &[u8]) -> u32 {
    let mut v = 0u32;
    for &c in b {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => return 0,
        };
        v = v.wrapping_shl(4) | d as u32;
    }
    v
}

/// Bring the watcher table in line with the registrations under `prefix`:
/// a new one starts at its resume revision with its deadline from now, a
/// known one keeps its cursor, a vanished one is dropped.
unsafe fn ev_sync(s: &mut State, now: i64) {
    let sys = &*s.syscalls;
    if s.stale_at_open != 0 && s.stale_purged == 0 {
        if !purge_page(s) {
            // Stale registrations are still going: the table is synced once
            // they are gone, and this runs again next step until then.
            s.dirty = 1;
            return;
        }
        s.stale_purged = 1;
    }
    for w in s.watchers.iter_mut() {
        w.seen = 0;
    }
    let plen = s.prefix_len as usize;
    let mut pfx = [0u8; MAX_PREFIX];
    pfx[..plen].copy_from_slice(&s.prefix[..plen]);
    let mut cur = [0u8; MAX_CURSOR];
    let mut clen = 0usize;
    loop {
        let mut lout = [0u8; LIST_BUF];
        let mut next = [0u8; MAX_CURSOR];
        let (n, nlen) = list_page(
            sys,
            &pfx[..plen],
            &cur[..clen],
            LIST_BUF,
            &mut lout,
            &mut next,
        );
        let mut rp = 0usize;
        while let Some((name, q)) = raw_entry(&lout[..n], rp) {
            rp = q;
            if name.len() > WNAME {
                continue;
            }
            if let Some(w) = s
                .watchers
                .iter_mut()
                .find(|w| w.live != 0 && &w.name[..w.name_len as usize] == name)
            {
                w.seen = 1;
                continue;
            }
            let mut val = [0u8; MAX_VALUE];
            let Some(vl) = get_value(sys, name, &mut val) else {
                continue;
            };
            let v = &val[..vl];
            let p = reg_field(v, b"p");
            if p.is_empty() || p.len() > MAX_KEY {
                continue;
            }
            let Some(w) = s.watchers.iter_mut().find(|w| w.live == 0) else {
                let m = b"[store_source] watch table FULL - a registration is NOT served";
                dev_log(sys, 1, m.as_ptr(), m.len());
                continue;
            };
            *w = WATCHER_EMPTY;
            w.live = 1;
            w.seen = 1;
            w.name[..name.len()].copy_from_slice(name);
            w.name_len = name.len() as u8;
            w.prefix[..p.len()].copy_from_slice(p);
            w.prefix_len = p.len() as u16;
            w.cursor = parse_dec_u64(reg_field(v, b"rv"));
            w.cid = parse_dec_u64(reg_field(v, b"cid")) as i64;
            let st = parse_dec_u64(reg_field(v, b"st")) as i64;
            w.st = if st > 0 { st } else { 200 };
            if reg_field(v, b"tb") == b"1" {
                let k = reg_field(v, b"k");
                if k.len() <= w.kind.len() {
                    w.table = 1;
                    w.kind[..k.len()].copy_from_slice(k);
                    w.kind_len = k.len() as u8;
                }
            }
            if reg_field(v, b"g") == b"1" {
                w.list = 1;
                w.get = 1;
                w.tail = parse_dec_u64(reg_field(v, b"tl")).min(u32::MAX as u64) as u32;
                w.lbytes = parse_dec_u64(reg_field(v, b"lb")).min(u32::MAX as u64) as u32;
            }
            if reg_field(v, b"l") == b"1" {
                w.list = 1;
                w.lim = parse_dec_u64(reg_field(v, b"lim")).min(u32::MAX as u64) as u32;
                // `continue`, as this connector issued it: `<cursor>.<n>` —
                // the provider page it stopped in (hex) and the entries of
                // it walked (hex). A bare `<n>` (an older token) is the
                // first page, `n` entries in: the same place, over any
                // store.
                let cont = reg_field(v, b"cont");
                let (c, n) = match cont.iter().position(|b| *b == b'.') {
                    Some(i) => (&cont[..i], &cont[i + 1..]),
                    None => (&cont[..0], cont),
                };
                w.pcur_len = unhex_into(c, &mut w.pcur) as u8;
                w.pskip = parse_hex_u32(n);
                w.head_len = json_unescape(reg_field(v, b"h"), &mut w.head) as u8;
            }
            let ts = parse_dec_u64(reg_field(v, b"ts")) as i64;
            w.deadline = now.saturating_add(if ts > 0 {
                ts.saturating_mul(1000)
            } else {
                DEFAULT_WATCH_MS
            });
            if reg_field(v, b"bm") == b"1" {
                w.next_bm = now.saturating_add(s.bookmark_ms as i64);
            }
        }
        if nlen == 0 || nlen > cur.len() {
            break;
        }
        cur[..nlen].copy_from_slice(&next[..nlen]);
        clen = nlen;
    }
    for w in s.watchers.iter_mut() {
        if w.live != 0 && w.seen == 0 {
            *w = WATCHER_EMPTY;
        }
    }
}

/// Delete the previous runtime's registrations under the prefix
/// ([`State::stale_at_open`]), one page per call: true once they are gone.
///
/// A delete may be taken and not yet decided, so the purge spans steps,
/// and a registration of this runtime can appear under the prefix while it
/// runs. Stale means present when the purge first listed: a key whose
/// revision is past that listing's is this runtime's and is kept. A pass
/// with a delete still undecided is listed again; a delete that is refused
/// is said, not retried forever.
unsafe fn purge_page(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let plen = s.prefix_len as usize;
    let mut pfx = [0u8; MAX_PREFIX];
    pfx[..plen].copy_from_slice(&s.prefix[..plen]);
    let mut cur = [0u8; MAX_CURSOR];
    let cl = s.purge_cur_len as usize;
    cur[..cl].copy_from_slice(&s.purge_cur[..cl]);
    let mut lout = [0u8; LIST_BUF];
    let mut next = [0u8; MAX_CURSOR];
    let mut rev = 0u64;
    let (n, nl) = list_page_fenced(
        sys,
        &pfx[..plen],
        &cur[..cl],
        LIST_BUF,
        &mut lout,
        &mut next,
        &mut rev,
    );
    if s.purge_rev == 0 {
        // A provider that reports no revision leaves every listed key stale.
        s.purge_rev = if rev == 0 { u64::MAX } else { rev };
    }
    let mut rp = 0usize;
    while let Some((name, q)) = raw_entry(&lout[..n], rp) {
        rp = q;
        match head_rev(sys, name) {
            Some(r) if r <= s.purge_rev => {}
            // Gone already, or this runtime's.
            _ => continue,
        }
        match delete_value(sys, name) {
            WriteAnswer::Pending => s.purge_pending = 1,
            WriteAnswer::Decided(0) => s.purge_gone += 1,
            WriteAnswer::Decided(_) => {
                let m = b"[store_watch] a stale registration was not removed";
                dev_log(sys, 1, m.as_ptr(), m.len());
            }
        }
    }
    if nl > 0 && nl <= MAX_CURSOR {
        s.purge_cur[..nl].copy_from_slice(&next[..nl]);
        s.purge_cur_len = nl as u8;
        return false;
    }
    s.purge_cur_len = 0;
    if s.purge_pending != 0 {
        // Its deletes are asked again by the next pass.
        s.purge_pending = 0;
        return false;
    }
    let mut m = [0u8; 96];
    let mut l = 0usize;
    let head = b"[store_watch] stale registrations removed at open: ";
    let _ = put(&mut m, &mut l, head);
    let mut d = [0u8; 20];
    let dl = write_dec(&mut d, s.purge_gone);
    let _ = put(&mut m, &mut l, &d[..dl]);
    dev_log(sys, 2, m.as_ptr(), l);
    true
}

/// storage.object DELETE `key` unconditionally.
unsafe fn delete_value(sys: &SyscallTable, key: &[u8]) -> WriteAnswer {
    let mut arg = [0u8; MAX_KEY + 32];
    if 2 + key.len() + 2 + 8 + 2 > arg.len() {
        return WriteAnswer::Decided(-22);
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    // The precondition PAIR `[precondition:u8][etag_len:u8]`: ANY, none.
    arg[p] = 0;
    p += 1;
    arg[p] = 0;
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    write_answer((sys.provider_call)(-1, OBJ_DELETE, arg.as_mut_ptr(), p))
}

/// Append `part` at `*at` in `out`; false when it does not fit. Sequential
/// appends, never an array of slices: an array holding byte-string LITERALS
/// is placed in .rodata with pointers the PIC loader does not relocate.
fn put(out: &mut [u8], at: &mut usize, part: &[u8]) -> bool {
    match out.get_mut(*at..*at + part.len()) {
        Some(dst) => {
            dst.copy_from_slice(part);
            *at += part.len();
            true
        }
        None => false,
    }
}

/// One event line — the schema's `event` framing around `obj`, then a
/// newline — into `out`; its length, or 0 when it does not fit.
fn event_line(sch: &[u8], t: &[u8], obj: &[u8], out: &mut [u8]) -> usize {
    let mut pre = [0u8; 96];
    let mut post = [0u8; 32];
    let Some((a, b)) = sch_event(sch, t, &mut pre, &mut post) else {
        return 0;
    };
    let mut l = 0usize;
    let ok = put(out, &mut l, &pre[..a])
        && put(out, &mut l, obj)
        && put(out, &mut l, &post[..b])
        && put(out, &mut l, b"\n");
    if ok {
        l
    } else {
        0
    }
}

/// A change streamed as one event line: the schema's `event` framing around
/// the object's body.
#[allow(
    clippy::too_many_arguments,
    reason = "one change: the watcher, its type and key, its revision, the object and its wire body"
)]
unsafe fn event_emit(
    s: &mut State,
    wi: usize,
    t: &[u8],
    k: &[u8],
    rev: u64,
    o: &[u8],
    body: &[u8],
) -> Emit {
    let mut pre = [0u8; 96];
    let mut post = [0u8; 32];
    let Some((a, mut b)) = sch_event(&s.schema[..s.schema_len as usize], t, &mut pre, &mut post)
    else {
        return Emit::Failed;
    };
    if b >= post.len() {
        return Emit::Failed;
    }
    post[b] = b'\n';
    b += 1;
    wire_emit_body(s, wi, t, k, rev, o, body, &pre[..a], &post[..b], true)
}

/// The object a DELETED event carries: the tombstone store_effect left
/// (`tomb` + the key, its `obj`), else what the key itself says — name and namespace
/// under the registration's kind — so a client can still drop it by key.
unsafe fn deleted_object(s: &State, key: &[u8], reg: &[u8], out: &mut [u8]) -> usize {
    let sys = &*s.syscalls;
    if s.tomb_len > 0 {
        let mut tk = [0u8; MAX_KEY + 16];
        let a = append_bytes(&mut tk, 0, &s.tomb[..s.tomb_len as usize]);
        let kl = append_bytes(&mut tk, a, key);
        let mut tv = [0u8; MAX_VALUE];
        if kl <= MAX_KEY {
            if let Some(n) = get_value(sys, &tk[..kl], &mut tv) {
                if let Some(obj) = j_sub1(&tv[..n], b"obj") {
                    if let Some(dst) = out.get_mut(..obj.len()) {
                        dst.copy_from_slice(obj);
                        return obj.len();
                    }
                }
            }
        }
    }
    // `/<res>/<ns>/<name>` or `/<res>/<name>`.
    let mut segs: [&[u8]; 4] = [&[]; 4];
    let mut ns = 0usize;
    for seg in key.split(|&c| c == b'/').filter(|g| !g.is_empty()) {
        if ns < segs.len() {
            segs[ns] = seg;
        }
        ns += 1;
    }
    let (namespace, name): (&[u8], &[u8]) = match ns {
        3 => (segs[1], segs[2]),
        2 => (&[], segs[1]),
        _ => (&[], &[]),
    };
    let mut l = 0usize;
    let mut ok = put(out, &mut l, b"{\"kind\":\"")
        && put(out, &mut l, reg_field(reg, b"k"))
        && put(out, &mut l, b"\",\"apiVersion\":\"")
        && put(out, &mut l, reg_field(reg, b"av"))
        && put(out, &mut l, b"\",\"metadata\":{\"name\":\"")
        && put(out, &mut l, name)
        && put(out, &mut l, b"\"");
    if ok && !namespace.is_empty() {
        ok = put(out, &mut l, b",\"namespace\":\"")
            && put(out, &mut l, namespace)
            && put(out, &mut l, b"\"");
    }
    if ok && put(out, &mut l, b"}}") {
        l
    } else {
        0
    }
}

/// Lower the filter pipeline once, at construction. A filter that does not
/// decode, lower or scan leaves `filt_fault` set: every candidate is then
/// refused, and the log says why — a selector that silently passed
/// everything would be a leak, not a degradation.
unsafe fn load_filter(s: &mut State) {
    if s.filt_hex_len == 0 && s.filt_fault == 0 {
        return;
    }
    let mut ir = [0u8; FILT_BUF];
    let mut kinds = [0u8; FILT_STAGES];
    let nk = if s.filt_kinds_hex_len > 0 {
        chron::hex_decode(
            &s.filt_kinds_hex[..s.filt_kinds_hex_len as usize],
            &mut kinds,
        )
        .unwrap_or(0)
    } else {
        0
    };
    let lowered = if s.filt_fault != 0 {
        None
    } else {
        chron::hex_decode(&s.filt_hex[..s.filt_hex_len as usize], &mut ir)
            .and_then(|n| chron::lower_stages_kinded(&ir[..n], &kinds[..nk], &mut s.filt).ok())
    };
    match lowered {
        Some(n) if chron::stage_count(&s.filt[..n]) <= FILT_STAGES => {
            s.filt_len = n as u16;
            s.filt_kinds = [chron::STAGE_KIND_COMPUTE; FILT_STAGES];
            for (d, k) in s.filt_kinds.iter_mut().zip(kinds.iter().take(nk)) {
                *d = *k;
            }
        }
        _ => {
            s.filt_fault = 1;
            let sys = &*s.syscalls;
            let m = b"[store_source] FAULT: filter (ir_stages) invalid - NOTHING passes";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
    }
}

/// Does the candidate record `frame` pass the filter? No filter: yes. A
/// faulted filter, or one that fails on this record: no.
unsafe fn filter_keeps(s: &State, frame: &[u8]) -> bool {
    if s.filt_fault != 0 {
        return false;
    }
    if s.filt_len == 0 {
        return true;
    }
    let c = &s.filt[..s.filt_len as usize];
    let ns = chron::stage_count(c);
    if ns > FILT_STAGES {
        return false;
    }
    let mut stages = [chron::Stage {
        code: &[],
        max_cost: 0,
        on_failure: None,
        kind: chron::STAGE_KIND_COMPUTE,
    }; FILT_STAGES];
    for (i, st) in stages.iter_mut().enumerate().take(ns) {
        match chron::stage_at(c, i) {
            Some(stage) => *st = stage,
            None => return false,
        }
        st.kind = s.filt_kinds[i];
    }
    let mut a = [0u8; FILT_FRAME];
    let mut b = [0u8; FILT_FRAME];
    let mut out = [0u8; FILT_FRAME];
    let mut spent = 0u64;
    let Ok(n) = chron::run_stages_with(
        &chron::ComputeOnly,
        &stages[..ns],
        frame,
        &mut a,
        &mut b,
        &mut out,
        &mut spent,
    ) else {
        return false;
    };
    let mut fields = [chron::Field {
        number: 0,
        value: chron::Value::Null,
    }; chron::MAX_PIPE_FIELDS];
    let Ok(nf) = chron::decode_frame(&out[..n], &mut fields) else {
        return false;
    };
    fields[..nf].iter().any(|f| {
        f.number == s.keep
            && matches!(
                f.value,
                chron::Value::Int(1..) | chron::Value::Int(..=-1) | chron::Value::Bool(true)
            )
    })
}

/// What became of a candidate record.
#[derive(PartialEq)]
enum Emit {
    Sent,
    /// The filter refused it: nothing went out.
    Filtered,
    /// It did not fit a record: skipped, and said so.
    Failed,
}

/// Emit one record for watcher `wi`: `obj`'s projections, the registration's,
/// and the event — type `t`, revision `rev`, the wire `line` a client
/// receives. With `filter`, the candidate must pass the filter pipeline first.
#[allow(
    clippy::too_many_arguments,
    reason = "one record's parts: the watcher, the event, its object and its line are distinct inputs, not a bundle"
)]
unsafe fn ev_emit(
    s: &mut State,
    wi: usize,
    t: &[u8],
    key: &[u8],
    rev: u64,
    obj: &[u8],
    line: &[u8],
    filter: bool,
) -> Emit {
    let sys = &*s.syscalls;
    let w = s.watchers[wi];
    let reg_key = &w.name[..w.name_len as usize];
    let mut frame = [0u8; FRAME_BUF];
    let mut len = project(s, key, obj, None, None, None, &mut frame);
    let mut n = 0u8;
    if len != 0 {
        n = frame[0];
        match joined_fields(s, &mut frame, len, n, obj, reg_key) {
            Some((q, m)) => {
                len = q;
                n = m;
                frame[0] = n;
            }
            None => len = 0,
        }
    }
    if len != 0 && filter && !filter_keeps(s, &frame[..len]) {
        return Emit::Filtered;
    }
    if len != 0 {
        let rb = (rev as i64).to_le_bytes();
        let cb = w.cid.to_le_bytes();
        let sb = w.st.to_le_bytes();
        let q = put_field(&mut frame, len, 70, TY_BYTES, t)
            .and_then(|q| put_field(&mut frame, q, 71, TY_I64, &rb))
            .and_then(|q| put_field(&mut frame, q, 72, TY_BYTES, line))
            .and_then(|q| put_field(&mut frame, q, 73, TY_I64, &cb))
            .and_then(|q| put_field(&mut frame, q, 74, TY_I64, &sb));
        match q {
            Some(q) => {
                frame[0] = n + 5;
                len = q;
            }
            None => len = 0,
        }
    }
    if len == 0 {
        let m = b"[store_source] watch event does not fit a record - SKIPPED";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return Emit::Failed;
    }
    s.seq = s.seq.wrapping_add(1);
    s.frame[..len].copy_from_slice(&frame[..len]);
    s.pend_at = 0;
    s.pend_len = len as u16;
    Emit::Sent
}

/// A value as the watch connector reads it: whole on the stack when it fits,
/// else whole on the heap (the `big` variant), else not at all.
enum Val {
    Stack(usize),
    Heap(*mut u8, usize),
    Missing,
    /// Past the stack and the heap could not hold it (no arena, arena
    /// exhausted, or past `DOC_MAX`).
    TooBig,
}

/// Read `key` whole: into `raw` when it fits, else onto the heap.
unsafe fn read_value(sys: &SyscallTable, key: &[u8], raw: &mut [u8]) -> Val {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return Val::Missing;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return Val::Missing;
    }
    let got = range_get(sys, h, 0, raw);
    let mut out = if got < 0 {
        Val::Missing
    } else {
        Val::Stack(got as usize)
    };
    if got >= 0 && got as usize == raw.len() {
        let mut probe = [0u8; 1];
        if range_get(sys, h, raw.len() as u64, &mut probe) > 0 {
            out = read_heap(sys, h);
        }
    }
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    out
}

/// `RANGE_GET` at `off` into `dst`: bytes copied, or the provider's error.
unsafe fn range_get(sys: &SyscallTable, h: i32, off: u64, dst: &mut [u8]) -> i32 {
    let mut rarg = [0u8; 20];
    rarg[0..8].copy_from_slice(&off.to_le_bytes());
    rarg[8..12].copy_from_slice(&(dst.len() as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20)
}

/// The whole value behind an open handle, on the heap, the buffer doubled as
/// it fills.
unsafe fn read_heap(sys: &SyscallTable, h: i32) -> Val {
    let mut cap = 64 * 1024usize;
    let mut p = heap_alloc(sys, cap as u32);
    let mut len = 0usize;
    loop {
        if p.is_null() {
            return Val::TooBig;
        }
        let dst = core::slice::from_raw_parts_mut(p.add(len), cap - len);
        let n = range_get(sys, h, len as u64, dst);
        if n < 0 {
            heap_free(sys, p);
            return Val::Missing;
        }
        len += n as usize;
        if len < cap {
            return Val::Heap(p, len);
        }
        if cap >= DOC_MAX {
            heap_free(sys, p);
            return Val::TooBig;
        }
        cap = (cap * 2).min(DOC_MAX);
        p = heap_realloc(sys, p, cap as u32);
    }
}

/// Emit `head + body + tail` as the wire line of one item or event. A line
/// that fits one record goes as one; a longer one goes as its first
/// `WIRE_CHUNK` now — the filter decides on that record — and the rest as
/// `MORE` records on the following steps. A long line needs the heap (the
/// `big` variant); without it the item is skipped, and said so.
#[allow(
    clippy::too_many_arguments,
    reason = "one record's parts: the watcher, the event, its object and its line are distinct inputs, not a bundle"
)]
unsafe fn wire_emit_body(
    s: &mut State,
    wi: usize,
    t: &[u8],
    key: &[u8],
    rev: u64,
    obj: &[u8],
    body: &[u8],
    head: &[u8],
    tail: &[u8],
    filter: bool,
) -> Emit {
    let total = head.len() + body.len() + tail.len();
    if total <= WIRE_CHUNK {
        let mut line = [0u8; WIRE_CHUNK];
        let mut l = 0usize;
        let ok = put(&mut line, &mut l, head)
            && put(&mut line, &mut l, body)
            && put(&mut line, &mut l, tail);
        if !ok {
            return Emit::Failed;
        }
        return ev_emit(s, wi, t, key, rev, obj, &line[..l], filter);
    }
    let sys = &*s.syscalls;
    let p = heap_alloc(sys, total as u32);
    if p.is_null() {
        let m = b"[store_source] an object past one record needs store_watch's heap - SKIPPED";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return Emit::Failed;
    }
    let buf = core::slice::from_raw_parts_mut(p, total);
    let mut l = 0usize;
    let _ = put(buf, &mut l, head) && put(buf, &mut l, body) && put(buf, &mut l, tail);
    let e = ev_emit(s, wi, t, key, rev, obj, &buf[..WIRE_CHUNK], filter);
    if e != Emit::Sent {
        heap_free(sys, p);
        return e;
    }
    s.wire = p;
    s.wire_len = total;
    s.wire_at = WIRE_CHUNK;
    s.wire_rev = rev;
    s.wire_cid = s.watchers[wi].cid;
    Emit::Sent
}

/// `wire_emit_body` with the object itself as the line's body.
#[allow(
    clippy::too_many_arguments,
    reason = "one record's parts: the watcher, the event, its object and its line are distinct inputs, not a bundle"
)]
unsafe fn wire_emit(
    s: &mut State,
    wi: usize,
    t: &[u8],
    key: &[u8],
    rev: u64,
    obj: &[u8],
    head: &[u8],
    tail: &[u8],
    filter: bool,
) -> Emit {
    wire_emit_body(s, wi, t, key, rev, obj, obj, head, tail, filter)
}

/// The next `MORE` record of a long wire line: the chunk, its revision and
/// connection, and nothing else (the filter already decided on the first).
unsafe fn wire_more(s: &mut State) {
    let sys = &*s.syscalls;
    let at = s.wire_at;
    let end = (at + WIRE_CHUNK).min(s.wire_len);
    let chunk = core::slice::from_raw_parts(s.wire.add(at), end - at);
    let mut frame = [0u8; FRAME_BUF];
    let rb = (s.wire_rev as i64).to_le_bytes();
    let cb = s.wire_cid.to_le_bytes();
    let sq = s.seq.to_le_bytes();
    let q = put_field(&mut frame, 1, 1, TY_I64, &sq)
        .and_then(|q| put_field(&mut frame, q, 70, TY_BYTES, b"MORE"))
        .and_then(|q| put_field(&mut frame, q, 71, TY_I64, &rb))
        .and_then(|q| put_field(&mut frame, q, 72, TY_BYTES, chunk))
        .and_then(|q| put_field(&mut frame, q, 73, TY_I64, &cb))
        .and_then(|q| put_field(&mut frame, q, 74, TY_I64, &200i64.to_le_bytes()));
    if let Some(len) = q {
        frame[0] = 6;
        s.seq = s.seq.wrapping_add(1);
        s.frame[..len].copy_from_slice(&frame[..len]);
        s.pend_at = 0;
        s.pend_len = len as u16;
    }
    s.wire_at = end;
    if end >= s.wire_len {
        heap_free(sys, s.wire);
        s.wire = core::ptr::null_mut();
        s.wire_len = 0;
        s.wire_at = 0;
    }
}

/// CHANGES into a heap buffer when the batch outgrew `ev_buf`: the buffer
/// doubled until it holds the batch or `EVH_MAX` is reached. The byte count,
/// or the provider's error (`ENOMEM` past `EVH_MAX`, or with no heap).
unsafe fn changes_on_heap(s: &mut State, prefix: &[u8], since: u64, fence: &mut u64) -> i32 {
    let sys = &*s.syscalls;
    let mut cap = if s.evh_cap == 0 {
        4 * EV_BUF
    } else {
        s.evh_cap
    };
    loop {
        if s.evh.is_null() || s.evh_cap < cap {
            let p = if s.evh.is_null() {
                heap_alloc(sys, cap as u32)
            } else {
                heap_realloc(sys, s.evh, cap as u32)
            };
            if p.is_null() {
                return ERR_ENOMEM;
            }
            s.evh = p;
            s.evh_cap = cap;
        }
        let dst = core::slice::from_raw_parts_mut(s.evh, s.evh_cap);
        let n = store_changes(sys, prefix, since, dst, fence);
        if n != ERR_ENOMEM || cap >= EVH_MAX {
            if n >= 0 {
                s.ev_heap = 1;
            }
            return n;
        }
        cap = (cap * 2).min(EVH_MAX);
    }
}

/// The current change-feed batch, wherever it lives — as a slice not tied to
/// `s`, so the emitters (which touch other fields) can run while it is read.
unsafe fn ev_bytes(s: &State) -> &'static [u8] {
    if s.ev_heap != 0 {
        core::slice::from_raw_parts(s.evh, (s.ev_len as usize).min(s.evh_cap))
    } else {
        core::slice::from_raw_parts(s.ev_buf.as_ptr(), (s.ev_len as usize).min(EV_BUF))
    }
}

/// Emit a watch event `t` for `obj` (the event line built here).
#[allow(
    clippy::too_many_arguments,
    reason = "one record's parts: the watcher, the event, its object and its line are distinct inputs, not a bundle"
)]
unsafe fn ev_event(
    s: &mut State,
    wi: usize,
    t: &[u8],
    key: &[u8],
    rev: u64,
    obj: &[u8],
    filter: bool,
) -> Emit {
    let mut line = [0u8; MAX_VALUE + 64];
    let ll = event_line(&s.schema[..s.schema_len as usize], t, obj, &mut line);
    if ll == 0 {
        return Emit::Failed;
    }
    ev_emit(s, wi, t, key, rev, obj, &line[..ll], filter)
}

/// One step of a LIST registration: the next item that passes the filter, or
/// the envelope's tail. Returns true when a record went out this step.
unsafe fn list_step(s: &mut State, wi: usize) -> bool {
    let sys = &*s.syscalls;
    if s.lp_cut != 0 {
        return list_end(s, wi, false);
    }
    // A GET: one key, the object as the whole body, then the end — or 404.
    if s.watchers[wi].get != 0 {
        let w = s.watchers[wi];
        // A subresource is read through its view (`<key>#scale`).
        let (key, view) = view_split(&w.prefix[..w.prefix_len as usize]);
        // A write's answer registers its key with the writer's marks
        // (`@<manager>`, `!`): the view is what comes before them.
        let view = view_args(view).0;
        let reg_key = &w.name[..w.name_len as usize];
        let mut raw = [0u8; MAX_VALUE];
        let (p, n) = match read_value(sys, key, &mut raw) {
            Val::Stack(n) => (core::ptr::null_mut(), n),
            Val::Heap(p, n) => (p, n),
            Val::Missing | Val::TooBig => {
                s.watchers[wi].done = 1;
                s.ev_w += 1;
                let mb = s.missing;
                let ml = s.missing_len as usize;
                return ev_emit(s, wi, b"NOTFOUND", reg_key, 0, b"{}", &mb[..ml], false)
                    == Emit::Sent;
            }
        };
        let doc: &[u8] = if p.is_null() {
            &raw[..n]
        } else {
            core::slice::from_raw_parts(p, n)
        };
        let mut sc = [0u8; 1024];
        let mut lg = [0u8; MAX_VALUE];
        let doc: &[u8] = match sch_view(&s.schema[..s.schema_len as usize], view) {
            SchView::Template(t) => match sch_render(t, doc, &mut sc) {
                Some(l) => &sc[..l],
                None => doc,
            },
            SchView::Text(prefix, path) => {
                related_text(sys, doc, prefix, path, w.tail, w.lbytes, &mut lg)
            }
            SchView::Whole => doc,
        };
        let rev = head_rev(sys, key).unwrap_or(0);
        s.watchers[wi].lrev = rev;
        let sent = stamped_emit(s, wi, b"ITEM", key, rev, doc, b"", false);
        if !p.is_null() {
            heap_free(sys, p);
        }
        s.lp_cut = 1;
        return sent;
    }
    loop {
        // A page drained (or none yet): the next one — from the cursor the
        // provider gave with the last page, or, first, the registration's
        // (a resumed `continue`), its walked entries skipped.
        if s.lp_rp >= s.lp_n {
            if s.lp_end != 0 {
                return list_end(s, wi, false);
            }
            if s.lp_n > 0 {
                let nl = s.lp_next_len;
                s.watchers[wi].pcur = s.lp_next;
                s.watchers[wi].pcur_len = nl;
                s.watchers[wi].pskip = 0;
            }
            let w = s.watchers[wi];
            let mut next = [0u8; MAX_CURSOR];
            let mut fence = 0u64;
            let (n, nlen) = list_page_fenced(
                sys,
                &w.prefix[..w.prefix_len as usize],
                &w.pcur[..w.pcur_len as usize],
                LIST_BUF,
                &mut s.lp_page,
                &mut next,
                &mut fence,
            );
            if w.lrev == 0 {
                s.watchers[wi].lrev = fence;
            }
            s.lp_n = n;
            s.lp_rp = 0;
            s.lp_end = if nlen == 0 { 1 } else { 0 };
            s.lp_next = next;
            s.lp_next_len = nlen.min(MAX_CURSOR) as u8;
            if n == 0 {
                return list_end(s, wi, false);
            }
            let mut skip = w.pskip;
            while skip > 0 {
                match raw_entry(&s.lp_page[..s.lp_n], s.lp_rp) {
                    Some((_, q)) => s.lp_rp = q,
                    None => {
                        s.lp_rp = s.lp_n;
                        break;
                    }
                }
                skip -= 1;
            }
            continue;
        }
        let w = s.watchers[wi];
        let Some((name, q)) = raw_entry(&s.lp_page[..s.lp_n], s.lp_rp) else {
            s.lp_rp = s.lp_n;
            continue;
        };
        s.lp_rp = q;
        s.watchers[wi].pskip = w.pskip.wrapping_add(1);
        let kl = name.len();
        if kl > MAX_KEY {
            continue;
        }
        let mut key = [0u8; MAX_KEY];
        key[..kl].copy_from_slice(name);
        let mut raw = [0u8; MAX_VALUE];
        let (p, vl) = match read_value(sys, &key[..kl], &mut raw) {
            Val::Stack(n) => (core::ptr::null_mut(), n),
            Val::Heap(p, n) => (p, n),
            Val::Missing => continue,
            Val::TooBig => {
                let m = b"[store_source] a LIST item past DOC_MAX or the heap - OMITTED";
                dev_log(sys, 1, m.as_ptr(), m.len());
                continue;
            }
        };
        let doc: &[u8] = if p.is_null() {
            &raw[..vl]
        } else {
            core::slice::from_raw_parts(p, vl)
        };
        let rev = head_rev(sys, &key[..kl]).unwrap_or(0);
        // "," before every item but the first; the head before the first.
        let e = if w.sent == 0 {
            let head = w.head;
            stamped_emit(
                s,
                wi,
                b"ITEM",
                &key[..kl],
                rev,
                doc,
                &head[..w.head_len as usize],
                true,
            )
        } else {
            stamped_emit(s, wi, b"ITEM", &key[..kl], rev, doc, b",", true)
        };
        if !p.is_null() {
            heap_free(sys, p);
        }
        if !e {
            continue;
        }
        let sent = w.sent + 1;
        s.watchers[wi].sent = sent;
        // `limit` reached with more behind it: the tail carries `continue`.
        if w.lim > 0 && sent >= w.lim && (s.lp_rp < s.lp_n || s.lp_end == 0) {
            s.lp_cut = 1;
        }
        return true;
    }
}

/// The List envelope's tail (and its head, when no item went out): the
/// revision the listing was read at, and `continue` when `limit` cut it.
unsafe fn list_end(s: &mut State, wi: usize, _cut: bool) -> bool {
    let w = s.watchers[wi];
    let mut digits = [0u8; 20];
    let dl = dec_into(w.lrev, &mut digits);
    let mut line = [0u8; 2048];
    let mut l = 0usize;
    let mut ok = true;
    if w.get != 0 {
        // A GET's body was the object: the end carries nothing — or, for a
        // Table, closes its rows.
        s.watchers[wi].done = 1;
        s.lp_cut = 0;
        s.ev_w += 1;
        let reg_key = w.name;
        if w.table != 0 {
            let vars = [(&b"rv"[..], &digits[..dl])];
            match sch_fill(
                sch_get(&s.schema[..s.schema_len as usize], b"list.tail"),
                &vars,
                &mut line,
            ) {
                Some(n) => l = n,
                None => ok = false,
            }
        }
        return ok
            && ev_emit(
                s,
                wi,
                b"END",
                &reg_key[..w.name_len as usize],
                w.lrev,
                b"{}",
                &line[..l],
                false,
            ) == Emit::Sent;
    }
    if w.sent == 0 {
        ok = if w.table != 0 {
            let cols = tbl_spec(
                &s.table[..s.table_len as usize],
                &w.kind[..w.kind_len as usize],
            );
            tbl_head(&s.schema[..s.schema_len as usize], cols, &mut line, &mut l)
        } else {
            put(&mut line, &mut l, &w.head[..w.head_len as usize])
        };
    }
    // The tail: the schema's `list.tail`, with the revision the list was
    // read at and — when a limit cut it — where the next page continues.
    let mut hex = [0u8; 2 * MAX_CURSOR + 1 + 8];
    let hl = if s.lp_cut != 0 {
        let mut at = hex_into(&w.pcur[..w.pcur_len as usize], &mut hex);
        hex[at] = b'.';
        at += 1;
        let mut n = [0u8; 8];
        let nl = hex_u32(w.pskip, &mut n);
        hex[at..at + nl].copy_from_slice(&n[..nl]);
        at + nl
    } else {
        0
    };
    if ok {
        let vars = [(&b"rv"[..], &digits[..dl]), (&b"continue"[..], &hex[..hl])];
        match sch_fill(
            sch_get(&s.schema[..s.schema_len as usize], b"list.tail"),
            &vars,
            &mut line[l..],
        ) {
            Some(n) => l += n,
            None => ok = false,
        }
    }
    s.watchers[wi].done = 1;
    s.lp_n = 0;
    s.lp_rp = 0;
    s.lp_end = 0;
    s.lp_cut = 0;
    s.ev_w += 1;
    if !ok {
        return false;
    }
    let reg_key = w.name;
    ev_emit(
        s,
        wi,
        b"END",
        &reg_key[..w.name_len as usize],
        w.lrev,
        b"{}",
        &line[..l],
        false,
    ) == Emit::Sent
}

/// `b` as lowercase hex into `out` (room for twice its length): the count.
fn hex_into(b: &[u8], out: &mut [u8]) -> usize {
    const HX: &[u8; 16] = b"0123456789abcdef";
    let mut n = 0usize;
    for &c in b {
        if n + 2 > out.len() {
            break;
        }
        out[n] = HX[(c >> 4) as usize];
        out[n + 1] = HX[(c & 0xF) as usize];
        n += 2;
    }
    n
}

/// Hex digits (a cursor this connector issued) into `out`: the byte count,
/// 0 when absent, odd, not hex, or longer than `out`.
fn unhex_into(h: &[u8], out: &mut [u8]) -> usize {
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    if !h.len().is_multiple_of(2) || h.len() / 2 > out.len() {
        return 0;
    }
    for (i, pair) in h.chunks(2).enumerate() {
        match (nib(pair[0]), nib(pair[1])) {
            (Some(a), Some(b)) => out[i] = (a << 4) | b,
            _ => return 0,
        }
    }
    h.len() / 2
}

/// `v` as lowercase hex without leading zeros ("0" for zero).
fn hex_u32(v: u32, out: &mut [u8; 8]) -> usize {
    const HX: &[u8; 16] = b"0123456789abcdef";
    let mut tmp = [0u8; 8];
    let mut n = 0usize;
    let mut x = v;
    loop {
        tmp[n] = HX[(x & 0xF) as usize];
        n += 1;
        x >>= 4;
        if x == 0 || n == 8 {
            break;
        }
    }
    for i in 0..n {
        out[i] = tmp[n - 1 - i];
    }
    n
}

/// `rev` stamped into the object at the schema's `rv` path, as a string; 0
/// when there is no such path, the value is not an object, or it does not fit.
fn stamp_rv(raw: &[u8], rev: u64, out: &mut [u8], rvp: &[u8]) -> usize {
    if rvp.is_empty() || jp_kind(raw) != b'{' {
        return 0;
    }
    let mut digits = [0u8; 20];
    let dl = dec_into(rev, &mut digits);
    let mut tok = [0u8; 24];
    tok[0] = b'"';
    tok[1..1 + dl].copy_from_slice(&digits[..dl]);
    tok[1 + dl] = b'"';
    let mut patch = [0u8; 160];
    let mut po = JpOut::new(&mut patch);
    if sch_one(rvp, &tok[..dl + 2], &mut po).is_err() {
        return 0;
    }
    let pl = po.n;
    let mut o = JpOut::new(out);
    match jp_merge(Some(raw), &patch[..pl], &mut o, None, b"", 0) {
        Ok(()) => o.n,
        Err(_) => 0,
    }
}

/// A `text:` view: the text stored at `<prefix><doc's value at path>` (for
/// a Pod's log, the sandbox's output tail under its uid), its last `tail`
/// lines and at most `lbytes` bytes when asked. Nothing stored: empty.
unsafe fn related_text<'a>(
    sys: &SyscallTable,
    doc: &[u8],
    prefix: &[u8],
    path: &[u8],
    tail: u32,
    lbytes: u32,
    out: &'a mut [u8; MAX_VALUE],
) -> &'a [u8] {
    let id = sch_at(doc, path).unwrap_or(&[]);
    let id = id
        .strip_prefix(b"\"")
        .and_then(|v| v.strip_suffix(b"\""))
        .unwrap_or(id);
    let mut key = [0u8; MAX_KEY];
    if id.is_empty() || prefix.len() + id.len() > key.len() {
        return &[];
    }
    key[..prefix.len()].copy_from_slice(prefix);
    key[prefix.len()..prefix.len() + id.len()].copy_from_slice(id);
    let n = match read_value(sys, &key[..prefix.len() + id.len()], out) {
        Val::Stack(n) => n,
        Val::Heap(p, _) => {
            heap_free(sys, p);
            0
        }
        Val::Missing | Val::TooBig => 0,
    };
    let text = &out[..n];
    // The last `tail` lines: a trailing newline ends the last line, it does
    // not start another.
    let mut start = 0usize;
    if tail > 0 {
        let body_end = if text.last() == Some(&b'\n') {
            n - 1
        } else {
            n
        };
        let mut seen = 0u32;
        for i in (0..body_end).rev() {
            if text[i] == b'\n' {
                seen += 1;
                if seen == tail {
                    start = i + 1;
                    break;
                }
            }
        }
    }
    let text = &out[start..n];
    if lbytes > 0 && (lbytes as usize) < text.len() {
        &text[..lbytes as usize]
    } else {
        text
    }
}

/// `doc` with its resourceVersion stamped (on the stack when it fits, the
/// heap when not), emitted after `head` as one item. False when nothing went
/// out (filtered, or it did not fit).
#[allow(
    clippy::too_many_arguments,
    reason = "one record's parts: the watcher, the event, its object and its line are distinct inputs, not a bundle"
)]
unsafe fn stamped_emit(
    s: &mut State,
    wi: usize,
    t: &[u8],
    key: &[u8],
    rev: u64,
    doc: &[u8],
    head: &[u8],
    filter: bool,
) -> bool {
    let sys = &*s.syscalls;
    let (rvb, rvl) = (s.rv, s.rv_len as usize);
    let mut obj = [0u8; MAX_VALUE + 32];
    let mut hp: *mut u8 = core::ptr::null_mut();
    let ol = if doc.len() + 32 <= obj.len() {
        stamp_rv(doc, rev, &mut obj, &rvb[..rvl])
    } else {
        hp = heap_alloc(sys, (doc.len() + 48) as u32);
        if hp.is_null() {
            0
        } else {
            stamp_rv(
                doc,
                rev,
                core::slice::from_raw_parts_mut(hp, doc.len() + 48),
                &rvb[..rvl],
            )
        }
    };
    let o: &[u8] = if ol == 0 {
        doc
    } else if hp.is_null() {
        &obj[..ol]
    } else {
        core::slice::from_raw_parts(hp, ol)
    };
    let w = s.watchers[wi];
    let e = if w.table != 0 {
        // A server-side Table: the first item (or a GET's object) opens it;
        // every item is a row. The filter still reads the object itself.
        let cols_ptr = s.table.as_ptr();
        let cols = tbl_spec(
            core::slice::from_raw_parts(cols_ptr, s.table_len as usize),
            &w.kind[..w.kind_len as usize],
        );
        let sch = core::slice::from_raw_parts(s.schema.as_ptr(), s.schema_len as usize);
        let cap = o.len() + 4096;
        let rp = heap_alloc(sys, cap as u32);
        if rp.is_null() {
            Emit::Failed
        } else {
            let rb = core::slice::from_raw_parts_mut(rp, cap);
            let mut at = 0usize;
            let now = dev_unix_millis(sys);
            let ok = (if head == b"," {
                put(rb, &mut at, b",")
            } else if !head.is_empty() || w.get != 0 {
                tbl_head(sch, cols, rb, &mut at)
            } else {
                true
            }) && tbl_row(sch, cols, o, now, rb, &mut at);
            let e = if ok {
                wire_emit_body(
                    s,
                    wi,
                    t,
                    key,
                    rev,
                    o,
                    &core::slice::from_raw_parts(rp, cap)[..at],
                    b"",
                    b"",
                    filter,
                )
            } else {
                Emit::Failed
            };
            heap_free(sys, rp);
            e
        }
    } else {
        wire_emit(s, wi, t, key, rev, o, head, b"", filter)
    };
    if !hp.is_null() {
        heap_free(sys, hp);
    }
    e == Emit::Sent
}

/// One step of the watch connector: at most ONE record, so a burst of changes
/// is paced by the consumer (`flush`) rather than stalling the graph.
unsafe fn events_step(s: &mut State) {
    let sys = &*s.syscalls;
    // A long wire line in flight finishes before anything else goes out: its
    // records are one response's bytes, in order.
    if !s.wire.is_null() {
        wire_more(s);
        return;
    }
    if s.ev_w == EV_IDLE {
        if s.dirty == 0 {
            return;
        }
        s.dirty = 0;
        s.scan_now = dev_millis(sys) as i64;
        ev_sync(s, s.scan_now);
        s.ev_w = 0;
        s.ev_len = 0;
    }
    let now = s.scan_now;
    while (s.ev_w as usize) < MAX_WATCHERS {
        let wi = s.ev_w as usize;
        let w = s.watchers[wi];
        if w.live == 0 || w.done != 0 {
            s.ev_w += 1;
            continue;
        }
        // A LIST: served once, one item per step, then its tail.
        if w.list != 0 {
            if list_step(s, wi) {
                return;
            }
            continue;
        }
        let reg_key = &w.name[..w.name_len as usize];
        if s.ev_len == 0 {
            // The watch's own end: timeoutSeconds.
            if now >= w.deadline {
                s.watchers[wi].done = 1;
                s.ev_w += 1;
                ev_event(s, wi, b"CLOSE", reg_key, w.cursor, b"{}", false);
                return;
            }
            let mut fence = 0u64;
            s.ev_heap = 0;
            let mut n = store_changes(
                sys,
                &w.prefix[..w.prefix_len as usize],
                w.cursor,
                &mut s.ev_buf,
                &mut fence,
            );
            // A batch past `ev_buf` (large objects): again, on the heap.
            if n == ERR_ENOMEM {
                n = changes_on_heap(s, &w.prefix[..w.prefix_len as usize], w.cursor, &mut fence);
            }
            s.ev_len = if n > 0 { n as u32 } else { 0 };
            let head5 = ev_bytes(s);
            // LOST (history no longer reaches the cursor) or a batch past
            // every buffer: 410, and the client relists.
            if n < 5 || head5.first() == Some(&1) {
                s.watchers[wi].done = 1;
                s.ev_w += 1;
                s.ev_len = 0;
                s.ev_heap = 0;
                let mut ex = [0u8; 256];
                let el = expired(s, &mut ex);
                ev_event(s, wi, b"ERROR", reg_key, fence, &ex[..el], false);
                return;
            }
            let count = u32::from_le_bytes([head5[1], head5[2], head5[3], head5[4]]);
            if count == 0 {
                if s.watchers[wi].cursor == 0 {
                    s.watchers[wi].cursor = fence;
                }
                if now >= w.next_bm {
                    s.watchers[wi].next_bm = now.saturating_add(s.bookmark_ms as i64);
                    s.ev_w += 1;
                    let rv = s.watchers[wi].cursor;
                    let mut reg = [0u8; MAX_VALUE];
                    let rl = get_value(sys, reg_key, &mut reg).unwrap_or(0);
                    let mut obj = [0u8; 256];
                    let ol = bookmark_object(
                        &s.schema[..s.schema_len as usize],
                        &reg[..rl],
                        rv,
                        &mut obj,
                    );
                    ev_event(s, wi, b"BOOKMARK", reg_key, rv, &obj[..ol], false);
                    return;
                }
                s.ev_len = 0;
                s.ev_heap = 0;
                s.ev_w += 1;
                continue;
            }
            s.ev_at = 5;
            s.ev_fence = fence;
        }
        // The next change of this watcher's batch.
        let at = s.ev_at as usize;
        let len = s.ev_len as usize;
        if at + 15 > len {
            let f = s.ev_fence;
            let c = &mut s.watchers[wi].cursor;
            *c = (*c).max(f);
            s.ev_len = 0;
            s.ev_heap = 0;
            s.ev_w += 1;
            continue;
        }
        let b = ev_bytes(s);
        let rev = u64::from_le_bytes([
            b[at],
            b[at + 1],
            b[at + 2],
            b[at + 3],
            b[at + 4],
            b[at + 5],
            b[at + 6],
            b[at + 7],
        ]);
        let kind = b[at + 8];
        let kl = u16::from_le_bytes([b[at + 9], b[at + 10]]) as usize;
        let vl = u32::from_le_bytes([b[at + 11], b[at + 12], b[at + 13], b[at + 14]]) as usize;
        let ks = at + 15;
        if ks + kl + vl > len || kl > MAX_KEY {
            // A malformed batch: stop reading it rather than misreading.
            s.watchers[wi].done = 1;
            s.ev_len = 0;
            s.ev_w += 1;
            let mut ex = [0u8; 256];
            let el = expired(s, &mut ex);
            ev_event(s, wi, b"ERROR", reg_key, rev, &ex[..el], false);
            return;
        }
        s.ev_at = (ks + kl + vl) as u32;
        let mut key = [0u8; MAX_KEY];
        key[..kl].copy_from_slice(&b[ks..ks + kl]);
        let mut raw = [0u8; MAX_VALUE];
        let doc: &[u8] = if kind == 2 {
            let mut reg = [0u8; MAX_VALUE];
            let rl = get_value(sys, reg_key, &mut reg).unwrap_or(0);
            let n = deleted_object(s, &key[..kl], &reg[..rl], &mut raw);
            &raw[..n]
        } else {
            &b[ks + kl..ks + kl + vl]
        };
        // The object's resourceVersion is the change's revision — stamped on
        // the stack when it fits, on the heap when it does not.
        let (rvb, rvl) = (s.rv, s.rv_len as usize);
        let mut obj = [0u8; MAX_VALUE + 32];
        let mut hp: *mut u8 = core::ptr::null_mut();
        let ol = if doc.len() + 32 <= obj.len() {
            stamp_rv(doc, rev, &mut obj, &rvb[..rvl])
        } else {
            hp = heap_alloc(sys, (doc.len() + 48) as u32);
            if hp.is_null() {
                0
            } else {
                stamp_rv(
                    doc,
                    rev,
                    core::slice::from_raw_parts_mut(hp, doc.len() + 48),
                    &rvb[..rvl],
                )
            }
        };
        let c = &mut s.watchers[wi].cursor;
        *c = (*c).max(rev);
        let o: &[u8] = if ol == 0 {
            doc
        } else if hp.is_null() {
            &obj[..ol]
        } else {
            core::slice::from_raw_parts(hp, ol)
        };
        // A watcher that asked for a Table gets each event's object as a
        // one-row Table (the columns again: a client may join mid-stream).
        let mut tp: *mut u8 = core::ptr::null_mut();
        let mut tl = 0usize;
        if w.table != 0 {
            let cols = tbl_spec(
                &s.table[..s.table_len as usize],
                &w.kind[..w.kind_len as usize],
            );
            let cap = o.len() + 4096;
            tp = heap_alloc(sys, cap as u32);
            if !tp.is_null() {
                let tb = core::slice::from_raw_parts_mut(tp, cap);
                let now = dev_unix_millis(sys);
                let sch = &s.schema[..s.schema_len as usize];
                if tbl_head(sch, cols, tb, &mut tl)
                    && tbl_row(sch, cols, o, now, tb, &mut tl)
                    && tbl_end(sch, tb, &mut tl)
                {
                } else {
                    tl = 0;
                }
            }
        }
        let body: &[u8] = if tl > 0 {
            core::slice::from_raw_parts(tp, tl)
        } else {
            o
        };
        // A change the filter refuses sends nothing: on to the next one. One
        // call per arm: a `match` that YIELDS a byte-string literal is a table
        // of unrelocated pointers in a PIC module.
        let k = &key[..kl];
        let sent = match kind {
            0 => event_emit(s, wi, b"ADDED", k, rev, o, body),
            1 => event_emit(s, wi, b"MODIFIED", k, rev, o, body),
            _ => event_emit(s, wi, b"DELETED", k, rev, o, body),
        };
        if !hp.is_null() {
            heap_free(sys, hp);
        }
        if !tp.is_null() {
            heap_free(sys, tp);
        }
        if sent == Emit::Sent {
            return;
        }
    }
    s.ev_w = EV_IDLE;
    ev_arm(s, sys);
}

/// A BOOKMARK's object: the schema's `bookmark` template, filled with the
/// registration's kind and apiVersion and the revision the watch reached.
fn bookmark_object(sch: &[u8], reg: &[u8], rv: u64, out: &mut [u8]) -> usize {
    let mut digits = [0u8; 20];
    let dl = dec_into(rv, &mut digits);
    let vars = [
        (&b"kind"[..], reg_field(reg, b"k")),
        (&b"av"[..], reg_field(reg, b"av")),
        (&b"rv"[..], &digits[..dl]),
    ];
    sch_fill(sch_get(sch, b"bookmark"), &vars, out).unwrap_or(0)
}

/// The event a watch past its history ends with (the schema's `expired`)
/// into `out`; its length.
fn expired(s: &State, out: &mut [u8; 256]) -> usize {
    let e = sch_get(&s.schema[..s.schema_len as usize], b"expired");
    let n = e.len().min(out.len());
    out[..n].copy_from_slice(&e[..n]);
    n
}

/// Arm the timer for the earliest watch deadline or bookmark ahead.
unsafe fn ev_arm(s: &mut State, sys: &SyscallTable) {
    let next = s
        .watchers
        .iter()
        .filter(|w| w.live != 0 && w.done == 0)
        .map(|w| w.deadline.min(w.next_bm))
        .min()
        .unwrap_or(i64::MAX);
    if s.timer_fd < 0 {
        if next == i64::MAX {
            return;
        }
        s.timer_fd = (sys.provider_open)(TIMER_CONTRACT, TIMER_CREATE, core::ptr::null_mut(), 0);
        if s.timer_fd < 0 {
            let m = b"[store_source] no timer fd - watch timeouts and bookmarks will NOT fire";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return;
        }
    }
    let mut none = [0u8; 4];
    if next == i64::MAX {
        (sys.provider_call)(s.timer_fd, TIMER_CANCEL, none.as_mut_ptr(), 0);
        return;
    }
    let delay = (next - s.scan_now).clamp(1, u32::MAX as i64) as u32;
    let mut d = delay.to_le_bytes();
    (sys.provider_call)(s.timer_fd, TIMER_SET, d.as_mut_ptr(), 4);
}

/// store_watch — this source built with `big` — takes a heap arena: a whole
/// object, its stamped copy and wire line, and a change-feed batch. store_source
/// takes none.
#[allow(
    unexpected_cfgs,
    reason = "`big` is store_watch's feature; this file is also store_source, whose build does not declare it"
)]
mod arena {
    #[cfg(feature = "big")]
    #[no_mangle]
    #[link_section = ".text.module_arena_size"]
    pub extern "C" fn module_arena_size() -> u32 {
        32 << 20
    }
}

const TIMER_CONTRACT: u32 = 0x0006;
const TIMER_CREATE: u32 = 0x0604;
const TIMER_SET: u32 = 0x0605;
const TIMER_CANCEL: u32 = 0x0606;

/// Arm the kernel timer for the earliest deadline ahead (or cancel it when
/// none is), at the end of a scan. The kernel wakes this module when it
/// fires; `module_step` sees POLL_IN and re-arms the scan.
unsafe fn arm_due(s: &mut State, sys: &SyscallTable) {
    if s.due_len == 0 {
        return;
    }
    if s.timer_fd < 0 {
        s.timer_fd = (sys.provider_open)(TIMER_CONTRACT, TIMER_CREATE, core::ptr::null_mut(), 0);
        if s.timer_fd < 0 {
            let m = b"[store_source] no timer fd - deadlines will NOT wake this source";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return;
        }
    }
    let mut none = [0u8; 4];
    if s.next_due == i64::MAX {
        (sys.provider_call)(s.timer_fd, TIMER_CANCEL, none.as_mut_ptr(), 0);
        return;
    }
    let delay = (s.next_due - s.scan_now).clamp(1, u32::MAX as i64) as u32;
    let mut d = delay.to_le_bytes();
    (sys.provider_call)(s.timer_fd, TIMER_SET, d.as_mut_ptr(), 4);
}

/// Decimal bytes → u32. A projected count arrives as the scalar token.
fn parse_dec_bytes(b: &[u8]) -> u32 {
    let mut v: u32 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u32);
    }
    v
}

/// Bounded copy; returns the new offset.
fn append_bytes(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

include!("../_shared/table.rs");
