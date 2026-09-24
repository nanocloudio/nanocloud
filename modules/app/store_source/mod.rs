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
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");

const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;
/// One LIST PAGE. Not a ceiling: fluxor's store provider pages, filling this
/// buffer and handing back a cursor, and `list_page` walks to the end across
/// steps. Deliberately modest — a bigger buffer buys fewer syscalls, not more
/// reachable objects.
const LIST_BUF: usize = 2048;
const MAX_PREFIX: usize = 96;
/// `paths` param: `spec.host,spec.to.name,...` — dotted, comma-separated.
const MAX_PATHS_SPEC: usize = 256;
const MAX_PATHS: usize = 12;
const FRAME_BUF: usize = 8192;
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
    /// Entries of the current page already delivered. Backpressure resumes HERE
    /// rather than at the page start: repeating from the top makes no forward
    /// progress when a page is larger than the downstream channel, which is a
    /// livelock, not a slow path.
    page_off: u32,
    /// The EXPANSION index already delivered for the entry `page_off` points
    /// at. The same argument as `page_off`, one level down: an object that
    /// expands into more records than the downstream channel can hold makes no
    /// forward progress if the expansion restarts at 0 — it re-emits the same
    /// prefix of indices forever and the tail is never reached. That is a
    /// livelock that LOOKS like convergence, because every index it does emit
    /// converges correctly and only the tail is missing.
    exp_off: i64,
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
    /// is the alarm clock; nothing polls.
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
    /// `ints = <path,path,…>`: those paths as INTEGERS at 50..54 (absent → 0).
    ints: [u8; MAX_PATHS_SPEC],
    ints_len: u16,
    /// Bytes offered to one LIST call. Smaller = more pages, more syscalls,
    /// less state held per step; it does not bound how many objects are
    /// reachable, because `list_page` walks the cursor to the end.
    page_bytes: u32,
    list_buf: [u8; LIST_BUF],
    val_buf: [u8; MAX_VALUE],
    frame: [u8; FRAME_BUF],
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
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    if n < 0 {
        None
    } else {
        Some(n as usize)
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
    (sys.provider_call)(-1, NS_SUBSCRIBE, arg.as_mut_ptr(), p)
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
    // Revision is not exposed by the object GET used here; 0 means "unknown",
    // which a consumer must treat as "re-read before committing" rather than as
    // a usable precondition.
    let Some(q) = put_field(out, p, 2, TY_I64, &0i64.to_le_bytes()) else {
        return 0;
    };
    p = q;
    n += 1;
    let Some(q) = put_field(out, p, 3, TY_BYTES, key_tail) else {
        return 0;
    };
    p = q;
    n += 1;

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
            let (one, modifier) = split_mod(one);
            let got = if one == b"." {
                // The WHOLE VALUE, unparsed. A projection reads FIELDS out of
                // an object, which is right for a reconciler and wrong for
                // anything bridging the store to something that wants opaque
                // bytes — a kagi key frame, an endpoint address, a document a
                // decision only forwards. Without it, reaching a raw value
                // takes a `store_effect` GET — a second round trip to read
                // what this walk already has in hand.
                value
            } else if s.flat != 0 {
                flat_field(value, one).unwrap_or(&[])
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
                let mut segs: [&[u8]; 6] = [&[]; 6];
                let ns = split_dots(one, &mut segs);
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
                }
                let paths = if colon < one.len() {
                    &one[colon + 1..]
                } else {
                    &one[..0]
                };
                let mut rk = [0u8; MAX_KEY + MAX_PREFIX];
                let rl = {
                    let a = append_bytes(&mut rk, 0, prefix);
                    append_bytes(&mut rk, a, key_tail)
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
                        let got = if rel_flat {
                            flat_field(rval, path).unwrap_or(&[])
                        } else {
                            let mut segs: [&[u8]; 6] = [&[]; 6];
                            let ns = split_dots(path, &mut segs);
                            j_sub(rval, &segs[..ns])
                                .or_else(|| j_path(rval, &segs[..ns]))
                                .unwrap_or(&[])
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
        while start <= spec.len() && fno < 58 {
            let end = spec[start..]
                .iter()
                .position(|&b| b == b',')
                .map(|i| start + i)
                .unwrap_or(spec.len());
            let one = &spec[start..end];
            if !one.is_empty() {
                let raw = if s.flat != 0 {
                    flat_field(value, one).unwrap_or(&[])
                } else {
                    let mut segs: [&[u8]; 6] = [&[]; 6];
                    let ns = split_dots(one, &mut segs);
                    j_path(value, &segs[..ns]).unwrap_or(&[])
                };
                let v = parse_dec_bytes(raw) as i64;
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
    // The content digest, at a fixed number like the trailers below. Absent
    // path → digest of nothing, still emitted: a decision comparing hashes
    // must never find the field missing.
    if s.hash_len > 0 {
        let mut segs: [&[u8]; 6] = [&[]; 6];
        let ns = split_dots(&s.hash[..s.hash_len as usize], &mut segs);
        let bytes = j_sub(value, &segs[..ns])
            .or_else(|| j_path(value, &segs[..ns]))
            .unwrap_or(&[]);
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
            // The joined object itself: its fields at 60..64 and, at 65, the
            // selector verdict — "1" when the joined object's `<a>` is a
            // superset of this object's `<b>` for `join_match = "a:b"`.
            if s.join_paths_len > 0 || s.join_match_len > 0 {
                let sys = &*s.syscalls;
                let mut jv = [0u8; MAX_VALUE];
                let jl = get_value(sys, seg, &mut jv).unwrap_or(0);
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
                        let Some(q) = put_field(out, p, fno, TY_BYTES, got) else {
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
                    let Some(q) = put_field(out, p, 65, TY_BYTES, hit) else {
                        return 0;
                    };
                    p = q;
                    n += 1;
                }
            }
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
    if s.join_scoped == 2 {
        match tail.iter().position(|&b| b == b'/') {
            Some(i) => &tail[..i],
            None => tail,
        }
    } else {
        tail
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
    let mut idx = 0u32;
    while let Some((name, q)) = raw_entry(&lout[..n], rp) {
        rp = q;
        let klen = name.len();
        if klen > MAX_KEY {
            idx += 1;
            continue;
        }
        // Skip what a previous step already delivered from this page.
        if idx < s.page_off {
            idx += 1;
            continue;
        }
        idx += 1;
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
                if !one.is_empty() {
                    let raw = if s.flat != 0 {
                        flat_field(&val[..vlen], one).unwrap_or(&[])
                    } else {
                        let mut segs: [&[u8]; 6] = [&[]; 6];
                        let ns = split_dots(one, &mut segs);
                        j_path(&val[..vlen], &segs[..ns]).unwrap_or(&[])
                    };
                    if !raw.is_empty() {
                        let d = parse_dec_u64(raw) as i64;
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
        let span = if s.expand_len > 0 {
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(&s.expand[..s.expand_len as usize], &mut segs);
            let c = j_path(&val[..vlen], &segs[..ns])
                .map(parse_dec_bytes)
                .unwrap_or(0) as i64;
            let mut hi = c + s.expand_tail as i64;
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
            if (sys.channel_write)(s.record_out, s.frame.as_ptr(), flen) > 0 {
                wrote += 1;
            } else {
                blocked = true;
                break;
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
            s.page_off = idx - 1; // retry THIS entry next step, not the page
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
        s.exp_off = 0;
        s.count = 0;
        s.hash_len = 0;
        s.join_len = 0;
        s.join_scoped = 0;
        s.join_match_len = 0;
        s.join_paths_len = 0;
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
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
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
            if s.paging == 0 && s.dirty == 1 {
                s.dirty = 0;
                s.paging = 1;
                s.clen = 0;
                s.page_off = 0;
                s.exp_off = 0;
                s.list_idx = 0;
                s.total = 0;
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
