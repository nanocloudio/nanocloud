//! Store effect — request/response access to the control-plane store for a
//! Chronicle graph. See `manifest.toml` for the wire and the rationale.

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::convert::TryInto;
use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
// The store fragment, not a local copy. store_effect carried its own
// `get_value` that returned a TRUNCATED value when the object exceeded the
// buffer instead of refusing — the silent-truncation class this repo has been
// bitten by four times, still live in the one module whose whole job is moving
// stored bytes. The shared reader probes one byte past a full buffer and
// REFUSES. `ListWalk` (paged, cursor-following) comes with it, which is what
// OP_LIST needs.
include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");
include!("../_shared/jsonq.rs");
include!("../_shared/moment.rs");
include!("../_shared/tomb.rs");
include!("../_shared/jsonpatch.rs");
include!("../_shared/rbac_cover.rs");
include!("../_shared/labelsel.rs");
include!("../_shared/views.rs");
include!("../_shared/fieldset.rs");
include!("../_shared/docschema.rs");
include!("../_shared/epdoc.rs");

// Chronicle's decision engine, from its published cores — not restated. With a
// `decision` param the connector DECIDES its own request: the record that
// arrives is the decision's input, and the outcome it constructs is the store
// request. A chain step is then one node instead of a decision node feeding a
// connector node — the same semantics, one channel hop fewer, and half the
// module slots, which is what a control plane made of params runs out of
// first. The engine is Chronicle's exactly: same container, same load scan,
// same first-hit driver, same frame codec.
mod dec {
    include!("../../../target/fluxor/chronicle-common/vm_core.rs");
    include!("../../../target/fluxor/chronicle-common/pipeline_core.rs");
    include!("../../../target/fluxor/chronicle-common/decision_core.rs");
    include!("../../../target/fluxor/chronicle-common/blob_core.rs");
}

/// The decision container — sized as Chronicle's own `decision` engine sizes
/// it on a full-arena target.
const DEC_CONT: usize = dec::MAX_CONTAINER_BIN_FULL;
/// `count_where`'s container: a predicate over one child, a few comparisons. Small because some fifty instances carry it;
/// a larger one is refused at load, never truncated.
const CW_CONT: usize = 2048;
/// The field a `wall = 1` node's fused decision reads the wall clock from
/// (Unix ms): outside the projections (40..59) and the carry (30..39).
const WALL_FIELD: u8 = 60;
/// `wall = 2` adds, at this field, the moment now as text a record keeps:
/// Unix ms when the clock is trusted, else `m<mono ms>@<boot>`
/// (`_shared/moment.rs`) — never a 1970 stamp from a clock not yet set.
const MOMENT_FIELD: u8 = 62;

/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const OBJ_PUT: u32 = 0x1420;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const OP_GET: i64 = 1;
const OP_EXISTS: i64 = 2;
const OP_PUT: i64 = 3;
const OP_DELETE: i64 = 4;
/// Set ONE scalar at a dotted path (field 19) inside the object at `key`, from
/// field 4 — an i64 is rendered as decimal digits, bytes are written verbatim
/// — and PUT the result if it changed. The JSON edit is the connector's, as
/// the byte-join is: the VM can compute a number but cannot render one, and
/// cannot open a document to place it. 404 when the object is absent.
const OP_PATCH: i64 = 5;
/// Upsert `k=v` pairs into a `;`-separated flat record: the joined value
/// parts are the pairs to set, an empty value REMOVES that key, and every
/// other key is left as it was. `PUT` replaces a record; a state machine
/// wants to change three fields of seven without restating the four it does
/// not own. Creates the record when absent. Put-if-changed like PUT.
const OP_MERGE: i64 = 6;
/// Do nothing and say so. A decision always produces an outcome, and Chronicle
/// has no conditional fork in the dataflow — so "this record needs no store
/// operation" has to be expressible as an operation. Answering 204 keeps the
/// chain linear instead of demanding a second graph.
const OP_NOOP: i64 = 0;
/// LIST the children under the key, answering with their LAST SEGMENTS joined
/// by `list_sep` (default ","). The request-driven counterpart to
/// `store_source`'s `list_children`: a collection GET needs the names under a
/// prefix, and there is no other way to get them — the VM has no iteration, so
/// walking a prefix is a connector's job by construction.
///
/// 200 with the joined names, including 200 with an EMPTY value for a prefix
/// with no children. An empty collection is not a 404: the collection exists
/// and is empty, and answering 404 tells a client the resource kind is
/// unknown.
const OP_LIST: i64 = 7;
/// Create-only: PUT under the store's `ABSENT` precondition. 200 when this
/// request created the key, 409 when it already existed. The check and the
/// write are ONE store operation — a probe followed by a PUT is the race where
/// two creators both see absence and both win.
const OP_CREATE: i64 = 8;
/// Replace under optimistic concurrency: PUT only if the key is still at the
/// expected revision (field 28, or the body's `rv_path` when field 28 is
/// absent), 409 when it moved. With no expected revision it is PUT. The
/// store's `ETAG` precondition, evaluated at one linearization point.
const OP_REPLACE: i64 = 9;
/// Does ANY of a family of keys exist? The key is built as usual, and every
/// key part whose field is named in the `wild` param may ALSO be `*`: each
/// combination is probed (2^n, n <= 6), and the answer is 200 on the first
/// that exists, 404 when none does. An authorizer asks "is (ns, verb,
/// resource) granted, or (ns, *, resource), or (*, verb, *)…" — a bounded set
/// probe, which is a connector's job, in one store step instead of eight.
/// A part whose value is `~` is a LITERAL that is never widened: a
/// non-resource URL grant (`~|verb|~|/metrics|~`) must not be reached by a
/// resource wildcard, and `*` there would.
const OP_EXISTS_ANY: i64 = 10;
/// Append the joined value parts to the object at `key`, keeping only its last
/// `append_cap` bytes — a bounded tail ring (a workload's output, a pty's). An
/// unbounded append would grow an object without limit.
const OP_APPEND: i64 = 11;
/// `append_tally`'s count beside an APPEND to `key`: `n` more bytes.
unsafe fn append_tally(sys: &Store, tally: &[u8], key: &[u8], n: usize) {
    let Some(c) = tally.iter().position(|&b| b == b':') else {
        return;
    };
    let (from, to) = (
        tally.get(..c).unwrap_or(&[]),
        tally.get(c + 1..).unwrap_or(&[]),
    );
    let Some(rest) = key.strip_prefix(from) else {
        return;
    };
    let mut tk = [0u8; MAX_KEY];
    let tl = to.len() + rest.len();
    let Some(dst) = tk.get_mut(..tl) else {
        return;
    };
    for (d, &b) in dst.iter_mut().zip(to.iter().chain(rest.iter())) {
        *d = b;
    }
    let mut cur = [0u8; 24];
    let had = get_value(sys, &tk[..tl], &mut cur).unwrap_or(0);
    let mut v: u64 = 0;
    for &b in cur.get(..had).unwrap_or(&[]) {
        if b.is_ascii_digit() {
            v = v.saturating_mul(10).saturating_add(u64::from(b - b'0'));
        }
    }
    v = v.saturating_add(n as u64);
    let mut d = [0u8; 20];
    let mut i = d.len();
    loop {
        i -= 1;
        d[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 || i == 0 {
            break;
        }
    }
    let _ = put_value(sys, &tk[..tl], d.get(i..).unwrap_or(&[]));
}

/// Count the keys under the prefix `key` (a bounded walk); the number comes
/// back at field 45 (i64) — a quota's "how many are there".
const OP_COUNT: i64 = 12;
/// A Kubernetes PATCH of the object at the key, value = the patch: RFC 7386
/// merge (13), RFC 6902 JSON Patch (14), strategic merge (15, merging lists on
/// the `patch_keys` schema). Read, patched and written back conditional on the
/// revision read — a lost race reads again, as upstream's GuaranteedUpdate
/// does. 200 patched; 404 no object; 409 the patch's own resourceVersion is
/// stale (or the race never settled); 400 a malformed patch; 422 a JSON Patch
/// `test` that failed, a path that names nothing, or a result that is not the
/// same object; 413 a result past `DOC_MAX`. Needs the heap (store_big).
const OP_PATCH_MERGE: i64 = 13;
const OP_PATCH_JSON: i64 = 14;
const OP_PATCH_SMP: i64 = 15;
/// Server-side apply (with `stamp`): an absent object is CREATED (stamped,
/// answered 201), a present one merged strategically (200).
const OP_APPLY: i64 = 16;
/// Take: answer the object at `key` (200) and delete it in the same step,
/// conditional on the revision it was read at — or 404 when it is absent or
/// another taker moved it first. A single-use claim (a join secret) is one
/// store step, so two callers presenting it cannot both be answered.
const OP_TAKE: i64 = 17;
/// Claim: a take (17) that leaves its claim — answered the same (200 with
/// the value, 404 absent or claimed first), but the record stays, SPENT:
/// every later take or claim answers 404 and a create of the key is refused.
/// A single-use offer its maker would make again once it is gone (it cannot
/// tell spent from never made) is spent for good in one step.
const OP_CLAIM: i64 = 22;
/// One key's records under several collections, gone together: for each name
/// in the value (comma-separated), delete-if-present `<key_prefix><name>/<key
/// parts>`. A Pod's node-side records are keyed by its uid under a dozen
/// collections; when its spec is gone they all go, in one step, and none is
/// left for a chain to read as a Pod that still exists. Answers 200.
const OP_DROP_EACH: i64 = 18;
/// A key and its FAMILY — every `<key>~…` that exists — written alike: PUT
/// (put-if-changed) the value at `<key>` and at each of them. A Pod's
/// containers are one node spec each, `<uid>` and `<uid>~c<n>` /
/// `<uid>~i<n>` (_shared/podres.rs); a Pod deleted outright is known by its
/// uid alone (its tomb's index), and every container it had is told so in
/// one step, by one listing of the uid's prefix, whatever their count. 200
/// when something was written, 204 when nothing changed.
const OP_PUT_FAMILY: i64 = 19;
/// A key and its family gone: delete-if-present every `<key>~…`, then
/// `<key>` (`_shared/tomb.rs` `drop_family`). Answers 200.
const OP_DELETE_FAMILY: i64 = 20;
/// A Service's Endpoints documents (`_shared/epdoc.rs`), the key naming it
/// (`<ns>/<svc>`): the discovery.k8s.io/v1 EndpointSlice and the v1
/// Endpoints rendered from the Service and the endpoints controller's rows
/// (and, for a targetPort that names a port, its Pods' named ports), each
/// written put-if-changed — the slice only when absent or this controller's
/// (its managed-by label), the Endpoints too unless the Service selects its
/// Pods (upstream's controller owns a selector Service's Endpoints). A
/// Service gone, or one with no selector and no rows, has its documents
/// deleted — only ever this controller's. The prefixes are the `ep_docs`
/// param. 200 written, 204 unchanged, 500 a read or write refused, 413 a
/// render past the heap; needs the heap (store_stamped, store_big).
const OP_EP_DOCS: i64 = 21;
/// RBAC escalation prevention (`_shared/rbac_cover.rs`): may the writer
/// (field 3, its identity as the grant index keys it) store this Role,
/// ClusterRole, RoleBinding, ClusterRoleBinding or ManagementBinding (field
/// 72, the resource) — the body the value parts, or a STAGED one by its
/// digest (`blob`) — in namespace 73? Every permission it would grant must
/// already be the writer's in the grant index, or the writer may `escalate`
/// / `bind` it. A patch (75: 13 merge, 14 JSON Patch, 15 strategic, 16 apply;
/// 0 a whole object) is checked on its result: the object at 74 (the
/// request's key, its marks after `#` ignored) patched by the body; 76 the
/// name the path gives. The prefixes are the `cover` param. 200 held, 403
/// not held, 404 the binding's role is not found (both with the Status to
/// answer as the value), 422 a body or patch that is not its kind's, 413
/// past the check's bounds or the heap, 500 a read refused; a read not yet
/// answered holds the record. Reads unjournaled (a role's check reads a
/// grant per permission): needs the heap (store_big).
const OP_COVER: i64 = 23;
/// Request fields `echo` may carry through (see the param).
const ECHO_MAX: usize = 8;

const MAX_KEY: usize = 256;
// Contracts the shared store fragment dispatches on; it takes its opcodes from
// whoever mounts it.
const NS_LIST: u32 = 0x1302;
const MAX_PREFIX: usize = 64;
const MAX_PATHS_SPEC: usize = 192;
/// The largest value read or written in place. Sized for the longest
/// record a node keeps of one image: /image-manifests/<hex> of an image at
/// its layer bound (65 — a Fluxor workload image, its graph and 64 modules):
/// `;layers=` 65 x 65 - 1 = 4224 B of digests, `;sizes=` up to 65 x 11, the
/// config digest and size, and repo, host, registry and pull names — under
/// 5.9 KiB with a 255-byte repo and a 256-byte pull; its /image-gc/ and
/// /image-blob-bad/ rows carry the same list. A reply holds one with its
/// header and carry inside `BUF`. A value past it is refused, never cut.
const MAX_VALUE: usize = 6144;
const BUF: usize = 8192;
/// The largest object read whole onto the heap (store_big): a staged request
/// body is at most 3 MiB.
const DOC_MAX: usize = 3 << 20;
/// The strategic-merge schema a graph may state.
const PATCH_KEYS_MAX: usize = 768;
/// The document schema (`schema`, _shared/docschema.rs).
const DOC_SCHEMA_MAX: usize = 2560;
/// Reads of a patched object before a lost race is answered 409.
const PATCH_TRIES: u32 = 4;

#[repr(C)]
struct State {
    list_sep: [u8; 8],
    list_sep_len: u8,
    /// 0 = LIST answers the children's NAMES; 1 = their VALUES.
    list_values: u32,
    /// Literals wrapped around a LIST answer. Bytes, joined by this module
    /// because the VM cannot concatenate — the ENVELOPE they spell is the
    /// graph's to choose, which is why they are params and not a constant.
    /// `{{tail}}` in either is the listed prefix past its first segment,
    /// its trailing `/` dropped (`/discovery-res/<group>/<version>/` ->
    /// `<group>/<version>`): an APIResourceList states its groupVersion.
    list_open: [u8; LIST_LIT_MAX],
    list_open_len: u8,
    list_close: [u8; LIST_LIT_MAX],
    list_close_len: u8,
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    served: u32,
    malformed: u32,
    /// PUTs skipped because the stored value already matched.
    unchanged: u32,
    key_prefix: [u8; MAX_PREFIX],
    key_prefix_len: u8,
    /// Dotted JSON paths projected out of a GET's value into fields 40..44.
    ///
    /// A decision cannot read `metadata.labels.pod-template-hash` out of the
    /// bytes this returns — the record frame is flat and the VM has no path
    /// reader. `store_source` already projects for exactly that reason when it
    /// LISTS; this is the same capability on the reply side, and without it a
    /// verdict that depends on a field of the fetched object cannot be
    /// expressed at all.
    paths: [u8; MAX_PATHS_SPEC],
    paths_len: u16,
    /// `flat = 1`: the value is `k=v;k=v`, so `paths`/`ints` name fields.
    flat: u32,
    /// Dotted paths projected out of a GET as INTEGERS into fields 45..49
    /// (absent or non-numeric → 0). A decision compares numbers as numbers;
    /// a projected string cannot be converted in the VM.
    ints: [u8; MAX_PATHS_SPEC],
    ints_len: u16,
    /// `rv_path = <a.b>`: the dotted path (two segments) where an object
    /// states its revision — `metadata.resourceVersion`. A GET or a LIST of
    /// values stamps the key's CURRENT store revision there, and a REPLACE
    /// with no explicit revision reads the expected one back from it: the
    /// optimistic-concurrency token a Kubernetes client round-trips.
    rv_path: [u8; 48],
    rv_path_len: u8,
    /// `tomb = <prefix>`: a DELETE first leaves the object's last state at
    /// `<prefix><key>` as `{"deadline":<ms>,"obj":<object>}` for `TOMB_MS` —
    /// what a watch's DELETED event carries (the store's change feed has no
    /// value for a delete). A `ttl` chain expires it.
    tomb: [u8; 48],
    tomb_len: u8,
    /// `tomb_ms`: how long a tomb stays, ms (default `TOMB_MS`) — a Pod's
    /// node specs live as long (tomb.plan).
    tomb_ms: u32,
    /// `ep_docs = "<services>|<rows>|<pods>|<slices>|<endpoints>"`: the
    /// prefixes OP_EP_DOCS reads and writes (`_shared/epdoc.rs`).
    ep_docs: [u8; 192],
    ep_docs_len: u8,
    /// `cover = "<grants>|<roles>|<clusterroles>|<agg>"`: what OP_COVER
    /// reads (`_shared/rbac_cover.rs`).
    cover: [u8; 96],
    cover_len: u8,
    /// OP_EP_DOCS's rows of the Service it serves, kept on the heap across
    /// the serves of ONE held record (`ep_memo_*`, see `ep_read_rows`): the
    /// rows are read outside the record's journal, and a row already read is
    /// not asked again when a later read holds the record. Freed when the
    /// record is answered.
    ep_memo: core::cell::Cell<*mut u8>,
    ep_memo_cap: core::cell::Cell<u32>,
    ep_memo_n: core::cell::Cell<u32>,
    ep_memo_tag: core::cell::Cell<u32>,
    /// The memo holds every row: a later serve of the record reads none.
    ep_memo_whole: core::cell::Cell<bool>,
    /// `blob = <prefix>`: a PUT/CREATE/REPLACE whose value is `sha256:<hex>`
    /// writes the STAGED body at `<prefix><hex>` instead (a request body the
    /// HTTP server staged because it outgrew a record). Needs a heap: the
    /// store_big build.
    blob: [u8; 48],
    blob_len: u8,
    /// `patch_keys`: the strategic-merge schema — `list=key`,
    /// `list@parent=key`, `+list` (see `_shared/jsonpatch.rs`).
    patch_keys: [u8; PATCH_KEYS_MAX],
    patch_keys_len: u16,
    patch_keys_bad: u8,
    /// `schema`: what a create stamps, a replace keeps, a patch may not
    /// change and a delete is held by (_shared/docschema.rs). With `stamp`.
    schema: [u8; DOC_SCHEMA_MAX],
    schema_len: u16,
    schema_bad: u8,
    /// `wild = "18,26"`: the key-part fields OP_EXISTS_ANY may replace by `*`.
    wild: [u8; 6],
    wild_len: u8,
    /// `hash = <path>` (or `.`, the whole value): field 27 on every reply is
    /// the 8-hex-digit FNV-1a of those bytes — the digest `store_source`
    /// stamps with the same param, so a row derived from an object can carry
    /// the object's digest and a later read can tell whether it is stale.
    hash: [u8; 48],
    hash_len: u8,
    /// `revision = 1`: every reply carries field 59, the key's store revision
    /// (0 when absent), so a chain can make a conditional write of its own.
    revision: u32,
    /// `stamp = 1`: writes follow the document `schema` the graph states
    /// (_shared/docschema.rs): a CREATE stamps its `create` paths (and its
    /// `generate` name when the key names no object), a REPLACE carries the
    /// stored object's `keep` paths across, a DELETE is held by a non-empty
    /// `guard` list and marked with `deleting`, and every write answers with
    /// the object as stored when it fits a record. Which paths those are is
    /// the schema's vocabulary, never this module's.
    stamp: u32,
    /// OP_APPEND's ring size (bytes kept).
    append_cap: u32,
    /// `append_tally = "<from>:<to>"`: an APPEND to `<from><rest>` also adds
    /// the bytes it appended to the decimal count at `<to><rest>` — every
    /// byte ever written, which a ring trimmed to its tail no longer says, so
    /// a follower tells new bytes from old (a Pod's log, `logs -f`).
    append_tally: [u8; 96],
    append_tally_len: u8,
    /// `echo = "a,b,…"`: request fields carried into the reply under their own
    /// numbers, like 30..39 — for a chain whose carry range is spent and whose
    /// next stage still needs the request's own fields.
    echo: [u8; ECHO_MAX],
    echo_len: u8,
    /// The fused decision (see `mod dec`): its container, a `blob` param
    /// arriving straight into `dec_cont`, and the outcome frame it
    /// constructs. `dec_len == 0` = no decision; the connector serves the
    /// request as it arrives.
    dec_in: dec::BlobIn,
    dec_cont: [u8; DEC_CONT],
    dec_len: u16,
    dec_buf: [u8; BUF],
    /// A decision param that was given but unusable: the node refuses input
    /// rather than serving requests a broken policy did not make.
    dec_fault: u8,
    /// Records the decision dropped (an empty outcome) or failed on.
    dec_dropped: u32,
    dec_failed: u32,
    /// `count_where`: the decision OP_COUNT runs over each child (see
    /// [`count_child`]), a `blob` param arriving into `cw_cont`. A container
    /// cut short, past its cap, or that does not scan is `cw_fault`, and
    /// every filtered COUNT answers 500.
    cw_in: dec::BlobIn,
    cw_cont: [u8; CW_CONT],
    cw_len: u16,
    cw_fault: u8,
    /// `count_sel = "<selector>|<labels>"`: a COUNT's predicate also sees,
    /// at field 48, "1" when the label selector selects the labels (else
    /// "") — each side either `@<n>` (field n of the request, as the fused
    /// decision built it) or a dotted path into the child: a PodDisruption
    /// Budget's `spec.selector` against a Pod's labels, or a Pod's
    /// `metadata.labels` under a budget's selector. Upstream's reading: an
    /// empty selector selects everything, an absent one nothing.
    count_sel: [u8; 96],
    count_sel_len: u8,
    /// `doc = <n>`: a NOOP's answer projects its `ints` from the request's
    /// own field n — a document the record already carries (a body handed
    /// inline) — where any other op projects from what the store answered
    /// (a body staged in the store, read back by a GET). Its `paths` stay
    /// empty: a name read off an inline body would be one the request's key
    /// already holds.
    doc: u32,
    /// `wall = 1`: the fused decision reads the wall clock at [`WALL_FIELD`];
    /// `wall = 2` also the recordable moment at [`MOMENT_FIELD`].
    wall: u32,
    /// `merge_existing = 1`: a MERGE only ever edits a record that exists — 404
    /// when the key is absent, and conditional on the revision it read (409
    /// when the record moved meanwhile), as a MERGE with field 28 is. A chain
    /// that adds one field to a record another chain owns cannot then bring
    /// back a record that chain removed.
    merge_existing: u32,
    in_buf: [u8; BUF],
    out_buf: [u8; BUF],
    /// A reply the output channel has not taken yet: `out_buf[out_at..out_len]`.
    /// Owed before the next request is read — a reply written once and
    /// dropped when the channel was full lost the middle of a 1 MiB watch
    /// stream (two 2 KiB chunks, silently); a partial write cut a frame.
    out_at: u32,
    out_len: u32,
    /// The store calls of the record in `in_buf` ([`Journal`]). While it
    /// holds the record for a write the store has not decided, the record is
    /// served again each step — the decided request in `dec_buf`, its
    /// lengths here — and answered once its writes are decided.
    journal: Journal,
    held_n: u32,
    held_req: u32,
    /// The held record's TAKE nonce ([`take_value`]): drawn when the record
    /// is first served, kept while it is held.
    take_nonce: [u8; 16],
}

/// Read one field out of a Chronicle v1 record frame:
/// `[count:u8]` then count × `[number:u8][type:u8][len:u16 LE][payload]`.
/// Returns `(type, payload)`.
/// Upsert every `k=v` of `set` into the `;`-separated record `cur`, in place
/// where the key exists and appended in `set` order where it does not; a pair
/// whose value is EMPTY removes the key. Returns the length written to `out`.
///
/// The record grammar is the control plane's, and a merge is what a state
/// machine needs a record for: `kill_phase=1;kill_deadline=…` changes two
/// fields of seven and says nothing about the other five, which a PUT would
/// have to restate and could therefore get wrong.
fn flat_merge(cur: &[u8], set: &[u8], out: &mut [u8]) -> usize {
    let mut o = 0usize;
    // Pass 1: every existing key, updated or dropped.
    let mut start = 0usize;
    while start <= cur.len() {
        let end = cur[start..]
            .iter()
            .position(|&b| b == b';')
            .map(|i| start + i)
            .unwrap_or(cur.len());
        let seg = &cur[start..end];
        if !seg.is_empty() {
            let eq = seg.iter().position(|&b| b == b'=').unwrap_or(seg.len());
            let name = &seg[..eq];
            match flat_field(set, name) {
                Some([]) => {} // removed
                Some(v) => {
                    if o > 0 && o < out.len() {
                        out[o] = b';';
                        o += 1;
                    }
                    o = append_flat(out, o, name, v);
                }
                None => {
                    if o > 0 && o < out.len() {
                        out[o] = b';';
                        o += 1;
                    }
                    let n = seg.len().min(out.len().saturating_sub(o));
                    out[o..o + n].copy_from_slice(&seg[..n]);
                    o += n;
                }
            }
        }
        if end >= cur.len() {
            break;
        }
        start = end + 1;
    }
    // Pass 2: keys `set` introduces, in its order.
    let mut ss = 0usize;
    while ss <= set.len() {
        let se = set[ss..]
            .iter()
            .position(|&b| b == b';')
            .map(|i| ss + i)
            .unwrap_or(set.len());
        let seg = &set[ss..se];
        if !seg.is_empty() {
            let eq = seg.iter().position(|&b| b == b'=').unwrap_or(seg.len());
            let name = &seg[..eq];
            let v = if eq < seg.len() {
                &seg[eq + 1..]
            } else {
                &seg[..0]
            };
            if !v.is_empty() && flat_field(cur, name).is_none() {
                if o > 0 && o < out.len() {
                    out[o] = b';';
                    o += 1;
                }
                o = append_flat(out, o, name, v);
            }
        }
        if se >= set.len() {
            break;
        }
        ss = se + 1;
    }
    o
}

/// The longest `list_open` / `list_close` literal.
const LIST_LIT_MAX: usize = 112;

/// `lit` onto `buf` at `at`, each `{{tail}}` in it written as `key` past
/// its first segment with its trailing `/` dropped (`/a/g/v/` -> `g/v`).
fn append_lit(buf: &mut [u8], mut at: usize, lit: &[u8], key: &[u8]) -> usize {
    const MARK: &[u8] = b"{{tail}}";
    let k = key.strip_prefix(b"/").unwrap_or(key);
    let tail = match k.iter().position(|&b| b == b'/') {
        Some(i) => &k[i + 1..],
        None => &k[..0],
    };
    let tail = tail.strip_suffix(b"/").unwrap_or(tail);
    let mut i = 0usize;
    while i < lit.len() {
        if lit[i..].starts_with(MARK) {
            at = append(buf, at, tail);
            i += MARK.len();
        } else {
            at = append(buf, at, &lit[i..i + 1]);
            i += 1;
        }
    }
    at
}

fn append_flat(out: &mut [u8], at: usize, name: &[u8], v: &[u8]) -> usize {
    let mut o = at;
    let n = name.len().min(out.len().saturating_sub(o));
    out[o..o + n].copy_from_slice(&name[..n]);
    o += n;
    if o < out.len() {
        out[o] = b'=';
        o += 1;
    }
    let n = v.len().min(out.len().saturating_sub(o));
    out[o..o + n].copy_from_slice(&v[..n]);
    o + n
}

/// `k=v;k=v` lookup for the control plane's flat records.
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

/// Split one dotted path into segments. A segment in double quotes is ONE
/// key, dots and all — `metadata.annotations."batch.kubernetes.io/x"`: a
/// Kubernetes label or annotation key is a DNS prefix, and has dots.
fn split_dots<'a>(spec: &'a [u8], segs: &mut [&'a [u8]; 6]) -> usize {
    let mut n = 0usize;
    let mut s2 = 0usize;
    while s2 <= spec.len() && n < segs.len() {
        let quoted = spec.get(s2) == Some(&b'"');
        let (seg, e2) = if quoted {
            let open = s2 + 1;
            let close = spec
                .get(open..)
                .and_then(|r| r.iter().position(|&b| b == b'"'))
                .map(|i| open + i)
                .unwrap_or(spec.len());
            (
                spec.get(open..close).unwrap_or(&[]),
                (close + 1).min(spec.len()),
            )
        } else {
            let e2 = spec[s2..]
                .iter()
                .position(|&b| b == b'.')
                .map(|i| s2 + i)
                .unwrap_or(spec.len());
            (&spec[s2..e2], e2)
        };
        segs[n] = seg;
        n += 1;
        if e2 >= spec.len() {
            break;
        }
        s2 = e2 + 1;
    }
    n
}

/// Decimal bytes → u64 (saturating); stops at the first non-digit.
fn parse_dec_u64(b: &[u8]) -> u64 {
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add(u64::from(c - b'0'));
    }
    v
}

/// Decimal bytes → u32; stops at the first non-digit.
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

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    if frame.is_empty() {
        return None;
    }
    let count = frame[0] as usize;
    let mut p = 1usize;
    for _ in 0..count {
        if p + 4 > frame.len() {
            return None;
        }
        let num = frame[p];
        let ty = frame[p + 1];
        let len = u16::from_le_bytes(frame[p + 2..p + 4].try_into().unwrap()) as usize;
        if p + 4 + len > frame.len() {
            return None;
        }
        if num == number {
            return Some((ty, &frame[p + 4..p + 4 + len]));
        }
        p += 4 + len;
    }
    None
}

fn frame_i64(frame: &[u8], number: u8) -> Option<i64> {
    let (ty, v) = frame_field(frame, number)?;
    if ty != TY_I64 || v.len() != 8 {
        return None;
    }
    Some(i64::from_le_bytes(v.try_into().ok()?))
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

/// What a reply projects out of the value it answers with: the `,`-separated
/// paths emitted as bytes (fields 40..), the paths emitted as integers (50..),
/// and whether the value is the flat `k=v;k=v` form rather than JSON.
///
/// One parameter rather than three, because the three always travel together:
/// they are read from the module's params once and every reply carries the
/// same spec, whatever branch produced it.
#[derive(Clone, Copy)]
struct Projection<'a> {
    paths: &'a [u8],
    ints: &'a [u8],
    flat: bool,
    hash: &'a [u8],
}

/// `[cid][status][value]` plus any CARRY-THROUGH the request supplied, echoed
/// verbatim in fields 9..=12.
///
/// This is what lets a linear graph do read-then-decide. Chronicle's effect
/// call/join — which would retain the pre-effect record for a later stage — is
/// another repository's item and not built; without it, the reply is all a
/// downstream decision sees, so the request's own context would be lost across
/// the effect. Carry-through hands that context to the connector and gets it
/// back untouched. It is deliberately narrow: bounded, opaque, and only across
/// ONE effect. It is not a substitute for effect-join in general.
fn reply_projected(
    out: &mut [u8],
    cid: i64,
    status: i64,
    value: &[u8],
    carry: &[(u8, u8, &[u8])],
    proj: &Projection,
) -> usize {
    reply_projected_from(out, cid, status, value, value, carry, proj)
}

/// One EMPTY field per `paths` entry, at 40.., appended to the reply of
/// `n` bytes in `out`: the layout a projection keeps when it has nothing to
/// read. The new length (`n` when a field does not fit).
fn reply_paths_empty(out: &mut [u8], n: usize, paths: &[u8]) -> usize {
    let mut p = n;
    let mut fno = 40u8;
    for one in paths.split(|&b| b == b',') {
        if one.is_empty() || fno >= 50 {
            continue;
        }
        match put_field(out, p, fno, TY_BYTES, &[]) {
            Some(q) => {
                p = q;
                if let Some(c) = out.get_mut(0) {
                    *c = c.wrapping_add(1);
                }
            }
            None => return p,
        }
        fno += 1;
    }
    p
}

/// [`reply_projected`] whose projections are read from `value` while the
/// reply answers `body` — an object past a record is answered with no value
/// and still projected, from a copy the reply could never carry.
fn reply_projected_from(
    out: &mut [u8],
    cid: i64,
    status: i64,
    body: &[u8],
    value: &[u8],
    carry: &[(u8, u8, &[u8])],
    proj: &Projection,
) -> usize {
    let Projection {
        paths,
        ints,
        flat,
        hash,
    } = *proj;
    let n0 = reply(out, cid, status, body, carry);
    if n0 == 0 || (paths.is_empty() && ints.is_empty() && hash.is_empty()) {
        return n0;
    }
    let mut n0 = n0;
    if !hash.is_empty() {
        // Field 27, on a miss too (the digest of nothing): a decision comparing
        // digests must never find the field missing.
        let bytes: &[u8] = if hash == b"." {
            value
        } else {
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(hash, &mut segs);
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
        match put_field(out, n0, 27, TY_BYTES, &h) {
            Some(q) => {
                out[0] += 1;
                n0 = q;
            }
            None => return 0,
        }
    }
    // `value` may be empty (a miss); `j_path` then yields nothing and every
    // projected field is emitted empty, which is exactly what keeps the layout
    // stable across hit and miss.
    // Append one field per configured path, at 40.., reading from the value we
    // just returned. Absent resolves to empty so the field numbers stay stable.
    let mut p = n0;
    let mut fno = 40u8;
    let mut start = 0usize;
    while start <= paths.len() && fno < 50 {
        let end = paths[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(paths.len());
        let one = &paths[start..end];
        if !one.is_empty() {
            // A path may name a SUBTREE (`spec`) as well as a scalar — the
            // same rule store_source applies, so a projection reads the same
            // bytes on the way back that it read on the way in. `j_path`
            // reads scalars and would hand back a subtree cut at its first
            // comma: `{"replicas":5` — which is what a rollout archived once.
            // `a[k=v].b` SELECTS (and `a[].b` collects) as store_source's
            // `paths` do (`_shared/jsonq.rs`), the modifier applied to the
            // element: a Pod read for its readiness asks for
            // `status.conditions[type=Ready].status`, a Node for its
            // `status.addresses[type=InternalIP].address`.
            #[allow(
                clippy::manual_contains,
                reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
            )]
            let query = !flat && one.iter().any(|&c| c == b'[');
            let whole = one;
            let (one, modifier) = split_mod(one);
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(one, &mut segs);
            let mut modbuf = [0u8; 512];
            let got: &[u8] = if query {
                match jq(value, whole, &mut modbuf) {
                    Some(n) => modbuf.get(..n).unwrap_or(&[]),
                    // Too long is refused, never cut: the one entry nothing
                    // names.
                    None => b"-",
                }
            } else {
                let got = if flat {
                    flat_field(value, one).unwrap_or(&[])
                } else {
                    j_sub(value, &segs[..ns])
                        .or_else(|| j_path(value, &segs[..ns]))
                        .unwrap_or(&[])
                };
                match modifier {
                    Some(m) => {
                        let n = apply_mod(m, got, &mut modbuf);
                        &modbuf[..n]
                    }
                    None => got,
                }
            };
            let Some(q) = put_field(out, p, fno, TY_BYTES, got) else {
                return n0;
            };
            p = q;
            fno += 1;
            out[0] += 1;
        }
        if end >= paths.len() {
            break;
        }
        start = end + 1;
    }
    // Integer projections, 50..58: the same lookup, parsed. Absent → 0, and
    // still emitted, for the same reason the byte projections are.
    let mut fno = 50u8;
    let mut start = 0usize;
    while start <= ints.len() && fno < 59 {
        let end = ints[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(ints.len());
        let one = &ints[start..end];
        if !one.is_empty() {
            // `.` is the whole value and `<path>!<mod>` decodes first, as in
            // store_source: `.!ip4` reads a raw lease address as a number.
            let (one, modifier) = split_mod(one);
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(one, &mut segs);
            let raw = if one == b"." {
                value
            } else if flat {
                flat_field(value, one).unwrap_or(&[])
            } else if modifier == Some(b"size") {
                // `!size` measures an object, so it reads the subtree.
                j_sub(value, &segs[..ns]).unwrap_or(&[])
            } else {
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
            // u64, not u32: a projection may be a time in milliseconds.
            let v = parse_dec_u64(raw).min(i64::MAX as u64) as i64;
            let Some(q) = put_field(out, p, fno, TY_I64, &v.to_le_bytes()) else {
                return n0;
            };
            p = q;
            fno += 1;
            out[0] += 1;
        }
        if end >= ints.len() {
            break;
        }
        start = end + 1;
    }
    p
}

fn reply(out: &mut [u8], cid: i64, status: i64, value: &[u8], carry: &[(u8, u8, &[u8])]) -> usize {
    let mut p = 1usize;
    let Some(q) = put_field(out, p, 1, TY_I64, &cid.to_le_bytes()) else {
        return 0;
    };
    p = q;
    let Some(q) = put_field(out, p, 2, TY_I64, &status.to_le_bytes()) else {
        return 0;
    };
    p = q;
    let Some(q) = put_field(out, p, 3, TY_BYTES, value) else {
        return 0;
    };
    p = q;
    let mut n = 3u8;
    for (num, ty, payload) in carry {
        let Some(q) = put_field(out, p, *num, *ty, payload) else {
            return 0;
        };
        p = q;
        n += 1;
    }
    out[0] = n;
    p
}

/// PUT, guarded: a write whose value already matches is skipped, so a converged
/// cluster spends no revisions and a graph writing under a prefix it also
/// watches does not wake itself forever. Every nanocloud reconciler holds this
/// invariant and a generic connector must not quietly drop it.
/// Returns (ok, wrote).
unsafe fn put_guarded(sys: &Store, key: &[u8], value: &[u8]) -> (bool, bool) {
    let mut cur = [0u8; MAX_VALUE];
    if let Some(n) = get_value(sys, key, &mut cur) {
        if &cur[..n] == value {
            return (true, false);
        }
    } else if value.len() > cur.len() {
        // A value past the stack (a spliced template) is compared on the
        // heap: rewriting an unchanged object would spend a revision, wake
        // every watch on it, and loop a controller that writes it.
        if let Some((p, n)) = read_whole(sys, key, value.len() + 1) {
            let same = core::slice::from_raw_parts(p, n) == value;
            heap_free(sys, p);
            if same {
                return (true, false);
            }
        }
    }
    (done(put_value(sys, key, value)), true)
}

/// SPLICE (request fields 70 and 71): a write whose value carries subtrees
/// copied from a STORED document rather than through the record. A
/// controller's object is mostly its owner's template — a Deployment's spec,
/// a ReplicaSet's pod template — and a template is as large as its user
/// makes it: carried through a decision it overflows the 512 B scratch, past
/// a record it cannot cross at all. So the decision names WHERE the bytes
/// are, and this connector copies them, whatever their size.
///   70  the source key's tail (bytes)
///   71  `<prefix>|<at>=<path>[;<at>=<path>…]`: the source is `<prefix><70>`;
///       each `<path>`'s subtree goes in before value part `<at>` (a value
///       part field number; 0 = after the last). `<path>.*` is an object's
///       MEMBERS (braces stripped, empty for none); a leading `,` writes a
///       comma before what it splices when there is anything to splice — a
///       member list joined after a literal member. A whole subtree that is
///       not there splices `null`.
/// The value is assembled on the heap (store_stamped, store_big): a build
/// without one answers 413. A source that is gone answers 404 and writes
/// nothing; one that could not be read now holds the record.
const F_SPLICE_SRC: u8 = 70;
const F_SPLICE: u8 = 71;
/// Subtrees one write may splice.
const SPLICE_MAX: usize = 6;

/// A value part rendered: bytes as they are, an integer as decimal.
fn part_bytes<'a>(req: &'a [u8], f: u8, digits: &'a mut [u8; 21]) -> &'a [u8] {
    match frame_field(req, f) {
        Some((TY_BYTES, part)) => part,
        Some((TY_I64, b)) if b.len() == 8 => {
            let v = i64::from_le_bytes(b.try_into().unwrap_or([0; 8]));
            let mut d = [0u8; 20];
            let n = dec(v.unsigned_abs(), &mut d);
            let mut at = 0usize;
            if v < 0 {
                digits[0] = b'-';
                at = 1;
            }
            for (dst, &c) in digits.iter_mut().skip(at).zip(d.iter().take(n)) {
                *dst = c;
            }
            digits.get(..at + n).unwrap_or(&[])
        }
        _ => &[],
    }
}

/// An object's members, or an array's elements, braces (brackets) and
/// surrounding whitespace stripped: what a decision splices more members or
/// elements in beside, with literals (a ClusterRole's rules: its own and the
/// ones a ManagementBinding states, management.plan).
fn splice_members(sub: &[u8]) -> &[u8] {
    let t = sub.trim_ascii();
    match (t.first(), t.last()) {
        (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']')) if t.len() >= 2 => {
            t.get(1..t.len() - 1).unwrap_or(&[]).trim_ascii()
        }
        _ => &[],
    }
}

/// The value of a splicing write, assembled on the heap: `Ok((heap, len))`,
/// or the status to answer instead (the source is read whole through the
/// journal, so a held write splices the same bytes when asked again).
unsafe fn splice_value(sys: &Store, req: &[u8], spec: &[u8]) -> Result<(*mut u8, usize), i64> {
    let bar = spec.iter().position(|&b| b == b'|').ok_or(400i64)?;
    let prefix = spec.get(..bar).unwrap_or(&[]);
    let items = spec.get(bar + 1..).unwrap_or(&[]);
    let tail = match frame_field(req, F_SPLICE_SRC) {
        Some((TY_BYTES, t)) if !t.is_empty() => t,
        _ => return Err(400),
    };
    let mut kb = [0u8; MAX_KEY];
    let kl = prefix.len() + tail.len();
    let Some(dst) = kb.get_mut(..kl) else {
        return Err(400);
    };
    for (d, &b) in dst.iter_mut().zip(prefix.iter().chain(tail.iter())) {
        *d = b;
    }
    let src = &kb[..kl];
    // Read LIVE, outside the journal. What a held write's replay must
    // reproduce is the write itself — the journal checks its value's length
    // and digest — not the whole source: a controller's object moves while
    // its Pod's create is held (a Job's status, patched as the Job starts),
    // and recorded, that move broke the journal (DIVERGED, the write never
    // asked again, the node's writes wedged behind it). A source whose
    // spliced subtrees did not move splices the same bytes; one whose
    // template moved builds another value, which the journal refuses.
    if sys.refusal().is_some() {
        return Err(500);
    }
    if sys.silent() {
        return Err(503);
    }
    let (dp, dn) = match read_whole_rev_live(sys.table(), sys.nag(), src, DOC_MAX) {
        Got::Found((p, n, _)) => (p, n),
        // Not answered yet: the record is held and served again.
        Got::Pending => {
            sys.wait();
            return Err(503);
        }
        // Refused for good: the serve fails, never held for it.
        Got::Refused(rc) => {
            sys.refuse(rc);
            return Err(500);
        }
        // Gone, or past the heap.
        Got::Absent => return Err(if exists(sys, src) { 413 } else { 404 }),
    };
    let doc = core::slice::from_raw_parts(dp, dn);
    // (at, comma, bytes) for each item.
    let mut cuts: [(u8, bool, &[u8]); SPLICE_MAX] = [(0, false, &[]); SPLICE_MAX];
    let mut nc = 0usize;
    for item in items.split(|&b| b == b';') {
        let Some(eq) = item.iter().position(|&b| b == b'=') else {
            continue;
        };
        let mut at = 0u32;
        for &c in item.get(..eq).unwrap_or(&[]) {
            if c.is_ascii_digit() {
                at = at * 10 + u32::from(c - b'0');
            }
        }
        let mut path = item.get(eq + 1..).unwrap_or(&[]);
        let comma = path.first() == Some(&b',');
        if comma {
            path = path.get(1..).unwrap_or(&[]);
        }
        let members = path.ends_with(b".*");
        if members {
            path = path.get(..path.len() - 2).unwrap_or(&[]);
        }
        let mut segs: [&[u8]; 6] = [&[]; 6];
        let ns = split_dots(path, &mut segs);
        let sub = j_sub(doc, &segs[..ns]).or_else(|| j_path(doc, &segs[..ns]));
        let bytes: &[u8] = match (sub, members) {
            (Some(s), true) => splice_members(s),
            (None, true) => &[],
            (Some(s), false) => s,
            (None, false) => b"null",
        };
        if let Some(c) = cuts.get_mut(nc) {
            *c = (at.min(255) as u8, comma && !bytes.is_empty(), bytes);
            nc += 1;
        }
    }
    let cuts = &cuts[..nc];
    let fields = (4u8..=16).chain(80u8..=99);
    let mut total = 0usize;
    for f in fields.clone().chain(core::iter::once(0u8)) {
        for &(_, comma, b) in cuts.iter().filter(|c| c.0 == f) {
            total += usize::from(comma) + b.len();
        }
        if f != 0 {
            let mut d = [0u8; 21];
            total += part_bytes(req, f, &mut d).len();
        }
    }
    let p = heap_alloc(sys.table(), total.max(1) as u32);
    if p.is_null() {
        heap_free(sys.table(), dp);
        return Err(413);
    }
    let mut o = 0usize;
    let put = |src: &[u8], o: &mut usize| {
        if *o + src.len() <= total {
            core::ptr::copy_nonoverlapping(src.as_ptr(), p.add(*o), src.len());
            *o += src.len();
        }
    };
    for f in fields.chain(core::iter::once(0u8)) {
        for &(_, comma, b) in cuts.iter().filter(|c| c.0 == f) {
            if comma {
                put(b",", &mut o);
            }
            put(b, &mut o);
        }
        if f != 0 {
            let mut d = [0u8; 21];
            put(part_bytes(req, f, &mut d), &mut o);
        }
    }
    heap_free(sys.table(), dp);
    Ok((p, o))
}

/// PATCH (op 5) of an object past a record: read whole and edited on the
/// heap, written put-if-changed. 413 without a heap (or past it), 400 when
/// the path names nothing settable, else the write's status.
unsafe fn patch_whole(sys: &Store, key: &[u8], segs: &[&[u8]], val: &[u8]) -> i64 {
    let Some((cp, cn)) = read_whole(sys, key, DOC_MAX) else {
        return if sys.waiting() { 503 } else { 413 };
    };
    let cur = core::slice::from_raw_parts(cp, cn);
    let room = cn + val.len() + segs.iter().map(|g| g.len()).sum::<usize>() + 16;
    let wp = heap_alloc(sys.table(), room as u32);
    if wp.is_null() {
        heap_free(sys.table(), cp);
        return 413;
    }
    let work = core::slice::from_raw_parts_mut(wp, room);
    let el = match segs {
        [a] => j_set_top(cur, a, val, work),
        [a, b] => j_set2(cur, a, b, val, work),
        _ => 0,
    };
    let status = if el == 0 {
        400
    } else {
        match put_guarded(sys, key, work.get(..el).unwrap_or(&[])) {
            (false, _) => 500,
            (true, true) => 200,
            (true, false) => 204,
        }
    };
    heap_free(sys.table(), wp);
    heap_free(sys.table(), cp);
    status
}

/// One shard's rows, as OP_EP_DOCS hands a slice render them (on the heap):
/// EPD_SHARD rows of at most EP_REC bytes.
const EP_SHARD_BUF: usize = EPD_SHARD * EP_REC;
/// One row as the walk builds it: the Pod's name, its row, its named ports
/// and its other address, separated.
const EP_REC: usize = MAX_KEY + 512 + 512 + 64 + 4;
/// The most the v1 Endpoints' rows (EPD_EP_MAX of them) take on the heap; a
/// buffer grows to it from EP_EP_BUF0.
const EP_EP_BUF_MAX: usize = 1 << 20;
const EP_EP_BUF0: usize = 64 << 10;
/// A Service's rows read once into the heap, up to this; past it every pass
/// walks the store again instead.
const EP_ALL_BUF_MAX: usize = 1 << 20;

/// `<a><b><c>` into a key buffer; None past MAX_KEY.
fn ep_key(parts: &[&[u8]], out: &mut [u8; MAX_KEY]) -> Option<usize> {
    let mut n = 0usize;
    for p in parts {
        out.get_mut(n..n + p.len())?.copy_from_slice(p);
        n += p.len();
    }
    Some(n)
}

/// One of a Service's documents decided: written (rendered by `render`) when
/// `manage` and this controller may write it, deleted when it is this
/// controller's and nothing is managed. Answers (ok, wrote).
#[allow(
    clippy::too_many_arguments,
    reason = "one document's facts, each read by the caller"
)]
unsafe fn ep_doc_write(
    s: &State,
    sys: &Store,
    wr: &Store,
    key: &[u8],
    manage: bool,
    owns_any: bool,
    label: &[u8],
    manager: &[u8],
    render: impl FnOnce(&[u8], &[u8], &mut [u8]) -> Option<usize>,
    cap: usize,
) -> (bool, bool) {
    // Through the journal: a held write's serve must see what it first read.
    let cur = read_whole(wr, key, DOC_MAX);
    if wr.waiting() {
        if let Some((p, _)) = cur {
            heap_free(sys.table(), p);
        }
        return (false, false);
    }
    let doc: &[u8] = match cur {
        Some((p, n)) => core::slice::from_raw_parts(p, n),
        None => &[],
    };
    let ours = !doc.is_empty() && epd_managed(doc, label, manager);
    let result = if !manage {
        if ours {
            leave_tomb(s, wr, key, doc);
            (done(delete_value(wr, key)), true)
        } else {
            (true, false)
        }
    } else if !doc.is_empty() && !ours && !owns_any {
        // Someone else's: left alone.
        (true, false)
    } else {
        // Identity: kept from this controller's object, else drawn now.
        let mut uid = [0u8; 36];
        let mut ts = [0u8; 20];
        let (u, t): (&[u8], &[u8]) = match (
            ours.then(|| epd_str2(doc, b"metadata", b"uid")).flatten(),
            ours.then(|| epd_str2(doc, b"metadata", b"creationTimestamp"))
                .flatten(),
        ) {
            (Some(u), Some(t)) => (u, t),
            _ => {
                let mut r = [0u8; 16];
                rand_fill(wr, &mut r);
                epd_uuid(&r, &mut uid);
                rfc3339(now_millis(wr), &mut ts);
                (&uid[..], &ts[..])
            }
        };
        let wp = heap_alloc(sys.table(), cap as u32);
        if wp.is_null() {
            let m = b"[store_effect] EP_DOCS: no heap for the render - REFUSED";
            dev_log(sys.table(), 1, m.as_ptr(), m.len());
            (false, false)
        } else {
            let work = core::slice::from_raw_parts_mut(wp, cap);
            let r = match render(u, t, work) {
                // Put-if-changed against the document read whole above:
                // put_guarded's own read is a record's buffer, and a slice
                // or Endpoints past it would be REFUSED there — failing the
                // pass rather than comparing.
                Some(n) if work[..n] == *doc => (true, false),
                Some(n) => (done(put_value(wr, key, &work[..n])), true),
                None => {
                    let m = b"[store_effect] EP_DOCS: a render past its room - REFUSED";
                    dev_log(sys.table(), 1, m.as_ptr(), m.len());
                    (false, false)
                }
            };
            heap_free(sys.table(), wp);
            r
        }
    };
    if let Some((p, _)) = cur {
        heap_free(sys.table(), p);
    }
    result
}

/// Walk a Service's rows (`<rows_pre><ns>/<svc>/<pod>`), each built into
/// one record (`_shared/epdoc.rs` rows: pod, row, named ports when `named`,
/// the Pod's other address when `dual`) and handed to `f`, which answers
/// whether to go on. Err(503) when a read is not answered yet, Err(500)
/// when the walk fails.
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
#[allow(
    clippy::too_many_arguments,
    reason = "where the rows and Pods are, and what to read of them"
)]
unsafe fn ep_walk(
    sys: &Store,
    rows_pre: &[u8],
    pods: &[u8],
    ns: &[u8],
    tail: &[u8],
    named: bool,
    dual: bool,
    mut f: impl FnMut(&[u8]) -> bool,
) -> Result<(), i64> {
    let mut rk = [0u8; MAX_KEY];
    let rkl = ep_key(&[rows_pre, tail, b"/"], &mut rk).ok_or(400i64)?;
    let mut walk = ListWalk::new(&rk[..rkl]);
    let mut kb = [0u8; MAX_KEY];
    let mut rec = [0u8; EP_REC];
    while let Some(one) = walk.next(sys) {
        let kl = one.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&one[..kl]);
        let pod = kb.get(rkl..kl).unwrap_or(&[]);
        if pod.is_empty() || pod.iter().any(|&c| c == b'/') {
            continue;
        }
        let mut v = [0u8; 512];
        let Some(vl) = get_value(sys, &kb[..kl], &mut v) else {
            continue;
        };
        let mut pp = [0u8; 512];
        let mut ppl = 0usize;
        let mut alt = [0u8; 64];
        let mut altl = 0usize;
        if named || dual {
            let mut pk = [0u8; MAX_KEY];
            if let Some(pkl) = ep_key(&[pods, ns, b"/", pod], &mut pk) {
                if let Some((p, n)) = read_whole(sys, &pk[..pkl], DOC_MAX) {
                    let doc = core::slice::from_raw_parts(p, n);
                    if named {
                        ppl = epd_pod_ports(doc, &mut pp);
                    }
                    if dual {
                        altl = epd_pod_other_ip(doc, epd_flat(&v[..vl], b"ip"), &mut alt);
                    }
                    heap_free(sys.table(), p);
                }
            }
        }
        let mut n = 0usize;
        for piece in [
            pod,
            &[EPD_US][..],
            &v[..vl],
            &[EPD_US][..],
            &pp[..ppl],
            &[EPD_US][..],
            &alt[..altl],
            &[EPD_RS][..],
        ] {
            let Some(d) = rec.get_mut(n..n + piece.len()) else {
                return Err(500);
            };
            d.copy_from_slice(piece);
            n += piece.len();
        }
        if !f(&rec[..n]) {
            break;
        }
    }
    if sys.waiting() {
        return Err(503);
    }
    if walk.failed {
        return Err(500);
    }
    Ok(())
}

unsafe fn ep_memo_clear(s: &State, sys: &Store) {
    let p = s.ep_memo.get();
    if !p.is_null() {
        heap_free(sys.table(), p);
    }
    s.ep_memo.set(core::ptr::null_mut());
    s.ep_memo_cap.set(0);
    s.ep_memo_n.set(0);
    s.ep_memo_tag.set(0);
    s.ep_memo_whole.set(false);
}

/// Pass 1's rows (`ep_walk`), each one already read in an earlier serve of
/// this record taken from the memo instead of asked again: a Service's rows
/// are read outside the record's journal (they outnumber the calls it
/// holds), so without it a serve held by any one read would ask every row
/// again, and a Service of many rows might never be read whole. The rows
/// are handed to `f` in key order; the memo becomes this serve's rows.
#[allow(
    clippy::too_many_arguments,
    reason = "where the rows and Pods are, and what to read of them"
)]
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
unsafe fn ep_read_rows(
    s: &State,
    sys: &Store,
    rows_pre: &[u8],
    pods: &[u8],
    ns: &[u8],
    tail: &[u8],
    named: bool,
    dual: bool,
    mut f: impl FnMut(&[u8]) -> bool,
) -> Result<(), i64> {
    let tag = key_hash(tail) | 1;
    if s.ep_memo_tag.get() != tag {
        ep_memo_clear(s, sys);
        s.ep_memo_tag.set(tag);
    }
    let old_p = s.ep_memo.get();
    let old_n = s.ep_memo_n.get() as usize;
    let old: &[u8] = if old_p.is_null() {
        &[]
    } else {
        core::slice::from_raw_parts(old_p, old_n)
    };
    // Read whole by an earlier serve of this record: those rows ARE the
    // rows. A serve held for a write asks it again as first asked, so it
    // must render what the first serve rendered — rows arrived since are
    // the next record's.
    if s.ep_memo_whole.get() {
        let mut at = 0usize;
        while let Some((_, _, _, _, next)) = epd_row(old, at) {
            if !f(&old[at..next]) {
                break;
            }
            at = next;
        }
        return Ok(());
    }
    let Some(mut fresh) = EpBuf::new(sys, old_n.max(EP_EP_BUF0), EP_ALL_BUF_MAX) else {
        return Err(413);
    };
    let mut rk = [0u8; MAX_KEY];
    let rkl = ep_key(&[rows_pre, tail, b"/"], &mut rk).ok_or(400i64)?;
    let mut walk = ListWalk::new(&rk[..rkl]);
    let mut kb = [0u8; MAX_KEY];
    let mut rec = [0u8; EP_REC];
    let mut mi = 0usize;
    let mut stop = false;
    let mut over = false;
    while let Some(one) = walk.next(sys) {
        let kl = one.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&one[..kl]);
        let pod = kb.get(rkl..kl).unwrap_or(&[]);
        if pod.is_empty() || pod.iter().any(|&c| c == b'/') {
            continue;
        }
        // The memo is in key order too: skip rows since gone, take this
        // one's when it was read.
        let mut hit: Option<&[u8]> = None;
        while let Some((mpod, _, _, _, next)) = epd_row(old, mi) {
            if mpod < pod {
                mi = next;
                continue;
            }
            if mpod == pod {
                hit = Some(&old[mi..next]);
                mi = next;
            }
            break;
        }
        let r: &[u8] = match hit {
            Some(h) => h,
            None => match ep_build_rec(sys, &kb[..kl], pod, pods, ns, named, dual, &mut rec) {
                Some(n) => &rec[..n],
                None => {
                    if sys.waiting() || sys.refusal().is_some() {
                        break;
                    }
                    continue;
                }
            },
        };
        if !over && !fresh.push(sys, r) {
            over = true;
        }
        if !f(r) {
            stop = true;
            break;
        }
    }
    // What this serve read is the memo now (the rows past a "not yet" are
    // asked in the next serve).
    if !old_p.is_null() {
        heap_free(sys.table(), old_p);
    }
    if over {
        fresh.free(sys);
        s.ep_memo.set(core::ptr::null_mut());
        s.ep_memo_n.set(0);
    } else {
        s.ep_memo.set(fresh.p);
        s.ep_memo_cap.set(fresh.cap as u32);
        s.ep_memo_n.set(fresh.n as u32);
    }
    if sys.waiting() {
        return Err(503);
    }
    if walk.failed || sys.refusal().is_some() {
        return Err(500);
    }
    // Every row read (not a walk the caller stopped short): kept as the
    // record's rows unless they outgrew the memo.
    s.ep_memo_whole.set(!stop && !over);
    Ok(())
}

/// One row's record (`_shared/epdoc.rs` rows) into `rec`: its length, or
/// None when the row is gone or a read did not answer.
#[allow(
    clippy::too_many_arguments,
    reason = "the row, and where its Pod is and what to read of it"
)]
unsafe fn ep_build_rec(
    sys: &Store,
    key: &[u8],
    pod: &[u8],
    pods: &[u8],
    ns: &[u8],
    named: bool,
    dual: bool,
    rec: &mut [u8; EP_REC],
) -> Option<usize> {
    let mut v = [0u8; 512];
    let vl = get_value(sys, key, &mut v)?;
    let mut pp = [0u8; 512];
    let mut ppl = 0usize;
    let mut alt = [0u8; 64];
    let mut altl = 0usize;
    if named || dual {
        let mut pk = [0u8; MAX_KEY];
        if let Some(pkl) = ep_key(&[pods, ns, b"/", pod], &mut pk) {
            match read_whole(sys, &pk[..pkl], DOC_MAX) {
                Some((p, n)) => {
                    let doc = core::slice::from_raw_parts(p, n);
                    if named {
                        ppl = epd_pod_ports(doc, &mut pp);
                    }
                    if dual {
                        altl = epd_pod_other_ip(doc, epd_flat(&v[..vl], b"ip"), &mut alt);
                    }
                    heap_free(sys.table(), p);
                }
                None if sys.waiting() || sys.refusal().is_some() => return None,
                None => {}
            }
        }
    }
    let mut n = 0usize;
    for piece in [
        pod,
        &[EPD_US][..],
        &v[..vl],
        &[EPD_US][..],
        &pp[..ppl],
        &[EPD_US][..],
        &alt[..altl],
        &[EPD_RS][..],
    ] {
        let d = rec.get_mut(n..n + piece.len())?;
        d.copy_from_slice(piece);
        n += piece.len();
    }
    Some(n)
}

/// A Service's rows, from `mem` (read once, whole) when there, else walked
/// from the store again (`ep_walk`) — the second for a Service whose rows
/// outgrow the buffer, so no number of endpoints is refused.
#[allow(
    clippy::too_many_arguments,
    reason = "where the rows and Pods are, and what to read of them"
)]
unsafe fn ep_rows(
    sys: &Store,
    mem: Option<&[u8]>,
    rows_pre: &[u8],
    pods: &[u8],
    ns: &[u8],
    tail: &[u8],
    named: bool,
    dual: bool,
    mut f: impl FnMut(&[u8]) -> bool,
) -> Result<(), i64> {
    match mem {
        Some(all) => {
            let mut at = 0usize;
            while let Some((_, _, _, _, next)) = epd_row(all, at) {
                let rec = &all[at..next];
                at = next;
                if !f(rec) {
                    break;
                }
            }
            Ok(())
        }
        None => ep_walk(sys, rows_pre, pods, ns, tail, named, dual, f),
    }
}

/// A heap buffer of rows that grows (doubling, up to `max`).
struct EpBuf {
    p: *mut u8,
    cap: usize,
    n: usize,
    max: usize,
}

impl EpBuf {
    unsafe fn new(sys: &Store, cap: usize, max: usize) -> Option<EpBuf> {
        let p = heap_alloc(sys.table(), cap as u32);
        (!p.is_null()).then_some(EpBuf { p, cap, n: 0, max })
    }
    /// Append `rec`; false when it would pass `max` or the heap.
    unsafe fn push(&mut self, sys: &Store, rec: &[u8]) -> bool {
        if self.n + rec.len() > self.cap {
            let mut cap = self.cap;
            while cap < self.n + rec.len() {
                cap *= 2;
            }
            if cap > self.max {
                return false;
            }
            let q = heap_alloc(sys.table(), cap as u32);
            if q.is_null() {
                return false;
            }
            core::ptr::copy_nonoverlapping(self.p, q, self.n);
            heap_free(sys.table(), self.p);
            self.p = q;
            self.cap = cap;
        }
        core::ptr::copy_nonoverlapping(rec.as_ptr(), self.p.add(self.n), rec.len());
        self.n += rec.len();
        true
    }
    unsafe fn bytes(&self) -> &[u8] {
        core::slice::from_raw_parts(self.p, self.n)
    }
    unsafe fn free(self, sys: &Store) {
        heap_free(sys.table(), self.p);
    }
}

/// The store as OP_COVER's check reads it: unjournaled (a role's check is a
/// probe per permission, past what one record's journal holds), each answer
/// "not yet" or refused leaving the pass undecided.
struct CvLive<'a> {
    sys: &'a Store,
}

/// An object read whole onto the heap, freed with its handle.
struct CvHeap {
    p: *mut u8,
    n: usize,
    sys: *const SyscallTable,
}

impl AsRef<[u8]> for CvHeap {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: `read_whole`'s allocation of `n` bytes, live until drop.
        unsafe { core::slice::from_raw_parts(self.p, self.n) }
    }
}

impl Drop for CvHeap {
    fn drop(&mut self) {
        // SAFETY: allocated by `read_whole` through this table.
        unsafe { heap_free(&*self.sys, self.p) }
    }
}

impl CvStore for CvLive<'_> {
    type Obj = CvHeap;
    fn has(&mut self, key: &[u8]) -> Option<bool> {
        // SAFETY: the serve's store, live for the pass.
        let e = unsafe { exists(self.sys, key) };
        (!self.sys.waiting() && self.sys.refusal().is_none()).then_some(e)
    }
    fn read(&mut self, key: &[u8]) -> Option<Option<CvHeap>> {
        // SAFETY: as `has`.
        let got = unsafe { read_whole(self.sys, key, DOC_MAX) };
        if self.sys.waiting() || self.sys.refusal().is_some() {
            if let Some((p, _)) = got {
                // SAFETY: just allocated through this table.
                unsafe { heap_free(self.sys.table(), p) };
            }
            return None;
        }
        Some(got.map(|(p, n)| CvHeap {
            p,
            n,
            sys: self.sys.table(),
        }))
    }
    fn children(&mut self, prefix: &[u8], out: &mut CvNames) -> Option<bool> {
        let mut walk = ListWalk::new(prefix);
        let mut fits = true;
        // SAFETY: as `has`.
        while let Some(name) = unsafe { walk.next(self.sys) } {
            if !out.push(name) {
                fits = false;
                break;
            }
        }
        if !fits {
            return Some(false);
        }
        if walk.failed || walk.pending || self.sys.waiting() || self.sys.refusal().is_some() {
            return None;
        }
        Some(true)
    }
}

/// OP_COVER: see the op. Answers (status, length of the Status in `why`).
unsafe fn cover(s: &State, outer: &Store, req: &[u8], body: &[u8], why: &mut [u8]) -> (i64, usize) {
    let spec = core::slice::from_raw_parts(s.cover.as_ptr(), s.cover_len as usize);
    let Some(pre) = cv_prefixes(spec) else {
        let m = b"[store_effect] COVER without a `cover` param - REFUSED";
        dev_log(outer.table(), 1, m.as_ptr(), m.len());
        return (500, 0);
    };
    let bytes = |n: u8| match frame_field(req, n) {
        Some((TY_BYTES, b)) => b,
        _ => &[][..],
    };
    let (id, res, ns, name) = (bytes(3), bytes(72), bytes(73), bytes(76));
    let okey = view_split(bytes(74)).0;
    let how = frame_i64(req, 75).unwrap_or(0);
    let Some(kind) = cv_kind(res) else {
        // Not a write the check guards.
        return (200, 0);
    };
    if id.is_empty() {
        return (500, 0);
    }
    let nag = ReadNag::new();
    let sys = Store::plain(outer.table(), &nag);
    // A patch's result is checked: the stored object, patched.
    let stored = if how != 0 && !okey.is_empty() {
        read_whole(&sys, okey, DOC_MAX)
    } else {
        None
    };
    let free = |p: Option<(*mut u8, usize)>| {
        if let Some((p, _)) = p {
            heap_free(outer.table(), p);
        }
    };
    if sys.waiting() {
        free(stored);
        outer.wait();
        return (503, 0);
    }
    if sys.refusal().is_some() {
        free(stored);
        return (500, 0);
    }
    let sbytes = stored.map(|(p, n)| core::slice::from_raw_parts(p as *const u8, n));
    // Room for the result: three times the document for a JSON Patch.
    let room = (body.len() + sbytes.map_or(0, <[u8]>::len) + 4096).saturating_mul(3);
    let work = if how != 0 && room <= 3 * DOC_MAX {
        heap_alloc(outer.table(), room as u32)
    } else {
        core::ptr::null_mut()
    };
    if how != 0 && work.is_null() {
        free(stored);
        return (413, 0);
    }
    let wbuf: &mut [u8] = if work.is_null() {
        &mut []
    } else {
        core::slice::from_raw_parts_mut(work, room)
    };
    let verdict = match cv_final(how, body, sbytes, wbuf) {
        Ok(obj) => {
            let r = CvReq {
                pre,
                id,
                ns,
                kind,
                obj,
                name,
            };
            let mut o = JpOut::new(&mut *why);
            let mut st = CvLive { sys: &sys };
            let v = cv_check(&mut st, &r, &mut o);
            Ok((v, o.n))
        }
        Err(JpErr::Full) => Err(413),
        Err(_) => Err(422),
    };
    if !work.is_null() {
        heap_free(outer.table(), work);
    }
    free(stored);
    if sys.waiting() {
        outer.wait();
        return (503, 0);
    }
    if sys.refusal().is_some() {
        return (500, 0);
    }
    match verdict {
        Err(st) => (st, 0),
        Ok((CvVerdict::Allowed, _)) => (200, 0),
        Ok((CvVerdict::Refused, n)) => (403, n),
        Ok((CvVerdict::RoleMissing, n)) => (404, n),
        Ok((CvVerdict::Malformed, _)) => (422, 0),
        Ok((CvVerdict::TooLarge, _)) => (413, 0),
        Ok((CvVerdict::Undecided, _)) => (500, 0),
    }
}

/// OP_EP_DOCS for the Service `<ns>/<svc>` (`tail`): see the op. A tail that
/// names one of a Service's other slices (`<svc>-<digest>…`, its
/// service-name label another name) is that Service's.
unsafe fn ep_docs(s: &State, outer: &Store, tail: &[u8]) -> i64 {
    // Reads unjournaled: a Service's documents read every row (and, for a
    // named targetPort or a second family, every Pod) — past the 96 calls
    // one record's journal holds, which then refused every write and left a
    // Service of ~75 endpoints frozen there. A render is a re-derivation, so
    // its reads are asked afresh each serve (a "not yet" holds the record,
    // as a source's pass waits); its WRITES (and the draw of a new
    // document's identity) stay the journal's, so a held write is asked
    // again as it was first asked, never proposed twice.
    let nag = ReadNag::new();
    let sys = Store::plain(outer.table(), &nag);
    let status = ep_docs_plain(s, &sys, outer, tail);
    if sys.waiting() {
        outer.wait();
        return 503;
    }
    if outer.waiting() {
        // Held by a journaled read or write: served again with these rows.
        return status;
    }
    // Answered: the next record reads its rows afresh.
    ep_memo_clear(s, outer);
    if sys.refusal().is_some() {
        return 500;
    }
    status
}

#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
unsafe fn ep_docs_plain(s: &State, sys: &Store, wr: &Store, tail: &[u8]) -> i64 {
    let spec = core::slice::from_raw_parts(s.ep_docs.as_ptr(), s.ep_docs_len as usize);
    let mut pre: [&[u8]; 5] = [&[]; 5];
    let mut np = 0usize;
    for part in spec.split(|&c| c == b'|') {
        if np < 5 {
            pre[np] = part;
        }
        np += 1;
    }
    let Some(slash) = tail.iter().position(|&c| c == b'/') else {
        return 400;
    };
    if np != 5 || slash == 0 || slash + 1 >= tail.len() {
        return 400;
    }
    let [svcs, rows_pre, pods, slices, endpoints] = pre;
    // Another slice of a Service: the Service it names.
    let mut tb = [0u8; MAX_KEY];
    let mut tl = tail.len().min(MAX_KEY);
    tb[..tl].copy_from_slice(&tail[..tl]);
    {
        let mut k = [0u8; MAX_KEY];
        if let Some(kl) = ep_key(&[slices, tail], &mut k) {
            let got = read_whole(wr, &k[..kl], DOC_MAX);
            if wr.waiting() {
                if let Some((p, _)) = got {
                    heap_free(sys.table(), p);
                }
                return 503;
            }
            if let Some((p, n)) = got {
                let doc = core::slice::from_raw_parts(p, n);
                if epd_managed(doc, EPD_LABEL_SLICE, EPD_MANAGER_SLICE) {
                    if let Some(l) = epd_str3(doc, b"metadata", b"labels", EPD_LABEL_SVC) {
                        if !l.is_empty()
                            && l != &tail[slash + 1..]
                            && !l.iter().any(|&c| c == b'/')
                            && slash + 1 + l.len() <= MAX_KEY
                        {
                            tb[slash + 1..slash + 1 + l.len()].copy_from_slice(l);
                            tl = slash + 1 + l.len();
                        }
                    }
                }
                heap_free(sys.table(), p);
            }
        }
    }
    let tail = &tb[..tl];
    let (ns, name) = (&tail[..slash], &tail[slash + 1..]);
    let mut k = [0u8; MAX_KEY];
    let Some(kl) = ep_key(&[svcs, tail], &mut k) else {
        return 400;
    };
    let svc_got = read_whole(wr, &k[..kl], DOC_MAX);
    if wr.waiting() {
        if let Some((p, _)) = svc_got {
            heap_free(sys.table(), p);
        }
        return 503;
    }
    let svc: &[u8] = match svc_got {
        Some((p, n)) => core::slice::from_raw_parts(p, n),
        None => &[],
    };
    let status = ep_docs_of(
        s, sys, wr, svc, tail, ns, name, rows_pre, pods, slices, endpoints,
    );
    if let Some((p, _)) = svc_got {
        heap_free(sys.table(), p);
    }
    status
}

/// The documents of the Service `svc` (empty: gone) — see `ep_docs`.
#[allow(
    clippy::too_many_arguments,
    reason = "the Service and the prefixes the op is given"
)]
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
unsafe fn ep_docs_of(
    s: &State,
    sys: &Store,
    wr: &Store,
    svc: &[u8],
    tail: &[u8],
    ns: &[u8],
    name: &[u8],
    rows_pre: &[u8],
    pods: &[u8],
    slices: &[u8],
    endpoints: &[u8],
) -> i64 {
    let named = !svc.is_empty() && epd_named_target(svc);
    let (first6, dual) = epd_families(svc);
    // Pass 1: the groups (family x port set) and their sizes.
    let mut groups = [epd_group_none(); EPD_GROUPS];
    let mut ng = 0usize;
    let mut any_row = false;
    let mut too_many = false;
    // The rows, kept whole while they fit (`mem_ok`), so every later pass
    // reads the heap rather than the store.
    let Some(mut all) = EpBuf::new(sys, EP_EP_BUF0, EP_ALL_BUF_MAX) else {
        return 413;
    };
    let mut mem_ok = true;
    if !svc.is_empty() {
        let r = ep_read_rows(s, sys, rows_pre, pods, ns, tail, named, dual, |rec| {
            any_row = true;
            if mem_ok && !all.push(sys, rec) {
                mem_ok = false;
            }
            if let Some((_, row, pp, alt, _)) = epd_row(rec, 0) {
                if !epd_group_add(&mut groups, &mut ng, svc, row, pp, alt) {
                    too_many = true;
                    return false;
                }
            }
            true
        });
        if let Err(st) = r {
            all.free(sys);
            return st;
        }
        if too_many {
            let m = b"[store_effect] EP_DOCS: a Service's endpoints fall in more port sets than EPD_GROUPS - REFUSED";
            dev_log(sys.table(), 1, m.as_ptr(), m.len());
            all.free(sys);
            return 500;
        }
    }
    let status = ep_docs_cut(
        s,
        sys,
        wr,
        svc,
        tail,
        ns,
        name,
        rows_pre,
        pods,
        slices,
        endpoints,
        named,
        dual,
        first6,
        &groups[..ng],
        any_row,
        mem_ok.then(|| all.bytes()),
    );
    all.free(sys);
    status
}

/// The slices and Endpoints of `ep_docs_of`, its groups in hand and its
/// rows in `mem` (else walked again).
#[allow(
    clippy::too_many_arguments,
    reason = "the Service, the prefixes the op is given and what pass 1 found"
)]
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
unsafe fn ep_docs_cut(
    s: &State,
    sys: &Store,
    wr: &Store,
    svc: &[u8],
    tail: &[u8],
    ns: &[u8],
    name: &[u8],
    rows_pre: &[u8],
    pods: &[u8],
    slices: &[u8],
    endpoints: &[u8],
    named: bool,
    dual: bool,
    first6: bool,
    groups: &[EpdGroup],
    any_row: bool,
    mem: Option<&[u8]>,
) -> i64 {
    let selects = epd_raw2(svc, b"spec", b"selector").is_some_and(|v| v.len() > 2);
    let manage = !svc.is_empty() && (selects || any_row);
    let primary = epd_primary(groups, svc);
    let cap = 2 * EP_SHARD_BUF + 2 * svc.len() + 8192;
    let mut wrote = false;
    let mut good = true;
    // The slices.
    let mut sk = [0u8; MAX_KEY];
    let skp = ep_key(&[slices, ns, b"/"], &mut sk).unwrap_or(0);
    if skp == 0 {
        return 400;
    }
    let write_slice = |name_len: usize,
                       sk: &mut [u8; MAX_KEY],
                       manage: bool,
                       v6: bool,
                       want: &[EpdPort],
                       rows: &[u8]|
     -> (bool, bool) {
        let key = &sk[..skp + name_len];
        let sname = &key[skp..];
        ep_doc_write(
            s,
            sys,
            wr,
            key,
            manage,
            false,
            EPD_LABEL_SLICE,
            EPD_MANAGER_SLICE,
            |u, t, out| epd_slice(svc, ns, name, sname, v6, want, rows, u, t, out),
            cap,
        )
    };
    if !manage || groups.is_empty() {
        // One slice, the Service's own: empty (the Service selects and has
        // no endpoints yet) or, unmanaged, deleted when ours.
        let Some(nl) = epd_slice_name(name, true, 0, 0, &mut sk[skp..]) else {
            return 400;
        };
        let (o, w) = write_slice(nl, &mut sk, manage, first6, &[], &[]);
        good &= o;
        wrote |= w;
    } else {
        let Some(mut buf) = EpBuf::new(sys, EP_SHARD_BUF, EP_SHARD_BUF) else {
            return 413;
        };
        for (gi, g) in groups.iter().enumerate() {
            let mut k = 0usize;
            let mut taken = 0usize;
            let mut fail = false;
            let flush = |buf: &mut EpBuf, k: usize, sk: &mut [u8; MAX_KEY]| -> (bool, bool) {
                let Some(nl) = epd_slice_name(name, primary == Some(gi), g.hash, k, &mut sk[skp..])
                else {
                    return (false, false);
                };
                let r = write_slice(nl, sk, true, g.v6, &g.ports[..g.n], buf.bytes());
                buf.n = 0;
                r
            };
            let r = ep_rows(sys, mem, rows_pre, pods, ns, tail, named, dual, |rec| {
                let Some((_, row, pp, alt, _)) = epd_row(rec, 0) else {
                    return true;
                };
                if !epd_in_group(svc, row, pp, alt, g.v6, &g.ports[..g.n]) {
                    return true;
                }
                if !buf.push(sys, rec) {
                    fail = true;
                    return false;
                }
                taken += 1;
                if taken == EPD_SHARD {
                    let (o, w) = flush(&mut buf, k, &mut sk);
                    good &= o;
                    wrote |= w;
                    k += 1;
                    taken = 0;
                }
                true
            });
            if let Err(st) = r {
                buf.free(sys);
                return st;
            }
            if fail {
                buf.free(sys);
                return 500;
            }
            if taken > 0 || k == 0 {
                let (o, w) = flush(&mut buf, k, &mut sk);
                good &= o;
                wrote |= w;
            }
        }
        buf.free(sys);
    }
    // The Service's other slices that are no longer made (a group gone, a
    // shard past its count), or every one of them when nothing is managed —
    // only this controller's, and only those its service-name label says
    // are this Service's.
    {
        let Some(pl) = ep_key(&[slices, ns, b"/", name, b"-"], &mut sk) else {
            return 400;
        };
        // The prune: through the journal, as every read but the rows.
        let mut walk = ListWalk::new(&sk[..pl]);
        let mut kb = [0u8; MAX_KEY];
        while let Some(one) = walk.next(wr) {
            let kl = one.len().min(MAX_KEY);
            kb[..kl].copy_from_slice(&one[..kl]);
            let sname = kb.get(skp..kl).unwrap_or(&[]);
            if sname.iter().any(|&c| c == b'/')
                || (manage && epd_slice_current(sname, name, groups, primary))
            {
                continue;
            }
            let Some((p, n)) = read_whole(wr, &kb[..kl], DOC_MAX) else {
                continue;
            };
            let doc = core::slice::from_raw_parts(p, n);
            if epd_managed(doc, EPD_LABEL_SLICE, EPD_MANAGER_SLICE)
                && epd_str3(doc, b"metadata", b"labels", EPD_LABEL_SVC) == Some(name)
            {
                leave_tomb(s, wr, &kb[..kl], doc);
                good &= done(delete_value(wr, &kb[..kl]));
                wrote = true;
            }
            heap_free(sys.table(), p);
        }
        good &= !walk.failed;
    }
    // The v1 Endpoints: the first family's listed rows, at most EPD_EP_MAX.
    let mut ek = [0u8; MAX_KEY];
    if let Some(ekl) = ep_key(&[endpoints, tail], &mut ek) {
        let Some(mut buf) = EpBuf::new(sys, EP_EP_BUF0, EP_EP_BUF_MAX) else {
            return 413;
        };
        let pnr = epd_pnr(svc);
        let mut listed = 0usize;
        let mut truncated = false;
        let mut fail = false;
        if manage {
            let r = ep_rows(sys, mem, rows_pre, pods, ns, tail, named, dual, |rec| {
                let Some((_, row, _, alt, _)) = epd_row(rec, 0) else {
                    return true;
                };
                if !epd_ep_listed(row, alt, first6, pnr) {
                    return true;
                }
                if listed == EPD_EP_MAX {
                    truncated = true;
                    return false;
                }
                if !buf.push(sys, rec) {
                    fail = true;
                    return false;
                }
                listed += 1;
                true
            });
            if let Err(st) = r {
                buf.free(sys);
                return st;
            }
        }
        if fail {
            let m = b"[store_effect] EP_DOCS: the Endpoints' rows past the heap - REFUSED";
            dev_log(sys.table(), 1, m.as_ptr(), m.len());
            buf.free(sys);
            return 413;
        }
        let rows = buf.bytes();
        let ecap = 2 * rows.len() + 2 * svc.len() + 8192;
        let (o, w) = ep_doc_write(
            s,
            sys,
            wr,
            &ek[..ekl],
            manage,
            selects,
            EPD_LABEL_EP,
            EPD_MANAGER_EP,
            |u, t, out| epd_endpoints(svc, ns, name, rows, truncated, u, t, out),
            ecap,
        );
        good &= o;
        wrote |= w;
        buf.free(sys);
    }
    if sys.waiting() {
        503
    } else if !good {
        500
    } else if wrote {
        200
    } else {
        204
    }
}

/// `wild = "18,26,24"` → up to six key-part field numbers.
fn parse_wild(s: &mut State, d: *const u8, len: usize) {
    let src = unsafe { core::slice::from_raw_parts(d, len) };
    s.wild_len = 0;
    let mut v: u32 = 0;
    let mut have = false;
    for &c in src.iter().chain(core::iter::once(&b',')) {
        if c.is_ascii_digit() {
            v = v * 10 + u32::from(c - b'0');
            have = true;
        } else if c == b',' {
            if have && (s.wild_len as usize) < s.wild.len() && v < 256 {
                s.wild[s.wild_len as usize] = v as u8;
                s.wild_len += 1;
            }
            v = 0;
            have = false;
        }
    }
}

/// Run the fused decision over the `n`-byte record in `in_buf`, encoding the
/// outcome into `dec_buf`. None when the policy constructed nothing (a
/// deliberate drop) or failed (counted); the record is then not served.
unsafe fn decide(s: &mut State, n: usize) -> Option<usize> {
    // `wall = 2`: the moment now as a record can keep it (_shared/moment.rs).
    let mut moment = [0u8; 64];
    let ml = if s.wall == 2 {
        mo_now(&*s.syscalls, &mut moment)
    } else {
        0
    };
    let mut fields = [dec::Field {
        number: 0,
        value: dec::Value::Null,
    }; dec::MAX_PIPE_FIELDS];
    let raw = core::slice::from_raw_parts(s.in_buf.as_ptr(), n);
    let mut nf = match dec::decode_frame(raw, &mut fields) {
        Ok(nf) => nf,
        Err(_) => {
            s.dec_failed = s.dec_failed.wrapping_add(1);
            return None;
        }
    };
    if s.wall != 0 {
        if nf == fields.len() {
            s.dec_failed = s.dec_failed.wrapping_add(1);
            return None;
        }
        fields[nf] = dec::Field {
            number: u32::from(WALL_FIELD),
            value: dec::Value::Int(dev_unix_millis(&*s.syscalls) as i64),
        };
        nf += 1;
    }
    if s.wall == 2 {
        if nf == fields.len() {
            s.dec_failed = s.dec_failed.wrapping_add(1);
            return None;
        }
        fields[nf] = dec::Field {
            number: u32::from(MOMENT_FIELD),
            value: dec::Value::Bytes(moment.get(..ml).unwrap_or(&[])),
        };
        nf += 1;
    }
    let params = [dec::Message {
        fields: &fields[..nf],
    }];
    let mut builder = dec::Builder::new();
    let mut sbuf = [0u8; dec::STAGE_SCRATCH_CAP];
    let mut scratch = dec::Scratch::new(&mut sbuf);
    let mut spent = 0u64;
    let cont = core::slice::from_raw_parts(s.dec_cont.as_ptr(), s.dec_len as usize);
    if dec::run_decision_metered(cont, &params, &mut builder, &mut scratch, &mut spent).is_err() {
        s.dec_failed = s.dec_failed.wrapping_add(1);
        failed_closed_log(
            &*s.syscalls,
            &s.key_prefix[..s.key_prefix_len as usize],
            s.dec_failed,
        );
        return None;
    }
    if builder.message().fields.is_empty() {
        s.dec_dropped = s.dec_dropped.wrapping_add(1);
        return None;
    }
    match dec::encode_frame_scratch(&builder.message(), &scratch, &mut s.dec_buf) {
        Ok(m) => Some(m),
        Err(_) => {
            s.dec_failed = s.dec_failed.wrapping_add(1);
            None
        }
    }
}

/// Whether `count_where` keeps one child of an OP_COUNT walk. Its input is
/// the child as a reply would answer it: field 3 the key's tail past the
/// walked prefix, 30..39 and 254 the request's carry (what the predicate
/// compares against), and this node's `paths` / `ints` projected out of the
/// child's value. Kept when the decision constructs a message; None when it
/// does not decide, which the walk takes as a failure, never as "no".
unsafe fn count_child(
    s: &State,
    cid: i64,
    tail: &[u8],
    value: &[u8],
    carry: &[(u8, u8, &[u8])],
    proj: &Projection,
    selected: Option<bool>,
) -> Option<i64> {
    let mut frame = [0u8; BUF];
    let mut n = reply_projected_from(&mut frame, cid, 200, tail, value, carry, proj);
    if n == 0 {
        return None;
    }
    // `count_sel`'s verdict, at 48.
    if let Some(sel) = selected {
        let v: &[u8] = if sel { b"1" } else { b"" };
        n = put_field(&mut frame, n, 48, TY_BYTES, v)?;
        frame[0] = frame[0].wrapping_add(1);
    }
    let mut fields = [dec::Field {
        number: 0,
        value: dec::Value::Null,
    }; dec::MAX_PIPE_FIELDS];
    let nf = dec::decode_frame(&frame[..n], &mut fields).ok()?;
    let params = [dec::Message {
        fields: &fields[..nf],
    }];
    let mut builder = dec::Builder::new();
    let mut sbuf = [0u8; dec::STAGE_SCRATCH_CAP];
    let mut scratch = dec::Scratch::new(&mut sbuf);
    let mut spent = 0u64;
    let cont = &s.cw_cont[..s.cw_len as usize];
    dec::run_decision_metered(cont, &params, &mut builder, &mut scratch, &mut spent).ok()?;
    // A message counts the child; one whose field 1 is a number counts it
    // THAT much — a sum (a Pod's requests), where a count is a sum of ones.
    let msg = builder.message();
    if msg.fields.is_empty() {
        return Some(0);
    }
    let weight = msg
        .fields
        .iter()
        .find(|f| f.number == 1)
        .map_or(1, |f| match f.value {
            dec::Value::Int(v) => v,
            _ => 1,
        });
    Some(weight)
}

/// `count_sel` over one child: the selector and the labels, each from the
/// request (`@<n>`) or the child (a dotted path), judged as upstream reads
/// a label selector. None when `count_sel` is not set.
fn count_sel_of(s: &State, req: &[u8], child: &[u8]) -> Option<bool> {
    if s.count_sel_len == 0 {
        return None;
    }
    let spec = s.count_sel.get(..s.count_sel_len as usize).unwrap_or(&[]);
    let bar = spec.iter().position(|&b| b == b'|')?;
    // `@<n>` is the request's field n; `@<n>.<path>` a path inside it (a
    // document the request carries).
    let side = |one: &'_ [u8]| -> Option<&'_ [u8]> {
        let num_path = one.strip_prefix(b"@")?;
        let dot = num_path
            .iter()
            .position(|&b| b == b'.')
            .unwrap_or(num_path.len());
        let mut n = 0u32;
        for &c in num_path.get(..dot).unwrap_or(&[]) {
            if !c.is_ascii_digit() {
                return None;
            }
            n = n * 10 + u32::from(c - b'0');
        }
        let v = match frame_field(req, n.min(255) as u8) {
            Some((TY_BYTES, v)) => v,
            _ => return None,
        };
        match num_path.get(dot + 1..) {
            Some(path) if !path.is_empty() => {
                let mut segs: [&[u8]; 6] = [&[]; 6];
                let ns = split_dots(path, &mut segs);
                j_sub(v, &segs[..ns])
            }
            _ => Some(v),
        }
    };
    let sel_spec = spec.get(..bar).unwrap_or(&[]);
    let lab_spec = spec.get(bar + 1..).unwrap_or(&[]);
    let from_child = |path: &[u8]| -> Option<&[u8]> {
        let mut segs: [&[u8]; 6] = [&[]; 6];
        let ns = split_dots(path, &mut segs);
        j_sub(child, &segs[..ns])
    };
    let sel = if sel_spec.first() == Some(&b'@') {
        side(sel_spec)
    } else {
        from_child(sel_spec)
    };
    let labels = if lab_spec.first() == Some(&b'@') {
        side(lab_spec)
    } else {
        from_child(lab_spec)
    };
    let sel = sel.filter(|v| !v.is_empty() && jp_kind(v) == b'{');
    Some(sk_selects(sel, labels.unwrap_or(b"{}")))
}

/// The fused decision decided nothing for a record: it read a field the
/// record lacks, or did not evaluate, and the record is not served. Logged
/// at the 1st, 2nd, 4th, … time, so the first is always seen and a steady
/// failure cannot flood the log.
unsafe fn failed_closed_log(sys: &SyscallTable, prefix: &[u8], count: u32) {
    if !count.is_power_of_two() {
        return;
    }
    let mut m = [0u8; 200];
    let mut n = 0usize;
    let mut digits = [0u8; 20];
    let dn = dec(u64::from(count), &mut digits);
    let mut put = |part: &[u8]| {
        let k = part.len().min(m.len() - n);
        m[n..n + k].copy_from_slice(&part[..k]);
        n += k;
    };
    put(b"[store_effect] ");
    put(prefix);
    put(b": the fused decision failed closed (");
    put(&digits[..dn]);
    put(b" so far): a rule read a field the record does not carry, or did not evaluate");
    dev_log(sys, 1, m.as_ptr(), n);
}

/// Render `v` as decimal digits into `out`; returns the length.
fn dec(v: u64, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 || i == 0 {
            break;
        }
    }
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// Stamp `rev` at the two-segment `rv_path` of a JSON object, as a string —
/// Kubernetes' `resourceVersion` is opaque text, and a client that reads it as
/// a number is wrong. Returns the stamped length, or None when the path is not
/// two segments or the object has no such parent (the value is then answered
/// unstamped, as it is stored).
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
fn stamp_rv(value: &[u8], rv_path: &[u8], rev: u64, out: &mut [u8]) -> Option<usize> {
    let dot = rv_path.iter().position(|&b| b == b'.')?;
    let (k1, k2) = (&rv_path[..dot], &rv_path[dot + 1..]);
    if k1.is_empty() || k2.is_empty() || k2.iter().any(|&b| b == b'.') {
        return None;
    }
    let mut digits = [0u8; 20];
    let dl = dec(rev, &mut digits);
    let mut tok = [0u8; 24];
    tok[0] = b'"';
    tok[1..1 + dl].copy_from_slice(&digits[..dl]);
    tok[1 + dl] = b'"';
    let n = j_set2(value, k1, k2, &tok[..dl + 2], out);
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

/// The expected revision a REPLACE is conditional on: field 28 when the
/// request states one, else the body's own `rv_path` — the token a client
/// read on its GET and sends back unchanged. 0 = none stated.
fn expected_rev(req: &[u8], value: &[u8], rv_path: &[u8]) -> u64 {
    if let Some(r) = frame_i64(req, 28) {
        if r > 0 {
            return r as u64;
        }
    }
    if rv_path.is_empty() {
        return 0;
    }
    let mut segs: [&[u8]; 6] = [&[]; 6];
    let ns = split_dots(rv_path, &mut segs);
    match j_path(value, &segs[..ns]) {
        Some(raw) => u64::from(parse_dec_bytes(raw)),
        None => 0,
    }
}

/// Apply a PATCH (`OP_PATCH_*`) to the object at `key`; the status to answer.
/// The object and the result are on the heap, so an object of any size a
/// record could never carry is patched where it lies. With a writer (`fw`)
/// the write is recorded under the schema's `owners` path; `apply` makes
/// `patch` an applied
/// configuration, whose conflicts are refused (409) with their Status in
/// `note`.
#[allow(
    clippy::too_many_arguments,
    reason = "a patch, where it goes, who writes it, and where a refusal is said"
)]
unsafe fn patch_object(
    sys: &Store,
    key: &[u8],
    op: i64,
    patch: &[u8],
    keys: &[u8],
    rv_path: &[u8],
    dry: Option<&mut [u8; MAX_VALUE]>,
    fw: Option<&FsWriter>,
    apply: bool,
    note: &mut [u8; MAX_VALUE],
    note_n: &mut usize,
    sch: &[u8],
) -> i64 {
    let mut dry = dry;
    let patch = jp_trim(patch);
    let want = expected_rev(&[], patch, rv_path);
    for _ in 0..PATCH_TRIES {
        let Some(rev) = head(sys, key) else {
            return 404;
        };
        // A patch that states a resourceVersion is conditional on it.
        if want != 0 && want != rev {
            return 409;
        }
        let Some((cur_p, cur_n)) = read_whole(sys, key, DOC_MAX) else {
            return if exists(sys, key) { 500 } else { 404 };
        };
        let cur = core::slice::from_raw_parts(cur_p, cur_n);
        // Room for the result: the object and the patch, each written at most
        // once by a merge; JSON Patch ping-pongs in thirds, with a `copy`'s
        // growth. Past DOC_MAX it is not an object this store keeps.
        let room = (cur_n + patch.len())
            .saturating_mul(2)
            .saturating_add(4096)
            .min(DOC_MAX);
        let work_n = if op == OP_PATCH_JSON { room * 3 } else { room };
        let work_p = heap_alloc(sys, work_n as u32);
        if work_p.is_null() {
            heap_free(sys, cur_p);
            return 500;
        }
        let work = core::slice::from_raw_parts_mut(work_p, work_n);
        let result = if op == OP_PATCH_JSON {
            jp_json(cur, patch, work)
        } else {
            let smp = jp_smp(keys, sch);
            let mut o = JpOut::new(work);
            let r = jp_merge(
                Some(cur),
                patch,
                &mut o,
                if op == OP_PATCH_SMP { Some(&smp) } else { None },
                b"",
                0,
            );
            r.map(|()| (0usize, o.n))
        };
        let status = match result {
            Ok((off, n)) => {
                let doc = &core::slice::from_raw_parts(work_p, work_n)[off..off + n];
                let tracked = match fw {
                    Some(w) if jp_kind(doc) == b'{' => {
                        track(sys, Some(cur), doc, apply.then_some(patch), w)
                    }
                    _ => Tracked::Same,
                };
                let status = if let Tracked::Refused(..) = tracked {
                    let t = tracked.doc(&[]);
                    *note_n = if t.len() <= note.len() {
                        note[..t.len()].copy_from_slice(t);
                        t.len()
                    } else {
                        0
                    };
                    409
                } else {
                    let doc = tracked.doc(doc);
                    if jp_kind(doc) != b'{' || !same_identity(cur, doc, sch) {
                        422
                    } else if let Some(d) = dry.as_deref_mut() {
                        // A dry run: the result, answered, never written.
                        dry_answer(doc, rev, rv_path, d)
                    } else if doc == cur {
                        // A patch that changes nothing writes nothing:
                        // upstream answers it and leaves the resourceVersion
                        // where it was. A chain that re-asserts a status on
                        // every pass then spends no revision, and no watcher
                        // is woken, when nothing moved.
                        200
                    } else {
                        match code(put_cond(sys, key, doc, PRE_ETAG, rev)) {
                            0 => 200,
                            ERR_EAGAIN => 0,
                            _ => 500,
                        }
                    }
                };
                tracked.free(sys);
                status
            }
            Err(JpErr::Malformed) => 400,
            Err(JpErr::Full) => 413,
            Err(JpErr::Test | JpErr::Path | JpErr::Deep) => 422,
        };
        heap_free(sys, work_p);
        heap_free(sys, cur_p);
        if status != 0 {
            return status;
        }
    }
    409
}

/// The object at `key` as a client is answered with: the stored value with
/// its revision stamped, when the write succeeded and it fits a record;
/// empty otherwise (the chain then answers it by a GET registration).
unsafe fn stored_answer<'a>(
    sys: &Store,
    key: &[u8],
    view: &[u8],
    status: i64,
    rv_path: &[u8],
    buf: &'a mut [u8; MAX_VALUE],
    sch: &[u8],
) -> &'a [u8] {
    if !(status == 200 || status == 204) {
        return &[];
    }
    let mut raw = [0u8; MAX_VALUE];
    let Some(mut n) = get_value(sys, key, &mut raw) else {
        return &[];
    };
    if let SchView::Template(t) = sch_view(sch, view) {
        let mut sc = [0u8; 1024];
        let Some(l) = sch_render(t, &raw[..n], &mut sc) else {
            return &[];
        };
        raw[..l].copy_from_slice(&sc[..l]);
        n = l;
    }
    let rev = head(sys, key).unwrap_or(0);
    match stamp_rv(&raw[..n], rv_path, rev, &mut buf[..]) {
        Some(l) => &buf[..l],
        None => {
            buf[..n].copy_from_slice(&raw[..n]);
            &buf[..n]
        }
    }
}

/// Whether the object's delete is held: the schema's `guard` list is
/// non-empty.
fn is_guarded(obj: &[u8], sch: &[u8]) -> bool {
    sch_nonempty(obj, sch_get(sch, b"guard"))
}

/// A held DELETE: stamp the schema's `deleting` fields (once, conditional on
/// the revision read) and keep the object. The status to answer.
unsafe fn mark_deleting(sys: &Store, key: &[u8], obj: &[u8], sch: &[u8]) -> i64 {
    let list = sch_get(sch, b"deleting");
    if sch_set(obj, sch_first_path(list)) {
        return 200;
    }
    let Some(rev) = head(sys, key) else {
        return 404;
    };
    let mut ts = [0u8; 20];
    rfc3339(now_millis(sys), &mut ts);
    let g = SchGen {
        uuid: &[],
        now: &ts,
        name: &[],
    };
    let mut patch = [0u8; 512];
    let Ok(pl) = sch_empty(&mut patch).and_then(|n| sch_stamps(list, &g, &mut patch, n)) else {
        return 500;
    };
    let cap = obj.len() + pl + 64;
    let wp = heap_alloc(sys, cap as u32);
    if wp.is_null() {
        return 500;
    }
    let mut o = JpOut::new(core::slice::from_raw_parts_mut(wp, cap));
    let status = match jp_merge(Some(obj), &patch[..pl], &mut o, None, b"", 0) {
        Ok(()) => match code(put_cond(
            sys,
            key,
            &core::slice::from_raw_parts(wp, cap)[..o.n],
            PRE_ETAG,
            rev,
        )) {
            0 => 200,
            ERR_EAGAIN => 409,
            _ => 500,
        },
        Err(_) => 500,
    };
    heap_free(sys, wp);
    status
}

/// After a write: an object marked for deletion whose guard list is now
/// empty is deleted (its tomb left for a watch's DELETED).
unsafe fn finish_deletion(s: &State, sys: &Store, key: &[u8], obj: &[u8]) {
    let sch = &s.schema[..s.schema_len as usize];
    let mark = sch_first_path(sch_get(sch, b"deleting"));
    if mark.is_empty() || !sch_set(obj, mark) || is_guarded(obj, sch) {
        return;
    }
    if s.tomb_len > 0 {
        leave_tomb(s, sys, key, obj);
    }
    let _ = delete_value(sys, key);
}

/// A dry run's answer: `doc` with the revision it was computed against
/// stamped, copied into `out` NUL-terminated (a JSON document holds no NUL).
/// The status: 200, or 413 when it does not fit a record.
fn dry_answer(doc: &[u8], rev: u64, rv_path: &[u8], out: &mut [u8; MAX_VALUE]) -> i64 {
    let mut tmp = [0u8; MAX_VALUE];
    let n = match stamp_rv(doc, rv_path, rev, &mut tmp) {
        Some(l) => l,
        None if doc.len() < MAX_VALUE => {
            tmp[..doc.len()].copy_from_slice(doc);
            doc.len()
        }
        None => return 413,
    };
    if n >= MAX_VALUE {
        return 413;
    }
    out[..n].copy_from_slice(&tmp[..n]);
    out[n] = 0;
    200
}

/// The dry-run answer held in `d` (NUL-terminated), through `view`.
fn dry_body<'a>(d: &'a mut [u8; MAX_VALUE], view: &[u8], sch: &[u8]) -> &'a [u8] {
    let n = d.iter().position(|&b| b == 0).unwrap_or(0);
    if let SchView::Template(t) = sch_view(sch, view) {
        let mut sc = [0u8; 1024];
        if let Some(l) = sch_render(t, &d[..n], &mut sc) {
            d[..l].copy_from_slice(&sc[..l]);
            return &d[..l];
        }
    }
    &d[..n]
}

/// Whether `b` holds a `/`.
#[allow(
    clippy::manual_contains,
    reason = "`<[u8]>::contains` lowers to memchr, which a PIC module cannot link"
)]
fn has_slash(b: &[u8]) -> bool {
    b.iter().any(|&c| c == b'/')
}

/// Lower-case hex of `b` into `out`.
fn hex_into(b: &[u8], out: &mut [u8]) {
    let h = b"0123456789abcdef";
    for (i, &x) in b.iter().enumerate() {
        out[2 * i] = h[(x >> 4) as usize];
        out[2 * i + 1] = h[(x & 15) as usize];
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ` for Unix milliseconds (civil-from-days).
fn rfc3339(ms: u64, out: &mut [u8; 20]) {
    let secs = ms / 1000;
    let days = (secs / 86400) as i64;
    let sod = secs % 86400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    let put = |o: &mut [u8; 20], at: usize, v: i64, w: usize| {
        let mut v = v;
        for k in (0..w).rev() {
            o[at + k] = b'0' + (v % 10) as u8;
            v /= 10;
        }
    };
    put(out, 0, y, 4);
    out[4] = b'-';
    put(out, 5, m, 2);
    out[7] = b'-';
    put(out, 8, d, 2);
    out[10] = b'T';
    put(out, 11, (sod / 3600) as i64, 2);
    out[13] = b':';
    put(out, 14, (sod / 60 % 60) as i64, 2);
    out[16] = b':';
    put(out, 17, (sod % 60) as i64, 2);
    out[19] = b'Z';
}

/// Create the object `value` at `key`, its schema's `create` paths stamped.
/// A `key` ending `/` names no object: the name is the schema's `generate`
/// path plus five random characters (422 without one), retried on a
/// collision. Writes the final key into `fk`; returns the status and its
/// length. With a writer (`fw`) the create is recorded under the schema's
/// `owners` path — as an apply when `apply` (the body is the applied
/// configuration).
#[allow(
    clippy::too_many_arguments,
    reason = "a create, where it goes, and who makes it"
)]
unsafe fn create_stamped(
    sys: &Store,
    key: &[u8],
    value: &[u8],
    fk: &mut [u8; MAX_KEY],
    dry: Option<&mut [u8; MAX_VALUE]>,
    rv_path: &[u8],
    fw: Option<&FsWriter>,
    apply: bool,
    sch: &[u8],
) -> (i64, usize) {
    let mut dry = dry;
    if jp_kind(value) != b'{' {
        return (400, 0);
    }
    let generate = key.last() == Some(&b'/');
    let gen = if generate {
        let g = sch_at(value, sch_get(sch, b"generate")).unwrap_or(&[]);
        let g = g
            .strip_prefix(b"\"")
            .and_then(|g| g.strip_suffix(b"\""))
            .unwrap_or(&[]);
        if g.is_empty() || g.len() > 58 {
            return (422, 0);
        }
        g
    } else {
        &[][..]
    };
    for _ in 0..4 {
        // The name: the key's last segment, or generated.
        let mut name = [0u8; 64];
        let nl = if generate {
            let alphabet = b"bcdfghjklmnpqrstvwxz2456789";
            let mut r = [0u8; 5];
            // Through the journal: a held create builds the same name.
            rand_fill(sys, &mut r);
            name[..gen.len()].copy_from_slice(gen);
            for (i, &x) in r.iter().enumerate() {
                name[gen.len() + i] = alphabet[x as usize % alphabet.len()];
            }
            gen.len() + 5
        } else {
            let last = key.rsplit(|&b| b == b'/').next().unwrap_or(&[]);
            let n = last.len().min(name.len());
            name[..n].copy_from_slice(&last[..n]);
            n
        };
        let name = &name[..nl];
        let kl = if generate { key.len() + nl } else { key.len() };
        if kl > fk.len() {
            return (422, 0);
        }
        fk[..key.len()].copy_from_slice(key);
        if generate {
            fk[key.len()..kl].copy_from_slice(name);
        }
        // The stamps, as a merge patch over what the client sent: the
        // schema's `create` fields, and a `named` one for a key it names.
        let mut u = [0u8; 16];
        // Through the journal: a held create builds the same uid, so the
        // value it asks again is the one first asked.
        rand_fill(sys, &mut u);
        u[6] = (u[6] & 0x0f) | 0x40;
        u[8] = (u[8] & 0x3f) | 0x80;
        let mut uh = [0u8; 32];
        hex_into(&u, &mut uh);
        let mut uuid = [b'-'; 36];
        uuid[..8].copy_from_slice(&uh[..8]);
        uuid[9..13].copy_from_slice(&uh[8..12]);
        uuid[14..18].copy_from_slice(&uh[12..16]);
        uuid[19..23].copy_from_slice(&uh[16..20]);
        uuid[24..].copy_from_slice(&uh[20..32]);
        let mut ts = [0u8; 20];
        rfc3339(now_millis(sys), &mut ts);
        let g = SchGen {
            uuid: &uuid,
            now: &ts,
            name,
        };
        let mut patch = [0u8; 1024];
        let stamped = sch_empty(&mut patch)
            .and_then(|n| sch_stamps(sch_get(sch, b"create"), &g, &mut patch, n))
            .and_then(|n| match sch_named(sch_get(sch, b"named"), key) {
                Some((path, gen)) => {
                    let mut one = [0u8; 256];
                    let mut o = JpOut::new(&mut one);
                    o.put(path)?;
                    o.c(b':')?;
                    o.put(gen)?;
                    let l = o.n;
                    sch_stamps(&one[..l], &g, &mut patch, n)
                }
                None => Ok(n),
            });
        let Ok(pl) = stamped else {
            return (422, 0);
        };
        let cap = value.len() + pl + 64;
        let wp = heap_alloc(sys, cap as u32);
        if wp.is_null() {
            // A stamped write builds the object on the heap: a build without
            // one (store_effect) can never stamp, and must say so.
            let m = b"[store_effect] FAULT: a stamped write needs a heap - use store_stamped or store_big";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return (500, 0);
        }
        let work = core::slice::from_raw_parts_mut(wp, cap);
        let mut o = JpOut::new(work);
        let status = match jp_merge(Some(value), &patch[..pl], &mut o, None, b"", 0) {
            Ok(()) => {
                let stamped = &core::slice::from_raw_parts(wp, cap)[..o.n];
                let tracked = match fw {
                    Some(w) => track(sys, None, stamped, apply.then_some(value), w),
                    None => Tracked::Same,
                };
                let doc = tracked.doc(stamped);
                let status = if let Some(d) = dry.as_deref_mut() {
                    // A dry run: the create as it would be stored, not stored.
                    if exists(sys, &fk[..kl]) {
                        if generate {
                            0
                        } else {
                            409
                        }
                    } else {
                        dry_answer(doc, 0, rv_path, d)
                    }
                } else {
                    match code(put_cond(sys, &fk[..kl], doc, PRE_ABSENT, 0)) {
                        0 => 200,
                        ERR_EEXIST if generate => 0,
                        ERR_EEXIST => 409,
                        _ => 500,
                    }
                };
                tracked.free(sys);
                status
            }
            Err(_) => 400,
        };
        heap_free(sys, wp);
        if status != 0 {
            return (status, kl);
        }
    }
    (409, 0)
}

/// A write's object with the write recorded under the schema's `owners`
/// path.
enum Tracked {
    /// Store this: `(heap, offset, length)`; the caller frees the heap.
    Doc(*mut u8, usize, usize),
    /// An apply refused on conflicts: the Status, as `Doc`.
    Refused(*mut u8, usize, usize),
    /// Store the object as it is.
    Same,
}

impl Tracked {
    unsafe fn doc<'a>(&self, same: &'a [u8]) -> &'a [u8] {
        match *self {
            Tracked::Doc(p, off, n) | Tracked::Refused(p, off, n) => {
                core::slice::from_raw_parts(p.add(off), n)
            }
            Tracked::Same => same,
        }
    }
    unsafe fn free(&self, sys: &SyscallTable) {
        if let Tracked::Doc(p, _, _) | Tracked::Refused(p, _, _) = *self {
            heap_free(sys, p);
        }
    }
}

/// Record the write of `new` over `old` (`None`: a create) by `w` —
/// fieldset.rs; `cfg` is the applied configuration of an apply. An object too
/// large to track keeps the owners it had.
unsafe fn track(
    sys: &Store,
    old: Option<&[u8]>,
    new: &[u8],
    cfg: Option<&[u8]>,
    w: &FsWriter,
) -> Tracked {
    let third = new.len() + 2 * FS_ARENA + 16384;
    let total = third * 3 + FS_ARENA;
    let wp = heap_alloc(sys, total as u32);
    if wp.is_null() {
        return Tracked::Same;
    }
    let Some((work, arena)) =
        core::slice::from_raw_parts_mut(wp, total).split_at_mut_checked(third * 3)
    else {
        heap_free(sys, wp);
        return Tracked::Same;
    };
    match fs_record(old, new, cfg, w, arena, work) {
        FsOutcome::Doc(off, n) => return Tracked::Doc(wp, off, n),
        FsOutcome::Conflict(off, n) => return Tracked::Refused(wp, off, n),
        FsOutcome::Untracked => {}
    }
    // Untracked: what was recorded stays, whatever the write sent.
    let owners = sch_get(w.sch, b"owners");
    let kept = old
        .and_then(|o| sch_at(o, owners))
        .filter(|_| !owners.is_empty());
    let Some(mf) = kept else {
        heap_free(sys, wp);
        return Tracked::Same;
    };
    let need = new.len() + 2 * mf.len() + 2 * owners.len() + 128;
    if need > total {
        heap_free(sys, wp);
        return Tracked::Same;
    }
    let split = mf.len() + 2 * owners.len() + 64;
    let Some((patch, out)) = core::slice::from_raw_parts_mut(wp, total).split_at_mut_checked(split)
    else {
        heap_free(sys, wp);
        return Tracked::Same;
    };
    // What was recorded, patched back in at the schema's `owners` path.
    let mut po = JpOut::new(patch);
    let Ok(pl) = sch_one(owners, mf, &mut po).map(|()| po.n) else {
        heap_free(sys, wp);
        return Tracked::Same;
    };
    let mut o = JpOut::new(out);
    match jp_merge(Some(new), &patch[..pl], &mut o, None, b"", 0) {
        Ok(()) => Tracked::Doc(wp, split, o.n),
        Err(_) => {
            heap_free(sys, wp);
            Tracked::Same
        }
    }
}

/// For a REPLACE: `value` with the STORED object's `keep` paths (and its
/// `named` stamp) carried across, the write recorded under the schema's
/// `owners` path when `fw` names the writer. `(heap to free, object,
/// length)`; `None`: store `value` as it is.
unsafe fn keep_identity(
    sys: &Store,
    key: &[u8],
    value: &[u8],
    fw: Option<&FsWriter>,
    sch: &[u8],
) -> Option<(*mut u8, *const u8, usize)> {
    if jp_kind(value) != b'{' {
        return None;
    }
    let mut cur = [0u8; MAX_VALUE];
    let (cp, cn, heap) = match get_value(sys, key, &mut cur) {
        Some(n) => (cur.as_ptr(), n, core::ptr::null_mut()),
        None => {
            let (p, n) = read_whole(sys, key, DOC_MAX)?;
            (p as *const u8, n, p)
        }
    };
    let stored = core::slice::from_raw_parts(cp, cn);
    let kept = keep_identity_of(sys, key, stored, value, sch);
    let merged: &[u8] = match kept {
        Some((p, n)) => core::slice::from_raw_parts(p, n),
        None => value,
    };
    let tracked = match fw {
        Some(w) => track(sys, Some(stored), merged, None, w),
        None => Tracked::Same,
    };
    if !heap.is_null() {
        heap_free(sys, heap);
    }
    match tracked {
        Tracked::Doc(p, off, n) => {
            if let Some((kp, _)) = kept {
                heap_free(sys, kp);
            }
            Some((p, p.add(off) as *const u8, n))
        }
        Tracked::Refused(..) => {
            tracked.free(sys);
            kept.map(|(p, n)| (p, p as *const u8, n))
        }
        Tracked::Same => kept.map(|(p, n)| (p, p as *const u8, n)),
    }
}

/// `value` with `stored`'s identity merged in — the schema's `keep` paths,
/// and a `named` key's field — on the heap; `None` when there is nothing to
/// keep or no room.
unsafe fn keep_identity_of(
    sys: &Store,
    key: &[u8],
    stored: &[u8],
    value: &[u8],
    sch: &[u8],
) -> Option<(*mut u8, usize)> {
    let mut patch = [0u8; 1024];
    let mut pl = sch_empty(&mut patch).ok()?;
    pl = sch_keeps(sch_get(sch, b"keep"), stored, &mut patch, pl).ok()?;
    if let Some((path, gen)) = sch_named(sch_get(sch, b"named"), key) {
        let name = key.rsplit(|&b| b == b'/').next().unwrap_or(&[]);
        let g = SchGen {
            uuid: &[],
            now: &[],
            name: &name[..name.len().min(63)],
        };
        let mut one = [0u8; 256];
        let mut o = JpOut::new(&mut one);
        o.put(path).ok()?;
        o.c(b':').ok()?;
        o.put(gen).ok()?;
        let l = o.n;
        pl = sch_stamps(&one[..l], &g, &mut patch, pl).ok()?;
    }
    if pl <= 2 {
        return None;
    }
    let cap = value.len() + pl + 64;
    let wp = heap_alloc(sys, cap as u32);
    if wp.is_null() {
        return None;
    }
    let mut o = JpOut::new(core::slice::from_raw_parts_mut(wp, cap));
    if jp_merge(Some(value), &patch[..pl], &mut o, None, b"", 0).is_err() {
        heap_free(sys, wp);
        return None;
    }
    Some((wp, o.n))
}

/// A patch edits an object; it does not turn it into another one: the
/// schema's `identity` paths are unchanged, or the patch is refused.
fn same_identity(a: &[u8], b: &[u8], sch: &[u8]) -> bool {
    sch_same(sch_get(sch, b"identity"), a, b)
}

/// store_big — this source built with `big` — takes a heap arena: a staged
/// request body read whole to be written, or an object past a record read
/// whole to be projected, or a template spliced into a controller's write.
/// store_stamped (`stamp_heap`) takes a smaller one: one controller object
/// read whole, or stamped. store_effect takes none.
#[allow(
    unexpected_cfgs,
    reason = "`big` and `stamp_heap` are store_big's and store_stamped's features; this file is also store_effect, whose build declares neither"
)]
mod arena {
    #[cfg(feature = "big")]
    #[no_mangle]
    #[link_section = ".text.module_arena_size"]
    pub extern "C" fn module_arena_size() -> u32 {
        16 << 20
    }
    /// store_stamped: ONE controller object on the heap at a time — read
    /// whole to be projected (a Pod's hash, a ReplicaSet's spec digest), or
    /// stamped and kept from the stored one: an object up to `DOC_MAX`, the
    /// API's largest (a whole read grows in place on a fresh heap). A write
    /// that SPLICES a template (`splice_value`) holds its source, the value
    /// and the stamp's copies at once, and is store_big's.
    #[cfg(feature = "stamp_heap")]
    #[no_mangle]
    #[link_section = ".text.module_arena_size"]
    pub extern "C" fn module_arena_size() -> u32 {
        4 << 20
    }
}

/// How long a deleted object's last state stays for a watch to carry
/// (`tomb_ms`, default).
const TOMB_MS: u64 = 300_000;

/// Leave `value` as `key`'s tombstone (see `tomb`). A tomb past a record
/// is built on the heap (store_big); without one it is not written: the
/// DELETED event then carries the object's identity only, which is what a
/// client needs to drop it. The deadline is read through the journal
/// as a moment ([`tomb_deadline`]), so a held delete asks again the tomb it first asked.
unsafe fn leave_tomb(s: &State, sys: &Store, key: &[u8], value: &[u8]) {
    let mut tk = [0u8; MAX_KEY];
    let tl = s.tomb_len as usize;
    if tl + key.len() > tk.len() {
        return;
    }
    tk[..tl].copy_from_slice(&s.tomb[..tl]);
    tk[tl..tl + key.len()].copy_from_slice(key);
    // The deadline as a moment (`_shared/tomb.rs`), drawn through the
    // journal: a held delete asks again the tomb it first asked.
    let mut moment = [0u8; 64];
    let ttl = u64::from(s.tomb_ms);
    let ml = tomb_deadline(sys, ttl, &mut moment);
    let deadline = moment.get(..ml).unwrap_or(&[]);
    // Sequential appends, never an array of byte-string literals: such an
    // array's pointers are not relocated in a PIC module.
    let need = value.len() + ml + 24;
    let mut stack = [0u8; MAX_VALUE];
    let heap = if need <= stack.len() {
        core::ptr::null_mut()
    } else {
        let p = heap_alloc(sys, need as u32);
        if p.is_null() {
            return;
        }
        p
    };
    let v: &mut [u8] = if heap.is_null() {
        &mut stack
    } else {
        core::slice::from_raw_parts_mut(heap, need)
    };
    let mut l = 0usize;
    l = append(v, l, b"{\"deadline\":\"");
    l = append(v, l, deadline);
    l = append(v, l, b"\",\"obj\":");
    l = append(v, l, value);
    l = append(v, l, b"}");
    // Its expiry index first ([`put_tomb_at`]): no tomb without the entry
    // that ends it.
    if put_tomb_at(sys, &s.tomb[..tl], key, deadline, ttl, value, None) {
        let _ = put_value(sys, &tk[..tl + key.len()], v.get(..l).unwrap_or(&[]));
    }
    if !heap.is_null() {
        heap_free(sys, heap);
    }
}

/// Append field 59 (the store revision, i64) to a finished reply frame.
fn append_rev(out: &mut [u8], flen: usize, rev: u64) -> usize {
    if flen == 0 {
        return 0;
    }
    match put_field(out, flen, 59, TY_I64, &(rev as i64).to_le_bytes()) {
        Some(q) => {
            out[0] += 1;
            q
        }
        None => 0,
    }
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PATHS_SPEC;
    use super::MAX_PREFIX;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        8, list_values, u32, 0
            => |s, d, len| { s.list_values = super::p_u32(d, len, 0, 0); };

        9, list_open, str, 0
            => |s, d, len| {
                let n = if len > super::LIST_LIT_MAX { super::LIST_LIT_MAX } else { len };
                s.list_open_len = n as u8;
                if n > 0 { ptr_copy(s.list_open.as_mut_ptr(), d, n); }
            };

        10, list_close, str, 0
            => |s, d, len| {
                let n = if len > super::LIST_LIT_MAX { super::LIST_LIT_MAX } else { len };
                s.list_close_len = n as u8;
                if n > 0 { ptr_copy(s.list_close.as_mut_ptr(), d, n); }
            };

        7, list_sep, str, 0
            => |s, d, len| {
                let n = if len > 8 { 8 } else { len };
                s.list_sep_len = n as u8;
                if n > 0 { ptr_copy(s.list_sep.as_mut_ptr(), d, n); }
            };

        1, key_prefix, str, 0
            => |s, d, len| {
                let n = if len > MAX_PREFIX { MAX_PREFIX } else { len };
                s.key_prefix_len = n as u8;
                if n > 0 { ptr_copy(s.key_prefix.as_mut_ptr(), d, n); }
            };

        2, paths, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.paths_len = n as u16;
                if n > 0 { ptr_copy(s.paths.as_mut_ptr(), d, n); }
            };

        3, flat, u32, 0
            => |s, d, len| { s.flat = super::p_u32(d, len, 0, 0); };

        4, ints, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.ints_len = n as u16;
                if n > 0 { ptr_copy(s.ints.as_mut_ptr(), d, n); }
            };

        11, rv_path, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.rv_path_len = n as u8;
                if n > 0 { ptr_copy(s.rv_path.as_mut_ptr(), d, n); }
            };

        41, stamp, u32, 0
            => |s, d, len| { s.stamp = super::p_u32(d, len, 0, 0); };

        12, revision, u32, 0
            => |s, d, len| { s.revision = super::p_u32(d, len, 0, 0); };

        // Chunked, as `decision` is: a schema is longer than one TLV entry.
        // Past PATCH_KEYS_MAX the whole schema is dropped (every list atomic)
        // rather than cut — a cut entry names a different key.
        20, patch_keys, str_chunked, 0
            => |s, d, len| {
                let at = s.patch_keys_len as usize;
                if at + len > super::PATCH_KEYS_MAX {
                    s.patch_keys_len = 0;
                    s.patch_keys_bad = 1;
                } else if s.patch_keys_bad == 0 {
                    ptr_copy(s.patch_keys.as_mut_ptr().add(at), d, len);
                    s.patch_keys_len = (at + len) as u16;
                }
            };

        // Chunked: a schema is longer than one TLV entry. Past DOC_SCHEMA_MAX it
        // is refused whole — a cut directive names a different path.
        43, schema, str_chunked, 0
            => |s, d, len| {
                let at = s.schema_len as usize;
                if at + len > super::DOC_SCHEMA_MAX {
                    s.schema_len = 0;
                    s.schema_bad = 1;
                } else if s.schema_bad == 0 {
                    ptr_copy(s.schema.as_mut_ptr().add(at), d, len);
                    s.schema_len = (at + len) as u16;
                }
            };

        19, blob, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.blob_len = n as u8;
                if n > 0 { ptr_copy(s.blob.as_mut_ptr(), d, n); }
            };

        18, tomb, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.tomb_len = n as u8;
                if n > 0 { ptr_copy(s.tomb.as_mut_ptr(), d, n); }
            };

        // How long a tomb stays, ms (default TOMB_MS).
        48, tomb_ms, u32, 300000
            => |s, d, len| { s.tomb_ms = super::p_u32(d, len, 0, super::TOMB_MS as u32).max(1); };

        49, ep_docs, str, 0
            => |s, d, len| {
                let n = if len > 192 { 0 } else { len };
                s.ep_docs_len = n as u8;
                if n > 0 { ptr_copy(s.ep_docs.as_mut_ptr(), d, n); }
            };

        24, cover, str, 0
            => |s, d, len| {
                let n = if len > 96 { 0 } else { len };
                s.cover_len = n as u8;
                if n > 0 { ptr_copy(s.cover.as_mut_ptr(), d, n); }
            };

        16, append_cap, u32, 1024
            => |s, d, len| { s.append_cap = super::p_u32(d, len, 0, 1024); };

        47, append_tally, str, 0
            => |s, d, len| {
                let n = if len > 96 { 0 } else { len };
                s.append_tally_len = n as u8;
                if n > 0 { ptr_copy(s.append_tally.as_mut_ptr(), d, n); }
            };

        17, echo, str, 0
            => |s, d, len| {
                s.echo_len = 0;
                let mut v: u32 = 0;
                let mut any = false;
                let mut i = 0usize;
                while i <= len {
                    let c = if i < len { *d.add(i) } else { b',' };
                    if c.is_ascii_digit() {
                        v = v * 10 + (c - b'0') as u32;
                        any = true;
                    } else if c == b',' {
                        if any && v <= 255 && (s.echo_len as usize) < super::ECHO_MAX {
                            s.echo[s.echo_len as usize] = v as u8;
                            s.echo_len += 1;
                        }
                        v = 0;
                        any = false;
                    }
                    i += 1;
                }
            };

        13, wild, str, 0
            => |s, d, len| { super::parse_wild(s, d, len); };

        14, hash, str, 0
            => |s, d, len| {
                let n = if len > 48 { 48 } else { len };
                s.hash_len = n as u8;
                if n > 0 { ptr_copy(s.hash.as_mut_ptr(), d, n); }
            };

        // `blob`: the container's own bytes, in chunks of at most 255, the
        // first led by its whole length (chronicle blob_core). One cut short
        // or past its buffer is a fault, never a truncation — a truncated
        // container decodes to a different policy.
        15, decision, blob, 0
            => |s, d, len| { s.dec_in.take(&mut s.dec_cont, core::slice::from_raw_parts(d, len)); };

        // The per-child decision OP_COUNT counts by, a `blob` like `decision`.
        21, count_where, blob, 0
            => |s, d, len| { s.cw_in.take(&mut s.cw_cont, core::slice::from_raw_parts(d, len)); };

        46, doc, u32, 0
            => |s, d, len| { s.doc = super::p_u32(d, len, 0, 0); };

        45, count_sel, str, 0
            => |s, d, len| {
                let n = if len > 96 { 0 } else { len };
                s.count_sel_len = n as u8;
                if n > 0 { ptr_copy(s.count_sel.as_mut_ptr(), d, n); }
            };

        22, wall, u32, 0
            => |s, d, len| { s.wall = super::p_u32(d, len, 0, 0); };

        23, merge_existing, u32, 0
            => |s, d, len| { s.merge_existing = super::p_u32(d, len, 0, 0); };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
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
/// instance arena and syscall table.
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
        s.request_in = in_chan;
        s.response_out = out_chan;
        s.served = 0;
        s.journal.init();
        s.out_at = 0;
        s.out_len = 0;
        s.malformed = 0;
        s.unchanged = 0;
        s.paths_len = 0;
        s.dec_in = dec::BlobIn::new();
        s.dec_len = 0;
        s.dec_fault = 0;
        s.dec_dropped = 0;
        s.dec_failed = 0;
        s.cw_in = dec::BlobIn::new();
        s.cw_len = 0;
        s.cw_fault = 0;
        s.count_sel_len = 0;
        s.doc = 0;
        s.wall = 0;
        s.merge_existing = 0;
        s.tomb_len = 0;
        s.ep_docs_len = 0;
        s.ep_memo = core::cell::Cell::new(core::ptr::null_mut());
        s.ep_memo_cap = core::cell::Cell::new(0);
        s.ep_memo_n = core::cell::Cell::new(0);
        s.ep_memo_tag = core::cell::Cell::new(0);
        s.ep_memo_whole = core::cell::Cell::new(false);
        s.blob_len = 0;
        s.patch_keys_len = 0;
        s.patch_keys_bad = 0;
        s.schema_len = 0;
        s.schema_bad = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        if s.dec_in.given() {
            match s.dec_in.whole() {
                Some(n) if dec::scan_decision_container(&s.dec_cont[..n]).is_ok() => {
                    s.dec_len = n as u16;
                }
                _ => s.dec_fault = 1,
            }
        }
        if s.cw_in.given() {
            match s.cw_in.whole() {
                Some(n) if dec::scan_decision_container(&s.cw_cont[..n]).is_ok() => {
                    s.cw_len = n as u16;
                }
                _ => s.cw_fault = 1,
            }
        }
        if s.cw_fault != 0 {
            let sys = &*(syscalls as *const SyscallTable);
            let m = b"[store_effect] FAULT: count_where invalid, past its cap, or needs a builtin not in this build; every COUNT answers 500";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        if s.schema_bad != 0 {
            let sys = &*(syscalls as *const SyscallTable);
            let m = b"[store_effect] schema past its cap - DROPPED, writes are not stamped";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        if s.patch_keys_bad != 0 {
            let sys = &*(syscalls as *const SyscallTable);
            let m = b"[store_effect] patch_keys past PATCH_KEYS_MAX - DROPPED, every list merges atomically";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        if s.dec_fault != 0 {
            let sys = &*(syscalls as *const SyscallTable);
            let m = b"[store_effect] FAULT: decision param invalid, truncated, or needs a builtin not in this build";
            dev_log(sys, 1, m.as_ptr(), m.len());
        }
        0
    }
}

/// Take ONE whole record off `request_in` into `in_buf`: its length.
///
/// Either edge shape delivers ONE whole record. A mailbox hands over a
/// record per write and cannot be peeked (EINVAL). A byte stream is
/// self-framed, as a Chronicle engine frames it: peek, read the length out
/// of the frame header, consume exactly that. The stream is what a producer
/// that emits a page of records per step needs — a scan stalled a record at
/// a time behind a mailbox re-lists between records, and a consumer that
/// deletes what it is handed shifts everything the scan resumes by.
unsafe fn take_request(s: &mut State, sys: &SyscallTable) -> Option<i32> {
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(s.request_in, s.in_buf.as_mut_ptr(), BUF);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(s.request_in, s.in_buf.as_mut_ptr(), BUF)
    } else if pk <= 0 {
        return None;
    } else {
        match dec::frame_len(&s.in_buf[..pk as usize]) {
            Some(fl) => (sys.channel_read)(s.request_in, s.in_buf.as_mut_ptr(), fl),
            // Still arriving.
            None if (pk as usize) < BUF => return None,
            // A buffer's worth and no frame boundary: not a record frame,
            // or one past max_record. Drop what is there and resync.
            None => {
                (sys.channel_read)(s.request_in, s.in_buf.as_mut_ptr(), BUF);
                s.dec_failed = s.dec_failed.wrapping_add(1);
                return None;
            }
        }
    };
    if n <= 0 || s.dec_fault != 0 {
        return None;
    }
    Some(n)
}

/// Write what is owed of the last reply. True when nothing is owed.
unsafe fn flush_reply(s: &mut State) -> bool {
    if s.out_len == 0 || s.response_out < 0 {
        return true;
    }
    let sys = &*s.syscalls;
    let (at, len) = (s.out_at as usize, s.out_len as usize);
    let w = (sys.channel_write)(s.response_out, s.out_buf.as_ptr().add(at), len - at);
    if w > 0 {
        s.out_at += w as u32;
    }
    if s.out_at >= s.out_len {
        s.out_at = 0;
        s.out_len = 0;
        s.served = s.served.wrapping_add(1);
        true
    } else {
        false
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
        if s.syscalls.is_null() || s.request_in < 0 {
            return 0;
        }
        let sys = &*s.syscalls;

        // What is owed goes first; nothing new is read until it has.
        if !flush_reply(s) {
            return 0;
        }
        let waiting = s.journal.held();
        let n = if waiting {
            s.journal.begin();
            s.held_n as i32
        } else {
            match take_request(s, sys) {
                Some(n) => n,
                None => return 0,
            }
        };
        // The fused decision: the arriving record is ITS input, and the
        // outcome it constructs is the request served below. An empty outcome
        // is the decision's filter — nothing is served and nothing answers.
        // A held record serves the request decided when it arrived.
        let req: &[u8] = if s.dec_len > 0 {
            if waiting {
                core::slice::from_raw_parts(s.dec_buf.as_ptr(), s.held_req as usize)
            } else {
                match decide(s, n as usize) {
                    Some(len) => {
                        s.held_req = len as u32;
                        core::slice::from_raw_parts(s.dec_buf.as_ptr(), len)
                    }
                    None => return 0,
                }
            }
        } else {
            core::slice::from_raw_parts(s.in_buf.as_ptr(), n as usize)
        };
        if !waiting {
            s.held_n = n as u32;
            s.journal.reset();
        }
        // Every store call of the serve goes through the record's journal.
        let store = Store::serving(sys, &mut s.journal);
        let sys = &store;

        let cid = frame_i64(req, 1).unwrap_or(0);
        let op = frame_i64(req, 2).unwrap_or(OP_GET);
        // Field 3 may be an INTEGER too (a claim keyed by the address it
        // claims), rendered as decimal like every other key part.
        let mut key3 = [0u8; 20];
        let key = match frame_field(req, 3) {
            Some((TY_BYTES, k)) => k,
            Some((TY_I64, b)) if b.len() == 8 => {
                let v = i64::from_le_bytes(b.try_into().unwrap_or([0; 8]));
                let n = dec(v.unsigned_abs(), &mut key3);
                &key3[..n]
            }
            _ => &[],
        };

        // VALUE PARTS, joined in field order (4..=16). The Chronicle VM cannot
        // concatenate — `ADD` is integer-only and there is no concat builtin —
        // yet a graph routinely needs to join a literal to a projected field
        // ("ready=1;endpoint=" + svc + ":" + port). Joining bytes is a
        // connector-level primitive, the writev of this seam, and carries no
        // domain knowledge: absent parts contribute nothing, so one decision
        // branch can emit four parts and another just one.
        // A value part that is an INTEGER is rendered as decimal digits. The
        // VM computes numbers (a deadline, a replica count, a backoff) and
        // cannot render one; joining bytes is already this connector's job,
        // and a number is bytes once someone writes it down.
        // 80..=99 continue the range, joined after 16, for a record of more
        // parts than thirteen (a Pod's node ask: its argv, env, limits).
        // A value past `MAX_VALUE` is refused whole (413), never written cut:
        // a cut record reads as a shorter, valid one (a manifest naming
        // fewer layers than it has).
        let mut vbuf = [0u8; MAX_VALUE];
        let mut vlen = 0usize;
        let mut value_over = false;
        for f in (4u8..=16).chain(80u8..=99) {
            match frame_field(req, f) {
                Some((TY_BYTES, part)) => {
                    if part.len() > MAX_VALUE - vlen {
                        value_over = true;
                    }
                    let n = part.len().min(MAX_VALUE - vlen);
                    vbuf[vlen..vlen + n].copy_from_slice(&part[..n]);
                    vlen += n;
                }
                Some((TY_I64, b)) if b.len() == 8 => {
                    let v = i64::from_le_bytes(b.try_into().unwrap_or([0; 8]));
                    let mut digits = [0u8; 24];
                    let neg = v < 0;
                    let mut u = v.unsigned_abs();
                    let mut i = digits.len();
                    if u == 0 {
                        i -= 1;
                        digits[i] = b'0';
                    }
                    while u > 0 && i > 0 {
                        i -= 1;
                        digits[i] = b'0' + (u % 10) as u8;
                        u /= 10;
                    }
                    if neg && i > 0 {
                        i -= 1;
                        digits[i] = b'-';
                    }
                    let part = &digits[i..];
                    if part.len() > MAX_VALUE - vlen {
                        value_over = true;
                    }
                    let n = part.len().min(MAX_VALUE - vlen);
                    vbuf[vlen..vlen + n].copy_from_slice(&part[..n]);
                    vlen += n;
                }
                _ => {}
            }
        }
        let value = &vbuf[..vlen];
        // A write of a STAGED body: the value named by its digest.
        let mut staged: *mut u8 = core::ptr::null_mut();
        let mut staged_bad = false;
        let value: &[u8] = if s.blob_len > 0
            && matches!(
                op,
                OP_PUT
                    | OP_CREATE
                    | OP_REPLACE
                    | OP_PATCH_MERGE
                    | OP_PATCH_JSON
                    | OP_PATCH_SMP
                    | OP_APPLY
                    | OP_COVER
            )
            && value.starts_with(b"sha256:")
        {
            let mut bk = [0u8; MAX_KEY];
            let bl = s.blob_len as usize;
            let hex = &value[7..];
            if bl + hex.len() <= bk.len() {
                bk[..bl].copy_from_slice(&s.blob[..bl]);
                bk[bl..bl + hex.len()].copy_from_slice(hex);
            }
            match read_whole(sys, &bk[..(bl + hex.len()).min(MAX_KEY)], DOC_MAX) {
                Some((p, n)) => {
                    staged = p;
                    core::slice::from_raw_parts(p, n)
                }
                None => {
                    staged_bad = true;
                    let m = b"[store_effect] a staged body is gone or past the heap - REFUSED";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                    value
                }
            }
        } else {
            value
        };
        // SPLICE (`splice_value`): subtrees copied from a stored document.
        let mut spliced: *mut u8 = core::ptr::null_mut();
        let mut splice_status = 0i64;
        let value: &[u8] = match frame_field(req, F_SPLICE) {
            Some((TY_BYTES, spec)) if !spec.is_empty() && op != OP_NOOP => {
                match splice_value(sys, req, spec) {
                    Ok((p, n)) => {
                        spliced = p;
                        core::slice::from_raw_parts(p, n)
                    }
                    Err(st) => {
                        splice_status = st;
                        value
                    }
                }
            }
            _ => value,
        };
        // The graph configures WHERE this connector writes; a record carries the
        // key TAIL (what `store_source` emits), so the convention stays in the
        // deployment rather than in a decision that would have to concatenate.
        // KEY PARTS: field 3, then 17 and 18, joined after `key_prefix`. Same
        // reason as the value parts — the VM cannot concatenate, and an owner
        // key like `<ns>/<owner>` has to be built from two projected fields and
        // a literal separator that only a decision knows to emit.
        let pl = s.key_prefix_len as usize;
        let mut full = [0u8; MAX_KEY];
        let mut klen = 0usize;
        if pl > 0 {
            full[..pl].copy_from_slice(&s.key_prefix[..pl]);
            klen = pl;
        }
        let mut append_part = |part: &[u8], klen: &mut usize| {
            let n = part.len().min(MAX_KEY - *klen);
            full[*klen..*klen + n].copy_from_slice(&part[..n]);
            *klen += n;
        };
        // Where each key part lands, by field — OP_EXISTS_ANY rebuilds the key
        // with some of them swapped for `*`.
        let mut bounds: [(u8, usize, usize); 12] = [(0, 0, 0); 12];
        let mut nb = 0usize;
        let k0 = klen;
        append_part(key, &mut klen);
        bounds[nb] = (3, k0, klen);
        nb += 1;
        // 25, 26 and 27 extend the same idea: a projection key like
        // `/authz/<subject>/<verb>/<resource>` is five parts and two literal
        // separators, and two extra parts could not build it. They are the
        // WIDEST the key ranges go — the carry range starts at 30 and the gap
        // is deliberate.
        // An INTEGER key part renders as decimal, as a value part does: an
        // allocator computes a candidate address as a number and keys its
        // claim by it, and the VM can compute a number but not render one.
        // 20..24 extend it again, appended AFTER 27 so every key built before
        // them is unchanged: a grant key `/rbac-g/<id>/<ns>|<verb>|<res>` plus
        // its source is more parts than six.
        for f in [17u8, 18, 25, 26, 27, 20, 21, 22, 23, 24] {
            let start = klen;
            match frame_field(req, f) {
                Some((TY_BYTES, part)) => append_part(part, &mut klen),
                Some((TY_I64, b)) if b.len() == 8 => {
                    let v = i64::from_le_bytes(b.try_into().unwrap_or([0; 8]));
                    let mut digits = [0u8; 20];
                    let n = dec(v.unsigned_abs(), &mut digits);
                    append_part(&digits[..n], &mut klen);
                }
                _ => {}
            }
            if nb < bounds.len() {
                bounds[nb] = (f, start, klen);
                nb += 1;
            }
        }
        let key = &full[..klen];
        let key = if klen == pl { &full[..0] } else { key };
        // A subresource is the object through a view (`<key>#scale`): stored
        // under the key, answered and written as the view says.
        let (key, view) = view_split(key);
        // The writer's marks after the view (views.rs `view_args`): its field
        // manager, and `!` — a DRY RUN: every write computed as it would be
        // (stamps, patches, identity) and answered, none stored.
        let (view, fmgr, force, dry) = view_args(view);
        let mut dry_doc = [0u8; MAX_VALUE];
        dry_doc[0] = 0;
        // Who writes, for the schema's `owners` (fieldset.rs): the API's
        // writes only.
        let mut now = [0u8; 20];
        rfc3339(now_millis(sys), &mut now);
        // Read in place: nothing below writes the schema.
        let sch = core::slice::from_raw_parts(s.schema.as_ptr(), s.schema_len as usize);
        let mut pk = [0u8; PATCH_KEYS_MAX];
        let pkl = s.patch_keys_len as usize;
        pk[..pkl].copy_from_slice(&s.patch_keys[..pkl]);
        let writer = FsWriter {
            mgr: if fmgr.is_empty() {
                &b"unknown"[..]
            } else {
                fmgr
            },
            sub: view,
            force,
            now: &now,
            keys: &pk[..pkl],
            sch,
        };
        let fw = (s.stamp != 0).then_some(&writer);
        let mut note = [0u8; MAX_VALUE];
        let mut note_n = 0usize;
        // Server-side apply is a create or a strategic merge, by what exists.
        let applying = op == OP_APPLY;
        let applied_create = applying && !exists(sys, key);
        let op = match op {
            OP_APPLY if applied_create => OP_CREATE,
            OP_APPLY => OP_PATCH_SMP,
            o => o,
        };

        // Opaque carry-through: fields 30..=39 come back untouched. Kept clear
        // of the value parts (4..16) and the key parts (3, 17, 18, 25..27) so widening
        // either range can never silently start echoing a value fragment.
        // Sized to the WHOLE 30..=39 range. It was 4 while the range was
        // 30..=33; widening the range without widening this silently dropped
        // the fields past the fourth, which reads downstream as a decision
        // whose inputs are empty rather than as an error.
        // Echoed with their TYPE: a carried integer comes back an integer, so a
        // number computed before the effect is still a number after it.
        // Field 254 is the record's carried context (chronicle `CARRY_FIELD`),
        // one nested message, echoed with the legacy range.
        let mut carry_buf: [(u8, u8, &[u8]); 11 + ECHO_MAX] = [(0, 0, &[]); 11 + ECHO_MAX];
        let mut nc = 0usize;
        for f in (30u8..=39).chain(core::iter::once(254u8)) {
            if let Some((ty, v)) = frame_field(req, f) {
                carry_buf[nc] = (f, ty, v);
                nc += 1;
            }
        }
        for k in 0..s.echo_len as usize {
            let f = s.echo[k];
            if let Some((ty, v)) = frame_field(req, f) {
                carry_buf[nc] = (f, ty, v);
                nc += 1;
            }
        }
        let carry = &carry_buf[..nc];

        let mut out = [0u8; BUF];
        // NOOP is checked FIRST: it legitimately carries no key, and the
        // malformed-key guard below would otherwise answer 400 — which a
        // downstream decision reads as "not 200", i.e. exactly the failure
        // branch NOOP exists to avoid.
        // Every reply carries the configured projection fields, EMPTY when
        // there is nothing to project — a NOOP's 204 as much as a GET's 200.
        // A decision indexes fields by number, and one absent because of
        // which branch ran upstream fails the whole decision silently.
        let proj = Projection {
            paths: &s.paths[..s.paths_len as usize],
            ints: &s.ints[..s.ints_len as usize],
            flat: s.flat != 0,
            hash: &s.hash[..s.hash_len as usize],
        };
        let rv_path = &s.rv_path[..s.rv_path_len as usize];
        // The key's store revision, for field 59 when `revision = 1`. Set by
        // each branch that knows it; 0 = absent or not asked.
        let mut rev: u64 = 0;
        let flen = if op == OP_NOOP {
            // `doc`: the record's own document is what a NOOP's ints read.
            match frame_field(req, s.doc.min(255) as u8) {
                Some((TY_BYTES, doc)) if s.doc != 0 => {
                    let ints_only = Projection {
                        paths: &[],
                        ints: proj.ints,
                        flat: proj.flat,
                        hash: &[],
                    };
                    let n = reply_projected_from(&mut out, cid, 204, &[], doc, carry, &ints_only);
                    // The paths, empty, at their numbers: a layout that
                    // shifted would be misread.
                    reply_paths_empty(&mut out, n, proj.paths)
                }
                _ => reply_projected(&mut out, cid, 204, &[], carry, &proj),
            }
        } else if value_over {
            // Its parts join past what this connector writes in place.
            let m = b"[store_effect] a value past 6144 bytes - REFUSED (413), not written cut";
            dev_log(sys, 1, m.as_ptr(), m.len());
            reply_projected(&mut out, cid, 413, &[], carry, &proj)
        } else if staged_bad {
            // The body it names is gone (collected) or could not be held:
            // writing the reference itself as the object would be a lie.
            reply_projected(&mut out, cid, 500, &[], carry, &proj)
        } else if splice_status != 0 {
            // What it splices from is gone, or could not be held: a write
            // without its template would be another object.
            reply_projected(&mut out, cid, splice_status, &[], carry, &proj)
        } else if key.is_empty() {
            // A request that names no key is malformed, not a 404: answering
            // 404 would let a caller mistake a broken frame for an absent
            // object and cache the wrong conclusion.
            s.malformed = s.malformed.wrapping_add(1);
            reply_projected(&mut out, cid, 400, &[], carry, &proj)
        } else {
            match op {
                OP_LIST => {
                    // The key is a PREFIX here. `ListWalk` hands one key at a
                    // time and follows the provider's cursor, so a collection
                    // larger than one page is not silently cut off — the
                    // truncation class this repo has already been bitten by
                    // four times.
                    let sep: &[u8] = if s.list_sep_len > 0 {
                        &s.list_sep[..s.list_sep_len as usize]
                    } else {
                        b","
                    };
                    let mut names = [0u8; MAX_VALUE];
                    let mut at = 0usize;
                    let mut walk = ListWalk::new(key);
                    if s.list_open_len > 0 {
                        at = append_lit(
                            &mut names,
                            at,
                            &s.list_open[..s.list_open_len as usize],
                            key,
                        );
                    }
                    let opened = at;
                    let mut overflow = false;
                    let mut vbuf = [0u8; MAX_VALUE];
                    while let Some(k) = walk.next(sys) {
                        // NAMES or VALUES. A collection GET that must answer a
                        // Kubernetes `List` needs the objects, not their keys,
                        // and reading them here is the same prefix walk either
                        // way — asking the graph to fetch each one would be a
                        // store round trip per item.
                        let name: &[u8] = if s.list_values != 0 {
                            let mut kb = [0u8; MAX_KEY];
                            let kl = k.len().min(MAX_KEY);
                            kb[..kl].copy_from_slice(&k[..kl]);
                            // HEAD before GET: a write landing between the two
                            // leaves the value NEWER than the stamped revision,
                            // so a later conditional write fails and retries —
                            // the safe direction to be wrong in.
                            let r = if rv_path.is_empty() {
                                None
                            } else {
                                head(sys, &kb[..kl])
                            };
                            match get_value(sys, &kb[..kl], &mut vbuf) {
                                Some(vl) => match r
                                    .and_then(|r| stamp_rv(&vbuf[..vl], rv_path, r, &mut s.out_buf))
                                {
                                    Some(sl) => &s.out_buf[..sl],
                                    None => &vbuf[..vl],
                                },
                                // Refused (oversized) or vanished mid-walk. A
                                // listing that quietly omits an object reads as
                                // a complete one, so the whole answer fails.
                                None => {
                                    overflow = true;
                                    break;
                                }
                            }
                        } else {
                            last_seg(k)
                        };
                        let need = name.len() + if at == opened { 0 } else { sep.len() };
                        if at + need + s.list_close_len as usize + key.len() > names.len() {
                            // REFUSE rather than answer a short list: a
                            // truncated collection reads as a complete one and
                            // a client concludes the missing objects are gone.
                            overflow = true;
                            break;
                        }
                        if at > opened {
                            at = append(&mut names, at, sep);
                        }
                        at = append(&mut names, at, name);
                    }
                    if s.list_close_len > 0 {
                        at = append_lit(
                            &mut names,
                            at,
                            &s.list_close[..s.list_close_len as usize],
                            key,
                        );
                    }
                    let status = if overflow || walk.failed { 500 } else { 200 };
                    let body: &[u8] = if status == 200 { &names[..at] } else { &[] };
                    reply_projected(&mut out, cid, status, body, carry, &proj)
                }
                OP_EXISTS_ANY => {
                    let wild = &s.wild[..s.wild_len as usize];
                    let mut found = false;
                    // Every subset of the wild fields, the exact key first.
                    let combos = 1u32 << wild.len();
                    let mut probe = [0u8; MAX_VALUE];
                    'combo: for mask in 0..combos {
                        let mut kb = [0u8; MAX_KEY];
                        let mut kl = 0usize;
                        let mut at = 0usize;
                        for &(f, a, b) in bounds.iter().take(nb) {
                            let starred = wild
                                .iter()
                                .position(|&w| w == f)
                                .map(|bit| mask & (1 << bit) != 0)
                                .unwrap_or(false);
                            if starred && &full[a..b] == b"~" {
                                continue 'combo; // a literal is never widened
                            }
                            // The prefix and any literal before this part.
                            kl = append(&mut kb, kl, &full[at..a]);
                            kl = if starred {
                                append(&mut kb, kl, b"*")
                            } else {
                                append(&mut kb, kl, &full[a..b])
                            };
                            at = b;
                        }
                        kl = append(&mut kb, kl, &full[at..klen]);
                        if get_value(sys, &kb[..kl], &mut probe).is_some() {
                            found = true;
                            break;
                        }
                    }
                    reply_projected(
                        &mut out,
                        cid,
                        if found { 200 } else { 404 },
                        &[],
                        carry,
                        &proj,
                    )
                }
                OP_DELETE => {
                    // delete-if-present, so a converged cluster spends no
                    // revisions and a repeated verdict is a no-op. With an
                    // expected revision in field 28 the delete is conditional:
                    // 409 when the object moved since it was read.
                    let want = match frame_i64(req, 28) {
                        Some(r) if r > 0 => r as u64,
                        _ => 0,
                    };
                    let mut probe = [0u8; MAX_VALUE];
                    let small = get_value(sys, key, &mut probe);
                    // An object past a record is there too: read whole where
                    // this build has a heap (store_big), for its guard and its
                    // tomb; without one it is deleted with no tomb (the
                    // DELETED event then carries its identity only).
                    let mut whole: *mut u8 = core::ptr::null_mut();
                    let mut whole_n = 0usize;
                    let past = small.is_none() && !sys.waiting() && exists(sys, key);
                    if past {
                        if let Some((p, n)) = read_whole(sys, key, DOC_MAX) {
                            whole = p;
                            whole_n = n;
                        }
                    }
                    let obj: &[u8] = match small {
                        Some(vl) => &probe[..vl],
                        None if !whole.is_null() => core::slice::from_raw_parts(whole, whole_n),
                        None => &[],
                    };
                    let found = if small.is_some() || past {
                        Some(obj.len())
                    } else {
                        None
                    };
                    // An object with finalizers is not deleted: it is marked
                    // (deletionTimestamp) and stays until the last finalizer
                    // is removed — which a later write completes.
                    let held = s.stamp != 0 && found.is_some() && is_guarded(obj, sch);
                    let status = if dry {
                        // A dry run deletes nothing: whether it would is answered.
                        let status = if found.is_some() { 200 } else { 404 };
                        reply_projected(&mut out, cid, status, &[], carry, &proj)
                    } else if past && whole.is_null() && s.stamp != 0 {
                        // Its guard (finalizers) cannot be read here: refused,
                        // never deleted past one.
                        reply_projected(&mut out, cid, 500, &[], carry, &proj)
                    } else if held {
                        let status = mark_deleting(sys, key, obj, sch);
                        if s.revision != 0 {
                            rev = head(sys, key).unwrap_or(0);
                        }
                        reply_projected(&mut out, cid, status, &[], carry, &proj)
                    } else {
                        if found.is_some() && !obj.is_empty() && s.tomb_len > 0 {
                            leave_tomb(s, sys, key, obj);
                        }
                        let status = if found.is_none() && s.stamp != 0 {
                            404 // the API's delete of nothing: NotFound
                        } else if found.is_none() {
                            200 // already gone: the verdict has nothing to undo
                        } else if want > 0 {
                            match code(delete_cond(sys, key, PRE_ETAG, want)) {
                                0 => 200,
                                ERR_EAGAIN => 409,
                                _ => 500,
                            }
                        } else if done(delete_value(sys, key)) {
                            200
                        } else {
                            500
                        };
                        reply_projected(&mut out, cid, status, &[], carry, &proj)
                    };
                    if !whole.is_null() {
                        heap_free(sys, whole);
                    }
                    status
                }
                OP_CREATE if s.stamp != 0 => {
                    let mut fk = [0u8; MAX_KEY];
                    let (status, fl) = create_stamped(
                        sys,
                        key,
                        value,
                        &mut fk,
                        dry.then_some(&mut dry_doc),
                        rv_path,
                        fw,
                        applied_create,
                        sch,
                    );
                    let fkey = &fk[..fl];
                    // An apply that created answers 201, as upstream does.
                    let status = if applied_create && status == 200 {
                        201
                    } else {
                        status
                    };
                    if s.revision != 0 {
                        rev = head(sys, fkey).unwrap_or(0);
                    }
                    let mut shown = [0u8; MAX_VALUE];
                    let body = if dry {
                        if status == 200 || status == 201 {
                            dry_body(&mut dry_doc, &[], sch)
                        } else {
                            &[]
                        }
                    } else {
                        stored_answer(
                            sys,
                            fkey,
                            &[],
                            if status == 201 { 200 } else { status },
                            rv_path,
                            &mut shown,
                            sch,
                        )
                    };
                    reply_projected(&mut out, cid, status, body, carry, &proj)
                }
                OP_CREATE => {
                    let status = match code(put_cond(sys, key, value, PRE_ABSENT, 0)) {
                        0 => 200,
                        ERR_EEXIST => 409,
                        _ => 500,
                    };
                    if s.revision != 0 {
                        rev = head(sys, key).unwrap_or(0);
                    }
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_PATCH_MERGE | OP_PATCH_JSON | OP_PATCH_SMP => {
                    let keys = &s.patch_keys[..s.patch_keys_len as usize];
                    let status = patch_object(
                        sys,
                        key,
                        op,
                        value,
                        keys,
                        rv_path,
                        dry.then_some(&mut dry_doc),
                        fw,
                        applying,
                        &mut note,
                        &mut note_n,
                        sch,
                    );
                    if s.revision != 0 {
                        rev = head(sys, key).unwrap_or(0);
                    }
                    let mut shown = [0u8; MAX_VALUE];
                    let body = if status == 409 && note_n > 0 {
                        // An apply's conflicts: the Status that names them.
                        &note[..note_n]
                    } else if dry {
                        if status == 200 {
                            dry_body(&mut dry_doc, view, sch)
                        } else {
                            &[]
                        }
                    } else if s.stamp != 0 {
                        let b = stored_answer(sys, key, view, status, rv_path, &mut shown, sch);
                        finish_deletion(s, sys, key, b);
                        b
                    } else {
                        &[]
                    };
                    reply_projected(&mut out, cid, status, body, carry, &proj)
                }
                OP_REPLACE if !view.is_empty() => {
                    // A PUT to a subresource changes only what the view owns.
                    let mut vp = [0u8; MAX_VALUE];
                    let status = match sch_view_patch(sch, view, rv_path, value, &mut vp) {
                        Some(l) => patch_object(
                            sys,
                            key,
                            OP_PATCH_MERGE,
                            &vp[..l],
                            &[],
                            rv_path,
                            dry.then_some(&mut dry_doc),
                            fw,
                            false,
                            &mut note,
                            &mut note_n,
                            sch,
                        ),
                        None => 422,
                    };
                    if s.revision != 0 {
                        rev = head(sys, key).unwrap_or(0);
                    }
                    let mut shown = [0u8; MAX_VALUE];
                    let body = if dry {
                        if status == 200 {
                            dry_body(&mut dry_doc, view, sch)
                        } else {
                            &[]
                        }
                    } else {
                        stored_answer(sys, key, view, status, rv_path, &mut shown, sch)
                    };
                    reply_projected(&mut out, cid, status, body, carry, &proj)
                }
                OP_REPLACE => {
                    let want = expected_rev(req, value, rv_path);
                    // What a replace carries across: the stored object's
                    // `keep` paths, whatever the client sent.
                    let kept = if s.stamp != 0 {
                        keep_identity(sys, key, value, fw, sch)
                    } else {
                        None
                    };
                    let value: &[u8] = match kept {
                        Some((_, d, n)) => core::slice::from_raw_parts(d, n),
                        None => value,
                    };
                    let status = if s.stamp != 0 && !dry && !exists(sys, key) {
                        // Upstream replaces what exists; nothing there is 404.
                        404
                    } else if dry {
                        match head(sys, key) {
                            Some(r) if want == 0 || want == r => {
                                dry_answer(value, r, rv_path, &mut dry_doc)
                            }
                            Some(_) => 409,
                            None => 404,
                        }
                    } else if want == 0 {
                        // No token stated: a Kubernetes PUT without a
                        // resourceVersion is an unconditional update.
                        match put_guarded(sys, key, value) {
                            (false, _) => 500,
                            (true, true) => 200,
                            (true, false) => 204,
                        }
                    } else {
                        match code(put_cond(sys, key, value, PRE_ETAG, want)) {
                            0 => 200,
                            // The key moved, or is gone: either way the
                            // client's copy is stale and it must re-read.
                            ERR_EAGAIN | ERR_EEXIST => 409,
                            _ => 500,
                        }
                    };
                    if let Some((p, _, _)) = kept {
                        heap_free(sys, p);
                    }
                    if s.revision != 0 {
                        rev = head(sys, key).unwrap_or(0);
                    }
                    let mut shown = [0u8; MAX_VALUE];
                    let body = if dry {
                        if status == 200 {
                            dry_body(&mut dry_doc, view, sch)
                        } else {
                            &[]
                        }
                    } else if s.stamp != 0 {
                        let b = stored_answer(sys, key, view, status, rv_path, &mut shown, sch);
                        finish_deletion(s, sys, key, b);
                        b
                    } else {
                        &[]
                    };
                    reply_projected(&mut out, cid, status, body, carry, &proj)
                }
                OP_MERGE => {
                    // With an expected revision in field 28 the merge is into
                    // THAT record: 409 when it moved or is gone, so a merge
                    // never recreates a record another writer removed.
                    // `merge_existing` takes the revision from the store,
                    // read BEFORE the value: a write between the two then
                    // fails the condition rather than being overwritten.
                    let stated = match frame_i64(req, 28) {
                        Some(r) if r > 0 => r as u64,
                        _ => 0,
                    };
                    let (want, absent) = if stated == 0 && s.merge_existing != 0 {
                        match head(sys, key) {
                            Some(r) if r > 0 => (r, false),
                            _ => (0, true),
                        }
                    } else {
                        (stated, false)
                    };
                    let mut cur = [0u8; MAX_VALUE];
                    let got = get_value(sys, key, &mut cur);
                    let cl = got.unwrap_or(0);
                    let mut merged = [0u8; MAX_VALUE];
                    let ml = flat_merge(&cur[..cl], value, &mut merged);
                    let status = if ml == 0 && !value.is_empty() {
                        s.malformed = s.malformed.wrapping_add(1);
                        400
                    } else if absent {
                        404
                    } else if want == 0 {
                        match put_guarded(sys, key, &merged[..ml]) {
                            (false, _) => 500,
                            (true, true) => 200,
                            (true, false) => 204,
                        }
                    } else if got.is_none() {
                        if exists(sys, key) {
                            413
                        } else {
                            409
                        }
                    } else if merged[..ml] == cur[..cl] {
                        match head(sys, key) {
                            Some(r) if r == want => 204,
                            _ => 409,
                        }
                    } else {
                        match code(put_cond(sys, key, &merged[..ml], PRE_ETAG, want)) {
                            0 => 200,
                            ERR_EAGAIN | ERR_EEXIST => 409,
                            _ => 500,
                        }
                    };
                    if s.revision != 0 {
                        rev = head(sys, key).unwrap_or(0);
                    }
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_PATCH => {
                    // The path rides in field 19 (the key parts 3/17/18 are
                    // untouched), the value in field 4 — as an integer to
                    // render or bytes to place verbatim.
                    let mut cur = [0u8; MAX_VALUE];
                    let cl = get_value(sys, key, &mut cur);
                    // The value is the joined parts, integers rendered.
                    let val: &[u8] = value;
                    let mut segs: [&[u8]; 6] = [&[]; 6];
                    let ns = match frame_field(req, 19) {
                        Some((_, path)) => split_dots(path, &mut segs),
                        None => 0,
                    };
                    let mut edited = [0u8; MAX_VALUE];
                    let el = match (cl, ns) {
                        (None, _) => 0,
                        (Some(cl), 1) => j_set_top(&cur[..cl], segs[0], val, &mut edited),
                        (Some(cl), 2) => j_set2(&cur[..cl], segs[0], segs[1], val, &mut edited),
                        _ => 0,
                    };
                    if cl.is_none() {
                        // Unreadable is not absent: an object past a record
                        // exists, and a caller told 404 would take it as gone.
                        // Where this build has a heap (store_stamped,
                        // store_big) it is patched there: a controller's
                        // status lands on its object whatever the size of
                        // the template it carries.
                        let status = if !exists(sys, key) {
                            404
                        } else {
                            patch_whole(sys, key, &segs[..ns], val)
                        };
                        reply_projected(&mut out, cid, status, &[], carry, &proj)
                    } else if el == 0 {
                        // Nothing settable at the path — or a result past the
                        // record (an object that fit grows past it with the
                        // value set: a 6 KiB Job's status). The latter is
                        // patched on the heap where this build has one, the
                        // former answered 400 there as here; never a cut
                        // object written.
                        let status = patch_whole(sys, key, &segs[..ns], val);
                        if status == 400 {
                            s.malformed = s.malformed.wrapping_add(1);
                        }
                        reply_projected(&mut out, cid, status, &[], carry, &proj)
                    } else {
                        let (ok, wrote) = put_guarded(sys, key, &edited[..el]);
                        let status = if !ok {
                            500
                        } else if wrote {
                            200
                        } else {
                            204
                        };
                        reply_projected(&mut out, cid, status, &[], carry, &proj)
                    }
                }
                OP_COUNT => {
                    // Every child, or with `count_where` those its decision
                    // keeps. A walk that fails, a child present but
                    // unreadable, or a predicate that does not decide answers
                    // 500: a short count reads as a complete one.
                    let mut walk = ListWalk::new(key);
                    let mut n = 0i64;
                    let mut bad = s.cw_fault != 0;
                    // The last child the predicate kept (its key tail):
                    // answered at 46 — WHICH one, when the count is one.
                    let mut last = [0u8; MAX_KEY];
                    let mut last_len = 0usize;
                    while !bad {
                        let Some(k) = walk.next(sys) else {
                            break;
                        };
                        if s.cw_len == 0 {
                            n += 1;
                            continue;
                        }
                        let mut kb = [0u8; MAX_KEY];
                        let kl = k.len().min(MAX_KEY);
                        kb[..kl].copy_from_slice(&k[..kl]);
                        let child = &kb[..kl];
                        let mut v = [0u8; MAX_VALUE];
                        match get_value(sys, child, &mut v) {
                            Some(vl) => {
                                let tail = child.get(key.len()..).unwrap_or(&[]);
                                let selected = count_sel_of(s, req, &v[..vl]);
                                match count_child(s, cid, tail, &v[..vl], carry, &proj, selected) {
                                    Some(w) => {
                                        if w != 0 {
                                            last_len = tail.len().min(MAX_KEY);
                                            for (d, &c) in last.iter_mut().zip(tail.iter()) {
                                                *d = c;
                                            }
                                        }
                                        n = n.saturating_add(w);
                                    }
                                    None => bad = true,
                                }
                            }
                            // Past the stack: counted from a heap copy where
                            // this build has one (store_stamped, store_big) —
                            // a Pod with a real template is a Pod. Gone since
                            // it was listed: not there to count.
                            None if exists(sys, child) => match read_whole(sys, child, DOC_MAX) {
                                Some((p, pn)) => {
                                    let doc = core::slice::from_raw_parts(p, pn);
                                    let tail = child.get(key.len()..).unwrap_or(&[]);
                                    let selected = count_sel_of(s, req, doc);
                                    let got =
                                        count_child(s, cid, tail, doc, carry, &proj, selected);
                                    heap_free(sys, p);
                                    match got {
                                        Some(w) => {
                                            if w != 0 {
                                                last_len = tail.len().min(MAX_KEY);
                                                for (d, &c) in last.iter_mut().zip(tail.iter()) {
                                                    *d = c;
                                                }
                                            }
                                            n = n.saturating_add(w);
                                        }
                                        None => bad = true,
                                    }
                                }
                                None => bad = true,
                            },
                            None => {}
                        }
                    }
                    let status = if bad || walk.failed { 500 } else { 200 };
                    let q = reply(&mut out, cid, status, &[], carry);
                    if q > 0 {
                        match put_field(&mut out, q, 45, TY_I64, &n.to_le_bytes()) {
                            Some(q2) => {
                                out[0] += 1;
                                match put_field(
                                    &mut out,
                                    q2,
                                    46,
                                    TY_BYTES,
                                    last.get(..last_len).unwrap_or(&[]),
                                ) {
                                    Some(q3) => {
                                        out[0] += 1;
                                        q3
                                    }
                                    None => q2,
                                }
                            }
                            None => 0,
                        }
                    } else {
                        0
                    }
                }
                OP_APPEND => {
                    let cap = (s.append_cap as usize).clamp(1, MAX_VALUE);
                    let mut cur = [0u8; MAX_VALUE];
                    let cl = get_value(sys, key, &mut cur).unwrap_or(0);
                    let mut buf = [0u8; MAX_VALUE];
                    let total = cl + value.len();
                    let n = if value.len() >= cap {
                        buf[..cap].copy_from_slice(&value[value.len() - cap..]);
                        cap
                    } else {
                        let keep = cl.min(cap - value.len());
                        buf[..keep].copy_from_slice(&cur[cl - keep..cl]);
                        buf[keep..keep + value.len()].copy_from_slice(value);
                        keep + value.len()
                    };
                    let _ = total;
                    let status = if value.is_empty() {
                        204
                    } else if done(put_value(sys, key, &buf[..n])) {
                        200
                    } else {
                        500
                    };
                    if status == 200 {
                        append_tally(
                            sys,
                            &s.append_tally[..s.append_tally_len as usize],
                            key,
                            value.len(),
                        );
                    }
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_PUT if s.stamp != 0 && key.last() != Some(&b'/') && jp_kind(value) == b'{' => {
                    // A controller's whole-object write under a schema is an
                    // upsert that keeps the object's identity: created
                    // stamped (its uid, creationTimestamp) when absent, and
                    // carrying the stored object's `keep` paths when present.
                    // A controller writes Pods and ReplicaSets straight to
                    // the store, past the API's create; a plain PUT left them
                    // without a uid, and a node keys a Pod's whole lifecycle
                    // on it. Answered as a PUT is: 200 written, 204
                    // unchanged, no body. A value that is not an object (a
                    // controller's own bookkeeping record) is no object to
                    // identify, and is written as any PUT is.
                    let mut status = 0i64;
                    if !exists(sys, key) {
                        let mut fk = [0u8; MAX_KEY];
                        let (st, _) =
                            create_stamped(sys, key, value, &mut fk, None, rv_path, fw, false, sch);
                        // 409: another writer created it first; it exists
                        // now, so it is kept as a present one is.
                        if st != 409 {
                            status = st;
                        }
                    }
                    if status == 0 {
                        let kept = keep_identity(sys, key, value, fw, sch);
                        let v: &[u8] = match kept {
                            Some((_, d, n)) => core::slice::from_raw_parts(d, n),
                            None => value,
                        };
                        let (ok, wrote) = put_guarded(sys, key, v);
                        if let Some((p, _, _)) = kept {
                            heap_free(sys, p);
                        }
                        if ok && !wrote {
                            s.unchanged = s.unchanged.wrapping_add(1);
                        }
                        status = if !ok {
                            500
                        } else if wrote {
                            200
                        } else {
                            204
                        };
                    }
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_DROP_EACH => {
                    let parts = full.get(pl..klen).unwrap_or(&[]);
                    for name in value.split(|&b| b == b',') {
                        if name.is_empty() {
                            continue;
                        }
                        let mut k = [0u8; MAX_KEY];
                        let mut kl = 0usize;
                        for piece in [&full[..pl], name, b"/".as_slice(), parts] {
                            for &b in piece {
                                if let Some(slot) = k.get_mut(kl) {
                                    *slot = b;
                                    kl += 1;
                                }
                            }
                        }
                        let one = k.get(..kl).unwrap_or(&[]);
                        // Whether it is there, whatever its size: a value
                        // past a record (a 1 MiB tomb) is there too.
                        if exists(sys, one) {
                            let _ = done(delete_value(sys, one));
                        }
                    }
                    reply_projected(&mut out, cid, 200, &[], carry, &proj)
                }
                OP_PUT_FAMILY => {
                    let mut ok = true;
                    let mut wrote = false;
                    let (o, w) = put_guarded(sys, key, value);
                    ok &= o;
                    wrote |= w;
                    let mut pre = [0u8; MAX_KEY];
                    let pn = append(&mut pre, 0, key);
                    let pn = append(&mut pre, pn, b"~");
                    if pn == key.len() + 1 {
                        let mut walk = ListWalk::new(pre.get(..pn).unwrap_or(&[]));
                        let mut k = [0u8; MAX_KEY];
                        while let Some(one) = walk.next(sys) {
                            let kl = one.len().min(MAX_KEY);
                            for (d, &c) in k.iter_mut().zip(one.iter()) {
                                *d = c;
                            }
                            let (o, w) = put_guarded(sys, k.get(..kl).unwrap_or(&[]), value);
                            ok &= o;
                            wrote |= w;
                        }
                        ok &= !walk.failed;
                    }
                    let status = if !ok {
                        500
                    } else if wrote {
                        200
                    } else {
                        204
                    };
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_DELETE_FAMILY => {
                    // A family whose listing failed is left whole: 500.
                    let status = if drop_family(sys, key) { 200 } else { 500 };
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_TAKE | OP_CLAIM => {
                    // 200 with the value, taken by this request alone; 404
                    // only when it is not there. A take not decided this
                    // step holds the record (below), and one that could not
                    // be decided is 503, never "missing".
                    if !waiting {
                        s.take_nonce = take_nonce(sys.table(), cid as u64);
                    }
                    let mut val = [0u8; MAX_VALUE];
                    let mut scratch = [0u8; MAX_VALUE];
                    let keep = op == OP_CLAIM;
                    match take_value(sys, key, &s.take_nonce, keep, &mut val, &mut scratch) {
                        Took::Taken(vlen) => reply_projected(
                            &mut out,
                            cid,
                            200,
                            val.get(..vlen).unwrap_or(&[]),
                            carry,
                            &proj,
                        ),
                        Took::Absent => reply_projected(&mut out, cid, 404, &[], carry, &proj),
                        Took::Waiting | Took::Failed => {
                            reply_projected(&mut out, cid, 503, &[], carry, &proj)
                        }
                    }
                }
                OP_EP_DOCS => {
                    let status = ep_docs(s, sys, key);
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                OP_COVER => {
                    let mut why = [0u8; 1536];
                    let (status, n) = if staged_bad {
                        (413, 0)
                    } else {
                        cover(s, sys, req, value, &mut why)
                    };
                    reply_projected(&mut out, cid, status, &why[..n], carry, &proj)
                }
                OP_PUT => {
                    let (ok, wrote) = put_guarded(sys, key, value);
                    if ok && !wrote {
                        s.unchanged = s.unchanged.wrapping_add(1);
                    }
                    // 200 only when something was WRITTEN; an unchanged PUT is
                    // 204, the same "nothing happened" a NOOP answers, so a
                    // downstream stage that records what happened does not
                    // record a write that did not.
                    let status = if !ok {
                        500
                    } else if wrote {
                        200
                    } else {
                        204
                    };
                    reply_projected(&mut out, cid, status, &[], carry, &proj)
                }
                _ => {
                    let mut val = [0u8; MAX_VALUE];
                    // HEAD first, for the same reason the LIST stamps that way.
                    let r = if s.revision != 0 || !rv_path.is_empty() {
                        head(sys, key)
                    } else {
                        None
                    };
                    rev = r.unwrap_or(0);
                    match get_value(sys, key, &mut val) {
                        Some(_) if op == OP_EXISTS => {
                            reply_projected(&mut out, cid, 200, &[], carry, &proj)
                        }
                        Some(vlen) => {
                            let mut stamped = [0u8; MAX_VALUE];
                            let body = match r
                                .and_then(|r| stamp_rv(&val[..vlen], rv_path, r, &mut stamped))
                            {
                                Some(sl) => &stamped[..sl],
                                None => &val[..vlen],
                            };
                            reply_projected(&mut out, cid, 200, body, carry, &proj)
                        }
                        // Present, but past a record: it EXISTS (200), its
                        // value is not answered (no record could carry it),
                        // and its projections come from a heap copy when this
                        // build has one (store_big).
                        None if exists(sys, key) => match read_whole(sys, key, DOC_MAX) {
                            Some((p, n)) => {
                                let doc = core::slice::from_raw_parts(p, n);
                                let f = reply_projected_from(
                                    &mut out,
                                    cid,
                                    200,
                                    &[],
                                    doc,
                                    carry,
                                    &proj,
                                );
                                heap_free(sys, p);
                                f
                            }
                            None => reply_projected(&mut out, cid, 200, &[], carry, &proj),
                        },
                        // A miss STILL projects, into empty fields. Field
                        // numbers must not depend on the outcome: a decision
                        // indexes by number, and comparing a field that is
                        // absent rather than empty fails the whole decision, so
                        // the record silently produces no outcome at all.
                        None => reply_projected(&mut out, cid, 404, &[], carry, &proj),
                    }
                }
            }
        };
        // A read of this record refused for good: what the serve concluded
        // from it — a miss, a listing ended short, a write refused unasked —
        // is not the store's answer, and asking again answers the same. The
        // record is answered 500, at once: never held, never a 404.
        let flen = if s.journal.refusal().is_some() {
            reply_projected(&mut out, cid, 500, &[], carry, &proj)
        } else {
            flen
        };
        if !staged.is_null() {
            heap_free(sys, staged);
        }
        if !spliced.is_null() {
            heap_free(sys, spliced);
        }
        if s.journal.held() {
            // A write of this record is undecided: nothing is answered, and
            // the record is served again next step.
            return 0;
        }
        // Answered: the pre-images kept for a replay are not needed.
        s.journal.settle();
        // Field 59 on EVERY reply when asked for, miss or hit: a decision
        // indexes by number, and a field that exists only on some outcomes
        // silently kills the decision on the others.
        let flen = if s.revision != 0 {
            append_rev(&mut out, flen, rev)
        } else {
            flen
        };
        if flen == 0 {
            return 0;
        }
        // A TERMINAL writer leaves `response_out` unwired: the last node in a
        // chain has nobody to answer, and demanding a sink there would put a
        // `debug` node in a production graph purely to swallow replies.
        if s.response_out >= 0 {
            s.out_buf[..flen].copy_from_slice(&out[..flen]);
            s.out_at = 0;
            s.out_len = flen as u32;
            flush_reply(s);
        } else {
            s.served = s.served.wrapping_add(1);
        }
        // Return 0, ALWAYS. A non-zero `module_step` return means DONE: the
        // scheduler stops stepping the module. Returning 1 to mean "I did
        // work" retires it after its first record, which presents as a
        // downstream stall rather than as the module being finished.
        0
    }
}
