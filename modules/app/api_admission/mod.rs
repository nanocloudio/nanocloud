//! Admission — the mutating/validating admission gate as a PIC module. Every
//! mutating request crosses admission before the store: it is a pure function
//! of the request plus the policy + object counts read from the store —
//! validate required fields, apply defaults (mutating the object), and enforce
//! quota. No host facts.
//!
//! Data model (request/response over the store seam; the Chronicle connector
//! seam carries the same fields as a record). The object is REAL nested JSON —
//! the same bytes the store holds and the reply renders; policy fields are
//! dotted JSON paths:
//!   /admit-req/<reqid>         = "verb=<create|update>;resource=<r>;ns=<ns>;obj=<nested JSON>"
//!   /admission-policy/<r>      = "required=<a.b.c>,...;defaults=<a.b>=<jsonval>,...;quota=<n>"
//!   /<resource>/<ns>/<name>    = <the live objects counted for quota>
//!   /admit-resp/<reqid>        = "200;<mutated JSON obj>"  |  "403;<reason>"
//!
//! A `required` field is present iff its dotted path resolves in the object
//! (scalar or sub-object). A `default` inserts a raw JSON value token at its
//! dotted path (depth ≤2: top-level or one-nested, e.g. `spec.priority`) only
//! when absent. Order: reject a missing required field; else add any missing
//! default (the mutation); else, on create, reject if the live count under
//! /<resource>/<ns>/ already meets the quota. No policy for a resource → admit
//! unchanged (the nested object passes through verbatim).

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

// Nested-JSON object model (shared, no_std, allocator-free path reader/mutator).
// Objects reaching admission are real k8s JSON; policy checks/defaults navigate
// them via the array-free j_* wrappers (NEVER literal-array paths — they
// const-promote to a nested pointer table that segfaults in PIC).
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes + CAS, storage.namespace
// (0x13) prefix LIST + change SUBSCRIBE. Changes are pushed to us as
// namespace.change on the `changes` input channel (allocated by a graph
// self-edge); we drain them to detect movement, then re-service.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

/// Chronicle record-frame value types (`pipeline_core.rs`).
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const REC_BUF: usize = 8192;
const EVENT_HEADER_SIZE: usize = 32;

const REQ_PREFIX: &[u8] = b"/admit-req/";
const RESP_PREFIX: &[u8] = b"/admit-resp/";
const POLICY_PREFIX: &[u8] = b"/admission-policy/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start service pass have run.
    subscribed: u8,
    admissions: u32,
    /// The connector seam. -1 when the graph left these unwired.
    request_in: i32,
    response_out: i32,
}

// ---- store ops ----

/// Live key count under `prefix` (LIST then count) — used for quota.
unsafe fn live_count(sys: &SyscallTable, prefix: &[u8]) -> u32 {
    list_count(sys, prefix) as u32
}

// ---- helpers ----

fn parse_u32(b: &[u8]) -> u32 {
    let mut n: u32 = 0;
    for &c in b {
        if c.is_ascii_digit() {
            n = n.wrapping_mul(10).wrapping_add((c - b'0') as u32);
        } else {
            break;
        }
    }
    n
}

fn rest_after<'a>(req: &'a [u8], marker: &[u8]) -> &'a [u8] {
    let mut i = 0;
    while i + marker.len() <= req.len() {
        if &req[i..i + marker.len()] == marker {
            return &req[i + marker.len()..];
        }
        i += 1;
    }
    &req[req.len()..]
}

/// Split a dotted JSON path (`a.b.c`) into its segments, filling `segs` with
/// sub-slices of `path` (runtime slices — never a const-promoted literal array,
/// so PIC-safe). Returns the segment count (capped at `segs.len()`).
fn split_dots<'a>(path: &'a [u8], segs: &mut [&'a [u8]]) -> usize {
    let mut n = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    while i <= path.len() {
        if i == path.len() || path[i] == b'.' {
            if n < segs.len() {
                segs[n] = &path[start..i];
                n += 1;
            }
            start = i + 1;
        }
        i += 1;
    }
    n
}

/// True iff the dotted path resolves in the nested-JSON object — as a scalar
/// (`j_path`) or a sub-object/array (`j_sub`). Empty/over-deep paths return true
/// (un-evaluable → never a false rejection).
fn j_present(obj: &[u8], dotted: &[u8]) -> bool {
    let mut segs: [&[u8]; 8] = [&dotted[..0]; 8];
    let n = split_dots(dotted, &mut segs);
    if n == 0 || n > segs.len() {
        return true;
    }
    let path = &segs[..n];
    j_path(obj, path).is_some() || j_sub(obj, path).is_some()
}

/// Insert `val` (a raw JSON token) at the dotted path, returning the rewritten
/// object length in `out`. Depth 1 (top-level) and 2 (one-nested) are supported
/// via `j_set_top`/`j_set2`; deeper or object-less parents return 0 (no change).
fn apply_default(obj: &[u8], dotted: &[u8], val: &[u8], out: &mut [u8]) -> usize {
    let mut segs: [&[u8]; 8] = [&dotted[..0]; 8];
    let n = split_dots(dotted, &mut segs);
    match n {
        1 => j_set_top(obj, segs[0], val, out),
        2 => j_set2(obj, segs[0], segs[1], val, out),
        _ => 0,
    }
}

/// The key of a `,`-separated `path=val` token (the part before the first `=`).
fn kv_key(kv: &[u8]) -> &[u8] {
    match kv.iter().position(|&b| b == b'=') {
        Some(i) => &kv[..i],
        None => kv,
    }
}

/// The value of a `path=val` token (the part after the first `=`; empty if none).
fn kv_val(kv: &[u8]) -> &[u8] {
    match kv.iter().position(|&b| b == b'=') {
        Some(i) => &kv[i + 1..],
        None => &kv[kv.len()..],
    }
}

/// Iterate `,`-separated tokens of `list`, calling `f(token)`.
fn for_each_csv(list: &[u8], mut f: impl FnMut(&[u8])) {
    let mut start = 0;
    while start <= list.len() {
        let end = list[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(list.len());
        let tok = &list[start..end];
        if !tok.is_empty() {
            f(tok);
        }
        if end >= list.len() {
            break;
        }
        start = end + 1;
    }
}

/// Run the admission decision for one request into `doc`; returns its length.
unsafe fn admit(sys: &SyscallTable, req: &[u8], doc: &mut [u8]) -> usize {
    let verb = field(req, b"verb=").unwrap_or(b"");
    let resource = field(req, b"resource=").unwrap_or(b"");
    let ns = field(req, b"ns=").unwrap_or(b"default");
    // Trailing field: read everything after `;obj=` so a JSON object (which may
    // contain `;`) survives. With no policy the object is admitted unchanged.
    let obj = rest_after(req, b";obj=");

    // Policy for this resource; absent → admit unchanged.
    let mut pkey = [0u8; MAX_KEY];
    let pkl = POLICY_PREFIX.len() + resource.len();
    let mut pbuf = [0u8; MAX_VALUE];
    let policy = if pkl <= pkey.len() {
        pkey[..POLICY_PREFIX.len()].copy_from_slice(POLICY_PREFIX);
        pkey[POLICY_PREFIX.len()..pkl].copy_from_slice(resource);
        get_value(sys, &pkey[..pkl], &mut pbuf).map(|n| &pbuf[..n])
    } else {
        None
    };
    let Some(policy) = policy else {
        let d = append(doc, 0, b"200;");
        return append(doc, d, obj);
    };

    // 1. Validation — every required field (dotted JSON path) must resolve.
    let required = field(policy, b"required=").unwrap_or(b"");
    let mut missing: Option<[u8; 64]> = None;
    let mut missing_len = 0usize;
    for_each_csv(required, |f| {
        if missing.is_none() && !j_present(obj, f) {
            let mut m = [0u8; 64];
            let n = f.len().min(64);
            m[..n].copy_from_slice(&f[..n]);
            missing = Some(m);
            missing_len = n;
        }
    });
    if let Some(m) = missing {
        let d = append(doc, 0, b"403;missing ");
        return append(doc, d, &m[..missing_len]);
    }

    // 2. Defaulting — insert each missing default at its dotted JSON path (the
    // mutation). Each default rewrites the whole object, so ping-pong through a
    // scratch buffer and copy the result back into `mutated`.
    let mut mutated = [0u8; MAX_VALUE];
    let mut ml = append(&mut mutated, 0, obj);
    let mut scratch = [0u8; MAX_VALUE];
    let defaults = field(policy, b"defaults=").unwrap_or(b"");
    let mut start = 0;
    while start <= defaults.len() {
        let end = defaults[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(defaults.len());
        let kv = &defaults[start..end];
        if !kv.is_empty() {
            let dpath = kv_key(kv);
            if !j_present(&mutated[..ml], dpath) {
                let n = apply_default(&mutated[..ml], dpath, kv_val(kv), &mut scratch);
                if n > 0 {
                    ml = append(&mut mutated, 0, &scratch[..n]);
                }
            }
        }
        if end >= defaults.len() {
            break;
        }
        start = end + 1;
    }

    // 3. Quota — on create, reject if the live count already meets it.
    if verb == b"create" {
        if let Some(q) = field(policy, b"quota=").map(parse_u32) {
            let mut qpref = [0u8; MAX_KEY];
            let mut qp = append(&mut qpref, 0, b"/");
            qp = append(&mut qpref, qp, resource);
            qp = append(&mut qpref, qp, b"/");
            qp = append(&mut qpref, qp, ns);
            qp = append(&mut qpref, qp, b"/");
            if live_count(sys, &qpref[..qp]) >= q {
                return append(doc, 0, b"403;quota exceeded");
            }
        }
    }

    let d = append(doc, 0, b"200;");
    append(doc, d, &mutated[..ml])
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

fn put_rec_field(out: &mut [u8], at: usize, number: u8, ty: u8, payload: &[u8]) -> Option<usize> {
    if payload.len() > u16::MAX as usize || at + 4 + payload.len() > out.len() {
        return None;
    }
    out[at] = number;
    out[at + 1] = ty;
    out[at + 2..at + 4].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    out[at + 4..at + 4 + payload.len()].copy_from_slice(payload);
    Some(at + 4 + payload.len())
}

/// Answer one record on the connector seam.
///
/// The flat `verb=…;resource=…;ns=…;obj=…` request `admit` already takes is
/// assembled here rather than being a second admission implementation: what a
/// required field is, what a default does and when a quota bites are answered
/// in exactly one place.
unsafe fn serve_record(s: &mut State, sys: &SyscallTable) {
    if s.request_in < 0 || s.response_out < 0 {
        return;
    }
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    let mut in_buf = [0u8; REC_BUF];
    let n = (sys.channel_read)(s.request_in, in_buf.as_mut_ptr(), REC_BUF);
    if n <= 0 {
        return;
    }
    let req = &in_buf[..n as usize];
    let g = |f: u8| match frame_field(req, f) {
        Some((TY_BYTES, v)) => v,
        _ => &b""[..],
    };
    let (verb, resource, ns, obj) = (g(3), g(4), g(5), g(6));

    let mut doc = [0u8; MAX_VALUE];
    let (status, body): (i64, &[u8]) = if verb.is_empty() {
        // Not a mutating request. A read has no object to validate, and a
        // resource whose policy names a required field would REJECT one — so
        // the skip is real behaviour, not an optimisation. WHICH requests skip
        // is the caller's decision, in params.
        (200, obj)
    } else {
        let mut flat = [0u8; MAX_VALUE];
        let mut fp = 0usize;
        fp = append(&mut flat, fp, b"verb=");
        fp = append(&mut flat, fp, verb);
        fp = append(&mut flat, fp, b";resource=");
        fp = append(&mut flat, fp, resource);
        fp = append(&mut flat, fp, b";ns=");
        fp = append(&mut flat, fp, ns);
        fp = append(&mut flat, fp, b";obj=");
        fp = append(&mut flat, fp, obj);
        let dl = admit(sys, &flat[..fp], &mut doc);
        // `admit` answers "<status>;<body>" — the same string the store lane
        // writes. Split it once, here.
        let semi = doc[..dl].iter().position(|&b| b == b';').unwrap_or(dl);
        let st = parse_u32(&doc[..semi]) as i64;
        (st, &doc[semi.min(dl) + 1..dl])
    };

    let mut out = [0u8; REC_BUF];
    let mut p = 1usize;
    let mut cnt = 0u8;
    macro_rules! put {
        ($num:expr, $ty:expr, $v:expr) => {
            match put_rec_field(&mut out, p, $num, $ty, $v) {
                Some(q) => {
                    p = q;
                    cnt += 1;
                }
                None => return,
            }
        };
    }
    put!(2, TY_I64, &status.to_le_bytes());
    put!(3, TY_BYTES, body);
    for f in 30u8..=39 {
        if let Some((ty, v)) = frame_field(req, f) {
            put!(f, ty, v);
        }
    }
    out[0] = cnt;
    (sys.channel_write)(s.response_out, out.as_ptr(), p);
    s.admissions = s.admissions.wrapping_add(1);
}

/// Service every /admit-req/ without a response yet.
unsafe fn reconcile(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut served = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        if klen <= REQ_PREFIX.len() {
            continue;
        }
        let tail = &key[REQ_PREFIX.len()..];
        let mut rkey = [0u8; MAX_KEY];
        let rlen = RESP_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..RESP_PREFIX.len()].copy_from_slice(RESP_PREFIX);
        rkey[RESP_PREFIX.len()..rlen].copy_from_slice(tail);

        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        let mut req = [0u8; MAX_VALUE];
        let Some(rqlen) = get_value(sys, key, &mut req) else {
            continue;
        };

        let mut doc = [0u8; MAX_VALUE];
        let dlen = admit(sys, &req[..rqlen], &mut doc);
        if put_value(sys, &rkey[..rlen], &doc[..dlen]) {
            served += 1;
        }
    }
    served
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
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
        s.out_chan = out_chan;
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.request_in = -1;
        s.response_out = -1;
        s.admissions = 0;
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

        // Cold start: resolve the change-sink channel (self-edge allocated),
        // SUBSCRIBE the request prefix onto it, then service pre-existing requests.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            // The connector ports, if the graph wired them. Unwired they
            // stay -1 and this module is the store-seam responder it has
            // always been — both apiserver graphs run during the cutover.
            s.request_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.response_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.subscribed = 1;
            s.admissions = s.admissions.wrapping_add(reconcile(sys));
            return 0;
        }

        serve_record(s, sys);

        // A pushed namespace.change means the request set moved — re-service.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.admissions = s.admissions.wrapping_add(reconcile(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
