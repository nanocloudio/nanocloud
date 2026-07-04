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
include!("../_shared/json.rs");
include!("../_shared/pathmod.rs");

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

const MAX_KEY: usize = 256;
const MAX_PREFIX: usize = 64;
const MAX_PATHS_SPEC: usize = 192;
const MAX_VALUE: usize = 4096;
const BUF: usize = 8192;

#[repr(C)]
struct State {
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
    in_buf: [u8; BUF],
    out_buf: [u8; BUF],
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

/// Split one dotted path into segments.
fn split_dots<'a>(spec: &'a [u8], segs: &mut [&'a [u8]; 6]) -> usize {
    let mut n = 0usize;
    let mut s2 = 0usize;
    while s2 <= spec.len() && n < segs.len() {
        let e2 = spec[s2..]
            .iter()
            .position(|&b| b == b'.')
            .map(|i| s2 + i)
            .unwrap_or(spec.len());
        segs[n] = &spec[s2..e2];
        n += 1;
        if e2 >= spec.len() {
            break;
        }
        s2 = e2 + 1;
    }
    n
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
    let Projection { paths, ints, flat } = *proj;
    let n0 = reply(out, cid, status, value, carry);
    if n0 == 0 || (paths.is_empty() && ints.is_empty()) {
        return n0;
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
            let (one, modifier) = split_mod(one);
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(one, &mut segs);
            let got = if flat {
                flat_field(value, one).unwrap_or(&[])
            } else {
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
    // Integer projections, 45..49: the same lookup, parsed. Absent → 0, and
    // still emitted, for the same reason the byte projections are.
    let mut fno = 50u8;
    let mut start = 0usize;
    while start <= ints.len() && fno < 55 {
        let end = ints[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(ints.len());
        let one = &ints[start..end];
        if !one.is_empty() {
            let mut segs: [&[u8]; 6] = [&[]; 6];
            let ns = split_dots(one, &mut segs);
            let raw = if flat {
                flat_field(value, one).unwrap_or(&[])
            } else {
                j_path(value, &segs[..ns]).unwrap_or(&[])
            };
            let v = parse_dec_bytes(raw) as i64;
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

unsafe fn get_value(sys: &SyscallTable, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut garg = [0u8; MAX_KEY];
    if key.is_empty() || key.len() > garg.len() {
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

/// The raw storage.object PUT, in the same arg layout every reconciler uses:
/// the layout is the provider's, the value goes by POINTER (not inline), and
/// the `[precondition][etag_len]` pair is two bytes. Any deviation hands the
/// provider a misaligned pointer and the write is silently lost.
unsafe fn put_value(sys: &SyscallTable, key: &[u8], value: &[u8]) -> bool {
    let mut arg = [0u8; MAX_KEY + MAX_VALUE + 64];
    // `1 + 1` is the precondition PAIR — `[precondition][etag_len]`. The
    // bound counts both bytes: the buffer's slack would otherwise hide an
    // undercount until a long key made it overflow.
    if 2 + key.len() + 1 + 8 + 8 + 1 + 1 + 8 + 2 > arg.len() {
        return false;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p] = 0; // content_type_len
    p += 1;
    arg[p..p + 8].copy_from_slice(&(value.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(value.len() as u64).to_le_bytes());
    p += 8;
    // storage.object writes carry a precondition PAIR — `[precondition:u8]
    // [etag_len:u8]` — two bytes, not one. Every field after it (including
    // `fence_out_ptr`) is positioned off that width, so getting it wrong hands
    // the provider a misaligned pointer and the write is silently lost. `ANY`
    // is the explicit "apply unconditionally".
    arg[p] = 0; // precondition::ANY
    p += 1;
    arg[p] = 0; // etag_len (none, under ANY)
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    (sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p) == 0
}

unsafe fn delete_value(sys: &SyscallTable, key: &[u8]) -> bool {
    let mut arg = [0u8; MAX_KEY + 32];
    if 2 + key.len() + 1 + 8 + 2 > arg.len() {
        return false;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    // storage.object writes carry a precondition PAIR — `[precondition:u8]
    // [etag_len:u8]` — two bytes, not one. Every field after it (including
    // `fence_out_ptr`) is positioned off that width, so getting it wrong hands
    // the provider a misaligned pointer and the write is silently lost. `ANY`
    // is the explicit "apply unconditionally".
    arg[p] = 0; // precondition::ANY
    p += 1;
    arg[p] = 0; // etag_len (none, under ANY)
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    (sys.provider_call)(-1, OBJ_DELETE, arg.as_mut_ptr(), p) == 0
}

/// PUT, guarded: a write whose value already matches is skipped, so a converged
/// cluster spends no revisions and a graph writing under a prefix it also
/// watches does not wake itself forever. Every nanocloud reconciler holds this
/// invariant and a generic connector must not quietly drop it.
/// Returns (ok, wrote).
unsafe fn put_guarded(sys: &SyscallTable, key: &[u8], value: &[u8]) -> (bool, bool) {
    let mut cur = [0u8; MAX_VALUE];
    if let Some(n) = get_value(sys, key, &mut cur) {
        if &cur[..n] == value {
            return (true, false);
        }
    }
    (put_value(sys, key, value), true)
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PATHS_SPEC;
    use super::MAX_PREFIX;
    use super::SCHEMA_MAX;

    define_params! {
        State;

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
        s.malformed = 0;
        s.unchanged = 0;
        s.paths_len = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
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
        if s.syscalls.is_null() || s.request_in < 0 {
            return 0;
        }
        let sys = &*s.syscalls;

        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return 0;
        }
        let n = (sys.channel_read)(s.request_in, s.in_buf.as_mut_ptr(), BUF);
        if n <= 0 {
            return 0;
        }
        let req = core::slice::from_raw_parts(s.in_buf.as_ptr(), n as usize);

        let cid = frame_i64(req, 1).unwrap_or(0);
        let op = frame_i64(req, 2).unwrap_or(OP_GET);
        let key = match frame_field(req, 3) {
            Some((TY_BYTES, k)) => k,
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
        let mut vbuf = [0u8; MAX_VALUE];
        let mut vlen = 0usize;
        for f in 4u8..=16 {
            match frame_field(req, f) {
                Some((TY_BYTES, part)) => {
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
                    let n = part.len().min(MAX_VALUE - vlen);
                    vbuf[vlen..vlen + n].copy_from_slice(&part[..n]);
                    vlen += n;
                }
                _ => {}
            }
        }
        let value = &vbuf[..vlen];
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
        append_part(key, &mut klen);
        for f in [17u8, 18] {
            if let Some((TY_BYTES, part)) = frame_field(req, f) {
                append_part(part, &mut klen);
            }
        }
        let key = &full[..klen];
        let key = if klen == pl { &full[..0] } else { key };

        // Opaque carry-through: fields 30..=39 come back untouched. Kept clear
        // of the value parts (4..16) and the key parts (3, 17, 18) so widening
        // either range can never silently start echoing a value fragment.
        // Sized to the WHOLE 30..=39 range. It was 4 while the range was
        // 30..=33; widening the range without widening this silently dropped
        // the fields past the fourth, which reads downstream as a decision
        // whose inputs are empty rather than as an error.
        // Echoed with their TYPE: a carried integer comes back an integer, so a
        // number computed before the effect is still a number after it.
        let mut carry_buf: [(u8, u8, &[u8]); 10] = [(0, 0, &[]); 10];
        let mut nc = 0usize;
        for f in 30u8..=39 {
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
        };
        let flen = if op == OP_NOOP {
            reply_projected(&mut out, cid, 204, &[], carry, &proj)
        } else if key.is_empty() {
            // A request that names no key is malformed, not a 404: answering
            // 404 would let a caller mistake a broken frame for an absent
            // object and cache the wrong conclusion.
            s.malformed = s.malformed.wrapping_add(1);
            reply_projected(&mut out, cid, 400, &[], carry, &proj)
        } else {
            match op {
                OP_DELETE => {
                    // delete-if-present, so a converged cluster spends no
                    // revisions and a repeated verdict is a no-op.
                    let mut probe = [0u8; MAX_VALUE];
                    let ok = if get_value(sys, key, &mut probe).is_some() {
                        delete_value(sys, key)
                    } else {
                        true // already gone: the verdict has nothing to undo
                    };
                    reply_projected(&mut out, cid, if ok { 200 } else { 500 }, &[], carry, &proj)
                }
                OP_MERGE => {
                    let mut cur = [0u8; MAX_VALUE];
                    let cl = get_value(sys, key, &mut cur).unwrap_or(0);
                    let mut merged = [0u8; MAX_VALUE];
                    let ml = flat_merge(&cur[..cl], value, &mut merged);
                    if ml == 0 && !value.is_empty() {
                        s.malformed = s.malformed.wrapping_add(1);
                        reply_projected(&mut out, cid, 400, &[], carry, &proj)
                    } else {
                        let (ok, wrote) = put_guarded(sys, key, &merged[..ml]);
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
                        reply_projected(&mut out, cid, 404, &[], carry, &proj)
                    } else if el == 0 {
                        s.malformed = s.malformed.wrapping_add(1);
                        reply_projected(&mut out, cid, 400, &[], carry, &proj)
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
                    match get_value(sys, key, &mut val) {
                        Some(_) if op == OP_EXISTS => {
                            reply_projected(&mut out, cid, 200, &[], carry, &proj)
                        }
                        Some(vlen) => {
                            reply_projected(&mut out, cid, 200, &val[..vlen], carry, &proj)
                        }
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
        if flen == 0 {
            return 0;
        }
        // A TERMINAL writer leaves `response_out` unwired: the last node in a
        // chain has nobody to answer, and demanding a sink there would put a
        // `debug` node in a production graph purely to swallow replies.
        if s.response_out >= 0 {
            s.out_buf[..flen].copy_from_slice(&out[..flen]);
            if (sys.channel_write)(s.response_out, s.out_buf.as_ptr(), flen) > 0 {
                s.served = s.served.wrapping_add(1);
            }
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
