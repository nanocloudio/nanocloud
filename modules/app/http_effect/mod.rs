//! Chronicle connector for an HTTP-speaking module in the same graph.
//!
//! See `manifest.toml` for the wire layouts and the rationale. A record comes
//! in, one exchange goes out to the HTTP module, its answer comes back as a
//! record. It decides nothing and knows no endpoint.

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
include!("../_shared/exchange_call.rs");

/// Chronicle record-frame value types (`pipeline_core.rs`).
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const BUF: usize = 8192;
const MAX_PATH: usize = 512;
const MAX_BODY: usize = 4096;
const MAX_PATHS: usize = 8;
const MAX_PATHS_SPEC: usize = 256;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
/// The record's carried context (chronicle `CARRY_FIELD`): one nested
/// message, echoed unchanged beside the legacy 30..=39 range.
const CARRY_FIELD: u8 = 254;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_in: i32,
    request_out: i32,
    response_out: i32,
    resolved: u32,

    paths: [u8; MAX_PATHS_SPEC],
    paths_len: u16,

    /// The exchange in flight; one at a time.
    call: Call,
    held: [u8; BUF],
    held_len: u16,
    /// A built reply record the chain's ring has not taken yet
    /// (`out_buf[..reply_len]`), placed before anything else is read.
    reply_len: u16,

    sent: u32,
    answered: u32,
    dropped: u32,

    in_buf: [u8; BUF],
    out_buf: [u8; BUF],
}

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PATHS_SPEC;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, paths, str, 0
            => |s, d, len| {
                let n = if len > MAX_PATHS_SPEC { MAX_PATHS_SPEC } else { len };
                s.paths_len = n as u16;
                if n > 0 { ptr_copy(s.paths.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
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
    match frame_field(frame, number) {
        Some((TY_I64, b)) if b.len() == 8 => Some(i64::from_le_bytes(b.try_into().unwrap())),
        _ => None,
    }
}

/// Byte length of the first record frame in `data`, or None while truncated.
fn frame_len(data: &[u8]) -> Option<usize> {
    let count = *data.first()? as usize;
    let mut off = 1usize;
    for _ in 0..count {
        if off + 4 > data.len() {
            return None;
        }
        off += 4 + u16::from_le_bytes([data[off + 2], data[off + 3]]) as usize;
    }
    (off <= data.len()).then_some(off)
}

/// Decimal digits of `v` into `out`; the length.
fn dec(v: i64, out: &mut [u8; 24]) -> usize {
    let mut tmp = [0u8; 24];
    let mut i = tmp.len();
    let mut u = v.unsigned_abs();
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    if v < 0 {
        i -= 1;
        tmp[i] = b'-';
    }
    let n = tmp.len() - i;
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// Join fields `lo..=hi` in field order into `out`: bytes as they are, an
/// integer as decimal. None when the join would not fit — a truncated path or
/// body is a different request.
fn join(frame: &[u8], lo: u8, hi: u8, out: &mut [u8]) -> Option<usize> {
    let mut n = 0usize;
    for f in lo..=hi {
        let mut digits = [0u8; 24];
        let part: &[u8] = match frame_field(frame, f) {
            Some((TY_BYTES, b)) => b,
            Some((TY_I64, b)) if b.len() == 8 => {
                let k = dec(i64::from_le_bytes(b.try_into().unwrap()), &mut digits);
                &digits[..k]
            }
            _ => continue,
        };
        if n + part.len() > out.len() {
            return None;
        }
        out[n..n + part.len()].copy_from_slice(part);
        n += part.len();
    }
    Some(n)
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

/// The reply record: correlation, status, body, the `paths` members, carry.
fn build_reply(out: &mut [u8], held: &[u8], status: i64, body: &[u8], paths: &[u8]) -> usize {
    let cid = frame_i64(held, 1).unwrap_or(0);
    let mut p = 1usize;
    let mut n = 0u8;
    macro_rules! field {
        ($e:expr) => {
            match $e {
                Some(v) => {
                    p = v;
                    n += 1;
                }
                None => return 0,
            }
        };
    }
    field!(put_field(out, p, 1, TY_I64, &cid.to_le_bytes()));
    field!(put_field(out, p, 2, TY_I64, &status.to_le_bytes()));
    field!(put_field(out, p, 3, TY_BYTES, body));
    let mut num = 4u8;
    for key in paths.split(|&b| b == b',') {
        if key.is_empty() || num as usize >= 4 + MAX_PATHS {
            continue;
        }
        let v = j_get1(body, key).unwrap_or(&[]);
        field!(put_field(out, p, num, TY_BYTES, v));
        num += 1;
    }
    for f in (CARRY_LO..=CARRY_HI).chain(core::iter::once(CARRY_FIELD)) {
        if let Some((ty, v)) = frame_field(held, f) {
            field!(put_field(out, p, f, ty, v));
        }
    }
    out[0] = n;
    p
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
/// null when `params_len == 0`). All are valid for the lifetime the loader
/// guarantees; never called concurrently.
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
        // First input and first output in manifest order.
        s.request_in = in_chan;
        s.request_out = out_chan;
        s.response_in = -1;
        s.response_out = -1;
        s.resolved = 0;
        s.paths_len = 0;
        s.call.reset();
        s.held_len = 0;
        s.reply_len = 0;
        s.sent = 0;
        s.answered = 0;
        s.dropped = 0;
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
        if s.syscalls.is_null() {
            return 0;
        }
        let sys = &*s.syscalls;
        if s.resolved == 0 {
            s.response_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.response_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.resolved = 1;
        }
        if s.request_in < 0 || s.request_out < 0 || s.response_in < 0 || s.response_out < 0 {
            return 0;
        }
        // A reply the chain has not taken goes first.
        if s.reply_len != 0 && !flush_reply(s, sys) {
            return 0;
        }
        flush_call(s, sys);
        drain_answers(s, sys);
        if s.reply_len == 0 && s.call.idle() {
            admit(s, sys);
        }
        0
    }
}

/// Place what the call owes the HTTP module: its request (first sent, or
/// asked again after a `LINK UP`), or the ABORT for an answer it refused.
unsafe fn flush_call(s: &mut State, sys: &SyscallTable) {
    let mut abort = [0u8; 32];
    if let Some(n) = s.call.abort_record(&mut abort) {
        (sys.channel_write)(s.request_out, abort.as_ptr(), n);
    }
    if let Some(req) = s.call.outgoing() {
        if (sys.channel_write)(s.request_out, req.as_ptr(), req.len()) > 0 {
            s.call.sent();
            s.sent = s.sent.wrapping_add(1);
        }
    }
}

/// Place the held reply record. True when nothing is left owed.
unsafe fn flush_reply(s: &mut State, sys: &SyscallTable) -> bool {
    let n = s.reply_len as usize;
    if (sys.channel_write)(s.response_out, s.out_buf.as_ptr(), n) > 0 {
        s.reply_len = 0;
        s.answered = s.answered.wrapping_add(1);
        return true;
    }
    false
}

/// Read the module's records; a call that ends becomes one reply record:
/// the answer's status and body, or the status the exchange ended with and
/// no body.
unsafe fn drain_answers(s: &mut State, sys: &SyscallTable) {
    loop {
        if s.reply_len != 0 {
            return;
        }
        let poll = (sys.channel_poll)(s.response_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.response_in, s.in_buf.as_mut_ptr(), BUF);
        if n <= 0 {
            return;
        }
        let rec = core::slice::from_raw_parts(s.in_buf.as_ptr(), n as usize);
        match s.call.take(rec) {
            CallEvent::Answered => {
                let (status, body) = s.call.answer();
                // Read through a raw place: the answer stays in the call,
                // which nothing below touches.
                let body = core::slice::from_raw_parts(body.as_ptr(), body.len());
                reply(s, sys, i64::from(status), body);
            }
            CallEvent::Failed(status) => reply(s, sys, i64::from(status), &[]),
            CallEvent::Nothing | CallEvent::LinkDown | CallEvent::LinkUp => {}
        }
    }
}

/// Emit the reply for the held request.
unsafe fn reply(s: &mut State, sys: &SyscallTable, status: i64, body: &[u8]) {
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let paths = core::slice::from_raw_parts(s.paths.as_ptr(), s.paths_len as usize);
    let mut out = [0u8; BUF];
    let flen = build_reply(&mut out, held, status, body, paths);
    s.held_len = 0;
    if flen == 0 {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    s.out_buf[..flen].copy_from_slice(&out[..flen]);
    s.reply_len = flen as u16;
    flush_reply(s, sys);
}

/// Take one whole record off `request_in` into `held`: a mailbox cannot be
/// peeked (EINVAL) and is read whole; a byte stream is self-framed.
unsafe fn take_record(s: &mut State, sys: &SyscallTable) -> Option<usize> {
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(s.request_in, s.held.as_mut_ptr(), BUF);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), BUF)
    } else if pk <= 0 {
        return None;
    } else {
        match frame_len(&s.held[..pk as usize]) {
            Some(fl) => (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), fl),
            None if (pk as usize) < BUF => return None,
            None => {
                (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), BUF);
                s.dropped = s.dropped.wrapping_add(1);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}

/// Take one record and open one exchange for it.
unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    let Some(len) = take_record(s, sys) else {
        return;
    };
    s.held_len = len as u16;
    let frame = core::slice::from_raw_parts(s.held.as_ptr(), len);
    let method = frame_i64(frame, 2).unwrap_or(0);
    if method <= 0 || method > 255 {
        s.held_len = 0;
        return; // the decision's filter: nothing to send
    }
    let mut path = [0u8; MAX_PATH];
    let mut body = [0u8; MAX_BODY];
    let (Some(pl), Some(bl)) = (join(frame, 3, 6, &mut path), join(frame, 8, 19, &mut body)) else {
        // A path or body past this connector's bounds is refused, never cut:
        // answered 413 so the chain decides on it.
        reply(s, sys, i64::from(exch::status::TOO_LARGE), &[]);
        return;
    };
    if pl == 0 {
        reply(s, sys, i64::from(exch::status::BAD_REQUEST), &[]);
        return;
    }
    let port = s.request_out;
    let (path, body) = (&path[..pl], &body[..bl]);
    if !s.call.begin(port, |id, out| {
        call_head(id, method as u8, path, b"", body, out)
    }) {
        reply(s, sys, i64::from(exch::status::TOO_LARGE), &[]);
        return;
    }
    flush_call(s, sys);
}
