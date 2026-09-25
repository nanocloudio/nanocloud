//! Chronicle connector for kagi's `token_mint`.
//!
//! See `manifest.toml` for the wire layouts and the rationale. The short
//! version: `kagi_verify`'s mirror. A record comes in, a `MSG_MINT_REQ` goes
//! out to kagi as one exchange, the signed token comes back as a record. The signing key is a
//! vault LABEL this module names once and never sees; the public half kagi
//! announces is stored for `kagi_verify` to load. It decides nothing.

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
include!("../_shared/store.rs");
include!("../_shared/exchange_call.rs");

// kagi owns the wire and the way a typed operation is called; compiled from
// its published tree, as in `kagi_verify`.
#[path = "../../../target/fluxor/kagi-common/auth_wire.rs"]
mod auth_wire;
#[path = "../../../target/fluxor/kagi-common/typed_exchange.rs"]
mod typed_exchange;
use auth_wire::{ExtraClaims, KeyRecord, MintClaim, MintClaimValue, MintRequest, MintResponse};

/// Chronicle record-frame value types (`pipeline_core.rs`).
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const BUF: usize = 8192;
const MAX_PARAM: usize = 128;
/// A joined subject, audience, scope or claim value.
const MAX_PART: usize = 1024;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;
/// Deepest nesting a raw claim may use. A ServiceAccount claim is three deep.
const MAX_JSON_DEPTH: usize = 8;

const DEFAULT_ISSUER: &[u8] = b"https://kubernetes.default.svc";
const DEFAULT_KID: &[u8] = b"sa";
const DEFAULT_ANNOUNCE: &[u8] = b"/authn-keys/sa";

/// The status a reply carries when the mint gave no typed answer: its
/// exchange ended with a refusal status, an ABORT, or an answer that was not
/// one `MSG_MINT_RESP`. Outside kagi's `mint_err` range, and not OK.
const NO_ANSWER: u8 = 255;

const SUB_LO: u8 = 40;
const SUB_HI: u8 = 43;
const F_AUD: u8 = 44;
const F_SCOPE: u8 = 45;
const F_TTL: u8 = 46;
const F_CLAIM_KEY: u8 = 47;
const CLAIM_LO: u8 = 48;
const CLAIM_HI: u8 = 55;
const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
/// The record's carried context (chronicle `CARRY_FIELD`): one nested
/// message, echoed unchanged beside the legacy 30..=39 range.
const CARRY_FIELD: u8 = 254;

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const EVENT_HEADER_SIZE: usize = 32;
const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    /// An announced key the store has not decided yet: asked again each
    /// step, before another announcement is read.
    ann: [u8; 4096],
    ann_len: u16,
    ann_owed: u8,
    request_in: i32,
    response_in: i32,
    announce_in: i32,
    request_out: i32,
    key_out: i32,
    response_out: i32,
    resolved: u32,

    issuer: [u8; MAX_PARAM],
    issuer_len: u16,
    key_label: [u8; MAX_PARAM],
    key_label_len: u16,
    kid: [u8; MAX_PARAM],
    kid_len: u16,
    announce_path: [u8; MAX_PARAM],
    announce_path_len: u16,
    ttl: u32,
    profile: u32,

    /// Whether the signing record has gone to the mint. Retried each step
    /// until the mint's ring takes it.
    key_sent: u32,

    /// The mint exchange in flight; one at a time, as in `kagi_verify`.
    call: Call,
    held: [u8; BUF],
    held_len: u16,
    /// A built reply record the chain's ring has not taken yet
    /// (`out_buf[..reply_len]`), placed before anything else is read.
    reply_len: u16,

    sent: u32,
    ok: u32,
    refused: u32,
    dropped: u32,
    announced: u32,

    in_buf: [u8; BUF],
    out_buf: [u8; BUF],
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::MAX_PARAM;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, issuer, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { MAX_PARAM } else { len };
                s.issuer_len = n as u16;
                if n > 0 { ptr_copy(s.issuer.as_mut_ptr(), d, n); }
            };

        2, key_label, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { MAX_PARAM } else { len };
                s.key_label_len = n as u16;
                if n > 0 { ptr_copy(s.key_label.as_mut_ptr(), d, n); }
            };

        3, kid, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { MAX_PARAM } else { len };
                s.kid_len = n as u16;
                if n > 0 { ptr_copy(s.kid.as_mut_ptr(), d, n); }
            };

        4, announce_path, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { MAX_PARAM } else { len };
                s.announce_path_len = n as u16;
                if n > 0 { ptr_copy(s.announce_path.as_mut_ptr(), d, n); }
            };

        5, ttl, u32, 3600
            => |s, d, len| { s.ttl = p_u32(d, len, 0, 3600); };

        6, profile, u32, 1
            => |s, d, len| { s.profile = p_u32(d, len, 0, 1); };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

fn or_default<'a>(v: &'a [u8], d: &'a [u8]) -> &'a [u8] {
    if v.is_empty() {
        d
    } else {
        v
    }
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

/// Join the BYTES fields `lo..=hi` of `frame` in field order. None when the
/// join would not fit — a truncated subject is a different subject.
fn join(frame: &[u8], lo: u8, hi: u8, out: &mut [u8; MAX_PART]) -> Option<usize> {
    let mut n = 0usize;
    for f in lo..=hi {
        if let Some((TY_BYTES, part)) = frame_field(frame, f) {
            if n + part.len() > MAX_PART {
                return None;
            }
            out[n..n + part.len()].copy_from_slice(part);
            n += part.len();
        }
    }
    Some(n)
}

/// One complete JSON value, strictly: RFC 8259 grammar, no trailing bytes.
/// kagi embeds a raw claim verbatim, so this is the last place a joined part
/// that closed a string early can be refused.
fn json_valid(b: &[u8]) -> bool {
    fn ws(b: &[u8], mut i: usize) -> usize {
        while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        i
    }
    fn string(b: &[u8], mut i: usize) -> Option<usize> {
        if b.get(i) != Some(&b'"') {
            return None;
        }
        i += 1;
        while i < b.len() {
            match b[i] {
                b'"' => return Some(i + 1),
                b'\\' => match b.get(i + 1)? {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => i += 2,
                    b'u' => {
                        let h = b.get(i + 2..i + 6)?;
                        if !h.iter().all(|c| c.is_ascii_hexdigit()) {
                            return None;
                        }
                        i += 6;
                    }
                    _ => return None,
                },
                c if c < 0x20 => return None,
                _ => i += 1,
            }
        }
        None
    }
    fn number(b: &[u8], mut i: usize) -> Option<usize> {
        let start = i;
        if b.get(i) == Some(&b'-') {
            i += 1;
        }
        let d0 = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == d0 || (b[d0] == b'0' && i - d0 > 1) {
            return None;
        }
        if b.get(i) == Some(&b'.') {
            i += 1;
            let f0 = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i == f0 {
                return None;
            }
        }
        if matches!(b.get(i), Some(b'e') | Some(b'E')) {
            i += 1;
            if matches!(b.get(i), Some(b'+') | Some(b'-')) {
                i += 1;
            }
            let e0 = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i == e0 {
                return None;
            }
        }
        (i > start).then_some(i)
    }
    fn value(b: &[u8], i: usize, depth: usize) -> Option<usize> {
        if depth > MAX_JSON_DEPTH {
            return None;
        }
        let i = ws(b, i);
        match *b.get(i)? {
            b'"' => string(b, i),
            b'{' => {
                let mut j = ws(b, i + 1);
                if b.get(j) == Some(&b'}') {
                    return Some(j + 1);
                }
                loop {
                    j = string(b, ws(b, j))?;
                    j = ws(b, j);
                    if b.get(j) != Some(&b':') {
                        return None;
                    }
                    j = ws(b, value(b, j + 1, depth + 1)?);
                    match b.get(j)? {
                        b',' => j += 1,
                        b'}' => return Some(j + 1),
                        _ => return None,
                    }
                }
            }
            b'[' => {
                let mut j = ws(b, i + 1);
                if b.get(j) == Some(&b']') {
                    return Some(j + 1);
                }
                loop {
                    j = ws(b, value(b, j, depth + 1)?);
                    match b.get(j)? {
                        b',' => j += 1,
                        b']' => return Some(j + 1),
                        _ => return None,
                    }
                }
            }
            b't' => b[i..].starts_with(b"true").then_some(i + 4),
            b'f' => b[i..].starts_with(b"false").then_some(i + 5),
            b'n' => b[i..].starts_with(b"null").then_some(i + 4),
            _ => number(b, i),
        }
    }
    matches!(value(b, 0, 0), Some(end) if ws(b, end) == b.len())
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

/// The reply record: correlation, status, token, then the carry echoed.
fn build_reply(out: &mut [u8], held: &[u8], status: u8, token: &[u8]) -> usize {
    let cid = match frame_field(held, 1) {
        Some((TY_I64, b)) if b.len() == 8 => i64::from_le_bytes(b.try_into().unwrap()),
        _ => 0,
    };
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
    field!(put_field(out, p, 2, TY_I64, &(status as i64).to_le_bytes()));
    field!(put_field(out, p, 3, TY_BYTES, token));
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
        // The first input and first output in manifest order; the rest are
        // resolved by direction-index on the first step.
        s.request_in = in_chan;
        s.request_out = out_chan;
        s.response_in = -1;
        s.announce_in = -1;
        s.key_out = -1;
        s.response_out = -1;
        s.resolved = 0;
        s.issuer_len = 0;
        s.key_label_len = 0;
        s.kid_len = 0;
        s.announce_path_len = 0;
        s.key_sent = 0;
        s.call.reset();
        s.held_len = 0;
        s.reply_len = 0;
        s.sent = 0;
        s.ok = 0;
        s.refused = 0;
        s.dropped = 0;
        s.announced = 0;
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        // No label, no key: a mint that would answer NO_KEY forever is a
        // misconfiguration, and refusing to construct names it.
        if s.key_label_len == 0 {
            let sys = &*s.syscalls;
            let m =
                b"[kagi_mint] FAULT: key_label is required (the vault label of the signing key)";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -3;
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
            s.response_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.announce_in = dev_channel_port(sys, PORT_INPUT, 2);
            s.key_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.response_out = dev_channel_port(sys, PORT_OUTPUT, 2);
            s.resolved = 1;
        }
        if s.key_sent == 0 {
            send_key(s, sys);
        }
        store_announcement(s, sys);
        if s.request_in < 0 || s.response_out < 0 || s.request_out < 0 || s.response_in < 0 {
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

/// Name the signing key to the mint: a SIGN record whose reference is the
/// vault label. ACTIVE, so an empty `kid` in a request selects it.
unsafe fn send_key(s: &mut State, sys: &SyscallTable) {
    if s.key_out < 0 {
        return;
    }
    let rec = KeyRecord {
        issuer: or_default(&s.issuer[..s.issuer_len as usize], DEFAULT_ISSUER),
        profile_id: s.profile as u16,
        kid: or_default(&s.kid[..s.kid_len as usize], DEFAULT_KID),
        suite: auth_wire::suite::ES256,
        state: auth_wire::key_state::ACTIVE,
        key_use: auth_wire::key_use::SIGN,
        generation: 0,
        activate_after_unix: 0,
        remove_after_unix: 0,
        key_ref: &s.key_label[..s.key_label_len as usize],
    };
    let mut out = [0u8; 1024];
    let Ok(n) = rec.encode_add(&mut out) else {
        let m = b"[kagi_mint] FAULT: the signing record does not encode (label too long?)";
        dev_log(sys, 1, m.as_ptr(), m.len());
        s.key_sent = 1; // not retried: it will never encode
        return;
    };
    if (sys.channel_write)(s.key_out, out.as_ptr(), n) > 0 {
        s.key_sent = 1;
    }
}

/// Store the mint's announced verification key, verbatim, where the verifier
/// loads it. Every announcement: a re-announced key (a re-add, a restart) is
/// the one now signing.
unsafe fn store_announcement(s: &mut State, sys: &SyscallTable) {
    if s.announce_in < 0 {
        return;
    }
    if s.ann_owed != 0 {
        ask_announcement(s, sys);
        return;
    }
    let poll = (sys.channel_poll)(s.announce_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return;
    }
    let n = (sys.channel_read)(s.announce_in, s.ann.as_mut_ptr(), s.ann.len());
    if n <= 0 {
        return;
    }
    s.ann_len = n as u16;
    s.ann_owed = 1;
    ask_announcement(s, sys);
}

/// Ask for the owed announcement's write; it stays owed until the store
/// decides it.
unsafe fn ask_announcement(s: &mut State, sys: &SyscallTable) {
    let path = or_default(
        &s.announce_path[..s.announce_path_len as usize],
        DEFAULT_ANNOUNCE,
    );
    let mut p = [0u8; MAX_PARAM];
    p[..path.len()].copy_from_slice(path);
    let a = put_value(
        &Store::plain(sys),
        &p[..path.len()],
        &s.ann[..s.ann_len as usize],
    );
    match a {
        WriteAnswer::Pending => {}
        WriteAnswer::Decided(rc) => {
            s.ann_owed = 0;
            if rc == 0 {
                s.announced = s.announced.wrapping_add(1);
            }
        }
    }
}

/// Place what the call owes the mint: its request (first sent, or asked
/// again after a `LINK UP`), or the ABORT for an answer it refused.
unsafe fn flush_call(s: &mut State, sys: &SyscallTable) {
    let mut abort = [0u8; 32];
    if let Some(n) = s.call.abort_record(&mut abort) {
        (sys.channel_write)(s.request_out, abort.as_ptr(), n);
    }
    if let Some(req) = s.call.outgoing() {
        if (sys.channel_write)(s.request_out, req.as_ptr(), req.len()) > 0 {
            s.call.sent();
        }
    }
}

/// Place the held reply record. True when nothing is left owed.
unsafe fn flush_reply(s: &mut State, sys: &SyscallTable) -> bool {
    let n = s.reply_len as usize;
    if (sys.channel_write)(s.response_out, s.out_buf.as_ptr(), n) > 0 {
        s.reply_len = 0;
        return true;
    }
    false
}

/// Read the mint's records; a call that ends becomes one reply record.
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
            CallEvent::Answered => answered(s, sys),
            CallEvent::Failed(_) => reply(s, sys, NO_ANSWER, b""),
            CallEvent::Nothing | CallEvent::LinkDown | CallEvent::LinkUp => {}
        }
    }
}

/// The call's whole answer: a 200 whose body is one `MSG_MINT_RESP` is the
/// mint's verdict, a refusal included; anything else is no answer at all.
unsafe fn answered(s: &mut State, sys: &SyscallTable) {
    let (status, body) = s.call.answer();
    // Read through a raw place: the answer stays in the call, which nothing
    // below touches, while the reply is built from it.
    let body = core::slice::from_raw_parts(body.as_ptr(), body.len());
    if status == exch::status::OK {
        if let Some((auth_wire::MSG_MINT_RESP, payload)) = typed_exchange::message(body) {
            if let Ok(resp) = MintResponse::decode(payload) {
                reply(s, sys, resp.status, resp.body);
                return;
            }
        }
    }
    reply(s, sys, NO_ANSWER, b"");
}

/// Emit the reply for the held request and free it.
unsafe fn reply(s: &mut State, sys: &SyscallTable, status: u8, token: &[u8]) {
    let mut out = [0u8; BUF];
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let flen = build_reply(&mut out, held, status, token);
    s.held_len = 0;
    if flen == 0 {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    if status == auth_wire::mint_err::OK {
        s.ok = s.ok.wrapping_add(1);
    } else {
        s.refused = s.refused.wrapping_add(1);
    }
    s.out_buf[..flen].copy_from_slice(&out[..flen]);
    s.reply_len = flen as u16;
    flush_reply(s, sys);
}

/// Take one whole record off `request_in` into `held`. Either edge shape: a
/// mailbox cannot be peeked (EINVAL) and is read whole; a byte stream is
/// self-framed by the record header.
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

/// Take one record and open one mint call for it.
unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    let Some(len) = take_record(s, sys) else {
        return;
    };
    s.held_len = len as u16;
    let frame = core::slice::from_raw_parts(s.held.as_ptr(), len);

    let mut sub = [0u8; MAX_PART];
    let mut claim = [0u8; MAX_PART];
    let (Some(sub_n), Some(claim_n)) = (
        join(frame, SUB_LO, SUB_HI, &mut sub),
        join(frame, CLAIM_LO, CLAIM_HI, &mut claim),
    ) else {
        reply(s, sys, auth_wire::mint_err::TOO_LARGE, b"");
        return;
    };
    let bytes_of = |f: u8| match frame_field(frame, f) {
        Some((TY_BYTES, v)) => v,
        _ => &b""[..],
    };
    let claim_key = bytes_of(F_CLAIM_KEY);
    // A claim half-given, or a value that is not one JSON value, is refused
    // here rather than handed to a mint that would embed it verbatim.
    if sub_n == 0
        || (claim_key.is_empty() != (claim_n == 0))
        || (claim_n > 0 && !json_valid(&claim[..claim_n]))
    {
        reply(s, sys, auth_wire::mint_err::MALFORMED, b"");
        return;
    }
    let ttl = match frame_field(frame, F_TTL) {
        Some((TY_I64, b)) if b.len() == 8 => {
            let v = i64::from_le_bytes(b.try_into().unwrap());
            if v > 0 && v <= u32::MAX as i64 {
                v as u32
            } else {
                s.ttl
            }
        }
        _ => s.ttl,
    };
    let extra = [MintClaim {
        key: claim_key,
        value: MintClaimValue::Raw(&claim[..claim_n]),
    }];
    let req = MintRequest {
        request_type: auth_wire::request_type::MINT,
        suite: auth_wire::suite::ES256,
        profile_id: s.profile as u16,
        // Empty: the profile's ACTIVE key, so a rotation needs no new kid here.
        kid: b"",
        ttl_seconds: ttl,
        iss: or_default(&s.issuer[..s.issuer_len as usize], DEFAULT_ISSUER),
        sub: &sub[..sub_n],
        aud: bytes_of(F_AUD),
        scope: bytes_of(F_SCOPE),
        thumbprint_alg: 0,
        jkt: None,
        extra: ExtraClaims::Slice(if claim_n > 0 { &extra[..] } else { &extra[..0] }),
    };
    let mut env = [0u8; BUF];
    let Ok(elen) = req.encode(&mut env) else {
        reply(s, sys, auth_wire::mint_err::TOO_LARGE, b"");
        return;
    };
    let port = s.request_out;
    let env = &env[..elen];
    if !s
        .call
        .begin(port, |id, out| typed_exchange::write_call(id, env, out))
    {
        reply(s, sys, auth_wire::mint_err::TOO_LARGE, b"");
        return;
    }
    s.sent = s.sent.wrapping_add(1);
    flush_call(s, sys);
}
