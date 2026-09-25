//! Chronicle connector for Kagi's storage-key service: a volume key effect
//! in, storage-key requests out as exchanges on an edge inside the control
//! plane's graph, the typed outcome back as a record.
//!
//! See `manifest.toml` for the wire. The effects are `_shared/
//! kagi_storage_ops.rs`, over kagi-common's own codecs; this module moves
//! frames, keeps one effect in flight, runs the provisioning vault, and
//! names the storage-grant signing key's vault label to the service.

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
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/exchange_call.rs");

// Kagi owns the wire; compiled from its published tree.
#[path = "../../../target/fluxor/kagi-common/auth_wire.rs"]
mod auth_wire;
#[path = "../../../target/fluxor/kagi-common/chan.rs"]
mod chan;
#[path = "../../../target/fluxor/fluxor-abi/sdk/contracts/key_vault.rs"]
mod key_vault;
#[path = "../../../target/fluxor/kagi-common/state_wire.rs"]
mod state_wire;
#[path = "../../../target/fluxor/kagi-common/storage_key.rs"]
mod storage_key;
#[path = "../../../target/fluxor/kagi-common/storage_key_service.rs"]
mod storage_key_service;
#[path = "../../../target/fluxor/kagi-common/typed_exchange.rs"]
mod typed_exchange;

use storage_key as sk;
use storage_key_service as svc;

include!("../_shared/kagi_storage_ops.rs");

const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;
const CARRY_FIELD: u8 = 254;
const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;
/// `channel_peek` on a mailbox channel, which has no byte stream to peek.
const CHAN_EINVAL: i32 = -22;

const REC: usize = 8192;
/// A request envelope: the header and the largest body.
const OUT: usize = 3 + 5 + svc::MAX_REQUEST;
/// A custodian's answer, read into the buffer sized for the largest
/// storage-key frame.
const REPLY: usize = 3 + 8 + 3 * 2 + sk::MAX_RECORD + sk::BUNDLE_LEN + 64;
const MAX_LABEL: usize = 64;
const DEFAULT_ACTOR: &[u8] = b"nanocloud";
const DEFAULT_LABEL: &[u8] = b"nanocloud/storage-grant";
const ISSUER: &[u8] = b"nanocloud";
const KID: &[u8] = b"storage-grant";

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_out: i32,
    request_out: i32,
    response_in: i32,
    key_out: i32,
    /// The custodians' orders and results, for their recovery keys when none
    /// are pinned.
    custodian_req: i32,
    custodian_in: i32,
    resolved: u8,
    /// An effect is in flight.
    busy: u8,
    /// 0 not configured yet, 1 ready, 2 refused (bad params).
    configured: u8,
    /// The signing record reached `key_out`.
    key_sent: u8,
    _pad: [u8; 4],

    timeout_ms: u32,
    aead: u32,
    custodians: [u8; CUSTODIANS],
    /// Keys pinned by params, one bit each.
    custodian_set: u8,
    /// Keys the in-graph custodians named, one bit each.
    learned: u8,
    asked_at: u64,
    actor: [u8; MAX_ACTOR],
    actor_len: u8,
    label: [u8; MAX_LABEL],
    label_len: u8,

    op: i64,
    started: u64,
    /// The storage-key exchange in flight.
    call: Call,
    /// The record being answered.
    held: [u8; REC],
    held_len: u16,
    out: [u8; OUT],
    in_buf: [u8; REPLY],
    rec_buf: [u8; REC],
    /// A finished outcome `response_out` has not taken yet.
    reply: [u8; REC],
    reply_len: u16,

    eng: KagiStorage,
    answered: u32,
    dropped: u32,
}

mod params_def {
    use super::p_u32;
    use super::State;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, timeout_ms, u32, 30000
            => |s, d, len| { s.timeout_ms = p_u32(d, len, 0, 30000); };

        // The custody envelopes' AEAD: 1 ChaCha20-Poly1305, 2 AES-256-GCM.
        2, aead, u32, 1
            => |s, d, len| { s.aead = p_u32(d, len, 0, 1); };

        // Who asked, when an effect names no one.
        3, actor, str, 0
            => |s, d, len| { super::set_str(&mut s.actor, &mut s.actor_len, d, len); };

        // The vault label of the storage-grant signing key.
        4, signing_label, str, 0
            => |s, d, len| { super::set_str(&mut s.label, &mut s.label_len, d, len); };

        // The custodians' recovery public keys, uncompressed P-256, hex.
        5, custodian_1, str, 0
            => |s, d, len| { super::set_custodian(s, 0, d, len); };
        6, custodian_2, str, 0
            => |s, d, len| { super::set_custodian(s, 1, d, len); };
        7, custodian_3, str, 0
            => |s, d, len| { super::set_custodian(s, 2, d, len); };
    }
}

unsafe fn set_str(dst: &mut [u8], dst_len: &mut u8, d: *const u8, len: usize) {
    let n = len.min(dst.len());
    dst[..n].copy_from_slice(core::slice::from_raw_parts(d, n));
    *dst_len = n as u8;
}

unsafe fn set_custodian(s: &mut State, i: usize, d: *const u8, len: usize) {
    let hex = core::slice::from_raw_parts(d, len);
    let at = i * sk::PUBLIC_LEN;
    if unhex(hex, &mut s.custodians[at..at + sk::PUBLIC_LEN]) {
        s.custodian_set |= 1 << i;
    }
}

fn unhex(hex: &[u8], out: &mut [u8]) -> bool {
    if hex.len() != 2 * out.len() {
        return false;
    }
    let v = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    for (i, o) in out.iter_mut().enumerate() {
        match (v(hex[2 * i]), v(hex[2 * i + 1])) {
            (Some(h), Some(l)) => *o = (h << 4) | l,
            _ => return false,
        }
    }
    true
}

fn sha256_fn(data: &[u8], out: &mut [u8; 32]) {
    *out = sha256(data);
}

// ---- record frames ----

fn frame_field(frame: &[u8], number: u8) -> Option<(u8, &[u8])> {
    let count = *frame.first()? as usize;
    let mut p = 1usize;
    for _ in 0..count {
        if p + 4 > frame.len() {
            return None;
        }
        let num = frame[p];
        let ty = frame[p + 1];
        let len = u16::from_le_bytes([frame[p + 2], frame[p + 3]]) as usize;
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

fn frame_i64(frame: &[u8], number: u8) -> i64 {
    match frame_field(frame, number) {
        Some((TY_I64, b)) if b.len() == 8 => i64::from_le_bytes(b.try_into().unwrap()),
        _ => 0,
    }
}

fn frame_bytes(frame: &[u8], number: u8) -> &[u8] {
    match frame_field(frame, number) {
        Some((TY_BYTES, b)) => b,
        _ => &[],
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

fn put(out: &mut [u8], at: usize, number: u8, ty: u8, v: &[u8]) -> Option<usize> {
    if v.len() > u16::MAX as usize || at + 4 + v.len() > out.len() {
        return None;
    }
    out[at] = number;
    out[at + 1] = ty;
    out[at + 2..at + 4].copy_from_slice(&(v.len() as u16).to_le_bytes());
    out[at + 4..at + 4 + v.len()].copy_from_slice(v);
    Some(at + 4 + v.len())
}

// ---- the provisioning vault ----

struct SysVault<'a>(&'a SyscallTable);

impl Vault for SysVault<'_> {
    fn generate(&mut self) -> i32 {
        // [suite u16][usage u32][flags u8][pad u8][pub_out_ptr u64]
        // [pub_out_cap u16][pub_len_out u16]: a KDF key has no public half.
        let mut arg = [0u8; 20];
        arg[0..2].copy_from_slice(&key_vault::suite::KDF_KEY.to_le_bytes());
        arg[2..6].copy_from_slice(&key_vault::usage::WRAP.to_le_bytes());
        unsafe { (self.0.provider_call)(-1, key_vault::GENERATE, arg.as_mut_ptr(), arg.len()) }
    }

    fn split(
        &mut self,
        handle: i32,
        arg: &mut [u8; key_vault::share::split::LEN],
        out: &mut [u8; ENVELOPES],
    ) -> i32 {
        use key_vault::share::split as a;
        arg[a::OUT_PTR..a::OUT_PTR + 8].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
        arg[a::OUT_CAP..a::OUT_CAP + 4].copy_from_slice(&(ENVELOPES as u32).to_le_bytes());
        unsafe { (self.0.provider_call)(handle, key_vault::SHARE_SPLIT, arg.as_mut_ptr(), a::LEN) }
    }

    fn destroy(&mut self, handle: i32) {
        unsafe {
            (self.0.provider_call)(handle, key_vault::DESTROY, core::ptr::null_mut(), 0);
        }
    }
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
    if syscalls.is_null() || state.is_null() {
        return -1;
    }
    if state_size < core::mem::size_of::<State>() {
        return -2;
    }
    // The arena is zeroed.
    let s = &mut *(state as *mut State);
    s.syscalls = syscalls as *const SyscallTable;
    s.request_in = in_chan;
    s.response_out = out_chan;
    s.request_out = -1;
    s.response_in = -1;
    s.key_out = -1;
    s.custodian_req = -1;
    s.custodian_in = -1;
    s.eng = KagiStorage::empty();
    s.call.reset();
    params_def::set_defaults(s);
    params_def::parse_tlv(s, params, params_len);
    0
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
            s.request_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.key_out = dev_channel_port(sys, PORT_OUTPUT, 2);
            s.custodian_req = dev_channel_port(sys, PORT_OUTPUT, 3);
            s.custodian_in = dev_channel_port(sys, PORT_INPUT, 2);
            s.resolved = 1;
        }
        if s.configured == 0 {
            configure(s, sys);
        }
        if s.configured != 1 {
            return 0;
        }
        name_key(s, sys);
        if s.request_in < 0 || s.response_out < 0 || s.request_out < 0 || s.response_in < 0 {
            return 0;
        }
        if s.reply_len != 0 {
            flush_reply(s, sys);
            return 0;
        }
        if s.busy != 0 {
            pump(s, sys);
            return 0;
        }
        admit(s, sys);
        0
    }
}

unsafe fn log(sys: &SyscallTable, m: &[u8]) {
    dev_log(sys, 1, m.as_ptr(), m.len());
}

/// Check the params once.
unsafe fn configure(s: &mut State, sys: &SyscallTable) {
    if s.actor_len == 0 {
        s.actor[..DEFAULT_ACTOR.len()].copy_from_slice(DEFAULT_ACTOR);
        s.actor_len = DEFAULT_ACTOR.len() as u8;
    }
    if s.label_len == 0 {
        s.label[..DEFAULT_LABEL.len()].copy_from_slice(DEFAULT_LABEL);
        s.label_len = DEFAULT_LABEL.len() as u8;
    }
    if s.custodian_set == 0 {
        // Nothing pinned: the custodians in this graph name their keys.
        if s.custodian_req < 0 || s.custodian_in < 0 {
            log(sys, b"[kagi_storage_effect] FAULT: no custodian_N keys pinned and no custodians wired to ask");
            s.configured = 2;
            return;
        }
        if !learn_custodians(s, sys) {
            return;
        }
    } else if s.custodian_set != 0b111 {
        log(
            sys,
            b"[kagi_storage_effect] FAULT: pin all three custodian_N keys, or none",
        );
        s.configured = 2;
        return;
    }
    let actor = &s.actor[..s.actor_len as usize];
    let aead = u16::try_from(s.aead).unwrap_or(0);
    if !s.eng.configure(&s.custodians, aead, actor) {
        log(sys, b"[kagi_storage_effect] FAULT: three distinct P-256 custodian keys and an aead of 1 or 2 are required");
        s.configured = 2;
        return;
    }
    s.configured = 1;
}

/// The custodians' recovery public keys, from the custodians themselves:
/// `CUSTODIAN_KEY_REQ` to each index until it answers. True once all three
/// have.
unsafe fn learn_custodians(s: &mut State, sys: &SyscallTable) -> bool {
    const ASK_EVERY_MS: u64 = 1000;
    const CORR: u32 = 0x7FFF_FF00;
    while chan::can_read(sys, s.custodian_in) {
        let (t, n) = chan::channel_read_msg(sys, s.custodian_in, &mut s.in_buf);
        if t == 0 {
            break;
        }
        if t != svc::msg::CUSTODIAN_KEY {
            continue;
        }
        // [corr u32][custodian u8][status u8][public f8]
        let p = &s.in_buf[..n as usize];
        if p.len() != 7 + sk::PUBLIC_LEN
            || p[5] != svc::STATUS_OK
            || p[6] as usize != sk::PUBLIC_LEN
        {
            continue;
        }
        let corr = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
        let i = p[4] as usize;
        if i > 2 || corr != CORR + i as u32 || p[7] != 0x04 {
            continue;
        }
        let at = i * sk::PUBLIC_LEN;
        s.custodians[at..at + sk::PUBLIC_LEN].copy_from_slice(&p[7..]);
        s.learned |= 1 << i;
    }
    if s.learned == 0b111 {
        log(
            sys,
            b"[kagi_storage_effect] custodian recovery keys named by the custodians",
        );
        return true;
    }
    let now = dev_millis(sys);
    if s.asked_at == 0 || now.saturating_sub(s.asked_at) >= ASK_EVERY_MS {
        s.asked_at = now.max(1);
        for i in 0..3u8 {
            if s.learned & (1 << i) != 0 {
                continue;
            }
            let mut req = [0u8; 5];
            req[..4].copy_from_slice(&(CORR + u32::from(i)).to_le_bytes());
            req[4] = i;
            chan::channel_write_msg(sys, s.custodian_req, svc::msg::CUSTODIAN_KEY_REQ, &req);
        }
    }
    false
}

/// Name the storage-grant signing key to the service: a SIGN record whose
/// reference is the vault label. A label, never key material: the service
/// opens it (generated in its vault on first open) and announces the public
/// half to the custodians itself.
unsafe fn name_key(s: &mut State, sys: &SyscallTable) {
    use auth_wire::{key_state, key_use, suite, KeyRecord};
    if s.key_out < 0 || s.key_sent != 0 {
        return;
    }
    let rec = KeyRecord {
        issuer: ISSUER,
        profile_id: suite::profile::STORAGE_GRANT,
        kid: KID,
        suite: suite::ED25519,
        state: key_state::ACTIVE,
        key_use: key_use::SIGN,
        generation: 0,
        activate_after_unix: 0,
        remove_after_unix: 0,
        key_ref: &s.label[..s.label_len as usize],
    };
    let mut out = [0u8; 512];
    let Ok(n) = rec.encode_add(&mut out) else {
        return;
    };
    if (sys.channel_write)(s.key_out, out.as_ptr(), n) == n as i32 {
        s.key_sent = 1;
    }
}

/// Take one record and start the effect it names.
unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    let Some(len) = take_record(s, sys) else {
        return;
    };
    s.held_len = len as u16;
    let rec = core::slice::from_raw_parts(s.held.as_ptr(), len);
    s.op = frame_i64(rec, 2);
    if !kagi_effect(s.op) {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    let e = Effect {
        op: s.op,
        a: [
            frame_bytes(rec, 4),
            frame_bytes(rec, 5),
            frame_bytes(rec, 6),
            frame_bytes(rec, 7),
            frame_bytes(rec, 8),
        ],
        n1: frame_i64(rec, 9),
        n2: frame_i64(rec, 10),
    };
    s.busy = 1;
    s.started = dev_millis(sys);
    let mut vault = SysVault(sys);
    let next = s.eng.begin(&e, sha256_fn, &mut vault, &mut s.out);
    proceed(s, sys, next);
}

unsafe fn proceed(s: &mut State, sys: &SyscallTable, next: Next) {
    match next {
        Next::Send(n) => {
            let port = s.request_out;
            let env = core::slice::from_raw_parts(s.out.as_ptr(), n);
            if !s
                .call
                .begin(port, |id, out| typed_exchange::write_call(id, env, out))
            {
                answer(s, sys, Code::Failed);
                return;
            }
            send(s, sys);
        }
        Next::Done(code) => answer(s, sys, code),
    }
}

/// Place what the call owes the service: its request (first sent, or asked
/// again after a `LINK UP`), or the ABORT for a call given up on.
unsafe fn send(s: &mut State, sys: &SyscallTable) {
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

/// Send what is waiting, take this effect's answer, act on it.
unsafe fn pump(s: &mut State, sys: &SyscallTable) {
    send(s, sys);
    loop {
        let poll = (sys.channel_poll)(s.response_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            break;
        }
        let n = (sys.channel_read)(s.response_in, s.rec_buf.as_mut_ptr(), REC);
        if n <= 0 {
            break;
        }
        // Answers fan out to every party on the service's edge; the call
        // takes only its own exchange's.
        let rec = core::slice::from_raw_parts(s.rec_buf.as_ptr(), n as usize);
        match s.call.take(rec) {
            CallEvent::Answered => {
                answered(s, sys);
                return;
            }
            CallEvent::Failed(status) => {
                answer(s, sys, unanswered(status));
                return;
            }
            CallEvent::Nothing | CallEvent::LinkDown | CallEvent::LinkUp => {}
        }
    }
    if s.busy != 0 && dev_millis(sys).saturating_sub(s.started) > u64::from(s.timeout_ms) {
        s.call.cancel();
        send(s, sys);
        answer(s, sys, Code::Unavailable);
    }
}

/// An exchange that ended without the service's typed answer: worth asking
/// again when the service was busy or gone, a failure otherwise.
fn unanswered(status: u16) -> Code {
    match status {
        exch::status::BUSY | exch::status::BAD_GATEWAY | exch::status::TIMEOUT => Code::Unavailable,
        _ => Code::Failed,
    }
}

/// The call's whole answer: a 200 whose body is one storage-key REPLY
/// envelope is the service's decision; the engine acts on it.
unsafe fn answered(s: &mut State, sys: &SyscallTable) {
    let (status, body) = s.call.answer();
    // Read through a raw place: the answer stays in the call while the
    // engine reads it and lays the next request in `out`.
    let body = core::slice::from_raw_parts(body.as_ptr(), body.len());
    if status != exch::status::OK {
        answer(s, sys, unanswered(status));
        return;
    }
    let reply = match typed_exchange::message(body) {
        Some((svc::msg::REPLY, payload)) => svc::read_reply(payload),
        _ => None,
    };
    let Some(r) = reply else {
        answer(s, sys, Code::Failed);
        return;
    };
    let mut vault = SysVault(sys);
    let next = s.eng.on_reply(&r, &mut vault, &mut s.out);
    proceed(s, sys, next);
}

fn code_name(c: Code) -> &'static [u8] {
    match c {
        Code::Ok => b"ok",
        Code::Refused => b"refused",
        Code::Failed => b"failed",
        Code::Unavailable => b"unavailable",
    }
}

/// Build the outcome record for the held request and end the effect.
unsafe fn answer(s: &mut State, sys: &SyscallTable, code: Code) {
    s.busy = 0;
    if code != Code::Ok && s.eng.refusal != 0 {
        // Kagi's reason is in its audit trail; the operator's log names it too.
        let mut m = *b"[kagi_storage_effect] op 00 refused: Kagi refusal 000";
        let at = m.len() - 3;
        m[25] = b'0' + (s.op / 10 % 10) as u8;
        m[26] = b'0' + (s.op % 10) as u8;
        let r = s.eng.refusal;
        m[at] = b'0' + r / 100;
        m[at + 1] = b'0' + r / 10 % 10;
        m[at + 2] = b'0' + r % 10;
        log(sys, &m);
    }
    let held = core::slice::from_raw_parts(s.held.as_ptr(), s.held_len as usize);
    let ok = code == Code::Ok;
    let field = if ok { s.eng.result_field } else { 0 };
    let value = s.eng.result();
    let pick = |n: u8| -> &[u8] {
        if field == n {
            value
        } else {
            &[]
        }
    };
    let mut out = [0u8; REC];
    let mut p = 1usize;
    let mut n = 0u8;
    let mut f = |out: &mut [u8], num: u8, ty: u8, v: &[u8]| -> bool {
        match put(out, p, num, ty, v) {
            Some(q) => {
                p = q;
                n += 1;
                true
            }
            None => false,
        }
    };
    let zero = 0i64.to_le_bytes();
    let fits = f(&mut out, 1, TY_I64, &frame_i64(held, 1).to_le_bytes())
        && f(&mut out, 2, TY_I64, &s.op.to_le_bytes())
        && f(&mut out, 3, TY_BYTES, frame_bytes(held, 3))
        && f(&mut out, 4, TY_BYTES, code_name(code))
        && f(&mut out, 5, TY_I64, &zero)
        && f(&mut out, 6, TY_I64, &zero)
        && f(&mut out, 7, TY_I64, &zero)
        && f(&mut out, 8, TY_BYTES, &[])
        && f(&mut out, 9, TY_BYTES, &[])
        && f(&mut out, 10, TY_BYTES, &[])
        && f(&mut out, 11, TY_BYTES, pick(11))
        && f(&mut out, 12, TY_BYTES, pick(12))
        && f(&mut out, 13, TY_I64, &zero)
        && f(&mut out, 14, TY_BYTES, pick(14))
        && f(&mut out, 15, TY_BYTES, &[])
        && f(&mut out, 16, TY_BYTES, &[])
        && match frame_field(held, CARRY_FIELD) {
            Some((ty, v)) => f(&mut out, CARRY_FIELD, ty, v),
            None => true,
        };
    if !fits {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    out[0] = n;
    s.reply[..p].copy_from_slice(&out[..p]);
    s.reply_len = p as u16;
    flush_reply(s, sys);
}

/// Put the finished outcome on `response_out`, or keep it for the next step.
unsafe fn flush_reply(s: &mut State, sys: &SyscallTable) {
    let n = s.reply_len as usize;
    if (sys.channel_write)(s.response_out, s.reply.as_ptr(), n) > 0 {
        s.reply_len = 0;
        s.answered = s.answered.wrapping_add(1);
    }
}

/// Take one whole record off `request_in` into `held`: a mailbox cannot be
/// peeked (EINVAL) and is read whole; a byte stream is self-framed.
unsafe fn take_record(s: &mut State, sys: &SyscallTable) -> Option<usize> {
    let poll = (sys.channel_poll)(s.request_in, POLL_IN);
    if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
        return None;
    }
    let pk = (sys.channel_peek)(s.request_in, s.held.as_mut_ptr(), REC);
    let n = if pk == CHAN_EINVAL {
        (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REC)
    } else if pk <= 0 {
        return None;
    } else {
        match frame_len(&s.held[..pk as usize]) {
            Some(fl) => (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), fl),
            None if (pk as usize) < REC => return None,
            None => {
                (sys.channel_read)(s.request_in, s.held.as_mut_ptr(), REC);
                s.dropped = s.dropped.wrapping_add(1);
                return None;
            }
        }
    };
    (n > 0).then_some(n as usize)
}
