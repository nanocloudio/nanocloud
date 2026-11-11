//! Chronicle connector for kagi's `token_verify`.
//!
//! See `manifest.toml` for the wire layouts and the rationale. The short
//! version: this is `store_effect`'s role for a different outside — a record
//! comes in, a credential goes out to kagi as one exchange, a typed identity
//! comes back, and a record carrying it goes on down the chain. It decides
//! nothing. Whether a verified subject may perform the request is params.

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

// kagi owns the identity protocols, so it owns the wire and the way a typed
// operation is called. Compiled from its published tree rather than restated
// here, so there is one definition of what a verify request is.
#[path = "../../../target/fluxor/kagi-common/auth_wire.rs"]
mod auth_wire;
#[path = "../../../target/fluxor/kagi-common/typed_exchange.rs"]
mod typed_exchange;
use auth_wire::{VerifiedIdentity, VerifyRequest};

/// Chronicle record-frame value types (`pipeline_core.rs`).
const TY_BYTES: u8 = 0;
const TY_I64: u8 = 1;

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

const BUF: usize = 8192;
const MAX_AUD: usize = 128;
const MAX_ISS: usize = 128;
const MAX_KEY_PATH: usize = 128;
/// How many key sets one verifier trusts (`key_paths`), and how many issuers
/// (`issuer`): a cell's own ServiceAccount issuer and the key it mints with,
/// and room for a provisioned one or two beside them.
const MAX_KEY_PATHS: usize = 4;
const MAX_ISSUERS: usize = 4;
/// `key_paths`: up to four paths, `,`-separated.
const KEY_PATHS_BUF: usize = MAX_KEY_PATHS * (MAX_KEY_PATH + 1);
/// `issuer`: up to four issuers, `,`-separated.
const ISSUERS_BUF: usize = MAX_ISSUERS * (MAX_ISS + 1);
/// The keyset as ONE `MSG_KEYSET_SNAPSHOT`: envelope, count, and every
/// stored `MSG_KEY_ADD` payload. Four ES256 records are ~500 bytes; a set
/// past this (post-quantum keys at every path) is not published, rather than
/// published short.
const SNAP_BUF: usize = 8192;
const DEFAULT_KEY_PATH: &[u8] = b"/authn-keys/sa";
/// Steps between key-fetch attempts while the key is absent.
const KEY_RETRY_STEPS: u32 = 64;
/// Steps between re-reads of a keyset already forwarded. A key changes when
/// its mint's does — every boot of a node whose vault cannot keep keys, or
/// once when a mint key replaces a seeded one — and a key set lands after
/// the first read when its mint starts after this module: a verifier that
/// read once would reject every token the new key signs. About a second: a
/// few GETs, and a token minted at boot is honoured that soon after.
const KEY_REFRESH_STEPS: u32 = 1024;

/// The status a reply carries when the verifier gave no typed answer: its
/// exchange ended with a refusal status, an ABORT, or an answer that was not
/// one `MSG_VERIFY_RESP`. Outside kagi's `verify_err` range, and not OK, so
/// a chain that refuses every non-OK status refuses this too.
const NO_ANSWER: i64 = 255;

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes, storage.namespace (0x13) prefix LIST.
// Named here because `_shared/store.rs` is an `include!` fragment and takes
// its opcodes from whoever mounts it.
const OBJ_PUT: u32 = abi::registry::storage_object::PUT;
const OBJ_GET: u32 = abi::registry::storage_object::GET;
const OBJ_RANGE_GET: u32 = abi::registry::storage_object::RANGE_GET;
const OBJ_DELETE: u32 = abi::registry::storage_object::DELETE;
const OBJ_CLOSE: u32 = abi::registry::storage_object::CLOSE;
const NS_LIST: u32 = abi::registry::storage_namespace::LIST;
const MAX_KEY: usize = 160;

/// Carry-through, echoed with its type. Ten fields, matching `store_effect`:
/// a chain that changes connectors must not change how much it can remember.
const CARRY_LO: u8 = 30;
const CARRY_HI: u8 = 39;
/// The record's carried context (chronicle `CARRY_FIELD`): one nested
/// message, echoed unchanged beside the legacy 30..=39 range.
const CARRY_FIELD: u8 = 254;

/// `TIMER::TRUSTED_UNIX` record offsets (`kernel_abi::trusted_time`).
const TT_UNIX_SECONDS: usize = 0;
const TT_SOURCE_CLASS: usize = 32;
const TT_FLAGS: usize = 33;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    response_in: i32,
    request_out: i32,
    key_out: i32,
    response_out: i32,
    /// Ports past the primary pair are resolved on the first step, not in
    /// `module_new` — the same cold-start the rest of this repo uses.
    resolved: u32,

    credential_field: u32,
    min_assurance: u32,
    /// The store keys of the key sets this verifier trusts, `,`-separated
    /// (empty: [`DEFAULT_KEY_PATH`] alone).
    key_paths: [u8; KEY_PATHS_BUF],
    key_paths_len: u16,
    /// Whether the verification key set has been forwarded. The store may not hold
    /// it yet when this module starts — the mint announces it on ITS start —
    /// so this is retried every step until it lands rather than resolved once.
    key_published: u32,
    /// Steps until the next key-fetch attempt. The key may legitimately be
    /// absent for a while (the mint announces on ITS start), and retrying a
    /// store read every tick turns a normal startup race into a log flood.
    key_backoff: u32,
    /// FNV-1a of the keyset last forwarded, so an unchanged re-read sends
    /// nothing.
    key_hash: u32,
    /// Key frames skipped because they were not one whole `MSG_KEY_ADD`.
    key_bad: u32,
    audience: [u8; MAX_AUD],
    audience_len: u16,
    /// The issuers a credential may name, `,`-separated (empty: any the
    /// keyset verifies).
    issuer: [u8; ISSUERS_BUF],
    issuer_len: u16,

    /// The verify exchange in flight. One at a time: see the manifest on why
    /// there is no table.
    call: Call,
    /// The request frame, held while the call is out, so the carry fields can
    /// be re-read at reply time rather than copied into a second layout.
    held: [u8; BUF],
    held_len: u16,
    /// A built reply record the chain's ring has not taken yet
    /// (`out_buf[..reply_len]`), placed before anything else is read.
    reply_len: u16,

    sent: u32,
    ok: u32,
    refused: u32,
    dropped: u32,

    in_buf: [u8; BUF],
    out_buf: [u8; BUF],
    /// The keyset snapshot being assembled (and the frames read into it).
    snap: [u8; SNAP_BUF],
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

mod params_def {
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::ISSUERS_BUF;
    use super::KEY_PATHS_BUF;
    use super::MAX_AUD;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, credential_field, u32, 8
            => |s, d, len| { s.credential_field = p_u32(d, len, 0, 8); };

        2, audience, str, 0
            => |s, d, len| {
                let n = if len > MAX_AUD { MAX_AUD } else { len };
                s.audience_len = n as u16;
                if n > 0 { ptr_copy(s.audience.as_mut_ptr(), d, n); }
            };

        3, issuer, str, 0
            => |s, d, len| {
                let n = if len > ISSUERS_BUF { ISSUERS_BUF } else { len };
                s.issuer_len = n as u16;
                if n > 0 { ptr_copy(s.issuer.as_mut_ptr(), d, n); }
            };

        4, min_assurance, u32, 0
            => |s, d, len| { s.min_assurance = p_u32(d, len, 0, 0); };

        5, key_paths, str, 0
            => |s, d, len| {
                let n = if len > KEY_PATHS_BUF { KEY_PATHS_BUF } else { len };
                s.key_paths_len = n as u16;
                if n > 0 { ptr_copy(s.key_paths.as_mut_ptr(), d, n); }
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

fn put_i64(out: &mut [u8], at: usize, number: u8, v: i64) -> Option<usize> {
    put_field(out, at, number, TY_I64, &v.to_le_bytes())
}

/// The request record's own correlation (field 1), echoed on its reply.
fn held_cid(held: &[u8]) -> i64 {
    match frame_field(held, 1) {
        Some((TY_I64, b)) if b.len() == 8 => i64::from_le_bytes(b.try_into().unwrap()),
        _ => 0,
    }
}

/// Build the reply record: the typed identity, then the request's carry echoed
/// with its own numbers and types. `id` is `None` when the verifier gave no
/// typed answer: every field is then empty or zero and the status is
/// [`NO_ANSWER`].
///
/// Every field is emitted even when empty, so a decision downstream reads a
/// stable shape. A refusal carries an empty subject and scope — kagi's own
/// decoder REFUSES a refusal that carried either, so this cannot report a
/// subject it did not establish.
fn build_reply(out: &mut [u8], id: Option<&VerifiedIdentity>, held: &[u8]) -> usize {
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
    let (status, subject, issuer, audience, scope, expires_at, suite) = match id {
        Some(i) => (
            i.status as i64,
            i.subject,
            i.issuer,
            i.audience,
            i.scope,
            i.expires_at as i64,
            i.suite as i64,
        ),
        None => (NO_ANSWER, &b""[..], &b""[..], &b""[..], &b""[..], 0, 0),
    };
    field!(put_i64(out, p, 1, held_cid(held)));
    field!(put_i64(out, p, 2, status));
    field!(put_field(out, p, 3, TY_BYTES, subject));
    field!(put_field(out, p, 4, TY_BYTES, issuer));
    field!(put_field(out, p, 5, TY_BYTES, audience));
    field!(put_field(out, p, 6, TY_BYTES, scope));
    field!(put_i64(out, p, 7, expires_at));
    field!(put_i64(out, p, 8, suite));

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
        // `module_new` hands over the FIRST input and the FIRST output, which
        // in manifest order are `request_in` and `request_out`. The rest are
        // resolved by direction-index on the first step.
        s.request_in = in_chan;
        s.request_out = out_chan;
        s.response_in = -1;
        s.key_out = -1;
        s.response_out = -1;
        s.resolved = 0;
        s.key_published = 0;
        s.key_backoff = 0;
        s.key_hash = 0;
        s.key_bad = 0;
        s.key_paths_len = 0;
        s.call.reset();
        s.held_len = 0;
        s.reply_len = 0;
        s.sent = 0;
        s.ok = 0;
        s.refused = 0;
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
            s.key_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.response_out = dev_channel_port(sys, PORT_OUTPUT, 2);
            s.resolved = 1;
        }
        // Until a key is out, nothing this module sends can be answered with
        // anything but ST_NO_KEY. Retried rather than resolved once: the issuer
        // publishes on its own start, which may be after ours.
        // Then re-read every KEY_REFRESH_STEPS, forwarding a changed keyset:
        // a snapshot replaces kagi's whole set.
        if s.key_backoff == 0 {
            publish_keys(s, sys);
            s.key_backoff = if s.key_published == 0 {
                KEY_RETRY_STEPS
            } else {
                KEY_REFRESH_STEPS
            };
        } else {
            s.key_backoff -= 1;
        }
        if s.request_in < 0 || s.response_out < 0 || s.request_out < 0 || s.response_in < 0 {
            return 0;
        }

        // A reply the chain has not taken goes first: nothing else is read
        // until it is away.
        if s.reply_len != 0 && !flush_reply(s, sys) {
            return 0;
        }
        flush_call(s, sys);
        drain_answers(s, sys);
        if s.reply_len == 0 && s.call.idle() {
            admit(s, sys);
        }
        // Return 0, ALWAYS. A non-zero `module_step` return means DONE: the
        // scheduler stops stepping the module.
        0
    }
}

/// The `,`-separated entries of a list param, trimmed of spaces, empty
/// entries skipped.
fn list_entries(list: &[u8]) -> impl Iterator<Item = &[u8]> {
    list.split(|&c| c == b',')
        .map(trim)
        .filter(|e| !e.is_empty())
}

fn trim(mut v: &[u8]) -> &[u8] {
    while let [b' ', rest @ ..] = v {
        v = rest;
    }
    while let [rest @ .., b' '] = v {
        v = rest;
    }
    v
}

/// Forward the trusted key sets to kagi as ONE keyset, every key verbatim.
///
/// The frame at each of `key_paths` is a kagi `MSG_KEY_ADD` that a mint
/// stored (or an operator provisioned). Each is taken WITHOUT being parsed —
/// it is kagi's wire, and a second reading of it here is a second answer to
/// "which key is this" — only its envelope checked, and its payload, a
/// `KeyRecord`, placed in a `MSG_KEYSET_SNAPSHOT`: the shape kagi takes a
/// whole keyset in, which REPLACES the verifier's set. So a key that left
/// its path (replaced, or removed) stops verifying at the next refresh,
/// rather than lingering beside its successor until a restart.
///
/// A read the store has not decided (not yet, or refused) publishes
/// nothing this pass: a set built without one of its keys would revoke
/// that key's every credential until the next pass. An absent path is
/// simply not in the set. Nothing is published until some path holds a
/// key; once one has been, an empty set is published when none does.
unsafe fn publish_keys(s: &mut State, sys: &SyscallTable) {
    unsafe {
        if s.key_out < 0 {
            return;
        }
        let paths_buf = s.key_paths;
        let paths: &[u8] = if s.key_paths_len > 0 {
            &paths_buf[..s.key_paths_len as usize]
        } else {
            DEFAULT_KEY_PATH
        };
        // [MSG_KEYSET_SNAPSHOT][len u16][count u16], then the records.
        let mut at = 5usize;
        let mut count = 0u16;
        for path in list_entries(paths).take(MAX_KEY_PATHS) {
            let room = &mut s.snap[at..];
            // `get_value_live` REFUSES an oversized value rather than
            // truncating it — a half key would verify nothing.
            match get_value_live(sys, &s.nag, path, room) {
                Got::Found(n) => {
                    let frame = &s.snap[at..at + n];
                    let whole = n > 3
                        && frame[0] == auth_wire::MSG_KEY_ADD
                        && usize::from(u16::from_le_bytes([frame[1], frame[2]])) == n - 3;
                    if !whole {
                        // Not one whole MSG_KEY_ADD: kagi would refuse the
                        // whole snapshot for it, so it is left out.
                        s.key_bad = s.key_bad.wrapping_add(1);
                        continue;
                    }
                    s.snap.copy_within(at + 3..at + n, at);
                    at += n - 3;
                    count += 1;
                }
                Got::Absent => {}
                Got::Pending | Got::Refused(_) => return,
            }
        }
        if count == 0 && s.key_published == 0 {
            return; // nothing published yet; retried
        }
        let payload = at - 3;
        s.snap[0] = auth_wire::MSG_KEYSET_SNAPSHOT;
        s.snap[1..3].copy_from_slice(&(payload as u16).to_le_bytes());
        s.snap[3..5].copy_from_slice(&count.to_le_bytes());
        let mut h: u32 = 0x811C_9DC5;
        for &b in &s.snap[..at] {
            h = (h ^ u32::from(b)).wrapping_mul(0x0100_0193);
        }
        if s.key_published != 0 && h == s.key_hash {
            return;
        }
        if (sys.channel_write)(s.key_out, s.snap.as_ptr(), at) > 0 {
            s.key_published = 1;
            s.key_hash = h;
            // Said once per change: which set is trusted is what an operator
            // asks first when a token is refused.
            let mut m = *b"[kagi_verify] keyset published: 0 key(s), 0 skipped";
            m[32] = b'0' + (count as u8 % 10);
            m[42] = b'0' + (s.key_bad % 10) as u8;
            dev_log(sys, 2, m.as_ptr(), m.len());
        }
    }
}

/// Place what the call owes the verifier: its request (first sent, or asked
/// again after a `LINK UP`), or the ABORT for an answer it refused.
unsafe fn flush_call(s: &mut State, sys: &SyscallTable) {
    let mut abort = [0u8; 32];
    if let Some(n) = s.call.abort_record(&mut abort) {
        // Best effort: the call is already over on this side.
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

/// Read the verifier's records; a call that ends becomes one reply record.
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
            CallEvent::Failed(_) => reply(s, sys, None),
            CallEvent::Nothing | CallEvent::LinkDown | CallEvent::LinkUp => {}
        }
    }
}

/// The call's whole answer, as the reply record.
unsafe fn answered(s: &mut State, sys: &SyscallTable) {
    let (status, body) = s.call.answer();
    // Read through a raw place: the answer stays in the call, which nothing
    // below touches, while the reply is built from it.
    let body = core::slice::from_raw_parts(body.as_ptr(), body.len());
    // A 200 whose body is one MSG_VERIFY_RESP is kagi's typed answer, a
    // refusal verdict included. Anything else is no answer at all.
    let id = if status == exch::status::OK {
        match typed_exchange::message(body) {
            Some((auth_wire::MSG_VERIFY_RESP, payload)) => VerifiedIdentity::decode(payload).ok(),
            _ => None,
        }
    } else {
        None
    };
    // A credential kagi verified under a key this node trusts, naming an
    // issuer it does not: refused as kagi refuses one, WRONG_ISSUER, with
    // nothing it established. (With one issuer configured kagi held it to
    // that one already.)
    if let Some(i) = id.as_ref() {
        let issuers = &s.issuer[..s.issuer_len as usize];
        if i.status == auth_wire::verify_err::OK
            && !issuers.is_empty()
            && !list_entries(issuers).any(|n| n == i.issuer)
        {
            let refusal = VerifiedIdentity::refused(auth_wire::verify_err::WRONG_ISSUER);
            reply(s, sys, Some(&refusal));
            return;
        }
    }
    reply(s, sys, id.as_ref());
}

/// Emit the reply for the held request and free the call.
unsafe fn reply(s: &mut State, sys: &SyscallTable, id: Option<&VerifiedIdentity>) {
    let mut out = [0u8; BUF];
    let held_len = s.held_len as usize;
    let flen = {
        let held = core::slice::from_raw_parts(s.held.as_ptr(), held_len);
        build_reply(&mut out, id, held)
    };
    s.held_len = 0;
    if flen == 0 {
        s.dropped = s.dropped.wrapping_add(1);
        return;
    }
    match id {
        Some(i) if i.status == auth_wire::verify_err::OK => s.ok = s.ok.wrapping_add(1),
        _ => s.refused = s.refused.wrapping_add(1),
    }
    s.out_buf[..flen].copy_from_slice(&out[..flen]);
    s.reply_len = flen as u16;
    flush_reply(s, sys);
}

/// Take one record and open one verify call for it.
unsafe fn admit(s: &mut State, sys: &SyscallTable) {
    unsafe {
        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in, s.in_buf.as_mut_ptr(), BUF);
        if n <= 0 {
            return;
        }
        let len = n as usize;
        s.held[..len].copy_from_slice(&s.in_buf[..len]);
        s.held_len = len as u16;

        let credential = {
            let frame = core::slice::from_raw_parts(s.held.as_ptr(), len);
            match frame_field(frame, s.credential_field as u8) {
                Some((TY_BYTES, v)) => v,
                _ => b"",
            }
        };

        // A security-grade observation, not `dev_unix_millis`: kagi's request
        // carries `time_source_class`/`time_flags` precisely so the verifier
        // can refuse to rule on validity rather than rule against a clock that
        // says 1970. Passing them through is the whole reason to read this
        // record instead of the bare u64.
        let tt = dev_trusted_unix(sys);
        let now_secs =
            u64::from_le_bytes(tt[TT_UNIX_SECONDS..TT_UNIX_SECONDS + 8].try_into().unwrap());

        // One configured issuer is kagi's to hold the credential to; with
        // several, kagi is asked for none and the issuer it reports is held
        // to the list when it answers (`answered`).
        let issuers = s.issuer;
        let mut named = list_entries(&issuers[..s.issuer_len as usize]);
        let expected_issuer = match (named.next(), named.next()) {
            (Some(one), None) => one,
            _ => &b""[..],
        };
        let req = VerifyRequest {
            credential,
            expected_profile: 0,
            expected_issuer,
            expected_audience: &s.audience[..s.audience_len as usize],
            // Empty: no proof-of-possession binding. See `manifest.toml`.
            method: b"",
            uri: b"",
            min_assurance: s.min_assurance as u8,
            now_unix_secs: now_secs,
            time_source_class: tt[TT_SOURCE_CLASS],
            time_flags: tt[TT_FLAGS],
        };
        let mut env = [0u8; BUF];
        let Ok(elen) = req.encode(&mut env) else {
            // A credential too long for one request: refused here, as the
            // verifier would, and answered rather than dropped.
            reply(s, sys, None);
            return;
        };
        let port = s.request_out;
        let env = &env[..elen];
        if !s
            .call
            .begin(port, |id, out| typed_exchange::write_call(id, env, out))
        {
            reply(s, sys, None);
            return;
        }
        s.sent = s.sent.wrapping_add(1);
        flush_call(s, sys);
    }
}
