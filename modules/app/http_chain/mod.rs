//! An HTTP application route as a Chronicle chain: the provider behind Wave's
//! `http` server. Each exchange becomes one request envelope for the chain
//! (`record_out`); the chain's reply envelopes (`reply_in`) become the
//! exchange's response. The desk — slots, credit, the response held past it,
//! the envelopes — is `_shared/http_chain.rs`; this module moves its records,
//! keeps its heap and stages a large body in the store. See `manifest.toml`.

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
include!("../_shared/store.rs");
include!("../_shared/k8s_proto_schema.rs");
include!("../_shared/k8s_proto.rs");
include!("../_shared/http_chain.rs");

const PORT_INPUT: u8 = 0;
const PORT_OUTPUT: u8 = 1;

/// Exchanges in hand at once. A watch holds its exchange for as long as it
/// runs, so this is the API plane's concurrent watches plus requests.
const SLOTS: usize = 64;
/// Response bytes held for one exchange the server has not yet taken: a
/// whole large object, so a client reading at network speed is never cut
/// off by a chain that writes at memory speed.
const STASH_MAX: u32 = 4 << 20;
/// The heap every request body and held response comes from: a few largest
/// bodies and held responses at once. A request the arena cannot hold is
/// refused (503), never cut; a response it cannot hold is aborted as too slow.
const ARENA: u32 = 16 << 20;
/// Records moved each way per step: enough that one busy exchange does not
/// starve the rest, few enough that a step stays bounded.
const PER_STEP: usize = 32;
/// The longest staging prefix.
const MAX_PREFIX: usize = 48;
/// How long a staged body stays before it is collected (its expiry key).
const STAGED_TTL_MS: u64 = 300_000;
/// A staging slower than this is said, with its asks. Each ask of a pending
/// write costs the store a digest of the WHOLE value (it knows a re-asked
/// write by its bytes) and moves its chunk upload on as far as the store
/// has room; a loaded host's store takes a 1 MiB body in 1-11 s, hundreds
/// of asks. Asked less often, it is taken later (measured), so it is asked
/// every step.
const SLOW_STAGE_MS: u64 = 2_000;

// storage.object, for staging (`_shared/store.rs` takes its opcodes from
// whoever mounts it).
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 160;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    request_in: i32,
    reply_in: i32,
    response_out: i32,
    record_out: i32,
    resolved: u8,
    /// A param past its bound: the module refuses to run rather than run on
    /// a value it did not take.
    bad_param: u8,

    max_body_kib: u32,
    inline_max: u32,
    prefix: [u8; MAX_PREFIX],
    prefix_len: u8,

    /// The body being staged: its slot, the step (0 the body, 1 its
    /// expiry) and its digest.
    stage_at: i32,
    stage_cid: u32,
    stage_step: u8,
    stage_hex: [u8; 64],
    /// The staged body's expiry, fixed when its staging starts.
    stage_deadline: u64,
    /// When the staging began, and the writes it has asked: a staging past
    /// [`SLOW_STAGE_MS`] is said.
    stage_began_ms: u64,
    stage_asks: u32,
    staged: u32,
    /// Exchanges answered 503 because the chain had not answered in time
    /// ([`HC_ANSWER_MS`]).
    late: u32,

    desk: HcDesk<SLOTS>,
    buf: [u8; HC_ENV_MAX],
    out: [u8; HC_ENV_MAX],
    /// This module's refused store reads, said rate-limited (`ReadNag`).
    nag: ReadNag,
}

mod params_def {
    use super::p_u32;
    use super::State;
    use super::MAX_PREFIX;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        // The largest request body, KiB (413 past it).
        1, max_body_kib, u32, 64
            => |s, d, len| { s.max_body_kib = p_u32(d, len, 0, 64); };

        // A body past this many bytes is staged at
        // `<body_ref_prefix>sha256/<hex>` and handed on as `sha256:<hex>`;
        // 0 stages nothing.
        2, body_inline_max, u32, 0
            => |s, d, len| { s.inline_max = p_u32(d, len, 0, 0); };

        3, body_ref_prefix, str, 0
            => |s, d, len| {
                if len > MAX_PREFIX {
                    s.bad_param = 1;
                } else {
                    s.prefix_len = len as u8;
                    if len > 0 { core::ptr::copy_nonoverlapping(d, s.prefix.as_mut_ptr(), len); }
                }
            };
    }
}

/// The module's heap arena.
struct Arena<'a>(&'a SyscallTable);

impl HcHeap for Arena<'_> {
    unsafe fn grow(&mut self, p: *mut u8, cap: u32) -> *mut u8 {
        heap_realloc(self.0, p, cap)
    }
    unsafe fn free(&mut self, p: *mut u8) {
        heap_free(self.0, p);
    }
}

mod arena {
    #[no_mangle]
    #[link_section = ".text.module_arena_size"]
    pub extern "C" fn module_arena_size() -> u32 {
        super::ARENA
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
    let s = &mut *(state as *mut State);
    s.syscalls = syscalls as *const SyscallTable;
    // The first input and output in manifest order; the rest on the first
    // step.
    s.request_in = in_chan;
    s.response_out = out_chan;
    s.reply_in = -1;
    s.record_out = -1;
    s.resolved = 0;
    s.bad_param = 0;
    s.prefix_len = 0;
    s.stage_at = -1;
    s.stage_step = 0;
    s.stage_deadline = 0;
    s.stage_began_ms = 0;
    s.stage_asks = 0;
    s.staged = 0;
    s.late = 0;
    params_def::set_defaults(s);
    params_def::parse_tlv(s, params, params_len);
    let sys = &*s.syscalls;
    let max_body = u64::from(s.max_body_kib) * 1024;
    if s.bad_param != 0 || max_body == 0 || max_body > u64::from(ARENA) {
        let m = b"[http_chain] FAULT: body_ref_prefix past 48 bytes, or max_body_kib 0 or past the arena";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return -3;
    }
    if s.inline_max != 0 && s.prefix_len == 0 {
        let m = b"[http_chain] FAULT: body_inline_max stages bodies, and needs body_ref_prefix";
        dev_log(sys, 1, m.as_ptr(), m.len());
        return -3;
    }
    s.desk.reset(HcConfig {
        max_body: max_body as u32,
        inline_max: s.inline_max,
        stash_max: STASH_MAX,
    });
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
            s.reply_in = dev_channel_port(sys, PORT_INPUT, 1);
            s.record_out = dev_channel_port(sys, PORT_OUTPUT, 1);
            s.resolved = 1;
        }
        if s.request_in < 0 || s.response_out < 0 || s.record_out < 0 {
            return 0;
        }
        let late = s.desk.tick(&mut Arena(sys), dev_millis(sys));
        if late > 0 {
            s.late = s.late.wrapping_add(late);
            let m = b"[http_chain] the chain did not answer in time - 503 ServiceUnavailable, Retry-After";
            dev_log(sys, 2, m.as_ptr(), m.len());
        }
        to_server(s, sys);
        from_server(s, sys);
        stage(s, sys);
        to_chain(s, sys);
        from_chain(s, sys);
        to_server(s, sys);
        0
    }
}

/// Place what the desk owes the server, in turn, until the channel is full.
unsafe fn to_server(s: &mut State, sys: &SyscallTable) {
    for _ in 0..PER_STEP {
        let Some((what, n)) = s.desk.next_out(&mut s.out) else {
            return;
        };
        if (sys.channel_write)(s.response_out, s.out.as_ptr(), n) <= 0 {
            return;
        }
        s.desk.out_sent(&mut Arena(sys), what);
    }
}

/// Take the server's request records: requests, their bodies, credit and
/// aborts.
unsafe fn from_server(s: &mut State, sys: &SyscallTable) {
    for _ in 0..PER_STEP {
        if !s.desk.can_take() {
            return;
        }
        let poll = (sys.channel_poll)(s.request_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.request_in, s.buf.as_mut_ptr(), HC_ENV_MAX);
        if n <= 0 {
            return;
        }
        let rec = core::slice::from_raw_parts(s.buf.as_ptr(), n as usize);
        s.desk.take_request(&mut Arena(sys), rec);
    }
}

/// Stage the next body past the inline bound: the body at
/// `<prefix>sha256/<hex>` (content-addressed, so the same body is the same
/// key), then its expiry at `<prefix>at/<hex>`, which the store's `ttl` chain
/// collects whether or not a request ever consumed it. A write the store has
/// not decided is asked again on the next step.
unsafe fn stage(s: &mut State, sys: &SyscallTable) {
    if s.stage_at < 0 {
        let Some((at, cid)) = s.desk.next_stage() else {
            return;
        };
        let Some(body) = s.desk.stage_body(at, cid) else {
            return;
        };
        s.stage_hex = hc_hex(&sha256(body));
        s.stage_at = at as i32;
        s.stage_cid = cid;
        s.stage_step = 0;
        // Fixed once per staging: the store identifies a write by its
        // value, so a deadline recomputed on every retry of a pending write
        // is a NEW write each step — it took a fresh slot until the
        // replica's table (32) was full, and every writer on the node then
        // waited 60 s for idle slots to be reclaimed (2026-10-10: every
        // staged POST hung, and with them the RBAC index).
        s.stage_deadline = dev_millis(sys).saturating_add(STAGED_TTL_MS);
        s.stage_began_ms = dev_millis(sys);
        s.stage_asks = 0;
    }
    let now = dev_millis(sys);
    let (at, cid) = (s.stage_at as usize, s.stage_cid);
    let mut key = [0u8; MAX_PREFIX + 7 + 64];
    let prefix = &s.prefix[..s.prefix_len as usize];
    let mut kl = 0usize;
    let sub: &[u8] = if s.stage_step == 0 {
        b"sha256/"
    } else {
        b"at/"
    };
    for part in [prefix, sub, &s.stage_hex[..]] {
        key[kl..kl + part.len()].copy_from_slice(part);
        kl += part.len();
    }
    let store = Store::plain(sys, &s.nag);
    let answer = if s.stage_step == 0 {
        let Some(body) = s.desk.stage_body(at, cid) else {
            // Aborted (or answered late) while it waited for the store:
            // nothing is owed.
            s.stage_at = -1;
            return;
        };
        put_value(&store, &key[..kl], body)
    } else {
        let mut v = [0u8; 40];
        let mut vl = 0usize;
        let mut digits = [0u8; 20];
        let dl = dec_u64(s.stage_deadline, &mut digits);
        for part in [&b"{\"deadline\":"[..], &digits[..dl], &b"}"[..]] {
            v[vl..vl + part.len()].copy_from_slice(part);
            vl += part.len();
        }
        put_value(&store, &key[..kl], &v[..vl])
    };
    s.stage_asks = s.stage_asks.wrapping_add(1);
    match answer {
        WriteAnswer::Pending => {}
        WriteAnswer::Decided(0) if s.stage_step == 0 => s.stage_step = 1,
        WriteAnswer::Decided(0) => {
            let hex = s.stage_hex;
            s.desk.staged(&mut Arena(sys), at, cid, Some(&hex));
            s.staged = s.staged.wrapping_add(1);
            s.stage_at = -1;
            let took = now.saturating_sub(s.stage_began_ms);
            if took > SLOW_STAGE_MS {
                let mut m = [0u8; 96];
                let mut d = [0u8; 20];
                let mut l = 0usize;
                let mut put = |part: &[u8], l: &mut usize| {
                    for &c in part {
                        if let Some(slot) = m.get_mut(*l) {
                            *slot = c;
                            *l += 1;
                        }
                    }
                };
                put(b"[http_chain] slow staging ms=", &mut l);
                let n = dec_u64(took, &mut d);
                put(d.get(..n).unwrap_or(&[]), &mut l);
                put(b" asks=", &mut l);
                let n = dec_u64(u64::from(s.stage_asks), &mut d);
                put(d.get(..n).unwrap_or(&[]), &mut l);
                dev_log(sys, 2, m.as_ptr(), l);
            }
        }
        WriteAnswer::Decided(_) => {
            s.desk.staged(&mut Arena(sys), at, cid, None);
            s.stage_at = -1;
        }
    }
}

/// `v` in decimal into `out`; its length.
fn dec_u64(mut v: u64, out: &mut [u8; 20]) -> usize {
    let mut tmp = [0u8; 20];
    let mut n = 0usize;
    loop {
        tmp[n] = b'0' + (v % 10) as u8;
        n += 1;
        v /= 10;
        if v == 0 || n == 20 {
            break;
        }
    }
    for i in 0..n {
        out[i] = tmp[n - 1 - i];
    }
    n
}

/// Hand the chain each whole request, oldest first, until its edge is full.
unsafe fn to_chain(s: &mut State, sys: &SyscallTable) {
    for _ in 0..PER_STEP {
        let Some((at, n)) = s.desk.next_envelope(&mut s.out) else {
            return;
        };
        if (sys.channel_write)(s.record_out, s.out.as_ptr(), n) <= 0 {
            return;
        }
        s.desk.envelope_sent(&mut Arena(sys), at);
    }
}

/// Take the chain's reply envelopes.
unsafe fn from_chain(s: &mut State, sys: &SyscallTable) {
    if s.reply_in < 0 {
        return;
    }
    for _ in 0..PER_STEP {
        let poll = (sys.channel_poll)(s.reply_in, POLL_IN);
        if poll <= 0 || ((poll as u32) & POLL_IN) == 0 {
            return;
        }
        let n = (sys.channel_read)(s.reply_in, s.buf.as_mut_ptr(), HC_ENV_MAX);
        if n <= 0 {
            return;
        }
        let env = core::slice::from_raw_parts(s.buf.as_ptr(), n as usize);
        s.desk.take_reply(&mut Arena(sys), env);
    }
}
