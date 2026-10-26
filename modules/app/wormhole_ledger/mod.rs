//! Wormhole's region-controller ledger → `/wh-ledger/` rows.
//!
//! See `manifest.toml`. The controller's `ledger_out` stream is split into
//! its records (the §9.4 header names each one's length); every
//! `FailoverExplain` that Wormhole's own decoder accepts is written
//! verbatim at `/wh-ledger/<service hex>/<seq>`, never over a row that holds
//! other bytes. `_shared/whexplain.rs` is the reader's side.

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
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;

include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/store.rs");
include!("../_shared/wormhole_portfolio.rs");
include!("../_shared/whexplain.rs");

/// The largest record the stream carries (an `explain.v1` record is the
/// longest kind it holds; `region_ctl.v1` DrStatus is shorter).
const REC_MAX: usize = 1024;
const KEY_MAX: usize = 96;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    ledger_in: i32,
    service: [u8; 16],
    service_ok: u8,
    /// A record is held: `rec[..rec_len]`, its row `key[..key_len]`.
    held: u8,
    _pad: [u8; 2],
    rec_len: u32,
    key_len: u32,
    retry_ms: u32,
    next_ms: u64,
    nag: ReadNag,
    pub written: u32,
    pub unchanged: u32,
    pub forks: u32,
    pub skipped: u32,
    pub malformed: u32,
    pub refused: u32,
    rec: [u8; REC_MAX],
    key: [u8; KEY_MAX],
    cur: [u8; REC_MAX],
}

fn nib(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

define_params! {
    State;

    1, service, str, 0
        => |s, d, len| {
            if len == 32 {
                let b = core::slice::from_raw_parts(d, len);
                let mut ok = true;
                for i in 0..16 {
                    match (nib(b[2 * i]), nib(b[2 * i + 1])) {
                        (Some(h), Some(l)) => s.service[i] = (h << 4) | l,
                        _ => ok = false,
                    }
                }
                s.service_ok = u8::from(ok);
            }
        };
    2, retry_ms, u32, 1000
        => |s, d, len| { s.retry_ms = p_u32(d, len, 0, 1000); };
}

/// Take the next record off the stream into `rec`, or `false` when none is
/// whole yet. A record that is not one (a header no decoder reads, longer
/// than any) is skipped a byte at a time to the next magic.
unsafe fn take(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    for _ in 0..8 {
        let n = (sys.channel_peek)(s.ledger_in, s.rec.as_mut_ptr(), traffic::HEADER_LEN);
        if n < traffic::HEADER_LEN as i32 {
            return false;
        }
        let total = match traffic::table_record_total(&s.rec[..traffic::HEADER_LEN]) {
            Ok(t) if t <= REC_MAX => t,
            _ => {
                (sys.channel_discard)(s.ledger_in, 1);
                s.malformed = s.malformed.wrapping_add(1);
                continue;
            }
        };
        if (sys.channel_peek)(s.ledger_in, s.rec.as_mut_ptr(), total) < total as i32 {
            return false;
        }
        (sys.channel_discard)(s.ledger_in, total);
        let rec = &s.rec[..total];
        let Ok(env) = traffic::decode_explain_envelope(rec) else {
            // DrStatus, or a record of another kind: not the ledger's rows.
            s.skipped = s.skipped.wrapping_add(1);
            continue;
        };
        if env.kind != traffic::ExplainKind::Failover {
            s.skipped = s.skipped.wrapping_add(1);
            continue;
        }
        let Ok(e) = traffic::FailoverExplain::decode(env.payload) else {
            s.malformed = s.malformed.wrapping_add(1);
            continue;
        };
        s.key_len = ledger_key(&s.service, e.seq, &mut s.key) as u32;
        s.rec_len = total as u32;
        s.held = 1;
        return true;
    }
    false
}

/// Write the held record unless its row already stands; `true` when it is
/// settled (written, already there, or refused as a fork).
unsafe fn settle(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let key = &s.key[..s.key_len as usize];
    let rec = &s.rec[..s.rec_len as usize];
    match get_value(&store, key, &mut s.cur) {
        Some(n) if s.cur[..n] == *rec => {
            s.unchanged = s.unchanged.wrapping_add(1);
            return true;
        }
        Some(_) => {
            // History is never rewritten: the row stands, the fork is the
            // reader's to name.
            s.forks = s.forks.wrapping_add(1);
            let m = b"[wormhole_ledger] a ledger entry differs from the row its sequence holds - not overwritten";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return true;
        }
        None if store.stopped() => return false,
        None => {}
    }
    match put_value(&store, key, rec) {
        WriteAnswer::Decided(0) => {
            s.written = s.written.wrapping_add(1);
            true
        }
        WriteAnswer::Pending => false,
        WriteAnswer::Decided(_) => {
            s.refused = s.refused.wrapping_add(1);
            s.next_ms = dev_millis(sys) + u64::from(s.retry_ms);
            false
        }
    }
}

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
    _out_chan: i32,
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
        core::ptr::write_bytes(state, 0, core::mem::size_of::<State>());
        let s = &mut *(state as *mut State);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.ledger_in = in_chan;
        s.retry_ms = 1000;
        s.nag = ReadNag::new();
        parse_tlv(s, params, params_len);
        if s.ledger_in < 0 || s.service_ok == 0 {
            let m = b"[wormhole_ledger] ledger_in must be wired and service given (32 hex)";
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
        let now = dev_millis(&*s.syscalls);
        if now < s.next_ms {
            return 0;
        }
        for _ in 0..4 {
            if s.held == 0 && !take(s) {
                return 0;
            }
            if !settle(s) {
                return 0;
            }
            s.held = 0;
        }
        0
    }
}
