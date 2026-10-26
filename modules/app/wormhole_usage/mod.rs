//! Wormhole's `usage_record.v1` → Wormhole's usage ledger → `/wh-usage/`.
//!
//! See `manifest.toml`. The ledger is Wormhole's (`usage_core.rs`), the row
//! codec Nanocloud's (`_shared/whusage.rs`); this module moves records from
//! the port through the one into the store with the other, and takes the
//! streams back from the store at start.

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
include!("../_shared/whusage.rs");

const ROWS: &[u8] = b"/wh-usage/";
const ROW_MAX: usize = 320;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    usage_in: i32,
    /// The streams were taken back from the store.
    restored: u8,
    /// A record is held (its row not yet written): `rec`.
    held: u8,
    _pad: [u8; 2],
    retry_ms: u32,
    next_ms: u64,
    nag: ReadNag,
    pub applied: u32,
    pub duplicates: u32,
    pub conflicts: u32,
    pub gaps: u32,
    pub malformed: u32,
    pub refused: u32,
    pub restored_streams: u32,
    ledger: traffic::UsageLedger,
    /// The ledger before the held record, so a write the store refuses
    /// leaves it as it was and the record is applied again.
    before: traffic::UsageLedger,
    rec: [u8; traffic::USAGE_RECORD_LEN],
}

define_params! {
    State;

    1, retry_ms, u32, 1000
        => |s, d, len| { s.retry_ms = p_u32(d, len, 0, 1000); };
}

/// Take every stream back from its row; `false` while the store has not
/// answered the whole listing.
unsafe fn restore(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut ledger = traffic::UsageLedger::default();
    let mut n = 0u32;
    let mut walk = ListWalk::new(ROWS);
    while let Some(key) = walk.next(&store) {
        let mut kb = [0u8; MAX_KEY];
        let kl = key.len().min(MAX_KEY);
        kb[..kl].copy_from_slice(&key[..kl]);
        let mut row = [0u8; ROW_MAX];
        let Some(rl) = get_value(&store, &kb[..kl], &mut row) else {
            if store.stopped() {
                return false;
            }
            continue;
        };
        if usage_restore(&mut ledger, &kb[..kl], &row[..rl]) {
            n += 1;
        }
    }
    if walk.pending || store.stopped() {
        return false;
    }
    s.ledger = ledger;
    s.restored_streams = n;
    true
}

/// Write the row of the stream the held record moved; `true` when settled.
unsafe fn settle(s: &mut State, stream: &traffic::LedgerStream) -> bool {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut k = [0u8; MAX_KEY];
    let mut v = [0u8; ROW_MAX];
    let (Some(kl), Some(vl)) = (usage_key(stream, &mut k), usage_row(stream, &mut v)) else {
        return true;
    };
    match put_value(&store, &k[..kl], &v[..vl]) {
        WriteAnswer::Decided(0) => true,
        WriteAnswer::Pending => false,
        WriteAnswer::Decided(_) => {
            s.refused = s.refused.wrapping_add(1);
            s.next_ms = dev_millis(sys) + u64::from(s.retry_ms);
            false
        }
    }
}

/// One record: applied by the ledger, its stream's row written, then taken
/// off the port. `false` when nothing more can be done this step.
unsafe fn one(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    let len = traffic::USAGE_RECORD_LEN;
    if (sys.channel_peek)(s.usage_in, s.rec.as_mut_ptr(), len) < len as i32 {
        return false;
    }
    if s.held == 0 {
        s.before = traffic::UsageLedger {
            streams: s.ledger.streams,
        };
        s.held = 1;
    }
    let rec = s.rec;
    let Some((outcome, moved)) = usage_ingest(&mut s.ledger, &rec) else {
        s.malformed = s.malformed.wrapping_add(1);
        (sys.channel_discard)(s.usage_in, len);
        s.held = 0;
        return true;
    };
    if let Some(stream) = moved {
        if !settle(s, &stream) {
            // Not written: the ledger goes back, the record stays.
            s.ledger = traffic::UsageLedger {
                streams: s.before.streams,
            };
            return false;
        }
    }
    match outcome {
        traffic::LedgerOutcome::Applied | traffic::LedgerOutcome::Filled => {
            s.applied = s.applied.wrapping_add(1)
        }
        traffic::LedgerOutcome::Gap(_) => {
            s.applied = s.applied.wrapping_add(1);
            s.gaps = s.gaps.wrapping_add(1);
        }
        traffic::LedgerOutcome::Duplicate => s.duplicates = s.duplicates.wrapping_add(1),
        _ => s.conflicts = s.conflicts.wrapping_add(1),
    }
    (sys.channel_discard)(s.usage_in, len);
    s.held = 0;
    true
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
        s.usage_in = in_chan;
        s.retry_ms = 1000;
        s.nag = ReadNag::new();
        parse_tlv(s, params, params_len);
        if s.usage_in < 0 {
            let m = b"[wormhole_usage] usage_in must be wired";
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
        if s.restored == 0 {
            if !restore(s) {
                return 0;
            }
            s.restored = 1;
        }
        for _ in 0..8 {
            if !one(s) {
                return 0;
            }
        }
        0
    }
}
