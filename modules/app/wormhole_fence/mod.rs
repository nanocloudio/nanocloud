//! The regional fence provider's decision → Wormhole's `RegionFence`.
//!
//! See `manifest.toml`. Every `poll_ms` the region's rows are read
//! (`/wh-fence/<region>/…`): the provider's custody generation, then every
//! transit link (mechanism `upstream`) or power feed (mechanism `power`) as
//! its observers last wrote it. `_shared/whfence.rs` decides; a
//! confirmation leaves once per custody generation as the `region_ctl.v1`
//! record on `fence_out` and is written to `/wh-fence/<region>/confirmed`;
//! anything less confirms nothing and says why, once per change.

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
include!("../_shared/whfence.rs");

/// Links or feeds one region's decision weighs.
const MEMBERS_MAX: usize = 32;
const ROW_MAX: usize = 128;

const MECH_UPSTREAM: u8 = 0;
const MECH_POWER: u8 = 1;

/// Why the last pass confirmed nothing (logged once per change).
mod why {
    pub const NONE: u8 = 0;
    pub const NO_CUSTODY: u8 = 1;
    pub const UNKNOWN: u8 = 2;
    pub const UP: u8 = 3;
    pub const BAD_ROW: u8 = 4;
    pub const STORE: u8 = 5;
    pub const CONFIRMED: u8 = 6;
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    fence_out: i32,
    region: u32,
    region_ok: u8,
    mechanism: u8,
    last_why: u8,
    /// A record is owed on `fence_out` (it did not fit): `rec[..rec_len]`.
    owed: u8,
    poll_ms: u32,
    rec_len: u32,
    next_ms: u64,
    /// The custody generation a confirmation was last given under.
    confirmed_gen: u32,
    _pad: u32,
    nag: ReadNag,
    pub confirmations: u32,
    pub unproven: u32,
    rec: [u8; 128],
}

define_params! {
    State;

    1, region, u32, 0
        => |s, d, len| { s.region = p_u32(d, len, 0, 0); s.region_ok = 1; };
    2, mechanism, u8, 0, enum { upstream=0, power=1 }
        => |s, d, len| { s.mechanism = p_u8(d, len, 0, 0); };
    3, poll_ms, u32, 500
        => |s, d, len| { s.poll_ms = p_u32(d, len, 0, 500); };
}

/// `<k>=<v>` in a `;`-separated row.
fn field<'a>(row: &'a [u8], k: &[u8]) -> Option<&'a [u8]> {
    for kv in row.split(|&b| b == b';') {
        if kv.len() > k.len() && kv.starts_with(k) && kv[k.len()] == b'=' {
            return Some(&kv[k.len() + 1..]);
        }
    }
    None
}

fn flag(row: &[u8], k: &[u8]) -> Option<bool> {
    match field(row, k)? {
        b"1" => Some(true),
        b"0" => Some(false),
        _ => None,
    }
}

fn number(v: &[u8]) -> Option<u32> {
    if v.is_empty() || v.len() > 10 {
        return None;
    }
    let mut n: u64 = 0;
    for &c in v {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + u64::from(c - b'0');
    }
    u32::try_from(n).ok()
}

/// `/wh-fence/<region>/` plus `tail` into `out`; its length.
fn key(region: u32, tail: &[u8], out: &mut [u8; MAX_KEY]) -> usize {
    let mut n = append(out, 0, b"/wh-fence/");
    let mut d = [0u8; 10];
    let mut i = d.len();
    let mut r = region;
    loop {
        i -= 1;
        d[i] = b'0' + (r % 10) as u8;
        r /= 10;
        if r == 0 {
            break;
        }
    }
    n = append(out, n, &d[i..]);
    n = append(out, n, b"/");
    append(out, n, tail)
}

unsafe fn note(s: &mut State, w: u8) {
    if w == s.last_why {
        return;
    }
    s.last_why = w;
    let m: &[u8] = match w {
        why::NO_CUSTODY => b"[wormhole_fence] unproven: no custody of the region",
        why::UNKNOWN => b"[wormhole_fence] unproven: no link or feed of the region is known",
        why::UP => {
            b"[wormhole_fence] unproven: a link is up or a feed powered, or its cut unacknowledged"
        }
        why::BAD_ROW => b"[wormhole_fence] unproven: a row does not parse",
        why::STORE => b"[wormhole_fence] unproven: the store refused a read",
        why::CONFIRMED => b"[wormhole_fence] confirmed",
        _ => return,
    };
    dev_log(&*s.syscalls, 2, m.as_ptr(), m.len());
}

/// One pass: read the region's rows and decide. `false` when a read was not
/// answered yet (the pass is asked again).
unsafe fn pass(s: &mut State, now: u64) -> bool {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut k = [0u8; MAX_KEY];
    let mut row = [0u8; ROW_MAX];
    let kn = key(s.region, b"custody", &mut k);
    let custody = match get_value(&store, &k[..kn], &mut row) {
        Some(n) => field(&row[..n], b"gen").and_then(number),
        None if store.waiting() => return false,
        None => Some(0),
    };
    let Some(custody) = custody else {
        note(s, why::BAD_ROW);
        return true;
    };
    let tail: &[u8] = if s.mechanism == MECH_POWER {
        b"feed/"
    } else {
        b"link/"
    };
    let pn = key(s.region, tail, &mut k);
    let mut links = [UpstreamLink::default(); MEMBERS_MAX];
    let mut feeds = [PowerFeed::default(); MEMBERS_MAX];
    let mut n = 0usize;
    let mut bad = false;
    let mut walk = ListWalk::new(&k[..pn]);
    while let Some(member) = walk.next(&store) {
        let mut mk = [0u8; MAX_KEY];
        let ml = member.len().min(MAX_KEY);
        mk[..ml].copy_from_slice(&member[..ml]);
        let Some(rn) = get_value(&store, &mk[..ml], &mut row) else {
            if store.waiting() {
                return false;
            }
            continue;
        };
        if n == MEMBERS_MAX {
            // More members than the decision weighs: nothing is proven.
            bad = true;
            break;
        }
        let r = &row[..rn];
        if s.mechanism == MECH_POWER {
            match (flag(r, b"off"), flag(r, b"ack")) {
                (Some(off), Some(ack)) => {
                    feeds[n] = PowerFeed {
                        outlets_off: off,
                        acknowledged: ack,
                    }
                }
                _ => bad = true,
            }
        } else {
            match (flag(r, b"down"), flag(r, b"ack")) {
                (Some(down), Some(ack)) => {
                    links[n] = UpstreamLink {
                        session_down: down,
                        cut_acknowledged: ack,
                    }
                }
                _ => bad = true,
            }
        }
        n += 1;
    }
    if store.waiting() {
        return false;
    }
    if walk.failed || store.refusal().is_some() {
        note(s, why::STORE);
        return true;
    }
    if bad {
        note(s, why::BAD_ROW);
        s.unproven = s.unproven.wrapping_add(1);
        return true;
    }
    let decided = if s.mechanism == MECH_POWER {
        fence_power(s.region, &feeds[..n], custody, now)
    } else {
        fence_upstream_cut(s.region, &links[..n], custody, now)
    };
    let f = match decided {
        Ok(f) => f,
        Err(e) => {
            s.unproven = s.unproven.wrapping_add(1);
            note(
                s,
                match e {
                    FenceUnproven::NoCustody => why::NO_CUSTODY,
                    FenceUnproven::Unknown => why::UNKNOWN,
                    _ => why::UP,
                },
            );
            return true;
        }
    };
    if custody == s.confirmed_gen {
        // Confirmed once under this custody generation already.
        return true;
    }
    let Some(len) = fence_record(&f, &mut s.rec) else {
        return true;
    };
    s.rec_len = len as u32;
    s.owed = 1;
    // The confirmation's row: what was confirmed, under which custody.
    let mut v = [0u8; 96];
    let mut vn = append(
        &mut v,
        0,
        if s.mechanism == MECH_POWER {
            b"kind=power;gen="
        } else {
            b"kind=upstream;gen="
        },
    );
    let mut d = [0u8; 20];
    for (val, label) in [(u64::from(custody), &b";at="[..]), (now, &b""[..])] {
        let mut i = d.len();
        let mut x = val;
        loop {
            i -= 1;
            d[i] = b'0' + (x % 10) as u8;
            x /= 10;
            if x == 0 {
                break;
            }
        }
        vn = append(&mut v, vn, &d[i..]);
        vn = append(&mut v, vn, label);
    }
    let cn = key(s.region, b"confirmed", &mut k);
    let _ = put_value(&store, &k[..cn], &v[..vn]);
    s.confirmed_gen = custody;
    s.confirmations = s.confirmations.wrapping_add(1);
    note(s, why::CONFIRMED);
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
    _in_chan: i32,
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
        core::ptr::write_bytes(state, 0, core::mem::size_of::<State>());
        let s = &mut *(state as *mut State);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.fence_out = out_chan;
        s.poll_ms = 500;
        s.nag = ReadNag::new();
        parse_tlv(s, params, params_len);
        if s.fence_out < 0 || s.region_ok == 0 || s.mechanism > MECH_POWER || s.poll_ms == 0 {
            let m = b"[wormhole_fence] fence_out must be wired, region given and mechanism upstream|power";
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
        if s.owed != 0 {
            let n = s.rec_len as usize;
            if (sys.channel_write)(s.fence_out, s.rec.as_ptr(), n) != n as i32 {
                return 0;
            }
            s.owed = 0;
        }
        let now = dev_millis(sys);
        if now < s.next_ms {
            return 0;
        }
        if pass(s, now) {
            s.next_ms = now + u64::from(s.poll_ms);
        }
        0
    }
}
