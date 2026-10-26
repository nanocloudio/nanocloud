//! Kagi-signed `service_dns` projections → `/wh-dns/<service hex>` rows.
//!
//! See `manifest.toml`. Opened with Kagi's codec (`_shared/whdns.rs`
//! `ksdn_open`), decoded with Wormhole's (`ServiceDnsProjection`), written
//! verbatim only when newer than the row; `cluster_dns` is the reader.

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
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha384.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/hmac.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/p256.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/crypto/ed25519.rs");
include!("../_shared/store.rs");
include!("../_shared/wormhole_portfolio.rs");
/// Kagi's `KSDN` codec: the signed `service_dns` projection.
#[path = "../../../target/fluxor/kagi-common/service_dns.rs"]
mod ksdn;
include!("../_shared/whdns.rs");

const REC_MAX: usize = 512;
const ID_MAX: usize = 32;
const WH_DNS: &[u8] = b"/wh-dns/";

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    ksdn_in: i32,
    held: u8,
    issuer_len: u8,
    kid_len: u8,
    public_ok: u8,
    rec_len: u32,
    retry_ms: u32,
    next_ms: u64,
    nag: ReadNag,
    pub written: u32,
    pub unchanged: u32,
    pub older: u32,
    pub refused: u32,
    pub store_refused: u32,
    issuer: [u8; ID_MAX],
    kid: [u8; ID_MAX],
    public: [u8; 32],
    rec: [u8; REC_MAX + 2],
    cur: [u8; REC_MAX],
}

fn hex32(t: &[u8], out: &mut [u8; 32]) -> bool {
    if t.len() != 64 {
        return false;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    for i in 0..32 {
        match (nib(t[2 * i]), nib(t[2 * i + 1])) {
            (Some(h), Some(l)) => out[i] = (h << 4) | l,
            _ => return false,
        }
    }
    true
}

define_params! {
    State;

    1, dns_issuer, str, 0
        => |s, d, len| {
            let len = str_len(d, len);
            let n = if len > ID_MAX { 0 } else { len };
            s.issuer_len = n as u8;
            if n > 0 { core::ptr::copy_nonoverlapping(d, s.issuer.as_mut_ptr(), n); }
        };
    2, dns_kid, str, 0
        => |s, d, len| {
            let len = str_len(d, len);
            let n = if len > ID_MAX { 0 } else { len };
            s.kid_len = n as u8;
            if n > 0 { core::ptr::copy_nonoverlapping(d, s.kid.as_mut_ptr(), n); }
        };
    3, dns_public, str, 0
        => |s, d, len| {
            let len = str_len(d, len);
            s.public_ok = u8::from(hex32(core::slice::from_raw_parts(d, len), &mut s.public));
        };
    4, retry_ms, u32, 1000
        => |s, d, len| { s.retry_ms = p_u32(d, len, 0, 1000); };
}

/// A string param's length without the terminating NUL the builder writes.
///
/// # Safety
/// `d` is valid for `len` bytes.
unsafe fn str_len(d: *const u8, len: usize) -> usize {
    let mut n = 0;
    while n < len && *d.add(n) != 0 {
        n += 1;
    }
    n
}

/// Kagi's `SERVICE_DNS` suite 2: Ed25519.
fn ksdn_verify(suite: u16, public: &[u8], message: &[u8], signature: &[u8]) -> bool {
    match (
        suite,
        <&[u8; 32]>::try_from(public),
        <&[u8; 64]>::try_from(signature),
    ) {
        (2, Ok(p), Ok(sig)) => ed25519_verify(p, message, sig),
        _ => false,
    }
}

/// The projection a stored or offered record vouches for, opened now.
unsafe fn projection(s: &State, signed: &[u8]) -> Option<traffic::ServiceDnsProjection> {
    let keys = [DnsKey {
        issuer: &s.issuer[..usize::from(s.issuer_len)],
        kid: &s.kid[..usize::from(s.kid_len)],
        suite: 2,
        public: &s.public,
    }];
    // The projection's window is wall-clock time (Kagi signs it so), not
    // this runtime's uptime.
    let now = dev_unix_millis(&*s.syscalls);
    let bytes = ksdn_open(signed, &keys, ksdn_verify, now).ok()?;
    traffic::ServiceDnsProjection::decode(&bytes).ok()
}

/// Take the next framed record off the port into `rec`; `false` when none
/// is whole yet.
unsafe fn take(s: &mut State) -> bool {
    let sys = &*s.syscalls;
    if (sys.channel_peek)(s.ksdn_in, s.rec.as_mut_ptr(), 2) < 2 {
        return false;
    }
    let n = usize::from(u16::from_le_bytes([s.rec[0], s.rec[1]]));
    if n == 0 || n > REC_MAX {
        (sys.channel_discard)(s.ksdn_in, 2 + n.min(REC_MAX));
        s.refused = s.refused.wrapping_add(1);
        return false;
    }
    if (sys.channel_peek)(s.ksdn_in, s.rec.as_mut_ptr(), 2 + n) < (2 + n) as i32 {
        return false;
    }
    (sys.channel_discard)(s.ksdn_in, 2 + n);
    s.rec_len = n as u32;
    s.held = 1;
    true
}

/// Write the held record if it opens and is newer than its row; `true`
/// when settled.
unsafe fn settle(s: &mut State) -> bool {
    let rec = s.rec;
    let signed = &rec[2..2 + s.rec_len as usize];
    let Some(p) = projection(s, signed) else {
        s.refused = s.refused.wrapping_add(1);
        let m =
            b"[wormhole_dns] a projection that does not open under the SERVICE_DNS key - refused";
        dev_log(&*s.syscalls, 1, m.as_ptr(), m.len());
        return true;
    };
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut key = [0u8; MAX_KEY];
    let mut kl = append(&mut key, 0, WH_DNS);
    for v in p.service_id {
        const H: &[u8; 16] = b"0123456789abcdef";
        kl = append(
            &mut key,
            kl,
            &[H[usize::from(v >> 4)], H[usize::from(v & 15)]],
        );
    }
    let mut cur = [0u8; REC_MAX];
    match get_value(&store, &key[..kl], &mut cur) {
        Some(n) if cur[..n] == *signed => {
            s.unchanged = s.unchanged.wrapping_add(1);
            return true;
        }
        Some(n) => {
            // A row that still opens and is not older stays.
            if let Some(held) = projection(s, &cur[..n]) {
                if (held.global_generation, held.generation) >= (p.global_generation, p.generation)
                {
                    s.older = s.older.wrapping_add(1);
                    return true;
                }
            }
        }
        None if store.stopped() => return false,
        None => {}
    }
    match put_value(&store, &key[..kl], signed) {
        WriteAnswer::Decided(0) => {
            s.written = s.written.wrapping_add(1);
            true
        }
        WriteAnswer::Pending => false,
        WriteAnswer::Decided(_) => {
            s.store_refused = s.store_refused.wrapping_add(1);
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
        s.ksdn_in = in_chan;
        s.retry_ms = 1000;
        s.nag = ReadNag::new();
        parse_tlv(s, params, params_len);
        if s.ksdn_in < 0 || s.public_ok == 0 || s.issuer_len == 0 || s.kid_len == 0 {
            let m = b"[wormhole_dns] ksdn_in must be wired and the SERVICE_DNS key given";
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
