//! CNI IPAM — pod IP allocation as a PIC module. The CNI plugin's decision
//! half: which IP a pod gets. Pure store transform — watch /ipam-request/<uid>,
//! allocate the lowest free host from the pool CIDR, and write the lease. Only
//! the allocation *decision* lives here; the plugin's *effect* (veth,
//! addressing) is realized by the node's network backend, which reads the
//! lease.
//!
//! Data model:
//!   /ipam-pool          = "cidr=<a.b.c.0>/<prefix>"   (optional; default /24)
//!   /ipam-request/<uid> = ""                          (a pod needs an IP)
//!   /ipam-lease/<uid>   = "<ip>"                       (the allocated address)
//!
//! Allocation is deterministic (lowest free host, .1 upward, skipping the
//! network and broadcast) and stable — an existing lease is never reassigned,
//! so re-running is idempotent. v1 handles a single /8../30 IPv4 pool.

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
include!("../_shared/store.rs");

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes + CAS, storage.namespace
// (0x13) prefix LIST + change SUBSCRIBE. Changes are pushed to us as
// namespace.change on the `changes` input channel (allocated by a graph
// self-edge); we drain them to detect movement, then re-reconcile.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const POOL_KEY: &[u8] = b"/ipam-pool";
const REQ_PREFIX: &[u8] = b"/ipam-request/";
const LEASE_PREFIX: &[u8] = b"/ipam-lease/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 256;
/// Max hosts we track for the free-scan bitmap (a /24 → 256; larger pools are
/// bounded to this window from the base).
const MAX_HOSTS: usize = 1024;

const DEFAULT_BASE: u32 = 0x0AF4_0000; // 10.244.0.0
const DEFAULT_PREFIX: u32 = 24;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    leases: u32,
}

// ---- IPv4 parse/format + pool ----

/// Parse a dotted-quad IPv4 into a u32; None on malformed input.
fn parse_ipv4(s: &[u8]) -> Option<u32> {
    let mut octets = [0u32; 4];
    let mut oi = 0;
    let mut cur: u32 = 0;
    let mut digits = 0;
    for &b in s {
        if b == b'.' {
            if digits == 0 || oi >= 3 || cur > 255 {
                return None;
            }
            octets[oi] = cur;
            oi += 1;
            cur = 0;
            digits = 0;
        } else if b.is_ascii_digit() {
            cur = cur * 10 + (b - b'0') as u32;
            digits += 1;
            if cur > 255 || digits > 3 {
                return None;
            }
        } else {
            return None;
        }
    }
    if digits == 0 || oi != 3 || cur > 255 {
        return None;
    }
    octets[3] = cur;
    Some((octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3])
}

/// Format a u32 as dotted-quad IPv4 into `dst`; returns the length.
fn format_ipv4(dst: &mut [u8], ip: u32) -> usize {
    fn octet(dst: &mut [u8], at: usize, v: u32) -> usize {
        let mut p = at;
        if v >= 100 {
            dst[p] = b'0' + (v / 100) as u8;
            p += 1;
        }
        if v >= 10 {
            dst[p] = b'0' + ((v / 10) % 10) as u8;
            p += 1;
        }
        dst[p] = b'0' + (v % 10) as u8;
        p + 1
    }
    let mut p = 0;
    for i in 0..4 {
        if i > 0 {
            dst[p] = b'.';
            p += 1;
        }
        p = octet(dst, p, (ip >> (24 - i * 8)) & 0xff);
    }
    p
}

/// Read the pool (base, prefix) from /ipam-pool, or the default.
unsafe fn read_pool(sys: &SyscallTable) -> (u32, u32) {
    let mut buf = [0u8; MAX_VALUE];
    let Some(n) = get_value(sys, POOL_KEY, &mut buf) else {
        return (DEFAULT_BASE, DEFAULT_PREFIX);
    };
    // "cidr=<a.b.c.d>/<prefix>"
    let v = &buf[..n];
    let cidr = match v.windows(5).position(|w| w == b"cidr=") {
        Some(i) => &v[i + 5..],
        None => return (DEFAULT_BASE, DEFAULT_PREFIX),
    };
    let slash = match cidr.iter().position(|&b| b == b'/') {
        Some(i) => i,
        None => return (DEFAULT_BASE, DEFAULT_PREFIX),
    };
    let base = match parse_ipv4(&cidr[..slash]) {
        Some(b) => b,
        None => return (DEFAULT_BASE, DEFAULT_PREFIX),
    };
    let mut prefix = 0u32;
    for &b in &cidr[slash + 1..] {
        if b.is_ascii_digit() {
            prefix = prefix * 10 + (b - b'0') as u32;
        } else {
            break;
        }
    }
    if prefix == 0 || prefix > 30 {
        return (DEFAULT_BASE, DEFAULT_PREFIX);
    }
    (base, prefix)
}

/// The lowest free host in [base+1 .. broadcast-1] not already leased, scanning
/// the current /ipam-lease/ entries. 0 if the pool is exhausted.
unsafe fn allocate(sys: &SyscallTable, base: u32, prefix: u32) -> u32 {
    let host_bits = 32 - prefix;
    let size: u32 = if host_bits >= 32 {
        u32::MAX
    } else {
        1u32 << host_bits
    };
    let network = base & !(size - 1);
    let broadcast = network | (size - 1);

    // Mark leased hosts (offset from network) in a bounded bitmap.
    let mut used = [false; MAX_HOSTS];
    let mut walk = ListWalk::new(LEASE_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);

        let mut vbuf = [0u8; MAX_VALUE];
        if let Some(vlen) = get_value(sys, &keybuf[..klen], &mut vbuf) {
            if let Some(ip) = parse_ipv4(&vbuf[..vlen]) {
                if ip > network && ip < broadcast {
                    let off = (ip - network) as usize;
                    if off < MAX_HOSTS {
                        used[off] = true;
                    }
                }
            }
        }
    }

    // Lowest free host: offsets 1.. (skip network), below broadcast + bitmap.
    let hi = ((broadcast - network) as usize).min(MAX_HOSTS);
    match used[1..hi].iter().position(|&u| !u) {
        Some(p) => network + (p + 1) as u32,
        None => 0,
    }
}

/// Allocate a lease for every /ipam-request/ without one. Returns leases granted.
unsafe fn reconcile(sys: &SyscallTable) -> u32 {
    let (base, prefix) = read_pool(sys);

    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut granted = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let uid = last_seg(&keybuf[..klen]);

        // Lease key.
        let mut lkey = [0u8; MAX_KEY];
        let llen = LEASE_PREFIX.len() + uid.len();
        if llen > lkey.len() {
            continue;
        }
        lkey[..LEASE_PREFIX.len()].copy_from_slice(LEASE_PREFIX);
        lkey[LEASE_PREFIX.len()..llen].copy_from_slice(uid);

        // Already leased? Stable — never reassign.
        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &lkey[..llen], &mut probe).is_some() {
            continue;
        }

        let ip = allocate(sys, base, prefix);
        if ip == 0 {
            continue; // pool exhausted
        }
        let mut ipbuf = [0u8; 16];
        let iplen = format_ipv4(&mut ipbuf, ip);
        if put_value(sys, &lkey[..llen], &ipbuf[..iplen]) {
            granted += 1;
        }
    }
    granted
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

#[no_mangle]
#[link_section = ".text.module_new"]
pub extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    _params: *const u8,
    _params_len: usize,
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
        s.out_chan = out_chan;
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.leases = 0;
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

        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REQ_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.leases = s.leases.wrapping_add(reconcile(sys));
            return 0;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.leases = s.leases.wrapping_add(reconcile(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
