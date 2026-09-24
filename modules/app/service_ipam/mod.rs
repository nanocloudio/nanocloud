//! Service ClusterIP allocator — IPAM for Services as a PIC module. It drives
//! the control-plane store (`storage.object`/`storage.namespace`): SUBSCRIBE
//! `/services/`, and for each Service without a `clusterIP=` assign the lowest
//! free address from the service CIDR and write it back. The ClusterIP is the
//! stable virtual IP that `proxy_compiler` DNATs to backends and that DNS can
//! front.
//!
//! Data model (compact):
//!
//!   /services/<ns>/<name> = "sel=k=v[;clusterIP=<ip>]"
//!
//! CIDR: 10.96.0.0/16, allocating from 10.96.0.1 upward. Allocation scans all
//! Services to collect the ClusterIPs already in use, then hands each unassigned
//! Service the lowest free address — deterministic and collision-free, and
//! idempotent (a Service that already has `clusterIP=` is skipped, so the
//! `/services/` self-write never loops).

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
include!("../_shared/json.rs");
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

const SERVICES_PREFIX: &[u8] = b"/services/";

/// Service CIDR 10.96.0.0/16, allocating from 10.96.0.1.
const CIDR_BASE: u32 = 0x0A60_0000; // 10.96.0.0
const CIDR_MASK: u32 = 0xFFFF_0000; // /16
const ALLOC_START: u32 = CIDR_BASE + 1; // 10.96.0.1

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const MAX_USED: usize = 256;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    allocations: u32,
}

// ---- helpers ----

fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| b == b';' || b == b',')
            .map(|i| start + i)
            .unwrap_or(value.len());
        let seg = &value[start..end];
        if seg.len() >= tag.len() && &seg[..tag.len()] == tag {
            return Some(&seg[tag.len()..]);
        }
        if end >= value.len() {
            break;
        }
        start = end + 1;
    }
    None
}

/// Parse a dotted-decimal IPv4 into a u32, or None if malformed.
fn parse_ip(s: &[u8]) -> Option<u32> {
    let mut octets = [0u32; 4];
    let mut idx = 0;
    let mut cur: u32 = 0;
    let mut digits = 0;
    for &b in s {
        if b == b'.' {
            if digits == 0 || idx >= 3 {
                return None;
            }
            octets[idx] = cur;
            idx += 1;
            cur = 0;
            digits = 0;
        } else if b.is_ascii_digit() {
            cur = cur * 10 + (b - b'0') as u32;
            digits += 1;
            if cur > 255 {
                return None;
            }
        } else {
            return None;
        }
    }
    if idx != 3 || digits == 0 {
        return None;
    }
    octets[3] = cur;
    Some((octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3])
}

/// Format a u32 as dotted-decimal into `out`; returns the length.
fn format_ip(ip: u32, out: &mut [u8]) -> usize {
    let mut p = 0;
    for shift in [24, 16, 8, 0] {
        if shift != 24 {
            out[p] = b'.';
            p += 1;
        }
        let octet = ((ip >> shift) & 0xFF) as u8;
        p += write_u8(&mut out[p..], octet);
    }
    p
}

fn write_u8(dst: &mut [u8], n: u8) -> usize {
    if n >= 100 {
        dst[0] = b'0' + n / 100;
        dst[1] = b'0' + (n / 10) % 10;
        dst[2] = b'0' + n % 10;
        3
    } else if n >= 10 {
        dst[0] = b'0' + n / 10;
        dst[1] = b'0' + n % 10;
        2
    } else {
        dst[0] = b'0' + n;
        1
    }
}

/// A bounded set of allocated ClusterIPs.
struct Used {
    ips: [u32; MAX_USED],
    n: usize,
}
impl Used {
    fn empty() -> Self {
        Used {
            ips: [0; MAX_USED],
            n: 0,
        }
    }
    fn add(&mut self, ip: u32) {
        if self.n < MAX_USED {
            self.ips[self.n] = ip;
            self.n += 1;
        }
    }
    fn contains(&self, ip: u32) -> bool {
        self.ips[..self.n].contains(&ip)
    }
}

/// Collect every ClusterIP already assigned across all Services.
unsafe fn collect_used(sys: &SyscallTable) -> Used {
    let mut used = Used::empty();
    let mut walk = ListWalk::new(SERVICES_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let mut valbuf = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &keybuf[..klen], &mut valbuf) else {
            continue;
        };
        if let Some(ip) = j_get2(&valbuf[..vlen], b"spec", b"clusterIP").and_then(parse_ip) {
            used.add(ip);
        }
    }
    used
}

/// Lowest free address in the CIDR not already used.
fn next_free(used: &Used) -> Option<u32> {
    let mut ip = ALLOC_START;
    // Stay within the /16 (and bounded scan for safety).
    while (ip & CIDR_MASK) == CIDR_BASE {
        if !used.contains(ip) {
            return Some(ip);
        }
        ip += 1;
    }
    None
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut used = collect_used(sys);

    let mut walk = ListWalk::new(SERVICES_PREFIX);
    let mut allocated = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);

        let mut valbuf = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, &keybuf[..klen], &mut valbuf) else {
            continue;
        };
        if j_get2(&valbuf[..vlen], b"spec", b"clusterIP").is_some() {
            continue; // already allocated
        }
        let Some(ip) = next_free(&used) else {
            break; // CIDR exhausted
        };

        // Set spec.clusterIP = "<ip>" (a JSON string).
        let mut ipbuf = [0u8; 20];
        let mut il = j_cp(&mut ipbuf, 0, b"\"");
        il = format_ip(ip, &mut ipbuf[il..]) + il;
        il = j_cp(&mut ipbuf, il, b"\"");
        let mut nv = [0u8; MAX_VALUE];
        let total = j_set2(
            &valbuf[..vlen],
            b"spec",
            b"clusterIP",
            &ipbuf[..il],
            &mut nv,
        );
        if total == 0 {
            continue;
        }
        if put_value(sys, &keybuf[..klen], &nv[..total]) {
            used.add(ip);
            allocated += 1;
        }
    }
    allocated
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
        s.allocations = 0;
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
                store_subscribe(sys, SERVICES_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.allocations = s.allocations.wrapping_add(reconcile_all(sys));
            return 0;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.allocations = s.allocations.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
