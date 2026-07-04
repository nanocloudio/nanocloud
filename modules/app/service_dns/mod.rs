//! Service DNS zone projector — the cluster DNS zone as a PIC module. It drives
//! the control-plane store (`storage.object`/`storage.namespace`): SUBSCRIBE
//! `/endpoints/` and project, for each Service, an A-record set keyed at its
//! cluster DNS name. Chains off `endpoints_reconciler` (services + pods →
//! endpoints → DNS zone); the `cluster-dns` bundle serves the zone from
//! `/dns/`.
//!
//! Data model (compact):
//!
//!   /endpoints/<ns>/<name>                    = "<pod>=<ip>,<pod>=<ip>"
//!   /dns/<name>.<ns>.svc.cluster.local        = "a=<ip>;a=<ip>"
//!
//! The zone entry is just the endpoints' ready IPs projected onto the k8s DNS
//! name. Written only when it changed (a GET-compare guards the PUT). An empty
//! endpoints set yields an empty record (`a=`) — the name resolves with no
//! addresses rather than vanishing.

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

const ENDPOINTS_PREFIX: &[u8] = b"/endpoints/";
const DNS_PREFIX: &[u8] = b"/dns/";
const DNS_SUFFIX: &[u8] = b".svc.cluster.local";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 256;
const MAX_DOC: usize = 256;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    records: u32,
}

// ---- helpers ----

/// The `<ns>` segment (no trailing slash) after `prefix` in `key`.
fn namespace<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let rest = key.get(prefix.len()..)?;
    let slash = rest.iter().position(|&b| b == b'/')?;
    Some(&rest[..slash])
}

/// Project one endpoints doc to its DNS A-record set. Returns true if written.
/// `ep_val` is `<pod>=<ip>,<pod>=<ip>`; the record is `a=<ip>;a=<ip>`.
unsafe fn reconcile_endpoints(sys: &SyscallTable, ep_key: &[u8], ep_val: &[u8]) -> bool {
    let Some(ns) = namespace(ep_key, ENDPOINTS_PREFIX) else {
        return false;
    };
    let name = last_seg(ep_key);

    // DNS name: /dns/<name>.<ns>.svc.cluster.local
    let mut dkey = [0u8; MAX_KEY];
    let mut k = 0;
    k = append(&mut dkey, k, DNS_PREFIX);
    k = append(&mut dkey, k, name);
    k = append(&mut dkey, k, b".");
    k = append(&mut dkey, k, ns);
    k = append(&mut dkey, k, DNS_SUFFIX);
    if k >= dkey.len() {
        return false;
    }

    // A records from each `<pod>=<ip>` backend.
    let mut doc = [0u8; MAX_DOC];
    let mut d = 0;
    let mut first = true;
    for backend in ep_val.split(|&b| b == b',') {
        if backend.is_empty() {
            continue;
        }
        let ip = match backend.iter().position(|&b| b == b'=') {
            Some(i) => &backend[i + 1..],
            None => continue,
        };
        if ip.is_empty() {
            continue;
        }
        if !first {
            d = append(&mut doc, d, b";");
        }
        first = false;
        d = append(&mut doc, d, b"a=");
        d = append(&mut doc, d, ip);
    }

    // Only spend a revision when the record changed.
    let mut cur = [0u8; MAX_DOC];
    if let Some(cl) = get_value(sys, &dkey[..k], &mut cur) {
        if cur[..cl] == doc[..d] {
            return false;
        }
    } else if d == 0 {
        return false; // absent and empty — nothing to record
    }
    put_value(sys, &dkey[..k], &doc[..d])
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(ENDPOINTS_PREFIX);
    let mut wrote = 0u32;
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
        if reconcile_endpoints(sys, &keybuf[..klen], &valbuf[..vlen]) {
            wrote += 1;
        }
    }
    wrote
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
        s.records = 0;
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
                store_subscribe(sys, ENDPOINTS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.records = s.records.wrapping_add(reconcile_all(sys));
            return 0;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.records = s.records.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
