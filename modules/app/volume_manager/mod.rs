//! Volume manager — the mount-PLANNING decision as a PIC module. Attaching a
//! pod's volumes splits into a decision (resolve each mount's claim to its
//! bound device + filesystem; flag unbound claims as pending) and an effect
//! (the mount syscall). Only the decision is a pure store transform, so only it
//! lives here; the node's storage backend performs the mount from the plan.
//!
//! Data model:
//!   /volume-requests/<pod> = "vols=<mount>=<claim>,<mount>=<claim>,..."
//!   /volumes/<claim>       = "device=<path>;fs=<type>"   (a bound PV/PVC)
//!   /volume-plan/<pod>     = "mount=<mount>:<device>:<fs>,...;pending=<mount>,..."
//!
//! Loop (level-triggered): watch /volume-requests/ + /volumes/; on change, for
//! each pod resolve each mount's claim and PUT the plan when it changed. A claim
//! that has no /volumes/ binding yet lands in `pending=`; when it binds, the
//! mount moves into `mount=` on its own.

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
include!("../_shared/field.rs");

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

const REQ_PREFIX: &[u8] = b"/volume-requests/";
const VOLUMES_PREFIX: &[u8] = b"/volumes/";
const PLAN_PREFIX: &[u8] = b"/volume-plan/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 1024;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    plans: u32,
}

// ---- helpers ----

fn tail_after<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    key.get(prefix.len()..)
}

/// Look up a claim's binding: writes "<device>:<fs>" into `dst`, returns its
/// length, or None if the claim is unbound.
unsafe fn resolve_claim(sys: &SyscallTable, claim: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut key = [0u8; MAX_KEY];
    let klen = VOLUMES_PREFIX.len() + claim.len();
    if klen > key.len() {
        return None;
    }
    key[..VOLUMES_PREFIX.len()].copy_from_slice(VOLUMES_PREFIX);
    key[VOLUMES_PREFIX.len()..klen].copy_from_slice(claim);

    let mut vbuf = [0u8; MAX_VALUE];
    let vlen = get_value(sys, &key[..klen], &mut vbuf)?;
    let device = field(&vbuf[..vlen], b"device=")?;
    let fs = field(&vbuf[..vlen], b"fs=").unwrap_or(b"");
    let mut p = append(dst, 0, device);
    p = append(dst, p, b":");
    Some(append(dst, p, fs))
}

/// Compute the mount plan for one pod's volume request.
unsafe fn compute_plan(sys: &SyscallTable, request: &[u8], plan: &mut [u8]) -> usize {
    let vols = field(request, b"vols=").unwrap_or(b"");
    let mut mounts = [0u8; MAX_VALUE];
    let mut ml = 0usize;
    let mut mfirst = true;
    let mut pending = [0u8; MAX_VALUE];
    let mut pl = 0usize;
    let mut pfirst = true;

    let mut start = 0;
    while start <= vols.len() {
        let end = vols[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(vols.len());
        let entry = &vols[start..end];
        if let Some(eq) = entry.iter().position(|&b| b == b'=') {
            let name = &entry[..eq];
            let claim = &entry[eq + 1..];
            let mut resolved = [0u8; MAX_VALUE];
            match resolve_claim(sys, claim, &mut resolved) {
                Some(rlen) => {
                    if !mfirst {
                        ml = append(&mut mounts, ml, b",");
                    }
                    mfirst = false;
                    ml = append(&mut mounts, ml, name);
                    ml = append(&mut mounts, ml, b":");
                    ml = append(&mut mounts, ml, &resolved[..rlen]);
                }
                None => {
                    if !pfirst {
                        pl = append(&mut pending, pl, b",");
                    }
                    pfirst = false;
                    pl = append(&mut pending, pl, name);
                }
            }
        }
        if end >= vols.len() {
            break;
        }
        start = end + 1;
    }

    let mut p = append(plan, 0, b"mount=");
    p = append(plan, p, &mounts[..ml]);
    p = append(plan, p, b";pending=");
    append(plan, p, &pending[..pl])
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(REQ_PREFIX);
    let mut wrote = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        let mut valbuf = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut valbuf) else {
            continue;
        };

        let mut plan = [0u8; MAX_VALUE];
        let plen = compute_plan(sys, &valbuf[..vlen], &mut plan);

        let Some(tail) = tail_after(key, REQ_PREFIX) else {
            continue;
        };
        let mut pkey = [0u8; MAX_KEY];
        let pkl = PLAN_PREFIX.len() + tail.len();
        if pkl > pkey.len() {
            continue;
        }
        pkey[..PLAN_PREFIX.len()].copy_from_slice(PLAN_PREFIX);
        pkey[PLAN_PREFIX.len()..pkl].copy_from_slice(tail);

        let mut cur = [0u8; MAX_VALUE];
        if let Some(clen) = get_value(sys, &pkey[..pkl], &mut cur) {
            if cur[..clen] == plan[..plen] {
                continue;
            }
        }
        if put_value(sys, &pkey[..pkl], &plan[..plen]) {
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
        s.plans = 0;
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
                store_subscribe(sys, VOLUMES_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.plans = s.plans.wrapping_add(reconcile_all(sys));
            return 0;
        }

        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.plans = s.plans.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
