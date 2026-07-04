//! snapshot_reconciler — the VolumeSnapshot controller as a PIC module — the
//! CSI snapshot.storage.k8s.io surface. For each VolumeSnapshot it provisions a
//! bound VolumeSnapshotContent and marks the pair ready — the control-plane
//! half of a snapshot. The PHYSICAL data snapshot (copying the volume) is a
//! node-backend effect (a PIC module has no filesystem), so this records a
//! logical snapshot handle referencing the source claim; a real data copy is a
//! backend follow-up.
//!
//! Data model (real k8s objects, stored by the apiserver):
//!   /volumesnapshots.snapshot.storage.k8s.io/<ns>/<name>
//!       spec.source.persistentVolumeClaimName  → the source PVC
//!       status.{readyToUse,boundVolumeSnapshotContentName}  (set here)
//!   /volumesnapshotcontents.snapshot.storage.k8s.io/snapcontent-<name>
//!       spec.{volumeSnapshotRef,source.volumeHandle}, status.readyToUse (created here)
//!
//! Loop (level-triggered): watch VolumeSnapshots; for each not yet ready with a
//! source PVC, create its content + bind + mark ready. Idempotent (skips a
//! VolumeSnapshot whose status.readyToUse is already true).

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
include!("../_shared/json.rs");
include!("../_shared/store.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_CLOSE: u32 = 0x1425;
const OBJ_DELETE: u32 = 0x1424;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const VS_PREFIX: &[u8] = b"/volumesnapshots.snapshot.storage.k8s.io/";
const VSC_PREFIX: &[u8] = b"/volumesnapshotcontents.snapshot.storage.k8s.io/";

const MAX_KEY: usize = 160;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    sink: i32,
    subscribed: u8,
    bound: u32,
}

// ---- store helpers ----

// ---- helpers ----

fn cp(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

fn namespace_seg<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    if key.len() < prefix.len() {
        return None;
    }
    let rest = &key[prefix.len()..];
    rest.iter().position(|&b| b == b'/').map(|i| &rest[..i])
}

/// Reconcile one VolumeSnapshot: if it has a source PVC and isn't ready yet,
/// create a bound VolumeSnapshotContent and mark both ready. Returns 1 if it
/// bound this pass, else 0.
unsafe fn reconcile_one(sys: &SyscallTable, vs_key: &[u8], vs_val: &[u8]) -> u32 {
    // Idempotent: already ready?
    if j_get2(vs_val, b"status", b"readyToUse") == Some(b"true") {
        return 0;
    }
    // Source PVC (spec.source.persistentVolumeClaimName).
    let Some(pvc) = j_get3(vs_val, b"spec", b"source", b"persistentVolumeClaimName") else {
        return 0;
    };
    let Some(ns) = namespace_seg(vs_key, VS_PREFIX) else {
        return 0;
    };
    let name = last_seg(vs_key);

    // Content name = snapcontent-<name>.
    let mut cname = [0u8; 96];
    let mut cnl = cp(&mut cname, 0, b"snapcontent-");
    cnl = cp(&mut cname, cnl, name);

    // Create the VolumeSnapshotContent (cluster-scoped).
    let mut ckey = [0u8; MAX_KEY];
    let ckl = cp(&mut ckey, 0, VSC_PREFIX);
    let ckl = cp(&mut ckey, ckl, &cname[..cnl]);
    let mut content = [0u8; MAX_VALUE];
    let mut d = cp(&mut content, 0, b"{\"metadata\":{\"name\":\"");
    d = cp(&mut content, d, &cname[..cnl]);
    d = cp(
        &mut content,
        d,
        b"\"},\"spec\":{\"driver\":\"nanocloud.io/local\",\"deletionPolicy\":\"Delete\",\"volumeSnapshotRef\":{\"name\":\"",
    );
    d = cp(&mut content, d, name);
    d = cp(&mut content, d, b"\",\"namespace\":\"");
    d = cp(&mut content, d, ns);
    d = cp(&mut content, d, b"\"},\"source\":{\"volumeHandle\":\"");
    d = cp(&mut content, d, pvc);
    d = cp(
        &mut content,
        d,
        b"\"}},\"status\":{\"readyToUse\":true,\"snapshotHandle\":\"snap-",
    );
    d = cp(&mut content, d, name);
    d = cp(&mut content, d, b"\",\"restoreSize\":0}}");
    put_value(sys, &ckey[..ckl], &content[..d]);

    // Bind + mark the VolumeSnapshot ready (set its whole status object).
    let mut status = [0u8; 256];
    let mut sl = cp(
        &mut status,
        0,
        b"{\"readyToUse\":true,\"boundVolumeSnapshotContentName\":\"",
    );
    sl = cp(&mut status, sl, &cname[..cnl]);
    sl = cp(&mut status, sl, b"\",\"restoreSize\":\"0\"}");
    let mut updated = [0u8; MAX_VALUE];
    let ul = j_set_top(vs_val, b"status", &status[..sl], &mut updated);
    if ul == 0 {
        return 0;
    }
    if put_value(sys, vs_key, &updated[..ul]) {
        1
    } else {
        0
    }
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(VS_PREFIX);
    let mut bound = 0u32;
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
        // Copy the value out so reconcile_one can write back the same key.
        let mut val = [0u8; MAX_VALUE];
        val[..vlen].copy_from_slice(&valbuf[..vlen]);
        bound += reconcile_one(sys, &keybuf[..klen], &val[..vlen]);
    }
    bound
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
        s.sink = in_chan;
        s.subscribed = 0;
        s.bound = 0;
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
                store_subscribe(sys, VS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.bound = s.bound.wrapping_add(reconcile_all(sys));
            return 0;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.bound = s.bound.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
