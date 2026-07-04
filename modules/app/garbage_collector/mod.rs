//! Garbage collector — the ownerRef cascade as a PIC module. It drives the
//! control-plane store (`storage.object`/`storage.namespace`): SUBSCRIBE the
//! workload prefixes and delete any object whose `owner=` no longer resolves to
//! a live object — a ReplicaSet whose Deployment is gone, a Pod whose
//! ReplicaSet is gone. Over successive reconcile passes this cascades: delete a
//! Deployment, and next pass its ReplicaSet is collected (owner absent), then
//! the pass after its Pods are collected. The delete half of the workload
//! lifecycle, complementing `deployment_reconciler` + `replicaset_reconciler`.
//!
//! Owner relations (owned-prefix → owner-prefix, same namespace + owner name):
//!
//!   /replicasets.apps/<ns>/<name>  owner=<dep>  →  /deployments.apps/<ns>/<dep>
//!   /pods/<ns>/<name>              owner=<rs>   →  /replicasets.apps/<ns>/<rs>
//!
//! An object with no `owner=` field is never collected (top-level objects like
//! Deployments are user-owned). Deletes are unconditional and idempotent, so a
//! quiet cluster spends no revisions and the `/pods/` self-write never loops.

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

const DEPLOYMENTS_PREFIX: &[u8] = b"/deployments.apps/";
const REPLICASETS_PREFIX: &[u8] = b"/replicasets.apps/";
const PODS_PREFIX: &[u8] = b"/pods/";
const EVENTS_PREFIX: &[u8] = b"/events/";
/// Ceiling on retained Events (a count cap stands in for an age-based TTL).
const MAX_EVENTS: usize = 200;

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// Count of orphans collected — the observable of progress.
    collected: u32,
}

// ---- helpers ----

fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            // Accept both the reconciler ';' convention and the API plane's ','
            // (compact field values never contain either), so API-created objects
            // flow straight in.
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

fn namespace_seg<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let rest = key.get(prefix.len()..)?;
    let slash = rest.iter().position(|&b| b == b'/')?;
    Some(&rest[..slash + 1])
}

/// Sweep one owner relation: for each object under `owned_prefix` carrying an
/// `owner=`, delete it if the owner key (`owner_prefix<ns><owner>`) is absent.
/// Returns the number collected.
unsafe fn sweep(sys: &SyscallTable, owned_prefix: &[u8], owner_prefix: &[u8]) -> u32 {
    let mut walk = ListWalk::new(owned_prefix);
    let mut collected = 0u32;
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
        // No owner → never collected (user-owned top-level object).
        let Some(owner) = j_get4(
            &valbuf[..vlen],
            b"metadata",
            b"ownerReferences",
            b"0",
            b"name",
        ) else {
            continue;
        };
        let Some(ns) = namespace_seg(&keybuf[..klen], owned_prefix) else {
            continue;
        };

        // owner key = owner_prefix + <ns> + <owner>
        let mut ok = [0u8; MAX_KEY];
        let okl = owner_prefix.len() + ns.len() + owner.len();
        if okl > ok.len() {
            continue;
        }
        ok[..owner_prefix.len()].copy_from_slice(owner_prefix);
        ok[owner_prefix.len()..owner_prefix.len() + ns.len()].copy_from_slice(ns);
        ok[owner_prefix.len() + ns.len()..okl].copy_from_slice(owner);

        if !exists(sys, &ok[..okl]) && delete_value(sys, &keybuf[..klen]) {
            collected += 1;
        }
    }
    collected
}

/// Bound the Event backlog. Events carry no timestamps, so instead of the
/// age/TTL GC k8s does, cap the total Event count: when `/events/` exceeds
/// `MAX_EVENTS`, delete the excess in LIST (key) order. Keeps the store from
/// growing without limit under churn (events are never owner-swept — nothing
/// owns them — so this is their only reaper).
unsafe fn gc_events(sys: &SyscallTable) -> u32 {
    let count = list_count(sys, EVENTS_PREFIX);
    if count <= MAX_EVENTS {
        return 0;
    }
    let mut to_delete = count - MAX_EVENTS;
    let mut deleted = 0u32;
    let mut walk = ListWalk::new(EVENTS_PREFIX);
    while let Some(key) = walk.next(sys) {
        if to_delete == 0 {
            break;
        }
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        if delete_value(sys, &keybuf[..klen]) {
            deleted += 1;
            to_delete -= 1;
        }
    }
    deleted
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    // ReplicaSets orphaned by a deleted Deployment, then Pods orphaned by a
    // deleted ReplicaSet — in that order so one pass can carry a step of the
    // cascade (the rest follows on the next drain).
    let mut collected = sweep(sys, REPLICASETS_PREFIX, DEPLOYMENTS_PREFIX);
    collected += sweep(sys, PODS_PREFIX, REPLICASETS_PREFIX);
    collected += gc_events(sys);
    collected
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
        s.collected = 0;
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

        // Cold start: resolve the change-sink channel (self-edge allocated), then
        // SUBSCRIBE all three watched prefixes onto it (live-only — the cold-start
        // reconcile below captures pre-existing state via LIST), then reconcile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, DEPLOYMENTS_PREFIX, s.sink, 0);
                store_subscribe(sys, REPLICASETS_PREFIX, s.sink, 0);
                store_subscribe(sys, PODS_PREFIX, s.sink, 0);
                store_subscribe(sys, EVENTS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.collected = s.collected.wrapping_add(reconcile_all(sys));
            return 0;
        }
        // A pushed namespace.change (any prefix) means the input set moved.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.collected = s.collected.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
