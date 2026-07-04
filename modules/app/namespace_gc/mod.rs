//! Namespace GC — the namespace controller's teardown as a PIC module. When a
//! namespace is marked `phase=Terminating`, it deletes every object under that
//! namespace across all namespaced resource prefixes, then removes the
//! namespace record itself (the finalizer). It drives the control-plane store
//! (`storage.object`/`storage.namespace`): SUBSCRIBE `/namespaces/`, sweep,
//! finalize.
//!
//! Data model (compact):
//!
//!   /namespaces/<name>        = "phase=Active|Terminating"
//!   /<resource>/<ns>/<obj>    = …   (every namespaced resource)
//!
//! Sweep + finalize happen in one reconcile pass: deletes are synchronous, so
//! after clearing every prefix the namespace is empty and the record is removed
//! in the same pass. Objects in other namespaces are untouched (each delete is
//! scoped to `/<resource>/<ns>/`). Cluster-scoped objects are never swept.

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

const NAMESPACES_PREFIX: &[u8] = b"/namespaces/";

/// The namespaced resource prefixes swept on termination, by index. (A
/// discovery-driven list is the k8s way; this fixed set covers nanocloud's
/// namespaced kinds.)
///
/// Returned via a `match` rather than a `&[&[u8]]` const on purpose: PIC modules
/// load as flat binaries with no relocation applied, so a static ARRAY of fat
/// pointers (slice-of-slices) would hold un-relocated garbage pointers. Each
/// `b"…"` literal here is materialised PC-relative at its use site, which is
/// relocation-free — the same reason single `&[u8]` consts work.
fn namespaced_prefix(i: usize) -> Option<&'static [u8]> {
    Some(match i {
        0 => b"/pods/",
        1 => b"/services/",
        2 => b"/endpoints/",
        3 => b"/configmaps/",
        4 => b"/secrets/",
        5 => b"/serviceaccounts/",
        6 => b"/pvcs/",
        7 => b"/deployments.apps/",
        8 => b"/replicasets.apps/",
        9 => b"/daemonsets.apps/",
        10 => b"/statefulsets.apps/",
        11 => b"/jobs.batch/",
        12 => b"/hpa/",
        13 => b"/networkpolicies/",
        _ => return None,
    })
}

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
// Kept small: reconcile_all → reconcile_namespace → sweep_scope each hold a
// LIST page on the stack, and a PIC module's stack is bounded — a large page
// nested three deep overflows it. One page still covers a namespace's objects
// per resource kind; multi-page namespaces sweep across passes.
const LIST_BUF: usize = 1024;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    deletes: u32,
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

/// Build `<prefix><ns>/` (the namespace scope under a resource prefix) into buf.
/// Returns length, or 0 on overflow.
fn scope(buf: &mut [u8], prefix: &[u8], ns: &[u8]) -> usize {
    let total = prefix.len() + ns.len() + 1;
    if total > buf.len() {
        return 0;
    }
    buf[..prefix.len()].copy_from_slice(prefix);
    buf[prefix.len()..prefix.len() + ns.len()].copy_from_slice(ns);
    buf[prefix.len() + ns.len()] = b'/';
    total
}

/// Delete every object under one `<prefix><ns>/` scope. Returns the count.
unsafe fn sweep_scope(sys: &SyscallTable, scope_prefix: &[u8]) -> u32 {
    let mut walk = ListWalk::new(scope_prefix);
    let mut deleted = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        if delete_value(sys, &keybuf[..klen]) {
            deleted += 1;
        }
    }
    deleted
}

/// Terminate one namespace: sweep every namespaced prefix, then finalize (remove
/// the namespace record) once nothing remains. Sweep and finalize run in the
/// SAME pass — this module watches only `/namespaces/`, so a later object delete
/// would not wake it; the whole teardown must complete here. Returns deletes.
unsafe fn reconcile_namespace(sys: &SyscallTable, ns_key: &[u8], ns_val: &[u8]) -> u32 {
    if j_get2(ns_val, b"status", b"phase") != Some(b"Terminating") {
        return 0;
    }
    let name = last_seg(ns_key);
    let mut deletes = 0u32;

    // Sweep every namespaced prefix scoped to this namespace.
    let mut i = 0;
    while let Some(prefix) = namespaced_prefix(i) {
        let mut sb = [0u8; MAX_KEY];
        let sl = scope(&mut sb, prefix, name);
        if sl != 0 {
            deletes += sweep_scope(sys, &sb[..sl]);
        }
        i += 1;
    }

    // Re-check emptiness (a single LIST page bounds each sweep); finalize by
    // removing the namespace record only once truly empty.
    let mut remaining = 0usize;
    let mut j = 0;
    while let Some(prefix) = namespaced_prefix(j) {
        let mut sb = [0u8; MAX_KEY];
        let sl = scope(&mut sb, prefix, name);
        if sl != 0 {
            remaining += list_count(sys, &sb[..sl]);
        }
        j += 1;
    }
    if remaining == 0 && delete_value(sys, ns_key) {
        deletes += 1;
    }
    deletes
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(NAMESPACES_PREFIX);
    let mut total = 0u32;
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
        total += reconcile_namespace(sys, &keybuf[..klen], &valbuf[..vlen]);
    }
    total
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
        s.deletes = 0;
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
        // SUBSCRIBE the watched prefix onto it (live-only — the cold-start
        // reconcile below captures pre-existing state via LIST), then reconcile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, NAMESPACES_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.deletes = s.deletes.wrapping_add(reconcile_all(sys));
            return 0;
        }
        // A pushed namespace.change means the input set moved → re-reconcile.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.deletes = s.deletes.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
