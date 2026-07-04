//! DaemonSet reconciler — one Pod per ready Node, as a PIC module. It drives
//! the control-plane store (`storage.object`/`storage.namespace`): SUBSCRIBE
//! `/daemonsets.apps/`, `/nodes/` and `/pods/`, and for each DaemonSet ensure
//! exactly one Pod per ready Node. A distinct controller shape from ReplicaSet
//! — per-Node coverage, not a replica count.
//!
//! Data model — k8s objects are real nested JSON (read via the shared `json.rs`
//! `j_*` helpers); the /nodes/ readiness record is an internal flat projection:
//!
//!   /daemonsets.apps/<ns>/<name> = {…spec.template…}
//!   /nodes/<node>                = "ready=<0|1>"   (internal projection)
//!   /pods/<ns>/<ds>-<node>       = {…spec (template) embedded, spec.nodeName, metadata.ownerReferences…}
//!
//! DaemonSet Pods are named `<ds>-<node>` and pre-bound with `node=<node>` (they
//! bypass the scheduler — the placement is intrinsic). Creation is only-if-
//! absent (kubelet-added fields survive); a Pod whose Node is no longer ready is
//! pruned. Idempotent, so the `/pods/` self-write never loops.

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

const DAEMONSETS_PREFIX: &[u8] = b"/daemonsets.apps/";
const NODES_PREFIX: &[u8] = b"/nodes/";
const PODS_PREFIX: &[u8] = b"/pods/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const MAX_NODES: usize = 16;
const NODE_NAME: usize = 48;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    reconciles: u32,
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

/// A bounded set of ready node names.
struct NodeSet {
    names: [[u8; NODE_NAME]; MAX_NODES],
    lens: [usize; MAX_NODES],
    n: usize,
}

impl NodeSet {
    fn empty() -> Self {
        NodeSet {
            names: [[0u8; NODE_NAME]; MAX_NODES],
            lens: [0usize; MAX_NODES],
            n: 0,
        }
    }
    fn push(&mut self, name: &[u8]) {
        if self.n >= MAX_NODES || name.len() > NODE_NAME {
            return;
        }
        self.names[self.n][..name.len()].copy_from_slice(name);
        self.lens[self.n] = name.len();
        self.n += 1;
    }
    fn contains(&self, name: &[u8]) -> bool {
        (0..self.n).any(|i| &self.names[i][..self.lens[i]] == name)
    }
}

unsafe fn ready_nodes(sys: &SyscallTable) -> NodeSet {
    let mut set = NodeSet::empty();
    let mut walk = ListWalk::new(NODES_PREFIX);
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
        if j_get2(&valbuf[..vlen], b"status", b"ready") == Some(b"true") {
            set.push(last_seg(&keybuf[..klen]));
        }
    }
    set
}

/// Ensure one Pod per ready Node for one DaemonSet; prune Pods on dead Nodes.
unsafe fn reconcile_daemonset(
    sys: &SyscallTable,
    ds_key: &[u8],
    ds_val: &[u8],
    nodes: &NodeSet,
) -> u32 {
    let Some(ns) = namespace_seg(ds_key, DAEMONSETS_PREFIX) else {
        return 0;
    };
    let ds = last_seg(ds_key);
    let tmpl = j_sub3(ds_val, b"spec", b"template", b"spec").unwrap_or(b"{}");
    let ns_name = &ns[..ns.len().saturating_sub(1)];
    let mut writes = 0u32;

    // Create one Pod per ready Node: /pods/<ns>/<ds>-<node>.
    for i in 0..nodes.n {
        let node = &nodes.names[i][..nodes.lens[i]];
        let mut pk = [0u8; MAX_KEY];
        let pkl = PODS_PREFIX.len() + ns.len() + ds.len() + 1 + node.len();
        if pkl > pk.len() {
            continue;
        }
        let mut q = 0;
        pk[q..q + PODS_PREFIX.len()].copy_from_slice(PODS_PREFIX);
        q += PODS_PREFIX.len();
        pk[q..q + ns.len()].copy_from_slice(ns);
        q += ns.len();
        pk[q..q + ds.len()].copy_from_slice(ds);
        q += ds.len();
        pk[q] = b'-';
        q += 1;
        pk[q..q + node.len()].copy_from_slice(node);

        if exists(sys, &pk[..pkl]) {
            continue;
        }
        // JSON Pod: the DaemonSet's pod-template spec, node-pinned, owned by it.
        let mut pod0 = [0u8; MAX_VALUE];
        let mut v = j_cp(&mut pod0, 0, b"{\"metadata\":{\"name\":\"");
        v = j_cp(&mut pod0, v, ds);
        v = j_cp(&mut pod0, v, b"-");
        v = j_cp(&mut pod0, v, node);
        v = j_cp(&mut pod0, v, b"\",\"namespace\":\"");
        v = j_cp(&mut pod0, v, ns_name);
        v = j_cp(
            &mut pod0,
            v,
            b"\",\"ownerReferences\":[{\"kind\":\"DaemonSet\",\"name\":\"",
        );
        v = j_cp(&mut pod0, v, ds);
        v = j_cp(&mut pod0, v, b"\"}]},\"spec\":");
        v = j_cp(&mut pod0, v, tmpl);
        v = j_cp(&mut pod0, v, b"}");
        let mut fld = [0u8; MAX_KEY];
        let mut fp = j_cp(&mut fld, 0, b"\"nodeName\":\"");
        fp = j_cp(&mut fld, fp, node);
        fp = j_cp(&mut fld, fp, b"\"");
        let mut pod = [0u8; MAX_VALUE];
        let pl2 = j_insert(&pod0[..v], b"spec", &fld[..fp], &mut pod);
        if pl2 > 0 && put_value(sys, &pk[..pkl], &pod[..pl2]) {
            writes += 1;
        }
    }

    // Prune: Pods owned by this DaemonSet whose node= is no longer ready.
    let mut nsprefix = [0u8; MAX_KEY];
    let npl = PODS_PREFIX.len() + ns.len();
    if npl <= nsprefix.len() {
        nsprefix[..PODS_PREFIX.len()].copy_from_slice(PODS_PREFIX);
        nsprefix[PODS_PREFIX.len()..npl].copy_from_slice(ns);
        let mut walk = ListWalk::new(&nsprefix[..npl]);
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
            // Only our DaemonSet's pods.
            if j_get4(
                &valbuf[..vlen],
                b"metadata",
                b"ownerReferences",
                b"0",
                b"name",
            ) != Some(ds)
            {
                continue;
            }
            let on_ready = j_get2(&valbuf[..vlen], b"spec", b"nodeName")
                .map(|node| nodes.contains(node))
                .unwrap_or(false);
            if !on_ready && delete_value(sys, &keybuf[..klen]) {
                writes += 1;
            }
        }
    }

    writes
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let nodes = ready_nodes(sys);
    let mut walk = ListWalk::new(DAEMONSETS_PREFIX);
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
        wrote += reconcile_daemonset(sys, &keybuf[..klen], &valbuf[..vlen], &nodes);
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
        s.reconciles = 0;
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
        // SUBSCRIBE the watched prefixes onto it (live-only — the cold-start
        // reconcile below captures pre-existing state via LIST), then reconcile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, DAEMONSETS_PREFIX, s.sink, 0);
                store_subscribe(sys, NODES_PREFIX, s.sink, 0);
                store_subscribe(sys, PODS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.reconciles = s.reconciles.wrapping_add(reconcile_all(sys));
            return 0;
        }
        // A pushed namespace.change means the input set moved → re-reconcile.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.reconciles = s.reconciles.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
