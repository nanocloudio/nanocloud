//! Scheduler — the pod placement decision as a PIC module. It drives the
//! control-plane store (`storage.object`/`storage.namespace`) through the
//! classic kube-scheduler loop: SUBSCRIBE `/pods/` and `/nodes/`, and for every
//! unbound Pod (one with no `node=` field) pick the least-loaded ready Node and
//! bind it by writing `node=<name>` onto the Pod. Binding is the DECISION;
//! `pod_lifecycle` turns a bound Pod into a sandbox.
//!
//! Data model (compact):
//!
//!   /nodes/<name>         = "ready=<0|1>[;…]"
//!   /pods/<ns>/<name>     = "image=<img>;owner=<rs>[;node=<node>]"
//!
//! Policy: least-loaded spread. Count each ready Node's current Pod assignments,
//! then bind each unbound Pod to the Node with the fewest (ties broken by the
//! Node's sort order, since `LIST` is byte-ordered), incrementing as we go so a
//! batch of new Pods fans out rather than piling onto one Node. Idempotent — a
//! Pod that already carries `node=` is skipped, so the `/pods/` self-write never
//! loops.

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
include!("../_shared/event.rs");

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

const PODS_PREFIX: &[u8] = b"/pods/";
const NODES_PREFIX: &[u8] = b"/nodes/";
const NODE_TAG: &[u8] = b";node=";

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
    /// Count of bindings written — the observable of progress.
    bindings: u32,
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

/// A bounded table of ready nodes and their current pod load.
struct NodeTable {
    names: [[u8; NODE_NAME]; MAX_NODES],
    lens: [usize; MAX_NODES],
    counts: [u32; MAX_NODES],
    n: usize,
}

impl NodeTable {
    fn empty() -> Self {
        NodeTable {
            names: [[0u8; NODE_NAME]; MAX_NODES],
            lens: [0usize; MAX_NODES],
            counts: [0u32; MAX_NODES],
            n: 0,
        }
    }

    fn push(&mut self, name: &[u8]) {
        if self.n >= MAX_NODES || name.len() > NODE_NAME {
            return;
        }
        self.names[self.n][..name.len()].copy_from_slice(name);
        self.lens[self.n] = name.len();
        self.counts[self.n] = 0;
        self.n += 1;
    }

    fn index_of(&self, name: &[u8]) -> Option<usize> {
        (0..self.n).find(|&i| self.names[i][..self.lens[i]] == *name)
    }

    /// Index of the least-loaded node (ties → lowest index = sort order).
    fn least_loaded(&self) -> Option<usize> {
        if self.n == 0 {
            return None;
        }
        let mut best = 0usize;
        for i in 1..self.n {
            if self.counts[i] < self.counts[best] {
                best = i;
            }
        }
        Some(best)
    }
}

/// Load the ready nodes into a table.
unsafe fn ready_nodes(sys: &SyscallTable) -> NodeTable {
    let mut table = NodeTable::empty();
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
        let ready = j_get2(&valbuf[..vlen], b"status", b"ready") == Some(b"true");
        if ready {
            table.push(last_seg(&keybuf[..klen]));
        }
    }
    table
}

/// Bind each unbound pod to the least-loaded ready node. Returns bindings made.
unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut table = ready_nodes(sys);
    if table.n == 0 {
        return 0; // no capacity — pods stay Pending
    }

    let mut walk = ListWalk::new(PODS_PREFIX);

    // Pass 1: tally existing bindings so new pods spread on top of current load.
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
        if let Some(node) = j_get2(&valbuf[..vlen], b"spec", b"nodeName") {
            if let Some(idx) = table.index_of(node) {
                table.counts[idx] = table.counts[idx].saturating_add(1);
            }
        }
    }

    // Pass 2: bind unbound pods to the least-loaded node, updating load as we go.
    // A second walk, not a second read of a buffer: the listing is re-fetched.
    let mut bound = 0u32;
    let mut walk = ListWalk::new(PODS_PREFIX);
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
        if j_get2(&valbuf[..vlen], b"spec", b"nodeName").is_some() {
            continue; // already bound
        }
        let Some(idx) = table.least_loaded() else {
            break;
        };
        let node = {
            let nl = table.lens[idx];
            // borrow copy so we can mutate counts below
            let mut nb = [0u8; NODE_NAME];
            nb[..nl].copy_from_slice(&table.names[idx][..nl]);
            (nb, nl)
        };

        // Bind: set spec.nodeName on the pod JSON.
        let mut fld = [0u8; NODE_NAME + 16];
        let mut fp = j_cp(&mut fld, 0, b"\"nodeName\":\"");
        fp = j_cp(&mut fld, fp, &node.0[..node.1]);
        fp = j_cp(&mut fld, fp, b"\"");
        let mut nv = [0u8; MAX_VALUE];
        let total = j_insert(&valbuf[..vlen], b"spec", &fld[..fp], &mut nv);
        if total == 0 {
            continue;
        }
        if put_value(sys, &keybuf[..klen], &nv[..total]) {
            table.counts[idx] = table.counts[idx].saturating_add(1);
            bound += 1;
            // Record a Scheduled event (once per pod — it's skipped once bound).
            let (ns, name) = ev_ns_name(&keybuf[..klen], PODS_PREFIX);
            emit_event(
                sys,
                ns,
                b"Pod",
                name,
                b"Scheduled",
                b"Successfully assigned to a ready node",
                0,
            );
        }
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
        // The `changes` input port is the store's event sink (self-edge alloc).
        s.sink = in_chan;
        s.subscribed = 0;
        s.bindings = 0;
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
                store_subscribe(sys, PODS_PREFIX, s.sink, 0);
                store_subscribe(sys, NODES_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.bindings = s.bindings.wrapping_add(reconcile_all(sys));
            return 0;
        }
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.bindings = s.bindings.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
