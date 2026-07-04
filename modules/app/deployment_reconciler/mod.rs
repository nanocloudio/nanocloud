//! Deployment reconciler — the head of the workload chain as a PIC module. It
//! drives the control-plane store (`storage.object`/`storage.namespace`)
//! through the full reconcile loop: SUBSCRIBE `/deployments.apps/` and, for
//! every Deployment, project the owned ReplicaSet at
//! `/replicasets.apps/<ns>/<name>` — the desired-state (Deployment →
//! ReplicaSet) half of the workload chain. The ReplicaSet → Pods step is a
//! separate reconciler.
//!
//! Data model — real nested k8s JSON (read via the shared `json.rs` `j_*` path
//! helpers; the values carry `spec.replicas`, `spec.template`, `metadata.*`):
//!
//!   /deployments.apps/<ns>/<name>  = {"metadata":{…},"spec":{"replicas":N,"template":{…}}}
//!   /replicasets.apps/<ns>/<name>  = {…RS with the template + spec.replicas + metadata.ownerReferences}
//!
//! The ReplicaSet is the Deployment's pod template + replica count, stamped with
//! an owner back-reference in `metadata.ownerReferences`. A single-revision
//! Deployment maps to a single same-named ReplicaSet; template-hash rollout
//! naming and surge are follow-ups, as is ReplicaSet GC on Deployment delete
//! (the `garbage_collector` cascade covers it).
//!
//! Loop shape (level-triggered):
//!   first step → SUBSCRIBE /deployments.apps/ (NOT the root, or our own
//!                /replicasets.apps writes would wake us forever), then a
//!                cold-start full pass (Deployments may predate the watch).
//!   each step  → DRAIN; on any change, re-project every Deployment — but only
//!                write a ReplicaSet whose doc CHANGED (a GET-compare guards the
//!                PUT, so a quiet cluster spends no revisions).

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

// The control-plane store, consumed through the standard fluxor storage
// contracts: `storage.object` (0x14) for
// keyed byte values + CAS, `storage.namespace` (0x13) for prefix LIST and
// SUBSCRIBE. Changes are pushed to us as `namespace.change` events on our input
// channel; we drain them to detect "something changed", then re-reconcile.
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
const OWNER_TAG: &[u8] = b";owner=";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 256;
const MAX_DOC: usize = 4096;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Input-port channel the store pushes `namespace.change` events onto.
    sink: i32,
    /// 0 until the prefix SUBSCRIBE + cold-start reconcile have run.
    subscribed: u8,
    /// Count of replicaset docs (re)written — the observable of progress.
    reconciles: u32,
}

// ---- storage.object / storage.namespace ops ----

/// Format `v` as 8 lowercase hex digits. (`fnv1a` comes from the SDK runtime.)
fn hex8(out: &mut [u8; 8], mut v: u32) {
    let hexd = b"0123456789abcdef";
    for i in (0..8).rev() {
        out[i] = hexd[(v & 0xf) as usize];
        v >>= 4;
    }
}

unsafe fn store_put(sys: &SyscallTable, key: &[u8], value: &[u8]) -> bool {
    // [key_len:u16][key][ct_len:u8=0][body_ptr:u64][body_len:u64]
    // [precondition:u8=ANY][etag_len:u8=0][fence_ptr:u64][fence_cap:u16]
    let mut arg = [0u8; MAX_KEY + 64];
    // `1 + 1` is the precondition PAIR — `[precondition][etag_len]`. The
    // bound counts both bytes: the buffer's slack would otherwise hide an
    // undercount until a long key made it overflow.
    if 2 + key.len() + 1 + 8 + 8 + 1 + 1 + 8 + 2 > arg.len() {
        return false;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    arg[p..p + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
    p += 2;
    arg[p..p + key.len()].copy_from_slice(key);
    p += key.len();
    arg[p] = 0; // content_type_len
    p += 1;
    arg[p..p + 8].copy_from_slice(&(value.as_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 8].copy_from_slice(&(value.len() as u64).to_le_bytes());
    p += 8;
    // storage.object writes carry a precondition PAIR — `[precondition:u8]
    // [etag_len:u8]` — two bytes, not one. Every field after it (including
    // `fence_out_ptr`) is positioned off that width; get it wrong and the
    // provider reads a misaligned pointer and the write is silently lost —
    // the module loads, reaches ready, and produces nothing. `ANY` is the
    // explicit "apply unconditionally".
    arg[p] = 0; // precondition::ANY
    p += 1;
    arg[p] = 0; // etag_len (none, under ANY)
    p += 1;
    arg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    arg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    (sys.provider_call)(-1, OBJ_PUT, arg.as_mut_ptr(), p) == 0
}

/// `storage.object` GET+RANGE_GET+CLOSE — read `key` into `dst`. Returns length.
unsafe fn store_get(sys: &SyscallTable, key: &[u8], dst: &mut [u8]) -> Option<usize> {
    let mut garg = [0u8; MAX_KEY];
    if key.len() > garg.len() {
        return None;
    }
    garg[..key.len()].copy_from_slice(key);
    let h = (sys.provider_call)(-1, OBJ_GET, garg.as_mut_ptr(), key.len());
    if h < 0 {
        return None;
    }
    // RANGE_GET: [offset:u64=0][length:u32][out_ptr:u64]
    let mut rarg = [0u8; 20];
    rarg[8..12].copy_from_slice(&(dst.len() as u32).to_le_bytes());
    rarg[12..20].copy_from_slice(&(dst.as_mut_ptr() as u64).to_le_bytes());
    let n = (sys.provider_call)(h, OBJ_RANGE_GET, rarg.as_mut_ptr(), 20);
    let mut carg = [0u8; 4];
    (sys.provider_call)(h, OBJ_CLOSE, carg.as_mut_ptr(), 0);
    if n < 0 {
        None
    } else {
        Some(n as usize)
    }
}

/// `storage.namespace` LIST — enumerate a prefix into `out`. Returns bytes
/// written; entries are `[name_len:u8][kind:u8][name]`, terminated by 0xFF.
unsafe fn store_list(sys: &SyscallTable, prefix: &[u8], out: &mut [u8]) -> usize {
    // [prefix_len:u16][prefix][cursor_len:u16=0][out_ptr:u64][out_cap:u32]
    // [fence_ptr:u64][fence_cap:u16]
    let mut larg = [0u8; MAX_KEY + 32];
    if 2 + prefix.len() + 2 + 8 + 4 + 8 + 2 > larg.len() {
        return 0;
    }
    let mut fence = [0u8; 62];
    let mut p = 0;
    larg[p..p + 2].copy_from_slice(&(prefix.len() as u16).to_le_bytes());
    p += 2;
    larg[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    larg[p..p + 2].copy_from_slice(&0u16.to_le_bytes()); // cursor_len
    p += 2;
    larg[p..p + 8].copy_from_slice(&(out.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    larg[p..p + 4].copy_from_slice(&(out.len() as u32).to_le_bytes());
    p += 4;
    larg[p..p + 8].copy_from_slice(&(fence.as_mut_ptr() as u64).to_le_bytes());
    p += 8;
    larg[p..p + 2].copy_from_slice(&62u16.to_le_bytes());
    p += 2;
    let n = (sys.provider_call)(-1, NS_LIST, larg.as_mut_ptr(), p);
    if n < 0 {
        0
    } else {
        n as usize
    }
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

/// The `<ns>/` segment (with trailing slash) after `prefix` in `key`.
fn namespace_seg<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let rest = key.get(prefix.len()..)?;
    let slash = rest.iter().position(|&b| b == b'/')?;
    Some(&rest[..slash + 1])
}

/// Project one Deployment onto its owned ReplicaSet, writing only on change.
/// Returns true if a replicaset doc was (re)written.
unsafe fn reconcile_deployment(sys: &SyscallTable, dep_key: &[u8], dep_val: &[u8]) -> bool {
    // A Deployment must declare spec.replicas to project a ReplicaSet.
    if j_get2(dep_val, b"spec", b"replicas").is_none() {
        return false;
    }
    let Some(spec) = j_sub1(dep_val, b"spec") else {
        return false;
    };
    let Some(ns) = namespace_seg(dep_key, DEPLOYMENTS_PREFIX) else {
        return false;
    };
    let name = last_seg(dep_key);

    // Rollout: the pod-template-hash is a digest of `spec.template` — it changes
    // exactly when the template (image, etc.) changes, never on a replicas-only
    // edit. It is stamped on the ReplicaSet (and, by replicaset_reconciler, on
    // each Pod), so a template change makes running Pods stale → rolled.
    let tmpl = j_sub2(dep_val, b"spec", b"template").unwrap_or(spec);
    let mut hash = [0u8; 8];
    hex8(&mut hash, fnv1a(tmpl));

    // ReplicaSet JSON = the Deployment's spec, owned by the Deployment, labelled
    // with the pod-template-hash.
    let mut doc = [0u8; MAX_DOC];
    let mut d = append(&mut doc, 0, b"{\"metadata\":{\"name\":\"");
    d = append(&mut doc, d, name);
    d = append(&mut doc, d, b"\",\"namespace\":\"");
    d = append(&mut doc, d, &ns[..ns.len().saturating_sub(1)]); // strip trailing '/'
    d = append(
        &mut doc,
        d,
        b"\",\"ownerReferences\":[{\"kind\":\"Deployment\",\"name\":\"",
    );
    d = append(&mut doc, d, name);
    d = append(&mut doc, d, b"\"}],\"labels\":{\"pod-template-hash\":\"");
    d = append(&mut doc, d, &hash);
    d = append(&mut doc, d, b"\"}},\"spec\":");
    d = append(&mut doc, d, spec);
    d = append(&mut doc, d, b"}");

    // /replicasets.apps/<ns>/<name>  (same ns + name as the Deployment).
    let mut rs_key = [0u8; MAX_KEY];
    let rkl = REPLICASETS_PREFIX.len() + ns.len() + name.len();
    if rkl > rs_key.len() {
        return false;
    }
    rs_key[..REPLICASETS_PREFIX.len()].copy_from_slice(REPLICASETS_PREFIX);
    rs_key[REPLICASETS_PREFIX.len()..REPLICASETS_PREFIX.len() + ns.len()].copy_from_slice(ns);
    rs_key[REPLICASETS_PREFIX.len() + ns.len()..rkl].copy_from_slice(name);

    // Only spend a revision when the projected doc changed.
    let mut current = [0u8; MAX_DOC];
    let cur_len = store_get(sys, &rs_key[..rkl], &mut current);
    if let Some(clen) = cur_len {
        if clen == d && current[..clen] == doc[..d] {
            return false;
        }
        // Rollout history: on a template change, archive the previous RS spec so
        // `rollout undo` can restore it (one level — the last rollout).
        let old_hash = j_get3(
            &current[..clen],
            b"metadata",
            b"labels",
            b"pod-template-hash",
        );
        if old_hash.is_some() && old_hash != Some(&hash[..]) {
            if let Some(prev_spec) = j_sub1(&current[..clen], b"spec") {
                // /controllerrevisions.apps/<ns>/<name>/previous
                let ns_name = &ns[..ns.len().saturating_sub(1)];
                let mut pk = [0u8; MAX_KEY];
                let mut pp = append(&mut pk, 0, b"/controllerrevisions.apps/");
                pp = append(&mut pk, pp, ns_name);
                pp = append(&mut pk, pp, b"/");
                pp = append(&mut pk, pp, name);
                pp = append(&mut pk, pp, b"/previous");
                store_put(sys, &pk[..pp], prev_spec);
            }
        }
    }
    let changed = store_put(sys, &rs_key[..rkl], &doc[..d]);
    if changed {
        // Record a ScalingReplicaSet event on the Deployment.
        let ns_name = &ns[..ns.len().saturating_sub(1)];
        let mut msg = [0u8; 96];
        let mut ml = ev_cp(&mut msg, 0, b"Scaled up replica set ");
        ml = ev_cp(&mut msg, ml, name);
        emit_event(
            sys,
            ns_name,
            b"Deployment",
            name,
            b"ScalingReplicaSet",
            &msg[..ml],
            0,
        );
    }
    changed
}

/// Re-project every Deployment (level-triggered full pass). Returns the count
/// of replicaset docs (re)written.
unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut lout = [0u8; LIST_BUF];
    let n = store_list(sys, DEPLOYMENTS_PREFIX, &mut lout);
    let mut wrote = 0u32;
    // Entries: [name_len:u8][kind:u8][name] ... terminated by [0xFF].
    let mut p = 0;
    while p < n {
        let name_len = lout[p] as usize;
        if name_len == 0xFF {
            break; // cursor terminator
        }
        if p + 2 + name_len > n || name_len > MAX_KEY {
            break;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..name_len].copy_from_slice(&lout[p + 2..p + 2 + name_len]);
        p += 2 + name_len;

        let mut valbuf = [0u8; MAX_VALUE];
        let Some(vlen) = store_get(sys, &keybuf[..name_len], &mut valbuf) else {
            continue;
        };
        if reconcile_deployment(sys, &keybuf[..name_len], &valbuf[..vlen]) {
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
        // The `changes` input port is the store's event sink.
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

        // Lazy setup on the first step: resolve the event-sink channel (prefer
        // the wired input port; else the module's input-port lookup) and, if we
        // have one, SUBSCRIBE /deployments.apps/ onto it for live pushes. The
        // cold-start reconcile is via LIST and runs UNCONDITIONALLY — a level-
        // triggered controller must project pre-existing state whether or not a
        // change-notification channel exists.
        if s.subscribed == 0 {
            // Resolve the change-sink channel. A module cannot open a channel;
            // the graph gives us one by self-wiring our output port back to the
            // `changes` input (a 1-node cycle), so `dev_channel_port(INPUT, 0)`
            // yields a module-owned channel the store can push onto.
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                // Live-only SUBSCRIBE — the cold-start LIST pass below captures
                // pre-existing state, so we don't also replay it as events.
                store_subscribe(sys, DEPLOYMENTS_PREFIX, s.sink, 0);
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
