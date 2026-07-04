//! ReplicaSet reconciler — the second link of the workload chain as a PIC
//! module. It drives the control-plane store
//! (`storage.object`/`storage.namespace`) through the reconcile loop: SUBSCRIBE
//! `/replicasets.apps/` and `/pods/`, and for each ReplicaSet ensure exactly
//! `replicas` Pods exist — creating the missing, deleting the surplus. Together
//! with `deployment_reconciler` this completes Deployment → ReplicaSet → Pods,
//! all as cooperating modules over the one shared store; `pod_lifecycle` then
//! turns the Pods into sandboxes.
//!
//! Data model — real nested k8s JSON (read via the shared `json.rs` `j_*` path
//! helpers):
//!
//!   /replicasets.apps/<ns>/<name> = {…spec.replicas, spec.template, metadata.ownerReferences…}
//!   /pods/<ns>/<name>-<hash>-<i>  = {…spec (template) embedded, metadata.ownerReferences…}
//!
//! Pods are named `<rs>-<ordinal>` (deterministic + idempotent; real ReplicaSets
//! use a random suffix, but the reconcile essence is the replica *count*).
//! Creation is only-if-absent, so fields a kubelet/scheduler later adds to a Pod
//! (`ip=`, `r=`) survive re-reconciles; a template change makes a new ReplicaSet
//! rather than mutating live Pods, exactly as k8s does. Scale-down deletes the
//! Pods at ordinals ≥ N.
//!
//! Loop shape (level-triggered): SUBSCRIBE both prefixes (watching `/pods/` too
//! lets a deleted Pod be recreated — the ReplicaSet's self-heal); DRAIN both;
//! reconcile on any change. Writes are guarded (create-if-absent, delete-if-
//! present), so once converged a quiet cluster spends no revisions and there is
//! no self-wake loop despite writing under a watched prefix.

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

const REPLICASETS_PREFIX: &[u8] = b"/replicasets.apps/";
const PODS_PREFIX: &[u8] = b"/pods/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
/// Bound on `replicas` and the scale-down scan — keeps work/buffers finite.
const MAX_REPLICAS: u32 = 128;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// Count of pod creates + deletes — the observable of progress.
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

fn parse_u32(b: &[u8]) -> u32 {
    let mut n: u32 = 0;
    for &c in b {
        if c.is_ascii_digit() {
            n = n.wrapping_mul(10).wrapping_add((c - b'0') as u32);
        } else {
            break;
        }
    }
    n
}

fn write_u32(dst: &mut [u8], at: usize, mut n: u32) -> usize {
    if at >= dst.len() {
        return at;
    }
    if n == 0 {
        dst[at] = b'0';
        return at + 1;
    }
    let mut tmp = [0u8; 10];
    let mut i = 0;
    while n > 0 && i < tmp.len() {
        tmp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }
    let mut p = at;
    while i > 0 && p < dst.len() {
        i -= 1;
        dst[p] = tmp[i];
        p += 1;
    }
    p
}

/// Build `/pods/<ns><rs>-<i>` into `buf`; returns its length (0 on overflow).
/// `ns` includes its trailing slash (e.g. `default/`).
fn pod_key(buf: &mut [u8], ns: &[u8], rs: &[u8], i: u32) -> usize {
    let head = PODS_PREFIX.len() + ns.len() + rs.len() + 1; // +1 for '-'
    if head + 10 > buf.len() {
        return 0;
    }
    let mut p = 0;
    buf[p..p + PODS_PREFIX.len()].copy_from_slice(PODS_PREFIX);
    p += PODS_PREFIX.len();
    buf[p..p + ns.len()].copy_from_slice(ns);
    p += ns.len();
    buf[p..p + rs.len()].copy_from_slice(rs);
    p += rs.len();
    buf[p] = b'-';
    p += 1;
    write_u32(buf, p, i)
}

/// Reconcile one ReplicaSet: create Pods `<rs>-0..<rs>-(N-1)` that are absent,
/// delete Pods at ordinals ≥ N. Returns the number of create+delete writes.
unsafe fn reconcile_replicaset(sys: &SyscallTable, rs_key: &[u8], rs_val: &[u8]) -> u32 {
    if j_get2(rs_val, b"spec", b"replicas").is_none() {
        return 0;
    }
    let n = j_u32_2(rs_val, b"spec", b"replicas").min(MAX_REPLICAS);
    let Some(ns) = namespace_seg(rs_key, REPLICASETS_PREFIX) else {
        return 0;
    };
    let rs = last_seg(rs_key);
    // The Pod spec is the ReplicaSet's pod-template spec.
    let tmpl_spec = j_sub3(rs_val, b"spec", b"template", b"spec").unwrap_or(b"{}");
    let ns_name = &ns[..ns.len().saturating_sub(1)]; // strip trailing '/'

    let mut writes = 0u32;

    // Rollout: when the ReplicaSet carries a pod-template-hash (stamped by a
    // Deployment), a Pod whose own stamp differs is stale — deleted so it is
    // recreated from the new template. At most `stale_budget` are rolled per
    // pass (maxUnavailable = 1) for a gradual rolling update; a bare ReplicaSet
    // (no hash) keeps the plain create-if-absent behaviour.
    let rs_hash = j_get3(rs_val, b"metadata", b"labels", b"pod-template-hash");
    let mut stale_budget: u32 = 1;

    // Create missing Pods 0..N as JSON, owned by the ReplicaSet. Written only if
    // absent, so a kubelet's later status additions are preserved.
    for i in 0..n {
        let mut pk = [0u8; MAX_KEY];
        let pkl = pod_key(&mut pk, ns, rs, i);
        if pkl == 0 {
            break;
        }
        let mut cur = [0u8; MAX_VALUE];
        if let Some(clen) = get_value(sys, &pk[..pkl], &mut cur) {
            // Roll a stale pod (bounded per pass); it recreates next pass.
            if let Some(want) = rs_hash {
                let have = j_get3(&cur[..clen], b"metadata", b"labels", b"pod-template-hash");
                if have != Some(want) && stale_budget > 0 && delete_value(sys, &pk[..pkl]) {
                    stale_budget -= 1;
                    writes += 1;
                    let mut msg = [0u8; 96];
                    let mut ml = ev_cp(&mut msg, 0, b"Rolled pod ");
                    ml = ev_cp(&mut msg, ml, rs);
                    ml = ev_cp(&mut msg, ml, b"-");
                    ml = ev_u32(&mut msg, ml, i);
                    emit_event(
                        sys,
                        ns_name,
                        b"ReplicaSet",
                        rs,
                        b"SuccessfulDelete",
                        &msg[..ml],
                        i,
                    );
                }
            }
            continue;
        }
        let mut podval = [0u8; MAX_VALUE];
        let mut v = append(&mut podval, 0, b"{\"metadata\":{\"name\":\"");
        v = append(&mut podval, v, rs);
        v = append(&mut podval, v, b"-");
        v = write_u32(&mut podval, v, i);
        v = append(&mut podval, v, b"\",\"namespace\":\"");
        v = append(&mut podval, v, ns_name);
        v = append(
            &mut podval,
            v,
            b"\",\"ownerReferences\":[{\"kind\":\"ReplicaSet\",\"name\":\"",
        );
        v = append(&mut podval, v, rs);
        v = append(&mut podval, v, b"\"}]");
        if let Some(want) = rs_hash {
            v = append(&mut podval, v, b",\"labels\":{\"pod-template-hash\":\"");
            v = append(&mut podval, v, want);
            v = append(&mut podval, v, b"\"}");
        }
        v = append(&mut podval, v, b"},\"spec\":");
        v = append(&mut podval, v, tmpl_spec);
        v = append(&mut podval, v, b"}");
        if put_value(sys, &pk[..pkl], &podval[..v]) {
            writes += 1;
            // Record a SuccessfulCreate event on the ReplicaSet.
            let mut msg = [0u8; 96];
            let mut ml = ev_cp(&mut msg, 0, b"Created pod ");
            ml = ev_cp(&mut msg, ml, rs);
            ml = ev_cp(&mut msg, ml, b"-");
            ml = ev_u32(&mut msg, ml, i);
            emit_event(
                sys,
                ns_name,
                b"ReplicaSet",
                rs,
                b"SuccessfulCreate",
                &msg[..ml],
                i,
            );
        }
    }

    // Scale-down: delete Pods at ordinals >= N. The span is scanned WHOLE rather
    // than stopping at the first absent ordinal. Ordinals are contiguous only
    // while nothing else touches the Pods: one deleted out of band (kubectl, a
    // node loss, the GC) leaves a HOLE, and stopping there hides every higher
    // ordinal. Because the next pass stops at the same hole, those Pods are
    // orphaned permanently — a leak, not a delay. The `exists` probe keeps this
    // delete-if-present, so a converged cluster still spends no revisions; the
    // cost is one probe per surplus ordinal rather than one per live Pod.
    let mut i = n;
    while i < MAX_REPLICAS {
        let mut pk = [0u8; MAX_KEY];
        let pkl = pod_key(&mut pk, ns, rs, i);
        if pkl == 0 {
            break; // key overflow is monotonic in the ordinal — nothing higher fits
        }
        if exists(sys, &pk[..pkl]) && delete_value(sys, &pk[..pkl]) {
            writes += 1;
        }
        i += 1;
    }

    writes
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(REPLICASETS_PREFIX);
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
        wrote += reconcile_replicaset(sys, &keybuf[..klen], &valbuf[..vlen]);
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
        // SUBSCRIBE both watched prefixes onto it (live-only — the cold-start
        // reconcile below captures pre-existing state via LIST), then reconcile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, REPLICASETS_PREFIX, s.sink, 0);
                store_subscribe(sys, PODS_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            s.reconciles = s.reconciles.wrapping_add(reconcile_all(sys));
            return 0;
        }
        // A pushed namespace.change (either prefix) means the input set moved.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.reconciles = s.reconciles.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
