//! StatefulSet reconciler — ordered, stable-identity Pods as a PIC module.
//! Unlike a ReplicaSet's interchangeable replicas, a StatefulSet brings Pods up
//! ONE AT A TIME in ordinal order and tears them down highest-first. It drives
//! the control-plane store (`storage.object`/`storage.namespace`): SUBSCRIBE
//! `/statefulsets.apps/` and `/pods/`, and make at most one change per pass.
//!
//! Data model — real nested k8s JSON (read via the shared `json.rs` `j_*`
//! helpers):
//!
//!   /statefulsets.apps/<ns>/<name> = {…spec.replicas, spec.template…}
//!   /pods/<ns>/<name>-<i>          = {…spec (template) embedded, metadata.ownerReferences, status.ready…}
//!
//! Scale-up: create `<sts>-<i>` only once `<sts>-<i-1>` exists AND is ready
//! (`ready=1`, set by the kubelet); `<sts>-0` first. Scale-down: delete the
//! highest ordinal ≥ `replicas`, one per pass (reverse order). At most one write
//! per reconcile, so ordering is strict and the `/pods/` self-write can't loop.

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

const STATEFULSETS_PREFIX: &[u8] = b"/statefulsets.apps/";
const PODS_PREFIX: &[u8] = b"/pods/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const MAX_REPLICAS: u32 = 128;

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

// ---- store ops ----

/// GET a pod's value; returns (exists, ready). Rebuilt over storage.object so the
/// ordered scale-up logic (which gates each ordinal on its predecessor's
/// `ready=1`) is untouched.
unsafe fn pod_state(sys: &SyscallTable, key: &[u8]) -> (bool, bool) {
    let mut buf = [0u8; MAX_VALUE];
    match get_value(sys, key, &mut buf) {
        Some(vlen) => {
            let ready = j_get2(&buf[..vlen], b"status", b"ready") == Some(b"true");
            (true, ready)
        }
        None => (false, false),
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

fn pod_key(buf: &mut [u8], ns: &[u8], sts: &[u8], i: u32) -> usize {
    if PODS_PREFIX.len() + ns.len() + sts.len() + 1 + 10 > buf.len() {
        return 0;
    }
    let mut p = 0;
    buf[p..p + PODS_PREFIX.len()].copy_from_slice(PODS_PREFIX);
    p += PODS_PREFIX.len();
    buf[p..p + ns.len()].copy_from_slice(ns);
    p += ns.len();
    buf[p..p + sts.len()].copy_from_slice(sts);
    p += sts.len();
    buf[p] = b'-';
    p += 1;
    write_u32(buf, p, i)
}

/// Make at most one ordered change for one StatefulSet. Returns 1 if it did.
unsafe fn reconcile_statefulset(sys: &SyscallTable, sts_key: &[u8], sts_val: &[u8]) -> u32 {
    if j_get2(sts_val, b"spec", b"replicas").is_none() {
        return 0;
    }
    let n = j_u32_2(sts_val, b"spec", b"replicas").min(MAX_REPLICAS);
    let Some(ns) = namespace_seg(sts_key, STATEFULSETS_PREFIX) else {
        return 0;
    };
    let sts = last_seg(sts_key);
    let tmpl = j_sub3(sts_val, b"spec", b"template", b"spec").unwrap_or(b"{}");
    let ns_name = &ns[..ns.len().saturating_sub(1)];

    // Scale-up, ordered: the lowest absent ordinal is created only if its
    // predecessor is ready (ordinal 0 has an implicit ready predecessor).
    let mut prev_ready = true;
    for i in 0..n {
        let mut pk = [0u8; MAX_KEY];
        let pkl = pod_key(&mut pk, ns, sts, i);
        if pkl == 0 {
            return 0;
        }
        let (present, ready) = pod_state(sys, &pk[..pkl]);
        if !present {
            if !prev_ready {
                return 0; // predecessor not ready yet — hold
            }
            let mut pv = [0u8; MAX_VALUE];
            let mut v = j_cp(&mut pv, 0, b"{\"metadata\":{\"name\":\"");
            v = j_cp(&mut pv, v, sts);
            v = j_cp(&mut pv, v, b"-");
            v = write_u32(&mut pv, v, i);
            v = j_cp(&mut pv, v, b"\",\"namespace\":\"");
            v = j_cp(&mut pv, v, ns_name);
            v = j_cp(
                &mut pv,
                v,
                b"\",\"ownerReferences\":[{\"kind\":\"StatefulSet\",\"name\":\"",
            );
            v = j_cp(&mut pv, v, sts);
            v = j_cp(&mut pv, v, b"\"}]},\"spec\":");
            v = j_cp(&mut pv, v, tmpl);
            v = j_cp(&mut pv, v, b"}");
            return if put_value(sys, &pk[..pkl], &pv[..v]) {
                1
            } else {
                0
            };
        }
        prev_ready = ready;
    }

    // Scale-down, reverse: delete the highest existing ordinal >= N, one per pass.
    // The span is scanned WHOLE rather than stopping at the first absent
    // ordinal. Ordinals are contiguous only while nothing else touches the Pods:
    // one deleted out of band (kubectl, a node loss, the GC) leaves a HOLE, and
    // stopping there hides every higher ordinal. The next pass stops at the same
    // hole, so those Pods are orphaned permanently — a leak, not a delay.
    // Scanning on still yields the TRUE highest, so reverse order and the
    // one-per-pass pacing are both unchanged. Presence is all this scan needs,
    // so it probes with `exists` rather than reading each Pod through
    // `pod_state` — cheaper per ordinal than the partial scan it replaces.
    let mut highest: Option<u32> = None;
    let mut i = n;
    while i < MAX_REPLICAS {
        let mut pk = [0u8; MAX_KEY];
        let pkl = pod_key(&mut pk, ns, sts, i);
        if pkl == 0 {
            break; // key overflow is monotonic in the ordinal — nothing higher fits
        }
        if exists(sys, &pk[..pkl]) {
            highest = Some(i);
        }
        i += 1;
    }
    if let Some(h) = highest {
        let mut pk = [0u8; MAX_KEY];
        let pkl = pod_key(&mut pk, ns, sts, h);
        if pkl != 0 && delete_value(sys, &pk[..pkl]) {
            return 1;
        }
    }
    0
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(STATEFULSETS_PREFIX);
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
        wrote += reconcile_statefulset(sys, &keybuf[..klen], &valbuf[..vlen]);
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
                store_subscribe(sys, STATEFULSETS_PREFIX, s.sink, 0);
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
