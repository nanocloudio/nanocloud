//! Endpoints reconciler — nanocloud's endpoints controller as a PIC module. It
//! drives the control-plane store (`storage.object`/`storage.namespace`)
//! through the full reconcile loop via `provider_call`, over the ONE shared
//! cluster store: the API plane writes services and pods in; this module writes
//! endpoints out.
//!
//! Data model (the compact projection format):
//!
//!   /services/<ns>/<name>  = "sel=k=v,k=v"            (label selector)
//!   /pods/<ns>/<name>      = "ip=<addr>;l=k=v,k=v;r=1" (address; labels; ready)
//!   /endpoints/<ns>/<name> = "<pod>=<addr>,<pod>=<addr>"
//!                            (ready, selector-matched backends, key-ordered;
//!                             pod names included so the consumer can rebuild
//!                             per-backend DNS records)
//!
//! Loop shape (level-triggered):
//!   first step → SUBSCRIBE /services/, /pods/ and /probe-status/ (three watches
//!                — NOT the root, or our own /endpoints writes would wake us
//!                forever)
//!   each step  → DRAIN all; on any change: for every service, match ready
//!                pods in its namespace against its selector and write the
//!                endpoints doc — but only when it CHANGED (a GET-compare
//!                guards the PUT, so a quiet cluster spends no revisions).
//!
//! Readiness bridge: when probe_runner tracks a
//! pod, its verdict at `/probe-status/<uid>` (joined by the pod's
//! `metadata.uid`) OVERRIDES the static `status.ready`. A tracked pod with no
//! readiness probe defaults to `ready=1` there, so probeless pods keep their
//! standing; a pod with a readiness probe joins the endpoint set only once the
//! probe passes (even if its static field was seeded true). A pod probe_runner
//! doesn't track — no uid, or no `/probe-status` entry — keeps its static
//! `status.ready`. We only read that key; probe_runner is its single writer.
//!
//! Every op carries the store provider's caller-output convention: the arg
//! ends with `[out_ptr:u64][out_cap:u32][fence_ptr:u64][fence_cap:u16]` so the
//! result and its fence land in module-owned buffers. Ops are one-shot
//! (handle = -1, class-byte routed on `op >> 8`).

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

const SERVICES_PREFIX: &[u8] = b"/services/";
const PODS_PREFIX: &[u8] = b"/pods/";
const ENDPOINTS_PREFIX: &[u8] = b"/endpoints/";
// probe_runner's owned prefix: the readiness/liveness
// verdict for a running pod, keyed by pod uid — the SAME uid the pod's k8s object
// carries at `metadata.uid`. Reading it here folds a readiness probe's verdict
// into endpoints selection; we only GET it, we
// never write it (single-writer: probe_runner owns this prefix).
const PROBE_STATUS_PREFIX: &[u8] = b"/probe-status/";

/// Bounded work-buffer sizes: keys, one object's value, one endpoints doc,
/// and one LIST page. A cluster larger than these reconciles partially rather
/// than corrupting — acceptable for the single-node target; page/loop growth
/// is mechanical when needed.
const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const MAX_DOC: usize = 480;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// Count of endpoints docs (re)written — the observable of progress.
    reconciles: u32,
}

// ---- compact-format parsing (byte scanning; the format is ours) ----

/// Probe-readiness gate for a pod.
///
/// `uid` is the pod's `metadata.uid`, which is also probe_runner's key. Returns:
///   - `None`  — no `/probe-status/<uid>` entry (the pod isn't tracked by
///     probe_runner: pure control-plane pods, or a pod not yet/no longer
///     running). The caller keeps the pod's static `status.ready`.
///   - `Some(true|false)` — the probe verdict. probe_runner defaults a pod with
///     no readiness probe to `ready=1`, so a probeless-but-tracked pod is not
///     regressed; a pod WITH a readiness probe reads `ready=0` until it passes.
unsafe fn probe_ready(sys: &SyscallTable, uid: &[u8]) -> Option<bool> {
    let mut key = [0u8; MAX_KEY];
    let kl = PROBE_STATUS_PREFIX.len() + uid.len();
    if kl > key.len() {
        return None;
    }
    key[..PROBE_STATUS_PREFIX.len()].copy_from_slice(PROBE_STATUS_PREFIX);
    key[PROBE_STATUS_PREFIX.len()..kl].copy_from_slice(uid);
    let mut val = [0u8; 64];
    let vlen = get_value(sys, &key[..kl], &mut val)?;
    Some(field(&val[..vlen], b"ready=") == Some(b"1"))
}

/// Does the comma-separated label list contain the exact `k=v` token?
fn labels_contain(labels: &[u8], token: &[u8]) -> bool {
    let mut start = 0;
    while start <= labels.len() {
        let end = labels[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(labels.len());
        if &labels[start..end] == token {
            return true;
        }
        if end >= labels.len() {
            break;
        }
        start = end + 1;
    }
    false
}

/// Every selector token appears in the pod's labels. An EMPTY selector
/// matches nothing (a k8s empty selector selects no pods).
fn selector_matches(selector: &[u8], labels: &[u8]) -> bool {
    if selector.is_empty() {
        return false;
    }
    let mut start = 0;
    while start <= selector.len() {
        let end = selector[start..]
            .iter()
            .position(|&b| b == b',')
            .map(|i| start + i)
            .unwrap_or(selector.len());
        if !labels_contain(labels, &selector[start..end]) {
            return false;
        }
        if end >= selector.len() {
            break;
        }
        start = end + 1;
    }
    true
}

/// The `<ns>/` segment (with trailing slash) after `prefix` in `key`, e.g.
/// key `/services/default/web`, prefix `/services/` → `default/`.
fn namespace_seg<'a>(key: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    let rest = key.get(prefix.len()..)?;
    let slash = rest.iter().position(|&b| b == b'/')?;
    Some(&rest[..slash + 1])
}

/// Reconcile one service: match ready pods in its namespace against its
/// selector, build the endpoints doc, write it if it changed. Returns true
/// if a doc was (re)written.
unsafe fn reconcile_service(sys: &SyscallTable, svc_key: &[u8], svc_val: &[u8]) -> bool {
    let Some(selector) = j_sub2(svc_val, b"spec", b"selector") else {
        return false;
    };
    let Some(ns) = namespace_seg(svc_key, SERVICES_PREFIX) else {
        return false;
    };

    // Pods of the same namespace: /pods/<ns>/
    let mut pods_prefix = [0u8; MAX_KEY];
    let ppl = PODS_PREFIX.len() + ns.len();
    if ppl > pods_prefix.len() {
        return false;
    }
    pods_prefix[..PODS_PREFIX.len()].copy_from_slice(PODS_PREFIX);
    pods_prefix[PODS_PREFIX.len()..ppl].copy_from_slice(ns);

    let mut walk = ListWalk::new(&pods_prefix[..ppl]);

    // Build the doc: `<pod>=<addr>,...` for ready, selector-matched pods, in
    // the key order LIST guarantees.
    let mut doc = [0u8; MAX_DOC];
    let mut dlen = 0usize;
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
        let val = &valbuf[..vlen];
        let mut ready = j_get2(val, b"status", b"ready") == Some(b"true");
        // Readiness bridge: probe_runner is the readiness authority for
        // any pod it tracks. Join by the pod's metadata.uid — the same key it
        // writes at /probe-status/<uid> — and let that verdict OVERRIDE the
        // static field. A tracked pod with no readiness probe publishes ready=1
        // (probe_runner's default), so it is not regressed; a pod with a
        // readiness probe is included only once the probe passes, even if its
        // static status.ready was seeded true. No uid, or no /probe-status entry
        // (pure control-plane pods, or a pod not currently running), leaves the
        // static verdict authoritative — the endpoints-reconciler-e2e path.
        if let Some(uid) = j_get2(val, b"metadata", b"uid") {
            if let Some(pr) = probe_ready(sys, uid) {
                ready = pr;
            }
        }
        let labels = j_sub2(val, b"metadata", b"labels").unwrap_or(b"{}");
        let Some(ip) = j_get2(val, b"status", b"podIP") else {
            continue;
        };
        if !ready || !j_obj_subset(selector, labels) {
            continue;
        }
        let name = last_seg(&keybuf[..klen]);
        let need = name.len() + 1 + ip.len() + usize::from(dlen > 0);
        if dlen + need > doc.len() {
            break; // bounded doc; larger clusters truncate rather than corrupt
        }
        if dlen > 0 {
            doc[dlen] = b',';
            dlen += 1;
        }
        doc[dlen..dlen + name.len()].copy_from_slice(name);
        dlen += name.len();
        doc[dlen] = b'=';
        dlen += 1;
        doc[dlen..dlen + ip.len()].copy_from_slice(ip);
        dlen += ip.len();
    }

    // /endpoints/<ns>/<name>
    let svc_name = last_seg(svc_key);
    let mut ep_key = [0u8; MAX_KEY];
    let ekl = ENDPOINTS_PREFIX.len() + ns.len() + svc_name.len();
    if ekl > ep_key.len() {
        return false;
    }
    ep_key[..ENDPOINTS_PREFIX.len()].copy_from_slice(ENDPOINTS_PREFIX);
    ep_key[ENDPOINTS_PREFIX.len()..ENDPOINTS_PREFIX.len() + ns.len()].copy_from_slice(ns);
    ep_key[ENDPOINTS_PREFIX.len() + ns.len()..ekl].copy_from_slice(svc_name);

    // Only spend a revision when the doc actually changed.
    let mut current = [0u8; MAX_DOC];
    if let Some(clen) = get_value(sys, &ep_key[..ekl], &mut current) {
        if current[..clen] == doc[..dlen] {
            return false;
        }
    } else if dlen == 0 {
        return false; // absent and still empty — nothing to record
    }
    put_value(sys, &ep_key[..ekl], &doc[..dlen])
}

/// Reconcile every service (level-triggered full pass). Returns the count of
/// endpoints docs (re)written.
unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(SERVICES_PREFIX);
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
        let mut svc_val = [0u8; MAX_VALUE];
        svc_val[..vlen].copy_from_slice(&valbuf[..vlen]);
        if reconcile_service(sys, &keybuf[..klen], &svc_val[..vlen]) {
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
                store_subscribe(sys, SERVICES_PREFIX, s.sink, 0);
                store_subscribe(sys, PODS_PREFIX, s.sink, 0);
                // bridge: a readiness-probe flip changes only /probe-status/,
                // not the pod object — watch it so the flip wakes a re-reconcile.
                store_subscribe(sys, PROBE_STATUS_PREFIX, s.sink, 0);
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
