//! HorizontalPodAutoscaler — a feedback controller as a PIC module — the one
//! *closed-loop* controller: it reads a metric and adjusts a Deployment's
//! replica count, which then cascades through the workload controllers back to
//! Pods. It drives the control-plane store
//! (`storage.object`/`storage.namespace`): SUBSCRIBE `/hpa/` and
//! `/hpa-metrics/`, compute the desired replicas, and write them onto the
//! target Deployment.
//!
//! Data model — k8s objects are real nested JSON (read/written via the shared
//! `json.rs` `j_*` helpers); /hpa-metrics/ is an internal flat projection:
//!
//!   /hpa/<ns>/<name>          = {…spec.scaleTargetRef.name, spec.minReplicas, spec.maxReplicas…}
//!   /hpa-metrics/<ns>/<dep>   = "cpu=<avg-utilisation-pct>"   (internal projection, metrics-server's job)
//!   /deployments.apps/<ns>/<dep> = {…spec.replicas…}          (the knob it turns)
//!
//! Policy (the k8s HPA formula): `desired = ceil(R * currentCPU / targetCPU)`,
//! clamped to `[min, max]`. It does NOT watch the Deployment it writes — only
//! `/hpa/` and `/hpa-metrics/` — so a scale-up can't feed back into itself and
//! run away; the loop advances one step per metrics update (as utilisation
//! falls across the new replicas, the next metric drives it to a fixed point).
//! The replicas write is skipped when unchanged, so a settled HPA is quiet.

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

const HPA_PREFIX: &[u8] = b"/hpa/";
const METRICS_PREFIX: &[u8] = b"/hpa-metrics/";
const DEPLOYMENTS_PREFIX: &[u8] = b"/deployments.apps/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs have run (HPA does not cold-start reconcile).
    subscribed: u8,
    scales: u32,
}

// ---- helpers ----

/// Accept both the reconciler ';' and API-plane ',' field separators.
fn is_delim(b: u8) -> bool {
    b == b';' || b == b','
}

fn field<'a>(value: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut start = 0;
    while start <= value.len() {
        let end = value[start..]
            .iter()
            .position(|&b| is_delim(b))
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

/// Splice a new value for `tag` into a compact `src`, preserving every other
/// field and the surrounding delimiters. If `tag` is absent, append `;tag=val`.
/// Returns the new length, or None on overflow.
fn set_field(src: &[u8], tag: &[u8], newval: &[u8], out: &mut [u8]) -> Option<usize> {
    // Find the field: `tag` at src start or right after a delimiter.
    let mut i = 0usize;
    let mut found: Option<usize> = None;
    while i + tag.len() <= src.len() {
        let at_start = i == 0 || is_delim(src[i - 1]);
        if at_start && &src[i..i + tag.len()] == tag {
            found = Some(i);
            break;
        }
        i += 1;
    }

    match found {
        Some(pos) => {
            let vstart = pos + tag.len();
            let mut vend = vstart;
            while vend < src.len() && !is_delim(src[vend]) {
                vend += 1;
            }
            let total = vstart + newval.len() + (src.len() - vend);
            if total > out.len() {
                return None;
            }
            out[..vstart].copy_from_slice(&src[..vstart]);
            out[vstart..vstart + newval.len()].copy_from_slice(newval);
            out[vstart + newval.len()..total].copy_from_slice(&src[vend..]);
            Some(total)
        }
        None => {
            // Append ";tag=newval".
            let total = src.len() + 1 + tag.len() + newval.len();
            if total > out.len() {
                return None;
            }
            out[..src.len()].copy_from_slice(src);
            let mut p = src.len();
            out[p] = b';';
            p += 1;
            out[p..p + tag.len()].copy_from_slice(tag);
            p += tag.len();
            out[p..p + newval.len()].copy_from_slice(newval);
            Some(total)
        }
    }
}

/// Compute + apply the desired replica count for one HPA. Returns 1 if it
/// rescaled the target Deployment.
unsafe fn reconcile_hpa(sys: &SyscallTable, hpa_key: &[u8], hpa_val: &[u8]) -> u32 {
    let Some(target) = j_get3(hpa_val, b"spec", b"scaleTargetRef", b"name") else {
        return 0;
    };
    let min = j_get2(hpa_val, b"spec", b"minReplicas")
        .map(parse_u32)
        .unwrap_or(1)
        .max(1);
    let max = j_get2(hpa_val, b"spec", b"maxReplicas")
        .map(parse_u32)
        .unwrap_or(min)
        .max(min);
    let target_cpu = j_get2(hpa_val, b"spec", b"targetCPUUtilizationPercentage")
        .map(parse_u32)
        .unwrap_or(0);
    if target_cpu == 0 {
        return 0; // avoid divide-by-zero; a 0% target is meaningless
    }
    let Some(ns) = namespace_seg(hpa_key, HPA_PREFIX) else {
        return 0;
    };

    // Target Deployment key: /deployments.apps/<ns>/<target>.
    let mut dep_key = [0u8; MAX_KEY];
    let dkl = DEPLOYMENTS_PREFIX.len() + ns.len() + target.len();
    if dkl > dep_key.len() {
        return 0;
    }
    dep_key[..DEPLOYMENTS_PREFIX.len()].copy_from_slice(DEPLOYMENTS_PREFIX);
    dep_key[DEPLOYMENTS_PREFIX.len()..DEPLOYMENTS_PREFIX.len() + ns.len()].copy_from_slice(ns);
    dep_key[DEPLOYMENTS_PREFIX.len() + ns.len()..dkl].copy_from_slice(target);

    let mut dep_val = [0u8; MAX_VALUE];
    let Some(dvl) = get_value(sys, &dep_key[..dkl], &mut dep_val) else {
        return 0; // no such Deployment
    };
    let current = j_get2(&dep_val[..dvl], b"spec", b"replicas")
        .map(parse_u32)
        .unwrap_or(0);
    if current == 0 {
        return 0;
    }

    // Metrics: /hpa-metrics/<ns>/<target> = "cpu=<avg-util-pct>".
    let mut m_key = [0u8; MAX_KEY];
    let mkl = METRICS_PREFIX.len() + ns.len() + target.len();
    if mkl > m_key.len() {
        return 0;
    }
    m_key[..METRICS_PREFIX.len()].copy_from_slice(METRICS_PREFIX);
    m_key[METRICS_PREFIX.len()..METRICS_PREFIX.len() + ns.len()].copy_from_slice(ns);
    m_key[METRICS_PREFIX.len() + ns.len()..mkl].copy_from_slice(target);
    let mut m_val = [0u8; MAX_VALUE];
    let Some(mvl) = get_value(sys, &m_key[..mkl], &mut m_val) else {
        return 0; // no metrics yet — hold
    };
    let cur_cpu = field(&m_val[..mvl], b"cpu=").map(parse_u32).unwrap_or(0);

    // desired = ceil(current * curCPU / targetCPU), clamped to [min, max].
    let scaled = (current as u64) * (cur_cpu as u64);
    let mut desired = scaled.div_ceil(target_cpu as u64) as u32;
    if desired < min {
        desired = min;
    }
    if desired > max {
        desired = max;
    }
    if desired == current {
        return 0;
    }

    // Write the new replica count back onto the Deployment.
    let mut nb = [0u8; 10];
    let nl = write_u32(&mut nb, 0, desired);
    let mut newdep = [0u8; MAX_VALUE];
    let newlen = j_set2(
        &dep_val[..dvl],
        b"spec",
        b"replicas",
        &nb[..nl],
        &mut newdep,
    );
    if newlen == 0 {
        return 0;
    }
    if put_value(sys, &dep_key[..dkl], &newdep[..newlen]) {
        1
    } else {
        0
    }
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(HPA_PREFIX);
    let mut scaled = 0u32;
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
        scaled += reconcile_hpa(sys, &keybuf[..klen], &valbuf[..vlen]);
    }
    scaled
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
        s.scales = 0;
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
        // SUBSCRIBE both prefixes onto it. Unlike the other reconcilers, the HPA
        // does NOT reconcile at cold start: `desired` is a function of
        // `currentReplicas`, which the HPA itself mutates, so reconciling both at
        // cold-start AND on the subscribe-replayed metrics event would double-
        // count and run away. Instead every reconcile is metric-driven — so we
        // SUBSCRIBE with the include-initial-listing flag (bit0=1), which replays
        // the existing keys as change events: they arrive on `sink`, drain below
        // as the single first trigger, and each later update drives one step.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, HPA_PREFIX, s.sink, 1);
                store_subscribe(sys, METRICS_PREFIX, s.sink, 1);
            }
            s.subscribed = 1;
            return 0;
        }
        // A pushed namespace.change (either prefix) drives exactly one step.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            s.scales = s.scales.wrapping_add(reconcile_all(sys));
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
