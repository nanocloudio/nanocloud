//! Job reconciler — run-to-completion as a PIC module. A distinct controller
//! shape from the steady-state reconcilers: instead of maintaining a count, a
//! Job runs `completions` Pods once and tracks how many Succeeded. It drives
//! the control-plane store (`storage.object`/`storage.namespace`): SUBSCRIBE
//! `/jobs.batch/` and `/pods/`, create the Pods, and write a status.
//!
//! Data model — k8s objects are real nested JSON (read via the shared `json.rs`
//! `j_*` helpers); /job-status/ is an internal flat projection:
//!
//!   /jobs.batch/<ns>/<name>  = {…spec.completions, spec.template…}
//!   /pods/<ns>/<name>-<i>    = {…spec (template) embedded, metadata.ownerReferences, status.phase…}
//!   /job-status/<ns>/<name>  = "succeeded=<s>;complete=<0|1>"   (internal projection)
//!
//! Pods are created only-if-absent and never recreated on success (the essence
//! of run-to-completion); the kubelet/sandbox side sets `phase=`. `complete=1`
//! once `succeeded ≥ completions`. Pod-failure backoff/retry and parallelism
//! throttling are follow-ups (v1 launches all `completions` Pods at once). The
//! status write is GET-compare-guarded, so a settled Job spends no revisions.

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

const JOBS_PREFIX: &[u8] = b"/jobs.batch/";
const PODS_PREFIX: &[u8] = b"/pods/";
const JOBSTATUS_PREFIX: &[u8] = b"/job-status/";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const MAX_COMPLETIONS: u32 = 128;

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

/// `/pods/<ns><job>-<i>` into buf; returns length (0 on overflow).
fn pod_key(buf: &mut [u8], ns: &[u8], job: &[u8], i: u32) -> usize {
    if PODS_PREFIX.len() + ns.len() + job.len() + 1 + 10 > buf.len() {
        return 0;
    }
    let mut p = 0;
    buf[p..p + PODS_PREFIX.len()].copy_from_slice(PODS_PREFIX);
    p += PODS_PREFIX.len();
    buf[p..p + ns.len()].copy_from_slice(ns);
    p += ns.len();
    buf[p..p + job.len()].copy_from_slice(job);
    p += job.len();
    buf[p] = b'-';
    p += 1;
    write_u32(buf, p, i)
}

/// Reconcile one Job: launch `completions` Pods (once), count Succeeded, write
/// status. Returns writes made (pod creates + a status change).
unsafe fn reconcile_job(sys: &SyscallTable, job_key: &[u8], job_val: &[u8]) -> u32 {
    if j_get2(job_val, b"spec", b"completions").is_none() {
        return 0;
    }
    let n = j_u32_2(job_val, b"spec", b"completions").min(MAX_COMPLETIONS);
    let Some(ns) = namespace_seg(job_key, JOBS_PREFIX) else {
        return 0;
    };
    let job = last_seg(job_key);
    let tmpl = j_sub3(job_val, b"spec", b"template", b"spec").unwrap_or(b"{}");
    let ns_name = &ns[..ns.len().saturating_sub(1)];

    let mut writes = 0u32;
    let mut succeeded = 0u32;

    for i in 0..n {
        let mut pk = [0u8; MAX_KEY];
        let pkl = pod_key(&mut pk, ns, job, i);
        if pkl == 0 {
            break;
        }
        let mut pv = [0u8; MAX_VALUE];
        match get_value(sys, &pk[..pkl], &mut pv) {
            Some(vlen) => {
                // Existing pod — count it if Succeeded. Never recreate.
                if j_get2(&pv[..vlen], b"status", b"phase") == Some(b"Succeeded") {
                    succeeded += 1;
                }
            }
            None => {
                // Launch the pod (run-to-completion; created once).
                let mut nv = [0u8; MAX_VALUE];
                let mut v = j_cp(&mut nv, 0, b"{\"metadata\":{\"name\":\"");
                v = j_cp(&mut nv, v, job);
                v = j_cp(&mut nv, v, b"-");
                v = write_u32(&mut nv, v, i);
                v = j_cp(&mut nv, v, b"\",\"namespace\":\"");
                v = j_cp(&mut nv, v, ns_name);
                v = j_cp(
                    &mut nv,
                    v,
                    b"\",\"ownerReferences\":[{\"kind\":\"Job\",\"name\":\"",
                );
                v = j_cp(&mut nv, v, job);
                v = j_cp(&mut nv, v, b"\"}]},\"spec\":");
                v = j_cp(&mut nv, v, tmpl);
                v = j_cp(&mut nv, v, b"}");
                if put_value(sys, &pk[..pkl], &nv[..v]) {
                    writes += 1;
                }
            }
        }
    }

    // Status: succeeded=<s>;complete=<0|1>
    let mut doc = [0u8; 48];
    let mut d = 0;
    let put = |doc: &mut [u8; 48], d: &mut usize, s: &[u8]| {
        let take = s.len().min(doc.len() - *d);
        doc[*d..*d + take].copy_from_slice(&s[..take]);
        *d += take;
    };
    put(&mut doc, &mut d, b"succeeded=");
    d = write_u32(&mut doc, d, succeeded);
    put(&mut doc, &mut d, b";complete=");
    doc[d] = if succeeded >= n && n > 0 { b'1' } else { b'0' };
    d += 1;

    let mut sk = [0u8; MAX_KEY];
    let skl = JOBSTATUS_PREFIX.len() + ns.len() + job.len();
    if skl <= sk.len() {
        sk[..JOBSTATUS_PREFIX.len()].copy_from_slice(JOBSTATUS_PREFIX);
        sk[JOBSTATUS_PREFIX.len()..JOBSTATUS_PREFIX.len() + ns.len()].copy_from_slice(ns);
        sk[JOBSTATUS_PREFIX.len() + ns.len()..skl].copy_from_slice(job);
        // Only spend a revision when status changed.
        let mut cur = [0u8; 48];
        let changed = match get_value(sys, &sk[..skl], &mut cur) {
            Some(cl) => cur[..cl] != doc[..d],
            None => true,
        };
        if changed && put_value(sys, &sk[..skl], &doc[..d]) {
            writes += 1;
        }
    }

    writes
}

unsafe fn reconcile_all(sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(JOBS_PREFIX);
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
        wrote += reconcile_job(sys, &keybuf[..klen], &valbuf[..vlen]);
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
                store_subscribe(sys, JOBS_PREFIX, s.sink, 0);
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
