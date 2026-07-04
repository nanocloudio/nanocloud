//! Probe runner — liveness/readiness probes as a PIC module
//! (the DECISION half; no new fluxor surface). It
//! reads probe configs from pod specs, drives exec probes through the shipped
//! /sandbox-exec/ request seam (sandbox_runner owns the workload EXEC effect),
//! and writes verdicts to /probe-status/ — this module's ONLY owned prefix.
//! pod_lifecycle folds `live=0` into its restart+backoff machinery; `ready`
//! feeds the readiness plumbing.
//!
//! Data model:
//!   /pod-specs/<uid>       = "...;lprobe=<probe>;rprobe=<probe>;..."   (in)
//!   /sandbox-status/<uid>  = "state=..." (in — probes run only while running)
//!   /sandbox-exec/<uid>/<reqid>        = <cmd>   (out — request; runner execs)
//!   /sandbox-exec-result/<uid>/<reqid> = <exit>  (in — consumed + deleted here)
//!   /sandbox-exec-output/<uid>/<reqid>           (deleted here after use)
//!   /probe-status/<uid>    = "live=<0|1>;ready=<0|1>"                  (out)
//!
//! Compact probe encoding (fits the `;`-format — colon-separated INSIDE one
//! `;` field, so the value itself must not contain ';'):
//!
//!   <kind>:<target>:<period_s>:<timeout_s>:<fail_n>:<success_n>
//!
//!   kind      exec | tcp
//!   target    exec: the command line run inside the sandbox (exit 0 = pass;
//!             may itself contain ':' — the four trailing numeric fields are
//!             split from the RIGHT, everything between the kind and them is
//!             the target)
//!             tcp: the port to dial (parsed, NOT IMPLEMENTED — see below)
//!   period_s  probe cadence, seconds (min 1)
//!   timeout_s per-attempt timeout, seconds (min 1)
//!   fail_n    consecutive failures to flip the verdict to 0
//!   success_n consecutive successes to flip the verdict to 1
//!
//!   e.g. lprobe=exec:/bin/true:5:2:3:1   rprobe=exec:/bin/sh -c ...:1:2:1:1
//!
//! Verdict semantics (k8s-adjacent): liveness starts 1 (assumed live until
//! proven dead), readiness starts 0 (not ready until proven ready); a pod with
//! no readiness probe publishes ready=1. Verdicts reset to their initial
//! values whenever the sandbox is not running (created/destroyed/terminal) —
//! a relaunched run must re-prove/re-fail its probes, which also prevents a
//! stale live=0 from instantly re-killing the fresh run.
//!
//! tcp probes: parsed and validated, but NOT IMPLEMENTED — no nanocloud
//! module has a dial surface today (every app manifest grants only the
//! storage contracts, plus `workload` in sandbox_runner; there is no net
//! contract consumer to mirror). A configured tcp probe is ignored for
//! verdicts (treated as absent, never faked as passing OR failing) until a
//! dial seam exists.
//!
//! Time: this module reads the monotonic wall clock (`dev_millis`, TIMER::MILLIS
//! — uptime in ms) and measures true elapsed against it, so it is
//! `timer_class = "wall_clock"`: correct under any scheduler cadence, including
//! an adaptive/relaxed tick. Probe periods/timeouts are seconds-scale, so a
//! relaxed tick only changes when we sample, never the elapsed we measure.

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
include!("../_shared/store.rs");
include!("../_shared/field.rs");

// The control-plane store via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes, storage.namespace (0x13) prefix LIST +
// change SUBSCRIBE (pushed on our self-edge sink).
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

const SPECS_PREFIX: &[u8] = b"/pod-specs/";
const SANDBOX_STATUS_PREFIX: &[u8] = b"/sandbox-status/";
// The exec seam: we put /sandbox-exec/<uid>/<reqid> = cmd; sandbox_runner
// execs and puts /sandbox-exec-result/<uid>/<reqid> = <exit-code> (+ output);
// as the requesting provider WE delete all three keys once consumed.
const EXEC_PREFIX: &[u8] = b"/sandbox-exec/";
const EXEC_RESULT_PREFIX: &[u8] = b"/sandbox-exec-result/";
const EXEC_OUTPUT_PREFIX: &[u8] = b"/sandbox-exec-output/";
/// The verdict projection — this module's only owned prefix (single-writer
/// discipline: pod_lifecycle and the readiness plumbing only read it).
const PROBE_STATUS_PREFIX: &[u8] = b"/probe-status/";

/// Milliseconds per second: probe periods/timeouts are configured in whole
/// seconds and `dev_millis` (the clock) is in ms, so seconds → ms converts
/// through this.
const MS_PER_SEC: u64 = 1000;

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 256;
const LIST_BUF: usize = 2048;
const MAX_UID: usize = 96;
/// Bounded tracking (same degradation contract as the siblings' tables: an
/// overflowing cluster loses probes, never correctness).
const MAX_PODS: usize = 16;
const MAX_TARGET: usize = 128;

const KIND_EXEC: u8 = 0;
const KIND_TCP: u8 = 1;

/// One configured probe (liveness or readiness) + its runtime state.
#[repr(C)]
#[derive(Clone, Copy)]
struct Probe {
    present: bool,
    /// false = parsed but not driven (tcp — see the module header).
    enabled: bool,
    kind: u8,
    target_len: u8,
    fail_n: u8,
    success_n: u8,
    /// Consecutive fails/successes since the last opposite result.
    fails: u8,
    oks: u8,
    /// Current verdict (liveness inits 1, readiness inits 0).
    verdict: u8,
    inflight: bool,
    period_ms: u64,
    timeout_ms: u64,
    /// Monotonic-ms timestamp at/after which the next attempt fires (0 = immediately).
    next_due: u64,
    /// Monotonic-ms timestamp at which the in-flight attempt times out.
    deadline: u64,
    /// Monotonic per-probe request sequence (rides the exec reqid).
    seq: u32,
    target: [u8; MAX_TARGET],
}

const PROBE_EMPTY: Probe = Probe {
    present: false,
    enabled: false,
    kind: KIND_EXEC,
    target_len: 0,
    fail_n: 3,
    success_n: 1,
    fails: 0,
    oks: 0,
    verdict: 1,
    inflight: false,
    period_ms: 10 * MS_PER_SEC,
    timeout_ms: MS_PER_SEC,
    next_due: 0,
    deadline: 0,
    seq: 0,
    target: [0u8; MAX_TARGET],
};

/// Per-pod probe tracking. probes[0] = liveness (`lprobe=`, inits verdict 1),
/// probes[1] = readiness (`rprobe=`, inits verdict 0).
#[repr(C)]
#[derive(Clone, Copy)]
struct PodProbes {
    in_use: bool,
    /// Swept by sync_specs: a track whose spec vanished is freed (and its
    /// /probe-status/ deleted — owner cleanup).
    seen: bool,
    /// Last observed /sandbox-status/ state == running. Probes run only while
    /// true; on the transition to false the runtime state + verdicts reset.
    running: bool,
    uid_len: u8,
    /// Last published live/ready pair (bit0 = live, bit1 = ready, 0xFF =
    /// nothing published yet) — writes /probe-status/ only on change.
    published: u8,
    probes: [Probe; 2],
    uid: [u8; MAX_UID],
}

const POD_EMPTY: PodProbes = PodProbes {
    in_use: false,
    seen: false,
    running: false,
    uid_len: 0,
    published: 0xFF,
    probes: [PROBE_EMPTY; 2],
    uid: [0u8; MAX_UID],
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start sync have run.
    subscribed: u8,
    /// Count of verdict flips written — the observable of progress.
    flips: u32,
    pods: [PodProbes; MAX_PODS],
}

// ---- storage.object / storage.namespace ops (the repo-standard helpers) ----

// ---- compact-format parsing ----

/// Parse a leading run of ASCII digits as a u64 (saturating).
fn parse_u64(b: &[u8]) -> u64 {
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
    }
    v
}

/// Format an unsigned int as decimal into `buf`; returns the byte length.
fn write_u32(buf: &mut [u8], n: u32) -> usize {
    if buf.is_empty() {
        return 0;
    }
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }
    let mut v = n;
    let mut tmp = [0u8; 10];
    let mut i = 0;
    while v > 0 && i < tmp.len() {
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
        i += 1;
    }
    let mut p = 0;
    while i > 0 && p < buf.len() {
        i -= 1;
        buf[p] = tmp[i];
        p += 1;
    }
    p
}

/// Build `<prefix><uid>` into a key buffer; returns its length (0 on overflow).
fn make_key(dst: &mut [u8], prefix: &[u8], uid: &[u8]) -> usize {
    let n = prefix.len() + uid.len();
    if n > dst.len() {
        return 0;
    }
    dst[..prefix.len()].copy_from_slice(prefix);
    dst[prefix.len()..n].copy_from_slice(uid);
    n
}

/// Parse the compact probe encoding (module header) into `out`, preserving
/// runtime state when the config is unchanged. `init_verdict` = 1 for
/// liveness, 0 for readiness. Returns true when `out.present`.
///
/// The four trailing `:`-fields are split from the RIGHT so an exec target may
/// itself contain ':'. A malformed value parses as absent (no probe — never a
/// guessed one).
fn parse_probe(v: &[u8], init_verdict: u8, out: &mut Probe) -> bool {
    let Some(kpos) = v.iter().position(|&b| b == b':') else {
        return false;
    };
    let kind = match &v[..kpos] {
        b"exec" => KIND_EXEC,
        b"tcp" => KIND_TCP,
        _ => return false,
    };
    // Split the 4 numeric tail fields from the right.
    let rest = &v[kpos + 1..];
    let mut cuts = [0usize; 4]; // positions of the last 4 ':' in `rest`
    let mut found = 0;
    for i in (0..rest.len()).rev() {
        if rest[i] == b':' {
            cuts[3 - found] = i;
            found += 1;
            if found == 4 {
                break;
            }
        }
    }
    if found < 4 || cuts[0] == 0 {
        return false; // need target + 4 numbers
    }
    let target = &rest[..cuts[0]];
    let period = parse_u64(&rest[cuts[0] + 1..cuts[1]]).max(1);
    let timeout = parse_u64(&rest[cuts[1] + 1..cuts[2]]).max(1);
    let fail_n = parse_u64(&rest[cuts[2] + 1..cuts[3]]).clamp(1, 255) as u8;
    let success_n = parse_u64(&rest[cuts[3] + 1..]).clamp(1, 255) as u8;
    if target.is_empty() || target.len() > MAX_TARGET {
        return false;
    }

    // Unchanged config → keep the runtime state (counters, verdict, seq).
    let same = out.present
        && out.kind == kind
        && out.target_len as usize == target.len()
        && &out.target[..target.len()] == target
        && out.period_ms == period * MS_PER_SEC
        && out.timeout_ms == timeout * MS_PER_SEC
        && out.fail_n == fail_n
        && out.success_n == success_n;
    if same {
        return true;
    }
    *out = PROBE_EMPTY;
    out.present = true;
    // tcp: parsed but NOT driven — no nanocloud module has a dial surface
    // today (module header). Ignored for verdicts, never faked.
    out.enabled = kind == KIND_EXEC;
    out.kind = kind;
    out.target_len = target.len() as u8;
    out.target[..target.len()].copy_from_slice(target);
    out.period_ms = period * MS_PER_SEC;
    out.timeout_ms = timeout * MS_PER_SEC;
    out.fail_n = fail_n;
    out.success_n = success_n;
    out.verdict = init_verdict;
    true
}

// ---- pod table ----

fn find_pod(s: &State, uid: &[u8]) -> Option<usize> {
    s.pods
        .iter()
        .position(|t| t.in_use && &t.uid[..t.uid_len as usize] == uid)
}

/// Build the exec-seam tail `<uid>/p<l|r><seq>` into `buf`; returns its length
/// (0 on overflow). The reqid is namespaced with a `p` prefix so it can never
/// collide with CLI-issued exec request ids.
fn probe_tail(uid: &[u8], which: usize, seq: u32, buf: &mut [u8]) -> usize {
    let mut p = 0;
    if uid.len() + 8 > buf.len() {
        return 0;
    }
    p = append(buf, p, uid);
    p = append(buf, p, if which == 0 { b"/pl" } else { b"/pr" });
    let mut num = [0u8; 10];
    let nl = write_u32(&mut num, seq);
    if p + nl > buf.len() {
        return 0;
    }
    append(buf, p, &num[..nl])
}

/// Build `<prefix><tail>` and DELETE it (best-effort cleanup of the exec seam).
unsafe fn delete_prefixed(sys: &SyscallTable, prefix: &[u8], tail: &[u8]) {
    let mut key = [0u8; MAX_KEY];
    let n = prefix.len() + tail.len();
    if n > key.len() {
        return;
    }
    key[..prefix.len()].copy_from_slice(prefix);
    key[prefix.len()..n].copy_from_slice(tail);
    delete_value(sys, &key[..n]);
}

/// Record one probe attempt's outcome; returns true when the verdict flipped.
fn record(p: &mut Probe, pass: bool) -> bool {
    if pass {
        p.fails = 0;
        p.oks = p.oks.saturating_add(1);
        if p.verdict != 1 && p.oks >= p.success_n {
            p.verdict = 1;
            return true;
        }
    } else {
        p.oks = 0;
        p.fails = p.fails.saturating_add(1);
        if p.verdict != 0 && p.fails >= p.fail_n {
            p.verdict = 0;
            return true;
        }
    }
    false
}

/// Publish /probe-status/<uid> = live=<0|1>;ready=<0|1> when it changed since
/// the last publish. Verdict composition: no liveness probe → live=1; no
/// readiness probe → ready=1 (k8s-adjacent defaults); a disabled (tcp) probe
/// counts as absent.
unsafe fn publish(sys: &SyscallTable, t: &mut PodProbes) {
    let live: u8 = if t.probes[0].present && t.probes[0].enabled {
        t.probes[0].verdict
    } else {
        1
    };
    let ready: u8 = if t.probes[1].present && t.probes[1].enabled {
        t.probes[1].verdict
    } else {
        1
    };
    let packed = live | (ready << 1);
    if t.published == packed {
        return;
    }
    let ul = t.uid_len as usize;
    let mut key = [0u8; MAX_KEY];
    let kl = make_key(&mut key, PROBE_STATUS_PREFIX, &t.uid[..ul]);
    if kl == 0 {
        return;
    }
    let mut doc = [0u8; 16];
    let mut d = append(&mut doc, 0, b"live=");
    doc[d] = b'0' + live;
    d += 1;
    d = append(&mut doc, d, b";ready=");
    doc[d] = b'0' + ready;
    d += 1;
    if put_value(sys, &key[..kl], &doc[..d]) {
        t.published = packed;
    }
}

/// Reset a probe's runtime state to its initial verdict (sandbox not running:
/// a relaunched run re-proves its probes; prevents a stale live=0 from
/// re-killing the fresh run). Config survives; an in-flight request is
/// abandoned (its keys are deleted).
unsafe fn reset_probe(sys: &SyscallTable, uid: &[u8], which: usize, p: &mut Probe) {
    if p.inflight {
        let mut tail = [0u8; MAX_KEY];
        let tl = probe_tail(uid, which, p.seq, &mut tail);
        if tl != 0 {
            delete_prefixed(sys, EXEC_PREFIX, &tail[..tl]);
            delete_prefixed(sys, EXEC_RESULT_PREFIX, &tail[..tl]);
            delete_prefixed(sys, EXEC_OUTPUT_PREFIX, &tail[..tl]);
        }
    }
    p.inflight = false;
    p.fails = 0;
    p.oks = 0;
    p.next_due = 0;
    p.verdict = if which == 0 { 1 } else { 0 };
}

/// Sync the pod table from /pod-specs/ (probe configs) and /sandbox-status/
/// (running gate). Mark-and-sweep: vanished specs free their track and delete
/// its /probe-status/ (owner cleanup).
unsafe fn sync_specs(s: &mut State, sys: &SyscallTable) {
    for t in s.pods.iter_mut() {
        t.seen = false;
    }
    let mut walk = ListWalk::new(SPECS_PREFIX);
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
        let spec = &valbuf[..vlen];
        let uid = last_seg(&keybuf[..klen]);
        if uid.is_empty() || uid.len() > MAX_UID {
            continue;
        }
        let lprobe = field(spec, b"lprobe=");
        let rprobe = field(spec, b"rprobe=");
        let has_probe = lprobe.is_some() || rprobe.is_some();

        let idx = match find_pod(s, uid) {
            Some(i) => i,
            None => {
                if !has_probe {
                    continue; // nothing to track
                }
                let Some(i) = s.pods.iter().position(|t| !t.in_use) else {
                    continue; // table full — this pod loses probes, not correctness
                };
                s.pods[i] = POD_EMPTY;
                s.pods[i].in_use = true;
                s.pods[i].uid_len = uid.len() as u8;
                s.pods[i].uid[..uid.len()].copy_from_slice(uid);
                i
            }
        };
        s.pods[idx].seen = true;

        // (Re)parse configs — parse_probe keeps runtime state when unchanged.
        let mut lp = s.pods[idx].probes[0];
        let mut rp = s.pods[idx].probes[1];
        let lpresent = lprobe.map(|v| parse_probe(v, 1, &mut lp)).unwrap_or(false);
        let rpresent = rprobe.map(|v| parse_probe(v, 0, &mut rp)).unwrap_or(false);
        if !lpresent {
            lp = PROBE_EMPTY;
            lp.present = false;
        }
        if !rpresent {
            rp = PROBE_EMPTY;
            rp.present = false;
        }
        s.pods[idx].probes[0] = lp;
        s.pods[idx].probes[1] = rp;

        // Running gate from the runner-owned status.
        let mut skey = [0u8; MAX_KEY];
        let skl = make_key(&mut skey, SANDBOX_STATUS_PREFIX, uid);
        let mut sbuf = [0u8; MAX_VALUE];
        let running = skl != 0
            && get_value(sys, &skey[..skl], &mut sbuf)
                .map(|n| field(&sbuf[..n], b"state=") == Some(b"running"))
                .unwrap_or(false);
        if s.pods[idx].running && !running {
            // Left running (paused/terminal/destroyed): reset verdicts so the
            // next run re-proves them, and publish the reset.
            let ul = s.pods[idx].uid_len as usize;
            let mut uidbuf = [0u8; MAX_UID];
            uidbuf[..ul].copy_from_slice(&s.pods[idx].uid[..ul]);
            for w in 0..2 {
                if s.pods[idx].probes[w].present {
                    let mut pb = s.pods[idx].probes[w];
                    reset_probe(sys, &uidbuf[..ul], w, &mut pb);
                    s.pods[idx].probes[w] = pb;
                }
            }
        }
        s.pods[idx].running = running;
        publish(sys, &mut s.pods[idx]);
    }
    // Sweep vanished pods; delete their verdict key (we own it).
    for i in 0..MAX_PODS {
        if s.pods[i].in_use && !s.pods[i].seen {
            let ul = s.pods[i].uid_len as usize;
            let mut uid = [0u8; MAX_UID];
            uid[..ul].copy_from_slice(&s.pods[i].uid[..ul]);
            let mut key = [0u8; MAX_KEY];
            let kl = make_key(&mut key, PROBE_STATUS_PREFIX, &uid[..ul]);
            if kl != 0 {
                delete_value(sys, &key[..kl]);
            }
            s.pods[i] = POD_EMPTY;
        }
    }
}

/// Drive every enabled probe of every running pod one step: issue due
/// attempts over the exec seam, collect/timeout in-flight ones, fold results
/// into verdicts, publish flips.
unsafe fn pump_probes(s: &mut State, sys: &SyscallTable) {
    let now = dev_millis(sys).max(1);
    for i in 0..MAX_PODS {
        if !s.pods[i].in_use || !s.pods[i].running {
            continue;
        }
        let ul = s.pods[i].uid_len as usize;
        let mut uid = [0u8; MAX_UID];
        uid[..ul].copy_from_slice(&s.pods[i].uid[..ul]);
        let mut changed = false;
        for w in 0..2 {
            let mut pb = s.pods[i].probes[w];
            if !pb.present || !pb.enabled {
                continue;
            }
            if pb.inflight {
                let mut tail = [0u8; MAX_KEY];
                let tl = probe_tail(&uid[..ul], w, pb.seq, &mut tail);
                if tl == 0 {
                    pb.inflight = false;
                    s.pods[i].probes[w] = pb;
                    continue;
                }
                let mut rkey = [0u8; MAX_KEY];
                let rl = make_key(&mut rkey, EXEC_RESULT_PREFIX, &tail[..tl]);
                let mut rbuf = [0u8; 16];
                if let Some(n) = get_value(sys, &rkey[..rl], &mut rbuf) {
                    // Result landed: exit 0 = pass. Consume all three keys
                    // (we are the requesting provider on this seam).
                    let pass = &rbuf[..n] == b"0";
                    delete_prefixed(sys, EXEC_PREFIX, &tail[..tl]);
                    delete_prefixed(sys, EXEC_RESULT_PREFIX, &tail[..tl]);
                    delete_prefixed(sys, EXEC_OUTPUT_PREFIX, &tail[..tl]);
                    pb.inflight = false;
                    if record(&mut pb, pass) {
                        changed = true;
                        s.flips = s.flips.wrapping_add(1);
                    }
                } else if now >= pb.deadline {
                    // Timed out: abandon the request (delete it so the seam
                    // does not accumulate) and count a failure.
                    delete_prefixed(sys, EXEC_PREFIX, &tail[..tl]);
                    delete_prefixed(sys, EXEC_RESULT_PREFIX, &tail[..tl]);
                    delete_prefixed(sys, EXEC_OUTPUT_PREFIX, &tail[..tl]);
                    pb.inflight = false;
                    if record(&mut pb, false) {
                        changed = true;
                        s.flips = s.flips.wrapping_add(1);
                    }
                }
            } else if now >= pb.next_due {
                pb.seq = pb.seq.wrapping_add(1);
                let mut tail = [0u8; MAX_KEY];
                let tl = probe_tail(&uid[..ul], w, pb.seq, &mut tail);
                if tl != 0 {
                    // Clear any stale result under this reqid (a module
                    // restart forgets in-flight seq numbers), then request.
                    delete_prefixed(sys, EXEC_RESULT_PREFIX, &tail[..tl]);
                    delete_prefixed(sys, EXEC_OUTPUT_PREFIX, &tail[..tl]);
                    let mut ekey = [0u8; MAX_KEY];
                    let el = make_key(&mut ekey, EXEC_PREFIX, &tail[..tl]);
                    if el != 0 && put_value(sys, &ekey[..el], &pb.target[..pb.target_len as usize])
                    {
                        pb.inflight = true;
                        pb.deadline = now.saturating_add(pb.timeout_ms);
                    }
                }
                pb.next_due = now.saturating_add(pb.period_ms);
            }
            s.pods[i].probes[w] = pb;
        }
        if changed {
            publish(sys, &mut s.pods[i]);
        }
    }
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
        s.flips = 0;
        s.pods = [POD_EMPTY; MAX_PODS];
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

        // Cold start: resolve the change-sink channel (self-edge allocated),
        // SUBSCRIBE the watched prefixes, then an initial sync.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, SPECS_PREFIX, s.sink, 0);
                // Runner-owned status (running gate) — read-only here.
                store_subscribe(sys, SANDBOX_STATUS_PREFIX, s.sink, 0);
                // Exec results wake the in-flight collection promptly (the
                // per-step deadline scan would catch them anyway).
                store_subscribe(sys, EXEC_RESULT_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            sync_specs(s, sys);
            return 0;
        }

        // Config/status movement → re-sync the table.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            sync_specs(s, sys);
        }
        // Cadence runs every step regardless — probe deadlines generate no
        // store change. In-memory checks only, store I/O only when due.
        pump_probes(s, sys);
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
