//! Pod lifecycle — the kubelet DECISION half as a PIC module. It is a pure
//! state machine over pod specs: given what the control plane wants running
//! (/pod-specs/) and what the sandbox is doing (/sandbox-status/), it decides
//! the sandbox lifecycle phase (/sandboxes/, consumed by sandbox_runner) and
//! the resulting pod phase (/pod-lifecycle-status/). It never touches a
//! privileged surface — the runtime EFFECT is sandbox_runner's job — so it
//! needs only the store.
//!
//! Data model:
//!   /pod-specs/<uid>            = "cmd=<argv>;desired=<running|deleted>[;rootfs=<path>][;iso=1][;net=own][;grace=<secs>][;tasks=<n>]"
//!   /sandboxes/<uid>           = "cmd=<argv>;phase=<start|delete>[;rootfs=<path>][;iso=1][;net=own][;tasks=<n>]" (out → sandbox_runner)
//!   /sandbox-status/<uid>      = "state=<...>;code=<n>"                 (in ← sandbox_runner)
//!   /sandbox-kill/<uid>        = "sig=term" | "sig=kill"    (out → sandbox_runner two-phase kill)
//!   /probe-status/<uid>        = "live=<0|1>;ready=<0|1>"               (in ← probe_runner)
//!   /pod-lifecycle-status/<uid> = "phase=<Pending|Running|Paused|Succeeded|Failed|Terminating>[;restarts=<n>]"
//!
//! Paused: `state=paused` is neither terminal (no restart trigger, no
//! `n` increment) nor success (no backoff reset; the 600s reset clock
//! freezes — its baseline shifts forward by the paused span) nor ready. It
//! projects the distinguishable `phase=Paused`.
//!
//! Probe folding: a `live=0` verdict on /probe-status/<uid> (written by
//! probe_runner past its failure threshold) is treated as a failed run — it
//! drives the SAME restart+backoff machinery as a non-zero exit. This module
//! never writes /sandbox-status/ (runner-owned) or /probe-status/
//! (probe_runner-owned).
//!
//! State machine (per pod uid):
//!   desired=running:  project /sandboxes phase=start; map the sandbox state to
//!                     Pending → Running → Succeeded/Failed. A restarting pod
//!                     relaunches under exponential backoff (min(1s×2^(n-1),
//!                     300s), reset after a 600s run; n rides `;restarts=`).
//!   desired=deleted:  EVALUATED FIRST — before the restart branch — so a pod
//!                     being killed can never be relaunched by its
//!                     restartPolicy. grace>0
//!                     writes /sandbox-kill/ sig=term, escalates to sig=kill
//!                     after the grace deadline (in-state, monotonic ms), then
//!                     runs the delete flow; grace=0 = immediate delete.
//! GET-compare guards every write, so a settled pod spends no revisions.
//!
//! Time: all deadlines here are monotonic wall-clock ms, read from `dev_millis`
//! (TIMER::MILLIS — uptime ms), so timing is correct under any scheduler cadence
//! (`timer_class = "wall_clock"`). A module restart forgets in-flight
//! kill/backoff deadlines; the restart counts survive via
//! /pod-lifecycle-status/.

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
include!("../_shared/event.rs");
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

const SPECS_PREFIX: &[u8] = b"/pod-specs/";
const SANDBOXES_PREFIX: &[u8] = b"/sandboxes/";
/// Metal (fmod-graph) sandbox projection — the analogue of `/sandboxes/` for a
/// pod whose spec carries `graph=<template>` instead of `cmd=<argv>`.
/// Consumed by the `kubelet` fmod (the
/// metal EFFECT half, sibling of `sandbox_runner`), which composes the named
/// template into a FLXA subgraph and drives the metal `workload` 0x1A backend
/// (source_kind = FMOD_GRAPH). This module stays the shared DECISION half: it
/// projects start/delete here and reads `/sandbox-status/<uid>` (kubelet-written
/// for a metal pod, runner-written for a bundle pod — a uid is one XOR the
/// other, so the status key keeps a single writer) exactly as for a bundle pod.
const METAL_SANDBOXES_PREFIX: &[u8] = b"/metal-sandboxes/";
const SANDBOX_STATUS_PREFIX: &[u8] = b"/sandbox-status/";
const POD_STATUS_PREFIX: &[u8] = b"/pod-lifecycle-status/";
/// Two-phase kill seam: this module (the DECISION)
/// owns the key; sandbox_runner reacts to it with SIGNAL. Values: "sig=term",
/// "sig=kill". Never /sandbox-status/ — that key is runner-owned.
const KILL_PREFIX: &[u8] = b"/sandbox-kill/";
/// Probe verdicts, probe_runner-owned; read-only
/// here. `live=0` folds into the restart decision; `ready` is consumed by the
/// readiness plumbing (not this module).
const PROBE_STATUS_PREFIX: &[u8] = b"/probe-status/";
/// Image-backed pods: a spec's `image=<repo>[:<tag>]`
/// resolves through the pull chain. This module writes the request (the pull
/// trigger, consumed by image_fetcher) and reads the assembled-rootfs record
/// (image_assembler-owned); until it is `state=ready` the pod holds at Pending.
const IMAGE_REQUESTS_PREFIX: &[u8] = b"/image-requests/";
const IMAGE_ROOTFS_PREFIX: &[u8] = b"/image-rootfs/";
const IMAGE_CONFIG_PREFIX: &[u8] = b"/image-config/";

const MAX_KEY: usize = 128;
/// Must hold a full pod spec AND the /sandboxes/ doc projected from it —
/// truncation silently drops trailing fields. 1024 leaves ample headroom for
/// image-backed specs with long cmd/rootfs paths (bare-metal PIC stack keeps
/// these buffers state- or shallow-frame-resident; don't grow casually).
const MAX_VALUE: usize = 1024;
const LIST_BUF: usize = 2048;

/// Milliseconds per second. Grace/backoff deadlines are configured in whole
/// seconds and the clock (`dev_millis`, monotonic uptime ms) is in ms, so
/// seconds → ms converts through this constant. The module reads real elapsed
/// time, so it is `timer_class = "wall_clock"` — correct under any cadence,
/// including an adaptive/relaxed tick.
const MS_PER_SEC: u64 = 1000;
/// Default terminationGracePeriodSeconds (pod-spec field `grace=<secs>`,
/// the compact-format carrier of the k8s name). grace=0 preserves the
/// immediate-delete path.
const DEFAULT_GRACE_SECS: u64 = 10;
// Restart backoff: delay = min(BASE × 2^(n-1), CAP), n = consecutive
// failed runs; n resets after a run survives RESET. Constants rather than a
// per-pod `restartBackoff={base,cap,reset}` override: /pod-specs/ is the
// compact `;` format, not JSON, so an object-shaped override has no natural
// carrier yet. Defaults match the kubelet CrashLoopBackOff envelope.
const BACKOFF_BASE_SECS: u64 = 1;
const BACKOFF_CAP_SECS: u64 = 300;
const BACKOFF_RESET_SECS: u64 = 600;

/// Bounded per-pod tracking table (same degradation contract as
/// sandbox_runner's MAX_SB: an overflowing cluster loses grace/backoff
/// niceties — falling back to immediate delete / immediate restart — never
/// correctness).
const MAX_PODS: usize = 16;
const MAX_UID: usize = 96;

/// Per-pod deadlines + restart bookkeeping. All time values are monotonic
/// wall-clock ms (dev_millis) — nothing here goes on the wire.
#[repr(C)]
#[derive(Clone, Copy)]
struct PodTrack {
    in_use: bool,
    /// Swept by reconcile_all: a track whose /pod-specs/ key vanished is freed.
    seen: bool,
    /// Terminal-state latch — edge-detects the restart increment (the terminal
    /// status persists across reconciles until the teardown completes).
    was_terminal: bool,
    /// Kill escalation: 0 = none, 1 = sig=term written, 2 = sig=kill written.
    kill_phase: u8,
    uid_len: u8,
    /// Consecutive failed runs (`n` in the backoff formula); persisted as `;restarts=<n>`
    /// on /pod-lifecycle-status/ and recovered from it on track allocation.
    restarts: u32,
    /// Step at which sig=term escalates to sig=kill (kill_phase == 1 only).
    kill_deadline: u64,
    /// Relaunch deferred until this step (0 = no backoff pending).
    backoff_until: u64,
    /// Step at which the current run was first observed running (0 = not yet);
    /// a run older than BACKOFF_RESET_SECS resets `restarts` on its next fail.
    run_started: u64,
    /// Step at which the current run was observed paused (0 = not paused).
    /// the reset clock freezes while paused — on resume, `run_started`
    /// shifts forward by the paused span so the 600s window excludes it.
    paused_since: u64,
    uid: [u8; MAX_UID],
}

const POD_EMPTY: PodTrack = PodTrack {
    in_use: false,
    seen: false,
    was_terminal: false,
    kill_phase: 0,
    uid_len: 0,
    restarts: 0,
    kill_deadline: 0,
    backoff_until: 0,
    run_started: 0,
    paused_since: 0,
    uid: [0u8; MAX_UID],
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    transitions: u32,
    pods: [PodTrack; MAX_PODS],
}

// ---- helpers ----

/// Build `<prefix><uid>` into a key buffer; returns its length (0 on overflow).
/// Append the `,`-separated escaped list `src` (an `/image-config/` field) to
/// `dst` as space-separated argv tokens, undoing `%25`/`%2C`/`%3B`.
fn append_argv(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let mut p = at;
    let mut i = 0usize;
    while i < src.len() {
        let b = src[i];
        if b == b',' {
            p = append(dst, p, b" ");
            i += 1;
            continue;
        }
        if b == b'%' && i + 2 < src.len() {
            let dec = match &src[i + 1..i + 3] {
                b"25" => Some(b'%'),
                b"2C" => Some(b','),
                b"3B" => Some(b';'),
                _ => None,
            };
            if let Some(d) = dec {
                p = append(dst, p, &[d]);
                i += 3;
                continue;
            }
        }
        p = append(dst, p, &[b]);
        i += 1;
    }
    p
}

fn make_key(dst: &mut [u8], prefix: &[u8], uid: &[u8]) -> usize {
    let n = prefix.len() + uid.len();
    if n > dst.len() {
        return 0;
    }
    dst[..prefix.len()].copy_from_slice(prefix);
    dst[prefix.len()..n].copy_from_slice(uid);
    n
}

/// Parse a decimal u64 (stops at the first non-digit), saturating.
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

/// PUT `key = value` only when it changed (GET-compare). Returns true if written.
unsafe fn put_if_changed(sys: &SyscallTable, key: &[u8], value: &[u8]) -> bool {
    let mut cur = [0u8; MAX_VALUE];
    if let Some(clen) = get_value(sys, key, &mut cur) {
        if &cur[..clen] == value {
            return false;
        }
    }
    put_value(sys, key, value)
}

// ---- per-pod tracking (kill deadlines + restart backoff, all in monotonic ms) ----

fn find_track(s: &State, uid: &[u8]) -> Option<usize> {
    s.pods
        .iter()
        .position(|t| t.in_use && &t.uid[..t.uid_len as usize] == uid)
}

/// Find-or-allocate the pod's track. On allocation, recover the persisted
/// restart count from /pod-lifecycle-status/ (`;restarts=<n>`) — the
/// deadlines themselves are wall-clock ms and forgotten across module
/// restarts. None = table full (grace/backoff degrade to the ungraced path
/// for the overflow pod).
unsafe fn track_for(s: &mut State, sys: &SyscallTable, uid: &[u8]) -> Option<usize> {
    if let Some(i) = find_track(s, uid) {
        return Some(i);
    }
    if uid.len() > MAX_UID {
        return None;
    }
    let i = s.pods.iter().position(|t| !t.in_use)?;
    let t = &mut s.pods[i];
    *t = POD_EMPTY;
    t.in_use = true;
    t.uid_len = uid.len() as u8;
    t.uid[..uid.len()].copy_from_slice(uid);
    let mut key = [0u8; MAX_KEY];
    let kl = make_key(&mut key, POD_STATUS_PREFIX, uid);
    if kl != 0 {
        let mut val = [0u8; MAX_VALUE];
        if let Some(n) = get_value(sys, &key[..kl], &mut val) {
            if let Some(r) = field(&val[..n], b"restarts=") {
                s.pods[i].restarts = parse_u64(r) as u32;
            }
        }
    }
    Some(i)
}

/// PUT /sandbox-kill/<uid> = value ("sig=term" | "sig=kill").
unsafe fn kill_key_put(sys: &SyscallTable, uid: &[u8], value: &[u8]) {
    let mut key = [0u8; MAX_KEY];
    let kl = make_key(&mut key, KILL_PREFIX, uid);
    if kl != 0 {
        put_value(sys, &key[..kl], value);
    }
}

/// DELETE /sandbox-kill/<uid> (kill consumed or pod gone).
unsafe fn kill_key_delete(sys: &SyscallTable, uid: &[u8]) {
    let mut key = [0u8; MAX_KEY];
    let kl = make_key(&mut key, KILL_PREFIX, uid);
    if kl != 0 {
        delete_value(sys, &key[..kl]);
    }
}

/// The backoff delay in ms for failure count `n` (>=1):
/// min(BASE × 2^(n-1), CAP) seconds.
fn backoff_ms(n: u32) -> u64 {
    let sh = (n.saturating_sub(1)).min(15);
    (BACKOFF_BASE_SECS << sh).min(BACKOFF_CAP_SECS) * MS_PER_SEC
}

/// Map the sandbox status doc to a pod phase.
fn pod_phase(status: Option<&[u8]>) -> &'static [u8] {
    let Some(st) = status else {
        return b"Pending"; // no sandbox yet
    };
    let state = field(st, b"state=").unwrap_or(b"");
    let code = field(st, b"code=").unwrap_or(b"0");
    match state {
        b"running" => b"Running",
        // paused is a distinguishable, non-terminal, non-ready phase.
        b"paused" => b"Paused",
        b"created" => b"Pending",
        b"exited" if code == b"0" => b"Succeeded",
        b"exited" => b"Failed",
        b"failed" | b"killed" => b"Failed",
        b"destroyed" => b"Terminating",
        _ => b"Pending",
    }
}

/// Drive one pod spec through the state machine. Returns transitions written.
/// Restart is a status-driven loop on the SAME sandbox id: sandbox_runner frees
/// the slot on `phase=delete`, so exited → delete → destroyed → start re-runs
/// it — now under the exponential backoff (the destroyed → start relaunch
/// defers until the in-state deadline passes).
///
/// ORDERING PIN: `desired == deleted` is evaluated
/// FIRST, before the restart branch, so a pod being killed can never be
/// relaunched by its restartPolicy. The pod-lifecycle E2E asserts this.
unsafe fn reconcile_one(s: &mut State, sys: &SyscallTable, uid: &[u8], spec: &[u8]) -> u32 {
    // Host-process (container) spawn params — nanocloud owns the OCI→params
    // mapping; fluxor's backend reads no bundle. `cmd=` (argv) marks a
    // container pod; `rootfs=`/`iso=` are optional. Forwarded verbatim to the
    // sandbox spec for sandbox_runner.
    let cmd = field(spec, b"cmd=").unwrap_or(b"");
    let rootfs = field(spec, b"rootfs=").unwrap_or(b"");
    let iso = field(spec, b"iso=").unwrap_or(b"");
    // Metal (fmod-graph) selector: a `graph=<template>` spec is a bare-metal
    // pod composed from a flash-fmod template,
    // not a container pod. Its lifecycle EFFECT is the `kubelet` fmod, driven
    // off `/metal-sandboxes/` instead of `/sandboxes/`; this module's decision
    // machinery (restart/backoff/kill/pause + phase projection) is
    // artifact-agnostic and runs unchanged for both. A pod is container XOR
    // graph.
    let graph = field(spec, b"graph=").unwrap_or(b"");
    let desired = field(spec, b"desired=").unwrap_or(b"running");
    let restart = field(spec, b"restart=").unwrap_or(b"never");
    // Optional network mode: `net=own` gives the pod its own network domain
    // with a cni_ipam-leased address (passed through to sandbox_runner, which
    // owns the lease plumbing). Absent = shared host domain.
    let net = field(spec, b"net=").unwrap_or(b"");
    // Optional resource envelope passthrough: `tasks=<n>` → max_tasks on the
    // workload CREATE header (sandbox_runner plumbs it). A pausable pod needs
    // it — the freezer lives in the per-sandbox cgroup the envelope creates.
    let tasks = field(spec, b"tasks=").unwrap_or(b"");
    // terminationGracePeriodSeconds, riding the compact spec as `grace=<secs>`.
    let grace = field(spec, b"grace=")
        .map(parse_u64)
        .unwrap_or(DEFAULT_GRACE_SECS);
    let mut driven = 0u32;

    // Observe the sandbox.
    let mut stbuf = [0u8; MAX_VALUE];
    let mut stkey = [0u8; MAX_KEY];
    let stl = make_key(&mut stkey, SANDBOX_STATUS_PREFIX, uid);
    let status_len = if stl != 0 {
        get_value(sys, &stkey[..stl], &mut stbuf)
    } else {
        None
    };
    let status = status_len.map(|n| &stbuf[..n]);
    let state = status.and_then(|s| field(s, b"state=")).unwrap_or(b"");
    // sandbox_runner labels a clean exit `exited`, a non-zero exit `failed`, a
    // signalled exit `killed` — all three are terminal; only `exited` succeeded.
    // `paused` is deliberately NOT terminal.
    let raw_terminated = state == b"exited" || state == b"failed" || state == b"killed";
    // liveness folding: probe_runner's live=0 verdict (past its failure
    // threshold) is treated as a failed run, driving the same restart+backoff
    // machinery. Folded only while the run is actually running: paused runs
    // are frozen (probes freeze with them), and a relaunched run's stale
    // live=0 is reset by probe_runner on the non-running status transition
    // (destroyed/created) that precedes the new run.
    let mut prbuf = [0u8; 64];
    let mut prkey = [0u8; MAX_KEY];
    let prl = make_key(&mut prkey, PROBE_STATUS_PREFIX, uid);
    let probe_dead = state == b"running"
        && prl != 0
        && get_value(sys, &prkey[..prl], &mut prbuf)
            .map(|n| field(&prbuf[..n], b"live=") == Some(b"0"))
            .unwrap_or(false);
    let terminated = raw_terminated || probe_dead;
    let succeeded = state == b"exited" && !probe_dead;

    let now = dev_millis(sys).max(1);
    let ti = track_for(s, sys, uid);
    if let Some(i) = ti {
        s.pods[i].seen = true;
        if s.pods[i].backoff_until != 0 && now >= s.pods[i].backoff_until {
            s.pods[i].backoff_until = 0; // expired — the relaunch gate lifts
        }
    }

    // Image-backed pod: `image=<repo>[:<tag>]`
    // resolves to a pulled + assembled rootfs. Write the pull trigger
    // (/image-requests/<repo>) if absent, then gate on the assembler's
    // /image-rootfs/<repo> record: not `state=ready` → hold at Pending,
    // projecting NO sandbox (the pull/assembly chain drives it to ready and
    // the store change re-reconciles us). An explicit `rootfs=` wins.
    let image = field(spec, b"image=").unwrap_or(b"");
    let mut img_path_buf = [0u8; 160];
    let mut img_cmd_buf = [0u8; 512];
    let mut rootfs = rootfs;
    // An image-backed pod with no explicit `cmd=` takes its argv from the
    // image's own config blob (Entrypoint ++ Cmd) — the reason `image=nginx`
    // can run at all without the spec restating nginx's entrypoint.
    let mut cmd = cmd;
    if !image.is_empty() && rootfs.is_empty() && desired != b"deleted" {
        // repo[:tag] split (no digest refs in the compact spec).
        let (repo, tag) = match image.iter().position(|&b| b == b':') {
            Some(i) => (&image[..i], &image[i + 1..]),
            None => (image, &b"latest"[..]),
        };
        // Registry repos nest (`nanocloud/hello`), store key names must not:
        // the chain is keyed by the FLATTENED name (`/`→`_`); the request
        // record's `repo=` keeps the true slashed repo for the fetch URL.
        let mut flat = [0u8; 64];
        let fl = repo.len().min(64);
        for (i, &b) in repo[..fl].iter().enumerate() {
            flat[i] = if b == b'/' { b'_' } else { b };
        }
        let name = &flat[..fl];
        let mut rq = [0u8; MAX_KEY];
        let rql = make_key(&mut rq, IMAGE_REQUESTS_PREFIX, name);
        if rql != 0 && !exists(sys, &rq[..rql]) {
            let mut doc = [0u8; 160];
            let mut dl = append(&mut doc, 0, b"repo=");
            dl = append(&mut doc, dl, repo);
            dl = append(&mut doc, dl, b";tag=");
            dl = append(&mut doc, dl, tag);
            put_value(sys, &rq[..rql], &doc[..dl]);
            driven += 1;
        }
        let mut rk = [0u8; MAX_KEY];
        let rkl = make_key(&mut rk, IMAGE_ROOTFS_PREFIX, name);
        let mut rbuf = [0u8; 224];
        let ready_path_len = if rkl != 0 {
            get_value(sys, &rk[..rkl], &mut rbuf).and_then(|n| {
                let v = &rbuf[..n];
                if field(v, b"state=") == Some(b"ready") {
                    field(v, b"path=").map(|p| {
                        let m = p.len().min(img_path_buf.len());
                        img_path_buf[..m].copy_from_slice(&p[..m]);
                        m
                    })
                } else {
                    None
                }
            })
        } else {
            None
        };
        match ready_path_len {
            Some(n) if n > 0 => rootfs = &img_path_buf[..n],
            _ => {
                // Not ready: report Pending, project nothing.
                let mut pkey = [0u8; MAX_KEY];
                let pkl = make_key(&mut pkey, POD_STATUS_PREFIX, uid);
                if pkl != 0 && put_if_changed(sys, &pkey[..pkl], b"phase=Pending") {
                    driven += 1;
                }
                return driven;
            }
        }

        // Argv from the image config. A spec `cmd=` is an explicit override
        // and wins outright; otherwise the config record is REQUIRED — without
        // it there is no argv to spawn, so hold at Pending until the fetcher
        // has landed it (same shape as the rootfs gate above).
        if cmd.is_empty() {
            let mut ck = [0u8; MAX_KEY];
            let ckl = make_key(&mut ck, IMAGE_CONFIG_PREFIX, name);
            let mut cbuf = [0u8; 1024];
            let clen = if ckl != 0 {
                get_value(sys, &ck[..ckl], &mut cbuf)
            } else {
                None
            };
            let built = match clen {
                Some(n) => {
                    let v = &cbuf[..n];
                    let mut q = 0usize;
                    // Entrypoint ++ Cmd, exactly as the OCI runtime spec
                    // composes them. NOTE: the host-process spawn params split
                    // argv on whitespace, so a token containing a space cannot
                    // round-trip yet.
                    if let Some(ep) = field(v, b"entrypoint=") {
                        q = append_argv(&mut img_cmd_buf, q, ep);
                    }
                    if let Some(c) = field(v, b"cmd=") {
                        if q > 0 {
                            q = append(&mut img_cmd_buf, q, b" ");
                        }
                        q = append_argv(&mut img_cmd_buf, q, c);
                    }
                    q
                }
                None => 0,
            };
            if built == 0 {
                let mut pkey = [0u8; MAX_KEY];
                let pkl = make_key(&mut pkey, POD_STATUS_PREFIX, uid);
                if pkl != 0 && put_if_changed(sys, &pkey[..pkl], b"phase=Pending") {
                    driven += 1;
                }
                return driven;
            }
            cmd = &img_cmd_buf[..built];
        }
    }

    // Decide the sandbox phase to project and the pod phase to report.
    let (sb_phase, pod_phase_str): (&[u8], &[u8]) = if desired == b"deleted" {
        // kill-with-grace. `live` = a workload that may still be running
        // (anything not yet terminal/destroyed); no status at all means there
        // is nothing to signal.
        let live = status.is_some() && !terminated && state != b"destroyed";
        if grace == 0 || !live {
            // Immediate (grace=0) or completed kill: the direct delete flow.
            // Consume the kill key once, if one was in flight.
            if let Some(i) = ti {
                if s.pods[i].kill_phase != 0 {
                    kill_key_delete(sys, uid);
                    s.pods[i].kill_phase = 0;
                }
            }
            (b"delete", b"Terminating")
        } else {
            match ti {
                Some(i) if s.pods[i].kill_phase == 0 => {
                    // Phase 1: ask nicely; deadline recorded IN-STATE in step
                    // time. Hold the sandbox (no phase=delete) so the runner
                    // does not DESTROY before the grace window runs.
                    kill_key_put(sys, uid, b"sig=term");
                    s.pods[i].kill_phase = 1;
                    s.pods[i].kill_deadline = now.saturating_add(grace.saturating_mul(MS_PER_SEC));
                    driven += 1;
                    (b"start", b"Terminating")
                }
                Some(i) if s.pods[i].kill_phase == 1 && now >= s.pods[i].kill_deadline => {
                    // Grace expired: escalate, then let the normal delete path
                    // DESTROY (DESTROY force-kills too, so a backend without
                    // real SIGNAL delivery still converges).
                    kill_key_put(sys, uid, b"sig=kill");
                    s.pods[i].kill_phase = 2;
                    driven += 1;
                    (b"delete", b"Terminating")
                }
                Some(i) if s.pods[i].kill_phase == 1 => (b"start", b"Terminating"),
                // kill_phase == 2 (escalated): normal delete flow.
                Some(_) => (b"delete", b"Terminating"),
                // Track table full: degrade to an immediate delete.
                None => (b"delete", b"Terminating"),
            }
        }
    } else if terminated {
        let should_restart = restart == b"always" || (restart == b"onfailure" && !succeeded);
        if should_restart {
            // backoff bookkeeping, on the terminal EDGE only (the terminal
            // status persists until the teardown completes).
            if let Some(i) = ti {
                if !s.pods[i].was_terminal {
                    s.pods[i].was_terminal = true;
                    if succeeded {
                        // A completed run breaks the consecutive-failure streak
                        // (n counts FAILED runs); relaunch immediately.
                        s.pods[i].restarts = 0;
                        s.pods[i].backoff_until = 0;
                    } else {
                        if s.pods[i].run_started != 0
                            && now.saturating_sub(s.pods[i].run_started)
                                >= BACKOFF_RESET_SECS * MS_PER_SEC
                        {
                            s.pods[i].restarts = 0; // survived the reset window
                        }
                        s.pods[i].restarts = s.pods[i].restarts.saturating_add(1);
                        s.pods[i].backoff_until =
                            now.saturating_add(backoff_ms(s.pods[i].restarts));
                    }
                    s.pods[i].run_started = 0;
                    s.pods[i].paused_since = 0;
                }
            }
            (b"delete", b"Restarting") // tear down; destroyed → start re-runs it
        } else if succeeded {
            (b"start", b"Succeeded")
        } else {
            // Includes a live=0 probe verdict on a restart=never pod: the
            // sandbox is left as-is (no kill authority without a restart
            // policy) and the phase reports Failed; it self-corrects to
            // Running if the probe recovers.
            (b"start", b"Failed")
        }
    } else if state == b"destroyed" {
        // Slot freed after a restart teardown → re-create the same id, but only
        // once the backoff deadline (if any) has passed (cleared above on
        // expiry; module_step polls the deadline each step).
        match ti {
            Some(i) if s.pods[i].backoff_until != 0 => (b"delete", b"Restarting"),
            _ => {
                if let Some(i) = ti {
                    s.pods[i].was_terminal = false;
                }
                (b"start", b"Restarting")
            }
        }
    } else {
        if state == b"running" {
            if let Some(i) = ti {
                s.pods[i].was_terminal = false;
                if s.pods[i].run_started == 0 {
                    s.pods[i].run_started = now; // stamp for the reset window
                } else if s.pods[i].paused_since != 0 {
                    // Resumed: the reset clock froze while paused —
                    // shift the baseline forward by the paused span so the
                    // 600s window excludes it.
                    let span = now.saturating_sub(s.pods[i].paused_since);
                    s.pods[i].run_started = s.pods[i].run_started.saturating_add(span);
                }
                s.pods[i].paused_since = 0;
            }
        } else if state == b"paused" {
            // neither terminal (no restart, no n increment — the
            // terminated branch above never sees `paused`) nor success (no
            // backoff reset) nor ready. Freeze the reset-clock baseline.
            if let Some(i) = ti {
                if s.pods[i].run_started != 0 && s.pods[i].paused_since == 0 {
                    s.pods[i].paused_since = now;
                }
            }
        }
        (b"start", pod_phase(status))
    };

    // Project the sandbox spec. A metal pod (`graph=`) projects to
    // `/metal-sandboxes/` (source_ref = the template name) for the kubelet; a
    // container pod projects to `/sandboxes/` (explicit spawn params) for
    // sandbox_runner. Same start/delete phase, net, and tasks envelope — only
    // the artifact fields and the target prefix differ (one surface, the
    // backend resolved by the source artifact's type).
    let metal = !graph.is_empty();
    let target: &[u8] = if metal {
        METAL_SANDBOXES_PREFIX
    } else {
        SANDBOXES_PREFIX
    };
    let mut sbdoc = [0u8; MAX_VALUE];
    let mut d = 0;
    if metal {
        d = append(&mut sbdoc, d, b"graph=");
        d = append(&mut sbdoc, d, graph);
    } else {
        // Explicit host-process spawn params (cmd required; rootfs/iso optional).
        d = append(&mut sbdoc, d, b"cmd=");
        d = append(&mut sbdoc, d, cmd);
        if !rootfs.is_empty() {
            d = append(&mut sbdoc, d, b";rootfs=");
            d = append(&mut sbdoc, d, rootfs);
        }
        if iso == b"1" {
            d = append(&mut sbdoc, d, b";iso=1");
        }
    }
    d = append(&mut sbdoc, d, b";phase=");
    d = append(&mut sbdoc, d, sb_phase);
    if net == b"own" {
        d = append(&mut sbdoc, d, b";net=own");
    }
    if !tasks.is_empty() {
        d = append(&mut sbdoc, d, b";tasks=");
        d = append(&mut sbdoc, d, tasks);
    }
    let mut sbkey = [0u8; MAX_KEY];
    let sbl = make_key(&mut sbkey, target, uid);
    if sbl != 0 && put_if_changed(sys, &sbkey[..sbl], &sbdoc[..d]) {
        driven += 1;
    }

    // Project the pod phase (+ the persisted restart count — appended
    // only when non-zero, so a settled pod's value never churns).
    let restarts = ti.map(|i| s.pods[i].restarts).unwrap_or(0);
    let mut pdoc = [0u8; 48];
    let pl = {
        let mut a = append(&mut pdoc, 0, b"phase=");
        a = append(&mut pdoc, a, pod_phase_str);
        if restarts > 0 {
            a = append(&mut pdoc, a, b";restarts=");
            let mut num = [0u8; 10];
            let nl = write_u32(&mut num, restarts);
            a = append(&mut pdoc, a, &num[..nl]);
        }
        a
    };
    let mut pkey = [0u8; MAX_KEY];
    let pkl = make_key(&mut pkey, POD_STATUS_PREFIX, uid);
    if pkl != 0 && put_if_changed(sys, &pkey[..pkl], &pdoc[..pl]) {
        driven += 1;
        // Record a lifecycle event on the Pod. /pod-specs is uid-scoped (no pod
        // name here), so the event references the uid.
        let (reason, msg): (&[u8], &[u8]) = if pod_phase_str == b"Running" {
            (b"Started", b"Started pod sandbox")
        } else if pod_phase_str == b"Failed" {
            (b"Failed", b"Pod sandbox terminated with an error")
        } else if pod_phase_str == b"Succeeded" {
            (b"Completed", b"Pod sandbox completed")
        } else {
            (b"", b"")
        };
        if !reason.is_empty() {
            emit_event(sys, b"default", b"Pod", uid, reason, msg, 0);
        }
    }
    driven
}

unsafe fn reconcile_all(s: &mut State, sys: &SyscallTable) -> u32 {
    // Mark-and-sweep the track table: reconcile_one marks the tracks whose
    // /pod-specs/ key still exists; the sweep below frees the rest (and drops
    // any orphaned kill key with them).
    for t in s.pods.iter_mut() {
        t.seen = false;
    }
    let mut walk = ListWalk::new(SPECS_PREFIX);
    let mut driven = 0u32;
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
        let mut spec = [0u8; MAX_VALUE];
        spec[..vlen].copy_from_slice(&valbuf[..vlen]);

        let id = last_seg(&keybuf[..klen]);
        let mut idbuf = [0u8; MAX_KEY];
        let ilen = id.len().min(MAX_KEY);
        idbuf[..ilen].copy_from_slice(&id[..ilen]);

        driven += reconcile_one(s, sys, &idbuf[..ilen], &spec[..vlen]);
    }
    for i in 0..MAX_PODS {
        if s.pods[i].in_use && !s.pods[i].seen {
            if s.pods[i].kill_phase != 0 {
                let ul = s.pods[i].uid_len as usize;
                let mut uid = [0u8; MAX_UID];
                uid[..ul].copy_from_slice(&s.pods[i].uid[..ul]);
                kill_key_delete(sys, &uid[..ul]);
            }
            s.pods[i] = POD_EMPTY;
        }
    }
    driven
}

/// True when some in-state deadline (kill escalation or backoff expiry) has
/// passed and needs a reconcile — deadlines generate no store change, so
/// module_step must poll this each step.
unsafe fn deadline_due(s: &State, sys: &SyscallTable) -> bool {
    let now = dev_millis(sys).max(1);
    s.pods.iter().any(|t| {
        t.in_use
            && ((t.kill_phase == 1 && now >= t.kill_deadline)
                || (t.backoff_until != 0 && now >= t.backoff_until))
    })
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
        s.transitions = 0;
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

        // Cold start: resolve the change-sink channel (self-edge allocated), then
        // SUBSCRIBE /pod-specs/ (desired) and /sandbox-status/ (observed) onto it
        // (live-only — the cold-start reconcile below captures pre-existing state
        // via LIST), then reconcile.
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, SPECS_PREFIX, s.sink, 0);
                store_subscribe(sys, SANDBOX_STATUS_PREFIX, s.sink, 0);
                // probe_runner-owned verdicts — read-only here, so this
                // cannot self-wake.
                store_subscribe(sys, PROBE_STATUS_PREFIX, s.sink, 0);
                // Image-backed pods re-reconcile when the assembler flips
                // /image-rootfs/<repo> to ready.
                store_subscribe(sys, IMAGE_ROOTFS_PREFIX, s.sink, 0);
                store_subscribe(sys, IMAGE_CONFIG_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            let d = reconcile_all(s, sys);
            s.transitions = s.transitions.wrapping_add(d);
            return 0;
        }
        // A pushed namespace.change (either prefix) means the input set moved;
        // a passed in-state deadline (kill escalation / backoff expiry) also
        // demands a pass — deadlines generate no store change.
        let moved = s.sink >= 0 && drain_changes(sys, s.sink) > 0;
        if moved || deadline_due(s, sys) {
            let d = reconcile_all(s, sys);
            s.transitions = s.transitions.wrapping_add(d);
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
