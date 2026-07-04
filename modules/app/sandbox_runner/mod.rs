//! Sandbox runner — nanocloud's container runtime as a PIC module. It watches
//! desired sandbox specs in the control-plane store (fluxor `storage.object` /
//! `storage.namespace`) and drives fluxor's generic `workload` contract (0x1A,
//! the host-process backend behind it): reconciler fmods (`pod_lifecycle`)
//! project desired specs in; this module admits/starts/reaps workloads and
//! writes container status out.
//!
//! Division of labor: nanocloud owns the OCI world — image->rootfs
//! materialization AND the mapping of an image/pod spec to explicit spawn
//! params. fluxor's workload backend is a generic host-process isolation
//! primitive (CREATE/START/WAIT/DESTROY) that reads NO bundle/OCI format; this
//! module hands it the params directly.
//!
//! Data model (the compact projection format):
//!
//!   /sandboxes/<id>       = "cmd=<argv>;phase=<create|start|delete>[;rootfs=<path>][;iso=1][;net=own][;tasks=<n>]"
//!   /ipam-lease/<id>      = "<ip>" (cni_ipam; consumed when net=own)
//!   /sandbox-status/<id>  = "state=<created|running|paused|exited|killed|failed|destroyed>;code=<n>"
//!   /sandbox-pause/<id>   = "pause" | "resume"  (in ← orchestrator)
//!
//! Spawn params (composed by `build_spawn_src` into the workload CREATE
//! source-ref — this module owns the OCI→params mapping):
//!   cmd=<argv>     command line, whitespace-split into argv (required)
//!   rootfs=<path>  container root to pivot into (optional)
//!   iso=1          isolate: unshare MOUNT|UTS|IPC|PID (needs root; optional)
//!
//! `tasks=<n>` plumbs the Tier-1 resource envelope's max_tasks into CREATE —
//! the backend only creates a per-sandbox cgroup when the envelope is non-empty,
//! and PAUSE (the cgroup2 freezer) lives in that cgroup, so a pausable sandbox
//! needs it. `state=paused` is LIVE, not terminal: the status loop keeps
//! polling and projects `state=running` again after RESUME.
//!
//! Loop shape (level-triggered + liveness poll):
//!   first step → SUBSCRIBE /sandboxes/ (NOT the root, or our own status
//!                writes would wake us forever) + a cold-start reconcile
//!   each step  → poll WL_WAIT on every live sandbox (in-memory, cheap; a PUT
//!                only on the running->exited transition); on a /sandboxes/
//!                change, DRAIN + reconcile toward the desired phase.
//!
//! workload (0x1A) arg convention: the plain arg buffer is both input and
//! output (CREATE reads the Tier-1 header + explicit spawn params; WAIT writes
//! `[state:u8][code:i32]`). Handle = -1 one-shot for CREATE (class-byte routed
//! on `op >> 8 == 0x1A`); the lifecycle ops carry the FD_TAG_WORKLOAD handle
//! CREATE returned.

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

// The control-plane store, via the standard fluxor storage contracts:
// storage.object (0x14) keyed bytes, storage.namespace (0x13)
// prefix LIST + change SUBSCRIBE (pushed on our self-edge sink).
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

// workload wire opcodes (provider::contract::WORKLOAD = 0x1Axx): host
// isolation is the `workload` contract, whose Linux backend is a host-process
// mechanism. The runner drives the lifecycle
// create -> start -> wait -> destroy. CREATE takes a 68-byte Tier-1 header + a
// source-ref (SOURCE_HOST_PROCESS => explicit host-process spawn params).
const WL_CREATE: u32 = 0x1A00;
const WL_START: u32 = 0x1A01;
const WL_WAIT: u32 = 0x1A02;
/// `SIGNAL` (0x1A03) — deliver a portable signal (`[signo:u32 LE]`,
/// SIG_TERM=1 / SIG_KILL=2) to the workload's process group. Only issued when
/// the backend's CAPS `ops` bitmap advertises real-signal delivery (bit 3);
/// otherwise the kill path stays DESTROY-only.
const WL_SIGNAL: u32 = 0x1A03;
const WL_DESTROY: u32 = 0x1A04;
// Host-process ops (class 0x1B, `abi::platform::linux::host_process`): READ,
// EXEC, and the TTY_* session ops left the native workload contract (0x1A) in
// the D-WORKLOAD-ABI split — they are one backend's implementation vocabulary,
// not native workload semantics. They are `handle = -1` calls; a workload-scoped
// op (READ/EXEC/TTY_OPEN) carries the workload's tagged fd in the leading 4
// bytes of `arg` (i32 LE), and the payload/output follow that prefix. TTY
// session ops (STEP/RESIZE/CLOSE) carry the session id in `arg` unchanged.
/// `READ` (0x1B01) — drain the workload's merged stdout/stderr; returns bytes
/// read (0 = none pending). This is the pod-logs source: the runner appends
/// each drained chunk to `/sandbox-logs/<id>`, which the CLI reads back.
/// `arg` = `[workload_fd:i32 LE][out …]`; return = bytes written after the prefix.
const WL_READ: u32 = 0x1B01;
/// `EXEC` (0x1B02) — run a command inside a started sandbox and capture its
/// output (`kubectl exec`). `arg` = `[workload_fd:i32 LE][command line]`; on
/// return the body (after the fd prefix) holds `[out_len:u32 LE][output]` and
/// the call returns the command's exit code.
const WL_EXEC: u32 = 0x1B02;
/// Command + captured-output scratch for a single WL_EXEC (holds the 4-byte fd
/// prefix + the 4-byte length prefix + up to ~2 KB of output).
const EXEC_CAP: usize = 2048;
// Interactive PTY session ops (kubectl exec -it). TTY_OPEN is workload-scoped
// (fd prefix in arg); STEP/RESIZE/CLOSE carry the session id in the arg. All are
// handle -1 calls.
const WL_TTY_OPEN: u32 = 0x1B03;
const WL_TTY_STEP: u32 = 0x1B04;
const WL_TTY_CLOSE: u32 = 0x1B06;
/// `PAUSE`/`RESUME` (0x1A0B/0x1A0C) — freeze/thaw the workload (Linux backend:
/// cgroup2 freezer on the per-sandbox cgroup). Idempotent; PAUSE on a workload
/// whose cgroup containment failed returns ENOSYS (per-workload gate under the
/// host-level CAPS bit). Gated on CAPS `ops` bit 4.
const WL_PAUSE: u32 = 0x1A0B;
const WL_RESUME: u32 = 0x1A0C;
/// `CAPS` (0x1AFF) — backend capability discovery, fixed prefix
/// `[postures:u8][source_kinds:u8][ops:u16 LE][net:u8]` then an ns directory.
/// Queried once per backend session (cold start); the optional-op paths gate
/// on the `ops` bits (first CAPS consumer).
const WL_CAPS: u32 = 0x1AFF;
/// CAPS `ops` bit 3 — SIGNAL delivers real signals to the process group.
const CAPS_OP_SIGNAL: u16 = 1 << 3;
/// CAPS `ops` bit 4 — the PAUSE/RESUME pair (host-level "can freeze at all";
/// individual workloads may still get ENOSYS when their cgroup containment
/// failed, e.g. an unprivileged run).
const CAPS_OP_PAUSE: u16 = 1 << 4;
/// Portable SIGNAL subset (contract workload.rs).
const SIG_TERM: u32 = 1;
const SIG_KILL: u32 = 2;
/// Per-step pump buffer: `[sid:u32][wlen:u32][stdin]` in / `[rlen:u32][state:u8][code:i32][out]` out.
const TTY_CAP: usize = 4096;
const MAX_TTY: usize = 4;
// 68-byte header: 48 bytes of identity/posture/envelope/section-lengths plus
// the Tier-1 network identity (family/prefix/segment/addr, bytes 48..68). A
// `net=own` sandbox fills the network section from its cni_ipam lease and
// sets workload-level NET_ISO_OWN (byte 18); otherwise the section stays
// zeroed (NET_FAM_NONE, shared host domain).
const WL_CREATE_HEADER: usize = 68;
const POSTURE_SHARED: u8 = 0;
const SOURCE_HOST_PROCESS: u8 = 1;
// WL_WAIT state byte.
const OCI_STATE_RUNNING: u8 = 0;
const OCI_STATE_EXITED: u8 = 1;
const OCI_STATE_SIGNALLED: u8 = 2;
/// Frozen by PAUSE — LIVE, not terminal. The
/// status loop must NOT latch `exited` on it; only 1/2 latch.
const OCI_STATE_PAUSED: u8 = 3;

const SANDBOXES_PREFIX: &[u8] = b"/sandboxes/";
const STATUS_PREFIX: &[u8] = b"/sandbox-status/";
// Two-phase kill: pod_lifecycle (the DECISION)
// writes /sandbox-kill/<id> = "sig=term" | "sig=kill"; this module reacts with
// SIGNAL. The key is pod_lifecycle-owned — the runner never writes it, so
// subscribing to it cannot self-wake. The runner is time-free: the escalation
// deadline lives in pod_lifecycle's step time, never here.
const KILL_PREFIX: &[u8] = b"/sandbox-kill/";
// Pause/resume seam: an orchestrator (no
// CLI/API verb exists yet — today the E2E plays that role) writes
// /sandbox-pause/<id> = "pause" | "resume"; this module reacts with
// PAUSE/RESUME, gated on CAPS ops bit 4. Orchestrator-owned key — the runner
// never writes it, so subscribing cannot self-wake. Deleting the key does NOT
// auto-resume (pause is a runtime posture; write "resume" explicitly).
const PAUSE_PREFIX: &[u8] = b"/sandbox-pause/";
// cni_ipam plumbing (a sandbox spec carrying `net=own` gets its own network
// domain with a leased address): we write the request, cni_ipam grants the
// lease, and the lease + the pool prefix fill the workload CREATE header's
// Tier-1 network identity. Keys/formats owned by cni_ipam.
const IPAM_REQ_PREFIX: &[u8] = b"/ipam-request/";
const IPAM_LEASE_PREFIX: &[u8] = b"/ipam-lease/";
const IPAM_POOL_KEY: &[u8] = b"/ipam-pool";
// workload CREATE header net fields (contract `workload.rs`).
const WL_NET_ISO_OWN: u8 = 1;
const WL_NET_FAM_IPV4: u8 = 4;
const IPAM_DEFAULT_PREFIX: u8 = 24;
// Merged stdout/stderr of each sandbox, drained from the workload READ op and
// kept as a bounded tail ring (last LOG_CAP bytes). The CLI `logs` verb reads it.
const LOGS_PREFIX: &[u8] = b"/sandbox-logs/";
const LOG_CAP: usize = 1024;
// Exec request/response keys (the probe path): the provider puts
// /sandbox-exec/<id>/<reqid> = cmd; this module execs and puts
// /sandbox-exec-result/<id>/<reqid> = <exit-code>; the provider reads the
// result and deletes both keys (so no delete op is needed here, and requests
// don't accumulate).
const EXEC_PREFIX: &[u8] = b"/sandbox-exec/";
const EXEC_RESULT_PREFIX: &[u8] = b"/sandbox-exec-result/";
// Captured stdout/stderr of an exec request, keyed by the same <id>/<reqid> tail.
const EXEC_OUTPUT_PREFIX: &[u8] = b"/sandbox-exec-output/";
// Interactive PTY session seam. The client writes `/tty/<id>/<sid>/ctl`
// (`open;rows=;cols=;cmd=` then `close`) + `/tty/<id>/<sid>/in` (stdin); the
// runner streams the PTY to `/tty/<id>/<sid>/out` and posts
// `/tty/<id>/<sid>/status` (`open` / `exited;code=N`).
const TTY_PREFIX: &[u8] = b"/tty/";

/// Bounded work-buffer sizes. A cluster larger than these reconciles partially
/// rather than corrupting — acceptable for the single-node target.
const MAX_KEY: usize = 128;
/// Sized for image-assembly job specs: the
/// assembler's cmd carries the script + rootfs/blob paths + one 64-hex digest
/// PER LAYER, so a /sandboxes/ value can run to a few KB. A truncated read
/// silently drops trailing fields (`;phase=start` — a created-never-started
/// sandbox), so this must be ≥ the assembler's own value cap (4096).
const MAX_VALUE: usize = 4096;
const LIST_BUF: usize = 2048;
const MAX_ID: usize = 96;
const MAX_SB: usize = 16;

/// One tracked sandbox: its id, the live oci handle, and lifecycle flags.
#[repr(C)]
#[derive(Clone, Copy)]
struct Sandbox {
    in_use: bool,
    started: bool,
    exited: bool,
    /// Highest /sandbox-kill/ escalation delivered: 0=none, 1=term, 2=kill.
    /// Dedupes SIGNAL on repeated reconciles of the same key value.
    sig_sent: u8,
    /// Last /sandbox-pause/ intent applied: 0=none, 1=pause, 2=resume.
    /// Dedupes PAUSE/RESUME on repeated reconciles of the same key value.
    pause_sent: u8,
    /// status-projection latch for the LIVE paused state: true after WAIT
    /// reported STATE_PAUSED (state=paused written); cleared — and
    /// state=running re-written — when WAIT reports RUNNING again.
    paused: bool,
    id_len: u8,
    handle: i32,
    id: [u8; MAX_ID],
}

const SB_EMPTY: Sandbox = Sandbox {
    in_use: false,
    started: false,
    exited: false,
    sig_sent: 0,
    pause_sent: 0,
    paused: false,
    id_len: 0,
    handle: -1,
    id: [0u8; MAX_ID],
};

/// One tracked interactive PTY session: the backend session id + the store-key
/// tail `<id>/<sid>` that builds its in/out/status keys.
#[repr(C)]
#[derive(Clone, Copy)]
struct TtyTrack {
    in_use: bool,
    backend_sid: i32,
    tail_len: u8,
    tail: [u8; 128],
}

const TTY_EMPTY: TtyTrack = TtyTrack {
    in_use: false,
    backend_sid: -1,
    tail_len: 0,
    tail: [0u8; 128],
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    /// Change-sink channel (store SUBSCRIBE pushes namespace.change here).
    sink: i32,
    /// 0 until the prefix SUBSCRIBEs + cold-start reconcile have run.
    subscribed: u8,
    /// The backend CAPS `ops` bitmap, queried once at cold start. 0 = the
    /// backend predates CAPS (ENOSYS) ⇒ every gated optional path stays off.
    caps_ops: u16,
    /// Count of lifecycle transitions driven — the observable of progress.
    transitions: u32,
    sandboxes: [Sandbox; MAX_SB],
    /// Active interactive PTY sessions.
    ttys: [TtyTrack; MAX_TTY],
}

// ---- storage.object / storage.namespace ops ----

// ---- workload ops (Tier-1 header on CREATE; handle-based lifecycle) ----

/// WL_CREATE — admit + instantiate a host-process workload. Builds the 68-byte
/// Tier-1 header (identity = the sandbox id, posture SHARED for a null sandbox,
/// source kind BUNDLE) + the explicit spawn-params `source` (composed by
/// `build_spawn_src`) as the source-ref. `net` = the leased
/// (address, prefix) for a `net=own` sandbox — filled into the header's
/// network-identity fields with workload-level NET_ISO_OWN, so the backend
/// gives the workload its own netns + veth carrying that address; None leaves
/// the network section zeroed (NET_FAM_NONE, shared host domain). Returns the
/// tagged WorkloadHandle (>=0) or a negative errno.
unsafe fn wl_create(
    sys: &SyscallTable,
    _id: &[u8],
    source: &[u8],
    net: Option<([u8; 4], u8)>,
    max_tasks: u32,
) -> i32 {
    let mut arg = [0u8; WL_CREATE_HEADER + MAX_VALUE];
    if WL_CREATE_HEADER + source.len() > arg.len() {
        return -22; // EINVAL
    }
    // identity[0..16] = the null/system uid → owner 0. nanocloud edge is a
    // single-tenant node, which "runs entirely as owner 0"; the
    // workload backend resolves the null uid to the system owner. (A multi-tenant
    // node would carry the pod UID here, plan-admitted before CREATE.)
    arg[16] = POSTURE_SHARED;
    arg[17] = SOURCE_HOST_PROCESS;
    // Resource envelope: max_tasks (bytes 32..36 → pids.max). Non-zero makes
    // the backend create a per-sandbox cgroup — the PAUSE freezer's home; an
    // envelope-less workload has no cgroup and PAUSE returns ENOSYS.
    arg[32..36].copy_from_slice(&max_tasks.to_le_bytes());
    arg[40..42].copy_from_slice(&(source.len() as u16).to_le_bytes()); // source_ref_len
                                                                       // endpoint_count (42..44) and options_len (44..48) stay 0.
    if let Some((addr, prefix)) = net {
        arg[18] = WL_NET_ISO_OWN;
        arg[48] = WL_NET_FAM_IPV4;
        arg[49] = prefix;
        arg[52..56].copy_from_slice(&addr);
    }
    arg[WL_CREATE_HEADER..WL_CREATE_HEADER + source.len()].copy_from_slice(source);
    (sys.provider_call)(
        -1,
        WL_CREATE,
        arg.as_mut_ptr(),
        WL_CREATE_HEADER + source.len(),
    )
}

/// WL_START releases the workload from the create/start barrier.
unsafe fn oci_start(sys: &SyscallTable, handle: i32) -> i32 {
    let mut scratch = [0u8; 1];
    (sys.provider_call)(handle, WL_START, scratch.as_mut_ptr(), 0)
}

/// WL_WAIT (non-blocking); Some((state, code)) or None on error. Wire is
/// `[state:u8][code:i32 LE]`.
unsafe fn oci_wait(sys: &SyscallTable, handle: i32) -> Option<(u8, i32)> {
    let mut buf = [0u8; 5];
    let rc = (sys.provider_call)(handle, WL_WAIT, buf.as_mut_ptr(), buf.len());
    if rc != 5 {
        return None;
    }
    Some((buf[0], i32::from_le_bytes(buf[1..5].try_into().unwrap())))
}

/// WL_DESTROY stops (graceful → forced), reaps, and frees the workload.
unsafe fn oci_destroy(sys: &SyscallTable, handle: i32) -> i32 {
    let mut scratch = [0u8; 1];
    (sys.provider_call)(handle, WL_DESTROY, scratch.as_mut_ptr(), 0)
}

/// WL_SIGNAL — deliver a portable signal (`[signo:u32 LE]`) to the workload.
unsafe fn wl_signal(sys: &SyscallTable, handle: i32, signo: u32) -> i32 {
    let mut arg = signo.to_le_bytes();
    (sys.provider_call)(handle, WL_SIGNAL, arg.as_mut_ptr(), arg.len())
}

/// WL_PAUSE / WL_RESUME — no arg, resp `[status:u8]`; >=0 on success.
unsafe fn wl_pause_op(sys: &SyscallTable, handle: i32, op: u32) -> i32 {
    let mut scratch = [0u8; 1];
    (sys.provider_call)(handle, op, scratch.as_mut_ptr(), scratch.len())
}

/// WL_CAPS — one-shot backend discovery; returns the `ops` bitmap out of the
/// fixed prefix `[postures:u8][source_kinds:u8][ops:u16 LE][net:u8]`, or 0 when
/// the backend rejects the op (absent ⇒ ENOSYS ⇒ nothing optional advertised).
unsafe fn wl_caps_ops(sys: &SyscallTable) -> u16 {
    let mut buf = [0u8; 64];
    let rc = (sys.provider_call)(-1, WL_CAPS, buf.as_mut_ptr(), buf.len());
    if rc < 5 {
        0
    } else {
        u16::from_le_bytes([buf[2], buf[3]])
    }
}

/// Exec-into-sandbox via the workload EXEC op: run `cmd` inside the started
/// sandbox and capture its merged stdout/stderr. `buf` (EXEC_CAP) is passed as
/// both the command input and the output scratch — on return it holds
/// `[out_len:u32 LE][output]`. Returns `(exit_code, out_len)`; the output bytes
/// are `buf[4..4 + out_len]`. Synchronous (the backend forks + waits).
unsafe fn wl_exec(sys: &SyscallTable, handle: i32, cmd: &[u8], buf: &mut [u8]) -> (i32, usize) {
    // host_process EXEC (0x1B): arg = [workload_fd:i32 LE][command line]; on
    // return the body after the fd prefix holds [out_len:u32 LE][output], so the
    // captured output starts at buf[8]. Return = the command's exit code.
    if buf.len() < 8 || 4 + cmd.len() + 1 > buf.len() {
        return (255, 0);
    }
    buf[0..4].copy_from_slice(&handle.to_le_bytes());
    buf[4..4 + cmd.len()].copy_from_slice(cmd);
    buf[4 + cmd.len()] = 0; // NUL-terminate the command for the backend parser
    let code = (sys.provider_call)(-1, WL_EXEC, buf.as_mut_ptr(), buf.len());
    let out_len = (u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize)
        .min(buf.len().saturating_sub(8));
    (code, out_len)
}

// ---- compact-format parsing (byte scanning; the format is ours) ----

/// Compose the explicit host-process spawn-params source section into `buf`:
///   [isolate:u8][rootfs_len:u16 LE][rootfs bytes][argv: rest, NUL-separated].
/// `cmd` is split on ASCII whitespace into argv tokens. Returns the filled
/// slice, or None if it does not fit or argv is empty.
fn build_spawn_src<'a>(
    buf: &'a mut [u8],
    cmd: &[u8],
    rootfs: &[u8],
    isolate: bool,
) -> Option<&'a [u8]> {
    if rootfs.len() > u16::MAX as usize {
        return None;
    }
    let mut p = 0usize;
    let mut put = |b: u8, p: &mut usize| -> bool {
        if *p >= buf.len() {
            return false;
        }
        buf[*p] = b;
        *p += 1;
        true
    };
    if !put(if isolate { 1 } else { 0 }, &mut p) {
        return None;
    }
    let rl = (rootfs.len() as u16).to_le_bytes();
    if !put(rl[0], &mut p) || !put(rl[1], &mut p) {
        return None;
    }
    for &b in rootfs {
        if !put(b, &mut p) {
            return None;
        }
    }
    // argv: NUL-separated tokens (whitespace-split), no trailing NUL.
    let mut wrote_any = false;
    let mut first = true;
    let mut in_tok = false;
    for &b in cmd {
        if b == b' ' || b == b'\t' || b == b'\n' {
            in_tok = false;
            continue;
        }
        if !in_tok {
            if !first && !put(0, &mut p) {
                return None;
            }
            first = false;
            in_tok = true;
        }
        if !put(b, &mut p) {
            return None;
        }
        wrote_any = true;
    }
    if !wrote_any {
        return None; // empty argv
    }
    Some(&buf[..p])
}

/// Parse a leading run of ASCII digits as a u32 (saturating).
fn parse_u32(b: &[u8]) -> u32 {
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add((c - b'0') as u64);
        if v > u32::MAX as u64 {
            return u32::MAX;
        }
    }
    v as u32
}

/// Parse a dotted-quad IPv4; ignores anything after the fourth octet.
fn parse_ipv4(v: &[u8]) -> Option<[u8; 4]> {
    let mut out = [0u8; 4];
    let mut octet: u32 = 0;
    let mut digits = 0;
    let mut idx = 0;
    for &b in v {
        if b.is_ascii_digit() {
            octet = octet * 10 + (b - b'0') as u32;
            digits += 1;
            if octet > 255 || digits > 3 {
                return None;
            }
        } else if b == b'.' {
            if digits == 0 || idx >= 3 {
                return None;
            }
            out[idx] = octet as u8;
            idx += 1;
            octet = 0;
            digits = 0;
        } else {
            break;
        }
    }
    if idx != 3 || digits == 0 {
        return None;
    }
    out[3] = octet as u8;
    Some(out)
}

/// The pool prefix length from /ipam-pool (`cidr=<a.b.c.d>/<prefix>`, the
/// same key cni_ipam allocates from), or the default. One source of truth —
/// the lease value stays a bare address.
unsafe fn ipam_pool_prefix(sys: &SyscallTable) -> u8 {
    let mut buf = [0u8; MAX_VALUE];
    let Some(n) = get_value(sys, IPAM_POOL_KEY, &mut buf) else {
        return IPAM_DEFAULT_PREFIX;
    };
    let v = &buf[..n];
    let Some(i) = v.windows(5).position(|w| w == b"cidr=") else {
        return IPAM_DEFAULT_PREFIX;
    };
    let Some(s) = v[i..].iter().position(|&b| b == b'/') else {
        return IPAM_DEFAULT_PREFIX;
    };
    let mut prefix: u32 = 0;
    for &b in &v[i + s + 1..] {
        if b.is_ascii_digit() {
            prefix = prefix * 10 + (b - b'0') as u32;
        } else {
            break;
        }
    }
    if prefix == 0 || prefix > 30 {
        IPAM_DEFAULT_PREFIX
    } else {
        prefix as u8
    }
}

/// Resolve a `net=own` sandbox's leased identity: ensure /ipam-request/<id>
/// exists, and return the (address, prefix) once cni_ipam has granted
/// /ipam-lease/<id>. None = pending — the lease SUBSCRIBE re-wakes reconcile
/// when the grant lands.
unsafe fn ipam_identity(sys: &SyscallTable, id: &[u8]) -> Option<([u8; 4], u8)> {
    let mut lkey = [0u8; MAX_KEY];
    let llen = IPAM_LEASE_PREFIX.len() + id.len();
    if llen > lkey.len() {
        return None;
    }
    lkey[..IPAM_LEASE_PREFIX.len()].copy_from_slice(IPAM_LEASE_PREFIX);
    lkey[IPAM_LEASE_PREFIX.len()..llen].copy_from_slice(id);

    let mut val = [0u8; MAX_VALUE];
    if let Some(n) = get_value(sys, &lkey[..llen], &mut val) {
        let addr = parse_ipv4(&val[..n])?;
        return Some((addr, ipam_pool_prefix(sys)));
    }

    // No lease yet — make sure the request exists (idempotent: cni_ipam never
    // reassigns an existing lease, and re-putting an empty request is a no-op
    // for it).
    let mut rkey = [0u8; MAX_KEY];
    let rlen = IPAM_REQ_PREFIX.len() + id.len();
    if rlen <= rkey.len() {
        rkey[..IPAM_REQ_PREFIX.len()].copy_from_slice(IPAM_REQ_PREFIX);
        rkey[IPAM_REQ_PREFIX.len()..rlen].copy_from_slice(id);
        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_none() {
            put_value(sys, &rkey[..rlen], b"");
        }
    }
    None
}

/// Format a signed int as decimal into `buf`; returns the byte length.
fn write_int(buf: &mut [u8], n: i32) -> usize {
    if buf.is_empty() {
        return 0;
    }
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }
    let neg = n < 0;
    // i32::MIN handled via i64 to avoid overflow on negation.
    let mut v = (n as i64).unsigned_abs();
    let mut tmp = [0u8; 12];
    let mut i = 0;
    while v > 0 && i < tmp.len() {
        tmp[i] = b'0' + (v % 10) as u8;
        v /= 10;
        i += 1;
    }
    let mut p = 0;
    if neg && p < buf.len() {
        buf[p] = b'-';
        p += 1;
    }
    while i > 0 && p < buf.len() {
        i -= 1;
        buf[p] = tmp[i];
        p += 1;
    }
    p
}

/// Build `state=<label>;code=<n>` into `buf`; returns the length.
fn status_doc(buf: &mut [u8], label: &[u8], code: i32) -> usize {
    let mut p = 0;
    let prefix = b"state=";
    buf[p..p + prefix.len()].copy_from_slice(prefix);
    p += prefix.len();
    buf[p..p + label.len()].copy_from_slice(label);
    p += label.len();
    let mid = b";code=";
    buf[p..p + mid.len()].copy_from_slice(mid);
    p += mid.len();
    p + write_int(&mut buf[p..], code)
}

/// Write /sandbox-status/<id> = state=<label>;code=<n>.
unsafe fn put_status(sys: &SyscallTable, id: &[u8], label: &[u8], code: i32) {
    let mut key = [0u8; MAX_KEY];
    let klen = STATUS_PREFIX.len() + id.len();
    if klen > key.len() {
        return;
    }
    key[..STATUS_PREFIX.len()].copy_from_slice(STATUS_PREFIX);
    key[STATUS_PREFIX.len()..klen].copy_from_slice(id);
    let mut doc = [0u8; 64];
    let dlen = status_doc(&mut doc, label, code);
    put_value(sys, &key[..klen], &doc[..dlen]);
}

// ---- sandbox table ----

fn find_slot(s: &State, id: &[u8]) -> Option<usize> {
    s.sandboxes
        .iter()
        .position(|sb| sb.in_use && &sb.id[..sb.id_len as usize] == id)
}

fn alloc_slot(s: &mut State, id: &[u8]) -> Option<usize> {
    if id.len() > MAX_ID {
        return None;
    }
    let idx = s.sandboxes.iter().position(|sb| !sb.in_use)?;
    let sb = &mut s.sandboxes[idx];
    *sb = SB_EMPTY;
    sb.in_use = true;
    sb.id_len = id.len() as u8;
    sb.id[..id.len()].copy_from_slice(id);
    Some(idx)
}

/// Reconcile one desired sandbox toward its phase. Returns the count of
/// lifecycle transitions driven.
unsafe fn reconcile_one(s: &mut State, sys: &SyscallTable, id: &[u8], value: &[u8]) -> u32 {
    let phase = field(value, b"phase=").unwrap_or(b"create");
    let mut driven = 0u32;

    // Teardown short-circuits create/start.
    if phase == b"delete" {
        if let Some(idx) = find_slot(s, id) {
            let handle = s.sandboxes[idx].handle;
            oci_destroy(sys, handle);
            s.sandboxes[idx] = SB_EMPTY;
            put_status(sys, id, b"destroyed", 0);
            driven += 1;
        }
        return driven;
    }

    // Ensure created (phase create or start). A CREATE failure surfaces as
    // status=failed;code=<errno> and does NOT hold a slot.
    let idx = match find_slot(s, id) {
        Some(i) => i,
        None => {
            // Compose the EXPLICIT host-process spawn params (fluxor's backend
            // is a generic isolation primitive — it reads no bundle/OCI format):
            //   [isolate:u8][rootfs_len:u16 LE][rootfs][argv: NUL-separated].
            // The image materializer puts these in the spec; nanocloud owns the
            // OCI→params mapping. `cmd=` is required (whitespace-split argv).
            let Some(cmd) = field(value, b"cmd=") else {
                return driven;
            };
            let rootfs = field(value, b"rootfs=").unwrap_or(b"");
            let isolate = field(value, b"iso=") == Some(b"1".as_ref());
            let mut src = [0u8; MAX_VALUE];
            let src = match build_spawn_src(&mut src, cmd, rootfs, isolate) {
                Some(s) => s,
                None => {
                    put_status(sys, id, b"failed", -22); // EINVAL — params too large
                    return driven;
                }
            };
            // `net=own` needs a cni_ipam lease before CREATE — defer (no
            // status write, no slot) until the lease SUBSCRIBE wakes us with
            // the grant. Never create with silently-shared networking.
            let net = if field(value, b"net=") == Some(b"own".as_ref()) {
                match ipam_identity(sys, id) {
                    Some(n) => Some(n),
                    None => return driven,
                }
            } else {
                None
            };
            // Optional envelope: tasks=<n> → max_tasks (a pausable sandbox
            // needs the cgroup the envelope creates).
            let max_tasks = field(value, b"tasks=").map(parse_u32).unwrap_or(0);
            let handle = wl_create(sys, id, src, net, max_tasks);
            if handle < 0 {
                put_status(sys, id, b"failed", handle);
                return driven;
            }
            let Some(i) = alloc_slot(s, id) else {
                // No room — destroy the orphan so we don't leak the child.
                oci_destroy(sys, handle);
                put_status(sys, id, b"failed", -12); // ENOMEM
                return driven;
            };
            s.sandboxes[i].handle = handle;
            put_status(sys, id, b"created", 0);
            driven += 1;
            i
        }
    };

    // Start when desired and not yet started.
    if phase == b"start" && !s.sandboxes[idx].started {
        let handle = s.sandboxes[idx].handle;
        let rc = oci_start(sys, handle);
        if rc >= 0 {
            s.sandboxes[idx].started = true;
            put_status(sys, id, b"running", 0);
            driven += 1;
        }
    }
    driven
}

/// Full pass over desired /sandboxes/ specs. Returns transitions driven.
unsafe fn reconcile_all(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(SANDBOXES_PREFIX);
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
        let mut val = [0u8; MAX_VALUE];
        val[..vlen].copy_from_slice(&valbuf[..vlen]);

        // Copy the id out of keybuf so reconcile_one can borrow &mut State.
        let id_slice = last_seg(&keybuf[..klen]);
        let ilen = id_slice.len().min(MAX_ID);
        let mut idbuf = [0u8; MAX_ID];
        idbuf[..ilen].copy_from_slice(&id_slice[..ilen]);

        driven += reconcile_one(s, sys, &idbuf[..ilen], &val[..vlen]);
    }
    driven
}

/// Poll OCI_WAIT on every live, started sandbox; write terminal status once on
/// the running->terminal transition. Returns transitions driven. Cheap: it
/// touches only the in-memory table and PUTs only on a state change.
unsafe fn poll_liveness(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut driven = 0u32;
    for idx in 0..MAX_SB {
        let sb = s.sandboxes[idx];
        if !sb.in_use || !sb.started || sb.exited {
            continue;
        }
        let Some((state, code)) = oci_wait(sys, sb.handle) else {
            continue;
        };
        let id_len = sb.id_len as usize;
        let mut idbuf = [0u8; MAX_ID];
        idbuf[..id_len].copy_from_slice(&sb.id[..id_len]);
        if state == OCI_STATE_RUNNING {
            // Back from paused (RESUME reflected within one WAIT poll):
            // re-project state=running once.
            if sb.paused {
                put_status(sys, &idbuf[..id_len], b"running", 0);
                s.sandboxes[idx].paused = false;
                driven += 1;
            }
            continue;
        }
        // PAUSED is LIVE — project state=paused once, keep polling,
        // never latch `exited` (only 1/2 latch terminal below).
        if state == OCI_STATE_PAUSED {
            if !sb.paused {
                put_status(sys, &idbuf[..id_len], b"paused", 0);
                s.sandboxes[idx].paused = true;
                driven += 1;
            }
            continue;
        }
        let label: &[u8] = match state {
            OCI_STATE_EXITED if code == 0 => b"exited",
            OCI_STATE_EXITED => b"failed",
            OCI_STATE_SIGNALLED => b"killed",
            _ => b"failed",
        };
        put_status(sys, &idbuf[..id_len], label, code);
        s.sandboxes[idx].exited = true;
        s.sandboxes[idx].paused = false;
        driven += 1;
    }
    driven
}

/// Append `new` to the store object at `key`, keeping only the last `LOG_CAP`
/// bytes (a bounded tail ring — the tail is what `kubectl logs`/interactive
/// output want, and an unbounded object would grow without limit). RMW.
unsafe fn append_ring(sys: &SyscallTable, key: &[u8], new: &[u8]) {
    if new.is_empty() {
        return;
    }
    let mut cur = [0u8; LOG_CAP];
    let curlen = get_value(sys, key, &mut cur).unwrap_or(0).min(LOG_CAP);
    let mut buf = [0u8; LOG_CAP];
    if curlen + new.len() <= LOG_CAP {
        buf[..curlen].copy_from_slice(&cur[..curlen]);
        buf[curlen..curlen + new.len()].copy_from_slice(new);
        put_value(sys, key, &buf[..curlen + new.len()]);
    } else if new.len() >= LOG_CAP {
        // The new chunk alone fills the ring — keep its tail.
        buf.copy_from_slice(&new[new.len() - LOG_CAP..]);
        put_value(sys, key, &buf);
    } else {
        // Keep the tail of the existing content + all of the new chunk.
        let keep_cur = LOG_CAP - new.len();
        buf[..keep_cur].copy_from_slice(&cur[curlen - keep_cur..curlen]);
        buf[keep_cur..].copy_from_slice(new);
        put_value(sys, key, &buf);
    }
}

/// Append `new` to `/sandbox-logs/<id>` (a bounded tail ring).
unsafe fn append_log(sys: &SyscallTable, id: &[u8], new: &[u8]) {
    let mut key = [0u8; MAX_KEY];
    let klen = LOGS_PREFIX.len() + id.len();
    if klen > key.len() {
        return;
    }
    key[..LOGS_PREFIX.len()].copy_from_slice(LOGS_PREFIX);
    key[LOGS_PREFIX.len()..klen].copy_from_slice(id);
    append_ring(sys, &key[..klen], new);
}

/// Drain each started sandbox's merged stdout/stderr (workload READ) and append
/// it to `/sandbox-logs/<id>`. Cheap: READ returns 0 when nothing is pending.
unsafe fn poll_logs(s: &State, sys: &SyscallTable) {
    for idx in 0..MAX_SB {
        let sb = s.sandboxes[idx];
        if !sb.in_use || !sb.started {
            continue;
        }
        // host_process READ (0x1B): [workload_fd:i32 LE][out …]; the drained
        // bytes land after the 4-byte fd prefix, and the call returns their length.
        let mut arg = [0u8; 4 + 512];
        arg[0..4].copy_from_slice(&sb.handle.to_le_bytes());
        let n = (sys.provider_call)(-1, WL_READ, arg.as_mut_ptr(), arg.len());
        if n > 0 {
            let id_len = sb.id_len as usize;
            let n = (n as usize).min(arg.len() - 4);
            append_log(sys, &sb.id[..id_len], &arg[4..4 + n]);
        }
    }
}

/// Service exec requests: for each /sandbox-exec/<id>/<reqid> without a result
/// yet, exec the command inside sandbox <id> and write /sandbox-exec-result/
/// <id>/<reqid> = <exit-code>. The provider deletes both keys once it reads the
/// result. Exec is synchronous (waitpid), so this briefly blocks the step — fine
/// for probes. Returns the count of requests serviced.
unsafe fn reconcile_execs(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(EXEC_PREFIX);
    let mut driven = 0u32;
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];

        // The "<id>/<reqid>" tail after the prefix; result key swaps prefixes.
        if klen <= EXEC_PREFIX.len() {
            continue;
        }
        let tail = &key[EXEC_PREFIX.len()..];
        let mut rkey = [0u8; MAX_KEY];
        let rlen = EXEC_RESULT_PREFIX.len() + tail.len();
        if rlen > rkey.len() {
            continue;
        }
        rkey[..EXEC_RESULT_PREFIX.len()].copy_from_slice(EXEC_RESULT_PREFIX);
        rkey[EXEC_RESULT_PREFIX.len()..rlen].copy_from_slice(tail);

        // Already serviced? (the provider hasn't deleted it yet.)
        let mut probe = [0u8; MAX_VALUE];
        if get_value(sys, &rkey[..rlen], &mut probe).is_some() {
            continue;
        }

        // <id> is the tail up to the first '/'.
        let id = match tail.iter().position(|&b| b == b'/') {
            Some(i) => &tail[..i],
            None => tail,
        };

        // Resolve the sandbox; a request for an unknown/gone sandbox fails the
        // probe deterministically (255) rather than hanging the caller. On a
        // live sandbox, run the command via the workload EXEC op and capture
        // its output.
        let mut exec_buf = [0u8; EXEC_CAP];
        let mut out_len = 0usize;
        let code = match find_slot(s, id) {
            Some(idx) if s.sandboxes[idx].started && !s.sandboxes[idx].exited => {
                let handle = s.sandboxes[idx].handle;
                let mut cmd = [0u8; MAX_VALUE];
                match get_value(sys, key, &mut cmd) {
                    Some(clen) => {
                        let (c, ol) = wl_exec(sys, handle, &cmd[..clen], &mut exec_buf);
                        out_len = ol;
                        c
                    }
                    None => 255,
                }
            }
            _ => 255,
        };

        let mut doc = [0u8; 12];
        let dlen = write_int(&mut doc, code);
        put_value(sys, &rkey[..rlen], &doc[..dlen]);

        // Publish the captured output at /sandbox-exec-output/<id>/<reqid>.
        if out_len > 0 {
            let mut okey = [0u8; MAX_KEY];
            let olen = EXEC_OUTPUT_PREFIX.len() + tail.len();
            if olen <= okey.len() {
                okey[..EXEC_OUTPUT_PREFIX.len()].copy_from_slice(EXEC_OUTPUT_PREFIX);
                okey[EXEC_OUTPUT_PREFIX.len()..olen].copy_from_slice(tail);
                // Output follows the [workload_fd][out_len] prefixes → starts at buf[8].
                put_value(sys, &okey[..olen], &exec_buf[8..8 + out_len]);
            }
        }
        driven += 1;
    }
    driven
}

/// React to /sandbox-kill/<id> = "sig=term" | "sig=kill" (written by
/// pod_lifecycle as the kill-with-grace decision): deliver SIGNAL
/// once per escalation level (term=1 < kill=2; `sig_sent` dedupes). Time-free:
/// the runner never computes deadlines — it only reacts to the key's current
/// value. Gated on CAPS `ops` bit 3; without it the kill path stays
/// DESTROY-only (the caller skips us entirely).
unsafe fn reconcile_kills(s: &mut State, sys: &SyscallTable) {
    let mut walk = ListWalk::new(KILL_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        if klen <= KILL_PREFIX.len() {
            continue;
        }
        let id = &key[KILL_PREFIX.len()..];

        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut val) else {
            continue;
        };
        let want: u8 = match &val[..vlen] {
            b"sig=term" => 1,
            b"sig=kill" => 2,
            _ => continue,
        };
        let Some(idx) = find_slot(s, id) else {
            continue; // unknown/already-reaped sandbox — nothing to signal
        };
        let sb = s.sandboxes[idx];
        if !sb.started || sb.exited || sb.sig_sent >= want {
            continue;
        }
        let signo = if want == 2 { SIG_KILL } else { SIG_TERM };
        if wl_signal(sys, sb.handle, signo) >= 0 {
            s.sandboxes[idx].sig_sent = want;
        }
    }
}

/// React to /sandbox-pause/<id> = "pause" | "resume"
/// (orchestrator-owned): issue PAUSE/RESUME once per intent
/// (`pause_sent` dedupes repeated reconciles of the same value; the ops are
/// idempotent anyway). Gated on CAPS `ops` bit 4 by the caller. A per-workload
/// ENOSYS (cgroup containment failed — e.g. unprivileged run) leaves the
/// status projection untouched: the workload simply stays running.
unsafe fn reconcile_pauses(s: &mut State, sys: &SyscallTable) {
    let mut walk = ListWalk::new(PAUSE_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        if klen <= PAUSE_PREFIX.len() {
            continue;
        }
        let id = &key[PAUSE_PREFIX.len()..];

        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut val) else {
            continue;
        };
        let want: u8 = match &val[..vlen] {
            b"pause" => 1,
            b"resume" => 2,
            _ => continue,
        };
        let Some(idx) = find_slot(s, id) else {
            continue; // unknown/already-reaped sandbox — nothing to freeze
        };
        let sb = s.sandboxes[idx];
        if !sb.started || sb.exited || sb.pause_sent == want {
            continue;
        }
        let op = if want == 1 { WL_PAUSE } else { WL_RESUME };
        if wl_pause_op(sys, sb.handle, op) >= 0 {
            s.sandboxes[idx].pause_sent = want;
        }
    }
}

// ---- interactive PTY sessions ----

/// Parse a decimal `u16` (stops at the first non-digit), saturating.
fn parse_u16(b: &[u8]) -> u16 {
    let mut v: u32 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10) + (c - b'0') as u32;
        if v > 65535 {
            return 65535;
        }
    }
    v as u16
}

/// Build a session key `/tty/<tail>/<leaf>` into `buf`; length or 0 on overflow.
fn tty_key(tail: &[u8], leaf: &[u8], buf: &mut [u8]) -> usize {
    let n = TTY_PREFIX.len() + tail.len() + 1 + leaf.len();
    if n > buf.len() {
        return 0;
    }
    let mut p = 0;
    buf[p..p + TTY_PREFIX.len()].copy_from_slice(TTY_PREFIX);
    p += TTY_PREFIX.len();
    buf[p..p + tail.len()].copy_from_slice(tail);
    p += tail.len();
    buf[p] = b'/';
    p += 1;
    buf[p..p + leaf.len()].copy_from_slice(leaf);
    p += leaf.len();
    p
}

fn find_tty(s: &State, tail: &[u8]) -> Option<usize> {
    s.ttys
        .iter()
        .position(|t| t.in_use && &t.tail[..t.tail_len as usize] == tail)
}

fn alloc_tty(s: &mut State, tail: &[u8], sid: i32) -> Option<usize> {
    if tail.len() > 128 {
        return None;
    }
    let idx = s.ttys.iter().position(|t| !t.in_use)?;
    let t = &mut s.ttys[idx];
    *t = TTY_EMPTY;
    t.in_use = true;
    t.backend_sid = sid;
    t.tail_len = tail.len() as u8;
    t.tail[..tail.len()].copy_from_slice(tail);
    Some(idx)
}

/// Open a PTY session for `/tty/<id>/<sid>` (`ctl` = `open;rows=;cols=;cmd=`):
/// resolve the sandbox `<id>` (the tail up to the first `/`), WL_TTY_OPEN, track
/// it, and post `status=open` (or `failed`).
unsafe fn open_tty(s: &mut State, sys: &SyscallTable, tail: &[u8], ctl: &[u8]) {
    let id = match tail.iter().position(|&b| b == b'/') {
        Some(i) => &tail[..i],
        None => tail,
    };
    let mut idbuf = [0u8; MAX_ID];
    let il = id.len().min(MAX_ID);
    idbuf[..il].copy_from_slice(&id[..il]);
    let Some(sbidx) = find_slot(s, &idbuf[..il]) else {
        return;
    };
    if !s.sandboxes[sbidx].started {
        return;
    }
    let handle = s.sandboxes[sbidx].handle;
    let rows = field(ctl, b"rows=").map(parse_u16).unwrap_or(24);
    let cols = field(ctl, b"cols=").map(parse_u16).unwrap_or(80);
    let cmd = field(ctl, b"cmd=").unwrap_or(b"/bin/sh");
    // host_process TTY_OPEN (0x1B): workload-scoped, so arg =
    // [workload_fd:i32 LE][rows:u16][cols:u16][cmd…]; return = session id.
    let mut arg = [0u8; 4 + 4 + MAX_VALUE];
    arg[0..4].copy_from_slice(&handle.to_le_bytes());
    arg[4..6].copy_from_slice(&rows.to_le_bytes());
    arg[6..8].copy_from_slice(&cols.to_le_bytes());
    let cl = cmd.len().min(MAX_VALUE - 1);
    arg[8..8 + cl].copy_from_slice(&cmd[..cl]);
    arg[8 + cl] = 0;
    let sid = (sys.provider_call)(-1, WL_TTY_OPEN, arg.as_mut_ptr(), 8 + cl + 1);
    let mut skey = [0u8; MAX_KEY];
    let sk = tty_key(tail, b"status", &mut skey);
    if sid < 0 {
        if sk != 0 {
            put_value(sys, &skey[..sk], b"failed");
        }
        return;
    }
    alloc_tty(s, tail, sid);
    if sk != 0 {
        put_value(sys, &skey[..sk], b"open");
    }
}

/// On a `/tty/` change: open sessions for new `open` ctls, close on `close`.
unsafe fn reconcile_tty(s: &mut State, sys: &SyscallTable) {
    let mut walk = ListWalk::new(TTY_PREFIX);
    while let Some(key) = walk.next(sys) {
        let klen = key.len();
        if klen > MAX_KEY {
            continue;
        }
        let mut keybuf = [0u8; MAX_KEY];
        keybuf[..klen].copy_from_slice(key);
        let key = &keybuf[..klen];
        // Only ctl keys drive lifecycle; in/out/status are data.
        if !key.ends_with(b"/ctl") || key.len() <= TTY_PREFIX.len() + 4 {
            continue;
        }
        let tail = &key[TTY_PREFIX.len()..key.len() - 4];
        let mut val = [0u8; MAX_VALUE];
        let Some(vlen) = get_value(sys, key, &mut val) else {
            continue;
        };
        let ctl = &val[..vlen];
        let tl = tail.len().min(128);
        let mut tbuf = [0u8; 128];
        tbuf[..tl].copy_from_slice(&tail[..tl]);
        if ctl.starts_with(b"open") {
            if find_tty(s, &tbuf[..tl]).is_none() {
                // The ctl key stays `open` for the session's life, and our own
                // /out and /status writes are `/tty/` changes that re-trigger
                // this pass — so a completed session (terminal status) must NOT
                // be re-opened.
                let mut skey = [0u8; MAX_KEY];
                let sk = tty_key(&tbuf[..tl], b"status", &mut skey);
                let mut sval = [0u8; 32];
                let done = sk != 0
                    && get_value(sys, &skey[..sk], &mut sval)
                        .map(|n| {
                            sval[..n].starts_with(b"exited") || sval[..n].starts_with(b"failed")
                        })
                        .unwrap_or(false);
                if !done {
                    open_tty(s, sys, &tbuf[..tl], ctl);
                }
            }
        } else if ctl.starts_with(b"close") {
            if let Some(i) = find_tty(s, &tbuf[..tl]) {
                let sid = s.ttys[i].backend_sid;
                let mut ck = [0u8; 4];
                ck.copy_from_slice(&(sid as u32).to_le_bytes());
                (sys.provider_call)(-1, WL_TTY_CLOSE, ck.as_mut_ptr(), 4);
                s.ttys[i] = TTY_EMPTY;
            }
        }
    }
}

/// Pump every active PTY session one step: feed pending stdin, drain output to
/// `/tty/<tail>/out`, and finalize (`status=exited;code=N` + backend close) when
/// the child exits.
unsafe fn poll_tty(s: &mut State, sys: &SyscallTable) {
    for i in 0..MAX_TTY {
        if !s.ttys[i].in_use {
            continue;
        }
        let tl = s.ttys[i].tail_len as usize;
        let mut tail = [0u8; 128];
        tail[..tl].copy_from_slice(&s.ttys[i].tail[..tl]);
        let backend_sid = s.ttys[i].backend_sid;

        // Pending stdin (client-written `/in`).
        let mut inkey = [0u8; MAX_KEY];
        let ik = tty_key(&tail[..tl], b"in", &mut inkey);
        let mut stdin_buf = [0u8; 1024];
        let sn = if ik != 0 {
            get_value(sys, &inkey[..ik], &mut stdin_buf).unwrap_or(0)
        } else {
            0
        }
        .min(TTY_CAP - 8);

        // Pump: [sid][wlen][stdin] → [rlen][state][code][output].
        let mut arg = [0u8; TTY_CAP];
        arg[0..4].copy_from_slice(&(backend_sid as u32).to_le_bytes());
        arg[4..8].copy_from_slice(&(sn as u32).to_le_bytes());
        arg[8..8 + sn].copy_from_slice(&stdin_buf[..sn]);
        let rc = (sys.provider_call)(-1, WL_TTY_STEP, arg.as_mut_ptr(), TTY_CAP);
        if rc < 0 {
            s.ttys[i] = TTY_EMPTY;
            continue;
        }
        if sn > 0 && ik != 0 {
            delete_value(sys, &inkey[..ik]);
        }

        let rlen = (u32::from_le_bytes([arg[0], arg[1], arg[2], arg[3]]) as usize).min(TTY_CAP - 9);
        let state = arg[4];
        let code = i32::from_le_bytes([arg[5], arg[6], arg[7], arg[8]]);
        if rlen > 0 {
            let mut okey = [0u8; MAX_KEY];
            let ok = tty_key(&tail[..tl], b"out", &mut okey);
            if ok != 0 {
                append_ring(sys, &okey[..ok], &arg[9..9 + rlen]);
            }
        }
        if state == 1 {
            let mut skey = [0u8; MAX_KEY];
            let sk = tty_key(&tail[..tl], b"status", &mut skey);
            if sk != 0 {
                let mut doc = [0u8; 32];
                let pfx = b"exited;code=";
                doc[..pfx.len()].copy_from_slice(pfx);
                let dl = pfx.len() + write_int(&mut doc[pfx.len()..], code);
                put_value(sys, &skey[..sk], &doc[..dl]);
            }
            let mut ck = [0u8; 4];
            ck.copy_from_slice(&(backend_sid as u32).to_le_bytes());
            (sys.provider_call)(-1, WL_TTY_CLOSE, ck.as_mut_ptr(), 4);
            s.ttys[i] = TTY_EMPTY;
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
        s.caps_ops = 0;
        s.transitions = 0;
        s.sandboxes = [SB_EMPTY; MAX_SB];
        s.ttys = [TTY_EMPTY; MAX_TTY];
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
        // SUBSCRIBE both watched prefixes (desired specs + exec requests) onto
        // it, then an initial full pass (specs may predate the watch).
        if s.subscribed == 0 {
            if s.sink < 0 {
                s.sink = dev_channel_port(sys, PORT_INPUT, 0);
            }
            if s.sink >= 0 {
                store_subscribe(sys, SANDBOXES_PREFIX, s.sink, 0);
                // Lease grants must re-wake reconcile: a net=own sandbox
                // defers CREATE until its /ipam-lease/<id> lands.
                store_subscribe(sys, IPAM_LEASE_PREFIX, s.sink, 0);
                store_subscribe(sys, EXEC_PREFIX, s.sink, 0);
                store_subscribe(sys, TTY_PREFIX, s.sink, 0);
                // pod_lifecycle-owned kill keys — never written here, so
                // this cannot self-wake (unlike /sandbox-status/).
                store_subscribe(sys, KILL_PREFIX, s.sink, 0);
                // Orchestrator-owned pause keys — same discipline.
                store_subscribe(sys, PAUSE_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            // First CAPS consumer: one discovery
            // call per backend session; the SIGNAL path gates on ops bit 3.
            s.caps_ops = wl_caps_ops(sys);
            // Bring sandboxes up first, then service any exec request that
            // predated the watch (so it runs against a live sandbox).
            let d = reconcile_all(s, sys);
            let e = reconcile_execs(s, sys);
            if s.caps_ops & CAPS_OP_SIGNAL != 0 {
                reconcile_kills(s, sys);
            }
            if s.caps_ops & CAPS_OP_PAUSE != 0 {
                reconcile_pauses(s, sys);
            }
            reconcile_tty(s, sys);
            s.transitions = s.transitions.wrapping_add(d + e);
            return 0;
        }

        // Drain each sandbox's stdout/stderr into /sandbox-logs/<id> (a PUT only
        // when there are new bytes), then poll liveness, then pump PTY sessions.
        poll_logs(s, sys);
        let d = poll_liveness(s, sys);
        poll_tty(s, sys);
        s.transitions = s.transitions.wrapping_add(d);

        // A pushed namespace.change (any watched prefix) → reconcile the surfaces.
        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            let d = reconcile_all(s, sys);
            let e = reconcile_execs(s, sys);
            if s.caps_ops & CAPS_OP_SIGNAL != 0 {
                reconcile_kills(s, sys);
            }
            if s.caps_ops & CAPS_OP_PAUSE != 0 {
                reconcile_pauses(s, sys);
            }
            reconcile_tty(s, sys);
            s.transitions = s.transitions.wrapping_add(d + e);
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
