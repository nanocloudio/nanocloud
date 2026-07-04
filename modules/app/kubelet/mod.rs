//! Kubelet — nanocloud's METAL (bare-metal, bcm2712/pi5) pod runtime as a PIC
//! module, and the fmod-graph sibling of `sandbox_runner`. Where sandbox_runner
//! drives the Linux host-process backend of the `workload` contract (0x1A) for
//! **bundle** (container-image) pods, this module drives the METAL backend of
//! the same 0x1A contract for **fmod-graph** pods: a metal "pod" is not a
//! container image (metal has no OCI rootfs/bundle transport) but a composition
//! of flash fmod modules wired into an owned subgraph with its own IP identity
//! — the kubelet-fmod.
//!
//! Backend selection is by the CREATE header's `source_kind` byte, resolved by
//! the source artifact's type: `SOURCE_FMOD_GRAPH` (0)
//! routes to the metal `workload_graph` provider (registered on bcm2712 by
//! `bcm_init_providers`); `SOURCE_HOST_PROCESS` (1) routes to the Linux backend. The
//! consumer never names a platform — it names *what to run* (a template) and the
//! node's placement-resolved backend realizes it. This module emits FMOD_GRAPH;
//! sandbox_runner emits BUNDLE.
//!
//! **A metal pod is a fmod GRAPH, not an image (the modeling decision).** The
//! pod→FLXA mapping is the least-surface viable model: a pod spec
//! **names a pre-registered workload TEMPLATE** — a known module set + internal
//! wiring the node ships in flash — and the kubelet param-fills it with the
//! pod's identity. This is "a pod runs a known workload", not "a pod is an
//! arbitrary graph": a metal node cannot run an arbitrary container image, so it
//! runs a small, curated set of named workloads. The alternative (a pod spec
//! enumerating modules + wiring inline) is strictly more surface for no benefit
//! at this altitude and is rejected.
//!
//! Data model (the compact projection format, mirroring sandbox_runner):
//!
//!   /metal-sandboxes/<uid> = "graph=<template>;phase=<start|delete>[;net=own][;tasks=<n>]"
//!                            (in ← pod_lifecycle; the DECISION half is shared)
//!   /ipam-lease/<uid>      = "<ip>"  (cni_ipam; consumed when net=own)
//!   /sandbox-status/<uid>  = "state=<created|running|paused|exited|killed|failed|destroyed>;code=<n>"
//!                            (out → pod_lifecycle; a uid is metal XOR bundle, so
//!                             this key keeps a single writer across the two runtimes)
//!   /sandbox-kill/<uid>    = "sig=term" | "sig=kill"   (in ← pod_lifecycle)
//!   /sandbox-pause/<uid>   = "pause" | "resume"        (in ← orchestrator)
//!
//! FLXA composition (the wire the metal backend's `apply_add_encoded` decodes,
//! fluxor `scheduler/live.rs`): big-endian, PIC-modules-by-name_hash. The
//! kubelet composes it OFF-NODE (in-fmod) and hands it inline in the CREATE
//! `source_ref`; the backend overwrites `pod_uid` from the header identity and
//! clamps the caps to the memory envelope, so the composer zeroes both.
//!
//!   magic "FLXA":u32  version:u16=1  reserved:u16  pod_uid:[16]  state_cap:u32
//!   buffer_cap:u32  module_count:u8  edge_count:u8
//!   per module: name_hash:u32  domain_id:u8  params_len:u16  params[params_len]
//!   per edge:   from_kind:u8 from_idx:u16 from_port:u8
//!               to_kind:u8   to_idx:u16   to_port:u8   buffer_bytes:u32
//!   (endpoint kind 0 = New/subgraph-local, 1 = Existing/global slot)
//!
//! **Net-facing spare-lane wire.** A
//! `net=own` workload's net-facing producer (its http/tls egress) must reach the
//! node's ONE shared `ip` module through a boot-provisioned merge spare lane
//! (`Endpoint::ExistingChannel`). That endpoint kind does NOT
//! exist in the FLXA v1 wire (`decode_endpoint` knows only New/Existing) and the
//! spare-lane channel id is unknown off-node, so the composer cannot name it
//! directly. Instead it emits a SENTINEL edge: the net-facing
//! producer's `to` is `Existing(SPARE_LANE_SENTINEL = 0xFFFF)`, a reserved index
//! no live module owns. The metal backend (`workload_graph.rs`), between decode
//! and apply, resolves the boot merge's next free spare lane on `ip.net_in` and
//! rewrites the sentinel to `ExistingChannel(lane)` — no ABI/wire change, the
//! composer stays honest (it names a target, the kernel resolves the channel).
//! So a `net=own` template now composes its module set + internal edges + the
//! one net-facing sentinel edge; a HOST-SHARED pod emits none.
//!
//! Loop shape (level-triggered + liveness poll), identical discipline to
//! sandbox_runner: first step SUBSCRIBEs the watched prefixes (NOT the status
//! we write, or we'd self-wake) + a cold reconcile; each step polls WL_WAIT on
//! every live pod and reconciles on a pushed change.

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

// Control-plane store (storage.object 0x14 keyed bytes; storage.namespace 0x13
// prefix LIST + change SUBSCRIBE), same seams sandbox_runner uses.
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const NS_SUBSCRIBE: u32 = 0x1305;
const PORT_INPUT: u8 = 0;
const EVENT_HEADER_SIZE: usize = 32;

// workload (0x1A) lifecycle opcodes — same contract as sandbox_runner; only the
// CREATE source_kind (FMOD_GRAPH) and the source_ref (a FLXA blob) differ.
const WL_CREATE: u32 = 0x1A00;
const WL_START: u32 = 0x1A01;
const WL_WAIT: u32 = 0x1A02;
const WL_SIGNAL: u32 = 0x1A03;
const WL_DESTROY: u32 = 0x1A04;
const WL_PAUSE: u32 = 0x1A0B;
const WL_RESUME: u32 = 0x1A0C;
const WL_CAPS: u32 = 0x1AFF;
const CAPS_OP_SIGNAL: u16 = 1 << 3;
const CAPS_OP_PAUSE: u16 = 1 << 4;
const SIG_TERM: u32 = 1;
const SIG_KILL: u32 = 2;

// 68-byte Tier-1 CREATE header (workload.rs). Byte 17 = source_kind; for the
// metal backend it is SOURCE_FMOD_GRAPH (0). Bytes 48..68 carry the net
// identity a `net=own` pod fills from its cni_ipam lease.
const WL_CREATE_HEADER: usize = 68;
const POSTURE_SHARED: u8 = 0;
const SOURCE_FMOD_GRAPH: u8 = 0;
const WL_NET_ISO_OWN: u8 = 1;
const WL_NET_FAM_IPV4: u8 = 4;
const IPAM_DEFAULT_PREFIX: u8 = 24;

// WL_WAIT state byte: the fmod model — RUNNING while the owner is Active,
// SIGNALLED on a module fault, EXITED only on owner teardown, PAUSED is LIVE.
const WL_STATE_RUNNING: u8 = 0;
const WL_STATE_EXITED: u8 = 1;
const WL_STATE_SIGNALLED: u8 = 2;
const WL_STATE_PAUSED: u8 = 3;

const METAL_SANDBOXES_PREFIX: &[u8] = b"/metal-sandboxes/";
const STATUS_PREFIX: &[u8] = b"/sandbox-status/";
const KILL_PREFIX: &[u8] = b"/sandbox-kill/";
const PAUSE_PREFIX: &[u8] = b"/sandbox-pause/";
const IPAM_REQ_PREFIX: &[u8] = b"/ipam-request/";
const IPAM_LEASE_PREFIX: &[u8] = b"/ipam-lease/";
const IPAM_POOL_KEY: &[u8] = b"/ipam-pool";

const MAX_KEY: usize = 128;
const MAX_VALUE: usize = 256;
const LIST_BUF: usize = 2048;
const MAX_ID: usize = 96;
const MAX_POD: usize = 16;
// A composed FLXA blob is bounded: header (34) + up to a few modules/edges. A
// template stays small (a metal workload is a handful of flash fmods), so this
// ceiling is generous and stack-only.
const FLXA_CAP: usize = 512;

// ── FLXA wire constants (fluxor scheduler/live.rs) ──────────────────────────
const FLXA_MAGIC: u32 = 0x464C_5841; // "FLXA", big-endian on the wire
const FLXA_VERSION: u16 = 1;
const EP_NEW: u8 = 0; // subgraph-local module index
const EP_EXISTING: u8 = 1; // global slot of a live base-graph module
/// Reserved `Existing` global index marking a `net=own` workload's net-facing
/// spare-lane edge (fluxor `scheduler/live.rs::SPARE_LANE_SENTINEL`). The
/// composer cannot name the boot merge's spare-lane channel (a
/// kernel runtime value), so it emits the net-facing producer's `to` as
/// `Existing(SPARE_LANE_SENTINEL)`; the metal backend resolves the free lane and
/// rewrites it to `ExistingChannel(lane)` between decode and apply. `0xFFFF` is
/// never a real live module slot, so it rides the FLXA v1 wire with no ABI
/// change.
const SPARE_LANE_SENTINEL: u16 = 0xFFFF;
// Workloads dispatch on the primary domain / core 0 (the system graph's
// domain), which is where the quiesce invariant holds — so template modules
// stage into domain 0.
const WORKLOAD_DOMAIN: u8 = 0;

// `fnv1a32` (the module-name→name_hash function the loader resolves against the
// flash table) is provided by the SDK `wire.rs`, mounted via `runtime.rs` — used
// in the `TEMPLATES` const below to hash each template module's name.

/// One internal edge of a template's subgraph — producer→consumer among the
/// template's OWN modules (subgraph-local indices). The net-facing edge into the
/// node's shared `ip` is deliberately NOT expressible here (the spare-lane gap,
/// see the module doc): it is not an internal edge and needs the backend assist.
#[derive(Clone, Copy)]
struct TemplateEdge {
    from_mod: u8,
    from_port: u8,
    to_mod: u8,
    to_port: u8,
}

/// A named workload template: the module set (by name_hash, flash-resolved) and
/// their internal wiring. The kubelet param-fills nothing structural — identity
/// rides the CREATE header, not the FLXA — so a template is a pure constant.
struct Template {
    name: &'static [u8],
    modules: &'static [u32],
    edges: &'static [TemplateEdge],
    /// The net-facing producer for a `net=own` pod: `(subgraph-local module
    /// index, out port)` whose egress must reach the node's one shared `ip`.
    /// When the pod is `net=own`, the composer emits ONE extra edge
    /// `New(module).out[port] → Existing(SPARE_LANE_SENTINEL)`; the metal backend
    /// rewrites the sentinel to the resolved boot-merge spare lane. `None` =
    /// a template with no net egress (a pure-compute workload); a HOST-SHARED
    /// pod ignores it and emits no sentinel.
    net_producer: Option<(u8, u8)>,
}

/// The node's known workload templates. First cut ships ONE: `http-echo` — a
/// single `http` app module. For a HOST-SHARED pod it composes bare; for a
/// `net=own` pod the composer additionally emits the net-facing sentinel edge
/// from http's egress (module 0, out port 0) so the backend wires it onto the
/// shared `ip`'s boot-merge spare lane. A metal pod spec's
/// `graph=<name>` selects one of these; an unknown name fails CREATE
/// deterministically (status=failed) rather than composing a bogus graph.
static TEMPLATES: &[Template] = &[Template {
    name: b"http-echo",
    modules: &[fnv1a32(b"http")],
    edges: &[],
    net_producer: Some((0, 0)),
}];

fn find_template(name: &[u8]) -> Option<&'static Template> {
    TEMPLATES.iter().find(|t| t.name == name)
}

/// One tracked metal pod: its uid, the live workload handle, lifecycle flags.
#[repr(C)]
#[derive(Clone, Copy)]
struct Pod {
    in_use: bool,
    started: bool,
    exited: bool,
    /// Highest /sandbox-kill/ escalation delivered: 0=none, 1=term, 2=kill.
    sig_sent: u8,
    /// Last /sandbox-pause/ intent applied: 0=none, 1=pause, 2=resume.
    pause_sent: u8,
    /// status-projection latch for the LIVE paused state.
    paused: bool,
    id_len: u8,
    handle: i32,
    id: [u8; MAX_ID],
}

const POD_EMPTY: Pod = Pod {
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

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    out_chan: i32,
    sink: i32,
    subscribed: u8,
    caps_ops: u16,
    transitions: u32,
    pods: [Pod; MAX_POD],
}

// ---- storage.object / storage.namespace ops (identical seams to sandbox_runner) ----

// ---- FLXA composition (the crux: pod template → the exact apply_add wire) ----

/// Big-endian append helpers over a bounded cursor. The FLXA codec is big-endian
/// (`Cur::u16/u32` in fluxor live.rs), UNLIKE the little-endian store framing.
fn put_u8(buf: &mut [u8], p: usize, v: u8) -> usize {
    if p < buf.len() {
        buf[p] = v;
    }
    p + 1
}
fn put_u16_be(buf: &mut [u8], p: usize, v: u16) -> usize {
    if p + 2 <= buf.len() {
        buf[p..p + 2].copy_from_slice(&v.to_be_bytes());
    }
    p + 2
}
fn put_u32_be(buf: &mut [u8], p: usize, v: u32) -> usize {
    if p + 4 <= buf.len() {
        buf[p..p + 4].copy_from_slice(&v.to_be_bytes());
    }
    p + 4
}

/// Compose `template` into a FLXA `AddSubgraph` blob in `dst`, returning its
/// length (0 = would overflow `dst`). `pod_uid` and the caps are left zero: the
/// metal backend overwrites `pod_uid` from the CREATE header identity and clamps
/// the caps to the memory envelope, so the composer never guesses them.
///
/// Emits exactly what `apply_add_encoded` accepts — modules by name_hash
/// (flash-resolved) with empty params + internal `Endpoint::New` edges. For a
/// `net=own` pod (`net_own == true`) with a `net_producer`, it ALSO emits the
/// net-facing SENTINEL edge `New(producer).out → Existing(SPARE_LANE_SENTINEL)`:
/// the composer cannot name the boot-merge spare-lane channel off-node, so it
/// marks the edge and the metal backend rewrites it to the resolved
/// `ExistingChannel(lane)` between decode and apply. A HOST-SHARED pod (or a
/// template with no `net_producer`) emits no sentinel.
fn compose_flxa(dst: &mut [u8], template: &Template, net_own: bool) -> usize {
    let net_edge = if net_own { template.net_producer } else { None };
    let mc = template.modules.len();
    let ec = template.edges.len() + usize::from(net_edge.is_some());
    if mc > 16 || ec > 32 {
        return 0; // MAX_ADD_MODULES / MAX_ADD_EDGES
    }
    let mut p = 0;
    p = put_u32_be(dst, p, FLXA_MAGIC);
    p = put_u16_be(dst, p, FLXA_VERSION);
    p = put_u16_be(dst, p, 0); // reserved
    for b in dst.iter_mut().skip(p).take(16) {
        *b = 0; // pod_uid[16] — backend overwrites from the header identity
    }
    p += 16;
    p = put_u32_be(dst, p, 0); // state_cap — backend clamps to the envelope
    p = put_u32_be(dst, p, 0); // buffer_cap
    p = put_u8(dst, p, mc as u8);
    p = put_u8(dst, p, ec as u8);
    for &name_hash in template.modules {
        p = put_u32_be(dst, p, name_hash);
        p = put_u8(dst, p, WORKLOAD_DOMAIN);
        p = put_u16_be(dst, p, 0); // params_len — templates are param-free
    }
    for e in template.edges {
        p = put_u8(dst, p, EP_NEW);
        p = put_u16_be(dst, p, e.from_mod as u16);
        p = put_u8(dst, p, e.from_port);
        p = put_u8(dst, p, EP_NEW);
        p = put_u16_be(dst, p, e.to_mod as u16);
        p = put_u8(dst, p, e.to_port);
        p = put_u32_be(dst, p, 0); // buffer_bytes — derive from module hints
    }
    // Net-facing sentinel edge: the producer's egress → the
    // reserved spare-lane marker; the backend resolves it onto `ip.net_in`'s
    // boot-merge free lane.
    if let Some((from_mod, from_port)) = net_edge {
        p = put_u8(dst, p, EP_NEW);
        p = put_u16_be(dst, p, from_mod as u16);
        p = put_u8(dst, p, from_port);
        p = put_u8(dst, p, EP_EXISTING);
        p = put_u16_be(dst, p, SPARE_LANE_SENTINEL);
        p = put_u8(dst, p, 0); // to_port — irrelevant once rewritten to a channel
        p = put_u32_be(dst, p, 0); // buffer_bytes
    }
    if p > dst.len() {
        0
    } else {
        p
    }
}

// ---- workload ops (Tier-1 header on CREATE; handle-based lifecycle) ----

/// WL_CREATE — admit + stage a metal fmod-graph workload. Builds the 68-byte
/// Tier-1 header (posture SHARED, source kind FMOD_GRAPH) with the composed FLXA
/// blob as the source-ref, and — for a `net=own` pod — the leased (address,
/// prefix) in the network-identity section with NET_ISO_OWN, so the backend
/// installs the workload's own address into the node's shared `ip` and scopes
/// its binds to it. Returns the tagged WorkloadHandle (>=0) or a negative
/// errno.
unsafe fn wl_create(
    sys: &SyscallTable,
    flxa: &[u8],
    net: Option<([u8; 4], u8)>,
    max_tasks: u32,
) -> i32 {
    let mut arg = [0u8; WL_CREATE_HEADER + FLXA_CAP];
    if WL_CREATE_HEADER + flxa.len() > arg.len() {
        return -22; // EINVAL
    }
    // identity[0..16] = the pod uid; the null/system uid → owner 0 for the
    // single-tenant edge node. (Left zero here; the metal backend allocates the
    // owner and bridges the header uid into the FLXA pod_uid.)
    arg[16] = POSTURE_SHARED;
    arg[17] = SOURCE_FMOD_GRAPH;
    arg[32..36].copy_from_slice(&max_tasks.to_le_bytes()); // memory/tasks envelope
    arg[40..42].copy_from_slice(&(flxa.len() as u16).to_le_bytes()); // source_ref_len
                                                                     // endpoint_count (42..44) + options_len (44..48) stay 0.
    if let Some((addr, prefix)) = net {
        arg[18] = WL_NET_ISO_OWN;
        arg[48] = WL_NET_FAM_IPV4;
        arg[49] = prefix;
        arg[52..56].copy_from_slice(&addr);
    }
    arg[WL_CREATE_HEADER..WL_CREATE_HEADER + flxa.len()].copy_from_slice(flxa);
    (sys.provider_call)(
        -1,
        WL_CREATE,
        arg.as_mut_ptr(),
        WL_CREATE_HEADER + flxa.len(),
    )
}

unsafe fn wl_start(sys: &SyscallTable, handle: i32) -> i32 {
    let mut scratch = [0u8; 1];
    (sys.provider_call)(handle, WL_START, scratch.as_mut_ptr(), 0)
}

/// WL_WAIT (non-blocking); Some((state, code)) or None on error.
unsafe fn wl_wait(sys: &SyscallTable, handle: i32) -> Option<(u8, i32)> {
    let mut buf = [0u8; 5];
    let rc = (sys.provider_call)(handle, WL_WAIT, buf.as_mut_ptr(), buf.len());
    if rc != 5 {
        return None;
    }
    Some((buf[0], i32::from_le_bytes(buf[1..5].try_into().unwrap())))
}

unsafe fn wl_destroy(sys: &SyscallTable, handle: i32) -> i32 {
    let mut scratch = [0u8; 1];
    (sys.provider_call)(handle, WL_DESTROY, scratch.as_mut_ptr(), 0)
}

unsafe fn wl_signal(sys: &SyscallTable, handle: i32, signo: u32) -> i32 {
    let mut arg = signo.to_le_bytes();
    (sys.provider_call)(handle, WL_SIGNAL, arg.as_mut_ptr(), arg.len())
}

unsafe fn wl_pause_op(sys: &SyscallTable, handle: i32, op: u32) -> i32 {
    let mut scratch = [0u8; 1];
    (sys.provider_call)(handle, op, scratch.as_mut_ptr(), scratch.len())
}

/// WL_CAPS — one-shot metal backend discovery; returns the `ops` bitmap.
unsafe fn wl_caps_ops(sys: &SyscallTable) -> u16 {
    let mut buf = [0u8; 64];
    let rc = (sys.provider_call)(-1, WL_CAPS, buf.as_mut_ptr(), buf.len());
    if rc < 5 {
        0
    } else {
        u16::from_le_bytes([buf[2], buf[3]])
    }
}

// ---- compact-format parsing (byte scanning; the format is ours) ----

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

/// The pool prefix length from /ipam-pool (`cidr=<a.b.c.d>/<prefix>`), else the
/// default — one source of truth; the lease value stays a bare address.
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

/// Resolve a `net=own` pod's leased identity: ensure /ipam-request/<uid> exists,
/// return the (address, prefix) once cni_ipam has granted /ipam-lease/<uid>.
/// None = pending — the lease SUBSCRIBE re-wakes reconcile when the grant lands.
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

fn write_int(buf: &mut [u8], n: i32) -> usize {
    if buf.is_empty() {
        return 0;
    }
    if n == 0 {
        buf[0] = b'0';
        return 1;
    }
    let neg = n < 0;
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

// ---- pod table ----

fn find_slot(s: &State, id: &[u8]) -> Option<usize> {
    s.pods
        .iter()
        .position(|p| p.in_use && &p.id[..p.id_len as usize] == id)
}

fn alloc_slot(s: &mut State, id: &[u8]) -> Option<usize> {
    if id.len() > MAX_ID {
        return None;
    }
    let idx = s.pods.iter().position(|p| !p.in_use)?;
    let p = &mut s.pods[idx];
    *p = POD_EMPTY;
    p.in_use = true;
    p.id_len = id.len() as u8;
    p.id[..id.len()].copy_from_slice(id);
    Some(idx)
}

/// Reconcile one desired metal pod toward its phase. Returns transitions driven.
unsafe fn reconcile_one(s: &mut State, sys: &SyscallTable, id: &[u8], value: &[u8]) -> u32 {
    let phase = field(value, b"phase=").unwrap_or(b"start");
    let mut driven = 0u32;

    if phase == b"delete" {
        if let Some(idx) = find_slot(s, id) {
            let handle = s.pods[idx].handle;
            wl_destroy(sys, handle);
            s.pods[idx] = POD_EMPTY;
            put_status(sys, id, b"destroyed", 0);
            driven += 1;
        }
        return driven;
    }

    // Ensure created. A CREATE failure surfaces status=failed;code=<errno> and
    // holds no slot (a bad template / rejected FLXA is deterministic, not a leak).
    let idx = match find_slot(s, id) {
        Some(i) => i,
        None => {
            let Some(tmpl_name) = field(value, b"graph=") else {
                return driven;
            };
            let Some(template) = find_template(tmpl_name) else {
                put_status(sys, id, b"failed", -22); // EINVAL — unknown template
                return driven;
            };
            // `net=own` needs a cni_ipam lease before CREATE — defer (no status,
            // no slot) until the lease SUBSCRIBE wakes us. Never CREATE with
            // silently-shared networking.
            let net = if field(value, b"net=") == Some(b"own".as_ref()) {
                match ipam_identity(sys, id) {
                    Some(n) => Some(n),
                    None => return driven,
                }
            } else {
                None
            };
            let max_tasks = field(value, b"tasks=").map(parse_u32).unwrap_or(0);

            let mut flxa = [0u8; FLXA_CAP];
            let flen = compose_flxa(&mut flxa, template, net.is_some());
            if flen == 0 {
                put_status(sys, id, b"failed", -22); // composition overflow
                return driven;
            }
            let handle = wl_create(sys, &flxa[..flen], net, max_tasks);
            if handle < 0 {
                put_status(sys, id, b"failed", handle);
                return driven;
            }
            let Some(i) = alloc_slot(s, id) else {
                wl_destroy(sys, handle);
                put_status(sys, id, b"failed", -12); // ENOMEM
                return driven;
            };
            s.pods[i].handle = handle;
            put_status(sys, id, b"created", 0);
            driven += 1;
            i
        }
    };

    if phase == b"start" && !s.pods[idx].started {
        let handle = s.pods[idx].handle;
        if wl_start(sys, handle) >= 0 {
            s.pods[idx].started = true;
            put_status(sys, id, b"running", 0);
            driven += 1;
        }
    }
    driven
}

/// Full pass over desired /metal-sandboxes/ specs. Returns transitions driven.
unsafe fn reconcile_all(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut walk = ListWalk::new(METAL_SANDBOXES_PREFIX);
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

        let id_slice = last_seg(&keybuf[..klen]);
        let ilen = id_slice.len().min(MAX_ID);
        let mut idbuf = [0u8; MAX_ID];
        idbuf[..ilen].copy_from_slice(&id_slice[..ilen]);

        driven += reconcile_one(s, sys, &idbuf[..ilen], &valbuf[..vlen]);
    }
    driven
}

/// Poll WL_WAIT on every live, started pod; write terminal/paused status on the
/// transition. Cheap: in-memory table, PUT only on a state change.
unsafe fn poll_liveness(s: &mut State, sys: &SyscallTable) -> u32 {
    let mut driven = 0u32;
    for idx in 0..MAX_POD {
        let pod = s.pods[idx];
        if !pod.in_use || !pod.started || pod.exited {
            continue;
        }
        let Some((state, code)) = wl_wait(sys, pod.handle) else {
            continue;
        };
        let id_len = pod.id_len as usize;
        let mut idbuf = [0u8; MAX_ID];
        idbuf[..id_len].copy_from_slice(&pod.id[..id_len]);
        if state == WL_STATE_RUNNING {
            if pod.paused {
                put_status(sys, &idbuf[..id_len], b"running", 0);
                s.pods[idx].paused = false;
                driven += 1;
            }
            continue;
        }
        if state == WL_STATE_PAUSED {
            if !pod.paused {
                put_status(sys, &idbuf[..id_len], b"paused", 0);
                s.pods[idx].paused = true;
                driven += 1;
            }
            continue;
        }
        // EXITED = owner torn down (code 0); SIGNALLED = a module fault.
        let label: &[u8] = match state {
            WL_STATE_EXITED if code == 0 => b"exited",
            WL_STATE_EXITED => b"failed",
            WL_STATE_SIGNALLED => b"killed",
            _ => b"failed",
        };
        put_status(sys, &idbuf[..id_len], label, code);
        s.pods[idx].exited = true;
        s.pods[idx].paused = false;
        driven += 1;
    }
    driven
}

/// React to /sandbox-kill/<uid> = "sig=term" | "sig=kill"
/// (pod_lifecycle-owned): deliver SIGNAL once per escalation level. Gated on CAPS ops bit 3.
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
            continue;
        };
        let pod = s.pods[idx];
        if !pod.started || pod.exited || pod.sig_sent >= want {
            continue;
        }
        let signo = if want == 2 { SIG_KILL } else { SIG_TERM };
        if wl_signal(sys, pod.handle, signo) >= 0 {
            s.pods[idx].sig_sent = want;
        }
    }
}

/// React to /sandbox-pause/<uid> = "pause" | "resume"
/// (orchestrator-owned): issue PAUSE/RESUME once per intent. Gated on CAPS ops bit 4.
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
            continue;
        };
        let pod = s.pods[idx];
        if !pod.started || pod.exited || pod.pause_sent == want {
            continue;
        }
        let op = if want == 1 { WL_PAUSE } else { WL_RESUME };
        if wl_pause_op(sys, pod.handle, op) >= 0 {
            s.pods[idx].pause_sent = want;
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
        s.sink = in_chan;
        s.subscribed = 0;
        s.caps_ops = 0;
        s.transitions = 0;
        s.pods = [POD_EMPTY; MAX_POD];
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
                store_subscribe(sys, METAL_SANDBOXES_PREFIX, s.sink, 0);
                // Lease grants must re-wake reconcile: a net=own pod defers
                // CREATE until its /ipam-lease/<uid> lands.
                store_subscribe(sys, IPAM_LEASE_PREFIX, s.sink, 0);
                // pod_lifecycle-owned kill keys + orchestrator-owned pause
                // keys — never written here, so subscribing cannot self-wake.
                store_subscribe(sys, KILL_PREFIX, s.sink, 0);
                store_subscribe(sys, PAUSE_PREFIX, s.sink, 0);
            }
            s.subscribed = 1;
            // One CAPS discovery per backend session; the SIGNAL/PAUSE paths
            // gate on the ops bits (the metal backend advertises PAUSE).
            s.caps_ops = wl_caps_ops(sys);
            let d = reconcile_all(s, sys);
            if s.caps_ops & CAPS_OP_SIGNAL != 0 {
                reconcile_kills(s, sys);
            }
            if s.caps_ops & CAPS_OP_PAUSE != 0 {
                reconcile_pauses(s, sys);
            }
            s.transitions = s.transitions.wrapping_add(d);
            return 0;
        }

        let d = poll_liveness(s, sys);
        s.transitions = s.transitions.wrapping_add(d);

        if s.sink >= 0 && drain_changes(sys, s.sink) > 0 {
            let d = reconcile_all(s, sys);
            if s.caps_ops & CAPS_OP_SIGNAL != 0 {
                reconcile_kills(s, sys);
            }
            if s.caps_ops & CAPS_OP_PAUSE != 0 {
                reconcile_pauses(s, sys);
            }
            s.transitions = s.transitions.wrapping_add(d);
        }
        0
    }
}

include!("../../../target/fluxor/fluxor-abi/sdk/runtime/wasm_entry.rs");
