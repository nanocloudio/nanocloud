//! Wormhole's failure-domain controller ⇄ the store: its preflight proofs,
//! its observations, and its placements acted on.
//!
//! See `manifest.toml`. The controller's `status_out` arrives on
//! `status_in`; what it waits for is answered on `ctl_out` (its `ctl_in`):
//!
//! - **a preflight** (`waiting` PREFLIGHT or RESTORE_PREFLIGHT): the zone's
//!   nodes read from `/nodes/` and the requests of the Pods bound to them
//!   from `/pods/`, folded by `_shared/whfd.rs` `fd_preflight` with the
//!   facts Wormhole's and Clustor's projections wrote at
//!   `/wh-fd/<zone>/facts`;
//! - **an observation** (`waiting` OBSERVATION): the zone's demand and
//!   readiness generation and each dial arm's spare room from the same
//!   nodes, and the health the director's aggregation wrote at
//!   `/wh-fd/<zone>/health`, for the dial generation the status names;
//! - **a placement** (`Placement` EVACUATE / RESTORE): `fd_node_patch`
//!   merged into every node of the zone.
//!
//! Each is answered once per operation and generation. A source that is
//! missing or does not parse answers nothing (counted, logged once): the
//! controller's own timers decide what silence means.

#![no_std]
#![allow(
    dead_code,
    unused_imports,
    unreachable_patterns,
    reason = "PIC build path-mounts modules/sdk/* via include!/mod, so each module's compile sees the full ABI surface; consumers use a subset"
)]

use core::ffi::c_void;

#[path = "../../../target/fluxor/fluxor-abi/sdk/abi.rs"]
mod abi;
use abi::SyscallTable;

include!("../../../target/fluxor/fluxor-abi/sdk/runtime.rs");
include!("../../../target/fluxor/fluxor-abi/sdk/runtime/params.rs");
const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 8192;

include!("../../../target/fluxor/fluxor-abi/sdk/crypto/sha256.rs");
include!("../_shared/store.rs");
include!("../_shared/json.rs");
include!("../_shared/jsonpatch.rs");
include!("../_shared/wormhole_portfolio.rs");
include!("../_shared/whfd.rs");

const NODES: &[u8] = b"/nodes/";
const PODS: &[u8] = b"/pods/";
const NODES_MAX: usize = 64;
const NAME_MAX: usize = 64;
const REC_MAX: usize = 512;
/// The zone as `FdNode::zone` names it to the core; every other zone's
/// nodes are `OTHER`, and a dial arm's zone is matched by name.
const INSIDE: u32 = 1;
const OTHER: u32 = 2;

#[derive(Clone, Copy)]
struct Node {
    name: [u8; NAME_MAX],
    nl: u8,
    /// The dial arm whose zone it is in, plus one; 0 none.
    arm: u8,
    fd: FdNode,
    /// The store revision its row was read at.
    rev: u64,
}

const NODE_ZERO: Node = Node {
    name: [0; NAME_MAX],
    nl: 0,
    arm: 0,
    fd: FdNode {
        zone: 0,
        ready: false,
        cordoned: false,
        voter: false,
        cpu_allocatable_m: 0,
        cpu_requested_m: 0,
    },
    rev: 0,
};

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    status_in: i32,
    ctl_out: i32,
    zone: [u8; NAME_MAX],
    zone_len: u8,
    n_arms: u8,
    /// An answer is owed on `ctl_out`: `out[..out_len]`.
    owed: u8,
    last_note: u8,
    out_len: u32,
    /// What was last answered: (operation, generation, what).
    done_op: [u8; 16],
    done_gen: u64,
    done_what: u8,
    /// A request is held (its sources not answered yet): what and for whom.
    pending: u8,
    pending_op: [u8; 16],
    pending_gen: u64,
    pending_rec: [u8; REC_MAX],
    pending_len: u32,
    arms: [[u8; NAME_MAX]; 4],
    arm_len: [u8; 4],
    nag: ReadNag,
    pub preflights: u32,
    pub observations: u32,
    pub patched: u32,
    pub unanswered: u32,
    nodes: [Node; NODES_MAX],
    n_nodes: usize,
    rec: [u8; REC_MAX],
    out: [u8; REC_MAX],
    doc: [u8; MAX_VALUE],
    merged: [u8; MAX_VALUE],
}

unsafe fn set_name(dst: &mut [u8; NAME_MAX], len: &mut u8, d: *const u8, n: usize) {
    let t = core::slice::from_raw_parts(d, n);
    let t = t.split(|&b| b == 0).next().unwrap_or(&[]);
    let k = t.len().min(NAME_MAX);
    dst[..k].copy_from_slice(&t[..k]);
    *len = k as u8;
}

define_params! {
    State;

    1, zone, str, 0 => |s, d, len| { set_name(&mut s.zone, &mut s.zone_len, d, len); };
    // The dial's arms, in its order: the zone each arm's endpoints are in.
    2, arms, str, 0 => |s, d, len| {
        let t = core::slice::from_raw_parts(d, len);
        let t = t.split(|&b| b == 0).next().unwrap_or(&[]);
        let mut k = 0usize;
        for a in t.split(|&b| b == b',') {
            if a.is_empty() || k == 4 {
                continue;
            }
            let n = a.len().min(NAME_MAX);
            s.arms[k][..n].copy_from_slice(&a[..n]);
            s.arm_len[k] = n as u8;
            k += 1;
        }
        s.n_arms = k as u8;
    };
}

/// A CPU quantity in millicores: `250m`, `2`, `1.5`.
fn cpu_milli(q: &[u8]) -> Option<u64> {
    if let Some(m) = q.strip_suffix(b"m") {
        let mut v: u64 = 0;
        if m.is_empty() {
            return None;
        }
        for &c in m {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
        }
        return Some(v);
    }
    let (int, frac) = match q.iter().position(|&c| c == b'.') {
        Some(p) => (&q[..p], &q[p + 1..]),
        None => (q, &b""[..]),
    };
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in int {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
    v = v.checked_mul(1000)?;
    let mut scale = 100u64;
    for &c in frac.iter().take(3) {
        if !c.is_ascii_digit() {
            return None;
        }
        v += u64::from(c - b'0') * scale;
        scale /= 10;
    }
    Some(v)
}

/// `<k>=<v>` in a `;`-separated row.
fn field<'a>(row: &'a [u8], k: &[u8]) -> Option<&'a [u8]> {
    row.split(|&b| b == b';')
        .find(|kv| kv.len() > k.len() && kv.starts_with(k) && kv[k.len()] == b'=')
        .map(|kv| &kv[k.len() + 1..])
}

fn num(v: &[u8]) -> Option<u64> {
    if v.is_empty() || v.len() > 19 {
        return None;
    }
    let mut n: u64 = 0;
    for &c in v {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + u64::from(c - b'0');
    }
    Some(n)
}

fn flag(row: &[u8], k: &[u8]) -> Option<bool> {
    match field(row, k)? {
        b"1" => Some(true),
        b"0" => Some(false),
        _ => None,
    }
}

/// `/wh-fd/<zone>/<tail>` into `out`.
fn row_key(s: &State, tail: &[u8], out: &mut [u8; MAX_KEY]) -> usize {
    let mut n = append(out, 0, b"/wh-fd/");
    n = append(out, n, &s.zone[..usize::from(s.zone_len)]);
    n = append(out, n, b"/");
    append(out, n, tail)
}

unsafe fn note(s: &mut State, code: u8, m: &[u8]) {
    s.unanswered = s.unanswered.wrapping_add(1);
    if s.last_note != code {
        s.last_note = code;
        dev_log(&*s.syscalls, 2, m.as_ptr(), m.len());
    }
}

/// Read every node, then the Pods bound to them; `None` while the store
/// has not answered (asked again), `Some(false)` when it refused.
unsafe fn read_nodes(s: &mut State) -> Option<bool> {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    s.n_nodes = 0;
    let mut walk = ListWalk::new(NODES);
    while let Some(k) = walk.next(&store) {
        let mut key = [0u8; MAX_KEY];
        let kl = k.len().min(MAX_KEY);
        key[..kl].copy_from_slice(&k[..kl]);
        let Some((n, rev)) = get_value_rev(&store, &key[..kl], &mut s.doc) else {
            if store.waiting() {
                return None;
            }
            continue;
        };
        if s.n_nodes == NODES_MAX {
            break;
        }
        let doc = &s.doc[..n];
        let mut node = NODE_ZERO;
        let name = &key[NODES.len()..kl];
        let nl = name.len().min(NAME_MAX);
        node.name[..nl].copy_from_slice(&name[..nl]);
        node.nl = nl as u8;
        node.rev = rev;
        let zone =
            j_get3(doc, b"metadata", b"labels", b"topology.kubernetes.io/zone").unwrap_or(b"");
        node.fd.zone = if zone == &s.zone[..usize::from(s.zone_len)] {
            INSIDE
        } else {
            OTHER
        };
        for a in 0..usize::from(s.n_arms) {
            if zone == &s.arms[a][..usize::from(s.arm_len[a])] {
                node.arm = a as u8 + 1;
            }
        }
        node.fd.voter =
            j_get3(doc, b"metadata", b"labels", b"nanocloud.io/store-voter") == Some(&b"true"[..]);
        node.fd.cordoned = j_get2(doc, b"spec", b"unschedulable") == Some(&b"true"[..]);
        node.fd.cpu_allocatable_m = j_get3(doc, b"status", b"allocatable", b"cpu")
            .or_else(|| j_get3(doc, b"status", b"capacity", b"cpu"))
            .and_then(cpu_milli)
            .unwrap_or(0);
        // Ready: the `Ready` condition's status is "True".
        if let Some(conds) = j_sub2(doc, b"status", b"conditions") {
            let mut i = 0;
            while let Some(at) = j_arr_get(conds, 0, i) {
                let c = &conds[at..j_skip(conds, at)];
                if j_get1(c, b"type") == Some(&b"Ready"[..]) {
                    node.fd.ready = j_get1(c, b"status") == Some(&b"True"[..]);
                    break;
                }
                i += 1;
            }
        }
        s.nodes[s.n_nodes] = node;
        s.n_nodes += 1;
    }
    if store.waiting() {
        return None;
    }
    if walk.failed || store.refusal().is_some() {
        return Some(false);
    }
    // The requests of the Pods bound to each node.
    let mut walk = ListWalk::new(PODS);
    while let Some(k) = walk.next(&store) {
        let mut key = [0u8; MAX_KEY];
        let kl = k.len().min(MAX_KEY);
        key[..kl].copy_from_slice(&k[..kl]);
        let Some(n) = get_value(&store, &key[..kl], &mut s.doc) else {
            if store.waiting() {
                return None;
            }
            continue;
        };
        let doc = &s.doc[..n];
        let Some(on) = j_get2(doc, b"spec", b"nodeName") else {
            continue;
        };
        let Some(node) = s.nodes[..s.n_nodes]
            .iter_mut()
            .find(|x| &x.name[..usize::from(x.nl)] == on)
        else {
            continue;
        };
        if let Some(cs) = j_sub2(doc, b"spec", b"containers") {
            let mut c = 0;
            while let Some(at) = j_arr_get(cs, 0, c) {
                let ct = &cs[at..j_skip(cs, at)];
                if let Some(cpu) = j_get3(ct, b"resources", b"requests", b"cpu") {
                    node.fd.cpu_requested_m = node
                        .fd
                        .cpu_requested_m
                        .saturating_add(cpu_milli(cpu).unwrap_or(0));
                }
                c += 1;
            }
        }
    }
    if store.waiting() {
        return None;
    }
    Some(!(walk.failed || store.refusal().is_some()))
}

/// The preflight for `phase`, or `None` (nothing answered: logged).
unsafe fn preflight(s: &mut State, phase: u8, op: &[u8; 16], gen: u64) -> Option<usize> {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut k = [0u8; MAX_KEY];
    let kn = row_key(s, b"facts", &mut k);
    let mut row = [0u8; 512];
    let Some(n) = get_value(&store, &k[..kn], &mut row) else {
        note(
            s,
            1,
            b"[wormhole_fd] no facts row for the zone: no preflight",
        );
        return None;
    };
    let r = &row[..n];
    let mut f = FdFacts::default();
    let parsed = (|| {
        f.endpoints_in_zone = u32::try_from(num(field(r, b"endpoints")?)?).ok()?;
        f.readiness_generation = num(field(r, b"rgen")?)?;
        f.returning_demand_m = field(r, b"returning").and_then(num).unwrap_or(0);
        f.standbys_ok = flag(r, b"standbys")?;
        f.route_ok = flag(r, b"route")?;
        f.keys_ok = flag(r, b"keys")?;
        f.sovereignty_ok = flag(r, b"sovereignty")?;
        f.fence_ok = flag(r, b"fence")?;
        f.conflicting = flag(r, b"conflicting")?;
        if let Some(e) = field(r, b"epochs") {
            for (i, v) in e.split(|&b| b == b',').enumerate() {
                if i >= f.epochs.len() {
                    return None;
                }
                f.epochs[i] = num(v)?;
            }
        }
        Some(())
    })();
    if parsed.is_none() {
        note(
            s,
            2,
            b"[wormhole_fd] the facts row does not parse: no preflight",
        );
        return None;
    }
    let mut nodes = [FdNode::default(); NODES_MAX];
    for (d, n) in nodes.iter_mut().zip(&s.nodes[..s.n_nodes]) {
        *d = n.fd;
    }
    let p = fd_preflight(phase, INSIDE, &nodes[..s.n_nodes], &f);
    let mut b = [0u8; traffic::FD_PREFLIGHT_LEN];
    p.encode(&mut b);
    let n =
        traffic::encode_fd_envelope(traffic::FdKind::Preflight, gen, op, &b, &mut s.out).ok()?;
    s.preflights = s.preflights.wrapping_add(1);
    Some(n)
}

/// The observation for dial generation `gen`, or `None`.
unsafe fn observation(s: &mut State, op: &[u8; 16], gen: u64) -> Option<usize> {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut k = [0u8; MAX_KEY];
    let kn = row_key(s, b"health", &mut k);
    let mut row = [0u8; 256];
    let Some(n) = get_value(&store, &k[..kn], &mut row) else {
        note(
            s,
            3,
            b"[wormhole_fd] no health row for the zone: no observation",
        );
        return None;
    };
    let r = &row[..n];
    let parsed = (|| {
        Some((
            u32::try_from(num(field(r, b"error_ppm")?)?).ok()?,
            u32::try_from(num(field(r, b"breaks")?)?).ok()?,
            u32::try_from(num(field(r, b"pinned")?)?).ok()?,
            flag(r, b"alarm")?,
        ))
    })();
    let Some((error_ppm, breaks, pinned, alarm)) = parsed else {
        note(
            s,
            4,
            b"[wormhole_fd] the health row does not parse: no observation",
        );
        return None;
    };
    let mut o = traffic::FdObservation {
        dial_generation: gen,
        error_ppm,
        continuity_breaks: breaks,
        pinned_in_domain: pinned,
        alarm,
        ..traffic::FdObservation::default()
    };
    for n in &s.nodes[..s.n_nodes] {
        if n.fd.zone == INSIDE {
            o.demand = o.demand.saturating_add(n.fd.cpu_requested_m);
            o.readiness_generation = o.readiness_generation.max(n.rev);
        }
        if n.arm != 0 && n.fd.ready && !n.fd.cordoned {
            let room = n.fd.cpu_allocatable_m.saturating_sub(n.fd.cpu_requested_m);
            let c = &mut o.capacity[usize::from(n.arm - 1)];
            *c = c.saturating_add(room);
        }
    }
    let mut b = [0u8; traffic::FD_OBSERVATION_LEN];
    o.encode(&mut b);
    let n =
        traffic::encode_fd_envelope(traffic::FdKind::Observation, gen, op, &b, &mut s.out).ok()?;
    s.observations = s.observations.wrapping_add(1);
    Some(n)
}

/// Merge the placement's patch into every node of the zone; `false` while
/// a write is not decided yet.
unsafe fn place(s: &mut State, p: &FdPlacement) -> bool {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut patch = [0u8; 256];
    let Some(pl) = fd_node_patch(p, &mut patch) else {
        return true;
    };
    for i in 0..s.n_nodes {
        let n = s.nodes[i];
        if n.fd.zone != INSIDE {
            continue;
        }
        let mut key = [0u8; MAX_KEY];
        let mut kl = append(&mut key, 0, NODES);
        kl = append(&mut key, kl, &n.name[..usize::from(n.nl)]);
        let Some(dn) = get_value(&store, &key[..kl], &mut s.doc) else {
            if store.waiting() {
                return false;
            }
            continue;
        };
        let doc = s.doc;
        let mut o = JpOut::new(&mut s.merged);
        if jp_merge(Some(&doc[..dn]), &patch[..pl], &mut o, None, b"", 0).is_err() {
            continue;
        }
        let mn = o.n;
        if s.merged[..mn] == doc[..dn] {
            continue;
        }
        let merged = s.merged;
        match put_value(&store, &key[..kl], &merged[..mn]) {
            WriteAnswer::Pending => return false,
            WriteAnswer::Decided(0) => s.patched = s.patched.wrapping_add(1),
            WriteAnswer::Decided(_) => {}
        }
    }
    true
}

/// Take the next whole record off `status_in` into `rec`; its length.
unsafe fn take(s: &mut State) -> Option<usize> {
    let sys = &*s.syscalls;
    for _ in 0..8 {
        let n = (sys.channel_peek)(s.status_in, s.rec.as_mut_ptr(), traffic::HEADER_LEN);
        if n < traffic::HEADER_LEN as i32 {
            return None;
        }
        let total = match traffic::table_record_total(&s.rec[..traffic::HEADER_LEN]) {
            Ok(t) if t <= REC_MAX => t,
            _ => {
                (sys.channel_discard)(s.status_in, 1);
                continue;
            }
        };
        if (sys.channel_peek)(s.status_in, s.rec.as_mut_ptr(), total) < total as i32 {
            return None;
        }
        (sys.channel_discard)(s.status_in, total);
        return Some(total);
    }
    None
}

#[no_mangle]
#[link_section = ".text.module_state_size"]
pub extern "C" fn module_state_size() -> u32 {
    core::mem::size_of::<State>() as u32
}

#[no_mangle]
#[link_section = ".text.module_init"]
pub extern "C" fn module_init(_syscalls: *const c_void) {}

/// # Safety
/// Kernel module-ABI entry point: `state`/`syscalls` are the loader-owned
/// instance arena and syscall table; `params` is the config TLV blob (may be
/// null when `params_len == 0`). All are valid for the lifetime the loader
/// guarantees; never called concurrently.
#[no_mangle]
#[link_section = ".text.module_new"]
pub unsafe extern "C" fn module_new(
    in_chan: i32,
    out_chan: i32,
    _ctrl_chan: i32,
    params: *const u8,
    params_len: usize,
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
        core::ptr::write_bytes(state, 0, core::mem::size_of::<State>());
        let s = &mut *(state as *mut State);
        let sys = &*(syscalls as *const SyscallTable);
        s.syscalls = sys;
        s.status_in = in_chan;
        s.ctl_out = out_chan;
        s.nag = ReadNag::new();
        parse_tlv(s, params, params_len);
        if s.status_in < 0 || s.ctl_out < 0 || s.zone_len == 0 {
            let m = b"[wormhole_fd] status_in and ctl_out must be wired and zone given";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -3;
        }
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
        if s.owed != 0 {
            let n = s.out_len as usize;
            if (sys.channel_write)(s.ctl_out, s.out.as_ptr(), n) != n as i32 {
                return 0;
            }
            s.owed = 0;
        }
        // A request is held until its sources answer.
        if s.pending == 0 {
            let Some(n) = take(s) else {
                return 0;
            };
            s.pending_rec[..n].copy_from_slice(&s.rec[..n]);
            s.pending_len = n as u32;
            s.pending = 1;
        }
        let rec = s.pending_rec;
        let rec = &rec[..s.pending_len as usize];
        let Ok(env) = traffic::decode_fd_envelope(rec) else {
            s.pending = 0;
            return 0;
        };
        let op = env.operation_id;
        let gen = env.generation;
        let (what, wait) = match env.kind {
            traffic::FdKind::Status if env.payload.len() == traffic::FD_STATUS_LEN => {
                let w = env.payload[2];
                (1u8, w)
            }
            traffic::FdKind::Placement => (2u8, 0),
            _ => {
                s.pending = 0;
                return 0;
            }
        };
        let key_what = if what == 2 {
            0x80 | env.payload.first().copied().unwrap_or(0)
        } else {
            wait
        };
        if s.done_op == op && s.done_gen == gen && s.done_what == key_what {
            s.pending = 0;
            return 0;
        }
        let answer = what == 2
            || matches!(
                wait,
                traffic::fd_wait::PREFLIGHT
                    | traffic::fd_wait::RESTORE_PREFLIGHT
                    | traffic::fd_wait::OBSERVATION
            );
        if !answer {
            s.pending = 0;
            return 0;
        }
        match read_nodes(s) {
            None => return 0,
            Some(false) => {
                note(s, 5, b"[wormhole_fd] the store refused the nodes' listing");
                s.pending = 0;
                return 0;
            }
            Some(true) => {}
        }
        if what == 2 {
            let Some(p) = fd_placement_decode(rec) else {
                s.pending = 0;
                return 0;
            };
            if !place(s, &p) {
                return 0;
            }
        } else {
            let written = match wait {
                traffic::fd_wait::OBSERVATION => observation(s, &op, gen),
                traffic::fd_wait::RESTORE_PREFLIGHT => preflight(s, FD_PHASE_RESTORE, &op, gen),
                _ => preflight(s, FD_PHASE_EVACUATE, &op, gen),
            };
            match written {
                Some(n) => {
                    s.out_len = n as u32;
                    s.owed = 1;
                }
                None => {
                    s.pending = 0;
                    return 0;
                }
            }
        }
        s.done_op = op;
        s.done_gen = gen;
        s.done_what = key_what;
        s.pending = 0;
        0
    }
}
