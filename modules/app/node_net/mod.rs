//! The node's `net_route` writer: its segment, its peer routes and the
//! load-balancer addresses it answers for.
//!
//! See `manifest.toml` for what is written, where it comes from and what is
//! reported. The tables are replaced whole and only when what they would
//! hold changed; what took effect is the provider's answer to READ, written
//! down for the cluster to see.

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
include!("../_shared/json.rs");
include!("../_shared/store.rs");
include!("../_shared/field.rs");
include!("../_shared/vip_hold.rs");

use abi::contracts::net::route as wire;

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;

const PORT_INPUT: u8 = 0;
const MAX_PARAM: usize = 64;
/// A Service, read whole.
const DOC: usize = 16 * 1024;
/// A Node, read whole.
const NODE_DOC: usize = 8 * 1024;
/// A Pod, read whole (pod_resolve's bound): one past it is taken for live.
const POD: usize = 32 * 1024;
/// Peer routes this writer states: one per other node.
const MAX_PEERS: usize = 64;
/// Load-balancer addresses this writer states.
const MAX_HELD: usize = 32;
/// The table being built, and READ's answer.
const TBL: usize = 2 + MAX_PEERS * wire::peer::SIZE;
const RD: usize = 8 + (1 + MAX_PEERS + MAX_HELD) * wire::entry::SIZE;
/// The status record.
const NET_REC: usize = 1536;
/// How often what took effect is asked (READ), and how often the whole pass
/// runs with nothing changed (a refused table asked again, a holder's
/// readiness read again).
const STATUS_MS: u64 = 2000;
const PASS_MS: u64 = 5_000;
/// The node's one segment.
const SEGMENT_ID: u16 = 1;

const NODES: &[u8] = b"/nodes/";
const SERVICES: &[u8] = b"/services/";
const SLICES: &[u8] = b"/endpoint-rows/";
const HOLDS: &[u8] = b"/net-hold/";
/// An address controller's marks: an address a controller drives, which no
/// node's own hold claims while the mark stands (`_shared/vip_hold.rs`).
const MARKS: &[u8] = b"/net-vip/";
/// svc_alloc's load-balancer addresses, by Service.
const LB_ADDRS: &[u8] = b"/lb-addr/";
const REQUESTS: &[u8] = b"/ipam-request/";
const LEASES: &[u8] = b"/ipam-lease/";
const SANDBOXES: &[u8] = b"/sandboxes/";
const METAL_SANDBOXES: &[u8] = b"/metal-sandboxes/";
const PODS: &[u8] = b"/pods/";
const NODE_NET: &[u8] = b"/node-net/";
const SANDBOX_NET: &[u8] = b"/sandbox-net";
const SANDBOX_STATUS: &[u8] = b"/sandbox-status/";

/// What one pass did.
#[derive(PartialEq, Eq)]
enum Pass {
    /// Nothing is owed.
    Idle,
    /// It wrote (or must look again): another pass next step.
    Again,
    /// The store refused a read for good, or a listing failed: asked again
    /// at once it answers the same, so the periodic pass (`PASS_MS`) runs it
    /// again — never left for a next change that may not come.
    Failed,
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    /// The kernel event every subscription rings (`store_event`), or -1.
    event: i32,
    /// Prefixes subscribed so far, in [`SUBS`] order.
    subscribed: u8,
    /// A pass is owed: set by a ring or the periodic pass, cleared only
    /// when one runs.
    dirty: u8,
    pass_ms: u64,
    status_ms: u64,

    node: [u8; MAX_PARAM],
    node_len: u8,
    cl_txt: [u8; MAX_PARAM],
    cl_len: u8,
    sv_txt: [u8; MAX_PARAM],
    sv_len: u8,
    mtu_txt: [u8; 16],
    mtu_len: u8,
    gp_txt: [u8; 4],
    gp_len: u8,
    /// The node's graph-backed Pods (/metal-sandboxes/) get addresses of
    /// their own too: a runtime that attaches its module workloads to the
    /// segment (bare metal).
    graph_pods: bool,
    cluster: (u32, u8),
    services: (u32, u8),
    mtu: u16,

    caps: u32,
    /// The node as last read: its segment's CIDR (len 0: none yet), its
    /// address, whether it is ready.
    cidr: (u32, u8),
    own_addr: u32,
    ready: bool,

    /// Each table as last replaced: the hash of its records (and whether
    /// any was), the provider's answer, its entry count.
    seg_hash: u64,
    seg_set: bool,
    seg_rc: i32,
    seg_n: u8,
    seg_cidr: (u32, u8),
    /// The segment was in place at the last READ.
    seg_up: bool,
    route_hash: u64,
    route_set: bool,
    route_rc: i32,
    route_n: u16,
    held_hash: u64,
    held_set: bool,
    held_rc: i32,
    held_n: u16,
    held_addr: [u32; MAX_HELD],
    held_armed: [u8; MAX_HELD],
    /// The held addresses the current pass found, and whether this node
    /// holds each one's claim.
    want_addr: [u32; MAX_HELD],
    want_armed: [u8; MAX_HELD],
    want_n: u16,
    want_over: bool,

    /// The status records as last written.
    net_rec: [u8; NET_REC],
    net_len: u16,
    net_said: bool,
    sb_rec: [u8; 16],
    sb_len: u8,
    sb_said: bool,

    /// A write the store answered EINPROGRESS: asked again, byte-identical,
    /// before anything else.
    owed: u8,
    owed_pre: u8,
    owed_del: u8,
    owed_rev: u64,
    owed_key: [u8; MAX_KEY],
    owed_klen: u16,
    owed_val: [u8; NET_REC],
    owed_vlen: u16,

    doc: [u8; DOC],
    node_doc: [u8; NODE_DOC],
    pod: [u8; POD],
    tbl: [u8; TBL],
    rd: [u8; RD],
    nag: ReadNag,

    claimed: u32,
    /// Passes run, and those that asked for another; the last write's
    /// decided answer (0, or the store's errno); when the heartbeat last
    /// spoke, and what it said.
    passes: u32,
    agains: u32,
    write_rc: i32,
    hb_ms: u64,
    hb_sum: u32,
    released: u32,
    replaced: u32,
    refused: u32,

    /// An address controller on this node: `addr_ctl` frames in (in[1]),
    /// `addr_evt` events out (out[1]), and the addresses it drives here.
    addr_ctl_in: i32,
    addr_evt_out: i32,
    ctl_buf: [u8; 64],
    vh: VipHold,
}

/// The prefixes a change under which runs the pass again: the nodes (their
/// CIDRs, addresses, readiness), the Services (their load-balancer
/// addresses) and the claims on those. An endpoint moving, a request or a
/// sandbox going, is picked up by the periodic pass (`PASS_MS`): the store
/// holds a bounded number of subscriptions for the whole node.
const SUBS: usize = 5;
name_table!(
    SUB_PREFIXES = [
        b"/nodes/",
        b"/services/",
        b"/net-hold/",
        b"/lb-addr/",
        b"/net-vip/"
    ]
);

mod params_def {
    use super::ptr_copy;
    use super::State;
    use super::MAX_PARAM;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, node_name, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.node_len = n as u8;
                if n > 0 { ptr_copy(s.node.as_mut_ptr(), d, n); }
            };

        2, cluster_cidr, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.cl_len = n as u8;
                if n > 0 { ptr_copy(s.cl_txt.as_mut_ptr(), d, n); }
            };

        3, service_cidr, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.sv_len = n as u8;
                if n > 0 { ptr_copy(s.sv_txt.as_mut_ptr(), d, n); }
            };

        4, mtu, str, 0
            => |s, d, len| {
                let n = if len > 16 { 0 } else { len };
                s.mtu_len = n as u8;
                if n > 0 { ptr_copy(s.mtu_txt.as_mut_ptr(), d, n); }
            };

        5, graph_pods, str, 0
            => |s, d, len| {
                let n = if len > 4 { 0 } else { len };
                s.gp_len = n as u8;
                if n > 0 { ptr_copy(s.gp_txt.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

// ---- text and addresses ----

fn parse_num(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 10 {
        return None;
    }
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
    }
    (v <= u32::MAX as u64).then_some(v as u32)
}

fn parse_ip4(b: &[u8]) -> Option<u32> {
    let mut out = 0u32;
    let mut n = 0;
    for part in b.split(|&c| c == b'.') {
        let v = parse_num(part)?;
        if v > 255 || n == 4 {
            return None;
        }
        out = (out << 8) | v;
        n += 1;
    }
    (n == 4).then_some(out)
}

fn mask(len: u8) -> u32 {
    if len == 0 {
        0
    } else if len >= 32 {
        u32::MAX
    } else {
        u32::MAX << (32 - len as u32)
    }
}

/// `<a.b.c.d>/<n>`, as its network (host bits cleared) and length.
fn parse_cidr(b: &[u8]) -> Option<(u32, u8)> {
    let slash = b.iter().position(|&c| c == b'/')?;
    let a = parse_ip4(b.get(..slash)?)?;
    let n = parse_num(b.get(slash + 1..)?)?;
    if n > 32 {
        return None;
    }
    Some((a & mask(n as u8), n as u8))
}

fn in_cidr(addr: u32, c: (u32, u8)) -> bool {
    c.1 > 0 && addr & mask(c.1) == c.0
}

fn addr16(a: u32) -> [u8; wire::ADDR_LEN] {
    let mut o = [0u8; wire::ADDR_LEN];
    for (d, s) in o.iter_mut().zip(a.to_be_bytes()) {
        *d = s;
    }
    o
}

fn prefix(c: (u32, u8)) -> wire::Prefix {
    wire::Prefix {
        family: wire::FAM_IPV4,
        len: c.1,
        addr: addr16(c.0),
    }
}

fn put(dst: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    let d = dst.get_mut(at..end)?;
    for (o, &i) in d.iter_mut().zip(src.iter()) {
        *o = i;
    }
    Some(end)
}

fn put_dec(out: &mut [u8], at: usize, v: u64) -> Option<usize> {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        if let Some(t) = tmp.get_mut(i) {
            *t = b'0' + (u % 10) as u8;
        }
        u /= 10;
        if u == 0 || i == 0 {
            break;
        }
    }
    put(out, at, tmp.get(i..).unwrap_or(&[]))
}

fn put_ip(out: &mut [u8], mut at: usize, a: u32) -> Option<usize> {
    for (i, o) in a.to_be_bytes().iter().enumerate() {
        if i > 0 {
            at = put(out, at, b".")?;
        }
        at = put_dec(out, at, *o as u64)?;
    }
    Some(at)
}

fn put_cidr(out: &mut [u8], at: usize, c: (u32, u8)) -> Option<usize> {
    let at = put_ip(out, at, c.0)?;
    let at = put(out, at, b"/")?;
    put_dec(out, at, c.1 as u64)
}

fn fnv(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in b {
        h ^= c as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// `parts` joined into `out`; its length, or 0 when it does not fit.
fn cat(out: &mut [u8], parts: &[&[u8]]) -> usize {
    let mut n = 0usize;
    for p in parts {
        match put(out, n, p) {
            Some(e) => n = e,
            None => return 0,
        }
    }
    n
}

// READ's states and reasons by their values (`wire::state`, `wire::reason`).
name_table!(STATE_NAMES = [b"pending", b"realized", b"fenced", b"refused", b"unknown"]);
name_table!(
    REASON_NAMES = [
        b"",
        b"no_privilege",
        b"no_state_dir",
        b"host_forward_drop",
        b"table_full",
        b"conflict",
        b"unsupported",
    ]
);

fn state_word(s: u8) -> &'static [u8] {
    match STATE_NAMES.get(s as usize) {
        b"" => STATE_NAMES.get(wire::state::UNKNOWN as usize),
        w => w,
    }
}

fn reason_word(r: u8) -> &'static [u8] {
    REASON_NAMES.get(r as usize)
}

/// A Node's podCIDR, InternalIP and readiness.
fn node_facts(doc: &[u8]) -> ((u32, u8), u32, bool) {
    let cidr = j_get2(doc, b"spec", b"podCIDR")
        .and_then(parse_cidr)
        .filter(|c| c.1 > 0 && c.1 < 31)
        .unwrap_or((0, 0));
    let mut addr = 0u32;
    if let Some(addrs) = j_sub2(doc, b"status", b"addresses") {
        let mut k = 0usize;
        while let Some(a) = j_idx(addrs, k) {
            k += 1;
            if j_get1(a, b"type") == Some(&b"InternalIP"[..]) {
                addr = j_get1(a, b"address").and_then(parse_ip4).unwrap_or(0);
                break;
            }
        }
    }
    let ready = j_get2(doc, b"status", b"ready") == Some(&b"true"[..]);
    (cidr, addr, ready)
}

// ---- the store ----

/// Ask (or ask again) the owed write.
unsafe fn ask_owed(s: &mut State) {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let key = s.owed_key.get(..s.owed_klen as usize).unwrap_or(&[]);
    let a = if s.owed_del != 0 {
        delete_cond(&store, key, s.owed_pre, s.owed_rev)
    } else {
        let v = s.owed_val.get(..s.owed_vlen as usize).unwrap_or(&[]);
        put_cond(&store, key, v, s.owed_pre, s.owed_rev)
    };
    s.owed = (a == WriteAnswer::Pending) as u8;
    if let WriteAnswer::Decided(rc) = a {
        s.write_rc = rc;
    }
}

/// Write `key = val` (or delete it) under `pre`/`rev`, owed until the store
/// decides.
unsafe fn act(s: &mut State, key: &[u8], val: Option<&[u8]>, pre: u8, rev: u64) {
    let kl = key.len().min(MAX_KEY);
    for (d, &c) in s.owed_key.iter_mut().zip(key.iter().take(kl)) {
        *d = c;
    }
    s.owed_klen = kl as u16;
    match val {
        Some(v) => {
            let vl = v.len().min(NET_REC);
            for (d, &c) in s.owed_val.iter_mut().zip(v.iter().take(vl)) {
                *d = c;
            }
            s.owed_vlen = vl as u16;
            s.owed_del = 0;
        }
        None => {
            s.owed_vlen = 0;
            s.owed_del = 1;
        }
    }
    s.owed_pre = pre;
    s.owed_rev = rev;
    ask_owed(s);
}

fn key_of<'a>(buf: &'a mut [u8; MAX_KEY], parts: &[&[u8]]) -> &'a [u8] {
    let n = cat(buf, parts);
    buf.get(..n).unwrap_or(&[])
}

// ---- the tables ----

/// The node's segment, replaced when what it would hold changed.
unsafe fn apply_segment(s: &mut State, sys: &SyscallTable) {
    if s.cidr.1 == 0 {
        // No podCIDR (yet, or a renewal that has not carried it back): the
        // segment as it stands, never removed under the node's workloads.
        return;
    }
    let mut seg = wire::Segment::EMPTY;
    seg.id = SEGMENT_ID;
    seg.prefix = prefix(s.cidr);
    seg.gateway = addr16(s.cidr.0.wrapping_add(1));
    seg.mtu = s.mtu;
    seg.egress = wire::segment::EGRESS_TRANSLATED;
    let mut n = 0u8;
    for c in [s.cluster, s.services] {
        if c.1 > 0 {
            if let Some(slot) = seg.cluster.get_mut(n as usize) {
                *slot = prefix(c);
                n += 1;
            }
        }
    }
    seg.n_cluster = n;
    let mut arg =
        [0u8; 1 + wire::segment::HEAD + wire::MAX_CLUSTER_PREFIXES * wire::segment::CLUSTER];
    let Some(len) = wire::write_segment(&mut arg, 1, &seg) else {
        return;
    };
    if let Some(c) = arg.get_mut(0) {
        *c = 1;
    }
    let len = len + 1;
    let h = fnv(arg.get(..len).unwrap_or(&[]));
    if s.seg_set && h == s.seg_hash && s.seg_rc == 0 {
        return;
    }
    let rc = (sys.provider_call)(-1, wire::SEGMENTS, arg.as_mut_ptr(), len);
    s.seg_rc = rc;
    if rc == 0 {
        s.seg_hash = h;
        s.seg_set = true;
        s.seg_n = 1;
        s.seg_cidr = s.cidr;
        s.replaced = s.replaced.wrapping_add(1);
    } else {
        s.refused = s.refused.wrapping_add(1);
    }
}

/// Peer routes to every other node with a podCIDR and an address.
unsafe fn apply_routes(s: &mut State, sys: &SyscallTable, store: &Store) -> Pass {
    let mut n = 0usize;
    let mut walk = ListWalk::new(NODES);
    let mut k = [0u8; MAX_KEY];
    let me_len = s.node_len as usize;
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        for (d, &c) in k.iter_mut().zip(key.iter().take(kl)) {
            *d = c;
        }
        let name = k.get(NODES.len()..kl).unwrap_or(&[]);
        if name == s.node.get(..me_len).unwrap_or(&[]) {
            continue;
        }
        let Some(dl) = get_value(store, k.get(..kl).unwrap_or(&[]), &mut s.node_doc) else {
            continue;
        };
        let (cidr, addr, _) = node_facts(s.node_doc.get(..dl).unwrap_or(&[]));
        if cidr.1 == 0 || addr == 0 || n >= MAX_PEERS {
            continue;
        }
        let r = wire::Route {
            prefix: prefix(cidr),
            carriage: wire::peer::CARRIAGE_DIRECT,
            peer: addr16(addr),
        };
        if wire::write_route(&mut s.tbl, 2 + n * wire::peer::SIZE, &r).is_some() {
            n += 1;
        }
    }
    if store.refusal().is_some() || walk.failed {
        return Pass::Failed;
    }
    if store.waiting() {
        return Pass::Again;
    }
    for (d, c) in s.tbl.iter_mut().zip((n as u16).to_le_bytes()) {
        *d = c;
    }
    let len = 2 + n * wire::peer::SIZE;
    let h = fnv(s.tbl.get(..len).unwrap_or(&[]));
    if s.route_set && h == s.route_hash && s.route_rc == 0 {
        return Pass::Idle;
    }
    let rc = (sys.provider_call)(-1, wire::ROUTES, s.tbl.as_mut_ptr(), len);
    s.route_rc = rc;
    if rc == 0 {
        s.route_hash = h;
        s.route_set = true;
        s.route_n = n as u16;
        s.replaced = s.replaced.wrapping_add(1);
    } else {
        s.refused = s.refused.wrapping_add(1);
    }
    Pass::Idle
}

/// The Service `<ns>/<name>`'s endpoints on this node: `.0` one in its
/// podCIDR or its own address (a Pod of its own, or one on its network),
/// `.1` one at its own address (a Pod on its network: what the node itself
/// terminates).
unsafe fn local_endpoints(s: &State, store: &Store, svc: &[u8]) -> (bool, bool) {
    let mut p = [0u8; MAX_KEY];
    let pl = cat(&mut p, &[SLICES, svc, b"/"]);
    if pl == 0 {
        return (false, false);
    }
    let mut any = false;
    let mut walk = ListWalk::new(p.get(..pl).unwrap_or(&[]));
    let mut k = [0u8; MAX_KEY];
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        for (d, &c) in k.iter_mut().zip(key.iter().take(kl)) {
            *d = c;
        }
        // A row is `ip=<ip>;state=<ready|notready|serving|terminating>;…`
        // (endpoints.plan): only a ready one is an endpoint traffic may be
        // brought to, and its address is its `ip=`.
        let mut v = [0u8; 512];
        let Some(vl) = get_value(store, k.get(..kl).unwrap_or(&[]), &mut v) else {
            continue;
        };
        let slice = v.get(..vl).unwrap_or(&[]);
        if field(slice, b"state=") != Some(&b"ready"[..]) {
            continue;
        }
        let Some(a) = field(slice, b"ip=").and_then(parse_ip4) else {
            continue;
        };
        if a != 0 && a == s.own_addr {
            return (true, true);
        }
        any |= in_cidr(a, s.cidr);
    }
    (any, false)
}

/// Whether node `name` is ready (absent: not).
unsafe fn node_ready(s: &mut State, store: &Store, name: &[u8]) -> bool {
    let mut k = [0u8; MAX_KEY];
    let key = key_of(&mut k, &[NODES, name]);
    match get_value(store, key, &mut s.node_doc) {
        Some(dl) => node_facts(s.node_doc.get(..dl).unwrap_or(&[])).2,
        None => false,
    }
}

/// Who a `node=<name>` record names, for `vip_hold::decide`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Holder {
    None,
    Me,
    /// Another node, and whether it is ready.
    Other(bool),
}

/// Read `key` (`node=<name>`) and its revision; `None` when the store said
/// to stop (a read that waits, or was refused).
unsafe fn read_holder(
    s: &mut State,
    store: &Store,
    key: &[u8],
    me: &[u8],
) -> Option<(Holder, u64)> {
    let mut cv = [0u8; 128];
    let got = get_value_rev(store, key, &mut cv);
    if store.stopped() {
        return None;
    }
    let Some((cl, rev)) = got else {
        return Some((Holder::None, 0));
    };
    let holder = field(cv.get(..cl).unwrap_or(&[]), b"node=").unwrap_or(&[]);
    if holder == me {
        return Some((Holder::Me, rev));
    }
    let mut hb = [0u8; MAX_PARAM];
    let hl = cat(&mut hb, &[holder]);
    let ready = node_ready(s, store, hb.get(..hl).unwrap_or(&[]));
    if store.stopped() {
        return None;
    }
    Some((Holder::Other(ready), rev))
}

/// The load-balancer addresses: each claimed, given up or taken over as the
/// manifest says — and, where an address controller drives one here, as
/// it does (`_shared/vip_hold.rs`) — one write a pass. Fills `want_*`.
unsafe fn held_pass(s: &mut State, store: &Store) -> Pass {
    s.want_n = 0;
    s.want_over = false;
    let me_len = s.node_len as usize;
    let mut me = [0u8; MAX_PARAM];
    for (d, &c) in me.iter_mut().zip(s.node.iter().take(me_len)) {
        *d = c;
    }
    let me = me.get(..me_len).unwrap_or(&[]);
    let mut walk = ListWalk::new(SERVICES);
    let mut k = [0u8; MAX_KEY];
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        for (d, &c) in k.iter_mut().zip(key.iter().take(kl)) {
            *d = c;
        }
        let Some(dl) = get_value(store, k.get(..kl).unwrap_or(&[]), &mut s.doc) else {
            continue;
        };
        let local = {
            let doc = s.doc.get(..dl).unwrap_or(&[]);
            if j_get2(doc, b"spec", b"type") != Some(&b"LoadBalancer"[..]) {
                continue;
            }
            j_get2(doc, b"spec", b"externalTrafficPolicy") == Some(&b"Local"[..])
        };
        // The address svc_alloc gave it (`/lb-addr/<ns>/<name>` = `ip=…`),
        // not its status: the record is svc_alloc's own, the status follows
        // it.
        let mut ak = [0u8; MAX_KEY];
        let akl = cat(
            &mut ak,
            &[LB_ADDRS, k.get(SERVICES.len()..kl).unwrap_or(&[])],
        );
        let mut av = [0u8; 64];
        let addr = match get_value(store, ak.get(..akl).unwrap_or(&[]), &mut av) {
            Some(n) => field(av.get(..n).unwrap_or(&[]), b"ip=")
                .and_then(parse_ip4)
                .unwrap_or(0),
            None => 0,
        };
        if addr == 0 {
            continue;
        }
        let have = s
            .want_addr
            .iter()
            .take(s.want_n as usize)
            .any(|&a| a == addr);
        if have {
            continue;
        }
        if s.want_n as usize >= MAX_HELD {
            s.want_over = true;
            continue;
        }
        let svc = k.get(SERVICES.len()..kl).unwrap_or(&[]);
        let mut svcb = [0u8; MAX_KEY];
        let sl = cat(&mut svcb, &[svc]);
        let svc = svcb.get(..sl).unwrap_or(&[]);
        // A node that cannot announce (bare metal: `deliver` only) answers
        // only for what it terminates itself — a Service whose endpoint is
        // its own address; one backed by its workloads it cannot take in
        // from outside, and says so (`unsupported`).
        let deliver_only = s.caps & wire::caps::HOLD_ANNOUNCE == 0;
        let (any_local, own_local) = if local || deliver_only {
            local_endpoints(s, store, svc)
        } else {
            (false, false)
        };
        let unsupported = deliver_only && !own_local;
        let eligible = s.ready && s.cidr.1 > 0 && !unsupported && (!local || any_local);
        if store.stopped() {
            break;
        }
        // The address's own-hold claim and the controller's mark, with
        // their writers' readiness: what `vip_hold::decide` rules on.
        let mut ck = [0u8; MAX_KEY];
        let ckl = put(&mut ck, 0, HOLDS)
            .and_then(|p| put_dec(&mut ck, p, addr as u64))
            .unwrap_or(0);
        let ck = ck.get(..ckl).unwrap_or(&[]);
        let mut mk = [0u8; MAX_KEY];
        let mkl = put(&mut mk, 0, MARKS)
            .and_then(|p| put_dec(&mut mk, p, addr as u64))
            .unwrap_or(0);
        let mk = mk.get(..mkl).unwrap_or(&[]);
        let Some((claim, claim_rev)) = read_holder(s, store, ck, me) else {
            break;
        };
        let Some((mark, mark_rev)) = read_holder(s, store, mk, me) else {
            break;
        };
        let vh_claim = match claim {
            Holder::None => VhClaim::Free,
            Holder::Me => VhClaim::Ours,
            Holder::Other(ready) => VhClaim::Other { ready },
        };
        let vh_mark = match mark {
            Holder::None => VhMark::None,
            Holder::Me => VhMark::Ours,
            Holder::Other(ready) => VhMark::Other { ready },
        };
        let d = s.vh.decide(addr, eligible, vh_claim, vh_mark);
        let mut v = [0u8; MAX_PARAM + 8];
        let vl = cat(&mut v, &[b"node=", me]);
        let mine = v.get(..vl).unwrap_or(&[]);
        let wrote = match d.write {
            VhWrite::None => false,
            VhWrite::Claim => {
                act(s, ck, Some(mine), PRE_ABSENT, 0);
                s.claimed = s.claimed.wrapping_add(1);
                true
            }
            VhWrite::Unclaim => {
                // Ours, or a not-ready node's: on the revision judged.
                act(s, ck, None, PRE_ETAG, claim_rev);
                s.released = s.released.wrapping_add(1);
                true
            }
            VhWrite::Mark => {
                if mark == Holder::None {
                    act(s, mk, Some(mine), PRE_ABSENT, 0);
                } else {
                    // A not-ready node's mark, taken over on its revision.
                    act(s, mk, Some(mine), PRE_ETAG, mark_rev);
                }
                true
            }
            VhWrite::Unmark => {
                act(s, mk, None, PRE_ETAG, mark_rev);
                true
            }
        };
        if wrote {
            s.vh.settle(addr, d.write);
            return Pass::Again;
        }
        let armed = d.row == Some(true);
        let i = s.want_n as usize;
        if let (Some(a), Some(m)) = (s.want_addr.get_mut(i), s.want_armed.get_mut(i)) {
            *a = addr;
            // 2: not this node's to answer for, ever (`unsupported`).
            *m = if unsupported { 2 } else { armed as u8 };
            s.want_n += 1;
        }
    }
    if store.refusal().is_some() || walk.failed {
        return Pass::Failed;
    }
    if store.waiting() {
        return Pass::Again;
    }
    // A claim or a mark on an address no Service has any more goes (any
    // node's: its holder may be gone with it).
    for pfx in [HOLDS, MARKS] {
        if s.want_over {
            break;
        }
        let mut walk = ListWalk::new(pfx);
        while let Some(key) = walk.next(store) {
            let kl = key.len().min(MAX_KEY);
            for (d, &c) in k.iter_mut().zip(key.iter().take(kl)) {
                *d = c;
            }
            let Some(a) = k.get(pfx.len()..kl).and_then(parse_num) else {
                continue;
            };
            let live = s.want_addr.iter().take(s.want_n as usize).any(|&w| w == a);
            if !live {
                let mut kb = [0u8; MAX_KEY];
                let kbl = cat(&mut kb, &[k.get(..kl).unwrap_or(&[])]);
                act(s, kb.get(..kbl).unwrap_or(&[]), None, PRE_ANY, 0);
                s.released = s.released.wrapping_add(1);
                return Pass::Again;
            }
        }
        if store.refusal().is_some() || walk.failed {
            return Pass::Failed;
        }
        if store.waiting() {
            return Pass::Again;
        }
    }
    Pass::Idle
}

/// The held-address table, replaced when what it would hold changed. A
/// provider realizing neither mode is not asked: its addresses read
/// `unsupported` in the status.
unsafe fn apply_held(s: &mut State, sys: &SyscallTable) {
    let mode = if s.caps & wire::caps::HOLD_ANNOUNCE != 0 {
        wire::held::MODE_ANNOUNCE
    } else if s.caps & wire::caps::HOLD_DELIVER != 0 {
        wire::held::MODE_DELIVER
    } else {
        return;
    };
    let mut arg = [0u8; 2 + MAX_HELD * wire::held::SIZE];
    let mut addrs = [0u32; MAX_HELD];
    let mut armed = [0u8; MAX_HELD];
    let mut n = 0usize;
    for i in 0..s.want_n as usize {
        let (Some(&a), Some(&m)) = (s.want_addr.get(i), s.want_armed.get(i)) else {
            break;
        };
        if m > 1 {
            continue;
        }
        if let (Some(da), Some(dm)) = (addrs.get_mut(n), armed.get_mut(n)) {
            *da = a;
            *dm = m;
        }
        let h = wire::Held {
            family: wire::FAM_IPV4,
            mode,
            state_req: if m != 0 {
                wire::held::ARMED
            } else {
                wire::held::FENCED
            },
            addr: addr16(a),
        };
        wire::write_held(&mut arg, 2 + n * wire::held::SIZE, &h);
        n += 1;
    }
    for (d, c) in arg.iter_mut().zip((n as u16).to_le_bytes()) {
        *d = c;
    }
    let len = 2 + n * wire::held::SIZE;
    let h = fnv(arg.get(..len).unwrap_or(&[]));
    if s.held_set && h == s.held_hash && s.held_rc == 0 {
        return;
    }
    let rc = (sys.provider_call)(-1, wire::HOLD, arg.as_mut_ptr(), len);
    s.held_rc = rc;
    if rc == 0 {
        s.held_hash = h;
        s.held_set = true;
        s.held_n = n as u16;
        s.held_addr = addrs;
        s.held_armed = armed;
        s.replaced = s.replaced.wrapping_add(1);
    } else {
        s.refused = s.refused.wrapping_add(1);
    }
}

/// The address each of the node's graph-backed Pods is given, asked of its
/// Pod CIDR as pod_resolve asks for a container Pod's: a metal sandbox with
/// neither lease nor request gets a request (`node=;cidr=;pod=`, the Pod's
/// name from its /pod-specs/). Its CREATE waits for the lease
/// (sandbox.plan `mspec`). One write a pass.
unsafe fn request_pass(s: &mut State, store: &Store) -> Pass {
    if !s.graph_pods || !s.seg_up || s.seg_cidr.1 == 0 {
        return Pass::Idle;
    }
    let me_len = s.node_len as usize;
    let mut me = [0u8; MAX_PARAM];
    for (d, &c) in me.iter_mut().zip(s.node.iter().take(me_len)) {
        *d = c;
    }
    let me = me.get(..me_len).unwrap_or(&[]);
    let mut walk = ListWalk::new(METAL_SANDBOXES);
    let mut k = [0u8; MAX_KEY];
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        for (d, &c) in k.iter_mut().zip(key.iter().take(kl)) {
            *d = c;
        }
        let uid = k.get(METAL_SANDBOXES.len()..kl).unwrap_or(&[]);
        let mut ub = [0u8; MAX_KEY];
        let ul = cat(&mut ub, &[uid]);
        let uid = ub.get(..ul).unwrap_or(&[]);
        let mut lk = [0u8; MAX_KEY];
        if exists(store, key_of(&mut lk, &[LEASES, uid])) {
            continue;
        }
        let mut rk = [0u8; MAX_KEY];
        if exists(store, key_of(&mut rk, &[REQUESTS, uid])) {
            continue;
        }
        let mut sk = [0u8; MAX_KEY];
        let mut spec = [0u8; 2048];
        let Some(sl) = get_value(store, key_of(&mut sk, &[b"/pod-specs/", uid]), &mut spec) else {
            continue;
        };
        if store.stopped() {
            break;
        }
        let spec = spec.get(..sl).unwrap_or(&[]);
        let Some(pod) = field(spec, b"pod=") else {
            continue;
        };
        if field(spec, b"desired=") == Some(b"deleted") {
            continue;
        }
        // A hostNetwork Pod shares the node's network: no address of its own.
        if field(spec, b"hostnet=") == Some(b"true") {
            continue;
        }
        let mut v = [0u8; 512];
        let mut c = [0u8; 24];
        let cl = put_cidr(&mut c, 0, s.seg_cidr).unwrap_or(0);
        let vl = cat(
            &mut v,
            &[
                b"node=",
                me,
                b";cidr=",
                c.get(..cl).unwrap_or(&[]),
                b";pod=",
                pod,
            ],
        );
        let mut kb = [0u8; MAX_KEY];
        let kbl = cat(&mut kb, &[REQUESTS, uid]);
        act(
            s,
            kb.get(..kbl).unwrap_or(&[]),
            Some(v.get(..vl).unwrap_or(&[])),
            PRE_ABSENT,
            0,
        );
        s.claimed = s.claimed.wrapping_add(1);
        return Pass::Again;
    }
    if store.refusal().is_some() || walk.failed {
        return Pass::Failed;
    }
    if store.waiting() {
        return Pass::Again;
    }
    Pass::Idle
}

/// Whether the workload of sandbox `uid` is gone: no sandbox, or one
/// destroyed (its records outlive it until the Pod's tomb ends).
unsafe fn sandbox_gone(store: &Store, uid: &[u8]) -> bool {
    let mut sk = [0u8; MAX_KEY];
    let mut mk = [0u8; MAX_KEY];
    let has = exists(store, key_of(&mut sk, &[SANDBOXES, uid]))
        || exists(store, key_of(&mut mk, &[METAL_SANDBOXES, uid]));
    if !has {
        return !store.stopped();
    }
    let mut tk = [0u8; MAX_KEY];
    let mut st = [0u8; 256];
    match get_value(store, key_of(&mut tk, &[SANDBOX_STATUS, uid]), &mut st) {
        Some(n) => field(st.get(..n).unwrap_or(&[]), b"state=") == Some(b"destroyed"),
        None => false,
    }
}

/// This node's Pod-IP requests whose Pod is gone, and its sandbox with it:
/// the request, then (its lease left with no request) the lease. One write
/// a pass.
unsafe fn release_pass(s: &mut State, store: &Store) -> Pass {
    let me_len = s.node_len as usize;
    let mut me = [0u8; MAX_PARAM];
    for (d, &c) in me.iter_mut().zip(s.node.iter().take(me_len)) {
        *d = c;
    }
    let me = me.get(..me_len).unwrap_or(&[]);
    let mut walk = ListWalk::new(REQUESTS);
    let mut k = [0u8; MAX_KEY];
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        for (d, &c) in k.iter_mut().zip(key.iter().take(kl)) {
            *d = c;
        }
        let mut v = [0u8; 512];
        let Some(vl) = get_value(store, k.get(..kl).unwrap_or(&[]), &mut v) else {
            continue;
        };
        let v = v.get(..vl).unwrap_or(&[]);
        if field(v, b"node=") != Some(me) {
            continue;
        }
        let uid = k.get(REQUESTS.len()..kl).unwrap_or(&[]);
        let mut ub = [0u8; MAX_KEY];
        let ul = cat(&mut ub, &[uid]);
        let uid = ub.get(..ul).unwrap_or(&[]);
        if let Some(pod) = field(v, b"pod=") {
            let mut pk = [0u8; MAX_KEY];
            let pk = key_of(&mut pk, &[PODS, pod]);
            match get_value(store, pk, &mut s.pod) {
                Some(n) => {
                    let doc = s.pod.get(..n).unwrap_or(&[]);
                    if j_get2(doc, b"metadata", b"uid") == Some(uid) {
                        continue;
                    }
                }
                None if store.stopped() => break,
                // Past the buffer reads as absent: a Pod that is there is
                // never released for it.
                None if exists(store, pk) => continue,
                None => {}
            }
        }
        if store.stopped() {
            break;
        }
        if !sandbox_gone(store, uid) || store.stopped() {
            if store.stopped() {
                break;
            }
            continue;
        }
        // The request first: a lease whose request still waits would be
        // given again (ipam_pod). Its lease on the next pass.
        let mut rk = [0u8; MAX_KEY];
        let rk = key_of(&mut rk, &[REQUESTS, uid]);
        act(s, rk, None, PRE_ANY, 0);
        s.released = s.released.wrapping_add(1);
        return Pass::Again;
    }
    if store.refusal().is_some() || walk.failed {
        return Pass::Failed;
    }
    if store.waiting() {
        return Pass::Again;
    }
    // Then a lease of this node's CIDR with no request left: a request is
    // made before its lease and never released while it is needed, so its
    // Pod is gone — the lease goes once its workload is.
    let c = if s.seg_set { s.seg_cidr } else { s.cidr };
    if c.1 == 0 {
        return Pass::Idle;
    }
    let mut walk = ListWalk::new(LEASES);
    while let Some(key) = walk.next(store) {
        let kl = key.len().min(MAX_KEY);
        for (d, &ch) in k.iter_mut().zip(key.iter().take(kl)) {
            *d = ch;
        }
        let mut v = [0u8; 64];
        let Some(vl) = get_value(store, k.get(..kl).unwrap_or(&[]), &mut v) else {
            continue;
        };
        let Some(a) = parse_ip4(v.get(..vl).unwrap_or(&[])) else {
            continue;
        };
        if !in_cidr(a, c) {
            continue;
        }
        let mut ub = [0u8; MAX_KEY];
        let ul = cat(&mut ub, &[k.get(LEASES.len()..kl).unwrap_or(&[])]);
        let uid = ub.get(..ul).unwrap_or(&[]);
        let mut rk = [0u8; MAX_KEY];
        if exists(store, key_of(&mut rk, &[REQUESTS, uid])) || !sandbox_gone(store, uid) {
            continue;
        }
        if store.stopped() {
            break;
        }
        let mut lk = [0u8; MAX_KEY];
        let lk = key_of(&mut lk, &[LEASES, uid]);
        act(s, lk, None, PRE_ANY, 0);
        s.released = s.released.wrapping_add(1);
        return Pass::Again;
    }
    if store.refusal().is_some() || walk.failed {
        return Pass::Failed;
    }
    if store.waiting() {
        return Pass::Again;
    }
    Pass::Idle
}

// ---- what took effect ----

/// READ, folded into the two status records; each written when it changed.
/// True when it wrote (one write at a time).
unsafe fn status(s: &mut State, sys: &SyscallTable) -> bool {
    let rc = (sys.provider_call)(-1, wire::READ, s.rd.as_mut_ptr(), RD);
    let gen = wire::generation(&s.rd).unwrap_or(0);
    let mut seg_state = wire::state::UNKNOWN;
    let mut seg_reason = wire::reason::NONE;
    let mut routes_ok = 0u32;
    let mut route_why = wire::reason::NONE;
    let mut held_state = [wire::state::UNKNOWN; MAX_HELD];
    let mut held_reason = [wire::reason::NONE; MAX_HELD];
    if rc > 8 {
        let n = (rc as usize - 8) / wire::entry::SIZE;
        for i in 0..n {
            let Some(e) = wire::entry_at(&s.rd, 8 + i * wire::entry::SIZE) else {
                continue;
            };
            match e.kind {
                wire::entry::KIND_SEGMENT if e.index == 0 => {
                    seg_state = e.state;
                    seg_reason = e.reason;
                }
                wire::entry::KIND_ROUTE => {
                    if e.state == wire::state::REALIZED {
                        routes_ok += 1;
                    } else if route_why == wire::reason::NONE {
                        route_why = e.reason;
                    }
                }
                wire::entry::KIND_HELD => {
                    let j = e.index as usize;
                    if let (Some(st), Some(rs)) = (held_state.get_mut(j), held_reason.get_mut(j)) {
                        *st = e.state;
                        *rs = e.reason;
                    }
                }
                _ => {}
            }
        }
    }
    // The node's segment is in place: in the table, and not refused.
    let seg_up = s.caps & wire::caps::SEGMENTS != 0
        && s.seg_set
        && s.seg_rc == 0
        && s.seg_n == 1
        && (seg_state == wire::state::PENDING || seg_state == wire::state::REALIZED);
    s.seg_up = seg_up;
    let mut r = [0u8; NET_REC];
    let mut p = put(&mut r, 0, if seg_up { b"seg=1" } else { b"seg=0" }).unwrap_or(0);
    let c = if s.seg_set { s.seg_cidr } else { s.cidr };
    if c.1 > 0 {
        p = put(&mut r, p, b";cidr=")
            .and_then(|e| put_cidr(&mut r, e, c))
            .and_then(|e| put(&mut r, e, b";gw="))
            .and_then(|e| put_ip(&mut r, e, c.0.wrapping_add(1)))
            .unwrap_or(p);
    }
    p = put(&mut r, p, b";segment=").unwrap_or(p);
    if s.seg_set {
        p = put(&mut r, p, state_word(seg_state)).unwrap_or(p);
        let why = reason_word(seg_reason);
        if !why.is_empty() {
            p = put(&mut r, p, b":")
                .and_then(|e| put(&mut r, e, why))
                .unwrap_or(p);
        }
    } else if s.seg_rc != 0 {
        p = put(&mut r, p, b"rc=-")
            .and_then(|e| put_dec(&mut r, e, s.seg_rc.unsigned_abs() as u64))
            .unwrap_or(p);
    } else {
        p = put(&mut r, p, b"none").unwrap_or(p);
    }
    p = put(&mut r, p, b";routes=")
        .and_then(|e| put_dec(&mut r, e, routes_ok as u64))
        .and_then(|e| put(&mut r, e, b"/"))
        .and_then(|e| put_dec(&mut r, e, s.route_n as u64))
        .unwrap_or(p);
    if s.route_rc != 0 {
        p = put(&mut r, p, b":rc=-")
            .and_then(|e| put_dec(&mut r, e, s.route_rc.unsigned_abs() as u64))
            .unwrap_or(p);
    } else if !reason_word(route_why).is_empty() {
        p = put(&mut r, p, b":")
            .and_then(|e| put(&mut r, e, reason_word(route_why)))
            .unwrap_or(p);
    }
    p = put(&mut r, p, b";held=").unwrap_or(p);
    let hold_caps = s.caps & (wire::caps::HOLD_ANNOUNCE | wire::caps::HOLD_DELIVER) != 0;
    for i in 0..usize::from(s.want_n).min(MAX_HELD) {
        let a = s.want_addr.get(i).copied().unwrap_or(0);
        let m = s.want_armed.get(i).copied().unwrap_or(0);
        // Its entry in the table as last replaced, by address.
        let at = (0..usize::from(s.held_n).min(MAX_HELD))
            .find(|&j| s.held_addr.get(j).copied() == Some(a));
        let (st, why): (&[u8], &[u8]) = if !hold_caps || m > 1 {
            (
                state_word(wire::state::REFUSED),
                reason_word(wire::reason::UNSUPPORTED),
            )
        } else if let Some(j) = at {
            (
                state_word(held_state.get(j).copied().unwrap_or(wire::state::UNKNOWN)),
                reason_word(held_reason.get(j).copied().unwrap_or(0)),
            )
        } else {
            (
                state_word(wire::state::PENDING),
                reason_word(wire::reason::NONE),
            )
        };
        let q = (if i > 0 { put(&mut r, p, b",") } else { Some(p) })
            .and_then(|e| put_ip(&mut r, e, a))
            .and_then(|e| put(&mut r, e, if m == 1 { b":armed:" } else { b":fenced:" }))
            .and_then(|e| put(&mut r, e, st))
            .and_then(|e| {
                if why.is_empty() {
                    Some(e)
                } else {
                    put(&mut r, e, b":").and_then(|e| put(&mut r, e, why))
                }
            });
        match q {
            Some(e) => p = e,
            None => break,
        }
    }
    p = put(&mut r, p, b";caps=")
        .and_then(|e| put_dec(&mut r, e, s.caps as u64))
        .and_then(|e| put(&mut r, e, b";gen="))
        .and_then(|e| put_dec(&mut r, e, gen))
        .unwrap_or(p);
    if !seg_up {
        let why: &[u8] = if s.cidr.1 == 0 && !s.seg_set {
            b"no-pod-cidr"
        } else if s.caps & wire::caps::SEGMENTS == 0 {
            // The provider does not attach workloads here (no state
            // directory, no privilege): READ's reason says which.
            match reason_word(seg_reason) {
                b"" => b"no-segments",
                w => w,
            }
        } else if s.seg_rc != 0 {
            b"refused"
        } else {
            match reason_word(seg_reason) {
                b"" => b"not-in-effect",
                w => w,
            }
        };
        p = put(&mut r, p, b";why=")
            .and_then(|e| put(&mut r, e, why))
            .unwrap_or(p);
    }
    // The node's own copy first: a Pod is given its own address only on
    // /node-net/ seg=1, and its CREATE reads this one.
    let sb: &[u8] = if seg_up && s.graph_pods {
        b"seg=1;graph=1"
    } else if seg_up {
        b"seg=1"
    } else {
        b"seg=0"
    };
    if !s.sb_said || s.sb_rec.get(..s.sb_len as usize) != Some(sb) {
        // Owed until the store decides it (asked again before anything
        // else): said once.
        act(s, SANDBOX_NET, Some(sb), PRE_ANY, 0);
        let l = sb.len().min(16);
        for (d, &c) in s.sb_rec.iter_mut().zip(sb.iter().take(l)) {
            *d = c;
        }
        s.sb_len = l as u8;
        s.sb_said = true;
        return true;
    }
    let rec = r.get(..p).unwrap_or(&[]);
    if !s.net_said || s.net_rec.get(..s.net_len as usize) != Some(rec) {
        let me_len = s.node_len as usize;
        let mut k = [0u8; MAX_KEY];
        let kl = cat(&mut k, &[NODE_NET, s.node.get(..me_len).unwrap_or(&[])]);
        let mut kb = [0u8; MAX_KEY];
        for (d, &c) in kb.iter_mut().zip(k.iter().take(kl)) {
            *d = c;
        }
        act(s, kb.get(..kl).unwrap_or(&[]), Some(rec), PRE_ANY, 0);
        for (d, &c) in s.net_rec.iter_mut().zip(rec.iter()) {
            *d = c;
        }
        s.net_len = rec.len() as u16;
        s.net_said = true;
        return true;
    }
    false
}

/// One pass: the node, its segment and routes, the held addresses, the
/// requests to release.
unsafe fn pass(s: &mut State) -> Pass {
    let sys = &*s.syscalls;
    let store = Store::plain(sys, &s.nag);
    let mut caps = [0u8; 4];
    s.caps = if (sys.provider_call)(-1, wire::CAPS, caps.as_mut_ptr(), 4) == 0 {
        u32::from_le_bytes(caps)
    } else {
        0
    };
    let me_len = s.node_len as usize;
    let mut k = [0u8; MAX_KEY];
    let kl = cat(&mut k, &[NODES, s.node.get(..me_len).unwrap_or(&[])]);
    match get_value(&store, k.get(..kl).unwrap_or(&[]), &mut s.node_doc) {
        Some(dl) => {
            let (cidr, addr, ready) = node_facts(s.node_doc.get(..dl).unwrap_or(&[]));
            s.cidr = cidr;
            s.own_addr = addr;
            s.ready = ready;
        }
        None if store.refusal().is_some() => return Pass::Failed,
        None if store.waiting() => return Pass::Again,
        None => {
            s.cidr = (0, 0);
            s.ready = false;
        }
    }
    apply_segment(s, sys);
    match apply_routes(s, sys, &store) {
        Pass::Idle => {}
        other => return other,
    }
    match held_pass(s, &store) {
        Pass::Idle => {}
        other => return other,
    }
    apply_held(s, sys);
    match request_pass(s, &store) {
        Pass::Idle => {}
        other => return other,
    }
    release_pass(s, &store)
}

/// `[node_net] hb passes=… again=… claimed=… released=… replaced=…
/// refused=… write_rc=…`, every 10 s while any moved, so a writer stuck in
/// a loop (a write the store keeps refusing) is visible.
unsafe fn heartbeat(s: &mut State, sys: &SyscallTable, now: u64) {
    let sum = s
        .agains
        .wrapping_add(s.claimed)
        .wrapping_add(s.released)
        .wrapping_add(s.replaced)
        .wrapping_add(s.refused);
    if sum == s.hb_sum || now.wrapping_sub(s.hb_ms) < 10_000 {
        return;
    }
    s.hb_ms = now;
    s.hb_sum = sum;
    let mut m = [0u8; 192];
    let mut p = put(&mut m, 0, b"[node_net] hb passes=").unwrap_or(0);
    for (label, v) in [
        (&b""[..], u64::from(s.passes)),
        (&b" again="[..], u64::from(s.agains)),
        (&b" claimed="[..], u64::from(s.claimed)),
        (&b" released="[..], u64::from(s.released)),
        (&b" replaced="[..], u64::from(s.replaced)),
        (&b" refused="[..], u64::from(s.refused)),
    ] {
        p = put(&mut m, p, label)
            .and_then(|e| put_dec(&mut m, e, v))
            .unwrap_or(p);
    }
    p = put(
        &mut m,
        p,
        if s.write_rc < 0 {
            b" write_rc=-"
        } else {
            b" write_rc="
        },
    )
    .and_then(|e| put_dec(&mut m, e, u64::from(s.write_rc.unsigned_abs())))
    .unwrap_or(p);
    dev_log(sys, 2, m.as_ptr(), p);
}

// ---- module ABI ----

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
    _in_chan: i32,
    _out_chan: i32,
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
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.event = -1;
        s.subscribed = 0;
        s.dirty = 1;
        s.pass_ms = 0;
        s.status_ms = 0;
        s.node_len = 0;
        s.cl_len = 0;
        s.sv_len = 0;
        s.mtu_len = 0;
        s.gp_len = 0;
        s.caps = 0;
        s.cidr = (0, 0);
        s.own_addr = 0;
        s.ready = false;
        s.seg_set = false;
        s.seg_rc = 0;
        s.seg_n = 0;
        s.seg_cidr = (0, 0);
        s.seg_up = false;
        s.route_set = false;
        s.route_rc = 0;
        s.route_n = 0;
        s.held_set = false;
        s.held_rc = 0;
        s.held_n = 0;
        s.want_n = 0;
        s.want_over = false;
        s.net_said = false;
        s.net_len = 0;
        s.sb_said = false;
        s.sb_len = 0;
        s.owed = 0;
        s.claimed = 0;
        s.released = 0;
        s.replaced = 0;
        s.refused = 0;
        s.passes = 0;
        s.agains = 0;
        s.write_rc = 0;
        s.hb_ms = 0;
        s.hb_sum = 0;
        core::ptr::write(&mut s.vh, VipHold::new());
        let sys0 = &*s.syscalls;
        s.addr_ctl_in = dev_channel_port(sys0, 0, 0);
        s.addr_evt_out = dev_channel_port(sys0, 1, 1);
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        let sys = &*s.syscalls;
        if s.node_len == 0 {
            let m = b"[node_net] FAULT: `node_name` is required";
            dev_log(sys, 1, m.as_ptr(), m.len());
            return -3;
        }
        let cl = s.cl_txt.get(..s.cl_len as usize).unwrap_or(&[]);
        let sv = s.sv_txt.get(..s.sv_len as usize).unwrap_or(&[]);
        let mt = s.mtu_txt.get(..s.mtu_len as usize).unwrap_or(&[]);
        let cluster = if cl.is_empty() {
            Some((0x0AF4_0000, 16))
        } else {
            parse_cidr(cl)
        };
        let services = if sv.is_empty() {
            Some((0x0A60_0000, 16))
        } else {
            parse_cidr(sv)
        };
        let mtu = if mt.is_empty() {
            Some(1500)
        } else {
            parse_num(mt)
        };
        s.graph_pods = s.gp_txt.get(..s.gp_len as usize) == Some(b"1");
        match (cluster, services, mtu) {
            (Some(c), Some(v), Some(m)) if c.1 > 0 && (576..=65535).contains(&m) => {
                s.cluster = c;
                s.services = v;
                s.mtu = m as u16;
            }
            _ => {
                let m = b"[node_net] FAULT: `cluster_cidr`/`service_cidr` is not <a.b.c.d>/<n>, or `mtu` is not 576..65535";
                dev_log(sys, 1, m.as_ptr(), m.len());
                return -3;
            }
        }
        0
    }
}

/// The address controller's `addr_ctl` frames, one at a time while events have
/// room, then the events it is owed. True when a frame moved an address.
unsafe fn vip_ctl(s: &mut State, sys: &SyscallTable) -> bool {
    let mut moved = false;
    while let Some(e) = s.vh.event() {
        if s.addr_evt_out < 0 {
            s.vh.event_written();
            continue;
        }
        if (sys.channel_write)(s.addr_evt_out, e.as_ptr(), e.len()) != e.len() as i32 {
            return moved;
        }
        s.vh.event_written();
    }
    if s.addr_ctl_in < 0 {
        return moved;
    }
    for _ in 0..4 {
        if !s.vh.ev_room() {
            break;
        }
        let n = (sys.channel_peek)(s.addr_ctl_in, s.ctl_buf.as_mut_ptr(), 3);
        if n < 3 {
            break;
        }
        let total = 3 + u16::from_le_bytes([s.ctl_buf[1], s.ctl_buf[2]]) as usize;
        if total > s.ctl_buf.len() {
            (sys.channel_discard)(s.addr_ctl_in, total);
            continue;
        }
        if (sys.channel_peek)(s.addr_ctl_in, s.ctl_buf.as_mut_ptr(), total) < total as i32 {
            break;
        }
        (sys.channel_discard)(s.addr_ctl_in, total);
        let addr = s.ctl_buf.get(3..19).and_then(vh_addr).unwrap_or(0);
        // The claim as the last pass found it decides adoption.
        let ours = s
            .want_addr
            .iter()
            .zip(s.want_armed.iter())
            .take(s.want_n as usize)
            .any(|(&a, &m)| a == addr && m == 1);
        let claim = if ours { VhClaim::Ours } else { VhClaim::Free };
        let mut token = [0u8; 16];
        if dev_csprng_fill(sys, token.as_mut_ptr(), token.len()) != 0 {
            // No entropy: an install would carry a predictable token.
            token = [0; 16];
        }
        let f = s.ctl_buf;
        if token == [0; 16] && f[0] == VH_ADDR_ADD {
            let mut p = [0u8; 18];
            p[..16].copy_from_slice(&f[3..19]);
            p[16] = VH_ADDR_ADD;
            p[17] = 4; // refusal::NO_ENTROPY
            let mut ev = [0u8; 21];
            ev[0] = VH_MSG_REFUSED;
            ev[1] = 18;
            ev[3..].copy_from_slice(&p);
            if s.addr_evt_out >= 0 {
                let _ = (sys.channel_write)(s.addr_evt_out, ev.as_ptr(), ev.len());
            }
            continue;
        }
        s.vh.on_frame(&f[..total], claim, token);
        moved = true;
    }
    while let Some(e) = s.vh.event() {
        if s.addr_evt_out >= 0
            && (sys.channel_write)(s.addr_evt_out, e.as_ptr(), e.len()) != e.len() as i32
        {
            break;
        }
        s.vh.event_written();
    }
    moved
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
        // One kernel event for every subscription. Without one (or with a
        // subscription refused, said at ERROR) the periodic pass still runs.
        if s.event < 0 && s.subscribed == 0 {
            s.event = store_event(sys);
            if s.event < 0 {
                let m = b"[node_net] ERROR no kernel event - changes will NOT wake this module, only its periodic pass";
                dev_log(sys, 1, m.as_ptr(), m.len());
                s.subscribed = SUBS as u8;
            }
        }
        while (s.subscribed as usize) < SUBS {
            if not_yet(store_subscribe(
                sys,
                SUB_PREFIXES.get(s.subscribed as usize),
                s.event,
                0,
            )) {
                return 0;
            }
            s.subscribed += 1;
        }
        if store_take(sys, s.event, |_| {}) > 0 {
            s.dirty = 1;
        }
        if vip_ctl(s, sys) {
            s.dirty = 1;
        }
        if s.owed != 0 {
            ask_owed(s);
            if s.owed != 0 {
                return 0;
            }
            s.dirty = 1;
        }
        let now = dev_millis(sys);
        if now.wrapping_sub(s.pass_ms) >= PASS_MS {
            s.dirty = 1;
        }
        if s.dirty != 0 {
            s.dirty = 0;
            s.pass_ms = now;
            s.passes = s.passes.wrapping_add(1);
            match pass(s) {
                Pass::Again => {
                    s.dirty = 1;
                    s.agains = s.agains.wrapping_add(1);
                }
                Pass::Failed => {
                    let m = b"[node_net] ERROR a pass FAILED (the store refused a read, or a listing failed) - run again by the periodic pass";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                }
                Pass::Idle => {
                    s.status_ms = 0;
                }
            }
            if s.owed != 0 {
                return 0;
            }
        }
        heartbeat(s, sys, now);
        if s.dirty == 0 && now.wrapping_sub(s.status_ms) >= STATUS_MS {
            if status(s, sys) {
                // One record a step; the other on the next.
                s.status_ms = 0;
            } else {
                s.status_ms = now;
            }
        }
        0
    }
}
