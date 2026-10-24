//! The cluster's DNS: Service names answered from the store.
//!
//! See `manifest.toml` for what is answered and how. A query is one
//! datagram in and one out, on the datagram surface of the node's network
//! provider: a server endpoint on `port`, and an ephemeral one for the
//! queries forwarded upstream.

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
include!("../../../target/fluxor/fluxor-abi/sdk/cores/datagram_endpoint.rs");
include!("../_shared/json.rs");
include!("../_shared/store.rs");

const OBJ_PUT: u32 = 0x1420;
const OBJ_GET: u32 = 0x1421;
const OBJ_RANGE_GET: u32 = 0x1423;
const OBJ_DELETE: u32 = 0x1424;
const OBJ_CLOSE: u32 = 0x1425;
const NS_LIST: u32 = 0x1302;
const MAX_KEY: usize = 256;
const MAX_VALUE: usize = 2048;

const MAX_PARAM: usize = 64;
/// Where this resolver's address is kept for the Pods' resolv.conf.
const CLUSTER_DNS_KEY: &[u8] = b"/cluster-dns";
/// The Service fronting it (`service_ip`), as Kubernetes names it.
const DNS_NS_KEY: &[u8] = b"/namespaces/kube-system";
const DNS_SVC_KEY: &[u8] = b"/services/kube-system/kube-dns";
const DNS_SLICE_PREFIX: &[u8] = b"/endpointslices/kube-system/kube-dns/cluster-dns-";
const DNS_CLAIM_PREFIX: &[u8] = b"/ipam/svc/";
const DNS_OWNER: &[u8] = b"owner=kube-system/kube-dns;ip=";
/// The classic UDP ceiling: a query or an answer past it is not served.
const DNS_MAX: usize = 512;
const DNS_HDR: usize = 12;
/// Framing scratch: a datagram, its address prefix and the frame header.
const NET_BUF: usize = DNS_MAX + 64;
/// A Service object or an Endpoints document, read whole.
const DOC_MAX: usize = 16 * 1024;
/// Queries held while the store answers EAGAIN.
const HELD: usize = 8;
/// Queries forwarded upstream and not yet answered.
const FWD: usize = 16;
/// How long a forwarded query waits for its answer.
const FWD_MS: u64 = 5000;
/// Frames drained from the network per step: the provider's output is fanned
/// to every reader of it, and a reader that falls behind holds them all.
const DRAIN: usize = 32;

const T_A: u16 = 1;
const T_CNAME: u16 = 5;
const T_SRV: u16 = 33;
const T_ANY: u16 = 255;
/// What a Service is to DNS.
const K_CLUSTER: u8 = 1;
const K_HEADLESS: u8 = 2;
const K_EXTERNAL: u8 = 3;
const K_NOADDR: u8 = 4;
const C_IN: u16 = 1;
const RC_OK: u8 = 0;
const RC_FORMERR: u8 = 1;
const RC_SERVFAIL: u8 = 2;
const RC_NXDOMAIN: u8 = 3;
const RC_REFUSED: u8 = 5;

#[derive(Clone, Copy)]
struct Held {
    used: u8,
    ip: u32,
    port: u16,
    len: u16,
    pkt: [u8; DNS_MAX],
}

const HELD_EMPTY: Held = Held {
    used: 0,
    ip: 0,
    port: 0,
    len: 0,
    pkt: [0; DNS_MAX],
};

#[derive(Clone, Copy)]
struct Fwd {
    used: u8,
    up_id: u16,
    client_id: u16,
    ip: u32,
    port: u16,
    deadline: u64,
}

const FWD_EMPTY: Fwd = Fwd {
    used: 0,
    up_id: 0,
    client_id: 0,
    ip: 0,
    port: 0,
    deadline: 0,
};

/// One of a Service's ports, as DNS names it (`_<name>._<proto>`).
#[derive(Clone, Copy)]
struct PortInfo {
    name: [u8; 16],
    nl: u8,
    udp: u8,
    port: u16,
    /// A numbered targetPort (0: named, or the same as `port`).
    tport: u16,
}

const PORT_EMPTY: PortInfo = PortInfo {
    name: [0; 16],
    nl: 0,
    udp: 0,
    port: 0,
    tport: 0,
};

/// The Service a query names, as read.
struct SvcInfo {
    kind: u8,
    cip: u32,
    ext: [u8; 253],
    ext_len: u16,
    ports: [PortInfo; 8],
    nports: u8,
}

/// What a name in the domain is.
enum Lookup {
    /// The store has not answered yet: held, asked again.
    Wait,
    /// The store refused the read for good: answered SERVFAIL, never held.
    Failed,
    /// No such name.
    None,
    /// The name, with these addresses (possibly none).
    Addrs(usize),
}

#[repr(C)]
struct State {
    syscalls: *const SyscallTable,
    net_in: i32,
    net_out: i32,
    server: DatagramEndpoint,
    upstream_ep: DatagramEndpoint,

    port: u16,
    upstream_port: u16,
    upstream: u32,
    ttl: u32,
    domain: [u8; MAX_PARAM],
    domain_len: u16,
    upstream_txt: [u8; 16],
    upstream_txt_len: u8,

    held: [Held; HELD],
    fwd: [Fwd; FWD],
    next_id: u16,

    addrs: [u32; 32],
    svc: SvcInfo,
    net_buf: [u8; NET_BUF],
    out: [u8; DNS_MAX],
    doc: [u8; DOC_MAX],
    nag: ReadNag,

    answered: u32,
    forwarded: u32,
    refused_said: u8,
    /// Where Pods reach this resolver (`address`), owed to the store at
    /// `/cluster-dns` until it holds it: what a Pod's resolv.conf names.
    address: [u8; 16],
    address_len: u8,
    address_owed: u8,
    /// The ClusterIP of the Service fronting this resolver (`service_ip`):
    /// what Pods name instead of `address` once published (`Publish`).
    service_ip: [u8; 16],
    service_ip_len: u8,
    /// How far the Service's publication has come (`PUB_*`).
    publish: u8,
}

/// The Service's publication, in order: each step a write asked until the
/// store decides it, then the next.
const PUB_NS: u8 = 0;
/// The ClusterIP's claim read first: another Service's, and nothing is
/// written. (The Service is written before the claim: the claims' release
/// frees a claim whose owner it cannot find.)
const PUB_CHECK: u8 = 1;
const PUB_SVC: u8 = 2;
const PUB_CLAIM: u8 = 3;
const PUB_SLICE: u8 = 4;
/// Done, or given up (the address is another Service's): `/cluster-dns`
/// says whichever this resolver is reached at.
const PUB_DONE: u8 = 5;

mod params_def {
    use super::p_u16;
    use super::p_u32;
    use super::ptr_copy;
    use super::State;
    use super::MAX_PARAM;
    use super::SCHEMA_MAX;

    define_params! {
        State;

        1, port, u16, 53
            => |s, d, len| { s.port = p_u16(d, len, 0, 53); };

        2, zone, str, 0
            => |s, d, len| {
                let n = if len > MAX_PARAM { 0 } else { len };
                s.domain_len = n as u16;
                if n > 0 { ptr_copy(s.domain.as_mut_ptr(), d, n); }
            };

        3, ttl, u32, 5
            => |s, d, len| { s.ttl = p_u32(d, len, 0, 5); };

        4, upstream, str, 0
            => |s, d, len| {
                let n = if len > 16 { 0 } else { len };
                s.upstream_txt_len = n as u8;
                if n > 0 { ptr_copy(s.upstream_txt.as_mut_ptr(), d, n); }
            };

        5, upstream_port, u16, 53
            => |s, d, len| { s.upstream_port = p_u16(d, len, 0, 53); };

        6, address, str, 0
            => |s, d, len| {
                let n = if len > 16 { 0 } else { len };
                s.address_len = n as u8;
                if n > 0 { ptr_copy(s.address.as_mut_ptr(), d, n); }
            };

        7, service_ip, str, 0
            => |s, d, len| {
                let n = if len > 16 { 0 } else { len };
                s.service_ip_len = n as u8;
                if n > 0 { ptr_copy(s.service_ip.as_mut_ptr(), d, n); }
            };
    }
}

#[inline(always)]
unsafe fn ptr_copy(dst: *mut u8, src: *const u8, n: usize) {
    core::ptr::copy_nonoverlapping(src, dst, n);
}

fn parse_u8(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 3 {
        return None;
    }
    let mut v = 0u32;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u32;
    }
    (v <= 255).then_some(v)
}

/// `a.b.c.d` as its number (`a` the high byte).
fn parse_ip4(b: &[u8]) -> Option<u32> {
    let mut out = 0u32;
    let mut n = 0;
    for part in b.split(|&c| c == b'.') {
        if n == 4 {
            return None;
        }
        out = (out << 8) | parse_u8(part)?;
        n += 1;
    }
    (n == 4).then_some(out)
}

fn lower(c: u8) -> u8 {
    if c.is_ascii_uppercase() {
        c + 32
    } else {
        c
    }
}

/// The question's name, dotted and lowercased into `name`, and the offset
/// just past its type and class; with them, (qtype, qclass).
fn question(pkt: &[u8], name: &mut [u8; 256]) -> Option<(usize, usize, u16, u16)> {
    let mut at = DNS_HDR;
    let mut nl = 0usize;
    loop {
        let l = *pkt.get(at)? as usize;
        at += 1;
        if l == 0 {
            break;
        }
        // A compression pointer has no place in a question.
        if l > 63 {
            return None;
        }
        if nl > 0 {
            *name.get_mut(nl)? = b'.';
            nl += 1;
        }
        for i in 0..l {
            *name.get_mut(nl)? = lower(*pkt.get(at + i)?);
            nl += 1;
        }
        at += l;
    }
    let qt = u16::from_be_bytes([*pkt.get(at)?, *pkt.get(at + 1)?]);
    let qc = u16::from_be_bytes([*pkt.get(at + 2)?, *pkt.get(at + 3)?]);
    Some((nl, at + 4, qt, qc))
}

/// Ends with `.suffix` (or is it), case already folded.
fn under<'a>(name: &'a [u8], suffix: &[u8]) -> Option<&'a [u8]> {
    if name == suffix {
        return Some(&name[..0]);
    }
    let n = name.len();
    let s = suffix.len();
    if n > s + 1 && name.get(n - s..)? == suffix && *name.get(n - s - 1)? == b'.' {
        return name.get(..n - s - 1);
    }
    None
}

/// Copy `src` into `dst` at `at`; the new end.
fn put(dst: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    let d = dst.get_mut(at..end)?;
    for (o, &i) in d.iter_mut().zip(src.iter()) {
        *o = i;
    }
    Some(end)
}

/// The header and question of the answer to `pkt` (its question ending at
/// `qend`), no records yet. Its length (0: nothing to answer with).
fn begin(out: &mut [u8; DNS_MAX], pkt: &[u8], qend: usize, rcode: u8, auth: bool) -> usize {
    let Some(q) = pkt.get(..qend) else {
        return 0;
    };
    let Some(at) = put(out, 0, q) else {
        return 0;
    };
    let rd = pkt.get(2).copied().unwrap_or(0) & 0x01;
    // QR, opcode QUERY, AA as given, RD echoed; RA: this resolver recurses
    // for the names it forwards.
    out[2] = 0x80 | if auth { 0x04 } else { 0 } | rd;
    out[3] = 0x80 | (rcode & 0x0F);
    // One question; answers counted as they are added; no authority, no
    // additional (an OPT the query carried is not echoed: the answer stays
    // within 512 bytes).
    out[4] = 0;
    out[5] = 1;
    for b in out[6..12].iter_mut() {
        *b = 0;
    }
    at
}

/// One answer record, owned by the question's name, at `at`: the new end,
/// or `at` with TC set when it does not fit in 512 bytes.
fn record(out: &mut [u8; DNS_MAX], at: usize, rtype: u16, ttl: u32, rdata: &[u8]) -> usize {
    let mut h = [0u8; 12];
    h[0] = 0xC0;
    h[1] = DNS_HDR as u8;
    let t = rtype.to_be_bytes();
    h[2] = t[0];
    h[3] = t[1];
    h[5] = C_IN as u8;
    let tt = ttl.to_be_bytes();
    h[6] = tt[0];
    h[7] = tt[1];
    h[8] = tt[2];
    h[9] = tt[3];
    let l = (rdata.len() as u16).to_be_bytes();
    h[10] = l[0];
    h[11] = l[1];
    match put(out, at, &h).and_then(|e| put(out, e, rdata)) {
        Some(e) => {
            let n = u16::from_be_bytes([out[6], out[7]])
                .wrapping_add(1)
                .to_be_bytes();
            out[6] = n[0];
            out[7] = n[1];
            e
        }
        None => {
            out[2] |= 0x02;
            at
        }
    }
}

/// A header, the question and an A record per address. Its length.
fn answer(
    out: &mut [u8; DNS_MAX],
    pkt: &[u8],
    qend: usize,
    rcode: u8,
    auth: bool,
    addrs: &[u32],
    ttl: u32,
) -> usize {
    let mut at = begin(out, pkt, qend, rcode, auth);
    if at == 0 {
        return 0;
    }
    for &a in addrs {
        at = record(out, at, T_A, ttl, &a.to_be_bytes());
    }
    at
}

/// `dotted` as wire labels (no compression) into `dst` at `at`: the new end.
fn wire_name(dst: &mut [u8], mut at: usize, dotted: &[u8]) -> Option<usize> {
    for label in dotted.split(|&c| c == b'.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        *dst.get_mut(at)? = label.len() as u8;
        at = put(dst, at + 1, label)?;
    }
    *dst.get_mut(at)? = 0;
    Some(at + 1)
}

/// `a.b.c.d` written dotted into `dst` at `at` (with `sep` between the
/// octets): the new end.
fn ip_text(dst: &mut [u8], mut at: usize, a: u32, sep: u8) -> Option<usize> {
    for (i, o) in a.to_be_bytes().iter().enumerate() {
        if i > 0 {
            *dst.get_mut(at)? = sep;
            at += 1;
        }
        let mut d = [0u8; 3];
        let mut n = 0;
        let mut v = *o;
        loop {
            d[n] = b'0' + v % 10;
            n += 1;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        for k in (0..n).rev() {
            *dst.get_mut(at)? = d[k];
            at += 1;
        }
    }
    Some(at)
}

/// `a-b-c-d` as the address it names.
fn parse_dashed(b: &[u8]) -> Option<u32> {
    let mut out = 0u32;
    let mut n = 0;
    for part in b.split(|&c| c == b'-') {
        if n == 4 {
            return None;
        }
        out = (out << 8) | parse_u8(part)?;
        n += 1;
    }
    (n == 4).then_some(out)
}

/// The decimal number `b` spells, or None.
fn parse_num(b: &[u8]) -> Option<u32> {
    if b.is_empty() || b.len() > 5 {
        return None;
    }
    let mut v = 0u32;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u32;
    }
    Some(v)
}

/// A Service, read from the store into `s.svc`. None: no such Service.
unsafe fn load(s: &mut State, svc: &[u8], ns: &[u8]) -> Lookup {
    let sys = &*s.syscalls;
    let mut key = [0u8; MAX_KEY];
    let Some(p) = put(&mut key, 0, b"/services/")
        .and_then(|p| put(&mut key, p, ns))
        .and_then(|p| put(&mut key, p, b"/"))
        .and_then(|p| put(&mut key, p, svc))
    else {
        return Lookup::None;
    };
    let n = match get_value_live(sys, &s.nag, &key[..p], &mut s.doc) {
        Got::Pending => return Lookup::Wait,
        Got::Refused(_) => return Lookup::Failed,
        Got::Absent => return Lookup::None,
        Got::Found(n) => n,
    };
    let doc = &s.doc[..n];
    let info = &mut s.svc;
    info.nports = 0;
    info.ext_len = 0;
    info.cip = 0;
    let ext = j_get2(doc, b"spec", b"externalName").unwrap_or(&[]);
    let cip = j_get2(doc, b"spec", b"clusterIP").unwrap_or(&[]);
    info.kind = if j_get2(doc, b"spec", b"type").unwrap_or(&[]) == b"ExternalName" {
        if !ext.is_empty() && ext.len() <= info.ext.len() {
            info.ext[..ext.len()].copy_from_slice(ext);
            info.ext_len = ext.len() as u16;
        }
        K_EXTERNAL
    } else if cip.is_empty() || cip == b"None" {
        K_HEADLESS
    } else {
        match parse_ip4(cip) {
            Some(a) => {
                info.cip = a;
                K_CLUSTER
            }
            // An address this resolver cannot name (IPv6): the name exists.
            None => K_NOADDR,
        }
    };
    if let Some(ports) = j_sub2(doc, b"spec", b"ports") {
        let mut i = 0;
        while let Some(el) = j_idx(ports, i) {
            i += 1;
            let k = info.nports as usize;
            if k == info.ports.len() {
                break;
            }
            let name = j_get1(el, b"name").unwrap_or(&[]);
            let pi = &mut info.ports[k];
            if name.len() > pi.name.len() {
                continue;
            }
            pi.name[..name.len()].copy_from_slice(name);
            pi.nl = name.len() as u8;
            pi.udp = (j_get1(el, b"protocol").unwrap_or(&[]) == b"UDP") as u8;
            pi.port = parse_num(j_get1(el, b"port").unwrap_or(&[])).unwrap_or(0) as u16;
            pi.tport = parse_num(j_get1(el, b"targetPort").unwrap_or(&[])).unwrap_or(0) as u16;
            info.nports += 1;
        }
    }
    Lookup::Addrs(0)
}

/// A headless Service's ready backends, from its Endpoints document, into
/// `s.addrs`.
unsafe fn backends(s: &mut State, svc: &[u8], ns: &[u8]) -> Lookup {
    let sys = &*s.syscalls;
    let mut key = [0u8; MAX_KEY];
    let Some(p) = put(&mut key, 0, b"/endpoints/")
        .and_then(|p| put(&mut key, p, ns))
        .and_then(|p| put(&mut key, p, b"/"))
        .and_then(|p| put(&mut key, p, svc))
    else {
        return Lookup::Addrs(0);
    };
    let n = match get_value_live(sys, &s.nag, &key[..p], &mut s.doc) {
        Got::Pending => return Lookup::Wait,
        Got::Refused(_) => return Lookup::Failed,
        Got::Absent => return Lookup::Addrs(0),
        Got::Found(n) => n,
    };
    let mut k = 0usize;
    for item in s.doc[..n].split(|&c| c == b',') {
        let ip = match item.iter().position(|&c| c == b'=') {
            Some(i) => &item[i + 1..],
            None => item,
        };
        if let Some(a) = parse_ip4(ip) {
            if k < s.addrs.len() && s.addrs[..k].iter().all(|&x| x != a) {
                s.addrs[k] = a;
                k += 1;
            }
        }
    }
    Lookup::Addrs(k)
}

/// Send `len` bytes of `s.out` to a client from the server endpoint.
unsafe fn reply(s: &mut State, ip: u32, port: u16, len: usize) {
    if len == 0 {
        return;
    }
    let sys = &*s.syscalls;
    let sent = s.server.send_to(
        sys,
        s.net_out,
        ip,
        port,
        s.out.as_ptr(),
        len,
        s.net_buf.as_mut_ptr(),
        NET_BUF,
    );
    if sent > 0 {
        s.answered = s.answered.wrapping_add(1);
    }
}

/// A fresh upstream transaction id: drawn, never the client's own.
unsafe fn fresh_id(s: &mut State) -> u16 {
    let sys = &*s.syscalls;
    let mut b = [0u8; 2];
    if dev_csprng_fill(sys, b.as_mut_ptr(), 2) != 0 {
        s.next_id = s.next_id.wrapping_add(0x9E37) ^ (dev_micros(sys) as u16);
        return s.next_id;
    }
    u16::from_le_bytes(b)
}

/// Forward a query outside the domain; REFUSED with no upstream, SERVFAIL
/// with no slot to remember it by.
unsafe fn forward(s: &mut State, ip: u32, port: u16, pkt: &[u8], qend: usize) {
    if s.upstream == 0 || !s.upstream_ep.is_ready() {
        let n = answer(&mut s.out, pkt, qend, RC_REFUSED, false, &[], 0);
        reply(s, ip, port, n);
        return;
    }
    let sys = &*s.syscalls;
    let now = dev_millis(sys);
    let Some(slot) = s.fwd.iter().position(|f| f.used == 0 || f.deadline <= now) else {
        let n = answer(&mut s.out, pkt, qend, RC_SERVFAIL, false, &[], 0);
        reply(s, ip, port, n);
        return;
    };
    let id = fresh_id(s);
    let Some(len) = put(&mut s.out, 0, pkt) else {
        return;
    };
    let client_id = u16::from_be_bytes([s.out[0], s.out[1]]);
    let ib = id.to_be_bytes();
    s.out[0] = ib[0];
    s.out[1] = ib[1];
    let sent = s.upstream_ep.send_to(
        sys,
        s.net_out,
        s.upstream,
        s.upstream_port,
        s.out.as_ptr(),
        len,
        s.net_buf.as_mut_ptr(),
        NET_BUF,
    );
    if sent == 0 {
        return; // backpressure: the client asks again
    }
    s.fwd[slot] = Fwd {
        used: 1,
        up_id: id,
        client_id,
        ip,
        port,
        deadline: now + FWD_MS,
    };
    s.forwarded = s.forwarded.wrapping_add(1);
}

/// An upstream answer: relayed to the client that asked, its id restored.
unsafe fn relay(s: &mut State, ip: u32, port: u16, pkt: &[u8]) {
    if ip != s.upstream || port != s.upstream_port || pkt.len() < DNS_HDR {
        return;
    }
    let id = u16::from_be_bytes([pkt[0], pkt[1]]);
    let Some(i) = s.fwd.iter().position(|f| f.used != 0 && f.up_id == id) else {
        return;
    };
    let f = s.fwd[i];
    s.fwd[i].used = 0;
    let Some(len) = put(&mut s.out, 0, pkt) else {
        return;
    };
    let cb = f.client_id.to_be_bytes();
    s.out[0] = cb[0];
    s.out[1] = cb[1];
    reply(s, f.ip, f.port, len);
}

/// Answer a query. False when the store has not answered: hold it.
unsafe fn query(s: &mut State, ip: u32, port: u16, pkt: &[u8]) -> bool {
    if pkt.len() < DNS_HDR || (pkt[2] & 0x80) != 0 {
        return true; // not a query: nothing to answer
    }
    let opcode = (pkt[2] >> 3) & 0x0F;
    let qd = u16::from_be_bytes([pkt[4], pkt[5]]);
    let mut name = [0u8; 256];
    let parsed = question(pkt, &mut name);
    let Some((nl, qend, qt, qc)) = parsed.filter(|_| opcode == 0 && qd == 1) else {
        let end = DNS_HDR.min(pkt.len());
        let n = answer(&mut s.out, pkt, end, RC_FORMERR, false, &[], 0);
        if n >= DNS_HDR {
            s.out[4] = 0;
            s.out[5] = 0;
        }
        reply(s, ip, port, n);
        return true;
    };
    let mut domain = [0u8; MAX_PARAM];
    let dl = s.domain_len as usize;
    domain[..dl].copy_from_slice(&s.domain[..dl]);
    let Some(rest) = under(&name[..nl], &domain[..dl]) else {
        forward(s, ip, port, pkt, qend);
        return true;
    };
    // The names a Service has (`<zone>` stripped):
    //   <svc>.<ns>.svc                 the Service
    //   _<port>._<proto>.<svc>.<ns>.svc  a named port of it (SRV)
    //   <a-b-c-d>.<svc>.<ns>.svc       a headless Service's backend
    // and nothing else in the zone is a name this resolver holds.
    let mut parts = [&[][..]; 6];
    let mut np = 0usize;
    for p in rest.split(|&c| c == b'.') {
        if np == parts.len() {
            np += 1;
            break;
        }
        parts[np] = p;
        np += 1;
    }
    let ttl = s.ttl;
    let svc_at = match np {
        3..=5 if parts[np - 1] == b"svc" => np - 3,
        _ => usize::MAX,
    };
    let (svc, ns) = match svc_at {
        usize::MAX => (&[][..], &[][..]),
        i => (parts[i], parts[i + 1]),
    };
    let srv = svc_at == 2 && parts[0].first() == Some(&b'_') && parts[1].first() == Some(&b'_');
    let pod = svc_at == 1;
    if svc.is_empty() || ns.is_empty() || svc.len() > 63 || ns.len() > 63 || (svc_at == 2 && !srv) {
        let n = answer(&mut s.out, pkt, qend, RC_NXDOMAIN, true, &[], ttl);
        reply(s, ip, port, n);
        return true;
    }
    let mut sv = [0u8; 64];
    let mut nv = [0u8; 64];
    sv[..svc.len()].copy_from_slice(svc);
    nv[..ns.len()].copy_from_slice(ns);
    let (sv, nv) = (&sv[..svc.len()], &nv[..ns.len()]);
    match load(s, sv, nv) {
        Lookup::Wait => return false,
        Lookup::Failed => {
            let n = answer(&mut s.out, pkt, qend, RC_SERVFAIL, false, &[], 0);
            reply(s, ip, port, n);
            return true;
        }
        Lookup::None => {
            let n = answer(&mut s.out, pkt, qend, RC_NXDOMAIN, true, &[], ttl);
            reply(s, ip, port, n);
            return true;
        }
        Lookup::Addrs(_) => {}
    }
    let kind = s.svc.kind;
    let mut k = 0usize;
    if kind == K_HEADLESS && (pod || srv || qt == T_A || qt == T_ANY) {
        match backends(s, sv, nv) {
            Lookup::Wait => return false,
            Lookup::Failed => {
                let n = answer(&mut s.out, pkt, qend, RC_SERVFAIL, false, &[], 0);
                reply(s, ip, port, n);
                return true;
            }
            Lookup::Addrs(n) => k = n,
            Lookup::None => {}
        }
    }
    let a_type = qc == C_IN && (qt == T_A || qt == T_ANY);
    let mut at;
    if pod {
        // A headless Service's backend, by its address: it, when it is one.
        let want = match parse_dashed(parts[0]) {
            Some(a) if kind == K_HEADLESS && !s.addrs[..k].iter().all(|&x| x != a) => a,
            _ => {
                let n = answer(&mut s.out, pkt, qend, RC_NXDOMAIN, true, &[], ttl);
                reply(s, ip, port, n);
                return true;
            }
        };
        at = begin(&mut s.out, pkt, qend, RC_OK, true);
        if at > 0 && a_type {
            at = record(&mut s.out, at, T_A, ttl, &want.to_be_bytes());
        }
    } else if srv {
        // `_<name>._<proto>`: the Service's port of that name and protocol.
        let pname = &parts[0][1..];
        let udp = match parts[1] {
            b"_tcp" => 0u8,
            b"_udp" => 1u8,
            _ => 2u8,
        };
        let info = &s.svc;
        let found = info.ports[..info.nports as usize]
            .iter()
            .find(|p| p.nl > 0 && &p.name[..p.nl as usize] == pname && p.udp == udp)
            .copied();
        let Some(pi) = found.filter(|_| kind == K_CLUSTER || kind == K_HEADLESS) else {
            let n = answer(&mut s.out, pkt, qend, RC_NXDOMAIN, true, &[], ttl);
            reply(s, ip, port, n);
            return true;
        };
        at = begin(&mut s.out, pkt, qend, RC_OK, true);
        if at > 0 && qc == C_IN && (qt == T_SRV || qt == T_ANY) {
            let dl = s.domain_len as usize;
            let mut zone = [0u8; MAX_PARAM];
            zone[..dl].copy_from_slice(&s.domain[..dl]);
            // The target: the Service's own name (a ClusterIP), or each
            // backend's (`<a-b-c-d>.<svc>.<ns>.svc.<zone>`) at its port.
            let n_targets = if kind == K_CLUSTER { 1 } else { k };
            for t in 0..n_targets {
                let mut target = [0u8; 256];
                let mut tl = 0usize;
                if kind == K_HEADLESS {
                    match ip_text(&mut target, 0, s.addrs[t], b'-')
                        .and_then(|e| put(&mut target, e, b"."))
                    {
                        Some(e) => tl = e,
                        None => continue,
                    }
                }
                let Some(tl) = put(&mut target, tl, sv)
                    .and_then(|e| put(&mut target, e, b"."))
                    .and_then(|e| put(&mut target, e, nv))
                    .and_then(|e| put(&mut target, e, b".svc."))
                    .and_then(|e| put(&mut target, e, &zone[..dl]))
                else {
                    continue;
                };
                let tport = if kind == K_HEADLESS && pi.tport != 0 {
                    pi.tport
                } else {
                    pi.port
                };
                let mut rd = [0u8; 6 + 256];
                rd[2] = 0;
                rd[3] = 100; // weight: one target or equal shares
                let pb = tport.to_be_bytes();
                rd[4] = pb[0];
                rd[5] = pb[1];
                let Some(rl) = wire_name(&mut rd, 6, &target[..tl]) else {
                    continue;
                };
                at = record(&mut s.out, at, T_SRV, ttl, &rd[..rl]);
            }
        }
    } else {
        at = begin(&mut s.out, pkt, qend, RC_OK, true);
        if at > 0 && kind == K_EXTERNAL {
            // ExternalName: the name it aliases, whatever was asked.
            let el = s.svc.ext_len as usize;
            let mut rd = [0u8; 258];
            if el > 0 && qc == C_IN {
                let ext = s.svc.ext;
                if let Some(rl) = wire_name(&mut rd, 0, &ext[..el]) {
                    at = record(&mut s.out, at, T_CNAME, ttl, &rd[..rl]);
                }
            }
        } else if at > 0 && a_type {
            if kind == K_CLUSTER {
                let a = s.svc.cip;
                at = record(&mut s.out, at, T_A, ttl, &a.to_be_bytes());
            } else if kind == K_HEADLESS {
                for t in 0..k {
                    let a = s.addrs[t];
                    at = record(&mut s.out, at, T_A, ttl, &a.to_be_bytes());
                }
            }
        }
    }
    reply(s, ip, port, at);
    true
}

/// Hold a query the store has not answered; SERVFAIL when nothing can.
unsafe fn hold(s: &mut State, ip: u32, port: u16, pkt: &[u8]) {
    if let Some(i) = s.held.iter().position(|h| h.used == 0) {
        let h = &mut s.held[i];
        if put(&mut h.pkt, 0, pkt).is_some() {
            h.used = 1;
            h.ip = ip;
            h.port = port;
            h.len = pkt.len() as u16;
            return;
        }
    }
    let n = answer(&mut s.out, pkt, pkt.len(), RC_SERVFAIL, false, &[], 0);
    reply(s, ip, port, n);
}

// ---- module ABI ----

/// What making a key hold a value came to.
enum Ensured {
    /// It holds it (written now, or already).
    Done,
    /// Not decided yet: asked again next step, byte-identical.
    Wait,
    /// Create-only and the key exists with another value: its length (in
    /// the caller's scratch, `cur`).
    Taken(usize),
    /// Refused for good.
    Failed,
}

/// Make `key` hold `val`: read, and write only when it differs —
/// create-only (`ABSENT`) when `create`, so a key another writer holds is
/// never overwritten (answered [`Ensured::Taken`]).
unsafe fn ensure(store: &Store, key: &[u8], val: &[u8], create: bool) -> Ensured {
    let mut cur = [0u8; 512];
    let held = get_value(store, key, &mut cur);
    if store.refusal().is_some() {
        return Ensured::Failed;
    }
    if store.waiting() {
        return Ensured::Wait;
    }
    if let Some(n) = held {
        if cur.get(..n) == Some(val) {
            return Ensured::Done;
        }
        if create {
            return Ensured::Taken(n);
        }
    }
    let a = if create {
        put_cond(store, key, val, PRE_ABSENT, 0)
    } else {
        put_value(store, key, val)
    };
    match a {
        WriteAnswer::Pending => Ensured::Wait,
        WriteAnswer::Decided(rc) if not_yet(rc) => Ensured::Wait,
        WriteAnswer::Decided(0) => Ensured::Done,
        // Lost to a writer between the read and the create: read again.
        WriteAnswer::Decided(ERR_EEXIST) => Ensured::Wait,
        WriteAnswer::Decided(_) => Ensured::Failed,
    }
}

/// The digits of `v`, at `at`.
fn put_num(out: &mut [u8], at: usize, v: u32) -> usize {
    let mut tmp = [0u8; 10];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    append(out, at, tmp.get(i..).unwrap_or(&[]))
}

/// One step of the Service's publication (kube-dns style): the
/// `kube-system` namespace; a read of the claim on its ClusterIP in the
/// Service pool (another Service's, and nothing is published); the Service
/// itself (UDP 53 to this resolver's port); the claim (create-only, owned as
/// the allocator owns one, so no other Service is given the address); its one
/// endpoint (this resolver's address — a Service without a selector, so the
/// endpoints chain leaves it be); and then `/cluster-dns`, its ClusterIP.
unsafe fn publish(s: &mut State, sys: &SyscallTable) {
    let store = Store::plain(sys, &s.nag);
    let sl = s.service_ip_len as usize;
    let al = s.address_len as usize;
    let mut sip_b = [0u8; 16];
    let mut adr_b = [0u8; 16];
    for (d, &c) in sip_b.iter_mut().zip(s.service_ip[..sl.min(16)].iter()) {
        *d = c;
    }
    for (d, &c) in adr_b.iter_mut().zip(s.address[..al.min(16)].iter()) {
        *d = c;
    }
    let sip = sip_b.get(..sl).unwrap_or(&[]);
    let adr = adr_b.get(..al).unwrap_or(&[]);
    let sip_n = parse_ip4(sip).unwrap_or(0);
    let mut key = [0u8; 96];
    let mut val = [0u8; 512];
    let (kl, vl, create) = match s.publish {
        PUB_NS => {
            let v = b"{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{\"name\":\"kube-system\"}}";
            (
                append(&mut key, 0, DNS_NS_KEY),
                append(&mut val, 0, v),
                true,
            )
        }
        PUB_CHECK | PUB_CLAIM => {
            let k = append(&mut key, 0, DNS_CLAIM_PREFIX);
            let k = put_num(&mut key, k, sip_n);
            let v = append(&mut val, 0, DNS_OWNER);
            (k, put_num(&mut val, v, sip_n), true)
        }
        PUB_SVC => {
            let v = append(&mut val, 0, b"{\"apiVersion\":\"v1\",\"kind\":\"Service\",\"metadata\":{\"name\":\"kube-dns\",\"namespace\":\"kube-system\",\"labels\":{\"k8s-app\":\"kube-dns\",\"kubernetes.io/cluster-service\":\"true\",\"kubernetes.io/name\":\"KubeDNS\"}},\"spec\":{\"type\":\"ClusterIP\",\"clusterIP\":\"");
            let v = append(&mut val, v, sip);
            let v = append(&mut val, v, b"\",\"clusterIPs\":[\"");
            let v = append(&mut val, v, sip);
            let v = append(&mut val, v, b"\"],\"ports\":[{\"name\":\"dns\",\"protocol\":\"UDP\",\"port\":53,\"targetPort\":");
            let v = put_num(&mut val, v, u32::from(s.port));
            let v = append(&mut val, v, b"}]}}");
            (append(&mut key, 0, DNS_SVC_KEY), v, false)
        }
        PUB_SLICE => {
            // One endpoint per resolver, named by its address: a cluster of
            // several control planes has as many.
            let mut k = append(&mut key, 0, DNS_SLICE_PREFIX);
            for &c in adr {
                k = append(
                    &mut key,
                    k,
                    if c == b'.' {
                        &b"-"[..]
                    } else {
                        core::slice::from_ref(&c)
                    },
                );
            }
            (k, append(&mut val, 0, adr), false)
        }
        _ => {
            s.publish = PUB_DONE;
            return;
        }
    };
    let k = key.get(..kl).unwrap_or(&[]);
    let v = val.get(..vl).unwrap_or(&[]);
    if s.publish == PUB_CHECK {
        // Read only: absent or ours goes on; another owner's is `Taken`.
        let mut cur = [0u8; 128];
        let held = get_value(&store, k, &mut cur);
        if store.waiting() {
            return;
        }
        let theirs =
            store.refusal().is_none() && matches!(held, Some(n) if cur.get(..n) != Some(v));
        if !theirs {
            s.publish += 1;
            return;
        }
    }
    let answer = if s.publish == PUB_CHECK {
        Ensured::Taken(0)
    } else {
        ensure(&store, k, v, create)
    };
    match answer {
        Ensured::Wait => {}
        Ensured::Done => s.publish += 1,
        Ensured::Taken(_) if s.publish == PUB_NS => s.publish += 1,
        Ensured::Taken(_) => {
            // The ClusterIP is another Service's: Pods are given this
            // resolver's own address instead, and the gap is said.
            let m = b"[cluster_dns] ERROR service_ip is claimed by another Service - kube-dns is NOT published; Pods name the resolver's own address";
            dev_log(sys, 1, m.as_ptr(), m.len());
            s.service_ip_len = 0;
            s.publish = PUB_DONE;
        }
        Ensured::Failed => {
            let m = b"[cluster_dns] ERROR the store refused a write of the kube-dns Service - Pods name the resolver's own address";
            dev_log(sys, 1, m.as_ptr(), m.len());
            s.service_ip_len = 0;
            s.publish = PUB_DONE;
        }
    }
    // Past the endpoint (PUB_DONE): `/cluster-dns` next, as its ClusterIP.
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
        let s = &mut *(state as *mut State);
        s.syscalls = syscalls as *const SyscallTable;
        s.net_in = in_chan;
        s.net_out = out_chan;
        s.server = DatagramEndpoint::new();
        s.upstream_ep = DatagramEndpoint::new();
        s.domain_len = 0;
        s.upstream_txt_len = 0;
        s.upstream = 0;
        s.held = [HELD_EMPTY; HELD];
        s.fwd = [FWD_EMPTY; FWD];
        s.next_id = 0;
        s.answered = 0;
        s.forwarded = 0;
        s.refused_said = 0;
        s.address_len = 0;
        s.service_ip_len = 0;
        s.publish = PUB_DONE;
        s.svc = SvcInfo {
            kind: 0,
            cip: 0,
            ext: [0; 253],
            ext_len: 0,
            ports: [PORT_EMPTY; 8],
            nports: 0,
        };
        params_def::set_defaults(s);
        params_def::parse_tlv(s, params, params_len);
        let sys = &*s.syscalls;
        if s.domain_len == 0 {
            let d = b"cluster.local";
            s.domain[..d.len()].copy_from_slice(d);
            s.domain_len = d.len() as u16;
        }
        let dl = s.domain_len as usize;
        for c in s.domain[..dl].iter_mut() {
            *c = lower(*c);
        }
        let al = s.address_len as usize;
        s.address_owed = (al > 0 && parse_ip4(&s.address[..al]).is_some()) as u8;
        let sl = s.service_ip_len as usize;
        if sl > 0 {
            if parse_ip4(&s.service_ip[..sl]).is_none() {
                let m = b"[cluster_dns] FAULT: `service_ip` is not an IPv4 address";
                dev_log(sys, 1, m.as_ptr(), m.len());
                return -3;
            }
            // The Service needs where it leads: this resolver's address.
            if s.address_owed != 0 {
                s.publish = PUB_NS;
            }
        }
        let ul = s.upstream_txt_len as usize;
        if ul > 0 {
            match parse_ip4(&s.upstream_txt[..ul]) {
                Some(a) => s.upstream = a,
                None => {
                    let m = b"[cluster_dns] FAULT: `upstream` is not an IPv4 address";
                    dev_log(sys, 1, m.as_ptr(), m.len());
                    return -3;
                }
            }
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
        // The server first, then (forwarding) the ephemeral upstream: one
        // bind outstanding at a time, so each BOUND is unambiguously ours.
        s.server
            .poll_bind(sys, s.net_out, s.port, s.net_buf.as_mut_ptr(), NET_BUF);
        if s.server.is_ready() && s.upstream != 0 {
            s.upstream_ep
                .poll_bind(sys, s.net_out, 0, s.net_buf.as_mut_ptr(), NET_BUF);
        }

        // `/cluster-dns`: where Pods reach this resolver, once serving — the
        // ClusterIP of the Service fronting it, published first, or (no
        // `service_ip`, or that address another Service's) its own address.
        if s.server.is_ready() {
            if s.publish != PUB_DONE {
                publish(s, sys);
            } else if s.address_owed != 0 {
                let store = Store::plain(sys, &s.nag);
                // A publication given up cleared `service_ip`.
                let (v, vl) = if s.service_ip_len > 0 {
                    (s.service_ip, s.service_ip_len as usize)
                } else {
                    (s.address, s.address_len as usize)
                };
                match ensure(&store, CLUSTER_DNS_KEY, v.get(..vl).unwrap_or(&[]), false) {
                    Ensured::Wait => {}
                    Ensured::Done | Ensured::Taken(_) => s.address_owed = 0,
                    Ensured::Failed => {
                        let m = b"[cluster_dns] ERROR the store refused /cluster-dns - this resolver's address is NOT published";
                        dev_log(sys, 1, m.as_ptr(), m.len());
                        s.address_owed = 0;
                    }
                }
            }
        }

        // Held queries: asked again now the store may answer.
        let mut did = false;
        for i in 0..HELD {
            if s.held[i].used == 0 {
                continue;
            }
            let h = s.held[i];
            let pkt = &h.pkt[..(h.len as usize).min(DNS_MAX)];
            if query(s, h.ip, h.port, pkt) {
                s.held[i].used = 0;
                did = true;
            }
        }

        let mut budget = DRAIN;
        while budget > 0 {
            budget -= 1;
            let Some(ev) = dg_recv(sys, s.net_in, s.net_buf.as_mut_ptr(), NET_BUF) else {
                if !net_in_readable(sys, s.net_in) {
                    break;
                }
                continue; // a frame of another surface on the shared channel
            };
            did = true;
            match ev {
                DgEvent::Bound { ep_id, local_port } => {
                    if local_port == s.port && !s.server.is_ready() {
                        s.server.on_bound(ep_id);
                        let m = b"[cluster_dns] serving";
                        dev_log(sys, 3, m.as_ptr(), m.len());
                    } else if s.server.is_ready() && !s.server.owns(ep_id) {
                        s.upstream_ep.on_bound(ep_id);
                    }
                }
                DgEvent::Rx {
                    ep_id,
                    src_ip,
                    src_port,
                    data,
                    len,
                    full_len,
                } => {
                    if len != full_len || len > DNS_MAX {
                        continue; // cut short: no answer is better than a wrong one
                    }
                    let mut pkt = [0u8; DNS_MAX];
                    ptr_copy(pkt.as_mut_ptr(), data, len);
                    if s.server.owns(ep_id) {
                        if !query(s, src_ip, src_port, &pkt[..len]) {
                            hold(s, src_ip, src_port, &pkt[..len]);
                        }
                    } else if s.upstream_ep.owns(ep_id) {
                        relay(s, src_ip, src_port, &pkt[..len]);
                    }
                }
                DgEvent::Err { .. } => {
                    // A bind refused (the port held, or not ours to take):
                    // the endpoint waiting on it backs off and asks again —
                    // said once, so a node whose :53 is taken is not silent.
                    if !s.server.is_ready() && s.refused_said == 0 {
                        s.refused_said = 1;
                        let m = b"[cluster_dns] port refused (held, or not this process's to bind); asking again";
                        dev_log(sys, 1, m.as_ptr(), m.len());
                    }
                    s.server.on_error(sys);
                    s.upstream_ep.on_error(sys);
                }
                DgEvent::Closed { .. } => {}
            }
        }
        if did {
            2
        } else {
            0
        }
    }
}
