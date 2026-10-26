// The Wormhole projection's pure core (rfc_lb_vip §6.1, §9): a Service's
// intent rows (wormhole.plan's) become the exact set of Wormhole v1 records
// of one route generation, in the §9.5 publication order. I/O-free and
// allocation-free: `wormhole_emit` does the writes, and the harness drives
// the same code against a modelled store. Needs `_shared/wormhole_traffic.rs`
// (the `traffic` module) included first.
//
// Identities (128-bit, derived, never stored as a mapping):
//   H(label, parts…) = SHA-256(label ‖ 0x00 ‖ part₁ ‖ 0x00 ‖ part₂ …)
//   tenant_id    = H("nc-tenant",   ns)[..16]
//   service_id   = H("nc-service",  ns, name)[..16]
//   endpoint_id  = H("nc-endpoint", ns, svc, pod)[..16]
//   host_id      = H("nc-node",     node)[..16]        (zero: no node)
//   zone         = H("nc-zone",     zone)[..4] as u32 BE (0: no zone)
//   operation_id = H("nc-operation", service_id, gen as u64 LE)[..16]
//   hash_seed_ref= H("nc-hash-seed", tenant_id, service_id)[..16]
// A name holds no NUL, so the encoding is unambiguous; a name past 63 bytes
// (Kubernetes' own bound) is refused, not truncated.
//
// One generation `g` of a Service is, under
// `/wh/<tenant hex>/<service hex>/gen/<g>/` (keys < 256 bytes):
//   r<i>-<4|6>/ep/<endpoint hex>   EndpointRecord (WHR1 kind 2), one per
//                                  (route, endpoint) — its port the route's
//                                  target port on that endpoint
//   r<i>-<4|6>/manifest            EndpointManifest (kind 3): the route's
//                                  endpoint set, its digest over the
//                                  endpoint records' content digests in
//                                  endpoint-identity order
//   policy                         the normalized locality/affinity inputs
//                                  (LocalityPolicy, kind 0x80), its payload's
//                                  SHA-256 the route's locality policy id
//   admission                      the source ranges (kind 0x81), only when
//                                  the Service names some; its payload's
//                                  SHA-256 the route's admission policy id
//   dial                           the Service's traffic dial (TrafficDial,
//                                  kind 0x84), only when `/wh-dial/<ns>/<svc>`
//                                  holds one; its payload's SHA-256 the
//                                  route's traffic_dial policy id
//   r<i>-<4|6>/route               RouteRecord (kind 1), one per (Service
//                                  port, listener family)
//   seal                           the sealed generation manifest (kind
//                                  0x82): record count, route count, and a
//                                  SHA-256 over (key length u16 LE ‖ key ‖
//                                  content digest) of every other record in
//                                  lexicographic key order
// written in exactly that order (§9.5 steps 1, 2, 3, 5), then the active
// pointer `/wh/<t>/<s>/active` advanced by compare-and-swap (step 6).
// A route with no eligible endpoint is not written: an empty endpoint set
// is not a manifest Wormhole's decoder accepts (count 0 is invalid), so a
// listener with no endpoints is a generation without that route.

/// The loadBalancerClass that names Wormhole. Unset is Wormhole too: it is
/// nanocloud's default load balancer.
const WH_CLASS: &[u8] = b"wormhole.nanocloud.io/lb";
/// Ports of one Service, IP families, routes, endpoints and source ranges a
/// generation carries; one past is refused, never truncated.
const WH_MAX_PORTS: usize = 8;
const WH_MAX_ROUTES: usize = WH_MAX_PORTS * 2;
const WH_MAX_EPS: usize = 64;
const WH_MAX_RANGES: usize = 16;
/// Items of one generation: every (route, endpoint), a manifest and a route
/// per route, the policy, the admission and the seal.
const WH_MAX_ITEMS: usize = WH_MAX_ROUTES * WH_MAX_EPS + 2 * WH_MAX_ROUTES + 3;
const WH_NAME: usize = 64;
const WH_PORTNAMES: usize = 128;
/// sessionAffinity ClientIP's timeout when the Service names none (upstream's).
const WH_AFFINITY_TIMEOUT: u32 = 10_800;

/// The record kinds nanocloud writes beside a generation, as Wormhole's v1
/// kinds (docs/architecture/wormhole-projection.md).
const WH_KIND_POLICY: u16 = traffic::RecordKind::LocalityPolicy as u16;
const WH_KIND_ADMISSION: u16 = traffic::RecordKind::SourceAdmission as u16;
const WH_KIND_SEAL: u16 = traffic::RecordKind::GenerationSeal as u16;
const WH_POLICY_LEN: usize = 8;
const WH_ADMISSION_ENTRY: usize = 20;
const WH_SEAL_LEN: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WhError {
    /// A name past 63 bytes, or an empty one.
    Name,
    /// An address, port or range that does not parse.
    Malformed,
    /// More ports, endpoints or ranges than a generation carries.
    TooMany,
    /// An output buffer that cannot hold the encoding.
    Buffer,
}

impl WhError {
    /// Its index in the reason names a status says (`name`, `malformed`,
    /// `too-many`, `buffer`, in that order): an includer holds the names in
    /// a `name_table!`, as a PIC module cannot hold a table of pointers.
    fn code(self) -> usize {
        match self {
            WhError::Name => 0,
            WhError::Malformed => 1,
            WhError::TooMany => 2,
            WhError::Buffer => 3,
        }
    }
}

fn wh_digest(label: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut h = traffic::Sha256::new();
    h.update(label);
    for p in parts {
        h.update(&[0]);
        h.update(p);
    }
    h.finalize()
}

fn wh_id16(d: &[u8; 32]) -> [u8; 16] {
    let mut o = [0u8; 16];
    o.copy_from_slice(&d[..16]);
    o
}

fn wh_tenant_id(ns: &[u8]) -> [u8; 16] {
    wh_id16(&wh_digest(b"nc-tenant", &[ns]))
}

fn wh_service_id(ns: &[u8], name: &[u8]) -> [u8; 16] {
    wh_id16(&wh_digest(b"nc-service", &[ns, name]))
}

fn wh_endpoint_id(ns: &[u8], svc: &[u8], pod: &[u8]) -> [u8; 16] {
    wh_id16(&wh_digest(b"nc-endpoint", &[ns, svc, pod]))
}

fn wh_host_id(node: &[u8]) -> [u8; 16] {
    if node.is_empty() {
        [0; 16]
    } else {
        wh_id16(&wh_digest(b"nc-node", &[node]))
    }
}

fn wh_zone(zone: &[u8]) -> u32 {
    if zone.is_empty() {
        return 0;
    }
    let d = wh_digest(b"nc-zone", &[zone]);
    let z = u32::from_be_bytes([d[0], d[1], d[2], d[3]]);
    // 0 is "no zone": a zone whose digest begins with four zero bytes is 1.
    z.max(1)
}

fn wh_operation_id(service: &[u8; 16], gen: u64) -> [u8; 16] {
    wh_id16(&wh_digest(b"nc-operation", &[service, &gen.to_le_bytes()]))
}

fn wh_seed_ref(tenant: &[u8; 16], service: &[u8; 16]) -> [u8; 16] {
    wh_id16(&wh_digest(b"nc-hash-seed", &[tenant, service]))
}

const WH_HEX: &[u8; 16] = b"0123456789abcdef";

/// `bytes` as lowercase hex into `out` at `at`; the new end (None: no room).
fn wh_hex(out: &mut [u8], at: usize, bytes: &[u8]) -> Option<usize> {
    let end = at.checked_add(bytes.len() * 2)?;
    let d = out.get_mut(at..end)?;
    for (i, b) in bytes.iter().enumerate() {
        d[2 * i] = WH_HEX[(b >> 4) as usize];
        d[2 * i + 1] = WH_HEX[(b & 15) as usize];
    }
    Some(end)
}

fn wh_put(out: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    out.get_mut(at..end)?.copy_from_slice(src);
    Some(end)
}

fn wh_dec(out: &mut [u8], at: usize, v: u64) -> Option<usize> {
    let mut t = [0u8; 20];
    let mut i = t.len();
    let mut u = v;
    loop {
        i -= 1;
        t[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 {
            break;
        }
    }
    wh_put(out, at, &t[i..])
}

/// Whether `b` holds `x`.
#[allow(
    clippy::manual_contains,
    reason = "`<[u8]>::contains` lowers to memchr, which the PIC link cannot resolve"
)]
fn wh_has(b: &[u8], x: u8) -> bool {
    b.iter().any(|&c| c == x)
}

/// `tag` (`ip=`) of a `;`-separated flat row; empty when absent.
fn wh_field<'a>(row: &'a [u8], tag: &[u8]) -> &'a [u8] {
    row.split(|&c| c == b';')
        .find(|seg| seg.len() >= tag.len() && &seg[..tag.len()] == tag)
        .map(|seg| &seg[tag.len()..])
        .unwrap_or(&[])
}

/// The `i`-th entry of a `,` list, or `""` past its end.
fn wh_nth(list: &[u8], i: usize) -> &[u8] {
    list.split(|&c| c == b',').nth(i).unwrap_or(&[])
}

fn wh_count(list: &[u8]) -> usize {
    if list.is_empty() {
        0
    } else {
        list.split(|&c| c == b',').count()
    }
}

fn wh_u64(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.len() > 19 {
        return None;
    }
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u64::from(c - b'0');
    }
    Some(v)
}

fn wh_port(b: &[u8]) -> Option<u16> {
    let v = wh_u64(b)?;
    (1..=65535).contains(&v).then_some(v as u16)
}

fn wh_v4(b: &[u8]) -> Option<[u8; 4]> {
    let mut o = [0u8; 4];
    let mut n = 0;
    for part in b.split(|&c| c == b'.') {
        if n == 4 || part.is_empty() || part.len() > 3 {
            return None;
        }
        let v = wh_u64(part)?;
        if v > 255 {
            return None;
        }
        o[n] = v as u8;
        n += 1;
    }
    (n == 4).then_some(o)
}

fn wh_hexval(c: u8) -> Option<u16> {
    match c {
        b'0'..=b'9' => Some(u16::from(c - b'0')),
        b'a'..=b'f' => Some(u16::from(c - b'a' + 10)),
        b'A'..=b'F' => Some(u16::from(c - b'A' + 10)),
        _ => None,
    }
}

/// Groups of one side of an IPv6 text (`a:b:…`, the last may be dotted
/// IPv4) into `out` from `at`; how many 16-bit groups it filled.
fn wh_v6_groups(side: &[u8], out: &mut [u16; 8]) -> Option<usize> {
    if side.is_empty() {
        return Some(0);
    }
    let mut n = 0usize;
    let parts = side.split(|&c| c == b':').count();
    for (i, g) in side.split(|&c| c == b':').enumerate() {
        if i + 1 == parts && wh_has(g, b'.') {
            let v4 = wh_v4(g)?;
            *out.get_mut(n)? = u16::from_be_bytes([v4[0], v4[1]]);
            *out.get_mut(n + 1)? = u16::from_be_bytes([v4[2], v4[3]]);
            n += 2;
            continue;
        }
        if g.is_empty() || g.len() > 4 {
            return None;
        }
        let mut v = 0u16;
        for &c in g {
            v = (v << 4) | wh_hexval(c)?;
        }
        *out.get_mut(n)? = v;
        n += 1;
    }
    Some(n)
}

fn wh_v6(b: &[u8]) -> Option<[u8; 16]> {
    let mut groups = [0u16; 8];
    let dc = b.windows(2).position(|w| w == b"::");
    match dc {
        None => {
            if wh_v6_groups(b, &mut groups)? != 8 {
                return None;
            }
        }
        Some(at) => {
            let (l, r) = (&b[..at], &b[at + 2..]);
            if r.windows(2).any(|w| w == b"::") {
                return None;
            }
            let mut lg = [0u16; 8];
            let mut rg = [0u16; 8];
            let ln = wh_v6_groups(l, &mut lg)?;
            let rn = wh_v6_groups(r, &mut rg)?;
            if ln + rn > 7 {
                return None;
            }
            groups[..ln].copy_from_slice(&lg[..ln]);
            groups[8 - rn..].copy_from_slice(&rg[..rn]);
        }
    }
    let mut o = [0u8; 16];
    for (i, g) in groups.iter().enumerate() {
        o[2 * i..2 * i + 2].copy_from_slice(&g.to_be_bytes());
    }
    Some(o)
}

/// An address in either family, in Wormhole's canonical form.
fn wh_addr(b: &[u8]) -> Option<traffic::NetAddr> {
    if wh_has(b, b':') {
        wh_v6(b).map(traffic::NetAddr::v6)
    } else {
        wh_v4(b).map(traffic::NetAddr::v4)
    }
}

/// Each quoted string of a raw JSON list of strings (`["a","b"]`), as the
/// source projects one; how many there were. A list that is not one yields
/// what it can read and the caller validates each entry.
fn wh_strs(raw: &[u8], mut each: impl FnMut(&[u8]) -> bool) -> bool {
    let mut rest = raw;
    while let Some(a) = rest.iter().position(|&c| c == b'"') {
        let after = &rest[a + 1..];
        let Some(b) = after.iter().position(|&c| c == b'"') else {
            return false;
        };
        if !each(&after[..b]) {
            return false;
        }
        rest = &after[b + 1..];
    }
    true
}

fn wh_family_of(name: &[u8]) -> Option<u8> {
    match name {
        b"IPv4" => Some(traffic::AF_INET),
        b"IPv6" => Some(traffic::AF_INET6),
        _ => None,
    }
}

/// A Service as Wormhole is told it: its identities, listeners, ports and
/// normalized policy inputs.
#[derive(Clone, Copy)]
struct WhSvc {
    ns: [u8; WH_NAME],
    nsl: u8,
    name: [u8; WH_NAME],
    namel: u8,
    tenant: [u8; 16],
    service: [u8; 16],
    /// One listener per family, IPv4 first: the load-balancer address for a
    /// LoadBalancer Service that has one (IPv4), else its ClusterIP of that
    /// family.
    listeners: [traffic::NetAddr; 2],
    nlisteners: u8,
    ports: [u16; WH_MAX_PORTS],
    /// TRANSPORT_TCP / TRANSPORT_UDP; 0 for a protocol Wormhole has no
    /// listener for (SCTP), which gets no route.
    transports: [u8; WH_MAX_PORTS],
    /// Each port's targetPort as written (a number, a name, or empty).
    tports: [[u8; 16]; WH_MAX_PORTS],
    tportl: [u8; WH_MAX_PORTS],
    nports: u8,
    ext_local: bool,
    int_local: bool,
    client_ip: bool,
    affinity_timeout: u32,
    /// Source ranges: (family, prefix length, address).
    ranges: [(u8, u8, [u8; 16]); WH_MAX_RANGES],
    nranges: u8,
    /// SHA-256 of the service intent row: the routes' provenance.
    provenance: [u8; 32],
    /// The Service's traffic dial (Wormhole's `TrafficDial`, 0x84), when
    /// the failure-domain controller or an operator set one at
    /// `/wh-dial/<ns>/<svc>`: folded into every generation as a placed
    /// record (`dial`) the routes name by its content address.
    dial: Option<traffic::TrafficDial>,
}

impl WhSvc {
    fn ns(&self) -> &[u8] {
        &self.ns[..self.nsl as usize]
    }
    fn name(&self) -> &[u8] {
        &self.name[..self.namel as usize]
    }
}

fn wh_name(src: &[u8], out: &mut [u8; WH_NAME]) -> Result<u8, WhError> {
    if src.is_empty() || src.len() >= WH_NAME || wh_has(src, 0) {
        return Err(WhError::Name);
    }
    out[..src.len()].copy_from_slice(src);
    Ok(src.len() as u8)
}

/// The service intent row (`kind=svc;…`, wormhole.plan's) as Wormhole is
/// told it. Defaults are upstream's: traffic policies `Cluster`, affinity
/// `None`, a ClientIP timeout of 10800 s.
fn wh_parse_svc(ns: &[u8], name: &[u8], row: &[u8]) -> Result<WhSvc, WhError> {
    let mut s = WhSvc {
        ns: [0; WH_NAME],
        nsl: 0,
        name: [0; WH_NAME],
        namel: 0,
        tenant: wh_tenant_id(ns),
        service: wh_service_id(ns, name),
        listeners: [traffic::NetAddr::v4([0; 4]); 2],
        nlisteners: 0,
        ports: [0; WH_MAX_PORTS],
        transports: [0; WH_MAX_PORTS],
        tports: [[0; 16]; WH_MAX_PORTS],
        tportl: [0; WH_MAX_PORTS],
        nports: 0,
        ext_local: wh_field(row, b"etp=") == b"Local",
        int_local: wh_field(row, b"itp=") == b"Local",
        client_ip: wh_field(row, b"aff=") == b"ClientIP",
        affinity_timeout: 0,
        ranges: [(0, 0, [0; 16]); WH_MAX_RANGES],
        nranges: 0,
        provenance: traffic::sha256(row),
        dial: None,
    };
    s.nsl = wh_name(ns, &mut s.ns)?;
    s.namel = wh_name(name, &mut s.name)?;
    if wh_field(row, b"kind=") != b"svc" {
        return Err(WhError::Malformed);
    }
    if s.client_ip {
        let t = wh_field(row, b"afft=");
        s.affinity_timeout = if t.is_empty() {
            WH_AFFINITY_TIMEOUT
        } else {
            match wh_u64(t) {
                Some(v) if (1..=86_400).contains(&v) => v as u32,
                _ => return Err(WhError::Malformed),
            }
        };
    }
    // Ports, aligned with their protocols and target ports.
    let (ports, protos, tports) = (
        wh_field(row, b"ports="),
        wh_field(row, b"protos="),
        wh_field(row, b"tports="),
    );
    let n = wh_count(ports);
    if n == 0 {
        return Err(WhError::Malformed);
    }
    if n > WH_MAX_PORTS {
        return Err(WhError::TooMany);
    }
    for i in 0..n {
        s.ports[i] = wh_port(wh_nth(ports, i)).ok_or(WhError::Malformed)?;
        s.transports[i] = match wh_nth(protos, i) {
            b"" | b"-" | b"TCP" => traffic::TRANSPORT_TCP,
            b"UDP" => traffic::TRANSPORT_UDP,
            _ => 0,
        };
        let tp = wh_nth(tports, i);
        let tp = if tp == b"-" { &[][..] } else { tp };
        if tp.len() > 15 {
            return Err(WhError::Malformed);
        }
        s.tports[i][..tp.len()].copy_from_slice(tp);
        s.tportl[i] = tp.len() as u8;
    }
    s.nports = n as u8;
    // Families: as the Service declares them, else its ClusterIP's.
    let cip = wh_field(row, b"cip=");
    let cips = wh_field(row, b"cips=");
    let mut fams = [0u8; 2];
    let mut nf = 0usize;
    let mut bad = false;
    wh_strs(wh_field(row, b"fam="), |f| {
        match wh_family_of(f) {
            Some(af) if nf < 2 && !wh_has(&fams[..nf], af) => {
                fams[nf] = af;
                nf += 1;
            }
            _ => bad = true,
        }
        !bad
    });
    if bad {
        return Err(WhError::Malformed);
    }
    if nf == 0 {
        let a = wh_addr(cip).ok_or(WhError::Malformed)?;
        fams[0] = a.family;
        nf = 1;
    }
    fams[..nf].sort_unstable();
    let lb = wh_field(row, b"type=") == b"LoadBalancer";
    let lbip = wh_field(row, b"lbip=");
    for &af in &fams[..nf] {
        let mut addr = None;
        if lb && af == traffic::AF_INET {
            addr = wh_addr(lbip).filter(|a| a.family == af);
        }
        if addr.is_none() {
            wh_strs(cips, |c| {
                if addr.is_none() {
                    addr = wh_addr(c).filter(|a| a.family == af);
                }
                true
            });
        }
        if addr.is_none() {
            addr = wh_addr(cip).filter(|a| a.family == af);
        }
        if let Some(a) = addr {
            s.listeners[s.nlisteners as usize] = a;
            s.nlisteners += 1;
        }
    }
    if s.nlisteners == 0 {
        return Err(WhError::Malformed);
    }
    // Source ranges, for the edge's early admission.
    let mut ok = true;
    wh_strs(wh_field(row, b"src="), |r| {
        let Some(slash) = r.iter().position(|&c| c == b'/') else {
            ok = false;
            return false;
        };
        let (Some(a), Some(len)) = (wh_addr(&r[..slash]), wh_u64(&r[slash + 1..])) else {
            ok = false;
            return false;
        };
        let max = if a.family == traffic::AF_INET {
            32
        } else {
            128
        };
        if len > max || s.nranges as usize >= WH_MAX_RANGES {
            ok = false;
            return false;
        }
        // A range is its prefix: `10.0.0.5/8` means `10.0.0.0/8`, and
        // Wormhole refuses a range that carries host bits past its prefix.
        let mut bytes = a.bytes;
        let first = if a.family == traffic::AF_INET { 12 } else { 0 };
        wh_mask_prefix(&mut bytes[first..], len as usize);
        s.ranges[s.nranges as usize] = (a.family, len as u8, bytes);
        s.nranges += 1;
        true
    });
    if !ok {
        return Err(if s.nranges as usize >= WH_MAX_RANGES {
            WhError::TooMany
        } else {
            WhError::Malformed
        });
    }
    Ok(s)
}

/// One endpoint, from its intent row (`kind=ep;<slice>;pnames=…;pnums=…`).
#[derive(Clone, Copy)]
struct WhEp {
    id: [u8; 16],
    gen: u64,
    addr: traffic::NetAddr,
    state: traffic::EndpointState,
    host: [u8; 16],
    zone: u32,
    pnames: [u8; WH_PORTNAMES],
    pnamel: u8,
    pnums: [u8; WH_PORTNAMES],
    pnuml: u8,
}

const WH_EP_NONE: WhEp = WhEp {
    id: [0; 16],
    gen: 0,
    addr: traffic::NetAddr::v4([0; 4]),
    state: traffic::EndpointState::Unknown,
    host: [0; 16],
    zone: 0,
    pnames: [0; WH_PORTNAMES],
    pnamel: 0,
    pnums: [0; WH_PORTNAMES],
    pnuml: 0,
};

/// The slice's state as Wormhole's (§14.2): ready -> Ready, serving (Ready
/// while being deleted) -> Degraded, terminating -> Draining. Absent is no
/// record at all.
fn wh_state(s: &[u8]) -> Option<traffic::EndpointState> {
    match s {
        b"ready" => Some(traffic::EndpointState::Ready),
        b"serving" => Some(traffic::EndpointState::Degraded),
        b"terminating" => Some(traffic::EndpointState::Draining),
        _ => None,
    }
}

/// An endpoint row; None for one that is not an endpoint Wormhole can be
/// told of (no address, an unknown state, a port list past its bound).
fn wh_parse_ep(ns: &[u8], svc: &[u8], pod: &[u8], row: &[u8]) -> Option<WhEp> {
    if wh_field(row, b"kind=") != b"ep" {
        return None;
    }
    let addr = wh_addr(wh_field(row, b"ip="))?;
    let af = wh_field(row, b"af=");
    let want = if addr.family == traffic::AF_INET {
        &b"4"[..]
    } else {
        &b"6"[..]
    };
    if !af.is_empty() && af != want {
        return None;
    }
    let (pn, pp) = (wh_field(row, b"pnames="), wh_field(row, b"pnums="));
    if pn.len() > WH_PORTNAMES || pp.len() > WH_PORTNAMES {
        return None;
    }
    let mut e = WhEp {
        id: wh_endpoint_id(ns, svc, pod),
        gen: wh_u64(wh_field(row, b"gen=")).unwrap_or(0),
        addr,
        state: wh_state(wh_field(row, b"state="))?,
        host: wh_host_id(wh_field(row, b"node=")),
        zone: wh_zone(wh_field(row, b"zone=")),
        ..WH_EP_NONE
    };
    e.pnames[..pn.len()].copy_from_slice(pn);
    e.pnamel = pn.len() as u8;
    e.pnums[..pp.len()].copy_from_slice(pp);
    e.pnuml = pp.len() as u8;
    Some(e)
}

/// Endpoints in identity order: the order of their keys, of a manifest's
/// digest, and of the seal.
fn wh_sort_eps(eps: &mut [WhEp]) {
    for i in 1..eps.len() {
        let mut j = i;
        while j > 0 && eps[j - 1].id > eps[j].id {
            eps.swap(j - 1, j);
            j -= 1;
        }
    }
}

/// The digest that says whether a generation is needed: the service row and
/// every endpoint's fields, in identity order. Equal to the active
/// generation's: nothing to publish.
fn wh_intent_digest(svc: &WhSvc, eps: &[WhEp]) -> [u8; 32] {
    let mut h = traffic::Sha256::new();
    h.update(b"nc-intent");
    h.update(&svc.provenance);
    // A dial is part of the intent: a new dial is a new generation. With
    // none the digest is what it always was.
    if let Some(d) = &svc.dial {
        let mut p = [0u8; traffic::DIAL_PAYLOAD_LEN];
        if d.encode(&mut p).is_ok() {
            h.update(b"dial");
            h.update(&p);
        }
    }
    for e in eps {
        h.update(&e.id);
        h.update(&e.gen.to_le_bytes());
        h.update(&[e.addr.family, e.state as u8]);
        h.update(&e.addr.bytes);
        h.update(&e.host);
        h.update(&e.zone.to_be_bytes());
        h.update(&[e.pnamel]);
        h.update(&e.pnames[..e.pnamel as usize]);
        h.update(&[e.pnuml]);
        h.update(&e.pnums[..e.pnuml as usize]);
    }
    h.finalize()
}

/// The target port of Service port `i` on endpoint `e`: the targetPort when
/// it is a number, the port itself when it is unset, the endpoint Pod's
/// port of that name when it is one (0: it has none, and the endpoint is
/// not in that route).
fn wh_target_port(s: &WhSvc, i: usize, e: &WhEp) -> u16 {
    let tp = &s.tports[i][..s.tportl[i] as usize];
    if tp.is_empty() {
        return s.ports[i];
    }
    if let Some(p) = wh_port(tp) {
        return p;
    }
    let names = &e.pnames[..e.pnamel as usize];
    let nums = &e.pnums[..e.pnuml as usize];
    match names.split(|&c| c == b',').position(|n| n == tp) {
        Some(k) => wh_port(wh_nth(nums, k)).unwrap_or(0),
        None => 0,
    }
}

/// One route of a generation: a Service port on one listener family, and
/// the endpoints (bits of the identity-ordered set) it reaches.
#[derive(Clone, Copy, Default)]
struct WhRoute {
    port: u8,
    listener: u8,
    eps: u64,
}

impl WhRoute {
    fn count(&self) -> usize {
        self.eps.count_ones() as usize
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WhItem {
    Endpoint(u8, u8),
    Manifest(u8),
    Policy,
    Admission,
    Dial,
    Route(u8),
    Seal,
}

/// A Service's dial as `/wh-dial/<ns>/<svc>` holds it: one WHR1
/// `TrafficDial` record, strictly decoded, for this Service. Anything else
/// is no dial (and the Service is published without one).
fn wh_parse_dial(svc: &WhSvc, rec: &[u8]) -> Option<traffic::TrafficDial> {
    let env = traffic::decode_envelope(rec).ok()?;
    if env.kind != traffic::RecordKind::TrafficDial {
        return None;
    }
    let d = traffic::TrafficDial::decode(env.payload).ok()?;
    (d.service_id == svc.service).then_some(d)
}

/// One generation of one Service: the records, their keys and their order.
struct WhGen<'a> {
    svc: &'a WhSvc,
    eps: &'a [WhEp],
    gen: u64,
    routes: [WhRoute; WH_MAX_ROUTES],
    nroutes: usize,
}

fn wh_generation<'a>(svc: &'a WhSvc, eps: &'a [WhEp], gen: u64) -> Result<WhGen<'a>, WhError> {
    if eps.len() > WH_MAX_EPS {
        return Err(WhError::TooMany);
    }
    let mut g = WhGen {
        svc,
        eps,
        gen,
        routes: [WhRoute::default(); WH_MAX_ROUTES],
        nroutes: 0,
    };
    for i in 0..svc.nports as usize {
        if svc.transports[i] == 0 {
            continue;
        }
        for l in 0..svc.nlisteners as usize {
            let fam = svc.listeners[l].family;
            let mut mask = 0u64;
            for (k, e) in eps.iter().enumerate() {
                if e.addr.family == fam && wh_target_port(svc, i, e) != 0 {
                    mask |= 1 << k;
                }
            }
            if mask != 0 {
                g.routes[g.nroutes] = WhRoute {
                    port: i as u8,
                    listener: l as u8,
                    eps: mask,
                };
                g.nroutes += 1;
            }
        }
    }
    Ok(g)
}

/// Clear every bit of `addr` past the first `prefix_len`.
fn wh_mask_prefix(addr: &mut [u8], prefix_len: usize) {
    for (i, byte) in addr.iter_mut().enumerate() {
        let start = i * 8;
        if start >= prefix_len {
            *byte = 0;
        } else if prefix_len - start < 8 {
            *byte &= 0xffu8 << (8 - (prefix_len - start));
        }
    }
}

/// The `n`-th set bit of `mask` (its endpoint index).
fn wh_nth_bit(mask: u64, n: usize) -> Option<u8> {
    let mut m = mask;
    for _ in 0..n {
        if m == 0 {
            return None;
        }
        m &= m - 1;
    }
    (m != 0).then(|| m.trailing_zeros() as u8)
}

/// The WHR1 envelope around `payload` with one of the generation kinds (0x80..),
/// byte-for-byte the layout `traffic::encode_envelope` writes for its own.
fn wh_envelope(
    kind: u16,
    gen: u64,
    op: &[u8; 16],
    payload: &[u8],
    out: &mut [u8],
) -> Result<usize, WhError> {
    let hl = traffic::HEADER_LEN;
    let total = hl + payload.len() + traffic::DIGEST_LEN;
    if payload.len() > traffic::MAX_PAYLOAD_LEN || out.len() < total {
        return Err(WhError::Buffer);
    }
    out[0..4].copy_from_slice(&traffic::MAGIC);
    out[4..6].copy_from_slice(&kind.to_le_bytes());
    out[6..8].copy_from_slice(&0u16.to_le_bytes());
    out[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    out[12..20].copy_from_slice(&gen.to_le_bytes());
    out[20..36].copy_from_slice(op);
    out[hl..hl + payload.len()].copy_from_slice(payload);
    let d = traffic::sha256(&out[..hl + payload.len()]);
    out[hl + payload.len()..total].copy_from_slice(&d);
    Ok(total)
}

/// The content digest of an encoded record: its trailing SHA-256.
fn wh_content_digest(rec: &[u8]) -> [u8; 32] {
    let mut d = [0u8; 32];
    d.copy_from_slice(&rec[rec.len() - 32..]);
    d
}

impl WhGen<'_> {
    fn op(&self) -> [u8; 16] {
        wh_operation_id(&self.svc.service, self.gen)
    }

    /// Records this generation writes, the seal included.
    fn len(&self) -> usize {
        let eps: usize = self.routes[..self.nroutes].iter().map(WhRoute::count).sum();
        eps + 2 * self.nroutes
            + 2
            + usize::from(self.svc.nranges > 0)
            + usize::from(self.svc.dial.is_some())
    }

    /// The `i`-th record in the §9.5 write order: every endpoint record,
    /// every manifest, the policy and the admission, every route, the seal.
    fn write_item(&self, i: usize) -> Option<WhItem> {
        let mut i = i;
        for (r, route) in self.routes[..self.nroutes].iter().enumerate() {
            if i < route.count() {
                return Some(WhItem::Endpoint(r as u8, wh_nth_bit(route.eps, i)?));
            }
            i -= route.count();
        }
        if i < self.nroutes {
            return Some(WhItem::Manifest(i as u8));
        }
        i -= self.nroutes;
        if i == 0 {
            return Some(WhItem::Policy);
        }
        i -= 1;
        if self.svc.nranges > 0 {
            if i == 0 {
                return Some(WhItem::Admission);
            }
            i -= 1;
        }
        if self.svc.dial.is_some() {
            if i == 0 {
                return Some(WhItem::Dial);
            }
            i -= 1;
        }
        if i < self.nroutes {
            return Some(WhItem::Route(i as u8));
        }
        (i == self.nroutes).then_some(WhItem::Seal)
    }

    /// The `i`-th record in lexicographic key order (the seal excluded):
    /// `admission`, `policy`, then each route's `ep/…` (identity order),
    /// `manifest`, `route` — routes in `r<port>-<family>` order.
    fn key_item(&self, i: usize) -> Option<WhItem> {
        let mut i = i;
        if self.svc.nranges > 0 {
            if i == 0 {
                return Some(WhItem::Admission);
            }
            i -= 1;
        }
        if self.svc.dial.is_some() {
            if i == 0 {
                return Some(WhItem::Dial);
            }
            i -= 1;
        }
        if i == 0 {
            return Some(WhItem::Policy);
        }
        i -= 1;
        for (r, route) in self.routes[..self.nroutes].iter().enumerate() {
            if i < route.count() {
                return Some(WhItem::Endpoint(r as u8, wh_nth_bit(route.eps, i)?));
            }
            i -= route.count();
            if i == 0 {
                return Some(WhItem::Manifest(r as u8));
            }
            if i == 1 {
                return Some(WhItem::Route(r as u8));
            }
            i -= 2;
        }
        None
    }

    /// `/wh/<tenant>/<service>/` into `out`; its length.
    fn service_prefix(&self, out: &mut [u8]) -> Option<usize> {
        let p = wh_put(out, 0, b"/wh/")?;
        let p = wh_hex(out, p, &self.svc.tenant)?;
        let p = wh_put(out, p, b"/")?;
        let p = wh_hex(out, p, &self.svc.service)?;
        wh_put(out, p, b"/")
    }

    /// The key of `item` into `out`; its length.
    fn key(&self, item: WhItem, out: &mut [u8]) -> Option<usize> {
        let p = self.service_prefix(out)?;
        let p = wh_put(out, p, b"gen/")?;
        let p = wh_dec(out, p, self.gen)?;
        let p = wh_put(out, p, b"/")?;
        let route_dir = |out: &mut [u8], p: usize, r: u8| -> Option<usize> {
            let route = self.routes[r as usize];
            let fam = self.svc.listeners[route.listener as usize].family;
            let p = wh_put(out, p, b"r")?;
            let p = wh_dec(out, p, u64::from(route.port))?;
            wh_put(
                out,
                p,
                if fam == traffic::AF_INET {
                    b"-4/"
                } else {
                    b"-6/"
                },
            )
        };
        match item {
            WhItem::Endpoint(r, e) => {
                let p = route_dir(out, p, r)?;
                let p = wh_put(out, p, b"ep/")?;
                wh_hex(out, p, &self.eps[e as usize].id)
            }
            WhItem::Manifest(r) => {
                let p = route_dir(out, p, r)?;
                wh_put(out, p, b"manifest")
            }
            WhItem::Route(r) => {
                let p = route_dir(out, p, r)?;
                wh_put(out, p, b"route")
            }
            WhItem::Policy => wh_put(out, p, b"policy"),
            WhItem::Admission => wh_put(out, p, b"admission"),
            WhItem::Dial => wh_put(out, p, b"dial"),
            WhItem::Seal => wh_put(out, p, b"seal"),
        }
    }

    fn policy_payload(&self) -> [u8; WH_POLICY_LEN] {
        let s = self.svc;
        let mut p = [0u8; WH_POLICY_LEN];
        p[0] = u8::from(s.ext_local);
        p[1] = u8::from(s.int_local);
        p[2] = u8::from(s.client_ip);
        p[4..8].copy_from_slice(&s.affinity_timeout.to_le_bytes());
        p
    }

    /// `service_id[16] count u16 reserved u16` then per range
    /// `family u8 prefix_len u8 reserved u16 address[16]`.
    fn admission_payload(&self, out: &mut [u8]) -> usize {
        let s = self.svc;
        out[..16].copy_from_slice(&s.service);
        out[16..18].copy_from_slice(&u16::from(s.nranges).to_le_bytes());
        out[18..20].copy_from_slice(&[0, 0]);
        let mut p = 20;
        for &(af, len, addr) in &s.ranges[..s.nranges as usize] {
            out[p] = af;
            out[p + 1] = len;
            out[p + 2..p + 4].copy_from_slice(&[0, 0]);
            out[p + 4..p + 20].copy_from_slice(&addr);
            p += WH_ADMISSION_ENTRY;
        }
        p
    }

    fn endpoint_record(&self, r: u8, e: u8, out: &mut [u8]) -> Result<usize, WhError> {
        let route = self.routes[r as usize];
        let ep = &self.eps[e as usize];
        let rec = traffic::EndpointRecord {
            endpoint_id: ep.id,
            generation: ep.gen,
            service_id: self.svc.service,
            address: ep.addr,
            port: wh_target_port(self.svc, route.port as usize, ep),
            state: ep.state,
            host_id: ep.host,
            region: 0,
            zone: ep.zone,
            rack: 0,
            weight: 1,
            capacity: 0,
            observed_route_epoch: self.gen,
            boot_incarnation: 0,
            last_evidence_revision: 0,
        };
        let mut p = [0u8; traffic::ENDPOINT_PAYLOAD_LEN];
        rec.encode(&mut p).map_err(|_| WhError::Malformed)?;
        traffic::encode_envelope(traffic::RecordKind::Endpoint, self.gen, &self.op(), &p, out)
            .map_err(|_| WhError::Buffer)
    }

    fn manifest_record(&self, r: u8, out: &mut [u8]) -> Result<usize, WhError> {
        let route = self.routes[r as usize];
        let mut h = traffic::Sha256::new();
        let mut buf = [0u8; 256];
        for k in 0..route.count() {
            let e = wh_nth_bit(route.eps, k).ok_or(WhError::Malformed)?;
            let n = self.endpoint_record(r, e, &mut buf)?;
            h.update(&wh_content_digest(&buf[..n]));
        }
        let m = traffic::EndpointManifest {
            service_id: self.svc.service,
            revision: self.gen,
            count: route.count() as u32,
            digest: h.finalize(),
        };
        let mut p = [0u8; traffic::MANIFEST_PAYLOAD_LEN];
        m.encode(&mut p);
        traffic::encode_envelope(
            traffic::RecordKind::EndpointManifest,
            self.gen,
            &self.op(),
            &p,
            out,
        )
        .map_err(|_| WhError::Buffer)
    }

    fn route_record(&self, r: u8, out: &mut [u8]) -> Result<usize, WhError> {
        let route = self.routes[r as usize];
        let s = self.svc;
        let mut mbuf = [0u8; 256];
        let mn = self.manifest_record(r, &mut mbuf)?;
        let listener = s.listeners[route.listener as usize];
        let mut admission = [0u8; 16];
        if s.nranges > 0 {
            let mut a = [0u8; 20 + WH_ADMISSION_ENTRY * WH_MAX_RANGES];
            let n = self.admission_payload(&mut a);
            admission = wh_id16(&traffic::sha256(&a[..n]));
        }
        let rec = traffic::RouteRecord {
            service_id: s.service,
            tenant_id: s.tenant,
            route_epoch: self.gen,
            listener,
            transport: s.transports[route.port as usize],
            port: s.ports[route.port as usize],
            prefix_len: if listener.family == traffic::AF_INET {
                32
            } else {
                128
            },
            termination: traffic::Termination::Passthrough,
            selection: if route.count() > 16 {
                traffic::SelectionPolicy::Maglev
            } else {
                traffic::SelectionPolicy::Rendezvous
            },
            continuity: if s.client_ip {
                traffic::ContinuityClass::DrainOnly
            } else {
                traffic::ContinuityClass::Reroutable
            },
            return_mode: traffic::ReturnMode::Proxy,
            affinity: if s.client_ip {
                traffic::AffinityPolicy::ApplicationKey
            } else {
                traffic::AffinityPolicy::Flow
            },
            source_identity: traffic::SourceIdentityMode::TransportPeer,
            policies: traffic::RoutePolicies {
                traffic_dial: match &s.dial {
                    Some(d) => d.id().map_err(|_| WhError::Malformed)?,
                    None => [0; 16],
                },
                service_chain: [0; 16],
                private_attachment: [0; 16],
                egress: [0; 16],
                health: [0; 16],
                admission,
                locality: wh_id16(&traffic::sha256(&self.policy_payload())),
                security: [0; 16],
            },
            hash_seed_ref: wh_seed_ref(&s.tenant, &s.service),
            endpoint_manifest_id: wh_id16(&wh_content_digest(&mbuf[..mn])),
            provenance_digest: s.provenance,
        };
        let mut p = [0u8; traffic::ROUTE_PAYLOAD_LEN];
        rec.encode(&mut p).map_err(|_| WhError::Malformed)?;
        traffic::encode_envelope(traffic::RecordKind::Route, self.gen, &self.op(), &p, out)
            .map_err(|_| WhError::Buffer)
    }

    /// The seal's digest: every other record's key and content digest, in
    /// lexicographic key order.
    fn seal_digest(&self) -> Result<[u8; 32], WhError> {
        let mut h = traffic::Sha256::new();
        let mut key = [0u8; 256];
        let mut val = [0u8; 1024];
        let mut i = 0;
        while let Some(item) = self.key_item(i) {
            let kl = self.key(item, &mut key).ok_or(WhError::Buffer)?;
            let vl = self.record(item, &mut val)?;
            h.update(&(kl as u16).to_le_bytes());
            h.update(&key[..kl]);
            h.update(&wh_content_digest(&val[..vl]));
            i += 1;
        }
        Ok(h.finalize())
    }

    /// The encoded record for `item` into `out`; its length.
    fn record(&self, item: WhItem, out: &mut [u8]) -> Result<usize, WhError> {
        match item {
            WhItem::Endpoint(r, e) => self.endpoint_record(r, e, out),
            WhItem::Manifest(r) => self.manifest_record(r, out),
            WhItem::Route(r) => self.route_record(r, out),
            WhItem::Policy => wh_envelope(
                WH_KIND_POLICY,
                self.gen,
                &self.op(),
                &self.policy_payload(),
                out,
            ),
            WhItem::Admission => {
                let mut a = [0u8; 20 + WH_ADMISSION_ENTRY * WH_MAX_RANGES];
                let n = self.admission_payload(&mut a);
                wh_envelope(WH_KIND_ADMISSION, self.gen, &self.op(), &a[..n], out)
            }
            WhItem::Dial => {
                let d = self.svc.dial.ok_or(WhError::Malformed)?;
                let mut p = [0u8; traffic::DIAL_PAYLOAD_LEN];
                d.encode(&mut p).map_err(|_| WhError::Malformed)?;
                traffic::encode_envelope(
                    traffic::RecordKind::TrafficDial,
                    self.gen,
                    &self.op(),
                    &p,
                    out,
                )
                .map_err(|_| WhError::Buffer)
            }
            WhItem::Seal => {
                let mut p = [0u8; WH_SEAL_LEN];
                p[..16].copy_from_slice(&self.svc.service);
                p[16..24].copy_from_slice(&self.gen.to_le_bytes());
                p[24..28].copy_from_slice(&((self.len() - 1) as u32).to_le_bytes());
                p[28..30].copy_from_slice(&(self.nroutes as u16).to_le_bytes());
                p[32..64].copy_from_slice(&self.seal_digest()?);
                wh_envelope(WH_KIND_SEAL, self.gen, &self.op(), &p, out)
            }
        }
    }

    /// The active pointer naming this generation:
    /// `gen=<g>;intent=<hex>;seal=<hex>;records=<n>;routes=<r>;svc=<ns>/<name>`.
    fn active(&self, intent: &[u8; 32], out: &mut [u8]) -> Result<usize, WhError> {
        let mut sb = [0u8; 256];
        let sl = self.record(WhItem::Seal, &mut sb)?;
        let seal = wh_content_digest(&sb[..sl]);
        let s = self.svc;
        (|| {
            let p = wh_put(out, 0, b"gen=")?;
            let p = wh_dec(out, p, self.gen)?;
            let p = wh_put(out, p, b";intent=")?;
            let p = wh_hex(out, p, intent)?;
            let p = wh_put(out, p, b";seal=")?;
            let p = wh_hex(out, p, &seal)?;
            let p = wh_put(out, p, b";records=")?;
            let p = wh_dec(out, p, self.len() as u64)?;
            let p = wh_put(out, p, b";routes=")?;
            let p = wh_dec(out, p, self.nroutes as u64)?;
            let p = wh_put(out, p, b";svc=")?;
            let p = wh_put(out, p, s.ns())?;
            let p = wh_put(out, p, b"/")?;
            wh_put(out, p, s.name())
        })()
        .ok_or(WhError::Buffer)
    }
}

/// The active pointer of a Service Wormhole is no longer told of: the next
/// generation, holding nothing (`withdrawn=1`). Generations never restart,
/// so a Service that returns continues from here.
fn wh_withdrawn(gen: u64, ns: &[u8], name: &[u8], out: &mut [u8]) -> Option<usize> {
    let p = wh_put(out, 0, b"gen=")?;
    let p = wh_dec(out, p, gen)?;
    let p = wh_put(out, p, b";intent=;seal=;records=0;routes=0;svc=")?;
    let p = wh_put(out, p, ns)?;
    let p = wh_put(out, p, b"/")?;
    let p = wh_put(out, p, name)?;
    wh_put(out, p, b";withdrawn=1")
}

// ── Publication (§9.5) ─────────────────────────────────────────────────

/// What the store answered one write.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WhAnswer {
    /// Decided and applied.
    Done,
    /// Taken, not yet decided (`EINPROGRESS`), or a read not yet answered:
    /// asked again, byte-identical, on a later step.
    Pending,
    /// A compare-and-swap whose predecessor moved: another publisher won.
    Lost,
    /// Refused for good (an errno).
    Failed(i32),
}

/// The store the publication writes through. The module answers it from
/// `_shared/store.rs`; the harness from a model.
trait WhStore {
    /// An unconditional PUT (a staging key nobody reads until activation).
    fn put(&mut self, key: &[u8], val: &[u8]) -> WhAnswer;
    /// The value at `key` into `out` and its revision; `Ok(None)` absent.
    fn get(&mut self, key: &[u8], out: &mut [u8]) -> Result<Option<(usize, u64)>, WhAnswer>;
    /// PUT conditional on the predecessor: its revision, or absent (None).
    fn cas(&mut self, key: &[u8], val: &[u8], rev: Option<u64>) -> WhAnswer;
}

/// The active pointer as read: absent, or its revision, generation and the
/// intent it was published for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct WhActive {
    rev: Option<u64>,
    gen: u64,
    intent: Option<[u8; 32]>,
}

fn wh_unhex32(h: &[u8]) -> Option<[u8; 32]> {
    if h.len() != 64 {
        return None;
    }
    let mut o = [0u8; 32];
    for i in 0..32 {
        o[i] = ((wh_hexval(h[2 * i])? << 4) | wh_hexval(h[2 * i + 1])?) as u8;
    }
    Some(o)
}

/// `/wh/<tenant>/<service>/active` for a Service into `out`; its length.
fn wh_active_key(tenant: &[u8; 16], service: &[u8; 16], out: &mut [u8]) -> Option<usize> {
    let p = wh_put(out, 0, b"/wh/")?;
    let p = wh_hex(out, p, tenant)?;
    let p = wh_put(out, p, b"/")?;
    let p = wh_hex(out, p, service)?;
    wh_put(out, p, b"/active")
}

fn wh_read_active(st: &mut impl WhStore, key: &[u8]) -> Result<WhActive, WhAnswer> {
    let mut v = [0u8; 512];
    match st.get(key, &mut v)? {
        None => Ok(WhActive {
            rev: None,
            gen: 0,
            intent: None,
        }),
        Some((n, rev)) => {
            let row = &v[..n];
            let gen = wh_u64(wh_field(row, b"gen=")).ok_or(WhAnswer::Failed(-5))?;
            Ok(WhActive {
                rev: Some(rev),
                gen,
                intent: wh_unhex32(wh_field(row, b"intent=")),
            })
        }
    }
}

/// One Service's publication in flight: the generation it stages, how far
/// it has written, and the predecessor its activation is conditional on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct WhPub {
    intent: [u8; 32],
    gen: u64,
    /// The next record (write order) to write; `len()` = all written.
    cursor: u32,
    pred: Option<u64>,
}

/// What one step of a publication came to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WhStep {
    /// More to write: step again.
    More,
    /// A write is undecided: asked again, unchanged, next step.
    Wait,
    /// The active pointer now names the generation.
    Activated,
    /// The predecessor moved under the CAS: read the active pointer again
    /// and stage the next generation.
    Raced,
    /// A write was refused, or a record could not be encoded.
    Failed,
}

/// A publication of `intent` after `active`: the next generation.
fn wh_begin(active: &WhActive, intent: &[u8; 32]) -> WhPub {
    WhPub {
        intent: *intent,
        gen: active.gen + 1,
        cursor: 0,
        pred: active.rev,
    }
}

/// Write up to `budget` records of `g` (whose generation is `p.gen`) from
/// the cursor, in order; with every record written, advance the active
/// pointer by compare-and-swap against the predecessor. Re-entrant: an
/// undecided write is asked again with the same bytes, which `g` rebuilds
/// identically from the same intent.
fn wh_publish(st: &mut impl WhStore, p: &mut WhPub, g: &WhGen, budget: usize) -> WhStep {
    let mut key = [0u8; 256];
    let mut val = [0u8; 1024];
    let total = g.len();
    let mut n = 0;
    while (p.cursor as usize) < total {
        if n == budget {
            return WhStep::More;
        }
        let Some(item) = g.write_item(p.cursor as usize) else {
            return WhStep::Failed;
        };
        let (Some(kl), Ok(vl)) = (g.key(item, &mut key), g.record(item, &mut val)) else {
            return WhStep::Failed;
        };
        match st.put(&key[..kl], &val[..vl]) {
            WhAnswer::Done => p.cursor += 1,
            WhAnswer::Pending => return WhStep::Wait,
            _ => return WhStep::Failed,
        }
        n += 1;
    }
    let Some(kl) = wh_active_key(&g.svc.tenant, &g.svc.service, &mut key) else {
        return WhStep::Failed;
    };
    let Ok(vl) = g.active(&p.intent, &mut val) else {
        return WhStep::Failed;
    };
    match st.cas(&key[..kl], &val[..vl], p.pred) {
        WhAnswer::Done => WhStep::Activated,
        WhAnswer::Pending => WhStep::Wait,
        WhAnswer::Lost => WhStep::Raced,
        WhAnswer::Failed(_) => WhStep::Failed,
    }
}

/// The generation a key under `/wh/<t>/<s>/gen/` names; None for another.
fn wh_key_gen(key: &[u8], gen_prefix_len: usize) -> Option<u64> {
    let rest = key.get(gen_prefix_len..)?;
    let end = rest.iter().position(|&c| c == b'/')?;
    wh_u64(&rest[..end])
}

/// Whether a staged or retired generation's key is garbage once `active`
/// is active: everything but the active generation and the one before it
/// (kept for the flows pinned to it, §9.3).
fn wh_collectable(gen: u64, active: u64) -> bool {
    gen != active && gen + 1 != active
}

// ── The VIP binding Wormhole writes back (§11.6) ──────────────────────

/// `/wh/<tenant>/<service>/vip/<4|6>`: the `VipBinding` record (WHR1 kind
/// 5) Wormhole's VIP controller keeps for the Service's address of that
/// family. Written by Wormhole, never by nanocloud: wormhole_emit writes
/// only `…/gen/` and `…/active` under the same Service.
fn wh_vip_key(tenant: &[u8; 16], service: &[u8; 16], family: u8, out: &mut [u8]) -> Option<usize> {
    let p = wh_put(out, 0, b"/wh/")?;
    let p = wh_hex(out, p, tenant)?;
    let p = wh_put(out, p, b"/")?;
    let p = wh_hex(out, p, service)?;
    wh_put(
        out,
        p,
        if family == traffic::AF_INET {
            b"/vip/4"
        } else {
            b"/vip/6"
        },
    )
}

/// A VIP binding as the status needs it: its phase and its address — only
/// when it decodes strictly, is this Service's, and of the key's family.
fn wh_vip(
    rec: &[u8],
    service: &[u8; 16],
    family: u8,
) -> Option<(traffic::VipPhase, traffic::NetAddr)> {
    let env = traffic::decode_envelope(rec).ok()?;
    if env.kind != traffic::RecordKind::VipBinding {
        return None;
    }
    let b = traffic::VipBinding::decode(env.payload).ok()?;
    (b.service_id == *service && b.prefix.family == family).then_some((b.phase, b.prefix))
}

/// Whether a phase is the admitted readiness a load-balancer status may
/// report (§6.1): ACTIVE, and DRAINING (still answering while it drains).
fn wh_vip_ready(phase: traffic::VipPhase) -> bool {
    matches!(
        phase,
        traffic::VipPhase::Active | traffic::VipPhase::Draining
    )
}

/// An address as text into `out` at `at`: dotted IPv4, or IPv6 in RFC 5952's
/// canonical form (lowercase, no leading zeros, the first longest run of two
/// or more zero groups as `::`) — what `status.loadBalancer.ingress[].ip`
/// says. The new end.
fn wh_fmt_addr(a: &traffic::NetAddr, out: &mut [u8], at: usize) -> Option<usize> {
    if a.family == traffic::AF_INET {
        let mut p = at;
        for (i, o) in a.bytes[12..].iter().enumerate() {
            if i > 0 {
                p = wh_put(out, p, b".")?;
            }
            p = wh_dec(out, p, u64::from(*o))?;
        }
        return Some(p);
    }
    let mut g = [0u16; 8];
    for (i, v) in g.iter_mut().enumerate() {
        *v = u16::from_be_bytes([a.bytes[2 * i], a.bytes[2 * i + 1]]);
    }
    // The first longest run of zero groups, if two or more.
    let (mut best, mut best_len, mut run, mut run_len) = (8usize, 0usize, 0usize, 0usize);
    for (i, v) in g.iter().enumerate() {
        if *v == 0 {
            if run_len == 0 {
                run = i;
            }
            run_len += 1;
            if run_len > best_len {
                best = run;
                best_len = run_len;
            }
        } else {
            run_len = 0;
        }
    }
    if best_len < 2 {
        best = 8;
    }
    let mut p = at;
    let mut i = 0;
    while i < 8 {
        if i == best {
            p = wh_put(out, p, b"::")?;
            i += best_len;
            continue;
        }
        if i > 0 && i != best + best_len {
            p = wh_put(out, p, b":")?;
        }
        let mut digits = [0u8; 4];
        let mut n = 0;
        let mut started = false;
        for shift in [12u16, 8, 4, 0] {
            let d = ((g[i] >> shift) & 15) as usize;
            if d != 0 || started || shift == 0 {
                digits[n] = WH_HEX[d];
                n += 1;
                started = true;
            }
        }
        p = wh_put(out, p, &digits[..n])?;
        i += 1;
    }
    Some(p)
}
