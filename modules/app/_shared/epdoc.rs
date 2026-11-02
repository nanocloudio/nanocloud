// A Service's Endpoints documents, as the API serves them — rendered from the
// Service and the endpoints controller's per-Pod rows (endpoints.plan). Pure
// and no_std: `include!`d after `json.rs` by store_effect (op EP_DOCS, which
// reads the inputs and writes the outputs) and by the harness.
//
//   discovery.k8s.io/v1 EndpointSlice   /endpointslices/<ns>/<svc>
//   v1 Endpoints                         /endpoints/<ns>/<svc>
//
// The rows (`epd_row` walks them) are what the store keeps per endpoint,
// `<pod> US <flat row> US <the Pod's named ports> US <its other address> RS`,
// the flat row being `ip=…;state=<ready|notready|serving|terminating>;zone=…;
// node=…;gen=…;af=…;uid=…[;at=…];grace=…`, the named ports
// `<name>/<protocol>/<number>,…` (`epd_pod_ports`, read from the Pod only
// when a targetPort names a port) and the other address the Pod's
// `status.podIPs` member of the family the row's `ip` is not (read only for
// a dual-stack Service, `epd_pod_other_ip`).
//
// Conditions, as upstream's controllers set them (`pnr` is the Service's
// spec.publishNotReadyAddresses):
//   state        EndpointSlice ready/serving/terminating   Endpoints
//   ready        true  / true  / false                     addresses
//   notready     pnr   / pnr   / false                     notReadyAddresses (addresses with pnr)
//   serving      pnr   / true  / true                      left out (addresses with pnr)
//   terminating  pnr   / pnr   / true                      left out (addresses with pnr)
// Ports: each Service port, its targetPort resolved per endpoint — a number
// as it is (absent: the port), a NAME against the Pod's container ports of
// that name and protocol; a name the Pod does not declare leaves that port
// out for that endpoint.
//
// Slices, as upstream's EndpointSlice controller cuts them: one GROUP per
// address family the Service serves (spec.ipFamilies, else its clusterIP's,
// else IPv4) and set of resolved ports (`epd_group_add`), each group cut
// into slices of at most EPD_SHARD endpoints in row (Pod name) order. Names
// are deterministic, so a render of the same facts is the same documents:
// the PRIMARY group's first slice (`epd_primary`: the Service's first family,
// the port set that resolves every port, the lowest digest) is named after
// the Service; every other slice `<svc>-<digest>[-<n>]`, the digest the
// group's (`epd_hash`: its family and ports), `n` its shard past the first.
// A Service with no endpoints keeps one empty slice of its first family.
// The Endpoints' subsets group its first family's addresses by the ports
// they resolved to, as upstream's repacking does, at most EPD_EP_MAX of
// them (past it, upstream's `over-capacity: truncated` annotation).
const EPD_US: u8 = 0x1f;
const EPD_RS: u8 = 0x1e;
const EPD_LABEL_SVC: &[u8] = b"kubernetes.io/service-name";
const EPD_LABEL_SLICE: &[u8] = b"endpointslice.kubernetes.io/managed-by";
const EPD_MANAGER_SLICE: &[u8] = b"endpointslice-controller.k8s.io";
const EPD_LABEL_EP: &[u8] = b"endpoints.kubernetes.io/managed-by";
const EPD_MANAGER_EP: &[u8] = b"endpoint-controller";
const EPD_LABEL_HEADLESS: &[u8] = b"service.kubernetes.io/headless";
/// Service ports one render resolves (upstream allows more; a Service past
/// this renders its first `EPD_PORTS`).
const EPD_PORTS: usize = 16;
/// Endpoints in one EndpointSlice (upstream's maxEndpointsPerSlice default).
const EPD_SHARD: usize = 100;
/// Addresses in one v1 Endpoints (upstream's maxCapacity); past it the
/// document says it was truncated.
const EPD_EP_MAX: usize = 1000;
/// Distinct (family, port set) groups of one Service a render cuts slices
/// for; a Service with more is refused (logged), never silently cut.
const EPD_GROUPS: usize = 16;
const EPD_ANN_OVER: &[u8] = b"endpoints.kubernetes.io/over-capacity";

/// A bounded writer: every append past the end marks it overflowed, and the
/// render answers None rather than a cut document.
struct EpdW<'a> {
    out: &'a mut [u8],
    n: usize,
    over: bool,
}

impl EpdW<'_> {
    fn s(&mut self, b: &[u8]) {
        if self.over || self.n + b.len() > self.out.len() {
            self.over = true;
            return;
        }
        self.out[self.n..self.n + b.len()].copy_from_slice(b);
        self.n += b.len();
    }
    /// A JSON string of plain bytes (a name, an address), escaped.
    fn q(&mut self, b: &[u8]) {
        self.s(b"\"");
        for &c in b {
            match c {
                b'"' => self.s(b"\\\""),
                b'\\' => self.s(b"\\\\"),
                c if c < 0x20 => self.s(b"?"),
                c => self.s(core::slice::from_ref(&c)),
            }
        }
        self.s(b"\"");
    }
    fn num(&mut self, v: u32) {
        let mut d = [0u8; 10];
        let mut i = d.len();
        let mut v = v;
        loop {
            i -= 1;
            d[i] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        self.s(&d[i..]);
    }
    fn bool(&mut self, v: bool) {
        if v {
            self.s(b"true");
        } else {
            self.s(b"false");
        }
    }
}

/// The raw token at an object path (a string WITH its quotes, a number, an
/// object, an array), or None.
fn epd_raw<'a>(b: &'a [u8], path: &[&[u8]]) -> Option<&'a [u8]> {
    let mut cur = 0usize;
    for seg in path {
        let v = j_ws(b, cur);
        if v >= b.len() || b[v] != b'{' {
            return None;
        }
        cur = j_obj_get(b, v, seg)?;
    }
    let s = j_ws(b, cur);
    if s >= b.len() {
        return None;
    }
    let e = j_skip(b, s);
    Some(&b[s..e])
}

/// A string member's content (unquoted), or None when absent or not a string.
fn epd_str<'a>(b: &'a [u8], path: &[&[u8]]) -> Option<&'a [u8]> {
    let r = epd_raw(b, path)?;
    if r.len() >= 2 && r[0] == b'"' && r[r.len() - 1] == b'"' {
        Some(&r[1..r.len() - 1])
    } else {
        None
    }
}

// Paths as separate arguments: a path literal `&[b"a", b"b"]` const-promotes
// to a pointer table a PIC module cannot use (json.rs `j_get2`).
fn epd_raw1<'a>(b: &'a [u8], k1: &[u8]) -> Option<&'a [u8]> {
    epd_raw(b, &[k1])
}
fn epd_raw2<'a>(b: &'a [u8], k1: &[u8], k2: &[u8]) -> Option<&'a [u8]> {
    epd_raw(b, &[k1, k2])
}
fn epd_str1<'a>(b: &'a [u8], k1: &[u8]) -> Option<&'a [u8]> {
    epd_str(b, &[k1])
}
fn epd_str2<'a>(b: &'a [u8], k1: &[u8], k2: &[u8]) -> Option<&'a [u8]> {
    epd_str(b, &[k1, k2])
}
fn epd_str3<'a>(b: &'a [u8], k1: &[u8], k2: &[u8], k3: &[u8]) -> Option<&'a [u8]> {
    epd_str(b, &[k1, k2, k3])
}

fn epd_u32(t: &[u8]) -> Option<u32> {
    if t.is_empty() || t.len() > 9 {
        return None;
    }
    let mut v = 0u32;
    for &c in t {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + u32::from(c - b'0');
    }
    Some(v)
}

/// The members of the object `obj`, raw: `(key with its quotes, value)`.
/// Calls `f` for each; stops at anything malformed.
fn epd_members(obj: &[u8], mut f: impl FnMut(&[u8], &[u8])) {
    let mut j = j_ws(obj, 0);
    if j >= obj.len() || obj[j] != b'{' {
        return;
    }
    j += 1;
    loop {
        j = j_ws(obj, j);
        if j >= obj.len() || obj[j] != b'"' {
            return;
        }
        let ks = j;
        j = j_skip(obj, j);
        let key = &obj[ks..j.min(obj.len())];
        j = j_ws(obj, j);
        if j >= obj.len() || obj[j] != b':' {
            return;
        }
        let vs = j_ws(obj, j + 1);
        let ve = j_skip(obj, vs);
        if ve > obj.len() || vs >= ve {
            return;
        }
        f(key, &obj[vs..ve]);
        j = j_ws(obj, ve);
        if j < obj.len() && obj[j] == b',' {
            j += 1;
            continue;
        }
        return;
    }
}

/// One `;`-separated `name=value` of a flat row.
fn epd_flat<'a>(row: &'a [u8], name: &[u8]) -> &'a [u8] {
    for seg in row.split(|&c| c == b';') {
        if seg.len() > name.len() && seg.starts_with(name) && seg[name.len()] == b'=' {
            return &seg[name.len() + 1..];
        }
    }
    &[]
}

/// A Pod's named container ports, `<name>/<protocol>/<number>,…` (every
/// container's), for a targetPort that names one. Answers the length.
fn epd_pod_ports(pod: &[u8], out: &mut [u8]) -> usize {
    let mut w = EpdW {
        out,
        n: 0,
        over: false,
    };
    let Some(cs) = j_sub2(pod, b"spec", b"containers") else {
        return 0;
    };
    let mut i = 0usize;
    while let Some(c) = j_idx(cs, i) {
        i += 1;
        let Some(ps) = j_sub1(c, b"ports") else {
            continue;
        };
        let mut k = 0usize;
        while let Some(p) = j_idx(ps, k) {
            k += 1;
            let Some(name) = epd_str1(p, b"name") else {
                continue;
            };
            let Some(num) = j_get1(p, b"containerPort").and_then(epd_u32) else {
                continue;
            };
            let proto = epd_str1(p, b"protocol").unwrap_or(b"TCP");
            let at = w.n;
            if at > 0 {
                w.s(b",");
            }
            w.s(name);
            w.s(b"/");
            w.s(proto);
            w.s(b"/");
            w.num(num);
            if w.over {
                // A whole entry or none.
                w.over = false;
                w.n = at;
                return w.n;
            }
        }
    }
    w.n
}

/// Whether any of the Service's ports names its targetPort (so the Pods must
/// be read for their named ports).
fn epd_named_target(svc: &[u8]) -> bool {
    let Some(ps) = j_sub2(svc, b"spec", b"ports") else {
        return false;
    };
    let mut i = 0usize;
    while let Some(p) = j_idx(ps, i) {
        i += 1;
        if let Some(t) = epd_raw1(p, b"targetPort") {
            if t.first() == Some(&b'"') && epd_u32(&t[1..t.len() - 1]).is_none() {
                return true;
            }
        }
    }
    false
}

/// The next row of the rows buffer at `at`: `(pod, flat row, named ports,
/// other address, next)`.
fn epd_row(rows: &[u8], at: usize) -> Option<(&[u8], &[u8], &[u8], &[u8], usize)> {
    if at >= rows.len() {
        return None;
    }
    let end = at + rows[at..].iter().position(|&c| c == EPD_RS)?;
    let rec = &rows[at..end];
    let mut it = rec.splitn(4, |&c| c == EPD_US);
    let pod = it.next()?;
    let row = it.next()?;
    let ports = it.next().unwrap_or(&[]);
    let alt = it.next().unwrap_or(&[]);
    Some((pod, row, ports, alt, end + 1))
}

#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
fn epd_is_v6(ip: &[u8]) -> bool {
    ip.iter().any(|&c| c == b':')
}

/// The endpoint's address of the family `v6`: the row's own, else the
/// Pod's other one; empty when it has none of that family.
fn epd_addr<'a>(row: &'a [u8], alt: &'a [u8], v6: bool) -> &'a [u8] {
    let ip = epd_flat(row, b"ip");
    if !ip.is_empty() && epd_is_v6(ip) == v6 {
        ip
    } else if !alt.is_empty() && epd_is_v6(alt) == v6 {
        alt
    } else {
        &[]
    }
}

/// The families the Service serves: `(first is IPv6, it serves both)` —
/// spec.ipFamilies, else its clusterIP's family, else IPv4.
fn epd_families(svc: &[u8]) -> (bool, bool) {
    if let Some(fams) = j_sub2(svc, b"spec", b"ipFamilies") {
        if let Some(first) = j_idx(fams, 0) {
            let v6 = jp_unq(first) == b"IPv6";
            let dual = j_idx(fams, 1).is_some_and(|f| (jp_unq(f) == b"IPv6") != v6);
            return (v6, dual);
        }
    }
    let ip = epd_str2(svc, b"spec", b"clusterIP").unwrap_or(b"");
    (epd_is_v6(ip), false)
}

/// A token's string content (its quotes off), else the token.
fn jp_unq(v: &[u8]) -> &[u8] {
    if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
        &v[1..v.len() - 1]
    } else {
        v
    }
}

/// The Pod's `status.podIPs` member of the family the row's `ip` is NOT,
/// into `out`: its length (0 for none).
fn epd_pod_other_ip(pod: &[u8], row_ip: &[u8], out: &mut [u8]) -> usize {
    let Some(ips) = j_sub2(pod, b"status", b"podIPs") else {
        return 0;
    };
    let mine = epd_is_v6(row_ip);
    let mut i = 0usize;
    while let Some(e) = j_idx(ips, i) {
        i += 1;
        let Some(ip) = epd_str1(e, b"ip") else {
            continue;
        };
        if !ip.is_empty() && epd_is_v6(ip) != mine && ip.len() <= out.len() {
            out[..ip.len()].copy_from_slice(ip);
            return ip.len();
        }
    }
    0
}

/// One resolved port: (name, protocol, number).
#[derive(Clone, Copy)]
struct EpdPort<'a> {
    name: &'a [u8],
    proto: &'a [u8],
    num: u32,
}

/// The Service's ports resolved for one endpoint (its Pod's named ports):
/// how many, into `out`.
fn epd_resolve<'a>(svc: &'a [u8], pod_ports: &[u8], out: &mut [EpdPort<'a>; EPD_PORTS]) -> usize {
    let Some(ps) = j_sub2(svc, b"spec", b"ports") else {
        return 0;
    };
    let mut n = 0usize;
    let mut i = 0usize;
    while let Some(p) = j_idx(ps, i) {
        i += 1;
        if n == EPD_PORTS {
            break;
        }
        let name = epd_str1(p, b"name").unwrap_or(b"");
        let proto = epd_str1(p, b"protocol").unwrap_or(b"TCP");
        let port = j_get1(p, b"port").and_then(epd_u32).unwrap_or(0);
        let num = match epd_raw1(p, b"targetPort") {
            None => port,
            Some(t) if t.first() == Some(&b'"') => {
                let tn = &t[1..t.len() - 1];
                match epd_u32(tn) {
                    Some(v) => v,
                    None => {
                        // A NAME: the Pod's port of that name and protocol.
                        let mut v = 0u32;
                        for e in pod_ports.split(|&c| c == b',') {
                            let mut f = e.splitn(3, |&c| c == b'/');
                            let (Some(en), Some(ep), Some(ev)) = (f.next(), f.next(), f.next())
                            else {
                                continue;
                            };
                            if en == tn && ep == proto {
                                v = epd_u32(ev).unwrap_or(0);
                                break;
                            }
                        }
                        v
                    }
                }
            }
            Some(t) => epd_u32(t).unwrap_or(0),
        };
        if num == 0 {
            continue;
        }
        out[n] = EpdPort { name, proto, num };
        n += 1;
    }
    n
}

fn epd_same(a: &[EpdPort], b: &[EpdPort]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(x, y)| x.num == y.num && x.name == y.name && x.proto == y.proto)
}

/// The metadata both documents share: name, namespace, identity, and the
/// Service's labels with the reserved ones replaced by this controller's:
/// `mgr_label: mgr`, the service-name label naming `svc_label`, the headless
/// one when `headless`.
#[allow(
    clippy::too_many_arguments,
    reason = "the metadata's parts, each the caller's"
)]
fn epd_meta(
    w: &mut EpdW,
    svc: &[u8],
    ns: &[u8],
    name: &[u8],
    uid: &[u8],
    ts: &[u8],
    mgr_label: &[u8],
    mgr: &[u8],
    svc_label: Option<&[u8]>,
    headless: bool,
) {
    w.s(b"\"metadata\":{\"name\":");
    w.q(name);
    w.s(b",\"namespace\":");
    w.q(ns);
    if !uid.is_empty() {
        w.s(b",\"uid\":");
        w.q(uid);
    }
    if !ts.is_empty() {
        w.s(b",\"creationTimestamp\":");
        w.q(ts);
    }
    w.s(b",\"labels\":{");
    let mut first = true;
    if let Some(labels) = j_sub2(svc, b"metadata", b"labels") {
        epd_members(labels, |k, v| {
            let bare = if k.len() >= 2 { &k[1..k.len() - 1] } else { k };
            if bare == mgr_label
                || (svc_label.is_some() && bare == EPD_LABEL_SVC)
                || (headless && bare == EPD_LABEL_HEADLESS)
            {
                return;
            }
            if !first {
                w.s(b",");
            }
            first = false;
            w.s(k);
            w.s(b":");
            w.s(v);
        });
    }
    if !first {
        w.s(b",");
    }
    w.q(mgr_label);
    w.s(b":");
    w.q(mgr);
    if let Some(sn) = svc_label {
        w.s(b",");
        w.q(EPD_LABEL_SVC);
        w.s(b":");
        w.q(sn);
    }
    if headless {
        w.s(b",");
        w.q(EPD_LABEL_HEADLESS);
        w.s(b":\"\"");
    }
    w.s(b"}");
}

fn epd_headless(svc: &[u8]) -> bool {
    epd_str2(svc, b"spec", b"clusterIP") == Some(b"None")
}

fn epd_pnr(svc: &[u8]) -> bool {
    epd_raw2(svc, b"spec", b"publishNotReadyAddresses") == Some(b"true")
}

/// One slice group: an address family and the ports its endpoints resolve
/// to, its digest and how many endpoints it has.
#[derive(Clone, Copy)]
struct EpdGroup<'a> {
    v6: bool,
    ports: [EpdPort<'a>; EPD_PORTS],
    n: usize,
    hash: u32,
    count: usize,
}

fn epd_group_none<'a>() -> EpdGroup<'a> {
    EpdGroup {
        v6: false,
        ports: [EpdPort {
            name: b"",
            proto: b"",
            num: 0,
        }; EPD_PORTS],
        n: 0,
        hash: 0,
        count: 0,
    }
}

/// A group's digest: FNV-1a over its family and ports.
fn epd_hash(v6: bool, ports: &[EpdPort]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    let mut eat = |b: &[u8]| {
        for &c in b {
            h ^= u32::from(c);
            h = h.wrapping_mul(0x0100_0193);
        }
    };
    eat(if v6 { b"6" } else { b"4" });
    for p in ports {
        let mut d = [0u8; 10];
        let mut w = EpdW {
            out: &mut d,
            n: 0,
            over: false,
        };
        w.num(p.num);
        let dn = w.n;
        eat(b"|");
        eat(p.name);
        eat(b"/");
        eat(p.proto);
        eat(b"/");
        eat(&d[..dn]);
    }
    h
}

/// Count one row into the groups it belongs to — one per family the Service
/// serves that the endpoint has an address of. False when a new group would
/// not fit `EPD_GROUPS`.
fn epd_group_add<'a>(
    groups: &mut [EpdGroup<'a>; EPD_GROUPS],
    ng: &mut usize,
    svc: &'a [u8],
    row: &[u8],
    pp: &[u8],
    alt: &[u8],
) -> bool {
    let (first6, dual) = epd_families(svc);
    let mut got = [EpdPort {
        name: b"",
        proto: b"",
        num: 0,
    }; EPD_PORTS];
    // The ports borrow the Service alone (a name resolves to a number), so
    // the group outlives the row.
    let gn = epd_resolve(svc, pp, &mut got);
    for f in 0..2 {
        if f == 1 && !dual {
            break;
        }
        let v6 = if f == 0 { first6 } else { !first6 };
        if epd_addr(row, alt, v6).is_empty() {
            continue;
        }
        let mut found = false;
        for g in groups[..*ng].iter_mut() {
            if g.v6 == v6 && epd_same(&g.ports[..g.n], &got[..gn]) {
                g.count += 1;
                found = true;
                break;
            }
        }
        if found {
            continue;
        }
        if *ng == EPD_GROUPS {
            return false;
        }
        groups[*ng] = EpdGroup {
            v6,
            ports: got,
            n: gn,
            hash: epd_hash(v6, &got[..gn]),
            count: 1,
        };
        *ng += 1;
    }
    true
}

/// The group whose first slice is named after the Service: of the Service's
/// first family when it has any (else of any), the port set that resolves
/// EVERY Service port before one that does not, then the lowest digest.
fn epd_primary(groups: &[EpdGroup], svc: &[u8]) -> Option<usize> {
    let (first6, _) = epd_families(svc);
    let all = j_sub2(svc, b"spec", b"ports")
        .map(|ps| {
            let mut n = 0usize;
            while j_idx(ps, n).is_some() {
                n += 1;
            }
            n.min(EPD_PORTS)
        })
        .unwrap_or(0);
    let fam_has = groups.iter().any(|g| g.v6 == first6);
    let mut best: Option<usize> = None;
    for (i, g) in groups.iter().enumerate() {
        if fam_has && g.v6 != first6 {
            continue;
        }
        best = match best {
            None => Some(i),
            Some(b) => {
                let o = &groups[b];
                let (gf, of) = (g.n == all, o.n == all);
                if (gf && !of) || (gf == of && g.hash < o.hash) {
                    Some(i)
                } else {
                    Some(b)
                }
            }
        };
    }
    best
}

/// How many slices a group is cut into.
fn epd_shards(g: &EpdGroup) -> usize {
    g.count.div_ceil(EPD_SHARD).max(1)
}

/// The name of slice `k` of a group: the Service's own for the primary
/// group's first, else `<svc>-<digest>[-<k>]`. Its length, or None past
/// `out`.
fn epd_slice_name(
    svc_name: &[u8],
    primary: bool,
    hash: u32,
    k: usize,
    out: &mut [u8],
) -> Option<usize> {
    let mut w = EpdW {
        out,
        n: 0,
        over: false,
    };
    w.s(svc_name);
    if !(primary && k == 0) {
        w.s(b"-");
        let hex = |v: u32| {
            if v < 10 {
                b'0' + v as u8
            } else {
                b'a' + (v - 10) as u8
            }
        };
        let mut d = [0u8; 8];
        for (i, c) in d.iter_mut().enumerate() {
            *c = hex((hash >> (28 - 4 * i)) & 15);
        }
        w.s(&d);
        if k > 0 {
            w.s(b"-");
            w.num(k as u32);
        }
    }
    if w.over {
        None
    } else {
        Some(w.n)
    }
}

/// Whether `name` is a slice the groups make for the Service `svc_name`
/// (so a stale one, of a group gone or a shard past its count, is not).
fn epd_slice_current(
    name: &[u8],
    svc_name: &[u8],
    groups: &[EpdGroup],
    primary: Option<usize>,
) -> bool {
    if name == svc_name {
        return true;
    }
    let mut nb = [0u8; 300];
    for (i, g) in groups.iter().enumerate() {
        for k in 0..epd_shards(g) {
            if let Some(n) = epd_slice_name(svc_name, primary == Some(i), g.hash, k, &mut nb) {
                if &nb[..n] == name {
                    return true;
                }
            }
        }
    }
    false
}

/// Whether a row belongs to the group `(v6, want)`.
fn epd_in_group(svc: &[u8], row: &[u8], pp: &[u8], alt: &[u8], v6: bool, want: &[EpdPort]) -> bool {
    if epd_addr(row, alt, v6).is_empty() {
        return false;
    }
    let mut got = [EpdPort {
        name: b"",
        proto: b"",
        num: 0,
    }; EPD_PORTS];
    let gn = epd_resolve(svc, pp, &mut got);
    epd_same(want, &got[..gn])
}

/// The EndpointSlice `slice_name` of the Service `svc` (`ns`/`svc_name`):
/// the rows of `rows` in the group `(v6, want)`, at most EPD_SHARD of them
/// (the caller hands one shard), with the identity given. Answers the
/// length, or None when it does not fit `out`.
#[allow(
    clippy::too_many_arguments,
    reason = "the render's inputs, each read by the caller"
)]
fn epd_slice(
    svc: &[u8],
    ns: &[u8],
    svc_name: &[u8],
    slice_name: &[u8],
    v6: bool,
    want: &[EpdPort],
    rows: &[u8],
    uid: &[u8],
    ts: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut w = EpdW {
        out,
        n: 0,
        over: false,
    };
    let pnr = epd_pnr(svc);
    w.s(b"{\"apiVersion\":\"discovery.k8s.io/v1\",\"kind\":\"EndpointSlice\",");
    let headless = epd_headless(svc);
    epd_meta(
        &mut w,
        svc,
        ns,
        slice_name,
        uid,
        ts,
        EPD_LABEL_SLICE,
        EPD_MANAGER_SLICE,
        Some(svc_name),
        headless,
    );
    w.s(b",\"ownerReferences\":[{\"apiVersion\":\"v1\",\"kind\":\"Service\",\"name\":");
    w.q(svc_name);
    if let Some(su) = epd_str2(svc, b"metadata", b"uid") {
        w.s(b",\"uid\":");
        w.q(su);
    }
    w.s(b",\"controller\":true,\"blockOwnerDeletion\":true}]}");
    if v6 {
        w.s(b",\"addressType\":\"IPv6\",\"endpoints\":[");
    } else {
        w.s(b",\"addressType\":\"IPv4\",\"endpoints\":[");
    }
    let mut at = 0usize;
    let mut first = true;
    let mut taken = 0usize;
    while let Some((pod, row, pp, alt, next)) = epd_row(rows, at) {
        at = next;
        if taken == EPD_SHARD || !epd_in_group(svc, row, pp, alt, v6, want) {
            continue;
        }
        taken += 1;
        let ip = epd_addr(row, alt, v6);
        let state = epd_flat(row, b"state");
        let (ready, serving, term) = match state {
            b"ready" => (true, true, false),
            b"serving" => (pnr, true, true),
            b"terminating" => (pnr, pnr, true),
            _ => (pnr, pnr, false),
        };
        if !first {
            w.s(b",");
        }
        first = false;
        w.s(b"{\"addresses\":[");
        w.q(ip);
        w.s(b"],\"conditions\":{\"ready\":");
        w.bool(ready);
        w.s(b",\"serving\":");
        w.bool(serving);
        w.s(b",\"terminating\":");
        w.bool(term);
        w.s(b"}");
        let uid = epd_flat(row, b"uid");
        if !uid.is_empty() {
            w.s(b",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":");
            w.q(ns);
            w.s(b",\"name\":");
            w.q(pod);
            w.s(b",\"uid\":");
            w.q(uid);
            w.s(b"}");
        }
        let node = epd_flat(row, b"node");
        if !node.is_empty() {
            w.s(b",\"nodeName\":");
            w.q(node);
        }
        let zone = epd_flat(row, b"zone");
        if !zone.is_empty() {
            w.s(b",\"zone\":");
            w.q(zone);
        }
        w.s(b"}");
    }
    w.s(b"],\"ports\":[");
    for (i, p) in want.iter().enumerate() {
        if i > 0 {
            w.s(b",");
        }
        w.s(b"{\"name\":");
        w.q(p.name);
        w.s(b",\"protocol\":");
        w.q(p.proto);
        w.s(b",\"port\":");
        w.num(p.num);
        w.s(b"}");
    }
    w.s(b"]}");
    if w.over {
        None
    } else {
        Some(w.n)
    }
}

/// Where an endpoint goes in the Endpoints document: 1 addresses, 2
/// notReadyAddresses, 0 neither.
fn epd_ep_slot(state: &[u8], pnr: bool) -> u8 {
    match state {
        b"ready" => 1,
        _ if pnr => 1,
        b"notready" => 2,
        _ => 0,
    }
}

/// Whether a row is one of the Endpoints' (its first family's) addresses.
fn epd_ep_listed(row: &[u8], alt: &[u8], v6: bool, pnr: bool) -> bool {
    epd_ep_slot(epd_flat(row, b"state"), pnr) != 0 && !epd_addr(row, alt, v6).is_empty()
}

fn epd_address(w: &mut EpdW, ns: &[u8], pod: &[u8], ip: &[u8], row: &[u8]) {
    w.s(b"{\"ip\":");
    w.q(ip);
    let node = epd_flat(row, b"node");
    if !node.is_empty() {
        w.s(b",\"nodeName\":");
        w.q(node);
    }
    let uid = epd_flat(row, b"uid");
    if !uid.is_empty() {
        w.s(b",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":");
        w.q(ns);
        w.s(b",\"name\":");
        w.q(pod);
        w.s(b",\"uid\":");
        w.q(uid);
        w.s(b"}");
    }
    w.s(b"}");
}

/// The v1 Endpoints of the Service over `rows`: its first family's
/// addresses, one subset per distinct set of resolved ports, each its
/// addresses and notReadyAddresses — at most EPD_EP_MAX (the caller hands
/// no more), `truncated` when it had more. Answers the length, or None when
/// it does not fit `out`.
#[allow(
    clippy::too_many_arguments,
    reason = "the render's inputs, each read by the caller"
)]
fn epd_endpoints(
    svc: &[u8],
    ns: &[u8],
    name: &[u8],
    rows: &[u8],
    truncated: bool,
    uid: &[u8],
    ts: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let mut w = EpdW {
        out,
        n: 0,
        over: false,
    };
    let pnr = epd_pnr(svc);
    let (v6, _) = epd_families(svc);
    w.s(b"{\"apiVersion\":\"v1\",\"kind\":\"Endpoints\",");
    let headless = epd_headless(svc);
    epd_meta(
        &mut w,
        svc,
        ns,
        name,
        uid,
        ts,
        EPD_LABEL_EP,
        EPD_MANAGER_EP,
        None,
        headless,
    );
    if truncated {
        w.s(b",\"annotations\":{");
        w.q(EPD_ANN_OVER);
        w.s(b":\"truncated\"}");
    }
    w.s(b"}");
    let mut subsets = 0usize;
    let mut at = 0usize;
    // Each row that is the FIRST of its port set (among those listed) opens
    // a subset holding every row of that set.
    while let Some((_, row, pp, alt, next)) = epd_row(rows, at) {
        let here = at;
        at = next;
        if !epd_ep_listed(row, alt, v6, pnr) {
            continue;
        }
        let mut mine = [EpdPort {
            name: b"",
            proto: b"",
            num: 0,
        }; EPD_PORTS];
        let mn = epd_resolve(svc, pp, &mut mine);
        let mut seen = false;
        let mut b = 0usize;
        while let Some((_, brow, bpp, balt, bnext)) = epd_row(rows, b) {
            if b == here {
                break;
            }
            b = bnext;
            if !epd_ep_listed(brow, balt, v6, pnr) {
                continue;
            }
            let mut other = [EpdPort {
                name: b"",
                proto: b"",
                num: 0,
            }; EPD_PORTS];
            let on = epd_resolve(svc, bpp, &mut other);
            if epd_same(&mine[..mn], &other[..on]) {
                seen = true;
                break;
            }
        }
        if seen {
            continue;
        }
        if subsets == 0 {
            w.s(b",\"subsets\":[{");
        } else {
            w.s(b",{");
        }
        subsets += 1;
        let mut wrote_field = false;
        for slot in [1u8, 2] {
            let mut opened = false;
            let mut c = here;
            while let Some((cpod, crow, cpp, calt, cnext)) = epd_row(rows, c) {
                c = cnext;
                if !epd_ep_listed(crow, calt, v6, pnr)
                    || epd_ep_slot(epd_flat(crow, b"state"), pnr) != slot
                {
                    continue;
                }
                let mut other = [EpdPort {
                    name: b"",
                    proto: b"",
                    num: 0,
                }; EPD_PORTS];
                let on = epd_resolve(svc, cpp, &mut other);
                if !epd_same(&mine[..mn], &other[..on]) {
                    continue;
                }
                if !opened {
                    if wrote_field {
                        w.s(b",");
                    }
                    if slot == 1 {
                        w.s(b"\"addresses\":[");
                    } else {
                        w.s(b"\"notReadyAddresses\":[");
                    }
                    opened = true;
                    wrote_field = true;
                } else {
                    w.s(b",");
                }
                epd_address(&mut w, ns, cpod, epd_addr(crow, calt, v6), crow);
            }
            if opened {
                w.s(b"]");
            }
        }
        if mn > 0 {
            w.s(b",\"ports\":[");
            for (i, p) in mine[..mn].iter().enumerate() {
                if i > 0 {
                    w.s(b",");
                }
                w.s(b"{");
                if !p.name.is_empty() {
                    w.s(b"\"name\":");
                    w.q(p.name);
                    w.s(b",");
                }
                w.s(b"\"port\":");
                w.num(p.num);
                w.s(b",\"protocol\":");
                w.q(p.proto);
                w.s(b"}");
            }
            w.s(b"]");
        }
        w.s(b"}");
    }
    if subsets > 0 {
        w.s(b"]");
    }
    w.s(b"}");
    if w.over {
        None
    } else {
        Some(w.n)
    }
}

/// Whether a stored document is this controller's: its `label` says `manager`.
fn epd_managed(doc: &[u8], label: &[u8], manager: &[u8]) -> bool {
    epd_str3(doc, b"metadata", b"labels", label) == Some(manager)
}

/// A uuid (v4 layout) from 16 random bytes, 36 characters.
fn epd_uuid(r: &[u8; 16], out: &mut [u8; 36]) {
    let hex = |v: u8| if v < 10 { b'0' + v } else { b'a' + v - 10 };
    let mut b = *r;
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let mut o = 0usize;
    for (i, &x) in b.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out[o] = b'-';
            o += 1;
        }
        out[o] = hex(x >> 4);
        out[o + 1] = hex(x & 15);
        o += 2;
    }
}
