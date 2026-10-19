// A Pod's /etc/resolv.conf, by Kubernetes' dnsPolicy rules — `include!`d
// after json.rs. No allocator: the file is written into the caller's buffer.
//
//   ClusterFirst (the default)   the cluster DNS as the nameserver, the
//                                search path `<ns>.svc.<zone>`,
//                                `svc.<zone>`, `<zone>`, `options ndots:5`
//                                — then the Pod's dnsConfig merged in
//   ClusterFirstWithHostNet      the same, for a Pod on the host network too
//   ClusterFirst + hostNetwork   Default (upstream's fallback)
//   Default                      the node's own resolv.conf, unchanged
//   None                         the Pod's dnsConfig alone
//
// dnsConfig merges as the kubelet merges it: its nameservers after the
// policy's (no repeats), its searches after the policy's (no repeats), its
// options by name (a later `value` replaces an earlier one; an option with
// no value is written bare). At most three nameservers are written (the
// resolver reads no more) and at most 32 search domains.
//
// Not the kubelet's: the node's own search path is not appended under
// ClusterFirst, and a Default Pod's dnsConfig is not merged into the node's
// file (the node's file is not read where this runs; Default binds it as it
// is). The cluster DNS address and zone are the caller's.

/// What a Pod's resolv.conf is.
#[derive(PartialEq, Eq, Debug)]
enum PodDns {
    /// The node's own (`dnsPolicy: Default`, or ClusterFirst on the host
    /// network): bind the node's file.
    Node,
    /// This file, `out[..len]`.
    File(usize),
    /// Past `out`, or a dnsPolicy Kubernetes does not have: no file.
    Refused,
}

const PD_MAX_NS: usize = 3;
const PD_MAX_SEARCH: usize = 32;
const PD_MAX_OPTS: usize = 16;

struct PdOut<'a> {
    buf: &'a mut [u8],
    at: usize,
    full: bool,
}

impl PdOut<'_> {
    fn put(&mut self, b: &[u8]) {
        let end = self.at + b.len();
        match self.buf.get_mut(self.at..end) {
            Some(d) => {
                for (o, &i) in d.iter_mut().zip(b.iter()) {
                    *o = i;
                }
                self.at = end;
            }
            None => self.full = true,
        }
    }
}

/// Each string of the JSON array at `pod`'s `spec.dnsConfig.<field>`.
fn pd_list<'a>(pod: &'a [u8], field: &[u8], out: &mut [&'a [u8]; PD_MAX_SEARCH]) -> usize {
    let mut n = 0usize;
    if let Some(arr) = j_sub2(pod, b"spec", b"dnsConfig").and_then(|c| j_sub1(c, field)) {
        while n < out.len() {
            let Some(el) = j_idx(arr, n) else {
                break;
            };
            let el = j_trim_str(el);
            out[n] = el;
            n += 1;
        }
    }
    n
}

/// A JSON string element as its contents (quotes stripped).
fn j_trim_str(el: &[u8]) -> &[u8] {
    let mut s = 0;
    let mut e = el.len();
    while s < e && el[s].is_ascii_whitespace() {
        s += 1;
    }
    while e > s && el[e - 1].is_ascii_whitespace() {
        e -= 1;
    }
    let v = &el[s..e];
    match (v.first(), v.last()) {
        (Some(b'"'), Some(b'"')) if v.len() >= 2 => &v[1..v.len() - 1],
        _ => v,
    }
}

/// The resolv.conf of the Pod `pod` (its JSON) in namespace `ns`, with
/// `dns` the cluster DNS's address and `zone` the cluster domain.
fn pod_dns(pod: &[u8], ns: &[u8], dns: &[u8], zone: &[u8], out: &mut [u8]) -> PodDns {
    let policy = j_get2(pod, b"spec", b"dnsPolicy").unwrap_or(&b"ClusterFirst"[..]);
    let host = j_get2(pod, b"spec", b"hostNetwork") == Some(&b"true"[..]);
    let cluster = match policy {
        b"" | b"ClusterFirst" => !host,
        b"ClusterFirstWithHostNet" => true,
        b"Default" => false,
        b"None" => false,
        _ => return PodDns::Refused,
    };
    let none = policy == b"None";
    if !cluster && !none {
        return PodDns::Node;
    }
    let mut ns_list: [&[u8]; PD_MAX_SEARCH] = [&[]; PD_MAX_SEARCH];
    let mut search: [&[u8]; PD_MAX_SEARCH] = [&[]; PD_MAX_SEARCH];
    let mut extra: [&[u8]; PD_MAX_SEARCH] = [&[]; PD_MAX_SEARCH];
    let mut o = PdOut {
        buf: out,
        at: 0,
        full: false,
    };

    // nameservers: the cluster's, then dnsConfig's, no repeats, at most 3.
    let mut nn = 0usize;
    if cluster && !dns.is_empty() {
        ns_list[0] = dns;
        nn = 1;
    }
    let ne = pd_list(pod, b"nameservers", &mut extra);
    for &e in extra[..ne].iter() {
        if !e.is_empty() && nn < ns_list.len() && ns_list[..nn].iter().all(|&x| x != e) {
            ns_list[nn] = e;
            nn += 1;
        }
    }
    for &n in ns_list[..nn.min(PD_MAX_NS)].iter() {
        o.put(b"nameserver ");
        o.put(n);
        o.put(b"\n");
    }

    // search: the cluster's three, then dnsConfig's, no repeats.
    let mut sbuf = [0u8; 3 * 256];
    let mut sn = 0usize;
    if cluster {
        // `<ns>.svc.<zone>`, `svc.<zone>`, `<zone>`, built in `sbuf`.
        let mut at = 0usize;
        let mut parts: [(usize, usize); 3] = [(0, 0); 3];
        for (k, part) in parts.iter_mut().enumerate() {
            let start = at;
            let mut push = |b: &[u8], at: &mut usize| {
                for &c in b {
                    if let Some(d) = sbuf.get_mut(*at) {
                        *d = c;
                        *at += 1;
                    }
                }
            };
            if k == 0 {
                push(ns, &mut at);
                push(b".", &mut at);
            }
            if k <= 1 {
                push(b"svc.", &mut at);
            }
            push(zone, &mut at);
            *part = (start, at);
        }
        for &(a, b) in parts.iter() {
            search[sn] = &sbuf[a..b];
            sn += 1;
        }
    }
    let se = pd_list(pod, b"searches", &mut extra);
    for &e in extra[..se].iter() {
        if !e.is_empty() && sn < PD_MAX_SEARCH && search[..sn].iter().all(|&x| x != e) {
            search[sn] = e;
            sn += 1;
        }
    }
    if sn > 0 {
        o.put(b"search");
        for &s in search[..sn].iter() {
            o.put(b" ");
            o.put(s);
        }
        o.put(b"\n");
    }

    // options: ndots:5 for the cluster, then dnsConfig's, by name.
    let mut names: [&[u8]; PD_MAX_OPTS] = [&[]; PD_MAX_OPTS];
    let mut vals: [Option<&[u8]>; PD_MAX_OPTS] = [None; PD_MAX_OPTS];
    let mut on = 0usize;
    if cluster {
        names[0] = b"ndots";
        vals[0] = Some(b"5");
        on = 1;
    }
    if let Some(arr) = j_sub2(pod, b"spec", b"dnsConfig").and_then(|c| j_sub1(c, b"options")) {
        let mut i = 0;
        while let Some(el) = j_idx(arr, i) {
            i += 1;
            let Some(name) = j_get1(el, b"name").filter(|n| !n.is_empty()) else {
                continue;
            };
            let value = j_get1(el, b"value");
            match names[..on].iter().position(|&n| n == name) {
                Some(k) => vals[k] = value,
                None if on < PD_MAX_OPTS => {
                    names[on] = name;
                    vals[on] = value;
                    on += 1;
                }
                None => {}
            }
        }
    }
    if on > 0 {
        o.put(b"options");
        for (&n, &v) in names[..on].iter().zip(vals[..on].iter()) {
            o.put(b" ");
            o.put(n);
            if let Some(v) = v {
                o.put(b":");
                o.put(v);
            }
        }
        o.put(b"\n");
    }
    if o.full {
        return PodDns::Refused;
    }
    PodDns::File(o.at)
}
