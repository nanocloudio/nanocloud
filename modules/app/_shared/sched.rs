// The scheduler's pick — `store_source`'s `sched` param. `include!`d by
// store_source after json.rs, jsonpatch.rs and pathmod.rs.
//
// `sched = "<nodes prefix>:<pods prefix>[:<pdb prefix>]"`, beside a `join`
// over the ready nodes' names: the join reduces to the ONE node the object —
// a pending Pod — is placed on, as upstream's default scheduler profile
// places it. Filters, in upstream's order, each node failing at its first:
//
//   NodeUnschedulable  spec.unschedulable (`kubectl cordon`), unless the Pod
//                      tolerates node.kubernetes.io/unschedulable:NoSchedule
//   NodeAffinity       spec.nodeSelector, and the required node affinity
//                      (nodeSelectorTerms ORed, their matchExpressions —
//                      In, NotIn, Exists, DoesNotExist, Gt, Lt — and
//                      matchFields on metadata.name ANDed)
//   TaintToleration    every NoSchedule and NoExecute taint tolerated
//   NodeResourcesFit   the Pod's requests (cpu, memory; a container's limit
//                      when it states no request; the largest init
//                      container's when larger) beside those of the Pods
//                      already bound there, within status.allocatable — and
//                      one more Pod within allocatable.pods. A node that
//                      reports no allocatable is not limited by it
//   InterPodAffinity   required affinity: a matching Pod in the node's
//                      topology domain (or none anywhere and the Pod matches
//                      its own term); required anti-affinity: none there
//   PodTopologySpread  DoNotSchedule constraints: the domain's matching
//                      Pods plus this one within maxSkew of the least
//
// Scores, upstream's plugins normalised to 0..100 and weighted as its
// default profile weights them: TaintToleration (PreferNoSchedule taints
// not tolerated, x3), NodeAffinity (preferred terms' weights, x2),
// InterPodAffinity (preferred affinity minus anti-affinity weights, x2),
// PodTopologySpread (ScheduleAnyway skew, x2), and NodeResourcesFit's
// LeastAllocated (x1); ties to the node with the fewest Pods, then the
// lowest name. A Pod no node fits gets upstream's FailedScheduling message:
// `0/<n> nodes are available: <count> <reason>, ….` (reasons sorted).
//
// Preemption (`sched_preempt`): a Pod no node fits, whose preemptionPolicy
// is not Never, may evict LOWER-priority Pods from a node it fails only for
// resources: all of them are taken off, then given back (reprieved) as far
// as the Pod still fits — those a PodDisruptionBudget protects first, then
// by descending priority. The node chosen has the fewest budget violations,
// then the lowest highest-victim priority, then the fewest victims.

const SCH_NODES: usize = 32;
const SCH_TERMS: usize = 8;
const SCH_NAME: usize = 64;
const SCH_VICTIMS: usize = 32;
/// "No limit": a node that reports no allocatable for a resource.
const SCH_NOLIMIT: u64 = u64::MAX;

const R_UNSCHED: u8 = 1;
const R_AFFINITY: u8 = 2;
const R_TAINT: u8 = 3;
const R_RES: u8 = 4;
const R_PODAFF: u8 = 5;
const R_ANTI: u8 = 6;
const R_SPREAD: u8 = 7;

const T_AFF: u8 = 1;
const T_ANTI: u8 = 2;
const T_PAFF: u8 = 3;
const T_PANTI: u8 = 4;
const T_SPREAD: u8 = 5;
const T_SOFT: u8 = 6;

#[derive(Clone, Copy)]
struct SchNode {
    name: [u8; SCH_NAME],
    nl: u8,
    /// The first filter it failed (0: none yet).
    fail: u8,
    /// R_RES: which resources (1 cpu, 2 memory, 4 pods).
    res: u8,
    /// Passed the node-local filters (selector/affinity): counted as a
    /// spread domain.
    eligible: bool,
    alloc_cpu: u64,
    alloc_mem: u64,
    alloc_pods: u64,
    used_cpu: u64,
    used_mem: u64,
    pods: u64,
    pref: i64,
    soft: i64,
    /// Per term: the node's topology value (an FNV-1a digest; 0 = the node
    /// has no such label) and the matching Pods bound to it.
    topo: [u64; SCH_TERMS],
    tmatch: [u32; SCH_TERMS],
    score: i64,
}

impl SchNode {
    const fn new() -> SchNode {
        SchNode {
            name: [0; SCH_NAME],
            nl: 0,
            fail: 0,
            res: 0,
            eligible: false,
            alloc_cpu: SCH_NOLIMIT,
            alloc_mem: SCH_NOLIMIT,
            alloc_pods: SCH_NOLIMIT,
            used_cpu: 0,
            used_mem: 0,
            pods: 0,
            pref: 0,
            soft: 0,
            topo: [0; SCH_TERMS],
            tmatch: [0; SCH_TERMS],
            score: 0,
        }
    }
    fn name(&self) -> &[u8] {
        self.name.get(..self.nl as usize).unwrap_or(&[])
    }
}

/// One term of the pending Pod: an affinity/anti-affinity term (required or
/// preferred) or a spread constraint. Its selector and namespaces are byte
/// ranges of the Pod's own value.
#[derive(Clone, Copy)]
struct SchTerm {
    kind: u8,
    /// A preferred term's weight; a spread constraint's maxSkew.
    weight: i64,
    key: (usize, usize),
    sel: (usize, usize),
    nss: (usize, usize),
}

/// One pick's inputs: the source, the pending Pod and its terms.
struct SchPick<'a> {
    s: &'a State,
    sys: &'a SyscallTable,
    tail: &'a [u8],
    pod: &'a [u8],
    terms: &'a [SchTerm],
}

/// What a pick says beyond the node: why nothing fits, and a preemption.
struct SchOut {
    msg: [u8; 320],
    ml: usize,
    nom: [u8; SCH_NAME],
    nl: usize,
    victims: [u8; 512],
    vl: usize,
    /// Which victim the record being projected carries.
    vi: usize,
}

impl SchOut {
    const fn new() -> SchOut {
        SchOut {
            msg: [0; 320],
            ml: 0,
            nom: [0; SCH_NAME],
            nl: 0,
            victims: [0; 512],
            vl: 0,
            vi: 0,
        }
    }

    /// The `vi`-th victim (`<ns>/<name>`), or empty.
    fn victim(&self) -> &[u8] {
        let all = self.victims.get(..self.vl).unwrap_or(&[]);
        all.split(|&b| b == b',').nth(self.vi).unwrap_or(&[])
    }
}

fn sk_fnv(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in b {
        h ^= u64::from(c);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h | 1
}

/// A nested member by a dotted path of keys (no array indices).
fn sk_at<'a>(o: &'a [u8], path: &[u8]) -> Option<&'a [u8]> {
    let mut cur = o;
    for k in path.split(|&b| b == b'.') {
        cur = jp_get(cur, k)?;
    }
    Some(cur)
}

fn sk_range(base: &[u8], part: &[u8]) -> (usize, usize) {
    let b = base.as_ptr() as usize;
    let p = part.as_ptr() as usize;
    if p >= b && p + part.len() <= b + base.len() {
        (p - b, p - b + part.len())
    } else {
        (0, 0)
    }
}

fn sk_slice(base: &[u8], r: (usize, usize)) -> &[u8] {
    base.get(r.0..r.1).unwrap_or(&[])
}

/// Upstream's ToleratesTaint: a toleration naming an effect must name the
/// taint's; `Exists` matches the key (an empty key: every taint); `Equal`
/// (or no operator) the key and the value.
fn sk_tolerates(tols: &[u8], key: &[u8], value: &[u8], effect: &[u8]) -> bool {
    for t in jp_elems(tols) {
        let te = sk_str(t, b"effect");
        if !te.is_empty() && te != effect {
            continue;
        }
        let tk = sk_str(t, b"key");
        let op = sk_str(t, b"operator");
        if op == b"Exists" {
            if tk.is_empty() || tk == key {
                return true;
            }
            continue;
        }
        if tk == key && sk_str(t, b"value") == value {
            return true;
        }
    }
    false
}

/// The toleration that tolerates a NoExecute taint for longest: `None` none
/// does; `Some(-1)` one without tolerationSeconds (for ever); `Some(s)` the
/// least seconds among those that tolerate it (upstream's minimum).
fn sk_noexec_secs(tols: &[u8], key: &[u8], value: &[u8]) -> Option<i64> {
    let mut best: Option<i64> = None;
    for t in jp_elems(tols) {
        let te = sk_str(t, b"effect");
        if !te.is_empty() && te != b"NoExecute" {
            continue;
        }
        let tk = sk_str(t, b"key");
        let op = sk_str(t, b"operator");
        let hit = if op == b"Exists" {
            tk.is_empty() || tk == key
        } else {
            tk == key && sk_str(t, b"value") == value
        };
        if !hit {
            continue;
        }
        // Zero or negative seconds evict at once (upstream).
        let secs = match jp_get(t, b"tolerationSeconds") {
            Some(v) if !jp_is_null(v) => sk_int(v).max(0),
            _ => -1,
        };
        best = Some(match best {
            None => secs,
            Some(-1) => secs,
            Some(b) if secs == -1 => b,
            Some(b) => b.min(secs),
        });
    }
    best
}

/// A NodeSelectorRequirement over a node's labels (or its name, for a
/// matchFields one): In, NotIn, Exists, DoesNotExist, Gt, Lt.
fn sk_node_req(e: &[u8], got: Option<&[u8]>) -> bool {
    let op = sk_str(e, b"operator");
    let vals = jp_get(e, b"values").unwrap_or(b"[]");
    match op {
        b"In" => got.is_some_and(|g| sk_in(vals, g)),
        b"NotIn" => got.is_none_or(|g| !sk_in(vals, g)),
        b"Exists" => got.is_some(),
        b"DoesNotExist" => got.is_none(),
        b"Gt" | b"Lt" => {
            let Some(g) = got else {
                return false;
            };
            if g.is_empty() || !g.iter().all(|c| c.is_ascii_digit() || *c == b'-') {
                return false;
            }
            let Some(first) = jp_elems(vals).next() else {
                return false;
            };
            let (a, b) = (sk_int(g), sk_int(first));
            if op == b"Gt" {
                a > b
            } else {
                a < b
            }
        }
        _ => false,
    }
}

/// One NodeSelectorTerm: its matchExpressions and matchFields ANDed. A
/// term with neither matches no node (upstream).
fn sk_node_term(term: &[u8], labels: &[u8], name: &[u8]) -> bool {
    let mut any = false;
    if let Some(mx) = jp_get(term, b"matchExpressions") {
        for e in jp_elems(mx) {
            any = true;
            if !sk_node_req(e, sk_label(labels, sk_str(e, b"key"))) {
                return false;
            }
        }
    }
    if let Some(mf) = jp_get(term, b"matchFields") {
        for e in jp_elems(mf) {
            any = true;
            let got = if sk_str(e, b"key") == b"metadata.name" {
                Some(name)
            } else {
                None
            };
            if !sk_node_req(e, got) {
                return false;
            }
        }
    }
    any
}

/// A Pod's requests: cpu (millicores) and memory (bytes) — each container's
/// request, or its limit when it states none (the API's default), summed;
/// the largest init container's when that is larger.
fn sk_requests(pod: &[u8]) -> (u64, u64) {
    fn one(c: &[u8]) -> (u64, u64) {
        let res = jp_get(c, b"resources").unwrap_or(b"{}");
        let req = jp_get(res, b"requests").unwrap_or(b"{}");
        let lim = jp_get(res, b"limits").unwrap_or(b"{}");
        let pick = |k: &[u8], scale: i32| -> u64 {
            let v = jp_get(req, k).or_else(|| jp_get(lim, k));
            v.and_then(|v| pm_quantity(sk_unq(v), scale)).unwrap_or(0)
        };
        (pick(b"cpu", 3), pick(b"memory", 0))
    }
    let spec = jp_get(pod, b"spec").unwrap_or(b"{}");
    let (mut cpu, mut mem) = (0u64, 0u64);
    for c in jp_elems(jp_get(spec, b"containers").unwrap_or(b"[]")) {
        let (a, b) = one(c);
        cpu = cpu.saturating_add(a);
        mem = mem.saturating_add(b);
    }
    for c in jp_elems(jp_get(spec, b"initContainers").unwrap_or(b"[]")) {
        let (a, b) = one(c);
        cpu = cpu.max(a);
        mem = mem.max(b);
    }
    (cpu, mem)
}

fn sk_priority(pod: &[u8]) -> i64 {
    sk_at(pod, b"spec.priority").map(sk_int).unwrap_or(0)
}

/// The Pod's terms: required and preferred pod (anti-)affinity, and its
/// topology spread constraints. At most SCH_TERMS; the rest are not seen.
fn sk_terms(pod: &[u8], terms: &mut [SchTerm; SCH_TERMS]) -> usize {
    let mut n = 0usize;
    let spec = jp_get(pod, b"spec").unwrap_or(b"{}");
    let aff = jp_get(spec, b"affinity").unwrap_or(b"{}");
    let mut push = |kind: u8, weight: i64, t: &[u8]| {
        if n >= SCH_TERMS {
            return;
        }
        let key = sk_range(pod, sk_str(t, b"topologyKey"));
        let sel = jp_get(t, b"labelSelector")
            .map(|s| sk_range(pod, s))
            .unwrap_or((0, 0));
        let nss = jp_get(t, b"namespaces")
            .map(|s| sk_range(pod, s))
            .unwrap_or((0, 0));
        if let Some(slot) = terms.get_mut(n) {
            *slot = SchTerm {
                kind,
                weight,
                key,
                sel,
                nss,
            };
            n += 1;
        }
    };
    for (k, kind_req, kind_pref) in [
        (&b"podAffinity"[..], T_AFF, T_PAFF),
        (&b"podAntiAffinity"[..], T_ANTI, T_PANTI),
    ] {
        let Some(a) = jp_get(aff, k) else {
            continue;
        };
        if let Some(req) = jp_get(a, b"requiredDuringSchedulingIgnoredDuringExecution") {
            for t in jp_elems(req) {
                push(kind_req, 0, t);
            }
        }
        if let Some(pref) = jp_get(a, b"preferredDuringSchedulingIgnoredDuringExecution") {
            for w in jp_elems(pref) {
                let weight = jp_get(w, b"weight").map(sk_int).unwrap_or(0);
                if let Some(t) = jp_get(w, b"podAffinityTerm") {
                    push(kind_pref, weight, t);
                }
            }
        }
    }
    if let Some(tsc) = jp_get(spec, b"topologySpreadConstraints") {
        for c in jp_elems(tsc) {
            let hard = sk_str(c, b"whenUnsatisfiable") != b"ScheduleAnyway";
            let skew = jp_get(c, b"maxSkew").map(sk_int).unwrap_or(1).max(1);
            push(if hard { T_SPREAD } else { T_SOFT }, skew, c);
        }
    }
    n
}

/// Whether the Pod `q` (namespace `qns`, its labels) is one term `t` of the
/// pending Pod (namespace `pns`) counts: in the term's namespaces (the
/// pending Pod's own when it names none — a spread constraint always), and
/// selected by its selector.
fn sk_term_matches(pod: &[u8], t: &SchTerm, pns: &[u8], qns: &[u8], qlabels: &[u8]) -> bool {
    let nss = sk_slice(pod, t.nss);
    let in_ns = if t.kind == T_SPREAD
        || t.kind == T_SOFT
        || nss.is_empty()
        || jp_elems(nss).next().is_none()
    {
        qns == pns
    } else {
        sk_in(nss, qns)
    };
    if !in_ns {
        return false;
    }
    let sel = if t.sel.1 > t.sel.0 {
        Some(sk_slice(pod, t.sel))
    } else {
        None
    };
    sk_selects(sel, qlabels)
}

/// The namespace and name of a key tail `<ns>/<name>`.
fn sk_ns(tail: &[u8]) -> &[u8] {
    match tail.iter().position(|&b| b == b'/') {
        Some(i) => tail.get(..i).unwrap_or(&[]),
        None => &[],
    }
}

/// Append `src` to `out` at `at`; the new length (cut at the end of `out`).
fn sk_put(out: &mut [u8], at: usize, src: &[u8]) -> usize {
    let mut p = at;
    for &c in src {
        match out.get_mut(p) {
            Some(slot) => {
                *slot = c;
                p += 1;
            }
            None => break,
        }
    }
    p
}

fn sk_put_dec(out: &mut [u8], at: usize, v: u64) -> usize {
    let mut t = [0u8; 20];
    let mut i = t.len();
    let mut u = v;
    loop {
        i -= 1;
        if let Some(slot) = t.get_mut(i) {
            *slot = b'0' + (u % 10) as u8;
        }
        u /= 10;
        if u == 0 || i == 0 {
            break;
        }
    }
    sk_put(out, at, t.get(i..).unwrap_or(&[]))
}

/// The nodes' state for one pending Pod: every node-local filter applied,
/// and the Pods already placed folded in (requests, counts, term matches).
/// The node objects and the Pods are read once each. None while a read was
/// not answered.
unsafe fn sk_gather(
    cx: &SchPick,
    nodes: &mut [SchNode; SCH_NODES],
    taint_msg: &mut [u8; 128],
    taint_ml: &mut usize,
) -> Option<usize> {
    let (s, sys, tail, pod, terms) = (cx.s, cx.sys, cx.tail, cx.pod, cx.terms);
    let spec_s = &s.sched[..s.sched_len as usize];
    let mut parts = spec_s.split(|&b| b == b':');
    let node_pfx = parts.next().unwrap_or(&[]);
    let pod_pfx = parts.next().unwrap_or(&[]);
    let pspec = jp_get(pod, b"spec").unwrap_or(b"{}");
    let tols = jp_get(pspec, b"tolerations").unwrap_or(b"[]");
    let nsel = jp_get(pspec, b"nodeSelector");
    let naff = sk_at(pspec, b"affinity.nodeAffinity");
    let req_terms = naff.and_then(|a| {
        sk_at(
            a,
            b"requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms",
        )
    });
    let pref_terms =
        naff.and_then(|a| jp_get(a, b"preferredDuringSchedulingIgnoredDuringExecution"));
    let pns = sk_ns(tail);
    let total = join_count(s, sys, tail);
    let mut n = 0usize;
    let mut i = 0i64;
    while i < total && n < SCH_NODES {
        let mut k = [0u8; MAX_KEY];
        let Some(kl) = join_nth(s, sys, tail, i, &mut k) else {
            break;
        };
        i += 1;
        let key = k.get(..kl).unwrap_or(&[]);
        let name = match key.iter().rposition(|&b| b == b'/') {
            Some(x) => key.get(x + 1..).unwrap_or(&[]),
            None => key,
        };
        if name.is_empty() || name.len() > SCH_NAME {
            continue;
        }
        let mut nk = [0u8; MAX_KEY + MAX_PREFIX];
        let nkl = append_bytes(&mut nk, 0, node_pfx);
        let nkl = append_bytes(&mut nk, nkl, name);
        // Whole (onto the heap past the stack, where there is one).
        let mut v = [0u8; MAX_VALUE];
        let got = read_value(sys, nk.get(..nkl).unwrap_or(&[]), &mut v, &s.waited);
        let _held = HeapDoc::of(sys, &got);
        if s.waited.get() {
            return None;
        }
        if matches!(got, Val::Missing | Val::TooBig) {
            continue;
        }
        let node = val_bytes(&got, &v);
        let Some(slot) = nodes.get_mut(n) else {
            break;
        };
        *slot = SchNode::new();
        for (d, &c) in slot.name.iter_mut().zip(name.iter()) {
            *d = c;
        }
        slot.nl = name.len() as u8;
        let labels = sk_at(node, b"metadata.labels").unwrap_or(b"{}");
        let nspec = jp_get(node, b"spec").unwrap_or(b"{}");
        // Allocatable (capacity when allocatable is not stated).
        let alloc = sk_at(node, b"status.allocatable").or_else(|| sk_at(node, b"status.capacity"));
        if let Some(a) = alloc {
            if let Some(c) = jp_get(a, b"cpu") {
                slot.alloc_cpu = pm_quantity(sk_unq(c), 3).unwrap_or(SCH_NOLIMIT);
            }
            if let Some(m) = jp_get(a, b"memory") {
                slot.alloc_mem = pm_quantity(sk_unq(m), 0).unwrap_or(SCH_NOLIMIT);
            }
            if let Some(p) = jp_get(a, b"pods") {
                slot.alloc_pods = pm_quantity(sk_unq(p), 0).unwrap_or(SCH_NOLIMIT);
            }
        }
        for (ti, t) in terms.iter().enumerate() {
            let key = sk_slice(pod, t.key);
            if let (Some(tv), Some(dst)) = (sk_label(labels, key), slot.topo.get_mut(ti)) {
                if !key.is_empty() {
                    *dst = sk_fnv(tv);
                }
            }
        }
        // NodeUnschedulable.
        let cordoned = jp_get(nspec, b"unschedulable").map(jp_trim) == Some(&b"true"[..]);
        if cordoned
            && !sk_tolerates(
                tols,
                b"node.kubernetes.io/unschedulable",
                b"",
                b"NoSchedule",
            )
        {
            slot.fail = R_UNSCHED;
        }
        // NodeAffinity: the selector, then the required terms.
        let mut fits = match nsel {
            Some(sel) if jp_kind(sel) == b'{' && jp_members(sel).next().is_some() => {
                j_obj_subset(sel, labels)
            }
            _ => true,
        };
        if fits {
            if let Some(rt) = req_terms {
                fits = jp_elems(rt).any(|t| sk_node_term(t, labels, name));
            }
        }
        slot.eligible = fits;
        if !fits && slot.fail == 0 {
            slot.fail = R_AFFINITY;
        }
        if let Some(pt) = pref_terms {
            for w in jp_elems(pt) {
                if let Some(t) = jp_get(w, b"preference") {
                    if sk_node_term(t, labels, name) {
                        slot.pref += jp_get(w, b"weight").map(sk_int).unwrap_or(0);
                    }
                }
            }
        }
        // TaintToleration.
        for t in jp_elems(jp_get(nspec, b"taints").unwrap_or(b"[]")) {
            let (tk, tv, te) = (sk_str(t, b"key"), sk_str(t, b"value"), sk_str(t, b"effect"));
            if te == b"PreferNoSchedule" {
                if !sk_tolerates(tols, tk, tv, te) {
                    slot.soft += 1;
                }
                continue;
            }
            if (te == b"NoSchedule" || te == b"NoExecute")
                && !sk_tolerates(tols, tk, tv, te)
                && slot.fail == 0
            {
                slot.fail = R_TAINT;
                if *taint_ml == 0 {
                    let mut p = sk_put(taint_msg, 0, b"node(s) had untolerated taint {");
                    p = sk_put(taint_msg, p, tk);
                    p = sk_put(taint_msg, p, b": ");
                    p = sk_put(taint_msg, p, tv);
                    p = sk_put(taint_msg, p, b"}");
                    *taint_ml = p;
                }
            }
        }
        n += 1;
    }
    // The Pods already placed: one walk.
    let mut cur = [0u8; MAX_CURSOR];
    let mut clen = 0usize;
    loop {
        let mut page = [0u8; LIST_BUF];
        let mut next = [0u8; MAX_CURSOR];
        let (pn, nlen) = list_page(
            sys,
            pod_pfx,
            cur.get(..clen).unwrap_or(&[]),
            LIST_BUF,
            &mut page,
            &mut next,
            &s.waited,
        );
        if s.waited.get() {
            return None;
        }
        let mut rp = 0usize;
        while let Some((qkey, q)) = raw_entry(page.get(..pn).unwrap_or(&[]), rp) {
            rp = q;
            let qtail = qkey.get(pod_pfx.len()..).unwrap_or(&[]);
            if qtail == tail {
                continue;
            }
            // Whole: a bound Pod past the stack (a real template) holds
            // its node's resources all the same. Read onto the heap where
            // this build has one (store_scan); one this build cannot hold
            // is NAMED and the pass held — never placed around as if the
            // node were emptier than it is.
            let mut v = [0u8; MAX_VALUE];
            let got = read_value(sys, qkey, &mut v, &s.waited);
            let _held = HeapDoc::of(sys, &got);
            if s.waited.get() {
                return None;
            }
            match got {
                Val::Missing => continue,
                Val::TooBig => {
                    too_big(sys, &s.waited, qkey);
                    return None;
                }
                _ => {}
            }
            let qv = val_bytes(&got, &v);
            let qnode = sk_at(qv, b"spec.nodeName").map(sk_unq).unwrap_or(&[]);
            if qnode.is_empty() {
                continue;
            }
            let phase = sk_at(qv, b"status.phase").map(sk_unq).unwrap_or(&[]);
            if phase == b"Succeeded" || phase == b"Failed" {
                continue;
            }
            let Some(ni) = nodes.iter().take(n).position(|x| x.name() == qnode) else {
                continue;
            };
            let (c, m) = sk_requests(qv);
            let qlabels = sk_at(qv, b"metadata.labels").unwrap_or(b"{}");
            let qns = sk_ns(qtail);
            if let Some(nd) = nodes.get_mut(ni) {
                nd.used_cpu = nd.used_cpu.saturating_add(c);
                nd.used_mem = nd.used_mem.saturating_add(m);
                nd.pods += 1;
                for (ti, t) in terms.iter().enumerate() {
                    if sk_term_matches(pod, t, pns, qns, qlabels) {
                        if let Some(x) = nd.tmatch.get_mut(ti) {
                            *x += 1;
                        }
                    }
                }
            }
        }
        if nlen == 0 {
            break;
        }
        for (d, &c) in cur.iter_mut().zip(next.iter().take(nlen)) {
            *d = c;
        }
        clen = nlen;
    }
    Some(n)
}

/// The resources a node still lacks for (cpu, mem) more — the R_RES bits.
fn sk_short(nd: &SchNode, cpu: u64, mem: u64, freed: (u64, u64, u64)) -> u8 {
    let mut r = 0u8;
    let used_cpu = nd.used_cpu.saturating_sub(freed.0);
    let used_mem = nd.used_mem.saturating_sub(freed.1);
    let pods = nd.pods.saturating_sub(freed.2);
    if cpu > 0 && nd.alloc_cpu != SCH_NOLIMIT && used_cpu.saturating_add(cpu) > nd.alloc_cpu {
        r |= 1;
    }
    if mem > 0 && nd.alloc_mem != SCH_NOLIMIT && used_mem.saturating_add(mem) > nd.alloc_mem {
        r |= 2;
    }
    if nd.alloc_pods != SCH_NOLIMIT && pods.saturating_add(1) > nd.alloc_pods {
        r |= 4;
    }
    r
}

/// The inter-pod filters on node `ci` (InterPodAffinity, PodTopologySpread);
/// the reason it fails, or 0.
fn sk_pod_filters(pod: &[u8], tail: &[u8], nodes: &[SchNode], ci: usize, terms: &[SchTerm]) -> u8 {
    let Some(c) = nodes.get(ci) else {
        return R_AFFINITY;
    };
    let plabels = sk_at(pod, b"metadata.labels").unwrap_or(b"{}");
    let pns = sk_ns(tail);
    for (ti, t) in terms.iter().enumerate() {
        let ctopo = c.topo.get(ti).copied().unwrap_or(0);
        let in_domain = |want: u64| -> u32 {
            nodes
                .iter()
                .filter(|x| want != 0 && x.topo.get(ti).copied().unwrap_or(0) == want)
                .map(|x| x.tmatch.get(ti).copied().unwrap_or(0))
                .sum()
        };
        match t.kind {
            T_AFF => {
                let anywhere: u32 = nodes
                    .iter()
                    .map(|x| x.tmatch.get(ti).copied().unwrap_or(0))
                    .sum();
                // No Pod matches anywhere and the Pod matches its own term:
                // the first of its group may go anywhere the key is.
                let selfmatch = anywhere == 0 && sk_term_matches(pod, t, pns, pns, plabels);
                if ctopo == 0 || (in_domain(ctopo) == 0 && !selfmatch) {
                    return R_PODAFF;
                }
            }
            T_ANTI => {
                if ctopo != 0 && in_domain(ctopo) > 0 {
                    return R_ANTI;
                }
            }
            T_SPREAD => {
                if ctopo == 0 {
                    return R_SPREAD;
                }
                // The least-loaded domain among the eligible nodes.
                let mut min = u32::MAX;
                for x in nodes.iter() {
                    let d = x.topo.get(ti).copied().unwrap_or(0);
                    if x.eligible && d != 0 {
                        min = min.min(in_domain(d));
                    }
                }
                if min == u32::MAX {
                    min = 0;
                }
                let me = u32::from(sk_term_matches(pod, t, pns, pns, plabels));
                if i64::from(in_domain(ctopo) + me) - i64::from(min) > t.weight {
                    return R_SPREAD;
                }
            }
            _ => {}
        }
    }
    0
}

/// The upstream FailedScheduling message for the nodes' first failures:
/// `0/<n> nodes are available: <count> <reason>, ….`, reasons sorted.
fn sk_message(nodes: &[SchNode], taint: &[u8], out: &mut [u8]) -> usize {
    if nodes.is_empty() {
        return sk_put(out, 0, b"no nodes available to schedule pods");
    }
    let mut rs: [(&[u8], u32); 9] = [(&[], 0); 9];
    let mut nr = 0usize;
    for x in nodes {
        let mut texts: [&[u8]; 3] = [&[]; 3];
        match x.fail {
            R_UNSCHED => texts[0] = b"node(s) were unschedulable",
            R_AFFINITY => texts[0] = b"node(s) didn't match Pod's node affinity/selector",
            R_TAINT => texts[0] = taint,
            R_RES => {
                if x.res & 1 != 0 {
                    texts[0] = b"Insufficient cpu";
                }
                if x.res & 2 != 0 {
                    texts[1] = b"Insufficient memory";
                }
                if x.res & 4 != 0 {
                    texts[2] = b"Too many pods";
                }
            }
            R_PODAFF => texts[0] = b"node(s) didn't match pod affinity rules",
            R_ANTI => texts[0] = b"node(s) didn't match pod anti-affinity rules",
            R_SPREAD => texts[0] = b"node(s) didn't match pod topology spread constraints",
            _ => {}
        }
        for t in texts.iter().filter(|t| !t.is_empty()) {
            if let Some(e) = rs.iter_mut().take(nr).find(|e| e.0 == *t) {
                e.1 += 1;
            } else if let Some(e) = rs.get_mut(nr) {
                *e = (t, 1);
                nr += 1;
            }
        }
    }
    let used = rs.get_mut(..nr).unwrap_or(&mut []);
    let mut i = 1usize;
    while i < used.len() {
        let mut j = i;
        while j > 0 && used.get(j).map(|e| e.0) < used.get(j - 1).map(|e| e.0) {
            used.swap(j, j - 1);
            j -= 1;
        }
        i += 1;
    }
    let mut p = sk_put(out, 0, b"0/");
    p = sk_put_dec(out, p, nodes.len() as u64);
    p = sk_put(out, p, b" nodes are available: ");
    for (k, e) in used.iter().enumerate() {
        if k > 0 {
            p = sk_put(out, p, b", ");
        }
        p = sk_put_dec(out, p, u64::from(e.1));
        p = sk_put(out, p, b" ");
        p = sk_put(out, p, e.0);
    }
    sk_put(out, p, b".")
}

/// Normalise `raw` over [lo, hi] to 0..=100 (100 everywhere when flat).
fn sk_norm(raw: i64, lo: i64, hi: i64) -> i64 {
    if hi <= lo {
        return 100;
    }
    raw.saturating_sub(lo)
        .saturating_mul(100)
        .checked_div(hi.saturating_sub(lo))
        .unwrap_or(100)
}

/// The scheduler's pick for the pending Pod `pod` (key tail `tail`): the
/// chosen node's name into `out`, or None with `o.msg` saying why — and,
/// when `preempt`, the node and victims a preemption would take.
unsafe fn sched_pick(
    s: &State,
    sys: &SyscallTable,
    tail: &[u8],
    pod: &[u8],
    out: &mut [u8],
    o: &mut SchOut,
    preempt: bool,
) -> Option<usize> {
    let mut terms = [SchTerm {
        kind: 0,
        weight: 0,
        key: (0, 0),
        sel: (0, 0),
        nss: (0, 0),
    }; SCH_TERMS];
    // A Pod whose class is not yet resolved to a priority (priority.uproc)
    // waits, unplaced and unjudged: placed now, it would be placed at the
    // wrong priority.
    let named = sk_at(pod, b"spec.priorityClassName")
        .map(sk_unq)
        .is_some_and(|n| !n.is_empty());
    if named && sk_at(pod, b"spec.priority").is_none() {
        return None;
    }
    let nt = sk_terms(pod, &mut terms);
    let terms = terms.get(..nt).unwrap_or(&[]);
    let cx = SchPick {
        s,
        sys,
        tail,
        pod,
        terms,
    };
    let mut nodes = [SchNode::new(); SCH_NODES];
    let mut taint = [0u8; 128];
    let mut taint_l = 0usize;
    let n = sk_gather(&cx, &mut nodes, &mut taint, &mut taint_l)?;
    let nodes = nodes.get_mut(..n).unwrap_or(&mut []);
    let (cpu, mem) = sk_requests(pod);
    for i in 0..nodes.len() {
        let short = match nodes.get(i) {
            Some(x) if x.fail == 0 => sk_short(x, cpu, mem, (0, 0, 0)),
            _ => continue,
        };
        if short != 0 {
            if let Some(x) = nodes.get_mut(i) {
                x.fail = R_RES;
                x.res = short;
            }
            continue;
        }
        let f = sk_pod_filters(pod, tail, nodes, i, terms);
        if let Some(x) = nodes.get_mut(i) {
            x.fail = f;
        }
    }
    // Scores, over the feasible nodes.
    let pns = sk_ns(tail);
    let plabels = sk_at(pod, b"metadata.labels").unwrap_or(b"{}");
    let (mut pmin, mut pmax, mut smax, mut amin, mut amax) =
        (i64::MAX, i64::MIN, 0i64, i64::MAX, i64::MIN);
    let mut aff = [0i64; SCH_NODES];
    for (i, x) in nodes.iter().enumerate() {
        if x.fail != 0 {
            continue;
        }
        let mut a = 0i64;
        for (ti, t) in terms.iter().enumerate() {
            let d = x.topo.get(ti).copied().unwrap_or(0);
            if d == 0 {
                continue;
            }
            let there: u32 = nodes
                .iter()
                .filter(|y| y.topo.get(ti).copied().unwrap_or(0) == d)
                .map(|y| y.tmatch.get(ti).copied().unwrap_or(0))
                .sum();
            match t.kind {
                T_PAFF if there > 0 => a += t.weight,
                T_PANTI if there > 0 => a -= t.weight,
                T_SOFT => {
                    let me = i64::from(sk_term_matches(pod, t, pns, pns, plabels));
                    a -= (i64::from(there) + me) * 10;
                }
                _ => {}
            }
        }
        if let Some(slot) = aff.get_mut(i) {
            *slot = a;
        }
        pmin = pmin.min(x.pref);
        pmax = pmax.max(x.pref);
        smax = smax.max(x.soft);
        amin = amin.min(a);
        amax = amax.max(a);
    }
    let mut best: Option<usize> = None;
    for i in 0..nodes.len() {
        let Some(x) = nodes.get(i) else {
            continue;
        };
        if x.fail != 0 {
            continue;
        }
        let taint_s = 100 - x.soft.saturating_mul(100).checked_div(smax).unwrap_or(0);
        let aff_s = sk_norm(aff.get(i).copied().unwrap_or(0), amin, amax);
        let pref_s = if pmax <= 0 {
            0
        } else {
            sk_norm(x.pref, pmin.min(0), pmax)
        };
        let frac = |used: u64, add: u64, alloc: u64| -> i64 {
            if alloc == SCH_NOLIMIT || alloc == 0 {
                return -1;
            }
            let u = used.saturating_add(add).min(alloc);
            (alloc
                .saturating_sub(u)
                .saturating_mul(100)
                .checked_div(alloc)
                .unwrap_or(0)) as i64
        };
        let (fc, fm) = (
            frac(x.used_cpu, cpu, x.alloc_cpu),
            frac(x.used_mem, mem, x.alloc_mem),
        );
        let least = match (fc >= 0, fm >= 0) {
            (true, true) => (fc + fm).checked_div(2).unwrap_or(0),
            (true, false) => fc,
            (false, true) => fm,
            (false, false) => 100,
        };
        let score = 3 * taint_s + 2 * pref_s + 2 * aff_s + least;
        if let Some(slot) = nodes.get_mut(i) {
            slot.score = score;
        }
        let better = match best.and_then(|b| nodes.get(b)) {
            None => true,
            Some(b) => {
                score > b.score
                    || (score == b.score && x_pods(nodes, i) < b.pods)
                    || (score == b.score
                        && x_pods(nodes, i) == b.pods
                        && x_name(nodes, i) < b.name())
            }
        };
        if better {
            best = Some(i);
        }
    }
    if let Some(b) = best.and_then(|b| nodes.get(b)) {
        let nm = b.name();
        let l = nm.len().min(out.len());
        for (d, &c) in out.iter_mut().zip(nm.iter()).take(l) {
            *d = c;
        }
        return Some(l);
    }
    o.ml = sk_message(nodes, taint.get(..taint_l).unwrap_or(&[]), &mut o.msg);
    if preempt {
        sk_preempt(&cx, nodes, (cpu, mem), o);
    }
    None
}

fn x_pods(nodes: &[SchNode], i: usize) -> u64 {
    nodes.get(i).map(|x| x.pods).unwrap_or(u64::MAX)
}
fn x_name(nodes: &[SchNode], i: usize) -> &[u8] {
    nodes.get(i).map(|x| x.name()).unwrap_or(&[])
}

/// A lower-priority Pod a preemption could take off a node.
#[derive(Clone, Copy)]
struct SchVictim {
    key: [u8; 128],
    kl: u8,
    prio: i64,
    cpu: u64,
    mem: u64,
    /// Taking it would break a PodDisruptionBudget.
    pdb: bool,
    out: bool,
}

/// Whether a PodDisruptionBudget in `ns` selects the Pod (its labels) with
/// no disruption left to allow.
unsafe fn sk_pdb_blocks(
    s: &State,
    sys: &SyscallTable,
    pdb_pfx: &[u8],
    ns: &[u8],
    labels: &[u8],
) -> bool {
    if pdb_pfx.is_empty() {
        return false;
    }
    let mut pfx = [0u8; MAX_KEY];
    let mut pl = append_bytes(&mut pfx, 0, pdb_pfx);
    pl = append_bytes(&mut pfx, pl, ns);
    pl = append_bytes(&mut pfx, pl, b"/");
    let mut page = [0u8; LIST_BUF];
    let mut next = [0u8; MAX_CURSOR];
    let (pn, _) = list_page(
        sys,
        pfx.get(..pl).unwrap_or(&[]),
        &[],
        LIST_BUF,
        &mut page,
        &mut next,
        &s.waited,
    );
    let mut rp = 0usize;
    while let Some((k, q)) = raw_entry(page.get(..pn).unwrap_or(&[]), rp) {
        rp = q;
        let mut v = [0u8; MAX_VALUE];
        let got = read_value(sys, k, &mut v, &s.waited);
        let _held = HeapDoc::of(sys, &got);
        if matches!(got, Val::Missing | Val::TooBig) {
            continue;
        }
        let pdb = val_bytes(&got, &v);
        let sel = sk_at(pdb, b"spec.selector");
        let allowed = sk_at(pdb, b"status.disruptionsAllowed")
            .map(sk_int)
            .unwrap_or(0);
        if allowed <= 0 && sk_selects(sel, labels) {
            return true;
        }
    }
    false
}

/// Preemption for a Pod no node fits (see the header): the node and its
/// victims into `o` (nothing when no node would do, or the Pod's
/// preemptionPolicy is Never).
unsafe fn sk_preempt(cx: &SchPick, nodes: &[SchNode], need: (u64, u64), o: &mut SchOut) {
    let (s, sys, tail, pod, terms) = (cx.s, cx.sys, cx.tail, cx.pod, cx.terms);
    let (cpu, mem) = need;
    if sk_at(pod, b"spec.preemptionPolicy").map(sk_unq) == Some(&b"Never"[..]) {
        return;
    }
    let prio = sk_priority(pod);
    let spec_s = &s.sched[..s.sched_len as usize];
    let mut parts = spec_s.split(|&b| b == b':');
    let _ = parts.next();
    let pod_pfx = parts.next().unwrap_or(&[]);
    let pdb_pfx = parts.next().unwrap_or(&[]);
    let blank = SchVictim {
        key: [0; 128],
        kl: 0,
        prio: 0,
        cpu: 0,
        mem: 0,
        pdb: false,
        out: true,
    };
    // The best plan so far: (pdb violations, highest victim priority, victims).
    let mut best: Option<(usize, (u32, i64, u32))> = None;
    let mut best_v = [blank; SCH_VICTIMS];
    let mut best_nv = 0usize;
    for (ni, x) in nodes.iter().enumerate() {
        if x.fail != R_RES {
            continue;
        }
        // The inter-pod filters must hold too (victims aside).
        if sk_pod_filters(pod, tail, nodes, ni, terms) != 0 {
            continue;
        }
        let mut vs = [blank; SCH_VICTIMS];
        let mut nv = 0usize;
        let mut cur = [0u8; MAX_CURSOR];
        let mut clen = 0usize;
        loop {
            let mut page = [0u8; LIST_BUF];
            let mut next = [0u8; MAX_CURSOR];
            let (pn, nlen) = list_page(
                sys,
                pod_pfx,
                cur.get(..clen).unwrap_or(&[]),
                LIST_BUF,
                &mut page,
                &mut next,
                &s.waited,
            );
            if s.waited.get() {
                return;
            }
            let mut rp = 0usize;
            while let Some((qkey, q)) = raw_entry(page.get(..pn).unwrap_or(&[]), rp) {
                rp = q;
                let mut v = [0u8; MAX_VALUE];
                let got = read_value(sys, qkey, &mut v, &s.waited);
                let _held = HeapDoc::of(sys, &got);
                if s.waited.get() {
                    return;
                }
                if matches!(got, Val::Missing | Val::TooBig) {
                    continue;
                }
                let qv = val_bytes(&got, &v);
                if sk_at(qv, b"spec.nodeName").map(sk_unq) != Some(x.name()) {
                    continue;
                }
                let phase = sk_at(qv, b"status.phase").map(sk_unq).unwrap_or(&[]);
                if phase == b"Succeeded" || phase == b"Failed" {
                    continue;
                }
                let qp = sk_priority(qv);
                let qtail = qkey.get(pod_pfx.len()..).unwrap_or(&[]);
                if qp >= prio || qtail.len() > 128 {
                    continue;
                }
                let (c, m) = sk_requests(qv);
                let qlabels = sk_at(qv, b"metadata.labels").unwrap_or(b"{}");
                let pdb = sk_pdb_blocks(s, sys, pdb_pfx, sk_ns(qtail), qlabels);
                if let Some(slot) = vs.get_mut(nv) {
                    for (d, &b) in slot.key.iter_mut().zip(qtail.iter()) {
                        *d = b;
                    }
                    slot.kl = qtail.len() as u8;
                    slot.prio = qp;
                    slot.cpu = c;
                    slot.mem = m;
                    slot.pdb = pdb;
                    nv += 1;
                }
            }
            if nlen == 0 {
                break;
            }
            for (d, &c) in cur.iter_mut().zip(next.iter().take(nlen)) {
                *d = c;
            }
            clen = nlen;
        }
        let vs = vs.get_mut(..nv).unwrap_or(&mut []);
        let freed = |vs: &[SchVictim]| -> (u64, u64, u64) {
            vs.iter().filter(|v| v.out).fold((0, 0, 0), |a, v| {
                (
                    a.0.saturating_add(v.cpu),
                    a.1.saturating_add(v.mem),
                    a.2 + 1,
                )
            })
        };
        // Every lower-priority Pod off: does it fit then?
        if sk_short(x, cpu, mem, freed(vs)) != 0 {
            continue;
        }
        // Reprieve: the budget-protected first, then the rest, each pass by
        // descending priority — given back while the Pod still fits.
        let mut tried = [false; SCH_VICTIMS];
        for protected in [true, false] {
            loop {
                let mut pick: Option<usize> = None;
                for (i, v) in vs.iter().enumerate() {
                    if !v.out || v.pdb != protected || tried.get(i).copied().unwrap_or(true) {
                        continue;
                    }
                    if pick.and_then(|p| vs.get(p)).is_none_or(|p| v.prio > p.prio) {
                        pick = Some(i);
                    }
                }
                let Some(p) = pick else {
                    break;
                };
                if let Some(t) = tried.get_mut(p) {
                    *t = true;
                }
                if let Some(v) = vs.get_mut(p) {
                    v.out = false;
                }
                if sk_short(x, cpu, mem, freed(vs)) != 0 {
                    if let Some(v) = vs.get_mut(p) {
                        v.out = true;
                    }
                }
            }
        }
        let (mut viol, mut hi, mut cnt) = (0u32, i64::MIN, 0u32);
        for v in vs.iter().filter(|v| v.out) {
            cnt += 1;
            hi = hi.max(v.prio);
            viol += u32::from(v.pdb);
        }
        if cnt == 0 {
            continue;
        }
        if best.is_none_or(|(_, b)| (viol, hi, cnt) < b) {
            best = Some((ni, (viol, hi, cnt)));
            best_nv = 0;
            for v in vs.iter().filter(|v| v.out) {
                if let Some(slot) = best_v.get_mut(best_nv) {
                    *slot = *v;
                    best_nv += 1;
                }
            }
        }
    }
    let Some((ni, _)) = best else {
        return;
    };
    let Some(x) = nodes.get(ni) else {
        return;
    };
    o.nl = sk_put(&mut o.nom, 0, x.name());
    let mut p = 0usize;
    for (i, v) in best_v.iter().take(best_nv).enumerate() {
        let k = v.key.get(..v.kl as usize).unwrap_or(&[]);
        if p + k.len() + 1 > o.victims.len() {
            break;
        }
        if i > 0 {
            p = sk_put(&mut o.victims, p, b",");
        }
        p = sk_put(&mut o.victims, p, k);
    }
    o.vl = p;
}

/// NoExecute (`sched_noexec`): for a Pod bound to a node, what the node's
/// NoExecute taints do to it — 0 nothing (none, or all tolerated for ever),
/// -1 evict now (one not tolerated), or the seconds it may stay (the least
/// tolerationSeconds among the taints it tolerates for a while). None while
/// a read was not answered.
unsafe fn sched_noexec(s: &State, sys: &SyscallTable, pod: &[u8]) -> Option<i64> {
    let spec_s = &s.sched[..s.sched_len as usize];
    let node_pfx = spec_s.split(|&b| b == b':').next().unwrap_or(&[]);
    let node = sk_at(pod, b"spec.nodeName").map(sk_unq).unwrap_or(&[]);
    if node.is_empty() {
        return Some(0);
    }
    let phase = sk_at(pod, b"status.phase").map(sk_unq).unwrap_or(&[]);
    if phase == b"Succeeded" || phase == b"Failed" {
        return Some(0);
    }
    let tols = sk_at(pod, b"spec.tolerations").unwrap_or(b"[]");
    let mut nk = [0u8; MAX_KEY + MAX_PREFIX];
    let nkl = append_bytes(&mut nk, 0, node_pfx);
    let nkl = append_bytes(&mut nk, nkl, node);
    let mut v = [0u8; MAX_VALUE];
    let got = read_value(sys, nk.get(..nkl).unwrap_or(&[]), &mut v, &s.waited);
    let _held = HeapDoc::of(sys, &got);
    if s.waited.get() {
        return None;
    }
    if matches!(got, Val::Missing | Val::TooBig) {
        return Some(0);
    }
    let nv = val_bytes(&got, &v);
    let mut least: i64 = 0;
    for t in jp_elems(sk_at(nv, b"spec.taints").unwrap_or(b"[]")) {
        if sk_str(t, b"effect") != b"NoExecute" {
            continue;
        }
        match sk_noexec_secs(tols, sk_str(t, b"key"), sk_str(t, b"value")) {
            None | Some(0) => return Some(-1),
            Some(-1) => {}
            Some(secs) => {
                least = if least == 0 { secs } else { least.min(secs) };
            }
        }
    }
    Some(least)
}
