// RBAC privilege-escalation prevention — `include!`d after `json.rs` and
// `jsonpatch.rs`.
//
// Upstream's rule (pkg/registry/rbac/*/policybased): a subject allowed to
// write a Role, ClusterRole, RoleBinding or ClusterRoleBinding may still
// grant only what it HOLDS:
//
//   Role / ClusterRole   every permission its rules grant must already be
//                        the writer's — in the Role's namespace, or
//                        cluster-wide for a ClusterRole — unless the writer
//                        may `escalate` that role (verb `escalate` on
//                        `roles` / `clusterroles`, its name). A ClusterRole
//                        with an `aggregationRule` can gather anything, so
//                        setting one needs full authority (`*` on `*.*` and
//                        every non-resource URL), as upstream's does.
//   (Cluster)RoleBinding the role its `roleRef` names must be held — in the
//                        binding's namespace, or cluster-wide for a
//                        ClusterRoleBinding — unless the writer may `bind`
//                        it (verb `bind` on `roles` / `clusterroles`, the
//                        roleRef's name, in the binding's namespace). A role
//                        that does not exist cannot be checked: 404, as
//                        upstream answers it.
//   ManagementBinding    a ClusterRole and a ClusterRoleBinding in one
//                        object (management.plan derives
//                        `nanocloud-management:<b>` from its spec.rules and
//                        the binding's own two rules), so both rules apply:
//                        its rules held cluster-wide, or BOTH `escalate` and
//                        `bind` on the clusterrole `nanocloud-management:<b>`.
//
// "Held" is decided by the same grant index the request path authorizes
// with (/rbac-g/<id>/<scope>|<verb>|<group>|<res>|<name>, rbac_rules.plan):
// a permission the new role would grant is held when the probe the API
// plane would make for it finds a grant — exact, or with the scope, verb,
// group, resource or name as `*`, never widening a `~`. So a role's `*` verb
// is held only by a writer granted `*`, a rule without resourceNames only by
// one granted every name, and a nonResourceURLs rule (`~|<verb>|~|<url>|~`)
// only cluster-wide — exactly what the index would hand the role's subjects.
// The rules read are the ones the rb_ chains derive from: a rule without
// verbs, without resources and URLs, or with resources and no apiGroups
// grants nothing and is skipped; a Role's nonResourceURLs and a RoleBinding's
// reach of a ClusterRole's grant nothing either. An aggregating ClusterRole
// grants its own rules and those of every ClusterRole its selectors match
// (rbac_agg.plan mirrors its bindings onto them, /rbac-agg/C::<A>/<S>~<k>):
// a binding of it must hold all of them, to the mirror's depth.
//
// A PATCH is checked on its RESULT: the stored object patched here as the
// store will patch it (merge, strategic, JSON Patch, apply), so a patch that
// leaves `rules` alone is checked against the rules the role keeps.
//
// No allocator: the store hands objects back as its own handles
// (`CvStore::Obj`), and the reasons are written into the caller's buffer.
// Every bound past which the check cannot read a whole role refuses (fail
// closed), never grants on a partial read.

/// What kind of object the write stores.
#[derive(Clone, Copy, PartialEq, Debug)]
enum CvKind {
    Role,
    ClusterRole,
    RoleBinding,
    ClusterRoleBinding,
    MgmtBinding,
}

/// The kind a resource name writes, if it is one the check guards.
fn cv_kind(resource: &[u8]) -> Option<CvKind> {
    match resource {
        b"roles" => Some(CvKind::Role),
        b"clusterroles" => Some(CvKind::ClusterRole),
        b"rolebindings" => Some(CvKind::RoleBinding),
        b"clusterrolebindings" => Some(CvKind::ClusterRoleBinding),
        b"managementbindings" => Some(CvKind::MgmtBinding),
        _ => None,
    }
}

/// The check's answer.
#[derive(Clone, Copy, PartialEq, Debug)]
enum CvVerdict {
    /// Every permission is held, or the writer may escalate / bind.
    Allowed,
    /// A permission is not held; the reason (a Status) is written.
    Refused,
    /// The binding's roleRef names no role; the reason (a Status) is written.
    RoleMissing,
    /// The object (or the patch) is not what its kind requires.
    Malformed,
    /// Past a bound of the check (tuples, aggregated roles, a key): refused.
    TooLarge,
    /// The store did not answer a read (not yet, or refused): decide nothing.
    Undecided,
}

/// Names a listing handed back, bounded.
const CV_NAMES: usize = 32;
const CV_NAMES_BYTES: usize = 2048;
/// ClusterRoles one binding's aggregation closure may reach.
const CV_ROLES: usize = 32;
/// The mirror's depth (rbac_agg.plan MAX_DEPTH).
const CV_AGG_DEPTH: u32 = 4;
/// Permission tuples one write may ask to be checked.
const CV_TUPLES: u32 = 4096;
/// A probed grant key.
const CV_KEY: usize = 512;

struct CvNames {
    buf: [u8; CV_NAMES_BYTES],
    end: [u16; CV_NAMES],
    n: usize,
}

impl CvNames {
    fn new() -> Self {
        CvNames {
            buf: [0; CV_NAMES_BYTES],
            end: [0; CV_NAMES],
            n: 0,
        }
    }
    /// Keep `name`; false when past the bound.
    fn push(&mut self, name: &[u8]) -> bool {
        let at = if self.n == 0 {
            0
        } else {
            self.end[self.n - 1] as usize
        };
        if self.n >= CV_NAMES || at + name.len() > CV_NAMES_BYTES {
            return false;
        }
        self.buf[at..at + name.len()].copy_from_slice(name);
        self.end[self.n] = (at + name.len()) as u16;
        self.n += 1;
        true
    }
    fn get(&self, i: usize) -> &[u8] {
        let s = if i == 0 { 0 } else { self.end[i - 1] as usize };
        &self.buf[s..self.end[i] as usize]
    }
    fn has(&self, name: &[u8]) -> bool {
        (0..self.n).any(|i| self.get(i) == name)
    }
}

/// The store, as the check reads it. `None` from any call: the store did not
/// answer (not yet, or refused) — the check decides nothing.
trait CvStore {
    type Obj: AsRef<[u8]>;
    /// Whether `key` exists.
    fn has(&mut self, key: &[u8]) -> Option<bool>;
    /// The object at `key`, whole (`Some(None)`: absent).
    fn read(&mut self, key: &[u8]) -> Option<Option<Self::Obj>>;
    /// The last segments of the keys under `prefix`, into `out`;
    /// `Some(false)` when they are more than `out` holds.
    fn children(&mut self, prefix: &[u8], out: &mut CvNames) -> Option<bool>;
}

/// Where the check reads: `<grants>|<roles>|<clusterroles>|<agg>` — the
/// grant index, the two role collections and the aggregation rows.
#[derive(Clone, Copy)]
struct CvPrefixes<'a> {
    grants: &'a [u8],
    roles: &'a [u8],
    croles: &'a [u8],
    agg: &'a [u8],
}

fn cv_prefixes(spec: &[u8]) -> Option<CvPrefixes<'_>> {
    let mut it = spec.split(|&c| c == b'|');
    let p = CvPrefixes {
        grants: it.next()?,
        roles: it.next()?,
        croles: it.next()?,
        agg: it.next()?,
    };
    if it.next().is_some() || p.grants.is_empty() || p.roles.is_empty() || p.croles.is_empty() {
        return None;
    }
    Some(p)
}

/// One write to check.
struct CvReq<'a> {
    pre: CvPrefixes<'a>,
    /// The writer's identity, as the grant index keys it.
    id: &'a [u8],
    /// The object's namespace (empty: cluster-scoped).
    ns: &'a [u8],
    kind: CvKind,
    /// The object as it will be stored (`cv_final`).
    obj: &'a [u8],
    /// The name the path gives (a replace or a patch), when the object
    /// states none.
    name: &'a [u8],
}

const CV_RBAC: &[u8] = b"rbac.authorization.k8s.io";

/// Member `key` of object `obj`, refusing a document a reader could take two
/// ways: a duplicated key (which copy would the store's readers see?) or a
/// key written with an escape (`"rules"` is `rules` to a JSON decoder,
/// and not to a reader comparing bytes).
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
fn cv_get<'a>(obj: &'a [u8], key: &[u8]) -> Result<Option<&'a [u8]>, CvVerdict> {
    let mut m = jp_members(obj);
    let mut got = None;
    for (k, v) in &mut m {
        if k.iter().any(|&c| c == b'\\') {
            return Err(CvVerdict::Malformed);
        }
        if k == key {
            if got.is_some() {
                return Err(CvVerdict::Malformed);
            }
            got = Some(v);
        }
    }
    if m.bad {
        return Err(CvVerdict::Malformed);
    }
    Ok(got.filter(|v| !jp_is_null(v)))
}

/// A string member's content (as written), `None` when absent.
fn cv_str<'a>(obj: &'a [u8], key: &[u8]) -> Result<Option<&'a [u8]>, CvVerdict> {
    match cv_get(obj, key)? {
        None => Ok(None),
        Some(v) => {
            let v = jp_trim(v);
            if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
                Ok(Some(&v[1..v.len() - 1]))
            } else {
                Err(CvVerdict::Malformed)
            }
        }
    }
}

/// An array member's string elements; `None` when absent. Anything but an
/// array of strings is malformed.
struct CvList<'a> {
    elems: Option<JpElems<'a>>,
}

impl<'a> Iterator for CvList<'a> {
    type Item = Result<&'a [u8], CvVerdict>;
    fn next(&mut self) -> Option<Self::Item> {
        let e = self.elems.as_mut()?;
        match e.next() {
            Some(v) => {
                let v = jp_trim(v);
                if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
                    Some(Ok(&v[1..v.len() - 1]))
                } else {
                    Some(Err(CvVerdict::Malformed))
                }
            }
            None if e.bad => Some(Err(CvVerdict::Malformed)),
            None => None,
        }
    }
}

fn cv_list<'a>(obj: &'a [u8], key: &[u8]) -> Result<(CvList<'a>, bool), CvVerdict> {
    match cv_get(obj, key)? {
        None => Ok((CvList { elems: None }, false)),
        Some(v) => {
            if jp_kind(v) != b'[' {
                return Err(CvVerdict::Malformed);
            }
            let empty = jp_elems(v).next().is_none();
            Ok((
                CvList {
                    elems: Some(jp_elems(v)),
                },
                !empty,
            ))
        }
    }
}

/// The first permission found not held, written as upstream lists one.
struct CvMiss {
    buf: [u8; 512],
    n: usize,
}

impl CvMiss {
    fn new() -> Self {
        CvMiss {
            buf: [0; 512],
            n: 0,
        }
    }
    fn put(&mut self, s: &[u8]) {
        let k = s.len().min(self.buf.len() - self.n);
        self.buf[self.n..self.n + k].copy_from_slice(&s[..k]);
        self.n += k;
    }
    /// `{APIGroups:["g"], Resources:["r"], ResourceNames:["n"], Verbs:["v"]}`
    /// or `{NonResourceURLs:["u"], Verbs:["v"]}`, its quotes escaped for a
    /// JSON string.
    fn set(&mut self, verb: &[u8], group: &[u8], res: &[u8], name: &[u8], url: bool) {
        self.n = 0;
        if url {
            self.put(b"{NonResourceURLs:[\\\"");
            self.put(res);
        } else {
            self.put(b"{APIGroups:[\\\"");
            self.put(group);
            self.put(b"\\\"], Resources:[\\\"");
            self.put(res);
            if name != b"*" {
                self.put(b"\\\"], ResourceNames:[\\\"");
                self.put(name);
            }
        }
        self.put(b"\\\"], Verbs:[\\\"");
        self.put(verb);
        self.put(b"\\\"]}");
    }
}

/// One check's probing of the grant index.
struct CvRun<'r, S: CvStore> {
    st: &'r mut S,
    pre: CvPrefixes<'r>,
    id: &'r [u8],
    tuples: u32,
    miss: CvMiss,
}

impl<S: CvStore> CvRun<'_, S> {
    /// Whether the writer holds (scope, verb, group, res, name): any grant
    /// the API plane's probe for it would find — each part exact or `*`, a
    /// `~` never widened (apiplane.plan `authz`, store_effect EXISTS_ANY).
    fn held(
        &mut self,
        scope: &[u8],
        verb: &[u8],
        group: &[u8],
        res: &[u8],
        name: &[u8],
    ) -> Result<bool, CvVerdict> {
        self.tuples += 1;
        if self.tuples > CV_TUPLES {
            return Err(CvVerdict::TooLarge);
        }
        let parts: [&[u8]; 5] = [scope, verb, group, res, name];
        // The widest grant first (an administrator's `*` everywhere), then
        // the exact one, then the rest: the answer does not depend on the
        // order, the number of probes does.
        for step in 0u32..32 {
            let mask = if step == 0 { 31 } else { step - 1 };
            let mut skip = false;
            for (bit, &p) in parts.iter().enumerate() {
                if mask & (1 << bit) != 0 && (p == b"~" || p == b"*") {
                    // A literal is never widened; `*` widened is itself.
                    skip = true;
                }
            }
            if skip {
                continue;
            }
            let mut k = [0u8; CV_KEY];
            let mut n = 0usize;
            let mut over = false;
            let mut put = |s: &[u8], n: &mut usize| {
                if *n + s.len() > CV_KEY {
                    over = true;
                    return;
                }
                k[*n..*n + s.len()].copy_from_slice(s);
                *n += s.len();
            };
            put(self.pre.grants, &mut n);
            put(self.id, &mut n);
            put(b"/", &mut n);
            for (bit, &p) in parts.iter().enumerate() {
                if bit > 0 {
                    put(b"|", &mut n);
                }
                let v: &[u8] = if mask & (1 << bit) != 0 { b"*" } else { p };
                put(v, &mut n);
            }
            if over {
                // No grant key that long exists (the index's are bounded):
                // not held.
                continue;
            }
            match self.st.has(&k[..n]) {
                Some(true) => return Ok(true),
                Some(false) => {}
                None => return Err(CvVerdict::Undecided),
            }
        }
        Ok(false)
    }

    /// Every permission `rules` (a JSON array of PolicyRules) grants, held at
    /// `scope`; `urls`: its nonResourceURLs rules grant (cluster-wide only).
    /// `Ok(false)` at the first not held, named in `self.miss`.
    fn rules(&mut self, rules: &[u8], scope: &[u8], urls: bool) -> Result<bool, CvVerdict> {
        if jp_kind(rules) != b'[' {
            return Err(CvVerdict::Malformed);
        }
        let mut el = jp_elems(rules);
        for rule in &mut el {
            if jp_kind(rule) != b'{' {
                return Err(CvVerdict::Malformed);
            }
            if !self.rule(rule, scope, urls)? {
                return Ok(false);
            }
        }
        if el.bad {
            return Err(CvVerdict::Malformed);
        }
        Ok(true)
    }

    fn rule(&mut self, rule: &[u8], scope: &[u8], urls: bool) -> Result<bool, CvVerdict> {
        let (verbs, any_verb) = cv_list(rule, b"verbs")?;
        let res_present = cv_get(rule, b"resources")?.is_some();
        let groups_present = cv_get(rule, b"apiGroups")?.is_some();
        if !any_verb {
            return Ok(true);
        }
        if !res_present {
            // A nonResourceURLs rule: ClusterRoles only, granted only through
            // a ClusterRoleBinding (rbac_grants.plan `grant_src`).
            let (_, any_url) = cv_list(rule, b"nonResourceURLs")?;
            if !urls || !any_url {
                return Ok(true);
            }
            for v in verbs {
                let v = v?;
                let (ul, _) = cv_list(rule, b"nonResourceURLs")?;
                for u in ul {
                    let u = u?;
                    if !self.held(b"~", v, b"~", u, b"~")? {
                        self.miss.set(v, b"", u, b"", true);
                        return Ok(false);
                    }
                }
            }
            return Ok(true);
        }
        if !groups_present {
            // Resources without apiGroups grant nothing (as upstream).
            return Ok(true);
        }
        let (_, named) = cv_list(rule, b"resourceNames")?;
        for v in verbs {
            let v = v?;
            let (gl, _) = cv_list(rule, b"apiGroups")?;
            for g in gl {
                let g = g?;
                let (rl, _) = cv_list(rule, b"resources")?;
                for r in rl {
                    let r = r?;
                    if !named {
                        if !self.held(scope, v, g, r, b"*")? {
                            self.miss.set(v, g, r, b"*", false);
                            return Ok(false);
                        }
                        continue;
                    }
                    let (nl, _) = cv_list(rule, b"resourceNames")?;
                    for n in nl {
                        let n = n?;
                        if !self.held(scope, v, g, r, n)? {
                            self.miss.set(v, g, r, n, false);
                            return Ok(false);
                        }
                    }
                }
            }
        }
        Ok(true)
    }

    /// Every verb in `verbs` (comma-separated) held on (group
    /// `rbac.authorization.k8s.io`, `res`, `name`) at `scope`: the escalate
    /// / bind exceptions. A list, not an array of names: a module holds no
    /// table of pointers (it is loaded unrelocated).
    fn may(
        &mut self,
        scope: &[u8],
        verbs: &[u8],
        res: &[u8],
        name: &[u8],
    ) -> Result<bool, CvVerdict> {
        for v in verbs.split(|&c| c == b',') {
            if !self.held(scope, v, CV_RBAC, res, name)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// A JSON string's content, escaped (`"` and `\` and control bytes): an
/// identity is the token's subject, not JSON text.
fn cv_esc(o: &mut JpOut, s: &[u8]) -> Result<(), JpErr> {
    for &c in s {
        match c {
            b'"' => o.put(b"\\\"")?,
            b'\\' => o.put(b"\\\\")?,
            c if c < 0x20 => o.put(b"?")?,
            c => o.c(c)?,
        }
    }
    Ok(())
}

/// The 403 upstream answers an escalation with.
fn cv_refusal(o: &mut JpOut, res: &[u8], group: &[u8], name: &[u8], id: &[u8], what: &[u8]) {
    let w = |o: &mut JpOut| -> Result<(), JpErr> {
        o.put(b"{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"")?;
        o.put(res)?;
        o.c(b'.')?;
        o.put(group)?;
        o.put(b" \\\"")?;
        o.put(name)?;
        o.put(b"\\\" is forbidden: ")?;
        if !id.is_empty() {
            o.put(b"user \\\"")?;
            cv_esc(o, id)?;
            o.put(b"\\\" is attempting to grant RBAC permissions not currently held:\\n")?;
        }
        o.put(what)?;
        o.put(b"\",\"reason\":\"Forbidden\",\"details\":{\"name\":\"")?;
        o.put(name)?;
        o.put(b"\",\"group\":\"")?;
        o.put(group)?;
        o.put(b"\",\"kind\":\"")?;
        o.put(res)?;
        o.put(b"\"},\"code\":403}")
    };
    o.n = 0;
    if w(o).is_err() {
        o.n = 0;
        let _ = o.put(b"{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"attempting to grant RBAC permissions not currently held\",\"reason\":\"Forbidden\",\"code\":403}");
    }
}

/// The 404 for a roleRef that names no role.
fn cv_missing(o: &mut JpOut, res: &[u8], name: &[u8]) {
    let w = |o: &mut JpOut| -> Result<(), JpErr> {
        o.put(b"{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"")?;
        o.put(res)?;
        o.put(b".rbac.authorization.k8s.io \\\"")?;
        o.put(name)?;
        o.put(b"\\\" not found\",\"reason\":\"NotFound\",\"details\":{\"name\":\"")?;
        o.put(name)?;
        o.put(b"\",\"group\":\"rbac.authorization.k8s.io\",\"kind\":\"")?;
        o.put(res)?;
        o.put(b"\"},\"code\":404}")
    };
    o.n = 0;
    if w(o).is_err() {
        o.n = 0;
        let _ = o.put(b"{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"the role the binding refers to was not found\",\"reason\":\"NotFound\",\"code\":404}");
    }
}

/// Upstream's full authority: `*` on `*.*`, and every non-resource URL.
const CV_FULL: &[u8] = b"[{\"apiGroups\":[\"*\"],\"resources\":[\"*\"],\"verbs\":[\"*\"]},{\"nonResourceURLs\":[\"*\"],\"verbs\":[\"*\"]}]";

/// Is the write `r` within what its writer holds? On `Refused` and
/// `RoleMissing`, `why` holds the Status to answer.
fn cv_check<S: CvStore>(st: &mut S, r: &CvReq, why: &mut JpOut) -> CvVerdict {
    match cv_check_inner(st, r, why) {
        Ok(v) | Err(v) => v,
    }
}

fn cv_check_inner<S: CvStore>(
    st: &mut S,
    r: &CvReq,
    why: &mut JpOut,
) -> Result<CvVerdict, CvVerdict> {
    let obj = jp_trim(r.obj);
    if jp_kind(obj) != b'{' {
        return Err(CvVerdict::Malformed);
    }
    let meta = cv_get(obj, b"metadata")?;
    let mname = match meta {
        Some(m) if jp_kind(m) == b'{' => cv_str(m, b"name")?,
        Some(_) => return Err(CvVerdict::Malformed),
        None => None,
    };
    let name = mname.unwrap_or(r.name);
    let cluster = matches!(
        r.kind,
        CvKind::ClusterRole | CvKind::ClusterRoleBinding | CvKind::MgmtBinding
    );
    let scope: &[u8] = if cluster { b"*" } else { r.ns };
    let mut run = CvRun {
        st,
        pre: r.pre,
        id: r.id,
        tuples: 0,
        miss: CvMiss::new(),
    };
    match r.kind {
        CvKind::Role | CvKind::ClusterRole => {
            let res: &[u8] = if r.kind == CvKind::Role {
                b"roles"
            } else {
                b"clusterroles"
            };
            if run.may(scope, b"escalate", res, name)? {
                return Ok(CvVerdict::Allowed);
            }
            if let Some(rules) = cv_get(obj, b"rules")? {
                if !run.rules(rules, scope, cluster)? {
                    cv_refusal(why, res, CV_RBAC, name, r.id, &run.miss.buf[..run.miss.n]);
                    return Ok(CvVerdict::Refused);
                }
            }
            if r.kind == CvKind::ClusterRole
                && cv_aggregates(obj)?
                && !run.rules(CV_FULL, b"*", true)?
            {
                cv_refusal(
                    why,
                    res,
                    CV_RBAC,
                    name,
                    b"",
                    b"must have cluster-admin privileges to use the aggregationRule",
                );
                return Ok(CvVerdict::Refused);
            }
            Ok(CvVerdict::Allowed)
        }
        CvKind::RoleBinding | CvKind::ClusterRoleBinding => {
            let res: &[u8] = if r.kind == CvKind::RoleBinding {
                b"rolebindings"
            } else {
                b"clusterrolebindings"
            };
            let Some(rr) = cv_get(obj, b"roleRef")? else {
                return Err(CvVerdict::Malformed);
            };
            if jp_kind(rr) != b'{' {
                return Err(CvVerdict::Malformed);
            }
            let rkind = cv_str(rr, b"kind")?.unwrap_or(b"");
            let rname = cv_str(rr, b"name")?.unwrap_or(b"");
            let of_role = rkind == b"Role" && r.kind == CvKind::RoleBinding;
            if !of_role && rkind != b"ClusterRole" {
                // A ClusterRoleBinding of a Role, or a kind that is no role:
                // the index derives no grant from it (rbac_grants.plan).
                return Ok(CvVerdict::Allowed);
            }
            let rres: &[u8] = if of_role { b"roles" } else { b"clusterroles" };
            let rkey_pre = if of_role { r.pre.roles } else { r.pre.croles };
            if run.may(scope, b"bind", rres, rname)? {
                return Ok(CvVerdict::Allowed);
            }
            if rres == b"roles" {
                let key = cv_key2(rkey_pre, r.ns, b"/", rname)?;
                let Some(role) = run.st.read(&key.0[..key.1]).ok_or(CvVerdict::Undecided)? else {
                    cv_missing(why, rres, rname);
                    return Ok(CvVerdict::RoleMissing);
                };
                let role = role.as_ref();
                if let Some(rules) = cv_get(jp_trim(role), b"rules")? {
                    if !run.rules(rules, scope, false)? {
                        cv_refusal(why, res, CV_RBAC, name, r.id, &run.miss.buf[..run.miss.n]);
                        return Ok(CvVerdict::Refused);
                    }
                }
                return Ok(CvVerdict::Allowed);
            }
            // A ClusterRole and every role it aggregates, to the mirror's
            // depth.
            let mut seen = CvNames::new();
            let mut depth = [0u32; CV_ROLES];
            if !seen.push(rname) {
                return Err(CvVerdict::TooLarge);
            }
            let mut i = 0usize;
            while i < seen.n {
                let mut cr = [0u8; CV_KEY];
                let cn = cv_cat(&mut cr, &[r.pre.croles, seen.get(i)])?;
                let got = run.st.read(&cr[..cn]).ok_or(CvVerdict::Undecided)?;
                match got {
                    None if i == 0 => {
                        cv_missing(why, rres, rname);
                        return Ok(CvVerdict::RoleMissing);
                    }
                    // An aggregated role gone (its row not yet pruned): it
                    // grants nothing.
                    None => {}
                    Some(role) => {
                        let role = role.as_ref();
                        if let Some(rules) = cv_get(jp_trim(role), b"rules")? {
                            if !run.rules(rules, scope, cluster)? {
                                cv_refusal(
                                    why,
                                    res,
                                    CV_RBAC,
                                    name,
                                    r.id,
                                    &run.miss.buf[..run.miss.n],
                                );
                                return Ok(CvVerdict::Refused);
                            }
                        }
                    }
                }
                if depth[i] < CV_AGG_DEPTH && !r.pre.agg.is_empty() {
                    let mut ak = [0u8; CV_KEY];
                    let an = cv_cat(&mut ak, &[r.pre.agg, b"C::", seen.get(i), b"/"])?;
                    let mut kids = CvNames::new();
                    if !run
                        .st
                        .children(&ak[..an], &mut kids)
                        .ok_or(CvVerdict::Undecided)?
                    {
                        return Err(CvVerdict::TooLarge);
                    }
                    for k in 0..kids.n {
                        let kid = kids.get(k);
                        // `<S>~<k>`: one selector of A matched S.
                        let s = match kid.iter().rposition(|&c| c == b'~') {
                            Some(t) => &kid[..t],
                            None => kid,
                        };
                        if seen.has(s) {
                            continue;
                        }
                        if !seen.push(s) {
                            return Err(CvVerdict::TooLarge);
                        }
                        depth[seen.n - 1] = depth[i] + 1;
                    }
                }
                i += 1;
            }
            Ok(CvVerdict::Allowed)
        }
        CvKind::MgmtBinding => {
            let mut crn = [0u8; CV_KEY];
            let cn = cv_cat(&mut crn, &[b"nanocloud-management:", name])?;
            if run.may(b"*", b"escalate,bind", b"clusterroles", &crn[..cn])? {
                return Ok(CvVerdict::Allowed);
            }
            const RES: &[u8] = b"managementbindings";
            const GROUP: &[u8] = b"nanocloud.io";
            // The binding's own rules (management.plan `d_cr`).
            for own in 0..2 {
                let (v, r2): (&[u8], &[u8]) = if own == 0 {
                    (b"get", RES)
                } else {
                    (b"create", b"managementbindings/epoch")
                };
                if !run.held(b"*", v, GROUP, r2, name)? {
                    run.miss.set(v, GROUP, r2, name, false);
                    cv_refusal(why, RES, GROUP, name, r.id, &run.miss.buf[..run.miss.n]);
                    return Ok(CvVerdict::Refused);
                }
            }
            let spec = cv_get(obj, b"spec")?;
            if let Some(spec) = spec {
                if jp_kind(spec) != b'{' {
                    return Err(CvVerdict::Malformed);
                }
                if let Some(rules) = cv_get(spec, b"rules")? {
                    if !run.rules(rules, b"*", true)? {
                        cv_refusal(why, RES, GROUP, name, r.id, &run.miss.buf[..run.miss.n]);
                        return Ok(CvVerdict::Refused);
                    }
                }
            }
            Ok(CvVerdict::Allowed)
        }
    }
}

/// Whether a ClusterRole sets an aggregation rule (one selector at least).
fn cv_aggregates(obj: &[u8]) -> Result<bool, CvVerdict> {
    let Some(a) = cv_get(obj, b"aggregationRule")? else {
        return Ok(false);
    };
    if jp_kind(a) != b'{' {
        return Err(CvVerdict::Malformed);
    }
    match cv_get(a, b"clusterRoleSelectors")? {
        None => Ok(false),
        Some(s) if jp_kind(s) == b'[' => Ok(jp_elems(s).next().is_some()),
        Some(_) => Err(CvVerdict::Malformed),
    }
}

fn cv_cat(out: &mut [u8; CV_KEY], parts: &[&[u8]]) -> Result<usize, CvVerdict> {
    let mut n = 0usize;
    for p in parts {
        if n + p.len() > CV_KEY {
            return Err(CvVerdict::TooLarge);
        }
        out[n..n + p.len()].copy_from_slice(p);
        n += p.len();
    }
    Ok(n)
}

fn cv_key2(pre: &[u8], a: &[u8], sep: &[u8], b: &[u8]) -> Result<([u8; CV_KEY], usize), CvVerdict> {
    let mut k = [0u8; CV_KEY];
    let n = cv_cat(&mut k, &[pre, a, sep, b])?;
    Ok((k, n))
}

/// The strategic-merge directives the store's dialect states (the apiplane
/// graph's `schema`, `smp.*`): a role's `rules` and a binding's `subjects`
/// carry no merge key, so lists here are replaced whole either way. Cut
/// from one literal at run time (`black_box`): a constant `JpSmp` would be
/// a table of pointers, which a module loaded unrelocated cannot hold.
const CV_SMP_NAMES: &[u8] = b"$patch$retainKeys$deleteFromPrimitiveList/";

fn cv_smp() -> JpSmp<'static> {
    let n: &'static [u8] = core::hint::black_box(CV_SMP_NAMES);
    JpSmp {
        keys: &n[..0],
        patch: &n[..6],
        retain: &n[6..17],
        delete: &n[17..],
        prefix: &n[..1],
    }
}

/// The object a write stores, as the check sees it: the body (a create or a
/// replace, `how` 0), or the stored object (`{}` when there is none) patched
/// by it — 13 merge, 15 strategic, 16 apply (an absent object is the body),
/// 14 JSON Patch. `work` holds the result; a JSON Patch needs three times
/// the document. Returns the result's bytes.
fn cv_final<'o>(
    how: i64,
    body: &'o [u8],
    stored: Option<&[u8]>,
    work: &'o mut [u8],
) -> Result<&'o [u8], JpErr> {
    match how {
        0 => Ok(body),
        16 if stored.is_none() => Ok(body),
        13 | 15 | 16 => {
            let smp = cv_smp();
            let mut o = JpOut::new(work);
            jp_merge(
                stored,
                body,
                &mut o,
                if how == 13 { None } else { Some(&smp) },
                b"",
                0,
            )?;
            let n = o.n;
            Ok(&work[..n])
        }
        14 => {
            let (at, n) = jp_json(stored.unwrap_or(&b"{}"[..]), body, work)?;
            Ok(&work[at..at + n])
        }
        _ => Err(JpErr::Malformed),
    }
}
