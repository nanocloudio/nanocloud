// Field ownership, as server-side apply keeps it — `include!`d after json.rs,
// jsonpatch.rs and docschema.rs by the API's writer. The vocabulary (where the
// entries live, their member names, the path marks, the refusal's wording) is
// the schema's (`FsDialect`); Kubernetes states it as managedFields/fieldsV1.
//
// Every write through the API is made by a MANAGER (the request's
// `fieldManager`, else its User-Agent's product). The object records, per
// manager and operation, the fields that manager set (`fieldsV1`):
//
//   Update  a PUT, PATCH, create or subresource write owns the fields it
//           CHANGED, taking them from whoever owned them before.
//   Apply   a server-side apply owns every field its configuration names.
//           A field another manager owns with a different value is a
//           CONFLICT (409) unless the apply forces it (`force=true`), which
//           takes it. A field the manager applied before and no longer names
//           is REMOVED from the object, unless another manager owns it too.
//
// A path is a sequence of fieldsV1 elements, kept as they are written inside
// the fieldsV1 JSON (escaped): `f:<member>`, `k:{"<key>":<value>}` for an
// element of a list merged on a key (the strategic-merge schema's), `v:<value>`
// for an element of a set list, and `.` for a keyed element itself.
//
// Bounded: an object past `FS_DOC_MAX`, more than `FS_ENTRIES` managers or a
// field set past `FS_ARENA` is not tracked — its managedFields are carried as
// stored, and an apply is the merge it was before ownership existed. Upstream
// has no such bound; one here keeps a write's cost linear-ish in the object.
// A keyed element names its single schema key (upstream names a container
// port by `containerPort` AND `protocol`).

/// Objects larger than this are not tracked.
const FS_DOC_MAX: usize = 512 * 1024;
/// Managers (entries) an object records.
const FS_ENTRIES: usize = 16;
/// The field-set arena: every entry's paths.
const FS_ARENA: usize = 48 * 1024;
/// One path.
const FS_PATH: usize = 1024;
/// Nesting a walk follows.
const FS_DEPTH: u32 = 24;
/// Conflicts one refusal lists.
const FS_CONFLICTS: usize = 8;

/// Between a path's elements (a JSON string never holds a raw control byte).
const FS_SEP: u8 = 0x1f;
/// Arena owners that are not an entry.
const FS_DEAD: u8 = 0xff;
const FS_NEW: u8 = 0xfe;

#[derive(Clone, Copy)]
struct FsEntry<'a> {
    mgr: &'a [u8],
    op: &'a [u8],
    api: &'a [u8],
    time: &'a [u8],
    sub: &'a [u8],
}

const FS_NO_ENTRY: FsEntry<'static> = FsEntry {
    mgr: &[],
    op: &[],
    api: &[],
    time: &[],
    sub: &[],
};

/// Who writes, and how.
struct FsWriter<'a> {
    mgr: &'a [u8],
    sub: &'a [u8],
    force: bool,
    /// The write's time, RFC 3339.
    now: &'a [u8],
    /// The strategic-merge schema (`patch_keys`): which lists are keyed.
    keys: &'a [u8],
    /// The document schema: the ownership dialect and merge directives.
    sch: &'a [u8],
}

/// The ownership vocabulary, from the schema:
///
///   owners=<path>                  where the entries live
///   owners.keys=<manager>,<operation>,<apiVersion>,<time>,<subresource>,<fields>
///                                  an entry's member names
///   owners.ops=<apply>,<update>    the two operations' names
///   owners.api=<path>              the object's version, recorded per entry
///   owners.entry=<tpl>             an entry: {{manager}} {{op}} {{api}}
///                                  {{time}} {{sub}} {{fields}}
///   owners.marks=<f>,<k>,<v>,<dot> a path element's marks: a member, a keyed
///                                  element, a set element, the element itself
///   unowned=<path>,…               fields no one owns (identity, bookkeeping)
///   conflict=<tpl>                 an apply refused: {{count}} {{many}}
///                                  {{list}} {{causes}}
///   conflict.item=<tpl>            one conflict in {{list}}: {{manager}}
///                                  {{api}} {{field}}
///   conflict.cause=<tpl>           one conflict in {{causes}}: the same
///
/// Without `owners`, `owners.keys`, `owners.ops` and `owners.marks` nothing is
/// tracked.
#[derive(Clone, Copy)]
struct FsDialect<'a> {
    owners: &'a [u8],
    keys: [&'a [u8]; 6],
    apply: &'a [u8],
    update: &'a [u8],
    api: &'a [u8],
    entry: &'a [u8],
    f: &'a [u8],
    k: &'a [u8],
    v: &'a [u8],
    dot: &'a [u8],
    unowned: &'a [u8],
    conflict: &'a [u8],
    item: &'a [u8],
    cause: &'a [u8],
}

/// The `n` comma-separated parts of `list` into `out`; false unless exactly `n`.
fn fs_parts<'a>(list: &'a [u8], out: &mut [&'a [u8]]) -> bool {
    let mut i = 0usize;
    for p in list.split(|&b| b == b',') {
        if i == out.len() || p.is_empty() {
            return false;
        }
        out[i] = p;
        i += 1;
    }
    i == out.len()
}

/// The schema's ownership dialect, if it states one.
fn fs_dialect(sch: &[u8]) -> Option<FsDialect<'_>> {
    let owners = sch_get(sch, b"owners");
    let mut keys: [&[u8]; 6] = [&[]; 6];
    let mut ops: [&[u8]; 2] = [&[]; 2];
    let mut marks: [&[u8]; 4] = [&[]; 4];
    if owners.is_empty()
        || !fs_parts(sch_get(sch, b"owners.keys"), &mut keys)
        || !fs_parts(sch_get(sch, b"owners.ops"), &mut ops)
        || !fs_parts(sch_get(sch, b"owners.marks"), &mut marks)
    {
        return None;
    }
    Some(FsDialect {
        owners,
        keys,
        apply: ops[0],
        update: ops[1],
        api: sch_get(sch, b"owners.api"),
        entry: sch_get(sch, b"owners.entry"),
        f: marks[0],
        k: marks[1],
        v: marks[2],
        dot: marks[3],
        unowned: sch_get(sch, b"unowned"),
        conflict: sch_get(sch, b"conflict"),
        item: sch_get(sch, b"conflict.item"),
        cause: sch_get(sch, b"conflict.cause"),
    })
}

/// What a tracked write comes to.
enum FsOutcome {
    /// The object to store: `work[off..off + len]`.
    Doc(usize, usize),
    /// An apply refused: the Status to answer, `work[off..off + len]`.
    Conflict(usize, usize),
    /// Not tracked (too large, too many managers): store the object as given.
    Untracked,
}

/// Every entry's paths: records `[owner u8][len u16 le][path]`.
struct FsSet<'a, 'b> {
    d: FsDialect<'a>,
    e: [FsEntry<'a>; FS_ENTRIES],
    n: usize,
    a: &'b mut [u8],
    an: usize,
}

/// The path being walked.
struct FsPath {
    b: [u8; FS_PATH],
    n: usize,
}

impl FsPath {
    /// Append an element made of `parts`; the length to restore.
    fn push(&mut self, parts: &[&[u8]]) -> Result<usize, JpErr> {
        let mark = self.n;
        if self.n > 0 {
            self.byte(FS_SEP)?;
        }
        for p in parts {
            let end = self.n + p.len();
            let dst = self.b.get_mut(self.n..end).ok_or(JpErr::Full)?;
            dst.copy_from_slice(p);
            self.n = end;
        }
        Ok(mark)
    }
    /// Append `v` JSON-escaped (as it reads inside a JSON string).
    fn escaped(&mut self, v: &[u8]) -> Result<(), JpErr> {
        for &c in v {
            if c == b'"' || c == b'\\' {
                self.byte(b'\\')?;
            }
            self.byte(c)?;
        }
        Ok(())
    }
    fn byte(&mut self, c: u8) -> Result<(), JpErr> {
        let d = self.b.get_mut(self.n).ok_or(JpErr::Full)?;
        *d = c;
        self.n += 1;
        Ok(())
    }
    fn pop(&mut self, mark: usize) {
        self.n = mark;
    }
    fn get(&self) -> &[u8] {
        &self.b[..self.n]
    }
}

/// A path's elements.
fn fs_elems(p: &[u8]) -> core::slice::Split<'_, u8, impl FnMut(&u8) -> bool> {
    p.split(|&c| c == FS_SEP)
}

/// Whether `raw` (JSON text) is `esc` once escaped for a JSON string.
fn fs_esc_eq(raw: &[u8], esc: &[u8]) -> bool {
    let mut j = 0usize;
    for &c in raw {
        if c == b'"' || c == b'\\' {
            if esc.get(j) != Some(&b'\\') {
                return false;
            }
            j += 1;
        }
        if esc.get(j) != Some(&c) {
            return false;
        }
        j += 1;
    }
    j == esc.len()
}

/// A keyed element's (`<k>{\"<key>\":<value>}`) key and (escaped) value.
fn fs_keyed<'e>(d: &FsDialect, e: &'e [u8]) -> Option<(&'e [u8], &'e [u8])> {
    let body = e
        .strip_prefix(d.k)?
        .strip_prefix(b"{\\\"")?
        .strip_suffix(b"}")?;
    let q = body.windows(3).position(|w| w == b"\\\":")?;
    Some((&body[..q], &body[q + 3..]))
}

/// The value at path `p` of `doc`; the element mark is the element it follows.
fn fs_resolve<'d>(d: &FsDialect, doc: &'d [u8], p: &[u8]) -> Option<&'d [u8]> {
    let mut v = jp_trim(doc);
    for e in fs_elems(p) {
        if e == d.dot {
            continue;
        }
        v = if let Some((k, val)) = fs_keyed(d, e) {
            jp_elems(v).find(|el| jp_get(el, k).is_some_and(|x| fs_esc_eq(jp_trim(x), val)))?
        } else if let Some(k) = e.strip_prefix(d.f) {
            jp_trim(jp_get(v, k)?)
        } else if let Some(val) = e.strip_prefix(d.v) {
            jp_elems(v).find(|el| fs_esc_eq(jp_trim(el), val))?
        } else {
            return None;
        };
    }
    Some(v)
}

/// Whether the walked path `cur` is one of the schema's `unowned` paths.
fn fs_unowned(d: &FsDialect, cur: &[u8]) -> bool {
    d.unowned.split(|&b| b == b',').any(|u| {
        let mut segs: [&[u8]; SCH_SEGS] = [&[]; SCH_SEGS];
        let n = sch_segs(jp_trim(u), &mut segs);
        n > 0
            && fs_elems(cur).count() == n
            && fs_elems(cur)
                .zip(segs[..n].iter())
                .all(|(e, seg)| e.strip_prefix(d.f) == Some(*seg))
    })
}

impl<'a, 'b> FsSet<'a, 'b> {
    fn new(a: &'b mut [u8], d: FsDialect<'a>) -> Self {
        FsSet {
            d,
            e: [FS_NO_ENTRY; FS_ENTRIES],
            n: 0,
            a,
            an: 0,
        }
    }

    fn add(&mut self, owner: u8, p: &[u8]) -> Result<(), JpErr> {
        let end = self.an + 3 + p.len();
        if end > self.a.len() || p.len() > u16::MAX as usize {
            return Err(JpErr::Full);
        }
        self.a[self.an] = owner;
        self.a[self.an + 1..self.an + 3].copy_from_slice(&(p.len() as u16).to_le_bytes());
        self.a[self.an + 3..end].copy_from_slice(p);
        self.an = end;
        Ok(())
    }

    /// The record after offset `i`: (owner, path range, next offset).
    fn rec(&self, i: usize) -> Option<(u8, usize, usize, usize)> {
        if i + 3 > self.an {
            return None;
        }
        let l = u16::from_le_bytes([self.a[i + 1], self.a[i + 2]]) as usize;
        Some((self.a[i], i + 3, i + 3 + l, i + 3 + l))
    }

    /// Whether an owner `pick` accepts holds `p`, or (`under`) a path below it.
    fn holds(&self, pick: impl Fn(u8) -> bool, p: &[u8], under: bool) -> bool {
        let mut i = 0usize;
        while let Some((o, s, e, next)) = self.rec(i) {
            if o != FS_DEAD && pick(o) {
                let q = &self.a[s..e];
                if q == p
                    || (under && q.len() > p.len() && q.starts_with(p) && q[p.len()] == FS_SEP)
                {
                    return true;
                }
            }
            i = next;
        }
        false
    }

    /// Every field `obj` sets, as `owner`'s.
    #[allow(
        clippy::too_many_arguments,
        reason = "one walk step: where it is, what it walks, and for whom"
    )]
    fn walk(
        &mut self,
        obj: &[u8],
        s: &JpSmp,
        parent: &[u8],
        cur: &mut FsPath,
        owner: u8,
        depth: u32,
    ) -> Result<(), JpErr> {
        if depth > FS_DEPTH {
            return Err(JpErr::Deep);
        }
        let mut members = jp_members(obj);
        for (k, v) in &mut members {
            let mark = cur.push(&[self.d.f, k])?;
            if fs_unowned(&self.d, cur.get()) {
                cur.pop(mark);
                continue;
            }
            let v = jp_trim(v);
            match jp_kind(v) {
                b'{' if jp_members(v).next().is_some() => {
                    self.walk(v, s, parent, cur, owner, depth + 1)?
                }
                b'[' if jp_elems(v).next().is_some() => {
                    self.list(v, k, s, parent, cur, owner, depth)?
                }
                _ => self.add(owner, cur.get())?,
            }
            cur.pop(mark);
        }
        if members.bad {
            return Err(JpErr::Malformed);
        }
        Ok(())
    }

    /// A non-empty list `v`, member `name` of an element of `parent`.
    #[allow(
        clippy::too_many_arguments,
        reason = "one walk step: where it is, what it walks, and for whom"
    )]
    fn list(
        &mut self,
        v: &[u8],
        name: &[u8],
        s: &JpSmp,
        parent: &[u8],
        cur: &mut FsPath,
        owner: u8,
        depth: u32,
    ) -> Result<(), JpErr> {
        match jp_smp_key(s, name, parent) {
            JpList::Keyed(key) => {
                // Every element must carry its key; one without makes the
                // list whole-owned (atomic), as nothing could name it.
                if jp_elems(v).any(|el| jp_kind(el) != b'{' || jp_get(el, key).is_none()) {
                    return self.add(owner, cur.get());
                }
                for el in jp_elems(v) {
                    let kv = jp_trim(jp_get(el, key).unwrap_or(b"null"));
                    let mark = cur.push(&[self.d.k, b"{\\\"", key, b"\\\":"])?;
                    cur.escaped(kv)?;
                    cur.byte(b'}')?;
                    let dot = cur.push(&[self.d.dot])?;
                    self.add(owner, cur.get())?;
                    cur.pop(dot);
                    self.walk(el, s, name, cur, owner, depth + 1)?;
                    cur.pop(mark);
                }
                Ok(())
            }
            JpList::Set => {
                for el in jp_elems(v) {
                    let mark = cur.push(&[self.d.v])?;
                    cur.escaped(jp_trim(el))?;
                    self.add(owner, cur.get())?;
                    cur.pop(mark);
                }
                Ok(())
            }
            JpList::Atomic => self.add(owner, cur.get()),
        }
    }

    /// A stored `fieldsV1` document, as `owner`'s.
    fn parse(&mut self, v: &[u8], cur: &mut FsPath, owner: u8, depth: u32) -> Result<(), JpErr> {
        if depth > FS_DEPTH {
            return Err(JpErr::Deep);
        }
        let mut members = jp_members(v);
        for (k, sub) in &mut members {
            let mark = cur.push(&[k])?;
            if jp_members(sub).next().is_none() {
                self.add(owner, cur.get())?;
            } else {
                self.parse(sub, cur, owner, depth + 1)?;
            }
            cur.pop(mark);
        }
        if members.bad {
            return Err(JpErr::Malformed);
        }
        Ok(())
    }

    /// The stored entries of `obj`'s ownership list.
    fn load(&mut self, obj: &'a [u8], cur: &mut FsPath) -> Result<(), JpErr> {
        let Some(mf) = sch_at(obj, self.d.owners) else {
            return Ok(());
        };
        let k = self.d.keys;
        for e in jp_elems(mf) {
            if self.n == FS_ENTRIES {
                return Err(JpErr::Full);
            }
            let s = |k: &[u8]| jp_str(e, k).unwrap_or(&[]);
            self.e[self.n] = FsEntry {
                mgr: s(k[0]),
                op: s(k[1]),
                api: s(k[2]),
                time: s(k[3]),
                sub: s(k[4]),
            };
            if let Some(f) = jp_get(e, k[5]) {
                self.parse(f, cur, self.n as u8, 0)?;
            }
            self.n += 1;
        }
        Ok(())
    }

    /// The entry of (`mgr`, `op`, `sub`), made when absent.
    fn entry(&mut self, mgr: &'a [u8], op: &'a [u8], sub: &'a [u8]) -> Result<u8, JpErr> {
        for i in 0..self.n {
            let e = &self.e[i];
            if e.mgr == mgr && e.op == op && e.sub == sub {
                return Ok(i as u8);
            }
        }
        if self.n == FS_ENTRIES {
            return Err(JpErr::Full);
        }
        self.e[self.n] = FsEntry {
            mgr,
            op,
            api: &[],
            time: &[],
            sub,
        };
        self.n += 1;
        Ok((self.n - 1) as u8)
    }

    /// Every record of `from` becomes `to`'s (one copy of each path).
    fn relabel(&mut self, from: u8, to: u8) {
        let mut i = 0usize;
        while let Some((o, s, e, next)) = self.rec(i) {
            if o == from {
                let dup = {
                    let p = &self.a[s..e];
                    self.holds(|x| x == to, p, false)
                };
                self.a[i] = if dup { FS_DEAD } else { to };
            }
            i = next;
        }
    }

    /// Drop every path that no longer names anything in `doc`.
    fn prune(&mut self, doc: &[u8]) {
        let mut i = 0usize;
        while let Some((o, s, e, next)) = self.rec(i) {
            if o != FS_DEAD && fs_resolve(&self.d, doc, &self.a[s..e]).is_none() {
                self.a[i] = FS_DEAD;
            }
            i = next;
        }
    }

    fn has_paths(&self, owner: u8) -> bool {
        let mut i = 0usize;
        while let Some((o, _, _, next)) = self.rec(i) {
            if o == owner {
                return true;
            }
            i = next;
        }
        false
    }

    /// `owner`'s fieldsV1 below `prefix`.
    fn render(
        &self,
        owner: u8,
        prefix: &mut FsPath,
        o: &mut JpOut,
        depth: u32,
    ) -> Result<(), JpErr> {
        if depth > FS_DEPTH {
            return Err(JpErr::Deep);
        }
        o.c(b'{')?;
        let mut first = true;
        let mut i = 0usize;
        while let Some((ow, s, e, next)) = self.rec(i) {
            let start = i;
            i = next;
            let Some(el) = self.next_elem(ow, s, e, owner, prefix.get()) else {
                continue;
            };
            // Once per element: skip it if an earlier record gave it already.
            let mut j = 0usize;
            let mut seen = false;
            while j < start {
                let Some((ow2, s2, e2, n2)) = self.rec(j) else {
                    break;
                };
                if self.next_elem(ow2, s2, e2, owner, prefix.get()) == Some(el) {
                    seen = true;
                    break;
                }
                j = n2;
            }
            if seen {
                continue;
            }
            o.key(&mut first, el)?;
            let mark = prefix.push(&[el])?;
            if !self.only_exact(owner, prefix.get()) {
                self.render(owner, prefix, o, depth + 1)?;
            } else {
                o.put(b"{}")?;
            }
            prefix.pop(mark);
        }
        o.c(b'}')
    }

    /// Whether `owner` holds nothing strictly below `p`.
    fn only_exact(&self, owner: u8, p: &[u8]) -> bool {
        let mut i = 0usize;
        while let Some((o, s, e, next)) = self.rec(i) {
            let q = &self.a[s..e];
            if o == owner && q.len() > p.len() && q.starts_with(p) && q[p.len()] == FS_SEP {
                return false;
            }
            i = next;
        }
        true
    }

    /// The element after `prefix` in record `[s, e)` when it is `owner`'s.
    fn next_elem(&self, ow: u8, s: usize, e: usize, owner: u8, prefix: &[u8]) -> Option<&[u8]> {
        if ow != owner {
            return None;
        }
        let q = &self.a[s..e];
        let rest = if prefix.is_empty() {
            q
        } else if q.len() > prefix.len() && q.starts_with(prefix) && q[prefix.len()] == FS_SEP {
            &q[prefix.len() + 1..]
        } else {
            return None;
        };
        fs_elems(rest).next()
    }

    /// The ownership list, as a merge patch over the object.
    fn render_patch(&self, o: &mut JpOut) -> Result<(), JpErr> {
        let opened = sch_open(self.d.owners, o)?;
        o.c(b'[')?;
        let (pre, post) = sch_split(self.d.entry, b"{{fields}}").ok_or(JpErr::Malformed)?;
        let mut first = true;
        let mut prefix = FsPath {
            b: [0u8; FS_PATH],
            n: 0,
        };
        for i in 0..self.n {
            let e = &self.e[i];
            if !self.has_paths(i as u8) {
                continue;
            }
            o.elem(&mut first)?;
            let vars = [
                (&b"manager"[..], e.mgr),
                (&b"op"[..], e.op),
                (&b"api"[..], e.api),
                (&b"time"[..], e.time),
                (&b"sub"[..], e.sub),
            ];
            sch_fill_out(pre, &vars, o)?;
            prefix.n = 0;
            self.render(i as u8, &mut prefix, o, 0)?;
            sch_fill_out(post, &vars, o)?;
        }
        o.c(b']')?;
        sch_close(opened, o)
    }
}

/// A path as a conflict prints it: `.spec.containers[name="c"].image`.
fn fs_path_text(d: &FsDialect, p: &[u8], o: &mut JpOut) -> Result<(), JpErr> {
    for e in fs_elems(p) {
        if let Some((k, v)) = fs_keyed(d, e) {
            o.c(b'[')?;
            o.put(k)?;
            o.c(b'=')?;
            o.put(v)?;
            o.c(b']')?;
        } else if let Some(k) = e.strip_prefix(d.f) {
            o.c(b'.')?;
            o.put(k)?;
        } else if let Some(v) = e.strip_prefix(d.v) {
            o.put(b"[=")?;
            o.put(v)?;
            o.c(b']')?;
        }
    }
    Ok(())
}

/// `doc` without the field at path `p` (ending in the element mark: the
/// element itself).
fn fs_without(d: &FsDialect, v: &[u8], p: &[u8], o: &mut JpOut, depth: u32) -> Result<(), JpErr> {
    if depth > FS_DEPTH {
        return Err(JpErr::Deep);
    }
    let v = jp_trim(v);
    let (e, rest) = match p.iter().position(|&c| c == FS_SEP) {
        Some(i) => (&p[..i], &p[i + 1..]),
        None => (p, &[][..]),
    };
    let last = rest.is_empty() || rest == d.dot;
    let keyed = fs_keyed(d, e);
    if let (None, Some(k)) = (keyed, e.strip_prefix(d.f)) {
        if jp_kind(v) != b'{' {
            return o.put(v);
        }
        o.c(b'{')?;
        let mut first = true;
        for (mk, mv) in jp_members(v) {
            if mk == k && last {
                continue;
            }
            o.key(&mut first, mk)?;
            if mk == k {
                fs_without(d, mv, rest, o, depth + 1)?;
            } else {
                o.put(jp_trim(mv))?;
            }
        }
        return o.c(b'}');
    }
    let set = e.strip_prefix(d.v);
    if jp_kind(v) != b'[' || (keyed.is_none() && set.is_none()) {
        return o.put(v);
    }
    let hit = |el: &[u8]| match (keyed, set) {
        (Some((k, val)), _) => jp_get(el, k).is_some_and(|x| fs_esc_eq(jp_trim(x), val)),
        (_, Some(val)) => fs_esc_eq(jp_trim(el), val),
        _ => false,
    };
    o.c(b'[')?;
    let mut first = true;
    for el in jp_elems(v) {
        if hit(el) && last {
            continue;
        }
        o.elem(&mut first)?;
        if hit(el) {
            fs_without(d, el, rest, o, depth + 1)?;
        } else {
            o.put(jp_trim(el))?;
        }
    }
    o.c(b']')
}

/// Whether `p` names a list MEMBER (a keyed element's mark, a set's
/// element): owned for its presence, not for a value.
fn fs_is_member(d: &FsDialect, p: &[u8]) -> bool {
    let last = p.rsplit(|&c| c == FS_SEP).next().unwrap_or(&[]);
    last == d.dot || last.starts_with(d.v)
}

/// `p` without a trailing element mark: the element that path names.
fn fs_base<'p>(d: &FsDialect, p: &'p [u8]) -> &'p [u8] {
    match p.iter().rposition(|&c| c == FS_SEP) {
        Some(i) if &p[i + 1..] == d.dot => &p[..i],
        _ => p,
    }
}

/// Record a write in the object's managedFields.
///
/// `old` is the object as stored (`None`: a create); `new` the object the
/// write makes; `cfg` the applied configuration when the write is an APPLY
/// (then `new` is `cfg` merged over `old`). `work` holds the answer; `arena`
/// the field sets.
#[allow(
    clippy::too_many_arguments,
    reason = "a write's before, after and configuration, its writer, and the buffers"
)]
fn fs_record(
    old: Option<&[u8]>,
    new: &[u8],
    cfg: Option<&[u8]>,
    w: &FsWriter,
    arena: &mut [u8],
    work: &mut [u8],
) -> FsOutcome {
    if new.len() > FS_DOC_MAX || old.is_some_and(|o| o.len() > FS_DOC_MAX) {
        return FsOutcome::Untracked;
    }
    let Some(d) = fs_dialect(w.sch) else {
        return FsOutcome::Untracked;
    };
    let s = jp_smp(w.keys, w.sch);
    match fs_record_in(old, new, cfg, w, &s, d, arena, work) {
        Ok(r) => r,
        Err(_) => FsOutcome::Untracked,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "a write's before, after and configuration, its writer, and the buffers"
)]
fn fs_record_in(
    old: Option<&[u8]>,
    new: &[u8],
    cfg: Option<&[u8]>,
    w: &FsWriter,
    s: &JpSmp,
    d: FsDialect,
    arena: &mut [u8],
    work: &mut [u8],
) -> Result<FsOutcome, JpErr> {
    let mut fs = FsSet::new(arena, d);
    let mut cur = FsPath {
        b: [0u8; FS_PATH],
        n: 0,
    };
    if let Some(o) = old {
        fs.load(o, &mut cur)?;
    }
    let api = sch_at(new, d.api)
        .and_then(|v| v.strip_prefix(b"\"").and_then(|v| v.strip_suffix(b"\"")))
        .unwrap_or(&[]);
    let third = work.len() / 3;
    let (a, rest) = work.split_at_mut_checked(third).ok_or(JpErr::Full)?;
    let (b, pbuf) = rest.split_at_mut_checked(third).ok_or(JpErr::Full)?;
    // The object as it will be stored, before its managedFields: in `a`.
    let mut doc_n = {
        let d = a.get_mut(..new.len()).ok_or(JpErr::Full)?;
        d.copy_from_slice(new);
        new.len()
    };
    let mut in_a = true;

    if let Some(cfg) = cfg {
        // ── Apply ──
        cur.n = 0;
        fs.walk(cfg, s, b"", &mut cur, FS_NEW, 0)?;
        let me = fs.entry(w.mgr, d.apply, b"")?;
        // Conflicts: a field another manager owns, applied with a value that
        // is not the one it holds.
        let mut conflicts = [(0u8, 0usize, 0usize); FS_CONFLICTS];
        let mut nc = 0usize;
        let mut total = 0usize;
        let mut i = 0usize;
        while let Some((o, ps, pe, next)) = fs.rec(i) {
            if o != FS_DEAD && o != FS_NEW && o != me {
                let p = &fs.a[ps..pe];
                let clash = !fs_is_member(&d, p)
                    && fs.holds(|x| x == FS_NEW, p, false)
                    && match (
                        fs_resolve(&d, cfg, p),
                        old.and_then(|o| fs_resolve(&d, o, p)),
                    ) {
                        (Some(x), Some(y)) => !jp_equal(x, y, 0),
                        _ => false,
                    };
                if clash {
                    if w.force {
                        fs.a[i] = FS_DEAD;
                    } else {
                        if nc < FS_CONFLICTS {
                            conflicts[nc] = (o, ps, pe);
                            nc += 1;
                        }
                        total += 1;
                    }
                }
            }
            i = next;
        }
        if total > 0 {
            let mut o = JpOut::new(pbuf);
            fs_conflict_status(&fs, &conflicts[..nc], total, &mut o)?;
            return Ok(FsOutcome::Conflict(2 * third, o.n));
        }
        // Removal: what this manager applied before and no longer names,
        // when no one else owns it (or anything below it).
        let mut i = 0usize;
        while let Some((o, ps, pe, next)) = fs.rec(i) {
            i = next;
            if o != me {
                continue;
            }
            let p = &fs.a[ps..pe];
            let base = fs_base(&d, p);
            if fs.holds(|x| x == FS_NEW, base, true)
                || fs.holds(|x| x != me && x != FS_NEW, base, true)
            {
                continue;
            }
            let (src, dst): (&[u8], &mut [u8]) = if in_a {
                (&a[..doc_n], &mut b[..])
            } else {
                (&b[..doc_n], &mut a[..])
            };
            let mut out = JpOut::new(dst);
            fs_without(&d, src, p, &mut out, 0)?;
            doc_n = out.n;
            in_a = !in_a;
        }
        // The configuration's fields are now this manager's, and only those.
        let mut i = 0usize;
        while let Some((o, _, _, next)) = fs.rec(i) {
            if o == me {
                fs.a[i] = FS_DEAD;
            }
            i = next;
        }
        fs.relabel(FS_NEW, me);
        fs.e[me as usize].time = w.now;
        fs.e[me as usize].api = api;
    } else {
        // ── Update ──: the fields this write changed become the writer's.
        cur.n = 0;
        fs.walk(new, s, b"", &mut cur, FS_NEW, 0)?;
        let mut i = 0usize;
        while let Some((o, ps, pe, next)) = fs.rec(i) {
            if o == FS_NEW {
                let p = &fs.a[ps..pe];
                let same = match old {
                    None => false,
                    Some(o) if fs_is_member(&d, p) => fs_resolve(&d, o, p).is_some(),
                    Some(o) => match (fs_resolve(&d, o, p), fs_resolve(&d, new, p)) {
                        (Some(x), Some(y)) => jp_equal(x, y, 0),
                        _ => false,
                    },
                };
                if same {
                    fs.a[i] = FS_DEAD;
                }
            }
            i = next;
        }
        if fs.has_paths(FS_NEW) {
            let me = fs.entry(w.mgr, d.update, w.sub)?;
            // Taken from whoever held them.
            let mut i = 0usize;
            while let Some((o, ps, pe, next)) = fs.rec(i) {
                if o != FS_DEAD && o != FS_NEW && o != me {
                    let taken = {
                        let p = &fs.a[ps..pe];
                        fs.holds(|x| x == FS_NEW, p, false)
                    };
                    if taken {
                        fs.a[i] = FS_DEAD;
                    }
                }
                i = next;
            }
            fs.relabel(FS_NEW, me);
            fs.e[me as usize].time = w.now;
            fs.e[me as usize].api = api;
        }
    }

    let doc: &[u8] = if in_a { &a[..doc_n] } else { &b[..doc_n] };
    fs.prune(doc);
    let mut po = JpOut::new(pbuf);
    fs.render_patch(&mut po)?;
    let pn = po.n;
    // The managedFields patched in: into whichever of a/b is free.
    let patch = &pbuf[..pn];
    let (src, dst, off): (&[u8], &mut [u8], usize) = if in_a {
        (&a[..doc_n], &mut b[..], third)
    } else {
        (&b[..doc_n], &mut a[..], 0)
    };
    let mut out = JpOut::new(dst);
    jp_merge(Some(src), patch, &mut out, None, b"", 0)?;
    Ok(FsOutcome::Doc(off, out.n))
}

/// The refusal an apply's conflicts answer with: the schema's `conflict`,
/// its `{{list}}` the `conflict.item`s (one per line) and its `{{causes}}` the
/// `conflict.cause`s.
fn fs_conflict_status(
    fs: &FsSet,
    conflicts: &[(u8, usize, usize)],
    total: usize,
    o: &mut JpOut,
) -> Result<(), JpErr> {
    let d = &fs.d;
    let mut list = [0u8; 2048];
    let mut causes = [0u8; 2048];
    let mut lo = JpOut::new(&mut list);
    let mut co = JpOut::new(&mut causes);
    for (i, &(ow, ps, pe)) in conflicts.iter().enumerate() {
        let e = &fs.e[ow as usize];
        let mut field = [0u8; 512];
        let mut fo = JpOut::new(&mut field);
        fs_path_text(d, &fs.a[ps..pe], &mut fo)?;
        let fl = fo.n;
        let vars = [
            (&b"manager"[..], e.mgr),
            (&b"api"[..], e.api),
            (&b"field"[..], &field[..fl]),
        ];
        if i > 0 {
            lo.put(b"\\n")?;
            co.c(b',')?;
        }
        sch_fill_out(d.item, &vars, &mut lo)?;
        sch_fill_out(d.cause, &vars, &mut co)?;
    }
    let (ln, cn) = (lo.n, co.n);
    let mut count = [0u8; 20];
    let mut cto = JpOut::new(&mut count);
    fs_dec(total, &mut cto)?;
    let ctn = cto.n;
    let vars = [
        (&b"count"[..], &count[..ctn]),
        (&b"many"[..], if total > 1 { &b"1"[..] } else { &[][..] }),
        (&b"list"[..], &list[..ln]),
        (&b"causes"[..], &causes[..cn]),
    ];
    sch_fill_out(d.conflict, &vars, o)
}

fn fs_dec(v: usize, o: &mut JpOut) -> Result<(), JpErr> {
    let mut d = [0u8; 20];
    let mut n = 0usize;
    let mut x = v;
    loop {
        d[n] = b'0' + (x % 10) as u8;
        n += 1;
        x /= 10;
        if x == 0 {
            break;
        }
    }
    while n > 0 {
        n -= 1;
        o.c(d[n])?;
    }
    Ok(())
}
