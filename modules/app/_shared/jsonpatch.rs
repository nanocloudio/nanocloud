// JSON patches applied to a stored document — `include!`d after `json.rs`.
//
// A Kubernetes PATCH arrives in one of three shapes, and all three are applied
// where the object is (the store connector): read it, edit it, write it back
// conditional on the revision read, and on a lost race read again — what
// upstream's GuaranteedUpdate does. Applying a patch anywhere upstream of the
// store would mean carrying the whole object through records, and a record
// is 4 KiB while an object is up to 1 MiB.
//
//   merge      RFC 7386 (`application/merge-patch+json`): objects merge
//              recursively, `null` deletes, anything else replaces.
//   strategic  Kubernetes' strategic merge
//              (`application/strategic-merge-patch+json`): a merge patch whose
//              object LISTS merge element by element on a key the schema names
//              (`keys`, see `jp_smp_key`), with the dialect's directives
//              (`JpSmp`: Kubernetes' `$patch` with `replace`/`delete`/
//              `merge`, `$retainKeys`, `$deleteFromPrimitiveList/<list>`,
//              and an ordering hint under the directive prefix
//              (an ordering hint: dropped — elements keep the stored order,
//              new ones follow in the patch's).
//   json       RFC 6902 (`application/json-patch+json`): `add`, `remove`,
//              `replace`, `move`, `copy`, `test` at JSON Pointers.
//
// No allocator: the caller supplies the output (and, for RFC 6902, the
// scratch). Running out is `Full` — never a cut document. Recursion is bounded
// by `JP_DEPTH`.

/// Nesting a patch may reach.
const JP_DEPTH: u32 = 48;
/// Tokens in one JSON Pointer.
const JP_TOKENS: usize = 32;

#[derive(Clone, Copy, PartialEq, Debug)]
enum JpErr {
    /// The output buffer is too small for the result.
    Full,
    /// The patch (or the document) is not the JSON its kind requires.
    Malformed,
    /// An RFC 6902 `test` did not hold.
    Test,
    /// An RFC 6902 path names nothing (or something it cannot edit).
    Path,
    /// Nested past `JP_DEPTH`.
    Deep,
}

struct JpOut<'a> {
    b: &'a mut [u8],
    n: usize,
}

impl<'a> JpOut<'a> {
    fn new(b: &'a mut [u8]) -> Self {
        JpOut { b, n: 0 }
    }
    fn put(&mut self, s: &[u8]) -> Result<(), JpErr> {
        let end = self.n.checked_add(s.len()).ok_or(JpErr::Full)?;
        let dst = self.b.get_mut(self.n..end).ok_or(JpErr::Full)?;
        dst.copy_from_slice(s);
        self.n = end;
        Ok(())
    }
    fn c(&mut self, c: u8) -> Result<(), JpErr> {
        let d = self.b.get_mut(self.n).ok_or(JpErr::Full)?;
        *d = c;
        self.n += 1;
        Ok(())
    }
    /// A member's key, quoted, and its colon — `k` as it was written.
    fn key(&mut self, first: &mut bool, k: &[u8]) -> Result<(), JpErr> {
        if !*first {
            self.c(b',')?;
        }
        *first = false;
        self.c(b'"')?;
        self.put(k)?;
        self.c(b'"')?;
        self.c(b':')
    }
    /// A separator before an array element.
    fn elem(&mut self, first: &mut bool) -> Result<(), JpErr> {
        if !*first {
            self.c(b',')?;
        }
        *first = false;
        Ok(())
    }
}

/// `v` without surrounding whitespace.
fn jp_trim(v: &[u8]) -> &[u8] {
    let s = j_ws(v, 0);
    let mut e = v.len();
    while e > s && j_is_ws(v[e - 1]) {
        e -= 1;
    }
    &v[s..e]
}

/// The first byte of a value: `{`, `[`, `"`, `n` (null), or a scalar's.
fn jp_kind(v: &[u8]) -> u8 {
    let i = j_ws(v, 0);
    if i < v.len() {
        v[i]
    } else {
        0
    }
}

fn jp_is_null(v: &[u8]) -> bool {
    jp_trim(v) == b"null"
}

/// An object's members: `(key as written, value)`, in order.
struct JpMembers<'a> {
    b: &'a [u8],
    i: usize,
    bad: bool,
}

fn jp_members(obj: &[u8]) -> JpMembers<'_> {
    let i = j_ws(obj, 0);
    let ok = i < obj.len() && obj[i] == b'{';
    JpMembers {
        b: obj,
        i: if ok { i + 1 } else { obj.len() },
        bad: !ok,
    }
}

impl<'a> Iterator for JpMembers<'a> {
    type Item = (&'a [u8], &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        let b = self.b;
        let mut j = j_ws(b, self.i);
        if j < b.len() && b[j] == b',' {
            j = j_ws(b, j + 1);
        }
        if j >= b.len() || b[j] == b'}' {
            self.i = b.len();
            return None;
        }
        if b[j] != b'"' {
            self.bad = true;
            self.i = b.len();
            return None;
        }
        let ks = j + 1;
        let mut ke = ks;
        while ke < b.len() && b[ke] != b'"' {
            if b[ke] == b'\\' {
                ke += 1;
            }
            ke += 1;
        }
        if ke >= b.len() {
            self.bad = true;
            self.i = b.len();
            return None;
        }
        let mut v = j_ws(b, ke + 1);
        if v >= b.len() || b[v] != b':' {
            self.bad = true;
            self.i = b.len();
            return None;
        }
        v = j_ws(b, v + 1);
        let e = j_skip(b, v);
        if e <= v || e > b.len() {
            self.bad = true;
            self.i = b.len();
            return None;
        }
        self.i = e;
        Some((&b[ks..ke], &b[v..e]))
    }
}

/// An array's elements, in order.
struct JpElems<'a> {
    b: &'a [u8],
    i: usize,
    bad: bool,
}

fn jp_elems(arr: &[u8]) -> JpElems<'_> {
    let i = j_ws(arr, 0);
    let ok = i < arr.len() && arr[i] == b'[';
    JpElems {
        b: arr,
        i: if ok { i + 1 } else { arr.len() },
        bad: !ok,
    }
}

impl<'a> Iterator for JpElems<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        let b = self.b;
        let mut j = j_ws(b, self.i);
        if j < b.len() && b[j] == b',' {
            j = j_ws(b, j + 1);
        }
        if j >= b.len() || b[j] == b']' {
            self.i = b.len();
            return None;
        }
        let e = j_skip(b, j);
        if e <= j || e > b.len() {
            self.bad = true;
            self.i = b.len();
            return None;
        }
        self.i = e;
        Some(&b[j..e])
    }
}

/// The value of member `key` of object `obj`.
fn jp_get<'a>(obj: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    jp_members(obj).find(|(k, _)| *k == key).map(|(_, v)| v)
}

/// Structural JSON equality: objects regardless of member order, everything
/// else as written (whitespace aside).
fn jp_equal(a: &[u8], b: &[u8], depth: u32) -> bool {
    if depth > JP_DEPTH {
        return false;
    }
    let (a, b) = (jp_trim(a), jp_trim(b));
    match (jp_kind(a), jp_kind(b)) {
        (b'{', b'{') => {
            let (mut na, mut nb) = (0usize, 0usize);
            for (k, va) in jp_members(a) {
                na += 1;
                match jp_get(b, k) {
                    Some(vb) if jp_equal(va, vb, depth + 1) => {}
                    _ => return false,
                }
            }
            for _ in jp_members(b) {
                nb += 1;
            }
            na == nb
        }
        (b'[', b'[') => {
            let mut ea = jp_elems(a);
            let mut eb = jp_elems(b);
            loop {
                match (ea.next(), eb.next()) {
                    (None, None) => return true,
                    (Some(x), Some(y)) if jp_equal(x, y, depth + 1) => {}
                    _ => return false,
                }
            }
        }
        _ => a == b,
    }
}

// ── RFC 7386 and strategic merge ─────────────────────────────────────────

/// The strategic-merge schema: `keys` is `list=key`, `list@parent=key` or
/// `+list`, comma-separated. `list@parent` applies inside an element of the
/// list `parent` (a container's `ports` merge on `containerPort`, a Service's
/// on `port`); `list` applies anywhere else; `+list` is a list of scalars that
/// merges as a set (`finalizers`). A list named nowhere is replaced whole, as
/// upstream does for an atomic list.
///
/// The directives are the dialect's (docschema.rs `jp_smp`): `patch` names a
/// member whose value `replace`/`delete`/`merge` acts on its object, `retain`
/// the list of members an object keeps, `delete` the prefix of a member that
/// lists scalars to drop from a set, and `prefix` what every directive member
/// starts with (never data). Empty names: no directives.
struct JpSmp<'k> {
    keys: &'k [u8],
    patch: &'k [u8],
    retain: &'k [u8],
    delete: &'k [u8],
    prefix: &'k [u8],
}

/// What the schema says about list `list` inside an element of `parent`.
enum JpList<'k> {
    Atomic,
    Keyed(&'k [u8]),
    Set,
}

fn jp_smp_key<'k>(smp: &JpSmp<'k>, list: &[u8], parent: &[u8]) -> JpList<'k> {
    let mut general = JpList::Atomic;
    for entry in smp.keys.split(|&c| c == b',') {
        let entry = jp_trim(entry);
        if let Some(name) = entry.strip_prefix(b"+") {
            if name == list {
                return JpList::Set;
            }
            continue;
        }
        let Some(eq) = entry.iter().position(|&c| c == b'=') else {
            continue;
        };
        let (lhs, key) = (&entry[..eq], &entry[eq + 1..]);
        match lhs.iter().position(|&c| c == b'@') {
            Some(at) => {
                if &lhs[..at] == list && &lhs[at + 1..] == parent {
                    return JpList::Keyed(key);
                }
            }
            None => {
                if lhs == list {
                    general = JpList::Keyed(key);
                }
            }
        }
    }
    general
}

/// A member key that is a strategic-merge directive, not data.
fn jp_directive(s: &JpSmp, k: &[u8]) -> bool {
    !s.prefix.is_empty() && k.starts_with(s.prefix)
}

/// The patch directive of an object value, if it has one.
fn jp_patch_directive<'v>(s: &JpSmp, v: &'v [u8]) -> Option<&'v [u8]> {
    if jp_kind(v) != b'{' || s.patch.is_empty() {
        return None;
    }
    jp_get(v, s.patch).map(|d| {
        let d = jp_trim(d);
        if d.len() >= 2 && d[0] == b'"' {
            &d[1..d.len() - 1]
        } else {
            d
        }
    })
}

/// Apply merge `patch` to `target` (absent: `None`), writing the result.
/// `smp` selects strategic merge; `parent` is the nearest enclosing list's
/// name (strategic merge's context for a list inside an element).
fn jp_merge(
    target: Option<&[u8]>,
    patch: &[u8],
    o: &mut JpOut,
    smp: Option<&JpSmp>,
    parent: &[u8],
    depth: u32,
) -> Result<(), JpErr> {
    if depth > JP_DEPTH {
        return Err(JpErr::Deep);
    }
    let patch = jp_trim(patch);
    if jp_kind(patch) != b'{' {
        // Not an object: the patch value IS the result.
        return o.put(patch);
    }
    // `$patch: replace` — this object as the patch has it, the stored one
    // ignored.
    let replace = smp.is_some_and(|s| jp_patch_directive(s, patch) == Some(b"replace"));
    let target = match target {
        Some(t) if !replace && jp_kind(t) == b'{' => Some(jp_trim(t)),
        _ => None,
    };
    // The retain directive: the stored members this object keeps; the rest go.
    let retain = smp.and_then(|s| {
        if s.retain.is_empty() {
            None
        } else {
            jp_get(patch, s.retain)
        }
    });
    o.c(b'{')?;
    let mut first = true;
    if let Some(t) = target {
        let mut members = jp_members(t);
        for (k, tv) in &mut members {
            if let Some(r) = retain {
                if !jp_elems(r).any(|e| jp_str_is(e, k)) {
                    continue;
                }
            }
            match jp_get(patch, k) {
                None => {
                    let v = jp_trim(tv);
                    let v = match smp {
                        Some(s) => jp_prim_delete(v, patch, k, s)?,
                        None => None,
                    };
                    o.key(&mut first, k)?;
                    match v {
                        Some(filtered) => jp_put_filtered(o, jp_trim(tv), filtered)?,
                        None => o.put(jp_trim(tv))?,
                    }
                }
                Some(pv) if jp_is_null(pv) => {}
                Some(pv) if smp.is_some_and(|s| jp_patch_directive(s, pv) == Some(b"delete")) => {}
                Some(pv) => {
                    o.key(&mut first, k)?;
                    jp_merge_value(Some(tv), pv, k, patch, o, smp, parent, depth)?;
                }
            }
        }
        if members.bad {
            return Err(JpErr::Malformed);
        }
    }
    let mut members = jp_members(patch);
    for (k, pv) in &mut members {
        if smp.is_some_and(|s| jp_directive(s, k)) {
            continue;
        }
        if target.is_some_and(|t| jp_get(t, k).is_some()) {
            continue;
        }
        if jp_is_null(pv) || smp.is_some_and(|s| jp_patch_directive(s, pv) == Some(b"delete")) {
            continue;
        }
        o.key(&mut first, k)?;
        jp_merge_value(None, pv, k, patch, o, smp, parent, depth)?;
    }
    if members.bad {
        return Err(JpErr::Malformed);
    }
    o.c(b'}')
}

/// One member's merged value: `tv` stored, `pv` patched, under key `k` of
/// the patch object `pobj`.
#[allow(
    clippy::too_many_arguments,
    reason = "one merge step's inputs: the two values, where they sit, and the context"
)]
fn jp_merge_value(
    tv: Option<&[u8]>,
    pv: &[u8],
    k: &[u8],
    pobj: &[u8],
    o: &mut JpOut,
    smp: Option<&JpSmp>,
    parent: &[u8],
    depth: u32,
) -> Result<(), JpErr> {
    let Some(s) = smp else {
        return jp_merge(tv, pv, o, None, parent, depth + 1);
    };
    if jp_kind(pv) != b'[' {
        return jp_merge(tv, pv, o, smp, parent, depth + 1);
    }
    let stored = tv.filter(|t| jp_kind(t) == b'[');
    match jp_smp_key(s, k, parent) {
        JpList::Keyed(key) => jp_merge_list(stored, pv, key, k, o, s, depth + 1),
        JpList::Set => {
            // Stored order, then what the patch adds, less what it deletes.
            let del = jp_del_list(s, pobj, k);
            o.c(b'[')?;
            let mut first = true;
            if let Some(t) = stored {
                for e in jp_elems(t) {
                    if del.is_some_and(|d| jp_elems(d).any(|x| jp_equal(x, e, 0))) {
                        continue;
                    }
                    o.elem(&mut first)?;
                    o.put(jp_trim(e))?;
                }
            }
            for e in jp_elems(pv) {
                if stored.is_some_and(|t| jp_elems(t).any(|x| jp_equal(x, e, 0))) {
                    continue;
                }
                o.elem(&mut first)?;
                o.put(jp_trim(e))?;
            }
            o.c(b']')
        }
        JpList::Atomic => {
            // Replaced whole; a patch's own elements are written as merged
            // into nothing (its nulls and directives dropped).
            o.c(b'[')?;
            let mut first = true;
            let mut elems = jp_elems(pv);
            for e in &mut elems {
                o.elem(&mut first)?;
                jp_merge(None, e, o, smp, k, depth + 1)?;
            }
            if elems.bad {
                return Err(JpErr::Malformed);
            }
            o.c(b']')
        }
    }
}

/// A list merged on `key`: stored elements in order (merged with the patch's
/// element of the same key, or deleted by `$patch: delete`), then the patch's
/// new ones. A `{"$patch":"replace"}` element makes the patch's list the
/// result.
fn jp_merge_list(
    stored: Option<&[u8]>,
    pl: &[u8],
    key: &[u8],
    name: &[u8],
    o: &mut JpOut,
    s: &JpSmp,
    depth: u32,
) -> Result<(), JpErr> {
    if depth > JP_DEPTH {
        return Err(JpErr::Deep);
    }
    let smp = Some(s);
    let replace = jp_elems(pl).any(|e| jp_patch_directive(s, e) == Some(b"replace"));
    o.c(b'[')?;
    let mut first = true;
    if let (Some(t), false) = (stored, replace) {
        for te in jp_elems(t) {
            let tk = jp_get(te, key);
            let pe = tk.and_then(|tk| {
                jp_elems(pl).find(|pe| jp_get(pe, key).is_some_and(|pk| jp_equal(pk, tk, 0)))
            });
            match pe {
                Some(pe) if jp_patch_directive(s, pe) == Some(b"delete") => {}
                Some(pe) => {
                    o.elem(&mut first)?;
                    jp_merge(Some(te), pe, o, smp, name, depth + 1)?;
                }
                None => {
                    o.elem(&mut first)?;
                    o.put(jp_trim(te))?;
                }
            }
        }
    }
    let mut elems = jp_elems(pl);
    for pe in &mut elems {
        let d = jp_patch_directive(s, pe);
        if d == Some(b"replace") || d == Some(b"delete") {
            continue;
        }
        let known = !replace
            && stored.is_some_and(|t| {
                jp_get(pe, key).is_some_and(|pk| {
                    jp_elems(t).any(|te| jp_get(te, key).is_some_and(|tk| jp_equal(pk, tk, 0)))
                })
            });
        if known {
            continue;
        }
        o.elem(&mut first)?;
        jp_merge(None, pe, o, smp, name, depth + 1)?;
    }
    if elems.bad {
        return Err(JpErr::Malformed);
    }
    o.c(b']')
}

/// The delete-from-set directive for list `k` of the patch object `pobj`.
fn jp_del_list<'a>(s: &JpSmp, pobj: &'a [u8], k: &[u8]) -> Option<&'a [u8]> {
    if s.delete.is_empty() {
        return None;
    }
    jp_members(pobj).find_map(|(dk, dv)| {
        let rest = dk.strip_prefix(s.delete)?;
        (rest == k).then_some(dv)
    })
}

/// A stored member the patch does not name, but whose values a
/// delete-from-set directive removes: the list of values to drop.
fn jp_prim_delete<'a>(
    tv: &[u8],
    pobj: &'a [u8],
    k: &[u8],
    s: &JpSmp,
) -> Result<Option<&'a [u8]>, JpErr> {
    if jp_kind(tv) != b'[' {
        return Ok(None);
    }
    Ok(jp_del_list(s, pobj, k))
}

/// `tv` (a list) less the values in `drop`.
fn jp_put_filtered(o: &mut JpOut, tv: &[u8], drop: &[u8]) -> Result<(), JpErr> {
    o.c(b'[')?;
    let mut first = true;
    for e in jp_elems(tv) {
        if jp_elems(drop).any(|x| jp_equal(x, e, 0)) {
            continue;
        }
        o.elem(&mut first)?;
        o.put(jp_trim(e))?;
    }
    o.c(b']')
}

/// Is JSON string `v` (quoted) the text `k` (as a key is written)?
fn jp_str_is(v: &[u8], k: &[u8]) -> bool {
    let v = jp_trim(v);
    v.len() == k.len() + 2 && v[0] == b'"' && &v[1..v.len() - 1] == k
}

// ── RFC 6902 ─────────────────────────────────────────────────────────────

/// A JSON Pointer's reference tokens, as `(start, end)` in the pointer.
struct JpPtr<'p> {
    p: &'p [u8],
    t: [(u16, u16); JP_TOKENS],
    n: usize,
}

fn jp_ptr(p: &[u8]) -> Result<JpPtr<'_>, JpErr> {
    let mut ptr = JpPtr {
        p,
        t: [(0, 0); JP_TOKENS],
        n: 0,
    };
    if p.is_empty() {
        return Ok(ptr);
    }
    if p[0] != b'/' || p.len() > u16::MAX as usize {
        return Err(JpErr::Malformed);
    }
    let mut s = 1usize;
    loop {
        let e = p[s..]
            .iter()
            .position(|&c| c == b'/')
            .map_or(p.len(), |i| s + i);
        if ptr.n >= JP_TOKENS {
            return Err(JpErr::Deep);
        }
        ptr.t[ptr.n] = (s as u16, e as u16);
        ptr.n += 1;
        if e >= p.len() {
            return Ok(ptr);
        }
        s = e + 1;
    }
}

impl JpPtr<'_> {
    fn tok(&self, i: usize) -> &[u8] {
        let (s, e) = self.t[i];
        &self.p[s as usize..e as usize]
    }
}

/// Does pointer token `tok` (with `~1` for `/` and `~0` for `~`) name key `k`?
fn jp_tok_is(tok: &[u8], k: &[u8]) -> bool {
    let (mut i, mut j) = (0usize, 0usize);
    while i < tok.len() {
        let c = if tok[i] == b'~' && i + 1 < tok.len() {
            i += 1;
            match tok[i] {
                b'1' => b'/',
                b'0' => b'~',
                _ => return false,
            }
        } else {
            tok[i]
        };
        if j >= k.len() || k[j] != c {
            return false;
        }
        i += 1;
        j += 1;
    }
    j == k.len()
}

/// Write pointer token `tok` as a JSON key (unescaped `~1`/`~0`, JSON-escaped
/// `"` and `\`).
fn jp_put_tok(o: &mut JpOut, first: &mut bool, tok: &[u8]) -> Result<(), JpErr> {
    if !*first {
        o.c(b',')?;
    }
    *first = false;
    o.c(b'"')?;
    let mut i = 0usize;
    while i < tok.len() {
        let c = if tok[i] == b'~' && i + 1 < tok.len() {
            i += 1;
            if tok[i] == b'1' {
                b'/'
            } else {
                b'~'
            }
        } else {
            tok[i]
        };
        if c == b'"' || c == b'\\' {
            o.c(b'\\')?;
        }
        o.c(c)?;
        i += 1;
    }
    o.c(b'"')?;
    o.c(b':')
}

/// The value `ptr` (from token `at`) names in `doc`.
fn jp_at<'a>(doc: &'a [u8], ptr: &JpPtr, at: usize) -> Option<&'a [u8]> {
    let mut cur = jp_trim(doc);
    for i in at..ptr.n {
        let tok = ptr.tok(i);
        cur = match jp_kind(cur) {
            b'{' => jp_members(cur)
                .find(|(k, _)| jp_tok_is(tok, k))
                .map(|(_, v)| v)?,
            b'[' => jp_elems(cur).nth(j_parse_usize(tok)?)?,
            _ => return None,
        };
        cur = jp_trim(cur);
    }
    Some(cur)
}

#[derive(Clone, Copy, PartialEq)]
enum JpEdit {
    Add,
    Remove,
    Replace,
}

/// `doc` with the edit at `ptr` (from token `at`), written to `o`.
fn jp_edit(
    doc: &[u8],
    ptr: &JpPtr,
    at: usize,
    edit: JpEdit,
    value: &[u8],
    o: &mut JpOut,
    depth: u32,
) -> Result<(), JpErr> {
    if depth > JP_DEPTH {
        return Err(JpErr::Deep);
    }
    let doc = jp_trim(doc);
    if at >= ptr.n {
        // The whole document: add/replace it; removing it is not an edit.
        return match edit {
            JpEdit::Remove => Err(JpErr::Path),
            _ => o.put(jp_trim(value)),
        };
    }
    let tok = ptr.tok(at);
    let last = at + 1 == ptr.n;
    match jp_kind(doc) {
        b'{' => {
            o.c(b'{')?;
            let mut first = true;
            let mut found = false;
            for (k, v) in jp_members(doc) {
                if !jp_tok_is(tok, k) {
                    o.key(&mut first, k)?;
                    o.put(jp_trim(v))?;
                    continue;
                }
                found = true;
                if last {
                    match edit {
                        JpEdit::Remove => {}
                        _ => {
                            o.key(&mut first, k)?;
                            o.put(jp_trim(value))?;
                        }
                    }
                } else {
                    o.key(&mut first, k)?;
                    jp_edit(v, ptr, at + 1, edit, value, o, depth + 1)?;
                }
            }
            if !found {
                if !(last && edit == JpEdit::Add) {
                    return Err(JpErr::Path);
                }
                jp_put_tok(o, &mut first, tok)?;
                o.put(jp_trim(value))?;
            }
            o.c(b'}')
        }
        b'[' => {
            let len = jp_elems(doc).count();
            let idx = if tok == b"-" {
                if !(last && edit == JpEdit::Add) {
                    return Err(JpErr::Path);
                }
                len
            } else {
                // No leading zeros (RFC 6901): `01` names nothing.
                if tok.len() > 1 && tok[0] == b'0' {
                    return Err(JpErr::Path);
                }
                j_parse_usize(tok).ok_or(JpErr::Path)?
            };
            let limit = if last && edit == JpEdit::Add {
                len
            } else {
                len.saturating_sub(1)
            };
            if idx > limit || (len == 0 && !(last && edit == JpEdit::Add)) {
                return Err(JpErr::Path);
            }
            o.c(b'[')?;
            let mut first = true;
            for (i, e) in jp_elems(doc).enumerate() {
                if i == idx {
                    if last {
                        match edit {
                            JpEdit::Add => {
                                o.elem(&mut first)?;
                                o.put(jp_trim(value))?;
                                o.elem(&mut first)?;
                                o.put(jp_trim(e))?;
                            }
                            JpEdit::Remove => {}
                            JpEdit::Replace => {
                                o.elem(&mut first)?;
                                o.put(jp_trim(value))?;
                            }
                        }
                    } else {
                        o.elem(&mut first)?;
                        jp_edit(e, ptr, at + 1, edit, value, o, depth + 1)?;
                    }
                    continue;
                }
                o.elem(&mut first)?;
                o.put(jp_trim(e))?;
            }
            if idx == len {
                o.elem(&mut first)?;
                o.put(jp_trim(value))?;
            }
            o.c(b']')
        }
        _ => Err(JpErr::Path),
    }
}

/// A JSON string member's content (`"op":"add"` → `add`).
fn jp_str<'a>(obj: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let v = jp_trim(jp_get(obj, key)?);
    if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
        Some(&v[1..v.len() - 1])
    } else {
        None
    }
}

/// `work` as three equal, disjoint thirds.
fn jp_thirds(work: &mut [u8]) -> Result<(&mut [u8], &mut [u8], &mut [u8]), JpErr> {
    let third = work.len() / 3;
    // The checked split: the plain one's assert formats a message, and a PIC
    // module has no formatter to link.
    let (a, rest) = work.split_at_mut_checked(third).ok_or(JpErr::Full)?;
    let (b, c) = rest.split_at_mut_checked(third).ok_or(JpErr::Full)?;
    Ok((a, b, c))
}

/// Apply the RFC 6902 operations in `patch` to `doc`. `work` is split in
/// three: the document ping-pongs between two thirds, and a moved or copied
/// value is held in the third. Returns the result's `(offset, len)` in `work`.
fn jp_json(doc: &[u8], patch: &[u8], work: &mut [u8]) -> Result<(usize, usize), JpErr> {
    let third = work.len() / 3;
    if jp_kind(patch) != b'[' {
        return Err(JpErr::Malformed);
    }
    let doc = jp_trim(doc);
    if doc.len() > third {
        return Err(JpErr::Full);
    }
    work[..doc.len()].copy_from_slice(doc);
    // Which third holds the document, and its length.
    let (mut in_a, mut len) = (true, doc.len());
    let mut ops = jp_elems(patch);
    for op in &mut ops {
        let kind = jp_str(op, b"op").ok_or(JpErr::Malformed)?;
        let path = jp_ptr(jp_str(op, b"path").ok_or(JpErr::Malformed)?)?;
        let (a, b, c) = jp_thirds(work)?;
        let (src_buf, dst_buf) = if in_a { (a, b) } else { (b, a) };
        match kind {
            b"test" => {
                let want = jp_get(op, b"value").ok_or(JpErr::Malformed)?;
                let got = jp_at(&src_buf[..len], &path, 0).ok_or(JpErr::Test)?;
                if !jp_equal(got, want, 0) {
                    return Err(JpErr::Test);
                }
            }
            b"add" | b"replace" | b"remove" => {
                let (edit, v) = if kind == b"remove" {
                    (JpEdit::Remove, &[][..])
                } else {
                    let v = jp_get(op, b"value").ok_or(JpErr::Malformed)?;
                    (
                        if kind == b"add" {
                            JpEdit::Add
                        } else {
                            JpEdit::Replace
                        },
                        v,
                    )
                };
                let mut o = JpOut::new(dst_buf);
                jp_edit(&src_buf[..len], &path, 0, edit, v, &mut o, 0)?;
                len = o.n;
                in_a = !in_a;
            }
            b"move" | b"copy" => {
                let from = jp_ptr(jp_str(op, b"from").ok_or(JpErr::Malformed)?)?;
                let v = jp_at(&src_buf[..len], &from, 0).ok_or(JpErr::Path)?;
                if v.len() > c.len() {
                    return Err(JpErr::Full);
                }
                c[..v.len()].copy_from_slice(v);
                let held = &c[..v.len()];
                if kind == b"copy" {
                    let mut o = JpOut::new(dst_buf);
                    jp_edit(&src_buf[..len], &path, 0, JpEdit::Add, held, &mut o, 0)?;
                    len = o.n;
                    in_a = !in_a;
                } else {
                    // A location cannot move into one of its own children.
                    if path.n > from.n && (0..from.n).all(|i| path.tok(i) == from.tok(i)) {
                        return Err(JpErr::Path);
                    }
                    // Remove into the other third, then add from there back
                    // into this one: two passes, one operation.
                    let mut o = JpOut::new(dst_buf);
                    jp_edit(&src_buf[..len], &from, 0, JpEdit::Remove, &[], &mut o, 0)?;
                    let mid = o.n;
                    let mut o2 = JpOut::new(src_buf);
                    jp_edit(&dst_buf[..mid], &path, 0, JpEdit::Add, held, &mut o2, 0)?;
                    len = o2.n;
                }
            }
            _ => return Err(JpErr::Malformed),
        }
    }
    if ops.bad {
        return Err(JpErr::Malformed);
    }
    Ok((if in_a { 0 } else { third }, len))
}
