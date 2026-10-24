// A document store's SCHEMA — `include!`d after json.rs and jsonpatch.rs.
//
// What an object's lifecycle stamps, keeps and guards is data, not code: the
// graph states it as one param of named directives, `name=value;…`, and the
// connector holds no document vocabulary of its own.
//
//   identity=<path>,…         paths a patch may not change (the same object)
//   create=<path>:<gen>,…     stamped on create, over what the client sent
//   generate=<path>           a create whose key names no object takes its
//                             name from this prefix plus five random chars
//   keep=<path>,…             carried from the stored object across a replace
//   named=<prefix>|<path>:<gen>   for a key `<prefix><name>` (one segment),
//                             stamped on create and kept on replace
//   guard=<path>              a non-empty list here holds a delete
//   deleting=<path>:<gen>,…   what a held delete stamps; the first path is the
//                             mark (set: the object is being deleted)
//
// A path is dotted; a segment holding dots is written in brackets
// (`labels[example.com/name]`). A generator is `uuid`, `now` (RFC 3339),
// `name` (the object's final name), or `#<json>` — a literal JSON value.

/// Segments a schema path may have.
const SCH_SEGS: usize = 8;

/// The value of directive `name`, or empty.
fn sch_get<'a>(schema: &'a [u8], name: &[u8]) -> &'a [u8] {
    for ent in schema.split(|&b| b == b';') {
        let ent = jp_trim(ent);
        if let Some(eq) = ent.iter().position(|&b| b == b'=') {
            if jp_trim(&ent[..eq]) == name {
                return jp_trim(&ent[eq + 1..]);
            }
        }
    }
    &[]
}

/// A path's segments into `segs`; their count (0: malformed or too deep).
fn sch_segs<'a>(path: &'a [u8], segs: &mut [&'a [u8]; SCH_SEGS]) -> usize {
    let mut n = 0usize;
    let mut i = 0usize;
    while i < path.len() {
        if n == SCH_SEGS {
            return 0;
        }
        let (seg, next) = if path[i] == b'[' {
            match path[i..].iter().position(|&b| b == b']') {
                Some(e) => (&path[i + 1..i + e], i + e + 1),
                None => return 0,
            }
        } else {
            let e = path[i..]
                .iter()
                .position(|&b| b == b'.' || b == b'[')
                .map_or(path.len(), |e| i + e);
            (&path[i..e], e)
        };
        if seg.is_empty() {
            return 0;
        }
        segs[n] = seg;
        n += 1;
        i = next;
        // A dot separates; a bracketed segment may follow directly.
        if i < path.len() && path[i] == b'.' {
            i += 1;
        } else if i < path.len() && path[i] != b'[' {
            return 0;
        }
    }
    n
}

/// The raw value at `path` in `doc`.
fn sch_at<'d>(doc: &'d [u8], path: &[u8]) -> Option<&'d [u8]> {
    let mut segs: [&[u8]; SCH_SEGS] = [&[]; SCH_SEGS];
    let n = sch_segs(path, &mut segs);
    if n == 0 {
        return None;
    }
    let mut v = jp_trim(doc);
    for seg in &segs[..n] {
        v = jp_trim(jp_get(v, seg)?);
    }
    Some(v)
}

/// Whether the value at `path` is set: present, not null, not empty.
fn sch_set(doc: &[u8], path: &[u8]) -> bool {
    sch_at(doc, path).is_some_and(|v| !v.is_empty() && v != b"null" && v != b"\"\"")
}

/// Values a generator draws on.
struct SchGen<'a> {
    uuid: &'a [u8],
    now: &'a [u8],
    name: &'a [u8],
}

/// A merge patch setting `path` to the raw JSON `val`, into `o`.
fn sch_one(path: &[u8], val: &[u8], o: &mut JpOut) -> Result<(), JpErr> {
    let mut segs: [&[u8]; SCH_SEGS] = [&[]; SCH_SEGS];
    let n = sch_segs(path, &mut segs);
    if n == 0 {
        return Err(JpErr::Path);
    }
    for seg in &segs[..n] {
        o.put(b"{\"")?;
        o.put(seg)?;
        o.put(b"\":")?;
    }
    o.put(val)?;
    for _ in 0..n {
        o.c(b'}')?;
    }
    Ok(())
}

/// Fold the single-path patch (`path`, `val`) into the patch `acc[..*n]`,
/// through `tmp`.
fn sch_fold(
    acc: &mut [u8],
    n: &mut usize,
    tmp: &mut [u8],
    path: &[u8],
    val: &[u8],
) -> Result<(), JpErr> {
    let mut one = [0u8; 512];
    let mut o1 = JpOut::new(&mut one);
    sch_one(path, val, &mut o1)?;
    let ol = o1.n;
    let mut o = JpOut::new(tmp);
    jp_merge(Some(&acc[..*n]), &one[..ol], &mut o, None, b"", 0)?;
    let m = o.n;
    let dst = acc.get_mut(..m).ok_or(JpErr::Full)?;
    dst.copy_from_slice(&tmp[..m]);
    *n = m;
    Ok(())
}

/// A generator's raw JSON value into `out`; its length.
fn sch_gen_value(gen: &[u8], g: &SchGen, out: &mut [u8]) -> Result<usize, JpErr> {
    let mut o = JpOut::new(out);
    match gen {
        b"uuid" | b"now" | b"name" => {
            let v = match gen {
                b"uuid" => g.uuid,
                b"now" => g.now,
                _ => g.name,
            };
            o.c(b'"')?;
            o.put(v)?;
            o.c(b'"')?;
        }
        _ => o.put(gen.strip_prefix(b"#").ok_or(JpErr::Malformed)?)?,
    }
    Ok(o.n)
}

/// An empty merge patch in `acc`; its length.
fn sch_empty(acc: &mut [u8]) -> Result<usize, JpErr> {
    let d = acc.get_mut(..2).ok_or(JpErr::Full)?;
    d.copy_from_slice(b"{}");
    Ok(2)
}

/// Every `<path>:<gen>` of `list` folded into the merge patch `acc[..n]`;
/// the new length.
fn sch_stamps(list: &[u8], g: &SchGen, acc: &mut [u8], n: usize) -> Result<usize, JpErr> {
    let mut n = n;
    let mut tmp = [0u8; 1024];
    for ent in list.split(|&b| b == b',') {
        let ent = jp_trim(ent);
        if ent.is_empty() {
            continue;
        }
        let c = ent
            .iter()
            .rposition(|&b| b == b':')
            .ok_or(JpErr::Malformed)?;
        let mut val = [0u8; 128];
        let vl = sch_gen_value(&ent[c + 1..], g, &mut val)?;
        sch_fold(acc, &mut n, &mut tmp, &ent[..c], &val[..vl])?;
    }
    Ok(n)
}

/// Every path of `list` that `from` sets, carried as one merge patch into
/// `acc` (appended to what is there); the new length.
fn sch_keeps(list: &[u8], from: &[u8], acc: &mut [u8], n: usize) -> Result<usize, JpErr> {
    let mut n = n;
    let mut tmp = [0u8; 1024];
    for path in list.split(|&b| b == b',') {
        let path = jp_trim(path);
        if path.is_empty() {
            continue;
        }
        if let Some(v) = sch_at(from, path).filter(|_| sch_set(from, path)) {
            if v.len() <= 256 {
                sch_fold(acc, &mut n, &mut tmp, path, v)?;
            }
        }
    }
    Ok(n)
}

/// Whether `a` and `b` agree on every path of `list` (both absent agrees).
fn sch_same(list: &[u8], a: &[u8], b: &[u8]) -> bool {
    list.split(|&c| c == b',').all(|path| {
        let path = jp_trim(path);
        path.is_empty() || sch_at(a, path) == sch_at(b, path)
    })
}

/// Whether the list at `path` holds anything.
fn sch_nonempty(doc: &[u8], path: &[u8]) -> bool {
    !path.is_empty() && sch_at(doc, path).is_some_and(|v| jp_elems(v).next().is_some())
}

#[allow(
    clippy::manual_contains,
    reason = "`<[u8]>::contains` lowers to memchr, which the PIC link cannot resolve"
)]
/// The `named` directive for `key`: (the path, its generator) when `key` is
/// `<prefix><one segment>`.
fn sch_named<'a>(named: &'a [u8], key: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let bar = named.iter().position(|&b| b == b'|')?;
    let (prefix, rest) = (&named[..bar], &named[bar + 1..]);
    let tail = key.strip_prefix(prefix)?;
    if tail.is_empty() || tail.iter().any(|&b| b == b'/') {
        return None;
    }
    let c = rest.iter().rposition(|&b| b == b':')?;
    Some((&rest[..c], &rest[c + 1..]))
}

/// The first path of a `<path>:<gen>,…` list: the mark of `deleting`.
fn sch_first_path(list: &[u8]) -> &[u8] {
    let first = list.split(|&b| b == b',').next().unwrap_or(&[]);
    let first = jp_trim(first);
    match first.iter().rposition(|&b| b == b':') {
        Some(c) => &first[..c],
        None => first,
    }
}

// ── Views ────────────────────────────────────────────────────────────────
//
//   view.<name>=tpl:<template>        read as a JSON template over the object
//   view.<name>=text:<lit>|<path>[|<lit>|<path>…]
//                                     read as the text stored at the key the
//                                     literals and the object's values at the
//                                     paths make, in turn: a Pod's log is
//                                     `/~node/|spec.nodeName|/sandbox-logs/|
//                                     metadata.uid`, its node's own record
//   write.<name>=<path>,…             a write through the view changes these
//                                     paths only (the object's revision rides
//                                     along as the write's precondition)
//
// A template is JSON with holes: `{{<path>|<default>}}` is the raw JSON value
// at path (the default's raw JSON when absent, `null` without one);
// `{{pairs:<path>}}` is an object's members as `k=v,k=v` (inside a string).

/// How a view reads.
enum SchView<'a> {
    Template(&'a [u8]),
    /// `<lit>|<path>|<lit>|<path>…`, the key's parts in turn.
    Text(&'a [u8]),
    Whole,
}

/// The directive `<kind>.<name>` of `sch`.
fn sch_dir<'a>(sch: &'a [u8], kind: &[u8], name: &[u8]) -> &'a [u8] {
    let mut dn = [0u8; 48];
    let l = kind.len() + 1 + name.len();
    if name.is_empty() || l > dn.len() {
        return &[];
    }
    dn[..kind.len()].copy_from_slice(kind);
    dn[kind.len()] = b'.';
    dn[kind.len() + 1..l].copy_from_slice(name);
    sch_get(sch, &dn[..l])
}

/// How view `name` reads.
fn sch_view<'a>(sch: &'a [u8], name: &[u8]) -> SchView<'a> {
    let d = sch_dir(sch, b"view", name);
    if let Some(t) = d.strip_prefix(b"tpl:") {
        return SchView::Template(t);
    }
    if let Some(t) = d.strip_prefix(b"text:") {
        if !t.iter().all(|&b| b != b'|') {
            return SchView::Text(t);
        }
    }
    SchView::Whole
}

/// Template `tpl` rendered over `doc` into `out`; its length.
fn sch_render(tpl: &[u8], doc: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut o = JpOut::new(out);
    let mut i = 0usize;
    while i < tpl.len() {
        let open = tpl[i..].windows(2).position(|w| w == b"{{");
        let Some(at) = open else {
            o.put(&tpl[i..]).ok()?;
            break;
        };
        o.put(&tpl[i..i + at]).ok()?;
        let inner_at = i + at + 2;
        let close = tpl[inner_at..].windows(2).position(|w| w == b"}}")?;
        let hole = &tpl[inner_at..inner_at + close];
        if let Some(path) = hole.strip_prefix(b"pairs:") {
            let mut first = true;
            if let Some(obj) = sch_at(doc, path) {
                for (k, v) in jp_members(obj) {
                    let v = jp_trim(v);
                    let v = v
                        .strip_prefix(b"\"")
                        .and_then(|v| v.strip_suffix(b"\""))
                        .unwrap_or(v);
                    if !first {
                        o.c(b',').ok()?;
                    }
                    first = false;
                    o.put(k).ok()?;
                    o.c(b'=').ok()?;
                    o.put(v).ok()?;
                }
            }
        } else {
            let (path, dflt) = match hole.iter().position(|&b| b == b'|') {
                Some(b) => (&hole[..b], &hole[b + 1..]),
                None => (hole, &b"null"[..]),
            };
            let v = sch_at(doc, path).filter(|v| !v.is_empty() && *v != b"null");
            o.put(v.unwrap_or(dflt)).ok()?;
        }
        i = inner_at + close + 2;
    }
    Some(o.n)
}

/// A write through view `name` as a merge patch into `out`: the view's
/// `write` paths from `body`, and `body`'s revision at `rv` (the write's
/// precondition). `None` when the view is not written this way or `body`
/// sets none of its paths.
fn sch_view_patch(
    sch: &[u8],
    name: &[u8],
    rv: &[u8],
    body: &[u8],
    out: &mut [u8],
) -> Option<usize> {
    let list = sch_dir(sch, b"write", name);
    if list.is_empty() {
        return None;
    }
    let mut n = sch_empty(out).ok()?;
    n = sch_keeps(list, body, out, n).ok()?;
    if n <= 2 {
        return None;
    }
    if !rv.is_empty() {
        n = sch_keeps(rv, body, out, n).ok()?;
    }
    Some(n)
}

// ── Wire fragments ───────────────────────────────────────────────────────
//
// A stream's framing is a template the connector fills with the values only
// it knows: `{{<name>}}` is a value; `{{?<name>:<text>}}` is `text` with `$`
// replaced by the value, and nothing when the value is empty.
//
//   event=<tpl>       one change: {{type}}, {{object}} (the object streams in
//                     chunks between the parts before and after it)
//   expired=<json>    the event a watch past its history ends with
//   failed=<json>     what a watch, list or get the store refused ends with:
//                     a watch's last ERROR event's object, a list's or a
//                     get's whole body
//   bookmark=<tpl>    a bookmark's object: {{kind}}, {{av}}, {{rv}}
//   list.tail=<tpl>   what closes a list: {{rv}}, {{continue}}

/// `tpl` with its holes filled from `vars` into `out`; its length.
fn sch_fill(tpl: &[u8], vars: &[(&[u8], &[u8])], out: &mut [u8]) -> Option<usize> {
    let var = |name: &[u8]| -> &[u8] {
        vars.iter()
            .find(|(n, _)| *n == name)
            .map_or(&[][..], |(_, v)| *v)
    };
    let mut o = JpOut::new(out);
    let mut i = 0usize;
    while i < tpl.len() {
        let Some(at) = tpl[i..].windows(2).position(|w| w == b"{{") else {
            o.put(&tpl[i..]).ok()?;
            break;
        };
        o.put(&tpl[i..i + at]).ok()?;
        let h = i + at + 2;
        let close = tpl[h..].windows(2).position(|w| w == b"}}")?;
        let hole = &tpl[h..h + close];
        if let Some(cond) = hole.strip_prefix(b"?") {
            let c = cond.iter().position(|&b| b == b':')?;
            let v = var(&cond[..c]);
            if !v.is_empty() {
                for &b in &cond[c + 1..] {
                    if b == b'$' {
                        o.put(v).ok()?;
                    } else {
                        o.c(b).ok()?;
                    }
                }
            }
        } else {
            o.put(var(hole)).ok()?;
        }
        i = h + close + 2;
    }
    Some(o.n)
}

/// The `event` template split around `{{object}}`, filled for `t`: the part
/// before the object into `pre`, after it into `post`; their lengths.
fn sch_event(sch: &[u8], t: &[u8], pre: &mut [u8], post: &mut [u8]) -> Option<(usize, usize)> {
    let tpl = sch_get(sch, b"event");
    let hole = b"{{object}}";
    let at = tpl.windows(hole.len()).position(|w| w == hole)?;
    let vars = [(&b"type"[..], t)];
    let a = sch_fill(&tpl[..at], &vars, pre)?;
    let b = sch_fill(&tpl[at + hole.len()..], &vars, post)?;
    Some((a, b))
}

// ── Merge dialect ───────────────────────────────────────────────────────
//
//   smp.patch=<member>     the member whose value replace|delete|merge acts
//   smp.retain=<member>    the list of members an object keeps
//   smp.delete=<prefix>    `<prefix><list>`: scalars to drop from a set list
//   smp.prefix=<prefix>    what every directive member starts with

/// The strategic-merge schema `keys` with the schema's directive names.
fn jp_smp<'k>(keys: &'k [u8], sch: &'k [u8]) -> JpSmp<'k> {
    JpSmp {
        keys,
        patch: sch_get(sch, b"smp.patch"),
        retain: sch_get(sch, b"smp.retain"),
        delete: sch_get(sch, b"smp.delete"),
        prefix: sch_get(sch, b"smp.prefix"),
    }
}

/// `path`'s objects opened (`{"a":{"b":`) into `o`, for a value to follow.
fn sch_open(path: &[u8], o: &mut JpOut) -> Result<usize, JpErr> {
    let mut segs: [&[u8]; SCH_SEGS] = [&[]; SCH_SEGS];
    let n = sch_segs(path, &mut segs);
    if n == 0 {
        return Err(JpErr::Path);
    }
    for seg in &segs[..n] {
        o.put(b"{\"")?;
        o.put(seg)?;
        o.put(b"\":")?;
    }
    Ok(n)
}

/// Close what `sch_open` opened (`n` objects).
fn sch_close(n: usize, o: &mut JpOut) -> Result<(), JpErr> {
    for _ in 0..n {
        o.c(b'}')?;
    }
    Ok(())
}

/// `tpl` filled from `vars`, appended to `o`.
fn sch_fill_out(tpl: &[u8], vars: &[(&[u8], &[u8])], o: &mut JpOut) -> Result<(), JpErr> {
    let n = {
        let dst = o.b.get_mut(o.n..).ok_or(JpErr::Full)?;
        sch_fill(tpl, vars, dst).ok_or(JpErr::Full)?
    };
    o.n += n;
    Ok(())
}

/// `tpl` split around the hole `hole`.
fn sch_split<'a>(tpl: &'a [u8], hole: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let at = tpl.windows(hole.len()).position(|w| w == hole)?;
    Some((&tpl[..at], &tpl[at + hole.len()..]))
}
