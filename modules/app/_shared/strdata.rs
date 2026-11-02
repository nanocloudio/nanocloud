// A write-only member folded into its stored one, as the API server does with
// a Secret's `stringData`: each of its string members is written into `data`
// base64-encoded (a key in both takes `stringData`'s value), and `stringData`
// itself is never stored, so a reader sees `data` alone — what upstream's
// Secret strategy does on create and on update.
//
// The rule is the schema's `fold` entry, `<key prefix>|<from>><to>` (several
// joined by `,`): `fold=/secrets/|stringData>data`.
//
// Mounted by store_effect (and the harness) after json.rs and jsonpatch.rs:
// it reads members with `jp_members` and writes with `JpOut`.

/// The `(from, to)` of the first rule whose prefix `key` lies under.
fn sd_rule<'a>(fold: &'a [u8], key: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    for rule in fold.split(|&b| b == b',') {
        let rule = jp_trim(rule);
        let Some(bar) = rule.iter().position(|&b| b == b'|') else {
            continue;
        };
        let (prefix, members) = (&rule[..bar], &rule[bar + 1..]);
        if prefix.is_empty() || !key.starts_with(prefix) || key.len() == prefix.len() {
            continue;
        }
        let Some(gt) = members.iter().position(|&b| b == b'>') else {
            continue;
        };
        let (from, to) = (&members[..gt], &members[gt + 1..]);
        if !from.is_empty() && !to.is_empty() {
            return Some((from, to));
        }
    }
    None
}

/// Whether a member key as written (`k`, unquoted bytes between the quotes)
/// is `name`. Keys in these documents are plain identifiers; an escaped key
/// is compared as written.
fn sd_key_is(k: &[u8], name: &[u8]) -> bool {
    k == name
}

/// Four hex digits.
fn sd_hex4(h: &[u8]) -> Option<u32> {
    if h.len() < 4 {
        return None;
    }
    let mut v = 0u32;
    for &c in &h[..4] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            _ => return None,
        };
        v = (v << 4) | u32::from(d);
    }
    Some(v)
}

/// The bytes a JSON string literal (`"…"`, quotes included) stands for,
/// into `out`; their count. `None` for a value that is not a string, a bad
/// escape, or an `out` too small.
fn sd_unescape(lit: &[u8], out: &mut [u8]) -> Option<usize> {
    let lit = jp_trim(lit);
    if lit.len() < 2 || lit[0] != b'"' || lit[lit.len() - 1] != b'"' {
        return None;
    }
    let s = &lit[1..lit.len() - 1];
    let mut n = 0usize;
    let mut i = 0usize;
    let mut put = |b: u8, n: &mut usize| -> Option<()> {
        *out.get_mut(*n)? = b;
        *n += 1;
        Some(())
    };
    while i < s.len() {
        let c = s[i];
        if c != b'\\' {
            put(c, &mut n)?;
            i += 1;
            continue;
        }
        let e = *s.get(i + 1)?;
        i += 2;
        let b = match e {
            b'"' => b'"',
            b'\\' => b'\\',
            b'/' => b'/',
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'u' => {
                let mut cp = sd_hex4(s.get(i..)?)?;
                i += 4;
                if (0xD800..0xDC00).contains(&cp) {
                    // A high surrogate: its low half follows as `\uXXXX`.
                    if s.get(i) != Some(&b'\\') || s.get(i + 1) != Some(&b'u') {
                        return None;
                    }
                    let lo = sd_hex4(s.get(i + 2..)?)?;
                    if !(0xDC00..0xE000).contains(&lo) {
                        return None;
                    }
                    i += 6;
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                } else if (0xDC00..0xE000).contains(&cp) {
                    return None;
                }
                let mut u = [0u8; 4];
                let l = if cp < 0x80 {
                    u[0] = cp as u8;
                    1
                } else if cp < 0x800 {
                    u[0] = 0xC0 | (cp >> 6) as u8;
                    u[1] = 0x80 | (cp & 0x3F) as u8;
                    2
                } else if cp < 0x10000 {
                    u[0] = 0xE0 | (cp >> 12) as u8;
                    u[1] = 0x80 | ((cp >> 6) & 0x3F) as u8;
                    u[2] = 0x80 | (cp & 0x3F) as u8;
                    3
                } else {
                    u[0] = 0xF0 | (cp >> 18) as u8;
                    u[1] = 0x80 | ((cp >> 12) & 0x3F) as u8;
                    u[2] = 0x80 | ((cp >> 6) & 0x3F) as u8;
                    u[3] = 0x80 | (cp & 0x3F) as u8;
                    4
                };
                for &x in &u[..l] {
                    put(x, &mut n)?;
                }
                continue;
            }
            _ => return None,
        };
        put(b, &mut n)?;
    }
    Some(n)
}

/// `src` as standard base64 (padded), quoted, onto `o`.
fn sd_b64(o: &mut JpOut, src: &[u8]) -> Result<(), JpErr> {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    o.c(b'"')?;
    for ch in src.chunks(3) {
        let b0 = ch[0];
        let b1 = ch.get(1).copied().unwrap_or(0);
        let b2 = ch.get(2).copied().unwrap_or(0);
        o.c(A[(b0 >> 2) as usize])?;
        o.c(A[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize])?;
        o.c(if ch.len() > 1 {
            A[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize]
        } else {
            b'='
        })?;
        o.c(if ch.len() > 2 {
            A[(b2 & 0x3f) as usize]
        } else {
            b'='
        })?;
    }
    o.c(b'"')
}

/// The largest string member `sd_fold` decodes; a longer one is refused
/// (`JpErr::Full`) rather than cut.
const SD_VALUE_MAX: usize = 1 << 20;

/// `doc` with its `from` member folded into `to` (see the file head), onto
/// `o`. `Ok(false)` — nothing written — when `doc` carries no `from` member
/// (the caller keeps `doc` as it is); a `from` that is `null` is dropped.
/// `Err(Malformed)` when `doc` is not an object, `from` is not an object of
/// strings, or `to` is there and not an object (upstream answers these 400).
/// `patch`: `doc` is a merge patch (RFC 7386, or a strategic one), whose
/// `null` members are deletions — of a write-only member they delete
/// nothing, so they are dropped rather than refused. The folded patch
/// merged over the stored object is the object upstream stores.
/// `scratch` holds one decoded value at a time.
fn sd_fold(
    doc: &[u8],
    from: &[u8],
    to: &[u8],
    patch: bool,
    o: &mut JpOut,
    scratch: &mut [u8],
) -> Result<bool, JpErr> {
    if jp_kind(doc) != b'{' {
        return Err(JpErr::Malformed);
    }
    let mut fv: Option<&[u8]> = None;
    let mut tv: Option<&[u8]> = None;
    for (k, v) in jp_members(doc) {
        if sd_key_is(k, from) {
            fv = Some(v);
        } else if sd_key_is(k, to) {
            tv = Some(v);
        }
    }
    let Some(fv) = fv else {
        return Ok(false);
    };
    let fnull = jp_is_null(fv);
    if !fnull && jp_kind(fv) != b'{' {
        return Err(JpErr::Malformed);
    }
    if !fnull {
        for (_, v) in jp_members(fv) {
            if jp_kind(v) != b'"' && !(patch && jp_is_null(v)) {
                return Err(JpErr::Malformed);
            }
        }
    }
    if let Some(t) = tv {
        if !jp_is_null(t) && jp_kind(t) != b'{' {
            return Err(JpErr::Malformed);
        }
    }
    // The members that write: a patch's `null`s delete nothing.
    let live = |v: &[u8]| jp_kind(v) == b'"';
    let has_from_members = !fnull && jp_members(fv).any(|(_, v)| live(v));
    // `to`, merged: its members `from` does not name, in order, then every
    // member of `from` encoded.
    let write_to = |o: &mut JpOut, scratch: &mut [u8]| -> Result<(), JpErr> {
        o.c(b'{')?;
        let mut first = true;
        if let Some(t) = tv.filter(|t| !jp_is_null(t)) {
            for (k, v) in jp_members(t) {
                let shadowed = !fnull && jp_members(fv).any(|(fk, fv)| fk == k && live(fv));
                if !shadowed {
                    o.key(&mut first, k)?;
                    o.put(jp_trim(v))?;
                }
            }
        }
        if !fnull {
            for (k, v) in jp_members(fv).filter(|(_, v)| live(v)) {
                let room = scratch.len().min(SD_VALUE_MAX);
                let n = sd_unescape(v, &mut scratch[..room]).ok_or(JpErr::Full)?;
                o.key(&mut first, k)?;
                sd_b64(o, &scratch[..n])?;
            }
        }
        o.c(b'}')
    };
    o.c(b'{')?;
    let mut first = true;
    let mut to_done = false;
    for (k, v) in jp_members(doc) {
        if sd_key_is(k, from) {
            // `to` takes `from`'s place when the document had none.
            if tv.is_none() && has_from_members {
                o.key(&mut first, to)?;
                write_to(o, scratch)?;
                to_done = true;
            }
            continue;
        }
        if sd_key_is(k, to) {
            if !to_done {
                if jp_is_null(v) && !has_from_members {
                    o.key(&mut first, k)?;
                    o.put(jp_trim(v))?;
                } else {
                    o.key(&mut first, k)?;
                    write_to(o, scratch)?;
                }
                to_done = true;
            }
            continue;
        }
        o.key(&mut first, k)?;
        o.put(jp_trim(v))?;
    }
    o.c(b'}')?;
    Ok(true)
}

/// A JSON Patch (RFC 6902) with its `add` of the whole `from` member folded:
/// `{"op":"add","path":"/<from>","value":{…}}` becomes an `add` of each
/// member under `/<to>/`, base64-encoded — preceded by an `add` of `/<to>`
/// itself when the stored object has none (`has_to`). Every other operation
/// is kept: one naming a path below `/<from>` finds no such member in a
/// stored object (it is never stored), and fails as upstream's does.
/// `Ok(false)` when no operation adds `/<from>`.
fn sd_fold_json_patch(
    patch: &[u8],
    from: &[u8],
    to: &[u8],
    has_to: bool,
    o: &mut JpOut,
    scratch: &mut [u8],
) -> Result<bool, JpErr> {
    if jp_kind(patch) != b'[' {
        return Err(JpErr::Malformed);
    }
    let names_from =
        |path: &[u8]| path.len() == from.len() + 1 && path[0] == b'/' && &path[1..] == from;
    let mut any = false;
    for op in jp_elems(patch) {
        let (Some(kind), Some(path)) = (jp_str(op, b"op"), jp_str(op, b"path")) else {
            continue;
        };
        if kind == b"add" && names_from(path) {
            any = true;
        }
    }
    if !any {
        return Ok(false);
    }
    o.c(b'[')?;
    let mut first = true;
    let mut to_exists = has_to;
    for op in jp_elems(patch) {
        let kind = jp_str(op, b"op").unwrap_or(&[]);
        let path = jp_str(op, b"path").unwrap_or(&[]);
        if !(kind == b"add" && names_from(path)) {
            o.elem(&mut first)?;
            o.put(jp_trim(op))?;
            continue;
        }
        let Some(v) = jp_get(op, b"value") else {
            return Err(JpErr::Malformed);
        };
        if jp_kind(v) != b'{' {
            return Err(JpErr::Malformed);
        }
        if !to_exists {
            o.elem(&mut first)?;
            o.put(b"{\"op\":\"add\",\"path\":\"/")?;
            o.put(to)?;
            o.put(b"\",\"value\":{}}")?;
            to_exists = true;
        }
        for (k, mv) in jp_members(v) {
            if jp_kind(mv) != b'"' {
                return Err(JpErr::Malformed);
            }
            let room = scratch.len().min(SD_VALUE_MAX);
            let n = sd_unescape(mv, &mut scratch[..room]).ok_or(JpErr::Full)?;
            o.elem(&mut first)?;
            o.put(b"{\"op\":\"add\",\"path\":\"/")?;
            o.put(to)?;
            o.c(b'/')?;
            // A JSON Pointer escapes `~` and `/` in a member name.
            for &c in k {
                match c {
                    b'~' => o.put(b"~0")?,
                    b'/' => o.put(b"~1")?,
                    _ => o.c(c)?,
                }
            }
            o.put(b"\",\"value\":")?;
            sd_b64(o, &scratch[..n])?;
            o.c(b'}')?;
        }
    }
    o.c(b']')?;
    Ok(true)
}
