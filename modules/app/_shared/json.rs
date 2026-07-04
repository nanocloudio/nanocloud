// Shared no_std JSON navigation for nanocloud fmods — included via
// `include!("../_shared/json.rs")`. No allocator: it reads scalar values out of a
// JSON object byte slice by path, borrowing the underlying bytes.
//
// The control plane stores k8s objects as JSON. A reconciler that needs a field
// reads it with `j_path(obj, &[b"spec", b"replicas"])` and gets the scalar
// token bytes (a string's content without quotes, or a number/bool literal).
// Array elements are addressed by a decimal-index segment
// (`&[b"spec", b"template", b"spec", b"containers", b"0", b"image"]`).
//
// These helpers are `#[allow(dead_code)]` at the module level already (every fmod
// carries the blanket allow), so an fmod that includes this but uses only some of
// it does not warn.

fn j_is_ws(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\n' || c == b'\r'
}

fn j_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && j_is_ws(b[i]) {
        i += 1;
    }
    i
}

/// Skip one JSON value starting at `i` (after leading ws); return the index just
/// past it.
fn j_skip(b: &[u8], i: usize) -> usize {
    let i = j_ws(b, i);
    if i >= b.len() {
        return i;
    }
    match b[i] {
        b'"' => {
            let mut j = i + 1;
            while j < b.len() {
                if b[j] == b'\\' {
                    j += 2;
                    continue;
                }
                if b[j] == b'"' {
                    return j + 1;
                }
                j += 1;
            }
            j
        }
        b'{' => j_skip_container(b, i, b'{', b'}'),
        b'[' => j_skip_container(b, i, b'[', b']'),
        _ => {
            let mut j = i;
            while j < b.len() && b[j] != b',' && b[j] != b'}' && b[j] != b']' && !j_is_ws(b[j]) {
                j += 1;
            }
            j
        }
    }
}

fn j_skip_container(b: &[u8], i: usize, open: u8, close: u8) -> usize {
    let mut depth = 0i32;
    let mut j = i;
    let mut in_str = false;
    while j < b.len() {
        let c = b[j];
        if in_str {
            if c == b'\\' {
                j += 2;
                continue;
            }
            if c == b'"' {
                in_str = false;
            }
        } else if c == b'"' {
            in_str = true;
        } else if c == open {
            depth += 1;
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                return j + 1;
            }
        }
        j += 1;
    }
    j
}

/// In an object at `i` (`{`), find `key`; return the index of its value (after
/// the colon + ws), or None.
fn j_obj_get(b: &[u8], i: usize, key: &[u8]) -> Option<usize> {
    let i = j_ws(b, i);
    if i >= b.len() || b[i] != b'{' {
        return None;
    }
    let mut j = i + 1;
    loop {
        j = j_ws(b, j);
        if j >= b.len() || b[j] == b'}' {
            return None;
        }
        if b[j] != b'"' {
            return None;
        }
        let ks = j + 1;
        let mut ke = ks;
        while ke < b.len() && b[ke] != b'"' {
            ke += 1;
        }
        let k = &b[ks..ke];
        j = j_ws(b, ke + 1);
        if j >= b.len() || b[j] != b':' {
            return None;
        }
        j = j_ws(b, j + 1);
        if k == key {
            return Some(j);
        }
        j = j_skip(b, j);
        j = j_ws(b, j);
        if j < b.len() && b[j] == b',' {
            j += 1;
            continue;
        }
        return None;
    }
}

/// In an array at `i` (`[`), return the start index of element `n`, or None.
fn j_arr_get(b: &[u8], i: usize, n: usize) -> Option<usize> {
    let i = j_ws(b, i);
    if i >= b.len() || b[i] != b'[' {
        return None;
    }
    let mut j = i + 1;
    let mut idx = 0usize;
    loop {
        j = j_ws(b, j);
        if j >= b.len() || b[j] == b']' {
            return None;
        }
        if idx == n {
            return Some(j);
        }
        j = j_skip(b, j);
        j = j_ws(b, j);
        if j < b.len() && b[j] == b',' {
            j += 1;
            idx += 1;
            continue;
        }
        return None;
    }
}

fn j_parse_usize(b: &[u8]) -> Option<usize> {
    if b.is_empty() {
        return None;
    }
    let mut n = 0usize;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (c - b'0') as usize;
    }
    Some(n)
}

/// Return the scalar token at `i`: a string's content (no quotes) or a
/// number/bool/null literal's bytes.
fn j_scalar(b: &[u8], i: usize) -> &[u8] {
    let i = j_ws(b, i);
    if i >= b.len() {
        return &b[b.len()..];
    }
    if b[i] == b'"' {
        let s = i + 1;
        let mut e = s;
        while e < b.len() && b[e] != b'"' {
            if b[e] == b'\\' {
                e += 1;
            }
            e += 1;
        }
        &b[s..e.min(b.len())]
    } else {
        let mut e = i;
        while e < b.len() && b[e] != b',' && b[e] != b'}' && b[e] != b']' && !j_is_ws(b[e]) {
            e += 1;
        }
        &b[i..e]
    }
}

/// Navigate `path` (object keys, or decimal indices for arrays) from the root of
/// the JSON object `b`; return the scalar token at the end, or None.
fn j_path<'a>(b: &'a [u8], path: &[&[u8]]) -> Option<&'a [u8]> {
    let mut cur = 0usize;
    for seg in path {
        let v = j_ws(b, cur);
        if v >= b.len() {
            return None;
        }
        if b[v] == b'{' {
            cur = j_obj_get(b, v, seg)?;
        } else if b[v] == b'[' {
            cur = j_arr_get(b, v, j_parse_usize(seg)?)?;
        } else {
            return None;
        }
    }
    Some(j_scalar(b, cur))
}

/// Return the sub-object/sub-array bytes (with braces/brackets) at `path`, e.g.
/// `spec.template` — for embedding a nested object verbatim into a child object.
/// None if the path is missing or the value is a scalar.
fn j_sub<'a>(b: &'a [u8], path: &[&[u8]]) -> Option<&'a [u8]> {
    let mut cur = 0usize;
    for seg in path {
        let v = j_ws(b, cur);
        if v >= b.len() {
            return None;
        }
        if b[v] == b'{' {
            cur = j_obj_get(b, v, seg)?;
        } else if b[v] == b'[' {
            cur = j_arr_get(b, v, j_parse_usize(seg)?)?;
        } else {
            return None;
        }
    }
    let s = j_ws(b, cur);
    if s < b.len() && (b[s] == b'{' || b[s] == b'[') {
        let e = j_skip(b, s);
        Some(&b[s..e])
    } else {
        None
    }
}

/// Convenience: parse the scalar at `path` as a u32 (0 if missing/non-numeric).
fn j_u32(b: &[u8], path: &[&[u8]]) -> u32 {
    let mut n = 0u32;
    if let Some(v) = j_path(b, path) {
        for &c in v {
            if c.is_ascii_digit() {
                n = n.saturating_mul(10).saturating_add((c - b'0') as u32);
            } else {
                break;
            }
        }
    }
    n
}

/// Internal copy helper (self-contained so this file needs no `append` from the
/// including fmod).
fn j_cp(dst: &mut [u8], at: usize, src: &[u8]) -> usize {
    let n = src.len().min(dst.len().saturating_sub(at));
    dst[at..at + n].copy_from_slice(&src[..n]);
    at + n
}

/// Insert `field` (raw JSON like `"nodeName":"node-1"`) into the object value of
/// top-level `key`, just after its `{`. Returns the length written to `out`, or 0
/// if `key` is absent or not an object. Caller ensures the field isn't already
/// present (e.g. via `j_path`), so this never duplicates.
fn j_insert(obj: &[u8], key: &[u8], field: &[u8], out: &mut [u8]) -> usize {
    let vstart = match j_obj_get(obj, 0, key) {
        Some(i) => i,
        None => return 0,
    };
    let v = j_ws(obj, vstart);
    if v >= obj.len() || obj[v] != b'{' {
        return 0;
    }
    let after = j_ws(obj, v + 1);
    let empty = after < obj.len() && obj[after] == b'}';
    let mut p = j_cp(out, 0, &obj[..=v]);
    p = j_cp(out, p, field);
    if !empty {
        p = j_cp(out, p, b",");
    }
    j_cp(out, p, &obj[v + 1..])
}

/// Set a top-level scalar `key` to `val` (raw JSON token — number/bool, or quote
/// it yourself for a string): replace it in place if present, else insert it.
/// Returns the length written to `out`.
fn j_set_top(obj: &[u8], key: &[u8], val: &[u8], out: &mut [u8]) -> usize {
    if let Some(vstart) = j_obj_get(obj, 0, key) {
        // Replace the existing value token with `val`.
        let vend = j_skip(obj, vstart);
        let mut p = j_cp(out, 0, &obj[..vstart]);
        p = j_cp(out, p, val);
        j_cp(out, p, &obj[vend..])
    } else {
        // Insert `"key":val` at the front of the root object.
        let root = j_ws(obj, 0);
        if root >= obj.len() || obj[root] != b'{' {
            return j_cp(out, 0, obj);
        }
        let after = j_ws(obj, root + 1);
        let empty = after < obj.len() && obj[after] == b'}';
        let mut p = j_cp(out, 0, &obj[..=root]);
        p = j_cp(out, p, b"\"");
        p = j_cp(out, p, key);
        p = j_cp(out, p, b"\":");
        p = j_cp(out, p, val);
        if !empty {
            p = j_cp(out, p, b",");
        }
        j_cp(out, p, &obj[root + 1..])
    }
}

// ── Array-free navigators (PIC-safe) ────────────────────────────────────────
// A path literal `&[b"a", b"b"]` const-promotes to a static nested pointer table
// whose inner pointers are NOT relocated in a PIC module → dereferencing them
// segfaults. These wrappers take the keys as separate arguments and build the
// path slice from those (runtime) params, so nothing const-promotes. ALWAYS use
// these, never `j_path(&[...])` with literal segments.

fn j_get1<'a>(b: &'a [u8], k1: &[u8]) -> Option<&'a [u8]> {
    j_path(b, &[k1])
}
fn j_get2<'a>(b: &'a [u8], k1: &[u8], k2: &[u8]) -> Option<&'a [u8]> {
    j_path(b, &[k1, k2])
}
fn j_get3<'a>(b: &'a [u8], k1: &[u8], k2: &[u8], k3: &[u8]) -> Option<&'a [u8]> {
    j_path(b, &[k1, k2, k3])
}
fn j_get4<'a>(b: &'a [u8], k1: &[u8], k2: &[u8], k3: &[u8], k4: &[u8]) -> Option<&'a [u8]> {
    j_path(b, &[k1, k2, k3, k4])
}
fn j_sub1<'a>(b: &'a [u8], k1: &[u8]) -> Option<&'a [u8]> {
    j_sub(b, &[k1])
}
fn j_sub2<'a>(b: &'a [u8], k1: &[u8], k2: &[u8]) -> Option<&'a [u8]> {
    j_sub(b, &[k1, k2])
}
fn j_sub3<'a>(b: &'a [u8], k1: &[u8], k2: &[u8], k3: &[u8]) -> Option<&'a [u8]> {
    j_sub(b, &[k1, k2, k3])
}
fn j_u32_1(b: &[u8], k1: &[u8]) -> u32 {
    j_u32(b, &[k1])
}
fn j_u32_2(b: &[u8], k1: &[u8], k2: &[u8]) -> u32 {
    j_u32(b, &[k1, k2])
}

/// Set a nested scalar `k1.k2` to `val` (raw JSON token): replace k2 within the
/// object value of top-level k1. Returns len in `out`, or 0 if k1 isn't an
/// object. Inserts k2 if absent.
fn j_set2(obj: &[u8], k1: &[u8], k2: &[u8], val: &[u8], out: &mut [u8]) -> usize {
    let s = match j_obj_get(obj, 0, k1) {
        Some(i) => j_ws(obj, i),
        None => return 0,
    };
    if s >= obj.len() || obj[s] != b'{' {
        return 0;
    }
    let e = j_skip(obj, s); // end of k1's object value
                            // Rewrite the sub-object with k2 set, into a scratch, then splice.
    let mut sub = [0u8; 4096];
    let sl = j_set_top(&obj[s..e], k2, val, &mut sub);
    let mut p = j_cp(out, 0, &obj[..s]);
    p = j_cp(out, p, &sub[..sl]);
    j_cp(out, p, &obj[e..])
}

/// True iff every key:scalar in `sub` (a JSON object) is present in `sup` (a JSON
/// object) with an equal scalar value. An empty `sub` returns false (a k8s empty
/// selector matches nothing). Used for label-selector matching.
fn j_obj_subset(sub: &[u8], sup: &[u8]) -> bool {
    let i = j_ws(sub, 0);
    if i >= sub.len() || sub[i] != b'{' {
        return false;
    }
    let mut j = i + 1;
    let mut any = false;
    loop {
        j = j_ws(sub, j);
        if j >= sub.len() || sub[j] == b'}' {
            return any;
        }
        if sub[j] != b'"' {
            return false;
        }
        let ks = j + 1;
        let mut ke = ks;
        while ke < sub.len() && sub[ke] != b'"' {
            ke += 1;
        }
        let key = &sub[ks..ke];
        j = j_ws(sub, ke + 1);
        if j >= sub.len() || sub[j] != b':' {
            return false;
        }
        j = j_ws(sub, j + 1);
        let val = j_scalar(sub, j);
        match j_obj_get(sup, 0, key) {
            Some(vi) => {
                if j_scalar(sup, vi) != val {
                    return false;
                }
            }
            None => return false,
        }
        any = true;
        j = j_skip(sub, j);
        j = j_ws(sub, j);
        if j < sub.len() && sub[j] == b',' {
            j += 1;
            continue;
        }
        return any;
    }
}

/// Return the `n`-th element bytes of a JSON array `b` (`[...]`), or None. The
/// element may be an object, array, or scalar (with its surrounding delimiters
/// for containers). Iterate by calling with n = 0, 1, … until None.
fn j_idx(b: &[u8], n: usize) -> Option<&[u8]> {
    let s = j_arr_get(b, 0, n)?;
    let e = j_skip(b, s);
    Some(&b[s..e])
}
