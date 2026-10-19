// Kubernetes label selectors — `include!`d after json.rs and jsonpatch.rs by
// store_source (the scheduler's pick, sched.rs) and store_effect (`count_sel`,
// a COUNT whose children are judged by a selector).
//
// A LabelSelector (matchLabels, and matchExpressions In, NotIn, Exists,
// DoesNotExist) against a label set, with upstream's reading: an empty
// selector selects everything; an absent (or null) one nothing.

/// A JSON string's content (quotes stripped); other values as written.
fn sk_unq(v: &[u8]) -> &[u8] {
    let v = jp_trim(v);
    if v.len() >= 2 && v.first() == Some(&b'"') && v.last() == Some(&b'"') {
        v.get(1..v.len() - 1).unwrap_or(&[])
    } else {
        v
    }
}

/// Member `k` of object `o`, unquoted ("" when absent).
fn sk_str<'a>(o: &'a [u8], k: &[u8]) -> &'a [u8] {
    jp_get(o, k).map(sk_unq).unwrap_or(&[])
}

fn sk_dec(b: &[u8]) -> u64 {
    let mut v = 0u64;
    for &c in b {
        if !c.is_ascii_digit() {
            break;
        }
        v = v.saturating_mul(10).saturating_add(u64::from(c - b'0'));
    }
    v
}

fn sk_int(b: &[u8]) -> i64 {
    let b = sk_unq(b);
    match b.first() {
        Some(&b'-') => -(sk_dec(b.get(1..).unwrap_or(&[])) as i64),
        _ => sk_dec(b) as i64,
    }
}

/// A label value in `labels` (an object), unquoted; None when absent.
fn sk_label<'a>(labels: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    jp_get(labels, key).map(sk_unq)
}

/// Whether `v` is among the strings of the array `values`.
fn sk_in(values: &[u8], v: &[u8]) -> bool {
    jp_elems(values).any(|e| sk_unq(e) == v)
}

/// A LabelSelector against a label set: matchLabels a subset, every
/// matchExpressions requirement holding. An empty selector matches every
/// object; an absent one (None) none — as upstream reads an affinity term's.
fn sk_selects(sel: Option<&[u8]>, labels: &[u8]) -> bool {
    let Some(sel) = sel else {
        return false;
    };
    if jp_is_null(sel) {
        return false;
    }
    if let Some(ml) = jp_get(sel, b"matchLabels") {
        for (k, v) in jp_members(ml) {
            if sk_label(labels, k) != Some(sk_unq(v)) {
                return false;
            }
        }
    }
    if let Some(mx) = jp_get(sel, b"matchExpressions") {
        for e in jp_elems(mx) {
            let key = sk_str(e, b"key");
            let op = sk_str(e, b"operator");
            let vals = jp_get(e, b"values").unwrap_or(b"[]");
            let got = sk_label(labels, key);
            let ok = match op {
                b"In" => got.is_some_and(|g| sk_in(vals, g)),
                b"NotIn" => got.is_none_or(|g| !sk_in(vals, g)),
                b"Exists" => got.is_some(),
                b"DoesNotExist" => got.is_none(),
                _ => false,
            };
            if !ok {
                return false;
            }
        }
    }
    true
}
