// JSON queries past a dotted path — the two shapes a flat record needs from an
// array. `include!`d after `json.rs` and `pathmod.rs` by the modules that
// project documents (store_source, http_fetch).
//
//   a[].b[!mod]      COLLECT: member `b` of every element of the array at `a`,
//                    `,`-joined, one entry per element — `-` where the element
//                    has no such member — each `%`-escaped (`%25`, `%2C`,
//                    `%3B`, the `!argv` grammar) and, with `!mod`, decoded
//                    first. `a[]` alone collects the elements themselves (an
//                    argv array). Alignment is the point: a port list and its
//                    protocol list index the same element, and "no peers" (an
//                    empty list) stays distinct from "a peer this cannot name"
//                    (`-`).
//   a[]#             COUNT: the number of elements, as decimal.
//   a[k=v&k2=v2].b   SELECT: member `b` of the FIRST element whose members
//                    equal the given values — an image index's entry for one
//                    platform. `k?=v` is a member absent or equal to `v`
//                    (`platform.variant?=v8`: an arm64 entry that names no
//                    variant, or v8). Empty when none matches.
//
// A dotted segment may be a decimal array index (`layers.0.digest`), as
// everywhere in `json.rs`. Anything else is a plain path. Bounded: at most
// `JQ_SEGS` segments, and output that does not fit is refused, never cut.

const JQ_SEGS: usize = 8;

fn jq_split<'a>(path: &'a [u8], segs: &mut [&'a [u8]; JQ_SEGS]) -> usize {
    let mut n = 0usize;
    for part in path.split(|&c| c == b'.') {
        if part.is_empty() || n == JQ_SEGS {
            continue;
        }
        segs[n] = part;
        n += 1;
    }
    n
}

/// A plain dotted path: a subtree as its bytes, a scalar as its token.
fn jq_plain<'a>(doc: &'a [u8], path: &[u8]) -> Option<&'a [u8]> {
    if path.is_empty() {
        return Some(doc);
    }
    let mut segs: [&[u8]; JQ_SEGS] = [&[]; JQ_SEGS];
    let n = jq_split(path, &mut segs);
    j_sub(doc, &segs[..n]).or_else(|| j_path(doc, &segs[..n]))
}

/// `%`-escape `src` into `out` at `at` (`%`, `,`, `;`). None when it overflows.
fn jq_esc(out: &mut [u8], mut at: usize, src: &[u8]) -> Option<usize> {
    for &b in src {
        let rep: &[u8] = match b {
            b'%' => b"%25",
            b',' => b"%2C",
            b';' => b"%3B",
            _ => core::slice::from_ref(&b),
        };
        if at + rep.len() > out.len() {
            return None;
        }
        out[at..at + rep.len()].copy_from_slice(rep);
        at += rep.len();
    }
    Some(at)
}

/// Evaluate `query` over `doc` into `out`. `Some(len)` (possibly 0 — absent),
/// or None when the answer does not fit `out`.
#[allow(
    clippy::manual_contains,
    reason = "contains() on a byte slice links memchr, which a PIC module cannot resolve"
)]
fn jq(doc: &[u8], query: &[u8], out: &mut [u8]) -> Option<usize> {
    let (query, modifier) = match query.iter().rposition(|&c| c == b'!') {
        Some(i) if !query[i..].iter().any(|&c| c == b']') => (&query[..i], Some(&query[i + 1..])),
        _ => (query, None),
    };
    let decode = |v: &[u8], buf: &mut [u8; 512]| -> usize {
        match modifier {
            Some(m) => apply_mod(m, v, buf),
            None => {
                let n = v.len().min(buf.len());
                buf[..n].copy_from_slice(&v[..n]);
                n
            }
        }
    };
    let Some(open) = query.iter().position(|&c| c == b'[') else {
        let v = jq_plain(doc, query).unwrap_or(&[]);
        let mut buf = [0u8; 512];
        if modifier.is_some() {
            let n = decode(v, &mut buf);
            if n > out.len() {
                return None;
            }
            out[..n].copy_from_slice(&buf[..n]);
            return Some(n);
        }
        if v.len() > out.len() {
            return None;
        }
        out[..v.len()].copy_from_slice(v);
        return Some(v.len());
    };
    let close = open + query[open..].iter().position(|&c| c == b']')?;
    let arr_path = &query[..open];
    let pred = &query[open + 1..close];
    let rest = &query[close + 1..];
    let sub = rest.strip_prefix(b".").unwrap_or(rest);
    let Some(arr) = jq_plain(doc, arr_path) else {
        return Some(0);
    };
    let mut i = 0usize;
    if pred.is_empty() && sub == b"#" {
        // COUNT: `a[]#`
        while j_idx(arr, i).is_some() {
            i += 1;
        }
        let mut tmp = [0u8; 20];
        let mut k = tmp.len();
        let mut v = i;
        loop {
            k -= 1;
            tmp[k] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        let n = tmp.len() - k;
        if n > out.len() {
            return None;
        }
        out[..n].copy_from_slice(&tmp[k..]);
        return Some(n);
    }
    if pred.is_empty() {
        // COLLECT
        let mut len = 0usize;
        while let Some(el) = j_idx(arr, i) {
            let v = if sub.is_empty() {
                let s = j_ws(el, 0);
                if el.get(s) == Some(&b'"') {
                    j_scalar(el, 0)
                } else {
                    el
                }
            } else {
                jq_plain(el, sub).unwrap_or(&[])
            };
            let mut buf = [0u8; 512];
            // An ARRAY member (`a[].list`) is its scalars joined with `+`, so
            // it stays ONE entry of the collected list — aligned with the
            // other members collected from the same elements.
            let n = if v.first() == Some(&b'[') {
                let mut k = 0usize;
                let mut len = 0usize;
                while let Some(x) = j_idx(v, k) {
                    let item = j_scalar(x, 0);
                    if k > 0 {
                        *buf.get_mut(len)? = b'+';
                        len += 1;
                    }
                    let dst = buf.get_mut(len..len + item.len())?;
                    for (d, c) in dst.iter_mut().zip(item.iter()) {
                        *d = *c;
                    }
                    len += item.len();
                    k += 1;
                }
                len
            } else {
                decode(v, &mut buf)
            };
            if i > 0 {
                if len + 1 > out.len() {
                    return None;
                }
                out[len] = b',';
                len += 1;
            }
            len = if n == 0 {
                if len + 1 > out.len() {
                    return None;
                }
                out[len] = b'-';
                len + 1
            } else {
                jq_esc(out, len, &buf[..n])?
            };
            i += 1;
        }
        return Some(len);
    }
    // SELECT
    while let Some(el) = j_idx(arr, i) {
        i += 1;
        let all =
            pred.split(|&c| c == b'&')
                .all(|cond| match cond.iter().position(|&c| c == b'=') {
                    // `k?=v`: the member absent, or equal (an index entry's
                    // `platform.variant`, which most entries leave out).
                    Some(e) if e > 0 && cond.get(e - 1) == Some(&b'?') => {
                        match jq_plain(el, &cond[..e - 1]) {
                            None => true,
                            Some(v) => v == &cond[e + 1..],
                        }
                    }
                    Some(e) => jq_plain(el, &cond[..e]) == Some(&cond[e + 1..]),
                    None => false,
                });
        if all {
            let v = jq_plain(el, sub).unwrap_or(&[]);
            let mut buf = [0u8; 512];
            let n = decode(v, &mut buf);
            if n > out.len() {
                return None;
            }
            out[..n].copy_from_slice(&buf[..n]);
            return Some(n);
        }
    }
    Some(0)
}
