// A server-side table — `include!`d after json.rs, jsonpatch.rs, docschema.rs
// and the connector's `put`.
//
// The columns are the graph's (`table`): per kind, `Kind=Header[/format]:
// <cell>,…`, entries `;`-separated; a `*` entry serves every kind not named.
// The envelope is the schema's (docschema.rs):
//
//   table.head=<tpl>   {{cols}}: the column definitions
//   table.col=<tpl>    one definition: {{name}}, {{format}}
//   table.row=<tpl>    one row: {{cells}}, and the object's paths as holes
//   table.end=<text>   what closes a table that is not a list (a watch event)
//
// A cell is `<path>|<default>` — the value at a dotted path, the default when
// it is absent or empty (`<none>` without one) — or an expression: literal
// text with `@fn(args)` parts:
//
//   @v(path|default)   the value at path
//   @n(path)           how many elements the list at path has
//   @n(path?k=v)       how many of them have member k equal to v
//   @sum(path:k)       the sum of member k over them
//   @age(path)         the time since the RFC 3339 timestamp at path, as
//                      kubectl's HumanDuration prints it
//
// Every cell is a string, pre-rendered, so a client prints it as sent.

/// The column spec for `kind` (`Header:cell,…`), else the `*` entry's.
fn tbl_spec<'a>(spec: &'a [u8], kind: &[u8]) -> &'a [u8] {
    let mut any: &[u8] = &[];
    for ent in spec.split(|&b| b == b';') {
        if let Some(eq) = ent.iter().position(|&b| b == b'=') {
            if &ent[..eq] == kind {
                return &ent[eq + 1..];
            }
            if &ent[..eq] == b"*" {
                any = &ent[eq + 1..];
            }
        }
    }
    any
}

/// A column entry's header, format and cell.
fn tbl_col(c: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let (head, cell) = match c.iter().position(|&b| b == b':') {
        Some(i) => (&c[..i], &c[i + 1..]),
        None => (c, &[][..]),
    };
    match head.iter().position(|&b| b == b'/') {
        Some(i) => (&head[..i], &head[i + 1..], cell),
        None => (head, &[][..], cell),
    }
}

/// `tpl` split around the hole `{{<name>}}`.
fn tbl_split<'a>(tpl: &'a [u8], hole: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let at = tpl.windows(hole.len()).position(|w| w == hole)?;
    Some((&tpl[..at], &tpl[at + hole.len()..]))
}

/// Append `f`'s output (it fills the slice it is given) to `out` at `*at`.
fn tbl_with(out: &mut [u8], at: &mut usize, f: impl FnOnce(&mut [u8]) -> Option<usize>) -> bool {
    let Some(dst) = out.get_mut(*at..) else {
        return false;
    };
    match f(dst) {
        Some(n) => {
            *at += n;
            true
        }
        None => false,
    }
}

/// The table's head through the rows' opening.
fn tbl_head(sch: &[u8], cols: &[u8], out: &mut [u8], at: &mut usize) -> bool {
    let Some((pre, post)) = tbl_split(sch_get(sch, b"table.head"), b"{{cols}}") else {
        return false;
    };
    let col = sch_get(sch, b"table.col");
    let mut ok = put(out, at, pre);
    let mut first = true;
    for c in cols.split(|&b| b == b',') {
        if c.is_empty() {
            continue;
        }
        let (name, format, _) = tbl_col(c);
        let vars = [(&b"name"[..], name), (&b"format"[..], format)];
        ok = ok && (first || put(out, at, b",")) && tbl_with(out, at, |d| sch_fill(col, &vars, d));
        first = false;
    }
    ok && put(out, at, post)
}

/// What closes a table that is not a list.
fn tbl_end(sch: &[u8], out: &mut [u8], at: &mut usize) -> bool {
    put(out, at, sch_get(sch, b"table.end"))
}

/// Decimal of `v`.
fn tbl_dec(v: u64, out: &mut [u8], at: &mut usize) -> bool {
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
    let mut ok = true;
    while n > 0 {
        n -= 1;
        ok = ok && put(out, at, &d[n..n + 1]);
    }
    ok
}

/// Unix seconds of an RFC 3339 `YYYY-MM-DDTHH:MM:SS…` timestamp.
fn tbl_unix(ts: &[u8]) -> Option<i64> {
    if ts.len() < 19 {
        return None;
    }
    let num = |a: usize, b: usize| -> Option<i64> {
        let mut v = 0i64;
        for &c in ts.get(a..b)? {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as i64;
        }
        Some(v)
    };
    let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (hh, mm, ss) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    // days from civil (Howard Hinnant)
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(days * 86400 + hh * 3600 + mm * 60 + ss)
}

/// kubectl's HumanDuration of `secs`.
fn tbl_age(secs: i64, out: &mut [u8], at: &mut usize) -> bool {
    if secs < 0 {
        return put(out, at, b"0s");
    }
    let s = secs as u64;
    let unit = |out: &mut [u8], at: &mut usize, a: u64, ua: &[u8], b: u64, ub: &[u8]| -> bool {
        tbl_dec(a, out, at)
            && put(out, at, ua)
            && (b == 0 || (tbl_dec(b, out, at) && put(out, at, ub)))
    };
    if s < 120 {
        return unit(out, at, s, b"s", 0, b"");
    }
    let m = s / 60;
    if m < 10 {
        return unit(out, at, m, b"m", s % 60, b"s");
    }
    if m < 180 {
        return unit(out, at, m, b"m", 0, b"");
    }
    let h = m / 60;
    if h < 8 {
        return unit(out, at, h, b"h", m % 60, b"m");
    }
    if h < 48 {
        return unit(out, at, h, b"h", 0, b"");
    }
    let d = h / 24;
    if h < 24 * 8 {
        return unit(out, at, d, b"d", h % 24, b"h");
    }
    if d < 365 * 2 {
        return unit(out, at, d, b"d", 0, b"");
    }
    let y = d / 365;
    if y < 8 {
        return unit(out, at, y, b"y", d % 365, b"d");
    }
    unit(out, at, y, b"y", 0, b"")
}

/// The value at the dotted `path` of `obj` (a string's content unquoted).
fn tbl_at<'a>(obj: &'a [u8], path: &[u8]) -> Option<&'a [u8]> {
    let mut segs: [&[u8]; 6] = [&[]; 6];
    let n = split_dots(path, &mut segs);
    j_path(obj, &segs[..n])
}

/// The array (or object) at the dotted `path` of `obj`, brackets included —
/// `tbl_at` sees scalars only, and a list column reads a list.
fn tbl_sub<'a>(obj: &'a [u8], path: &[u8]) -> Option<&'a [u8]> {
    let mut segs: [&[u8]; 6] = [&[]; 6];
    let n = split_dots(path, &mut segs);
    j_sub(obj, &segs[..n])
}

/// `path|default`: the value, else the default (`none` without one).
fn tbl_value(arg: &[u8], none: &[u8], obj: &[u8], out: &mut [u8], at: &mut usize) -> bool {
    let (path, dflt) = match arg.iter().position(|&b| b == b'|') {
        Some(i) => (&arg[..i], &arg[i + 1..]),
        None => (arg, none),
    };
    put(
        out,
        at,
        tbl_at(obj, path).filter(|v| !v.is_empty()).unwrap_or(dflt),
    )
}

/// One `@fn(arg)` of a cell expression.
fn tbl_fn(f: &[u8], arg: &[u8], obj: &[u8], now_ms: u64, out: &mut [u8], at: &mut usize) -> bool {
    match f {
        b"v" => tbl_value(arg, b"", obj, out, at),
        b"n" => {
            let (path, want) = match arg.iter().position(|&b| b == b'?') {
                Some(i) => (&arg[..i], Some(&arg[i + 1..])),
                None => (arg, None),
            };
            let list = tbl_sub(obj, path).unwrap_or(b"[]");
            let n = jp_elems(list)
                .filter(|e| match want {
                    None => true,
                    Some(kv) => match kv.iter().position(|&b| b == b'=') {
                        Some(i) => j_get1(e, &kv[..i]) == Some(&kv[i + 1..]),
                        None => false,
                    },
                })
                .count();
            tbl_dec(n as u64, out, at)
        }
        b"sum" => {
            let (path, member) = match arg.iter().position(|&b| b == b':') {
                Some(i) => (&arg[..i], &arg[i + 1..]),
                None => return false,
            };
            let list = tbl_sub(obj, path).unwrap_or(b"[]");
            let sum: u64 = jp_elems(list)
                .map(|e| j_get1(e, member).map(parse_dec_u64).unwrap_or(0))
                .sum();
            tbl_dec(sum, out, at)
        }
        b"age" => match tbl_at(obj, arg).and_then(tbl_unix) {
            Some(t) => tbl_age((now_ms / 1000) as i64 - t, out, at),
            None => put(out, at, b"<unknown>"),
        },
        // kubectl's node STATUS: the Ready condition's status, in words — a
        // Node carries no phase, and a missing condition is Unknown.
        b"ready" => {
            // `j_get1` hands a string back with its quotes.
            fn unq(v: &[u8]) -> &[u8] {
                if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
                    &v[1..v.len() - 1]
                } else {
                    v
                }
            }
            let list = tbl_sub(obj, arg).unwrap_or(b"[]");
            let status = jp_elems(list)
                .find(|e| j_get1(e, b"type").map(unq) == Some(b"Ready"))
                .and_then(|e| j_get1(e, b"status"))
                .map(unq);
            put(
                out,
                at,
                match status {
                    Some(b"True") => b"Ready",
                    Some(b"False") => b"NotReady",
                    _ => b"Unknown",
                },
            )
        }
        // kubectl's pod STATUS (printPod): the phase, or the Pod's own
        // reason, overridden by the last container's waiting reason
        // (ContainerCreating, ImagePullBackOff, CrashLoopBackOff, …) or its
        // terminated reason (Completed, Error; `ExitCode:<n>` without one);
        // Terminating once it is being deleted.
        b"podstatus" => {
            let reason = tbl_pod_reason(obj);
            put(out, at, reason.as_slice())
        }
        _ => false,
    }
}

/// What [`tbl_fn`]'s `@podstatus()` prints: borrowed from `obj`, or one of
/// its own words, or `ExitCode:<n>` built here.
enum PodReason<'a> {
    Of(&'a [u8]),
    Exit([u8; 32], usize),
}

impl PodReason<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            PodReason::Of(b) => b,
            PodReason::Exit(b, n) => b.get(..*n).unwrap_or(&[]),
        }
    }
}

/// The non-empty scalar at the dotted `path` of one list element.
fn tbl_in<'a>(el: &'a [u8], path: &[u8]) -> Option<&'a [u8]> {
    let mut segs: [&[u8]; 6] = [&[]; 6];
    let n = split_dots(path, &mut segs);
    j_path(el, &segs[..n]).filter(|v| !v.is_empty())
}

fn tbl_pod_reason(obj: &[u8]) -> PodReason<'_> {
    if tbl_in(obj, b"metadata.deletionTimestamp").is_some() {
        return PodReason::Of(&b"Terminating"[..]);
    }
    let mut reason: PodReason = PodReason::Of(
        tbl_in(obj, b"status.reason")
            .or_else(|| tbl_in(obj, b"status.phase"))
            .unwrap_or(&b"Unknown"[..]),
    );
    let list = tbl_sub(obj, b"status.containerStatuses").unwrap_or(&b"[]"[..]);
    let mut running = false;
    // kubectl walks the containers in reverse, so the FIRST one with a
    // reason names the Pod: forward, the first one taken is kept.
    let mut taken = false;
    for c in jp_elems(list) {
        let waiting = tbl_in(c, b"state.waiting.reason");
        let term = tbl_in(c, b"state.terminated.reason");
        let code = tbl_in(c, b"state.terminated.exitCode");
        if let (Some(w), false) = (waiting, taken) {
            reason = PodReason::Of(w);
            taken = true;
        } else if let (Some(t), false) = (term, taken) {
            reason = PodReason::Of(t);
            taken = true;
        } else if let (Some(code), false) = (code, taken) {
            let mut b = [0u8; 32];
            let mut n = 0usize;
            for &ch in b"ExitCode:".iter().chain(code.iter()) {
                if let Some(slot) = b.get_mut(n) {
                    *slot = ch;
                    n += 1;
                }
            }
            reason = PodReason::Exit(b, n);
            taken = true;
        } else if waiting.is_none()
            && term.is_none()
            && code.is_none()
            && tbl_in(c, b"ready") == Some(&b"true"[..])
            && j_sub(c, &[&b"state"[..], &b"running"[..]]).is_some()
        {
            running = true;
        }
    }
    // A Pod whose phase reads Completed while a container still runs is
    // Running again, as kubectl prints it.
    if running && reason.as_slice() == b"Completed" {
        return PodReason::Of(&b"Running"[..]);
    }
    reason
}

/// One cell's text for `cell` over `obj`.
#[allow(
    clippy::manual_contains,
    reason = "`<[u8]>::contains` lowers to memchr, which the PIC link cannot resolve"
)]
fn tbl_cell(cell: &[u8], obj: &[u8], now_ms: u64, out: &mut [u8], at: &mut usize) -> bool {
    if !cell.iter().any(|&b| b == b'@') {
        return tbl_value(cell, b"<none>", obj, out, at);
    }
    let mut i = 0usize;
    while i < cell.len() {
        if cell[i] != b'@' {
            if !put(out, at, &cell[i..i + 1]) {
                return false;
            }
            i += 1;
            continue;
        }
        let Some(open) = cell[i..].iter().position(|&b| b == b'(') else {
            return false;
        };
        let Some(close) = cell[i..].iter().position(|&b| b == b')') else {
            return false;
        };
        if close < open {
            return false;
        }
        let (f, arg) = (&cell[i + 1..i + open], &cell[i + open + 1..i + close]);
        if !tbl_fn(f, arg, obj, now_ms, out, at) {
            return false;
        }
        i += close + 1;
    }
    true
}

/// One row: the schema's `table.row` around the cells, its other holes the
/// object's.
fn tbl_row(
    sch: &[u8],
    cols: &[u8],
    obj: &[u8],
    now_ms: u64,
    out: &mut [u8],
    at: &mut usize,
) -> bool {
    let Some((pre, post)) = tbl_split(sch_get(sch, b"table.row"), b"{{cells}}") else {
        return false;
    };
    let mut ok = tbl_with(out, at, |d| sch_render(pre, obj, d));
    let mut first = true;
    for c in cols.split(|&b| b == b',') {
        if c.is_empty() {
            continue;
        }
        let (_, _, cell) = tbl_col(c);
        ok = ok
            && (first || put(out, at, b","))
            && put(out, at, b"\"")
            && tbl_cell(cell, obj, now_ms, out, at)
            && put(out, at, b"\"");
        first = false;
    }
    ok && tbl_with(out, at, |d| sch_render(post, obj, d))
}
