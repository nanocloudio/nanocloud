// A Pod's graceful deletion, as the API server and the kubelet share it
// upstream — `include!`d after store.rs and json.rs by the two halves here:
// store_doc (the API's DELETE: graceful, or outright) and pod_resolve (the
// kubelet's final delete, once nothing of the Pod runs).
//
// A DELETE of a Pod upstream is graceful: the Pod is marked
// (`metadata.deletionTimestamp`, the moment its grace ends, and
// `metadata.deletionGracePeriodSeconds`) and stays — Terminating — while its
// node stops every container (SIGTERM, then SIGKILL once the grace is
// spent); the kubelet then deletes it outright. The grace is the request's
// `DeleteOptions.gracePeriodSeconds` (`kubectl delete --grace-period=N`;
// `--now` is 1; `--force --grace-period=0` is 0: removed at once), else the
// Pod's `spec.terminationGracePeriodSeconds`, else 30. A Pod no node runs
// has nothing to stop: deleted at once, as upstream's API server does for an
// unscheduled or terminal Pod, and as its kubelet does for one it never
// started.
//
// Each of a Pod's containers is its own record on its node — `<uid>` the
// first app container, `<uid>~c<n>` and `<uid>~i<n>` the others
// (_shared/podres.rs) — so "anything of the Pod running" is any of those
// sandboxes running or paused: one listing of the uid's prefix.

/// The grace upstream's API server defaults `terminationGracePeriodSeconds`
/// to.
const PL_GRACE_DEFAULT: u64 = 30;

/// A JSON number's digits as seconds; `None` for anything else (a negative
/// grace, a string).
fn pl_secs(v: Option<&[u8]>) -> Option<u64> {
    let v = v?;
    if v.is_empty() || v.len() > 12 || v.iter().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    let mut n = 0u64;
    for &c in v {
        n = n * 10 + u64::from(c - b'0');
    }
    Some(n)
}

/// The grace a DELETE asks for in its body — a DeleteOptions'
/// `gracePeriodSeconds`, or an Eviction's `deleteOptions.gracePeriodSeconds`
/// — `None` when it states none.
fn pl_asked(opts: &[u8]) -> Option<u64> {
    let first = opts.iter().position(|&c| !j_is_ws(c))?;
    if opts.get(first) != Some(&b'{') {
        return None;
    }
    pl_secs(j_get1(opts, b"gracePeriodSeconds"))
        .or_else(|| pl_secs(j_get2(opts, b"deleteOptions", b"gracePeriodSeconds")))
}

/// The grace of the Pod `pod` itself: a delete already under way's
/// (`metadata.deletionGracePeriodSeconds`), else its own
/// `spec.terminationGracePeriodSeconds`, else the default.
fn pl_own(pod: &[u8]) -> u64 {
    pl_secs(j_get2(pod, b"metadata", b"deletionGracePeriodSeconds"))
        .or_else(|| pl_secs(j_get2(pod, b"spec", b"terminationGracePeriodSeconds")))
        .unwrap_or(PL_GRACE_DEFAULT)
}

/// Whether a delete of `pod` may wait at all: it is scheduled, not
/// terminal (Succeeded, Failed), and not graph-backed (a module subgraph has
/// no SIGTERM window: its node tears it down at once).
fn pl_waits(pod: &[u8]) -> bool {
    let node = j_get2(pod, b"spec", b"nodeName").unwrap_or(&[]);
    let phase = j_get2(pod, b"status", b"phase").unwrap_or(&[]);
    !node.is_empty()
        && node != b"null"
        && phase != b"Succeeded"
        && phase != b"Failed"
        && j_get2(pod, b"spec", b"runtimeClassName") != Some(b"fluxor".as_slice())
}

/// Whether anything of the Pod `uid` runs on `node`: a sandbox of any of its
/// containers (`/~node/<node>/sandbox-status/<uid>[~…]`) running or paused.
/// `None` while a read is not answered yet (the pass waits: ask again).
unsafe fn pl_live(st: &Store, node: &[u8], uid: &[u8]) -> Option<bool> {
    let mut pre = [0u8; MAX_KEY];
    let mut n = append(&mut pre, 0, b"/~node/");
    n = append(&mut pre, n, node);
    n = append(&mut pre, n, b"/sandbox-status/");
    let base = n;
    n = append(&mut pre, n, uid);
    if uid.is_empty() || node.is_empty() || n != base + uid.len() || n >= MAX_KEY {
        return Some(false);
    }
    let prefix = pre.get(..n).unwrap_or(&[]);
    let mut walk = ListWalk::new(prefix);
    let mut key = [0u8; MAX_KEY];
    let mut val = [0u8; 256];
    let mut live = false;
    while let Some(k) = walk.next(st) {
        // `<uid>` itself or one of its containers (`<uid>~…`), never
        // another uid that merely begins with this one.
        let rest = k.get(n..).unwrap_or(&[]);
        if !(rest.is_empty() || rest.first() == Some(&b'~')) || k.len() > MAX_KEY {
            continue;
        }
        for (d, &c) in key.iter_mut().zip(k.iter()) {
            *d = c;
        }
        let kl = k.len();
        if let Some(vl) = get_value(st, key.get(..kl).unwrap_or(&[]), &mut val) {
            let v = val.get(..vl).unwrap_or(&[]);
            if v.starts_with(b"state=running") || v.starts_with(b"state=paused") {
                live = true;
            }
        }
    }
    if walk.pending || st.waiting() {
        return None;
    }
    Some(live)
}

/// Days since 1970-01-01 of a civil date (Howard Hinnant's algorithm).
fn pl_days(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DDTHH:MM:SSZ` for Unix ms, into `out`.
fn pl_rfc3339(ms: u64, out: &mut [u8; 20]) {
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let sod = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    *out = *b"0000-00-00T00:00:00Z";
    let mut put = |at: usize, v: u64, w: usize| {
        let mut v = v;
        for k in (0..w).rev() {
            if let Some(slot) = out.get_mut(at + k) {
                *slot = b'0' + (v % 10) as u8;
            }
            v /= 10;
        }
    };
    put(0, y.max(0) as u64, 4);
    put(5, m as u64, 2);
    put(8, d as u64, 2);
    put(11, sod / 3600, 2);
    put(14, (sod / 60) % 60, 2);
    put(17, sod % 60, 2);
}

/// Unix ms of an RFC 3339 time (`YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)`);
/// `None` for anything else.
fn pl_parse_time(t: &[u8]) -> Option<u64> {
    let num = |a: usize, w: usize| -> Option<i64> {
        let mut v = 0i64;
        for k in 0..w {
            let c = *t.get(a + k)?;
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + i64::from(c - b'0');
        }
        Some(v)
    };
    let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    let (h, mi, s) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
    let mut i = 19usize;
    if t.get(i) == Some(&b'.') {
        i += 1;
        while t.get(i).is_some_and(|c| c.is_ascii_digit()) {
            i += 1;
        }
    }
    let off = match t.get(i) {
        Some(b'Z') | Some(b'z') => 0,
        Some(&sign) if sign == b'+' || sign == b'-' => {
            let o = num(i + 1, 2)? * 3600 + num(i + 4, 2)? * 60;
            if sign == b'+' {
                o
            } else {
                -o
            }
        }
        _ => return None,
    };
    let secs = pl_days(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s - off;
    u64::try_from(secs).ok().map(|v| v * 1000)
}
