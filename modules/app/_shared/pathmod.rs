// Projection modifiers: `<path>!<mod>` on a `paths` entry, decoding the
// record's own encoding on the way out. `include!`d by the two connectors.
//
// Each is a pure function of the bytes at that path, and each exists because
// the VM cannot do it: it has no byte arithmetic, no split, and no unescape.
// Deciding WHAT to do with an image reference is policy and belongs in a
// decision; turning `nanocloud/hello:v2` into the three things a key and a
// URL need is transcription.
//
//   An image reference, normalized as Docker does (`[<registry>/]<repo>
//   [:<tag>][@sha256:<hex>]`; the first `/`-component is a registry when
//   it holds `.` or `:` or is `localhost`). A reference that is not one
//   renders EMPTY under every one of these:
//   !reg    its registry (`host[:port]`), EMPTY for a bare reference: the
//           node's configured registry, whatever that is
//   !repo   its repository path, without the registry
//   !tag    its tag; `latest` when it names neither a tag nor a digest,
//           EMPTY when it names only a digest
//   !pin    the digest it pins (`sha256:<64 hex>`), EMPTY when none
//   !ikey   the store-key spelling of the whole reference — `<reg>~` (its
//           `:` as `+`) when it names one, the repo with `/` as `~`, then
//           `@<hex>` for a digest, else `+<tag>`. Injective: `~` and `+`
//           are in no repository, tag or host name
//   !ip4    a dotted quad (optionally `/<prefix>`) as the decimal u32 it
//           spells — `10.96.0.5` -> `174063621`. An allocator does arithmetic
//           on an address; the VM has integers but cannot parse one
//   !prefix the `/<n>` of a CIDR as decimal, `32` when there is none
//   !hash   the 8-hex-digit FNV-1a of the bytes — the digest `hash = .`
//           stamps at 27, so a row derived from an object can be compared,
//           through `related`, with what that object is now
//   !count  the number of `,`-separated entries, as decimal (0 when empty) —
//           how many volumes a spec names, to hold its start until as many
//           are bound
//   !esc    `%`, `,` and `;` escaped (`%25`, `%2C`, `%3B`) — a scalar made
//           safe to carry inside a `k=v;` record or a list (the inverse of
//           what `!argv` undoes)
//   !size   an object's members' key and string-value bytes, summed, as
//           decimal — the measure upstream bounds a ConfigMap's or a Secret's
//           `data` by (1 MiB), which the raw JSON text overstates by its
//           quotes, colons and commas. An escape counts as the byte(s) it
//           decodes to; anything but an object renders EMPTY
//   !lower  ASCII lowercased — an upstream enum (`OnFailure`) in the
//           spelling a flat record's reader compares against (`onfailure`)
//   !argv   a `,`-separated escaped list (image.uproc's `!esc` config lists:
//           `%25`/`%2C`/`%3B`) rendered as an argv ([`ArgvOut`]): tokens
//           separated by a space, each escaped so it keeps its bounds
//   !jargv  a JSON array of strings (a container's `command`) rendered as
//           the same argv
//   !jenv   a container's `env` (a JSON array of `{"name","value"}`) as the
//           list the image config's `Env` is carried in: `NAME=VALUE`
//           entries, `,`-separated, each `!esc`aped — so the two compose by
//           joining, later entries overriding earlier ones of the same name.
//           An entry with no `value` is `NAME=`; one sourced elsewhere
//           (`valueFrom`) is not this list's and is left out
//   !milli  a Kubernetes quantity (`500m`, `2`, `0.5`, `1e3`, `1k`) as
//           decimal thousandths, rounded up — a CPU limit in millicores
//   !bytes  a Kubernetes quantity (`128Mi`, `1G`, `1e9`, `1.5Gi`) as decimal
//           units, rounded up — a memory or storage limit in bytes
//           Both render EMPTY for anything that is not a quantity, a
//           negative one, or one past u64: no limit, rather than a guessed one
//   !unix   an RFC 3339 time (`2026-10-19T14:44:00Z`, fractions and an
//           offset allowed) as Unix seconds; EMPTY for anything else
//   !reqcpu !reqmem !limcpu !limmem
//           a POD (the whole object) as its quota and scheduler count it:
//           its containers' requests (a container's limit when it states
//           no request — the API's default) or limits, summed, and the
//           largest init container's when that is larger — cpu in
//           millicores, memory in bytes
//   !ts     Unix milliseconds (decimal) as an RFC 3339 UTC time to the
//           second, `2026-10-18T09:30:00Z` — a moment a node recorded with
//           its wall clock, in the spelling every Kubernetes time field has
//           (a Pod's startTime, a condition's lastTransitionTime). EMPTY for
//           anything that is not 1..19 decimal digits
//   !jts    the same as a JSON value: `"2026-10-18T09:30:00Z"`, quotes and
//           all, or `null` for no time — a moment a node could not yet stamp
//           truthfully (its clock untrusted) is absent, never 1970
//   !uidnum the uid of an OCI user (`uid[:gid]`, or a name): its digits,
//           `0` when it is empty (an image that names no User runs as
//           root), EMPTY when it is a name — who that is only the image's
//           own /etc/passwd says, so runAsNonRoot cannot verify it
//   !pick=<name>  the scalar member <name> of an object, by its WHOLE name —
//           an annotation or a label, whose key holds the `.` and `/` a
//           dotted path would split on (`metadata.annotations!pick=
//           nanocloud.io/paused`). EMPTY when there is no such member
//
// An unknown modifier yields the raw bytes rather than an error: a projection
// that silently changed meaning would be worse than one that did nothing.
//
// A rendering that does not fit its buffer (`!argv`, `!jargv`, `!jenv`,
// `!esc`) is never cut and never EMPTY — an empty argv runs the image's
// Entrypoint instead of the Pod's command, an empty env runs without it —
// but OVERFLOW: the three bytes `%00`, a NUL no real text carries, which
// the reader (workload_effect's spawn) refuses, so the Pod visibly fails.

/// Split `<path>!<mod>` into its parts. `None` when there is no modifier.
fn split_mod(spec: &[u8]) -> (&[u8], Option<&[u8]>) {
    match spec.iter().position(|&b| b == b'!') {
        Some(i) => (&spec[..i], Some(&spec[i + 1..])),
        None => (spec, None),
    }
}

/// Apply `m` to `src`, writing into `out`; returns the length written. The
/// caller uses the slice, so a modifier that is a pure SUBSTRING could return
/// a borrow — it writes instead so every modifier has one shape.
fn apply_mod(m: &[u8], src: &[u8], out: &mut [u8]) -> usize {
    if let Some(name) = m.strip_prefix(b"pick=") {
        return pm_pick(src, name, out);
    }
    match m {
        b"size" => match pm_member_size(src) {
            Some(n) => pm_dec(n, out),
            None => 0,
        },
        b"count" => {
            let n = if src.is_empty() {
                0
            } else {
                src.iter().filter(|&&c| c == b',').count() + 1
            };
            pm_dec(n as u64, out)
        }
        b"lower" => {
            let n = src.len().min(out.len());
            for (o, &b) in out[..n].iter_mut().zip(src) {
                *o = b.to_ascii_lowercase();
            }
            n
        }
        b"esc" => {
            let mut p = 0usize;
            for &b in src {
                let rep: &[u8] = match b {
                    b'%' => b"%25",
                    b',' => b"%2C",
                    b';' => b"%3B",
                    _ => core::slice::from_ref(&b),
                };
                if p + rep.len() > out.len() {
                    return pm_overflow(out);
                }
                out[p..p + rep.len()].copy_from_slice(rep);
                p += rep.len();
            }
            p
        }
        b"hash" => {
            let mut h: u32 = 0x811c_9dc5;
            for &b in src {
                h ^= u32::from(b);
                h = h.wrapping_mul(0x0100_0193);
            }
            let hexd = b"0123456789abcdef";
            if out.len() < 8 {
                return 0;
            }
            for i in (0..8).rev() {
                out[i] = hexd[(h & 0xf) as usize];
                h >>= 4;
            }
            8
        }
        b"ip4" => {
            // Four decimal octets, each <= 255, separated by `.`; anything
            // else renders EMPTY — a malformed address must not become a
            // plausible number an allocator would then claim.
            let end = src.iter().position(|&b| b == b'/').unwrap_or(src.len());
            let mut v: u64 = 0;
            let mut oct: u64 = 0;
            let mut digits = 0usize;
            let mut dots = 0usize;
            for &b in &src[..end] {
                if b.is_ascii_digit() {
                    oct = oct * 10 + u64::from(b - b'0');
                    digits += 1;
                    if oct > 255 || digits > 3 {
                        return 0;
                    }
                } else if b == b'.' && digits > 0 && dots < 3 {
                    v = (v << 8) | oct;
                    oct = 0;
                    digits = 0;
                    dots += 1;
                } else {
                    return 0;
                }
            }
            if dots != 3 || digits == 0 {
                return 0;
            }
            v = (v << 8) | oct;
            pm_dec(v, out)
        }
        b"prefix" => {
            let p = match src.iter().position(|&b| b == b'/') {
                Some(i) => &src[i + 1..],
                None => return pm_dec(32, out),
            };
            let mut n: u64 = 0;
            if p.is_empty() || p.len() > 2 {
                return 0;
            }
            for &b in p {
                if !b.is_ascii_digit() {
                    return 0;
                }
                n = n * 10 + u64::from(b - b'0');
            }
            if n > 32 {
                return 0;
            }
            pm_dec(n, out)
        }
        b"reg" | b"repo" | b"tag" | b"pin" | b"ikey" => pm_image_ref(m, src, out),
        b"argv" => {
            // `,` separates tokens; `%25`/`%2C`/`%3B` are the escaped `%`,
            // `,` and `;` that would otherwise break the record grammar.
            let mut w = ArgvOut::new(&mut *out);
            let mut i = 0usize;
            while i < src.len() {
                match src[i] {
                    b',' => {
                        w.next_token();
                        i += 1;
                    }
                    b'%' => match (
                        src.get(i + 1).and_then(|&c| argv_hex(c)),
                        src.get(i + 2).and_then(|&c| argv_hex(c)),
                    ) {
                        (Some(h), Some(l)) => {
                            w.byte((h << 4) | l);
                            i += 3;
                        }
                        _ => {
                            w.byte(b'%');
                            i += 1;
                        }
                    },
                    c => {
                        w.byte(c);
                        i += 1;
                    }
                }
            }
            if w.full {
                return pm_overflow(out);
            }
            w.len()
        }
        b"jargv" => pm_json_argv(src, out).unwrap_or(0),
        b"jenv" => pm_json_env(src, out).unwrap_or(0),
        b"milli" => pm_quantity(src, 3).map_or(0, |v| pm_dec(v, out)),
        b"unix" => pm_unix(src).map_or(0, |v| pm_dec(v, out)),
        b"reqcpu" => pm_dec(pm_pod_res(src, b"requests", b"cpu", 3), out),
        b"reqmem" => pm_dec(pm_pod_res(src, b"requests", b"memory", 0), out),
        b"limcpu" => pm_dec(pm_pod_res(src, b"limits", b"cpu", 3), out),
        b"limmem" => pm_dec(pm_pod_res(src, b"limits", b"memory", 0), out),
        b"bytes" => pm_quantity(src, 0).map_or(0, |v| pm_dec(v, out)),
        b"ts" => pm_rfc3339(src, out).unwrap_or(0),
        b"jts" => pm_json_time(src, out),
        b"uidnum" => {
            let uid = match src.iter().position(|&c| c == b':') {
                Some(i) => src.get(..i).unwrap_or(&[]),
                None => src,
            };
            if src.is_empty() {
                pm_put(out, 0, b"0").unwrap_or(0)
            } else if !uid.is_empty() && uid.iter().all(|c| c.is_ascii_digit()) {
                pm_put(out, 0, uid).unwrap_or(0)
            } else {
                0
            }
        }
        _ => {
            let n = src.len().min(out.len());
            out[..n].copy_from_slice(&src[..n]);
            n
        }
    }
}

include!("argv.rs");

/// An image reference's parts, as ranges of the reference (see the header).
struct PmRef {
    reg: (usize, usize),
    repo: (usize, usize),
    tag: Option<(usize, usize)>,
    pin: Option<(usize, usize)>,
}

/// Docker's reference grammar, as far as a node pulls one: `None` for
/// anything else (an empty or upper-case repository, a malformed tag, a
/// digest that is not sha256 over 64 lower-case hex digits).
fn pm_parse_ref(src: &[u8]) -> Option<PmRef> {
    let (name_end, pin) = match src.iter().position(|&b| b == b'@') {
        Some(at) => {
            let d = src.get(at + 1..)?;
            let hex = d.get(7..).unwrap_or(&[]);
            if d.len() != 71
                || !d.starts_with(b"sha256:")
                || !hex
                    .iter()
                    .all(|&c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            {
                return None;
            }
            (at, Some((at + 1, src.len())))
        }
        None => (src.len(), None),
    };
    let name = src.get(..name_end)?;
    let last_slash = name.iter().rposition(|&b| b == b'/');
    let (path_end, tag) = match name.iter().rposition(|&b| b == b':') {
        Some(c) if last_slash.is_none_or(|s| c > s) => (c, Some((c + 1, name_end))),
        _ => (name_end, None),
    };
    let path = name.get(..path_end)?;
    let (reg, repo) = match path.iter().position(|&b| b == b'/') {
        Some(i)
            if {
                let first = path.get(..i).unwrap_or(&[]);
                first.iter().any(|&c| c == b'.' || c == b':') || first == b"localhost"
            } =>
        {
            ((0, i), (i + 1, path_end))
        }
        _ => ((0, 0), (0, path_end)),
    };
    let r = src.get(reg.0..reg.1)?;
    if !r
        .iter()
        .all(|&c| c.is_ascii_alphanumeric() || c == b'.' || c == b':' || c == b'-')
    {
        return None;
    }
    let p = src.get(repo.0..repo.1)?;
    let mut prev = b'/';
    for &c in p {
        let ok =
            c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'.' | b'_' | b'-' | b'/');
        if !ok || (c == b'/' && prev == b'/') {
            return None;
        }
        prev = c;
    }
    if p.is_empty() || prev == b'/' {
        return None;
    }
    if let Some((a, b)) = tag {
        let t = src.get(a..b)?;
        if t.is_empty()
            || t.len() > 128
            || !t
                .iter()
                .all(|&c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
        {
            return None;
        }
    }
    Some(PmRef {
        reg,
        repo,
        tag,
        pin,
    })
}

/// Bytes into `out` at `at`, or `None` past its end.
fn pm_put(out: &mut [u8], at: usize, b: &[u8]) -> Option<usize> {
    let end = at.checked_add(b.len())?;
    for (o, &c) in out.get_mut(at..end)?.iter_mut().zip(b) {
        *o = c;
    }
    Some(end)
}

/// One of `!reg` `!repo` `!tag` `!pin` `!ikey` (the header). A reference
/// that does not parse, or a part past `out`, renders EMPTY: never a
/// truncated key, which could name another image.
fn pm_image_ref(m: &[u8], src: &[u8], out: &mut [u8]) -> usize {
    let Some(r) = pm_parse_ref(src) else {
        return 0;
    };
    let part = |(a, b): (usize, usize)| src.get(a..b).unwrap_or(&[]);
    let tag: &[u8] = match (r.tag, r.pin) {
        (Some(t), _) => part(t),
        (None, None) => b"latest",
        (None, Some(_)) => b"",
    };
    let got = match m {
        b"reg" => pm_put(out, 0, part(r.reg)),
        b"repo" => pm_put(out, 0, part(r.repo)),
        b"tag" => pm_put(out, 0, tag),
        b"pin" => pm_put(out, 0, r.pin.map_or(&[][..], part)),
        _ => (|| {
            let mut o = 0usize;
            let reg = part(r.reg);
            if !reg.is_empty() {
                for &c in reg {
                    o = pm_put(out, o, &[if c == b':' { b'+' } else { c }])?;
                }
                o = pm_put(out, o, b"~")?;
            }
            for &c in part(r.repo) {
                o = pm_put(out, o, &[if c == b'/' { b'~' } else { c }])?;
            }
            match r.pin {
                Some(p) => {
                    o = pm_put(out, o, b"@")?;
                    pm_put(out, o, part(p).get(7..).unwrap_or(&[]))
                }
                None => {
                    o = pm_put(out, o, b"+")?;
                    pm_put(out, o, tag)
                }
            }
        })(),
    };
    got.unwrap_or(0)
}

/// A JSON array of strings (`["sh","-c","a; b"]`) as an argv
/// ([`ArgvOut`]). `None` for anything else: an element that is not a
/// string, or a document that is not an array.
fn pm_json_argv(src: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut w = ArgvOut::new(&mut *out);
    let ws = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r');
    let mut i = 0usize;
    while src.get(i).is_some_and(|&b| ws(b)) {
        i += 1;
    }
    if src.get(i) != Some(&b'[') {
        return None;
    }
    i += 1;
    loop {
        while src.get(i).is_some_and(|&b| ws(b)) {
            i += 1;
        }
        match src.get(i)? {
            b']' => break,
            b'"' => i += 1,
            _ => return None,
        }
        // One string, its escapes decoded.
        loop {
            match *src.get(i)? {
                b'"' => {
                    i += 1;
                    break;
                }
                b'\\' => {
                    let e = *src.get(i + 1)?;
                    i += 2;
                    match e {
                        b'n' => w.byte(b'\n'),
                        b't' => w.byte(b'\t'),
                        b'r' => w.byte(b'\r'),
                        b'b' => w.byte(0x08),
                        b'f' => w.byte(0x0c),
                        b'u' => {
                            let mut v = 0u32;
                            for k in 0..4 {
                                v = (v << 4) | u32::from(argv_hex(*src.get(i + k)?)?);
                            }
                            i += 4;
                            let mut utf = [0u8; 4];
                            let c = char::from_u32(v)?;
                            for &b in c.encode_utf8(&mut utf).as_bytes() {
                                w.byte(b);
                            }
                        }
                        other => w.byte(other),
                    }
                }
                c => {
                    w.byte(c);
                    i += 1;
                }
            }
        }
        w.next_token();
        while src.get(i).is_some_and(|&b| ws(b)) {
            i += 1;
        }
        match src.get(i)? {
            b',' => i += 1,
            b']' => break,
            _ => return None,
        }
    }
    if w.full {
        return Some(pm_overflow(out));
    }
    Some(w.len())
}

/// Decimal digits of `v` into `out`; returns the length (0 if it does not fit).
fn pm_dec(v: u64, out: &mut [u8]) -> usize {
    let mut tmp = [0u8; 20];
    let mut i = tmp.len();
    let mut u = v;
    loop {
        i -= 1;
        tmp[i] = b'0' + (u % 10) as u8;
        u /= 10;
        if u == 0 || i == 0 {
            break;
        }
    }
    let n = tmp.len() - i;
    if n > out.len() {
        return 0;
    }
    out[..n].copy_from_slice(&tmp[i..]);
    n
}

/// `!ts`: decimal Unix milliseconds as `YYYY-MM-DDTHH:MM:SSZ` (civil from
/// days, Howard Hinnant's algorithm). `None` for anything but 1..19 digits,
/// or when `out` cannot hold the 20 bytes.
fn pm_rfc3339(src: &[u8], out: &mut [u8]) -> Option<usize> {
    if src.is_empty() || src.len() > 19 {
        return None;
    }
    let mut ms: u64 = 0;
    for &c in src {
        if !c.is_ascii_digit() {
            return None;
        }
        ms = ms.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
    }
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
    if !(0..=9999).contains(&y) {
        return None;
    }
    let mut ts = *b"0000-00-00T00:00:00Z";
    let mut put = |at: usize, v: u64, w: usize| {
        let mut v = v;
        for k in (0..w).rev() {
            if let Some(slot) = ts.get_mut(at + k) {
                *slot = b'0' + (v % 10) as u8;
            }
            v /= 10;
        }
    };
    put(0, y as u64, 4);
    put(5, m as u64, 2);
    put(8, d as u64, 2);
    put(11, sod / 3600, 2);
    put(14, (sod / 60) % 60, 2);
    put(17, sod % 60, 2);
    pm_put(out, 0, &ts)
}

/// `!jts`: [`pm_rfc3339`] quoted, or `null`.
fn pm_json_time(src: &[u8], out: &mut [u8]) -> usize {
    let mut t = [0u8; 24];
    match pm_rfc3339(src, &mut t) {
        Some(n) => pm_put(out, 0, b"\"")
            .and_then(|p| pm_put(out, p, t.get(..n).unwrap_or(&[])))
            .and_then(|p| pm_put(out, p, b"\""))
            .unwrap_or(0),
        None => pm_put(out, 0, b"null").unwrap_or(0),
    }
}

/// `!size`: the decoded bytes of every key and string value of the object
/// `src`; a non-string value counts as written. `None` unless `src` is an
/// object that parses to its close.
fn pm_member_size(src: &[u8]) -> Option<u64> {
    let ws = |b: &[u8], mut i: usize| {
        while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
            i += 1;
        }
        i
    };
    let mut i = ws(src, 0);
    if src.get(i) != Some(&b'{') {
        return None;
    }
    i = ws(src, i + 1);
    let mut total: u64 = 0;
    if src.get(i) == Some(&b'}') {
        return Some(0);
    }
    loop {
        // The key.
        let (n, at) = pm_str_size(src, i)?;
        total += n;
        i = ws(src, at);
        if src.get(i) != Some(&b':') {
            return None;
        }
        i = ws(src, i + 1);
        // The value: a string by its decoded size, anything else as written.
        if src.get(i) == Some(&b'"') {
            let (n, at) = pm_str_size(src, i)?;
            total += n;
            i = at;
        } else {
            let at = j_skip(src, i);
            if at <= i || at > src.len() {
                return None;
            }
            total += (at - i) as u64;
            i = at;
        }
        i = ws(src, i);
        match src.get(i) {
            Some(b',') => i = ws(src, i + 1),
            Some(b'}') => return Some(total),
            _ => return None,
        }
    }
}

/// `!pick=<name>`: the scalar value of member `name` (compared as written,
/// escapes and all) of the object `src`, into `out`. EMPTY when `src` is not
/// an object, has no such member, or its value is not a scalar; OVERFLOW when
/// the value does not fit.
fn pm_pick(src: &[u8], name: &[u8], out: &mut [u8]) -> usize {
    let ws = |b: &[u8], mut i: usize| {
        while b
            .get(i)
            .is_some_and(|&c| matches!(c, b' ' | b'\t' | b'\n' | b'\r'))
        {
            i += 1;
        }
        i
    };
    let mut i = ws(src, 0);
    if src.get(i) != Some(&b'{') {
        return 0;
    }
    i = ws(src, i + 1);
    while src.get(i) == Some(&b'"') {
        let ks = i + 1;
        let mut ke = ks;
        while let Some(&c) = src.get(ke) {
            if c == b'"' {
                break;
            }
            ke += if c == b'\\' { 2 } else { 1 };
        }
        let key = src.get(ks..ke).unwrap_or(&[]);
        i = ws(src, ke + 1);
        if src.get(i) != Some(&b':') {
            return 0;
        }
        i = ws(src, i + 1);
        if key == name {
            let v = j_scalar(src, i);
            if src.get(i).is_some_and(|&c| c == b'{' || c == b'[') {
                return 0;
            }
            if v.len() > out.len() {
                return pm_overflow(out);
            }
            for (o, &c) in out.iter_mut().zip(v) {
                *o = c;
            }
            return v.len();
        }
        let at = j_skip(src, i);
        if at <= i {
            return 0;
        }
        i = ws(src, at);
        if src.get(i) == Some(&b',') {
            i = ws(src, i + 1);
        }
    }
    0
}

/// The decoded byte size of the JSON string opening at `src[i]`, and the index
/// past its closing quote. `\uXXXX` counts as its UTF-8 length.
fn pm_str_size(src: &[u8], i: usize) -> Option<(u64, usize)> {
    if src.get(i) != Some(&b'"') {
        return None;
    }
    let mut i = i + 1;
    let mut n: u64 = 0;
    while i < src.len() {
        match src[i] {
            b'"' => return Some((n, i + 1)),
            b'\\' => {
                if src.get(i + 1) == Some(&b'u') {
                    let hex = src.get(i + 2..i + 6)?;
                    let mut cp: u32 = 0;
                    for &h in hex {
                        cp = cp * 16 + (h as char).to_digit(16)?;
                    }
                    n += if cp < 0x80 {
                        1
                    } else if cp < 0x800 {
                        2
                    } else {
                        3
                    };
                    i += 6;
                } else {
                    n += 1;
                    i += 2;
                }
            }
            _ => {
                n += 1;
                i += 1;
            }
        }
    }
    None
}

/// `!unix`: an RFC 3339 time as Unix seconds (before 1970: none).
fn pm_unix(t: &[u8]) -> Option<u64> {
    let num = |a: usize, b: usize| -> Option<i64> {
        let mut v = 0i64;
        for &c in t.get(a..b)? {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + i64::from(c - b'0');
        }
        Some(v)
    };
    if t.get(4) != Some(&b'-')
        || t.get(7) != Some(&b'-')
        || t.get(13) != Some(&b':')
        || t.get(16) != Some(&b':')
    {
        return None;
    }
    if !matches!(t.get(10), Some(&b'T') | Some(&b't') | Some(&b' ')) {
        return None;
    }
    let (y, m, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (hh, mm, ss) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    // Past the seconds: a fraction, then Z or an offset.
    let mut i = 19usize;
    if t.get(i) == Some(&b'.') {
        i += 1;
        while t.get(i).is_some_and(|c| c.is_ascii_digit()) {
            i += 1;
        }
    }
    let off = match t.get(i) {
        Some(&b'Z') | Some(&b'z') if i + 1 == t.len() => 0,
        Some(&s) if (s == b'+' || s == b'-') && t.len() == i + 6 && t.get(i + 3) == Some(&b':') => {
            let o = num(i + 1, i + 3)? * 3600 + num(i + 4, i + 6)? * 60;
            if s == b'+' {
                o
            } else {
                -o
            }
        }
        _ => return None,
    };
    let yy = if m <= 2 { y - 1 } else { y };
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    let secs = days * 86_400 + hh * 3600 + mm * 60 + ss - off;
    u64::try_from(secs).ok()
}

/// `!reqcpu` and friends: a Pod's `kind` (`requests` or `limits`) of
/// `res`, times 10^`scale10` — summed over spec.containers, the largest of
/// spec.initContainers when larger. A request a container does not state
/// is its limit (the API's default); a limit it does not state is none.
fn pm_pod_res(pod: &[u8], kind: &[u8], res: &[u8], scale10: i32) -> u64 {
    let one = |c: &[u8]| -> u64 {
        let v = j_path(c, &[&b"resources"[..], kind, res])
            .filter(|v| !v.is_empty())
            .or_else(|| {
                if kind == b"requests" {
                    j_path(c, &[&b"resources"[..], b"limits", res]).filter(|v| !v.is_empty())
                } else {
                    None
                }
            });
        v.and_then(|v| pm_quantity(v, scale10)).unwrap_or(0)
    };
    let mut sum = 0u64;
    if let Some(cs) = j_sub(pod, &[&b"spec"[..], b"containers"]) {
        let mut i = 0usize;
        while let Some(c) = j_idx(cs, i) {
            sum = sum.saturating_add(one(c));
            i += 1;
        }
    }
    if let Some(cs) = j_sub(pod, &[&b"spec"[..], b"initContainers"]) {
        let mut i = 0usize;
        while let Some(c) = j_idx(cs, i) {
            sum = sum.max(one(c));
            i += 1;
        }
    }
    sum
}

/// `!milli` / `!bytes`: a Kubernetes quantity — `[+]<digits>[.<digits>]`
/// then a binary suffix (`Ki`..`Ei`), a decimal one (`n u m k M G T P E`) or
/// an exponent (`e<n>`, `E<n>`) — times 10^`scale10`, rounded up. `None` for
/// anything else, a negative quantity, or a value past u64.
fn pm_quantity(src: &[u8], scale10: i32) -> Option<u64> {
    let mut i = 0usize;
    let ws = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r');
    while src.get(i).is_some_and(|&b| ws(b)) {
        i += 1;
    }
    let mut end = src.len();
    while end > i && src.get(end - 1).is_some_and(|&b| ws(b)) {
        end -= 1;
    }
    if src.get(i) == Some(&b'+') {
        i += 1;
    }
    let mut n: u64 = 0;
    let mut digits = 0usize;
    let mut frac: i32 = 0;
    let mut dot = false;
    while i < end {
        match src.get(i) {
            Some(&c) if c.is_ascii_digit() => {
                n = n.checked_mul(10)?.checked_add(u64::from(c - b'0'))?;
                digits += 1;
                if dot {
                    frac += 1;
                }
            }
            Some(&b'.') if !dot => dot = true,
            _ => break,
        }
        i += 1;
    }
    if digits == 0 {
        return None;
    }
    let suffix = src.get(i..end)?;
    let (e10, b2): (i32, u32) = match suffix {
        b"" => (0, 0),
        b"Ki" => (0, 10),
        b"Mi" => (0, 20),
        b"Gi" => (0, 30),
        b"Ti" => (0, 40),
        b"Pi" => (0, 50),
        b"Ei" => (0, 60),
        b"n" => (-9, 0),
        b"u" => (-6, 0),
        b"m" => (-3, 0),
        b"k" => (3, 0),
        b"M" => (6, 0),
        b"G" => (9, 0),
        b"T" => (12, 0),
        b"P" => (15, 0),
        b"E" => (18, 0),
        _ => {
            // An exponent: `e`/`E`, an optional sign, digits.
            let (&first, rest) = suffix.split_first()?;
            if first != b'e' && first != b'E' {
                return None;
            }
            let (neg, rest) = match rest.split_first() {
                Some((&b'-', r)) => (true, r),
                Some((&b'+', r)) => (false, r),
                _ => (false, rest),
            };
            if rest.is_empty() || rest.len() > 3 || !rest.iter().all(|c| c.is_ascii_digit()) {
                return None;
            }
            let mut e: i32 = 0;
            for &c in rest {
                e = e * 10 + i32::from(c - b'0');
            }
            (if neg { -e } else { e }, 0)
        }
    };
    let mut v = n;
    for _ in 0..b2 {
        v = v.checked_mul(2)?;
    }
    let k = e10 + scale10 - frac;
    if k >= 0 {
        for _ in 0..k {
            v = v.checked_mul(10)?;
        }
    } else {
        for _ in 0..(-k) {
            v = v / 10 + u64::from(!v.is_multiple_of(10));
        }
    }
    Some(v)
}

/// `!jenv`: a container's `env` array as `NAME=VALUE,…`, each entry escaped
/// (`%25`, `%2C`, `%3B`). `None` when `src` is not an array of objects; an
/// empty or absent array is the empty list; a list past `out` is OVERFLOW.
fn pm_json_env(src: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut w = PmOut {
        out: &mut *out,
        o: 0,
        full: false,
    };
    pm_json_env_into(src, &mut w)?;
    if w.full {
        return Some(pm_overflow(out));
    }
    Some(w.o)
}

/// What a modifier writes, never past its buffer: a byte that does not fit
/// marks it `full` (OVERFLOW) instead of failing the parse.
struct PmOut<'a> {
    out: &'a mut [u8],
    o: usize,
    full: bool,
}

impl PmOut<'_> {
    fn put(&mut self, b: u8) {
        match self.out.get_mut(self.o) {
            Some(slot) => {
                *slot = b;
                self.o += 1;
            }
            None => self.full = true,
        }
    }
    /// `b`, `!esc`aped.
    fn esc(&mut self, b: u8) {
        let hexd = b"0123456789ABCDEF";
        if matches!(b, b'%' | b',' | b';') {
            self.put(b'%');
            self.put(hexd[(b >> 4) as usize]);
            self.put(hexd[(b & 0xf) as usize]);
        } else {
            self.put(b);
        }
    }
}

/// The OVERFLOW rendering (`%00`; see the header), or nothing when even it
/// does not fit.
fn pm_overflow(out: &mut [u8]) -> usize {
    match out.get_mut(..3) {
        Some(d) => {
            d[0] = b'%';
            d[1] = b'0';
            d[2] = b'0';
            3
        }
        None => 0,
    }
}

fn pm_json_env_into(src: &[u8], w: &mut PmOut) -> Option<()> {
    let ws = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r');
    let skip = |i: &mut usize| {
        while src.get(*i).is_some_and(|&b| ws(b)) {
            *i += 1;
        }
    };
    let mut i = 0usize;
    skip(&mut i);
    if src.get(i).is_none() {
        return Some(());
    }
    if src.get(i) != Some(&b'[') {
        return None;
    }
    i += 1;
    let mut first = true;
    loop {
        skip(&mut i);
        match src.get(i)? {
            b']' => break,
            b',' => {
                i += 1;
                continue;
            }
            b'{' => i += 1,
            _ => return None,
        }
        // One object: its `name` and `value` strings, as ranges of `src`.
        let mut name: Option<(usize, usize)> = None;
        let mut value: Option<(usize, usize)> = None;
        let mut sourced = false;
        loop {
            skip(&mut i);
            match src.get(i)? {
                b'}' => {
                    i += 1;
                    break;
                }
                b',' => {
                    i += 1;
                    continue;
                }
                b'"' => {}
                _ => return None,
            }
            let ks = i;
            let ke = pm_jstr_end(src, ks)?;
            i = ke;
            skip(&mut i);
            if src.get(i) != Some(&b':') {
                return None;
            }
            i += 1;
            skip(&mut i);
            let key = src.get(ks + 1..ke - 1)?;
            if src.get(i) == Some(&b'"') {
                let ve = pm_jstr_end(src, i)?;
                match key {
                    b"name" => name = Some((i, ve)),
                    b"value" => value = Some((i, ve)),
                    _ => {}
                }
                i = ve;
            } else {
                if key == b"valueFrom" {
                    sourced = true;
                }
                let at = j_skip(src, i);
                if at <= i || at > src.len() {
                    return None;
                }
                i = at;
            }
        }
        let Some((ns, ne)) = name else {
            continue;
        };
        if sourced && value.is_none() {
            continue;
        }
        if !first {
            w.put(b',');
        }
        first = false;
        pm_jstr_esc(src, ns, ne, w)?;
        w.put(b'=');
        if let Some((vs, ve)) = value {
            pm_jstr_esc(src, vs, ve, w)?;
        }
    }
    Some(())
}

/// The index past the closing quote of the JSON string opening at `src[i]`.
fn pm_jstr_end(src: &[u8], i: usize) -> Option<usize> {
    if src.get(i) != Some(&b'"') {
        return None;
    }
    let mut i = i + 1;
    loop {
        match *src.get(i)? {
            b'"' => return Some(i + 1),
            b'\\' => i += 2,
            _ => i += 1,
        }
    }
}

/// The JSON string `src[s..e]` (quotes included) decoded and `!esc`aped
/// into `w`. `None` for an escape that is not JSON.
fn pm_jstr_esc(src: &[u8], s: usize, e: usize, w: &mut PmOut) -> Option<()> {
    let mut i = s + 1;
    let end = e.checked_sub(1)?;
    while i < end {
        let c = *src.get(i)?;
        if c != b'\\' {
            w.esc(c);
            i += 1;
            continue;
        }
        let x = *src.get(i + 1)?;
        i += 2;
        match x {
            b'n' => w.esc(b'\n'),
            b't' => w.esc(b'\t'),
            b'r' => w.esc(b'\r'),
            b'b' => w.esc(0x08),
            b'f' => w.esc(0x0c),
            b'u' => {
                let mut v = 0u32;
                for k in 0..4 {
                    v = (v << 4) | u32::from(argv_hex(*src.get(i + k)?)?);
                }
                i += 4;
                let mut utf = [0u8; 4];
                for &b in char::from_u32(v)?.encode_utf8(&mut utf).as_bytes() {
                    w.esc(b);
                }
            }
            other => w.esc(other),
        }
    }
    Some(())
}
