// Projection modifiers: `<path>!<mod>` on a `paths` entry, decoding the
// record's own encoding on the way out. `include!`d by the two connectors.
//
// Each is a pure function of the bytes at that path, and each exists because
// the VM cannot do it: it has no byte arithmetic, no split, and no unescape.
// Deciding WHAT to do with an image reference is policy and belongs in a
// decision; turning `nanocloud/hello:v2` into the three things a key and a
// URL need is transcription.
//
//   !repo   before the `:` (the whole string when there is none)
//   !tag    after the `:`, or `latest`
//   !flat   the repo with `/` -> `_`, the store-key spelling of a nested repo
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
//   !argv   a `,`-separated escaped list (image_fetcher's `append_esc`:
//           `%25`/`%2C`/`%3B`) rendered as an argv ([`ArgvOut`]): tokens
//           separated by a space, each escaped so it keeps its bounds
//   !jargv  a JSON array of strings (a container's `command`) rendered as
//           the same argv
//
// An unknown modifier yields the raw bytes rather than an error: a projection
// that silently changed meaning would be worse than one that did nothing.

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
                    return 0;
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
        b"repo" => {
            let end = src.iter().position(|&b| b == b':').unwrap_or(src.len());
            let n = end.min(out.len());
            out[..n].copy_from_slice(&src[..n]);
            n
        }
        b"tag" => {
            let tag: &[u8] = match src.iter().position(|&b| b == b':') {
                Some(i) => &src[i + 1..],
                None => b"latest",
            };
            let n = tag.len().min(out.len());
            out[..n].copy_from_slice(&tag[..n]);
            n
        }
        b"flat" => {
            let end = src.iter().position(|&b| b == b':').unwrap_or(src.len());
            let n = end.min(out.len());
            for (i, &b) in src[..n].iter().enumerate() {
                out[i] = if b == b'/' { b'_' } else { b };
            }
            n
        }
        b"argv" => {
            // `,` separates tokens; `%25`/`%2C`/`%3B` are the escaped `%`,
            // `,` and `;` that would otherwise break the record grammar.
            let mut w = ArgvOut::new(out);
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
            w.len()
        }
        b"jargv" => pm_json_argv(src, out).unwrap_or(0),
        _ => {
            let n = src.len().min(out.len());
            out[..n].copy_from_slice(&src[..n]);
            n
        }
    }
}

include!("argv.rs");

/// A JSON array of strings (`["sh","-c","a; b"]`) as an argv
/// ([`ArgvOut`]). `None` for anything else: an element that is not a
/// string, or a document that is not an array.
fn pm_json_argv(src: &[u8], out: &mut [u8]) -> Option<usize> {
    let mut w = ArgvOut::new(out);
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
