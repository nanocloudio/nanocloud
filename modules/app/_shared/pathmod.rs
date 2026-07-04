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
//   !argv   a `,`-separated escaped list (image_fetcher's `append_esc`:
//           `%25`/`%2C`/`%3B`) rendered as space-separated argv tokens
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
            let mut o = 0usize;
            let mut i = 0usize;
            let mut first = true;
            while i < src.len() {
                if src[i] == b',' {
                    if o < out.len() {
                        out[o] = b' ';
                        o += 1;
                    }
                    first = false;
                    i += 1;
                    continue;
                }
                let _ = first;
                let b = if src[i] == b'%' && i + 2 < src.len() {
                    let hex = |c: u8| -> Option<u8> {
                        match c {
                            b'0'..=b'9' => Some(c - b'0'),
                            b'a'..=b'f' => Some(c - b'a' + 10),
                            b'A'..=b'F' => Some(c - b'A' + 10),
                            _ => None,
                        }
                    };
                    match (hex(src[i + 1]), hex(src[i + 2])) {
                        (Some(h), Some(l)) => {
                            i += 3;
                            (h << 4) | l
                        }
                        _ => {
                            i += 1;
                            b'%'
                        }
                    }
                } else {
                    let c = src[i];
                    i += 1;
                    c
                };
                if o < out.len() {
                    out[o] = b;
                    o += 1;
                }
            }
            o
        }
        _ => {
            let n = src.len().min(out.len());
            out[..n].copy_from_slice(&src[..n]);
            n
        }
    }
}
