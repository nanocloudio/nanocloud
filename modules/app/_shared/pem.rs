// A credential file as DER: the file itself, or its PEM blocks decoded —
// what a module that reads a certificate chain, a key or trust anchors at
// runtime hands on to a module that takes DER (volume_host: the volume
// subgraph's `tls`). `include!`d; no allocation, no table of pointers (a PIC
// module holds none).

/// A credential file's DER into `out`: the file itself when it is DER (a
/// SEQUENCE), or — PEM — its blocks decoded: every CERTIFICATE,
/// concatenated (a chain, leaf first, or an anchor set), or for a `key` the
/// first PRIVATE KEY, EC PRIVATE KEY or RSA PRIVATE KEY; other blocks (EC
/// PARAMETERS) passed over. Its length; `None` for anything else, or what
/// does not fit.
fn pem_or_der(raw: &[u8], key: bool, out: &mut [u8]) -> Option<usize> {
    const BEGIN: &[u8] = b"-----BEGIN ";
    const END: &[u8] = b"-----END ";
    let starts = |at: usize, pat: &[u8]| raw.get(at..at + pat.len()) == Some(pat);
    // DER is a SEQUENCE; PEM is text, whatever precedes its first block.
    if raw.first() == Some(&0x30) {
        out.get_mut(..raw.len())?.copy_from_slice(raw);
        return Some(raw.len());
    }
    let mut i = 0usize;
    let mut o = 0usize;
    let mut found = false;
    while i < raw.len() {
        if !starts(i, BEGIN) {
            i += 1;
            continue;
        }
        let l0 = i + BEGIN.len();
        let mut l1 = l0;
        while l1 < raw.len() && !starts(l1, b"-----") {
            l1 += 1;
        }
        let label = raw.get(l0..l1)?;
        let body = l1 + 5;
        let mut e = body;
        while e < raw.len() && !starts(e, END) {
            e += 1;
        }
        if e >= raw.len() {
            return None;
        }
        let wanted = if key {
            !found
                && (label == b"PRIVATE KEY"
                    || label == b"EC PRIVATE KEY"
                    || label == b"RSA PRIVATE KEY")
        } else {
            label == b"CERTIFICATE"
        };
        if wanted {
            o = b64_decode(raw.get(body..e)?, out, o)?;
            found = true;
        }
        i = e + END.len();
    }
    if found && o > 0 {
        Some(o)
    } else {
        None
    }
}

/// Base64 (whitespace passed over, `=` padding ending it) appended to
/// `out` from `at`; the new end. `None` for a byte outside the alphabet or
/// what does not fit.
fn b64_decode(src: &[u8], out: &mut [u8], mut at: usize) -> Option<usize> {
    let mut acc = 0u32;
    let mut bits = 0u32;
    for &c in src {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            b' ' | b'\t' | b'\r' | b'\n' => continue,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            *out.get_mut(at)? = (acc >> bits) as u8;
            at += 1;
            acc &= (1 << bits) - 1;
        }
    }
    Some(at)
}
