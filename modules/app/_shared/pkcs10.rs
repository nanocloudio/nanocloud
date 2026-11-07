// PKCS#10 for the one shape a device sends: a P-256 key, signed with ECDSA and
// SHA-256, as `openssl req`, mbedTLS and a browser's WebCrypto make it. Base64,
// PEM and just enough DER to read it. Pure, `no_std`, allocation-free, no panic
// path: everything here reads what an unauthenticated network sent. (Acorn's
// `modules/app/_shared/pkcs10.rs`, taken whole; the two should stay one text.)

pub mod pkcs10 {
    const OID_EC_PUBLIC_KEY: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x02, 0x01];
    const OID_PRIME256V1: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x03, 0x01, 0x07];
    const OID_ECDSA_SHA256: &[u8] = &[0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x02];

    const SEQUENCE: u8 = 0x30;
    const INTEGER: u8 = 0x02;
    const BIT_STRING: u8 = 0x03;
    const OID: u8 = 0x06;

    /// A request's parts, its signature not yet checked.
    #[derive(Clone, Copy)]
    pub struct Csr<'a> {
        /// The DER the signature covers, its header included.
        pub info: &'a [u8],
        /// The key: 65 bytes, `0x04 ‖ X ‖ Y`.
        pub point: &'a [u8],
        /// `r ‖ s`.
        pub sig: [u8; 64],
    }

    /// One TLV at the head of `b`: its value and what follows it.
    fn tlv(b: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
        if *b.first()? != tag {
            return None;
        }
        let first = *b.get(1)?;
        let (len, at): (usize, usize) = if first < 0x80 {
            (usize::from(first), 2)
        } else if first == 0x81 {
            let n = usize::from(*b.get(2)?);
            if n < 0x80 {
                return None;
            }
            (n, 3)
        } else if first == 0x82 {
            let n = usize::from(*b.get(2)?) << 8 | usize::from(*b.get(3)?);
            if n < 0x100 {
                return None;
            }
            (n, 4)
        } else {
            return None;
        };
        let end = at.checked_add(len)?;
        Some((b.get(at..end)?, b.get(end..)?))
    }

    /// An INTEGER as 32 big-endian bytes: positive and within them.
    fn int32(v: &[u8], out: &mut [u8]) -> bool {
        if v.is_empty() || v[0] & 0x80 != 0 {
            return false;
        }
        let mut v = v;
        if v.len() > 1 && v[0] == 0 {
            v = &v[1..];
        }
        if v.len() > 32 {
            return false;
        }
        let at = 32 - v.len();
        let mut i = 0;
        while i < v.len() {
            out[at + i] = v[i];
            i += 1;
        }
        true
    }

    pub fn parse(der: &[u8]) -> Option<Csr<'_>> {
        let (outer, rest) = tlv(der, SEQUENCE)?;
        if !rest.is_empty() {
            return None;
        }
        // The signed part, whole: the signature is over its header too.
        let (body, after_info) = tlv(outer, SEQUENCE)?;
        let info = outer.get(..outer.len() - after_info.len())?;

        let (version, r) = tlv(body, INTEGER)?;
        if version != [0] {
            return None;
        }
        let (_subject, r) = tlv(r, SEQUENCE)?;
        let (spki, _attributes) = tlv(r, SEQUENCE)?;
        let (alg, r) = tlv(spki, SEQUENCE)?;
        let (key_oid, alg_rest) = tlv(alg, OID)?;
        let (curve, alg_end) = tlv(alg_rest, OID)?;
        if key_oid != OID_EC_PUBLIC_KEY || curve != OID_PRIME256V1 || !alg_end.is_empty() {
            return None;
        }
        let (bits, r) = tlv(r, BIT_STRING)?;
        if !r.is_empty() || bits.len() != 66 || bits[0] != 0 || bits[1] != 0x04 {
            return None;
        }
        let point = &bits[1..];

        let (sig_alg, r) = tlv(after_info, SEQUENCE)?;
        let (sig_oid, _) = tlv(sig_alg, OID)?;
        if sig_oid != OID_ECDSA_SHA256 {
            return None;
        }
        let (sig_bits, r) = tlv(r, BIT_STRING)?;
        if !r.is_empty() || sig_bits.first() != Some(&0) {
            return None;
        }
        let (rs, r) = tlv(&sig_bits[1..], SEQUENCE)?;
        if !r.is_empty() {
            return None;
        }
        let (rv, tail) = tlv(rs, INTEGER)?;
        let (sv, tail) = tlv(tail, INTEGER)?;
        if !tail.is_empty() {
            return None;
        }
        let mut sig = [0u8; 64];
        let (a, b) = sig.split_at_mut(32);
        if !int32(rv, a) || !int32(sv, b) {
            return None;
        }
        Some(Csr { info, point, sig })
    }

    fn b64_value(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }

    /// Base64 of either alphabet, padded or not, line breaks skipped. `None`
    /// for anything else, or past `out`.
    pub fn b64_decode(src: &[u8], out: &mut [u8]) -> Option<usize> {
        let (mut acc, mut bits, mut n, mut pad) = (0u32, 0u32, 0usize, false);
        for &c in src {
            match c {
                b'\r' | b'\n' => continue,
                b'=' => {
                    pad = true;
                    continue;
                }
                _ => {}
            }
            if pad {
                return None;
            }
            acc = acc << 6 | u32::from(b64_value(c)?);
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                *out.get_mut(n)? = (acc >> bits) as u8;
                n += 1;
                acc &= (1 << bits) - 1;
            }
        }
        Some(n)
    }

    const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

    fn encode(src: &[u8], out: &mut [u8], table: &[u8; 64], pad: bool) -> Option<usize> {
        let mut n = 0;
        for chunk in src.chunks(3) {
            let (a, b, c) = (
                chunk[0],
                chunk.get(1).copied().unwrap_or(0),
                chunk.get(2).copied().unwrap_or(0),
            );
            let v = [
                a >> 2,
                (a & 3) << 4 | b >> 4,
                (b & 15) << 2 | c >> 6,
                c & 63,
            ];
            let take = chunk.len() + 1;
            let mut i = 0;
            while i < 4 {
                if i < take {
                    *out.get_mut(n)? = table[usize::from(v[i])];
                    n += 1;
                } else if pad {
                    *out.get_mut(n)? = b'=';
                    n += 1;
                }
                i += 1;
            }
        }
        Some(n)
    }

    pub fn b64_encode(src: &[u8], out: &mut [u8]) -> Option<usize> {
        encode(src, out, B64, true)
    }

    pub fn b64url_encode(src: &[u8], out: &mut [u8]) -> Option<usize> {
        encode(src, out, B64URL, false)
    }

    fn starts_with(b: &[u8], p: &[u8]) -> bool {
        b.len() >= p.len() && &b[..p.len()] == p
    }

    fn find(b: &[u8], p: &[u8]) -> Option<usize> {
        if p.is_empty() || b.len() < p.len() {
            return None;
        }
        let mut i = 0;
        while i + p.len() <= b.len() {
            if starts_with(&b[i..], p) {
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// `bytes` holds `needle` somewhere.
    pub fn contains(bytes: &[u8], needle: &[u8]) -> bool {
        find(bytes, needle).is_some()
    }

    /// The DER inside a `CERTIFICATE REQUEST` PEM block. A CSR's `spec.request`
    /// is the base64 of this text.
    pub fn pem_request(pem: &[u8], der: &mut [u8]) -> Option<usize> {
        const BEGIN: &[u8] = b"-----BEGIN CERTIFICATE REQUEST-----";
        const END: &[u8] = b"-----END CERTIFICATE REQUEST-----";
        let from = find(pem, BEGIN)? + BEGIN.len();
        let to = from + find(&pem[from..], END)?;
        b64_decode(&pem[from..to], der)
    }

    /// `der` as a `CERTIFICATE` PEM block. The number of bytes written.
    pub fn pem_certificate(der: &[u8], out: &mut [u8]) -> Option<usize> {
        const BEGIN: &[u8] = b"-----BEGIN CERTIFICATE-----\n";
        const END: &[u8] = b"\n-----END CERTIFICATE-----\n";
        let mut n = 0;
        let mut put = |part: &[u8], out: &mut [u8]| -> Option<()> {
            out.get_mut(n..n + part.len())?.copy_from_slice(part);
            n += part.len();
            Some(())
        };
        put(BEGIN, out)?;
        let mut b64 = [0u8; 4096];
        let m = b64_encode(der, &mut b64)?;
        let mut lines = b64[..m].chunks(64).peekable();
        while let Some(line) = lines.next() {
            put(line, out)?;
            if lines.peek().is_some() {
                put(b"\n", out)?;
            }
        }
        put(END, out)?;
        Some(n)
    }
}
