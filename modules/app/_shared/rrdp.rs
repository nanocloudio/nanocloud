// RRDP (RFC 8182), the RPKI Repository Delta Protocol, as a relying party
// fetching a repository's objects needs it: the notification file names the
// session, its serial and the snapshot (its URI and the SHA-256 of its
// bytes); the snapshot carries every published object, base64, under the
// same session and serial. Parsing only — the fetch is the caller's, and an
// object's meaning is `rpki.rs`'s. I/O-free and allocation-free.
//
// What is read, strictly:
//
// - `<notification xmlns="http://www.ripe.net/rpki/rrdp" version="1"
//   session_id="…" serial="…"> <snapshot uri="…" hash="…"/> …` — version 1,
//   a session id (a UUID, 36 bytes), a decimal serial, one snapshot whose
//   hash is 64 hex digits; deltas are not used (a relying party that only
//   takes snapshots is correct, if not frugal);
// - `<snapshot … version="1" session_id="…" serial="…"> <publish uri="…">
//   base64 </publish> … </snapshot>` — the session and serial the
//   notification named, each publish's URI and its base64 body (whitespace
//   inside it skipped by the decoder's caller).
//
// Anything else — another version, a missing attribute, a snapshot of
// another session or serial — is refused, never guessed at.

pub const RRDP_SESSION_LEN: usize = 36;
pub const RRDP_URI_MAX: usize = 192;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RrdpWhy {
    Malformed,
    Version,
    /// The snapshot is of another session or serial than the notification's.
    NotThisSnapshot,
    /// The snapshot's bytes do not hash to the notification's hash.
    Hash,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RrdpNotification {
    pub session: [u8; RRDP_SESSION_LEN],
    pub serial: u64,
    pub snapshot_uri: [u8; RRDP_URI_MAX],
    pub snapshot_uri_len: usize,
    pub snapshot_hash: [u8; 32],
}

fn rrdp_find(h: &[u8], n: &[u8]) -> Option<usize> {
    if n.is_empty() || h.len() < n.len() {
        return None;
    }
    (0..=h.len() - n.len()).find(|&i| &h[i..i + n.len()] == n)
}

/// The value of attribute `name` in the tag text `tag` (between `<` and
/// `>`): `name="value"`, the name standing alone.
fn rrdp_attr<'a>(tag: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    let mut at = 0;
    while let Some(i) = rrdp_find(&tag[at..], name) {
        let s = at + i;
        let before_ok = s == 0 || tag[s - 1].is_ascii_whitespace();
        let rest = &tag[s + name.len()..];
        if before_ok && rest.len() >= 2 && rest[0] == b'=' && rest[1] == b'"' {
            let v = &rest[2..];
            let e = v.iter().position(|&c| c == b'"')?;
            return Some(&v[..e]);
        }
        at = s + name.len();
    }
    None
}

/// The first element named `name`: its tag text and the index after it.
fn rrdp_tag<'a>(x: &'a [u8], from: usize, name: &[u8]) -> Option<(&'a [u8], usize)> {
    let mut at = from;
    loop {
        let i = at + rrdp_find(&x[at..], b"<")?;
        let rest = &x[i + 1..];
        if rest.len() > name.len()
            && rest.starts_with(name)
            && (rest[name.len()].is_ascii_whitespace() || matches!(rest[name.len()], b'>' | b'/'))
        {
            let e = i + 1 + rrdp_find(rest, b">")?;
            return Some((&x[i + 1..e], e + 1));
        }
        at = i + 1;
    }
}

fn rrdp_dec(v: &[u8]) -> Option<u64> {
    if v.is_empty() || v.len() > 19 {
        return None;
    }
    let mut n = 0u64;
    for &c in v {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + u64::from(c - b'0');
    }
    Some(n)
}

fn rrdp_hex32(v: &[u8]) -> Option<[u8; 32]> {
    if v.len() != 64 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    };
    let mut o = [0u8; 32];
    for i in 0..32 {
        o[i] = (nib(v[2 * i])? << 4) | nib(v[2 * i + 1])?;
    }
    Some(o)
}

/// The root element's session and serial, version 1.
fn rrdp_root(x: &[u8], name: &[u8]) -> Result<([u8; RRDP_SESSION_LEN], u64, usize), RrdpWhy> {
    let (tag, after) = rrdp_tag(x, 0, name).ok_or(RrdpWhy::Malformed)?;
    if rrdp_attr(tag, b"version") != Some(&b"1"[..]) {
        return Err(RrdpWhy::Version);
    }
    let sid = rrdp_attr(tag, b"session_id").ok_or(RrdpWhy::Malformed)?;
    if sid.len() != RRDP_SESSION_LEN {
        return Err(RrdpWhy::Malformed);
    }
    let serial = rrdp_attr(tag, b"serial")
        .and_then(rrdp_dec)
        .ok_or(RrdpWhy::Malformed)?;
    let mut session = [0u8; RRDP_SESSION_LEN];
    session.copy_from_slice(sid);
    Ok((session, serial, after))
}

/// Read a notification file.
pub fn rrdp_notification(x: &[u8]) -> Result<RrdpNotification, RrdpWhy> {
    let (session, serial, after) = rrdp_root(x, b"notification")?;
    let (snap, _) = rrdp_tag(x, after, b"snapshot").ok_or(RrdpWhy::Malformed)?;
    let uri = rrdp_attr(snap, b"uri").ok_or(RrdpWhy::Malformed)?;
    let hash = rrdp_attr(snap, b"hash")
        .and_then(rrdp_hex32)
        .ok_or(RrdpWhy::Malformed)?;
    if uri.is_empty() || uri.len() > RRDP_URI_MAX {
        return Err(RrdpWhy::Malformed);
    }
    let mut n = RrdpNotification {
        session,
        serial,
        snapshot_uri: [0; RRDP_URI_MAX],
        snapshot_uri_len: uri.len(),
        snapshot_hash: hash,
    };
    n.snapshot_uri[..uri.len()].copy_from_slice(uri);
    Ok(n)
}

/// Check a snapshot against its notification and hand each published
/// object (`uri`, base64 body) to `each`; the objects' count.
pub fn rrdp_snapshot(
    x: &[u8],
    n: &RrdpNotification,
    each: &mut dyn FnMut(&[u8], &[u8]),
) -> Result<usize, RrdpWhy> {
    if sha256(x) != n.snapshot_hash {
        return Err(RrdpWhy::Hash);
    }
    let (session, serial, mut at) = rrdp_root(x, b"snapshot")?;
    if session != n.session || serial != n.serial {
        return Err(RrdpWhy::NotThisSnapshot);
    }
    let mut count = 0;
    while let Some((tag, after)) = rrdp_tag(x, at, b"publish") {
        let uri = rrdp_attr(tag, b"uri").ok_or(RrdpWhy::Malformed)?;
        let end = after + rrdp_find(&x[after..], b"</publish>").ok_or(RrdpWhy::Malformed)?;
        each(uri, &x[after..end]);
        count += 1;
        at = end + b"</publish>".len();
    }
    Ok(count)
}

/// `b64` with every whitespace byte dropped, decoded into `out`; its length.
pub fn rrdp_b64(b64: &[u8], scratch: &mut [u8], out: &mut [u8]) -> Option<usize> {
    let mut k = 0;
    for &c in b64 {
        if c.is_ascii_whitespace() {
            continue;
        }
        *scratch.get_mut(k)? = c;
        k += 1;
    }
    b64_decode(&scratch[..k], out)
}
