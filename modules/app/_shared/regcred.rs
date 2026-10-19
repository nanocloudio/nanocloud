// A registry's credentials out of a Kubernetes image pull Secret — included
// via `include!("../_shared/regcred.rs")` after `json.rs` and the SDK's
// `crypto/b64.rs` (http_fetch; tests/harness).
//
// A Pod's `imagePullSecrets` (or its ServiceAccount's) name Secrets of type
// `kubernetes.io/dockerconfigjson`: `data[".dockerconfigjson"]` is the base64
// of `{"auths":{"<registry>":{"auth":"<base64 user:password>"}}}` — or of
// `username`/`password` in place of `auth` — and the legacy
// `kubernetes.io/dockercfg` is `data[".dockercfg"]`, the `auths` map alone.
// An entry's key is a registry as a person wrote it: `host[:port]`, or a URL
// (`https://host[:port]/v1/`); it is the registry's host and port that name
// it, so the scheme and any path are dropped before comparing. Docker Hub's
// names (`docker.io`, `index.docker.io`, `registry-1.docker.io`) are one.
//
// What comes out is what an HTTP `Authorization: Basic` header carries — the
// base64 of `user:password` — for the registry itself (a Basic challenge) or
// for the token realm its Bearer challenge names. Bounded: no allocator, the
// caller's buffers, None when anything does not fit.

/// `key` as a registry's `host[:port]`: the scheme and any path dropped.
fn rc_host(key: &[u8]) -> &[u8] {
    let k = if key
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"https://"))
    {
        key.get(8..).unwrap_or(&[])
    } else if key
        .get(..7)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"http://"))
    {
        key.get(7..).unwrap_or(&[])
    } else {
        key
    };
    match k.iter().position(|&c| c == b'/') {
        Some(i) => k.get(..i).unwrap_or(&[]),
        None => k,
    }
}

/// `src` into `dst` from `at`: the end, or None when it does not fit.
fn rc_put(dst: &mut [u8], at: usize, src: &[u8]) -> Option<usize> {
    let end = at.checked_add(src.len())?;
    for (d, c) in dst.get_mut(at..end)?.iter_mut().zip(src) {
        *d = *c;
    }
    Some(end)
}

fn rc_hub(h: &[u8]) -> bool {
    h == b"docker.io" || h == b"index.docker.io" || h == b"registry-1.docker.io"
}

/// Whether an `auths` key names `registry`.
fn rc_names(key: &[u8], registry: &[u8]) -> bool {
    let h = rc_host(key);
    !h.is_empty() && (h.eq_ignore_ascii_case(registry) || (rc_hub(h) && rc_hub(registry)))
}

/// One `auths` entry's credentials as `Authorization: Basic` carries them.
fn rc_entry(entry: &[u8], out: &mut [u8]) -> Option<usize> {
    if let Some(a) = j_get1(entry, b"auth") {
        // Already `base64(user:password)`; checked, not trusted to be. (Not
        // `contains`: on a byte slice it links memchr, which a PIC module
        // cannot resolve.)
        let mut probe = [0u8; 384];
        if !a.is_empty()
            && b64_decode(a, &mut probe).is_some_and(|n| {
                probe
                    .get(..n)
                    .is_some_and(|p| !p.iter().all(|&c| c != b':'))
            })
        {
            return rc_put(out, 0, a);
        }
    }
    let u = j_get1(entry, b"username")?;
    let p = j_get1(entry, b"password")?;
    if u.is_empty() {
        return None;
    }
    let mut up = [0u8; 384];
    let n = rc_put(&mut up, 0, u)?;
    let n = rc_put(&mut up, n, b":")?;
    let n = rc_put(&mut up, n, p)?;
    b64_encode(up.get(..n)?, out)
}

/// The members of the JSON object `obj`, each `(key, value)`, in order;
/// `at` is the walk's position (0 to begin). None at the end.
fn rc_member<'a>(obj: &'a [u8], at: &mut usize) -> Option<(&'a [u8], &'a [u8])> {
    let mut j = j_ws(obj, *at);
    if *at == 0 {
        if obj.get(j) != Some(&b'{') {
            return None;
        }
        j += 1;
    }
    j = j_ws(obj, j);
    if obj.get(j) == Some(&b',') {
        j = j_ws(obj, j + 1);
    }
    if obj.get(j) != Some(&b'"') {
        return None;
    }
    let ks = j + 1;
    let mut ke = ks;
    while let Some(&c) = obj.get(ke) {
        if c == b'"' {
            break;
        }
        ke += if c == b'\\' { 2 } else { 1 };
    }
    let key = obj.get(ks..ke)?;
    j = j_ws(obj, ke + 1);
    if obj.get(j) != Some(&b':') {
        return None;
    }
    let vs = j_ws(obj, j + 1);
    let ve = j_skip(obj, vs);
    *at = ve;
    Some((key, obj.get(vs..ve)?))
}

/// The credentials a pull Secret (`secret`, the object as the API stores it)
/// holds for `registry` (`host[:port]`), into `out` as `base64(user:password)`
/// — `scratch` holds the decoded config. None when the Secret is not an
/// image pull Secret or names no such registry.
fn rc_basic_for(
    secret: &[u8],
    registry: &[u8],
    scratch: &mut [u8],
    out: &mut [u8],
) -> Option<usize> {
    let ty = j_get1(secret, b"type")?;
    // One call per type: the member's name is a literal of its own arm.
    if ty == b"kubernetes.io/dockerconfigjson" {
        rc_config(secret, b".dockerconfigjson", true, registry, scratch, out)
    } else if ty == b"kubernetes.io/dockercfg" {
        rc_config(secret, b".dockercfg", false, registry, scratch, out)
    } else {
        None
    }
}

fn rc_config(
    secret: &[u8],
    member: &[u8],
    nested: bool,
    registry: &[u8],
    scratch: &mut [u8],
    out: &mut [u8],
) -> Option<usize> {
    let data = j_sub1(secret, b"data")?;
    let mut at = 0usize;
    let mut enc: &[u8] = &[];
    while let Some((k, v)) = rc_member(data, &mut at) {
        if k == member {
            enc = j_scalar(v, 0);
            break;
        }
    }
    if enc.is_empty() {
        return None;
    }
    let n = b64_decode(enc, scratch)?;
    let cfg = scratch.get(..n)?;
    let auths = if nested { j_sub1(cfg, b"auths")? } else { cfg };
    let mut at = 0usize;
    while let Some((k, v)) = rc_member(auths, &mut at) {
        if rc_names(k, registry) {
            if let Some(n) = rc_entry(v, out) {
                return Some(n);
            }
        }
    }
    None
}

/// An `/image-pull-auth/<uid>` record (`rk=<pull>;s=<ns>/<name>,<name>…`):
/// whether it is for `pull`, and its namespace and its Secrets' names.
fn rc_record<'a>(rec: &'a [u8], pull: &[u8]) -> Option<(&'a [u8], &'a [u8])> {
    let mut rk: &[u8] = &[];
    let mut s: &[u8] = &[];
    for part in rec.split(|&c| c == b';') {
        if let Some(v) = part.strip_prefix(b"rk=") {
            rk = v;
        } else if let Some(v) = part.strip_prefix(b"s=") {
            s = v;
        }
    }
    if rk.is_empty() || rk != pull {
        return None;
    }
    let slash = s.iter().position(|&c| c == b'/')?;
    let (ns, names) = (s.get(..slash)?, s.get(slash + 1..)?);
    if ns.is_empty() || names.is_empty() {
        return None;
    }
    Some((ns, names))
}
