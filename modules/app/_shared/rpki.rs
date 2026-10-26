// An RPKI relying party for route-origin authorisations (rfc_lb_vip
// §11.15, §22 "Nanocloud: an RPKI relying party feeding VRPs"): a ROA
// (RFC 9582) — a CMS signed object (RFC 6488) carrying a
// `RouteOriginAttestation` and the end-entity certificate that signed it —
// validated against a configured trust anchor's certificate, and turned
// into the validated ROA payloads (`traffic::Vrp`: prefix, max length,
// origin AS) Wormhole's address lifecycle runs RFC 6811 origin validation
// over (`address_core.rs` `origin_validate`).
//
// What is checked, in order, each a refusal by name:
//
// - the trust anchor: a self-signed RSA certificate (sha256WithRSA), valid
//   at `now`, carrying RFC 3779 IP resources;
// - the CMS wrapper: `signedData`, the encapsulated content type
//   `id-ct-routeOriginAuthz`, exactly one certificate and one signer;
// - the EE certificate: signed by the trust anchor under its key, valid at
//   `now`, its RFC 3779 IP resources (or `inherit`, the anchor's) inside the
//   anchor's;
// - the signer: signed attributes present, `content-type` the ROA's,
//   `message-digest` the SHA-256 of the eContent, and the signature over
//   those attributes verifying under the EE certificate's key;
// - the ROA: version 0, every prefix inside the EE certificate's
//   resources, every max length between the prefix length and the family's
//   width.
//
// Not done here (each a stated limit, not an omission hidden): fetching
// (rsync/RRDP) — objects are handed in; CRLs and manifests; certificate
// chains deeper than anchor → EE (no intermediate CA certificates); AS
// resource checks; address ranges that are not prefixes. A deployment's
// fetcher hands this core the objects; tests hand it fixture ROAs.
//
// Needs `sha256` in scope, the SDK's `rsa.rs` included (with `sha256` and
// `sha384`), and `traffic` (`_shared/wormhole_traffic.rs`) first.
// Allocation-free; the caller owns the RSA job.

pub const RPKI_RESOURCES_MAX: usize = 16;
pub const ROA_PREFIXES_MAX: usize = 32;

const OID_SIGNED_DATA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x07, 0x02];
const OID_ROA: &[u8] = &[
    0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x10, 0x01, 0x18,
];
const OID_CONTENT_TYPE: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x03];
const OID_MESSAGE_DIGEST: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x04];
const OID_SHA256_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0b];
const OID_RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01];
const OID_IP_ADDR_BLOCKS: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x01, 0x07];

/// Why an object was refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RpkiRefusal {
    /// Not the DER this profile reads.
    Malformed,
    /// A signature algorithm other than sha256WithRSAEncryption, or a key
    /// that is not RSA.
    Algorithm,
    AnchorSignature,
    /// Outside its validity at `now`.
    Expired,
    /// The EE certificate is not the anchor's.
    IssuerSignature,
    /// Not a ROA, or not exactly one certificate and one signer.
    Shape,
    /// A signed attribute is missing or disagrees with the content.
    Attributes,
    SignerSignature,
    /// A resource outside its issuer's.
    Resources,
    /// A prefix or max length the ROA may not carry.
    Prefix,
}

// ── a minimal DER reader ──────────────────────────────────────────────────

/// One TLV: tag, contents and the whole encoding.
#[derive(Clone, Copy)]
struct Tlv<'a> {
    tag: u8,
    body: &'a [u8],
    whole: &'a [u8],
}

fn tlv(b: &[u8]) -> Option<(Tlv<'_>, &[u8])> {
    let tag = *b.first()?;
    let l0 = *b.get(1)?;
    let (len, hdr) = if l0 < 0x80 {
        (usize::from(l0), 2)
    } else {
        let n = usize::from(l0 & 0x7f);
        if n == 0 || n > 3 {
            return None;
        }
        let mut len = 0usize;
        for i in 0..n {
            len = (len << 8) | usize::from(*b.get(2 + i)?);
        }
        (len, 2 + n)
    };
    let end = hdr.checked_add(len)?;
    Some((
        Tlv {
            tag,
            body: b.get(hdr..end)?,
            whole: &b[..end],
        },
        &b[end..],
    ))
}

fn expect(b: &[u8], tag: u8) -> Result<(Tlv<'_>, &[u8]), RpkiRefusal> {
    match tlv(b) {
        Some((t, rest)) if t.tag == tag => Ok((t, rest)),
        _ => Err(RpkiRefusal::Malformed),
    }
}

/// Seconds since 1970 of a UTCTime (`YYMMDDHHMMSSZ`) or GeneralizedTime.
fn der_time(t: &Tlv<'_>) -> Option<u64> {
    let d = t.body;
    let num = |s: &[u8]| -> Option<u64> {
        let mut v = 0u64;
        for c in s {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + u64::from(c - b'0');
        }
        Some(v)
    };
    let (year, rest) = match t.tag {
        0x17 if d.len() == 13 => {
            let y = num(&d[0..2])?;
            (if y >= 50 { 1900 + y } else { 2000 + y }, &d[2..])
        }
        0x18 if d.len() == 15 => (num(&d[0..4])?, &d[4..]),
        _ => return None,
    };
    if rest.last() != Some(&b'Z') {
        return None;
    }
    let (mo, da, h, mi, s) = (
        num(&rest[0..2])?,
        num(&rest[2..4])?,
        num(&rest[4..6])?,
        num(&rest[6..8])?,
        num(&rest[8..10])?,
    );
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if mo <= 2 { year - 1 } else { year };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + da - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe;
    let days = days.checked_sub(719_468)?;
    Some(days * 86_400 + h * 3600 + mi * 60 + s)
}

// ── certificates ──────────────────────────────────────────────────────────

/// An IP prefix as RFC 3779 and ROAs encode it: family (4 or 6), the
/// address bits and the prefix length.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RpkiPrefix {
    pub family: u8,
    pub addr: [u8; 16],
    pub len: u8,
}

impl RpkiPrefix {
    /// Whether `other` lies inside this prefix.
    pub fn covers(&self, other: &RpkiPrefix) -> bool {
        if self.family != other.family || other.len < self.len {
            return false;
        }
        let full = usize::from(self.len / 8);
        if self.addr[..full] != other.addr[..full] {
            return false;
        }
        let rem = self.len % 8;
        if rem == 0 {
            return true;
        }
        let mask = 0xFFu8 << (8 - rem);
        self.addr[full] & mask == other.addr[full] & mask
    }
}

/// A BIT STRING address (`IPAddress`) of `family`.
fn bit_prefix(t: &Tlv<'_>, family: u8) -> Option<RpkiPrefix> {
    if t.tag != 0x03 || t.body.is_empty() {
        return None;
    }
    let unused = t.body[0];
    let bits = &t.body[1..];
    let width = if family == 4 { 4 } else { 16 };
    if unused > 7 || bits.len() > width || (bits.is_empty() && unused != 0) {
        return None;
    }
    let len = bits.len() * 8 - usize::from(unused);
    let mut addr = [0u8; 16];
    addr[..bits.len()].copy_from_slice(bits);
    Some(RpkiPrefix {
        family,
        addr,
        len: len as u8,
    })
}

fn afi_family(t: &Tlv<'_>) -> Option<u8> {
    if t.tag != 0x04 || t.body.len() < 2 {
        return None;
    }
    match (t.body[0], t.body[1]) {
        (0, 1) => Some(4),
        (0, 2) => Some(6),
        _ => None,
    }
}

/// A certificate's RFC 3779 IP resources: prefixes, or `inherit`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct IpResources {
    pub inherit: bool,
    pub n: usize,
    pub prefixes: [RpkiPrefix; RPKI_RESOURCES_MAX],
}

impl IpResources {
    pub fn prefixes(&self) -> &[RpkiPrefix] {
        &self.prefixes[..self.n]
    }

    pub fn covers(&self, p: &RpkiPrefix) -> bool {
        self.prefixes().iter().any(|r| r.covers(p))
    }
}

/// `IPAddrBlocks ::= SEQUENCE OF IPAddressFamily` (prefixes and inherit).
fn ip_resources(der: &[u8]) -> Result<IpResources, RpkiRefusal> {
    let bad = RpkiRefusal::Malformed;
    let (blocks, _) = expect(der, 0x30)?;
    let mut r = IpResources::default();
    let mut rest = blocks.body;
    while !rest.is_empty() {
        let (fam, next) = expect(rest, 0x30)?;
        rest = next;
        let (afi, choice) = tlv(fam.body).ok_or(bad)?;
        let family = afi_family(&afi).ok_or(bad)?;
        let (c, _) = tlv(choice).ok_or(bad)?;
        match c.tag {
            0x05 => r.inherit = true,
            0x30 => {
                let mut items = c.body;
                while !items.is_empty() {
                    let (item, next) = tlv(items).ok_or(bad)?;
                    items = next;
                    // A prefix; a range (a SEQUENCE) is not read here.
                    let p = bit_prefix(&item, family).ok_or(RpkiRefusal::Resources)?;
                    if r.n == RPKI_RESOURCES_MAX {
                        return Err(RpkiRefusal::Resources);
                    }
                    r.prefixes[r.n] = p;
                    r.n += 1;
                }
            }
            _ => return Err(bad),
        }
    }
    Ok(r)
}

/// What a certificate says that this profile uses.
#[derive(Clone, Copy)]
pub struct Cert<'a> {
    tbs: &'a [u8],
    signature: &'a [u8],
    not_before: u64,
    not_after: u64,
    /// The `RSAPublicKey` inside the subject public key info.
    rsa_key: &'a [u8],
    pub resources: IpResources,
}

pub fn cert_parse(der: &[u8]) -> Result<Cert<'_>, RpkiRefusal> {
    let bad = RpkiRefusal::Malformed;
    let (cert, _) = expect(der, 0x30)?;
    let (tbs, rest) = expect(cert.body, 0x30)?;
    let (alg, rest) = expect(rest, 0x30)?;
    let (sig, _) = expect(rest, 0x03)?;
    let (oid, _) = expect(alg.body, 0x06)?;
    if oid.body != OID_SHA256_RSA {
        return Err(RpkiRefusal::Algorithm);
    }
    let mut f = tbs.body;
    // [0] version, serial, signature, issuer, validity, subject, spki.
    if f.first() == Some(&0xa0) {
        f = tlv(f).ok_or(bad)?.1;
    }
    let (_, f) = expect(f, 0x02)?;
    let (_, f) = expect(f, 0x30)?;
    let (_, f) = expect(f, 0x30)?;
    let (validity, f) = expect(f, 0x30)?;
    let (_, f) = expect(f, 0x30)?;
    let (spki, mut f) = expect(f, 0x30)?;
    let (nb, rest_v) = tlv(validity.body).ok_or(bad)?;
    let (na, _) = tlv(rest_v).ok_or(bad)?;
    let (alg, key) = expect(spki.body, 0x30)?;
    let (koid, _) = expect(alg.body, 0x06)?;
    if koid.body != OID_RSA {
        return Err(RpkiRefusal::Algorithm);
    }
    let (bits, _) = expect(key, 0x03)?;
    if bits.body.first() != Some(&0) {
        return Err(bad);
    }
    let mut resources = IpResources::default();
    while !f.is_empty() {
        let (t, next) = tlv(f).ok_or(bad)?;
        f = next;
        if t.tag != 0xa3 {
            continue;
        }
        let (exts, _) = expect(t.body, 0x30)?;
        let mut e = exts.body;
        while !e.is_empty() {
            let (ext, next) = expect(e, 0x30)?;
            e = next;
            let (eoid, mut v) = expect(ext.body, 0x06)?;
            if v.first() == Some(&0x01) {
                v = tlv(v).ok_or(bad)?.1;
            }
            let (val, _) = expect(v, 0x04)?;
            if eoid.body == OID_IP_ADDR_BLOCKS {
                resources = ip_resources(val.body)?;
            }
        }
    }
    let sig_bits = sig.body.split_first().ok_or(bad)?;
    if *sig_bits.0 != 0 {
        return Err(bad);
    }
    Ok(Cert {
        tbs: tbs.whole,
        signature: sig_bits.1,
        not_before: der_time(&nb).ok_or(bad)?,
        not_after: der_time(&na).ok_or(bad)?,
        rsa_key: &bits.body[1..],
        resources,
    })
}

fn rsa_ok(job: &mut RsaVerifyJob, key: &[u8], msg: &[u8], sig: &[u8]) -> bool {
    let Some(k) = rsa_public_key_parse(key) else {
        return false;
    };
    rsa_pkcs1_v15_verify(job, &k, RsaHash::Sha256, &sha256(msg), sig)
}

/// A trust anchor: its self-signed certificate, checked at `now`.
pub fn anchor_check<'a>(
    job: &mut RsaVerifyJob,
    der: &'a [u8],
    now_s: u64,
) -> Result<Cert<'a>, RpkiRefusal> {
    let c = cert_parse(der)?;
    if !rsa_ok(job, c.rsa_key, c.tbs, c.signature) {
        return Err(RpkiRefusal::AnchorSignature);
    }
    if now_s < c.not_before || now_s > c.not_after {
        return Err(RpkiRefusal::Expired);
    }
    if c.resources.inherit || c.resources.n == 0 {
        return Err(RpkiRefusal::Resources);
    }
    Ok(c)
}

// ── the ROA ───────────────────────────────────────────────────────────────

/// The validated payloads of one ROA.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RoaVrps {
    pub asn: u32,
    pub n: usize,
    pub vrps: [traffic::Vrp; ROA_PREFIXES_MAX],
}

fn der_uint(t: &Tlv<'_>) -> Option<u64> {
    if t.tag != 0x02 || t.body.is_empty() || t.body.len() > 9 {
        return None;
    }
    if t.body[0] & 0x80 != 0 {
        return None;
    }
    let mut v = 0u64;
    for b in t.body {
        v = (v << 8) | u64::from(*b);
    }
    Some(v)
}

/// Validate a ROA (DER CMS) against the anchor certificate at `now_s`:
/// its validated ROA payloads, or why not.
pub fn roa_validate(
    job: &mut RsaVerifyJob,
    anchor_der: &[u8],
    roa_der: &[u8],
    now_s: u64,
) -> Result<RoaVrps, RpkiRefusal> {
    let (econtent, ee_res) = signed_object(job, anchor_der, roa_der, OID_ROA, now_s)?;
    roa_content(econtent, &ee_res)
}

/// An RPKI signed object (RFC 6488) of eContent type `econtent_type`,
/// checked against the anchor at `now_s` as `roa_validate` describes: its
/// eContent and the EE certificate's resources (the anchor's when it
/// inherits).
pub fn signed_object<'a>(
    job: &mut RsaVerifyJob,
    anchor_der: &[u8],
    der: &'a [u8],
    econtent_type: &[u8],
    now_s: u64,
) -> Result<(&'a [u8], IpResources), RpkiRefusal> {
    let bad = RpkiRefusal::Malformed;
    let anchor = anchor_check(job, anchor_der, now_s)?;
    // ContentInfo { contentType, [0] SignedData }
    let (ci, _) = expect(der, 0x30)?;
    let (ct, rest) = expect(ci.body, 0x06)?;
    if ct.body != OID_SIGNED_DATA {
        return Err(RpkiRefusal::Shape);
    }
    let (wrap, _) = expect(rest, 0xa0)?;
    let (sd, _) = expect(wrap.body, 0x30)?;
    let (_, f) = expect(sd.body, 0x02)?;
    let (_, f) = expect(f, 0x31)?;
    let (encap, f) = expect(f, 0x30)?;
    let (ect, erest) = expect(encap.body, 0x06)?;
    if ect.body != econtent_type {
        return Err(RpkiRefusal::Shape);
    }
    let (ew, _) = expect(erest, 0xa0)?;
    let (econtent, _) = expect(ew.body, 0x04)?;
    let (certs, f) = expect(f, 0xa0)?;
    let (ee_t, more) = tlv(certs.body).ok_or(bad)?;
    if !more.is_empty() {
        return Err(RpkiRefusal::Shape);
    }
    let mut f = f;
    if f.first() == Some(&0xa1) {
        f = tlv(f).ok_or(bad)?.1;
    }
    let (signers, _) = expect(f, 0x31)?;
    let (si, more) = expect(signers.body, 0x30)?;
    if !more.is_empty() {
        return Err(RpkiRefusal::Shape);
    }
    // The EE certificate under the anchor.
    let ee = cert_parse(ee_t.whole)?;
    if !rsa_ok(job, anchor.rsa_key, ee.tbs, ee.signature) {
        return Err(RpkiRefusal::IssuerSignature);
    }
    if now_s < ee.not_before || now_s > ee.not_after {
        return Err(RpkiRefusal::Expired);
    }
    let ee_res = if ee.resources.inherit {
        anchor.resources
    } else {
        ee.resources
    };
    if ee_res
        .prefixes()
        .iter()
        .any(|p| !anchor.resources.covers(p))
    {
        return Err(RpkiRefusal::Resources);
    }
    // SignerInfo { version, sid, digestAlg, [0] signedAttrs, sigAlg, sig }
    let (_, s) = expect(si.body, 0x02)?;
    let (_, s) = tlv(s).ok_or(bad)?;
    let (_, s) = expect(s, 0x30)?;
    let (attrs, s) = expect(s, 0xa0)?;
    let (salg, s) = expect(s, 0x30)?;
    let (sig, _) = expect(s, 0x04)?;
    let (soid, _) = expect(salg.body, 0x06)?;
    if soid.body != OID_SHA256_RSA && soid.body != OID_RSA {
        return Err(RpkiRefusal::Algorithm);
    }
    let mut content_type_ok = false;
    let mut digest_ok = false;
    let mut a = attrs.body;
    while !a.is_empty() {
        let (attr, next) = expect(a, 0x30)?;
        a = next;
        let (aoid, vals) = expect(attr.body, 0x06)?;
        let (set, _) = expect(vals, 0x31)?;
        let (v, _) = tlv(set.body).ok_or(bad)?;
        if aoid.body == OID_CONTENT_TYPE {
            content_type_ok = v.tag == 0x06 && v.body == econtent_type;
        } else if aoid.body == OID_MESSAGE_DIGEST {
            digest_ok = v.tag == 0x04 && v.body == sha256(econtent.body);
        }
    }
    if !content_type_ok || !digest_ok {
        return Err(RpkiRefusal::Attributes);
    }
    // The signature covers the attributes as a SET (tag 0x31).
    let mut signed = [0u8; 1024];
    let n = attrs.whole.len();
    if n > signed.len() {
        return Err(bad);
    }
    signed[..n].copy_from_slice(attrs.whole);
    signed[0] = 0x31;
    if !rsa_ok(job, ee.rsa_key, &signed[..n], sig.body) {
        return Err(RpkiRefusal::SignerSignature);
    }
    Ok((econtent.body, ee_res))
}

/// `RouteOriginAttestation`, every prefix inside `res`.
fn roa_content(der: &[u8], res: &IpResources) -> Result<RoaVrps, RpkiRefusal> {
    let bad = RpkiRefusal::Malformed;
    let (roa, _) = expect(der, 0x30)?;
    let mut f = roa.body;
    if f.first() == Some(&0xa0) {
        let (v, next) = tlv(f).ok_or(bad)?;
        let (ver, _) = tlv(v.body).ok_or(bad)?;
        if der_uint(&ver) != Some(0) {
            return Err(RpkiRefusal::Shape);
        }
        f = next;
    }
    let (asn_t, f) = expect(f, 0x02)?;
    let asn = der_uint(&asn_t)
        .filter(|a| *a <= u64::from(u32::MAX))
        .ok_or(bad)? as u32;
    let (blocks, _) = expect(f, 0x30)?;
    let mut out = RoaVrps {
        asn,
        n: 0,
        vrps: [traffic::Vrp {
            prefix: traffic::BgpPrefix::zero(),
            max_len: 0,
            asn: 0,
        }; ROA_PREFIXES_MAX],
    };
    let mut b = blocks.body;
    while !b.is_empty() {
        let (fam, next) = expect(b, 0x30)?;
        b = next;
        let (afi, addrs) = tlv(fam.body).ok_or(bad)?;
        let family = afi_family(&afi).ok_or(bad)?;
        let (list, _) = expect(addrs, 0x30)?;
        let mut l = list.body;
        while !l.is_empty() {
            let (ra, next) = expect(l, 0x30)?;
            l = next;
            let (bits, rest) = tlv(ra.body).ok_or(bad)?;
            let p = bit_prefix(&bits, family).ok_or(RpkiRefusal::Prefix)?;
            let width = if family == 4 { 32 } else { 128 };
            let max_len = if rest.is_empty() {
                u64::from(p.len)
            } else {
                der_uint(&tlv(rest).ok_or(bad)?.0).ok_or(bad)?
            };
            if max_len < u64::from(p.len) || max_len > width {
                return Err(RpkiRefusal::Prefix);
            }
            if !res.covers(&p) {
                return Err(RpkiRefusal::Resources);
            }
            if out.n == ROA_PREFIXES_MAX {
                return Err(RpkiRefusal::Prefix);
            }
            let addr = if family == 4 {
                traffic::NetAddr::v4([p.addr[0], p.addr[1], p.addr[2], p.addr[3]])
            } else {
                traffic::NetAddr::v6(p.addr)
            };
            out.vrps[out.n] = traffic::Vrp {
                prefix: traffic::BgpPrefix { addr, len: p.len },
                max_len: max_len as u8,
                asn,
            };
            out.n += 1;
        }
    }
    if out.n == 0 {
        return Err(RpkiRefusal::Prefix);
    }
    Ok(out)
}
