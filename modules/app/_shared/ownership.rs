// The ownership-evidence verifier (rfc_lb_vip §11.15, §22 "an ownership
// verifier"): whether a tenant holds an address prefix it asks to import
// is a fact about resource certificates, proven here with an RPKI Signed
// Checklist (RSC, RFC 9323) — a CMS signed object (RFC 6488) whose EE
// certificate the resource holder's CA issued under the trust anchor,
// listing the IP resources it speaks for and the SHA-256 of the documents
// it signs. The document the tenant has its CA sign is the import's
// challenge (`ownership_challenge`: the tenant and the prefix, so a
// checklist proves one tenant's claim to one prefix and nothing else).
//
// `ownership_verify` gives Wormhole's `OwnershipEvidence` (`address_core`'s
// import admission requires `verified`) only when, in order, each a refusal
// by name:
//
// - the RSC is a valid signed object under the anchor (`rpki.rs`
//   `signed_object`: the anchor, the EE certificate under it and inside its
//   resources, the signer, its signed attributes and the signature);
// - its content is an RSC: version 0, a resource block whose IP blocks are
//   prefixes inside the EE certificate's resources, SHA-256 as the digest
//   algorithm, a non-empty checklist;
// - one of its IP blocks covers the prefix claimed;
// - its checklist holds the SHA-256 of this tenant's and prefix's challenge.
//
// The evidence's digest is the SHA-256 of the RSC itself, so what was
// accepted can be named. AS resources in the block are allowed and not
// used. Needs `rpki.rs` (and what it needs) first. Allocation-free.

const OID_RSC: &[u8] = &[
    0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x10, 0x01, 0x30,
];
const OID_SHA256: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01];
pub const OWNERSHIP_CHALLENGE_LEN: usize = 21 + 16 + 1 + 16 + 1;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OwnershipRefusal {
    /// The signed object did not validate.
    Rpki(RpkiRefusal),
    /// Not an RSC this profile reads.
    Shape,
    /// The checklist's resources are not inside the EE certificate's.
    Resources,
    /// No resource of the checklist covers the prefix claimed.
    NotCovered,
    /// The checklist does not sign this tenant's claim to this prefix.
    NotChallenged,
}

/// The document a tenant has its resource holder's CA list in an RSC to
/// prove it holds `prefix`: `wormhole-ownership.v1` then the tenant, the
/// family, the address (16 bytes, IPv4 in the last four) and the length.
pub fn ownership_challenge(
    tenant: &[u8; 16],
    prefix: &traffic::BgpPrefix,
) -> [u8; OWNERSHIP_CHALLENGE_LEN] {
    let mut c = [0u8; OWNERSHIP_CHALLENGE_LEN];
    c[..21].copy_from_slice(b"wormhole-ownership.v1");
    c[21..37].copy_from_slice(tenant);
    c[37] = prefix.addr.family;
    c[38..54].copy_from_slice(&prefix.addr.bytes);
    c[54] = prefix.len;
    c
}

fn rsc_prefix(p: &traffic::BgpPrefix) -> RpkiPrefix {
    let mut addr = [0u8; 16];
    if p.addr.family == traffic::AF_INET {
        addr[..4].copy_from_slice(&p.addr.bytes[12..16]);
    } else {
        addr = p.addr.bytes;
    }
    RpkiPrefix {
        family: p.addr.family,
        addr,
        len: p.len,
    }
}

/// Verify `rsc_der` as `tenant`'s evidence that it holds `prefix`.
pub fn ownership_verify(
    job: &mut RsaVerifyJob,
    anchor_der: &[u8],
    rsc_der: &[u8],
    tenant: &[u8; 16],
    prefix: &traffic::BgpPrefix,
    now_s: u64,
) -> Result<traffic::OwnershipEvidence, OwnershipRefusal> {
    let shape = OwnershipRefusal::Shape;
    let (content, ee_res) =
        signed_object(job, anchor_der, rsc_der, OID_RSC, now_s).map_err(OwnershipRefusal::Rpki)?;
    // RpkiSignedChecklist { version [0] DEFAULT 0, resources ResourceBlock,
    //   digestAlgorithm AlgorithmIdentifier, checkList SEQUENCE OF FileNameAndHash }
    let (rsc, _) = expect(content, 0x30).map_err(|_| shape)?;
    let mut f = rsc.body;
    if f.first() == Some(&0xa0) {
        let (v, next) = tlv(f).ok_or(shape)?;
        let (ver, _) = tlv(v.body).ok_or(shape)?;
        if der_uint(&ver) != Some(0) {
            return Err(shape);
        }
        f = next;
    }
    // ResourceBlock { asID [0] OPTIONAL, ipAddrBlocks [1] OPTIONAL }
    let (block, f) = expect(f, 0x30).map_err(|_| shape)?;
    let mut b = block.body;
    if b.first() == Some(&0xa0) {
        b = tlv(b).ok_or(shape)?.1;
    }
    let (ips, _) = expect(b, 0xa1).map_err(|_| shape)?;
    let want = rsc_prefix(prefix);
    let mut covered = false;
    let mut any = false;
    let mut fams = ips.body;
    while !fams.is_empty() {
        let (fam, next) = expect(fams, 0x30).map_err(|_| shape)?;
        fams = next;
        let (afi, addrs) = tlv(fam.body).ok_or(shape)?;
        let family = afi_family(&afi).ok_or(shape)?;
        let (list, _) = expect(addrs, 0x30).map_err(|_| shape)?;
        let mut l = list.body;
        while !l.is_empty() {
            let (bits, next) = tlv(l).ok_or(shape)?;
            l = next;
            // Prefixes only: an address range is not read here.
            let p = bit_prefix(&bits, family).ok_or(shape)?;
            if !ee_res.covers(&p) {
                return Err(OwnershipRefusal::Resources);
            }
            any = true;
            covered |= p.covers(&want);
        }
    }
    if !any {
        return Err(shape);
    }
    if !covered {
        return Err(OwnershipRefusal::NotCovered);
    }
    let (alg, f) = expect(f, 0x30).map_err(|_| shape)?;
    let (oid, _) = expect(alg.body, 0x06).map_err(|_| shape)?;
    if oid.body != OID_SHA256 {
        return Err(shape);
    }
    let (checklist, _) = expect(f, 0x30).map_err(|_| shape)?;
    let challenge = sha256(&ownership_challenge(tenant, prefix));
    let mut found = false;
    let mut entries = checklist.body;
    while !entries.is_empty() {
        let (e, next) = expect(entries, 0x30).map_err(|_| shape)?;
        entries = next;
        let mut g = e.body;
        if g.first() == Some(&0x16) {
            g = tlv(g).ok_or(shape)?.1;
        }
        let (h, _) = expect(g, 0x04).map_err(|_| shape)?;
        found |= h.body == challenge;
    }
    if !found {
        return Err(OwnershipRefusal::NotChallenged);
    }
    Ok(traffic::OwnershipEvidence {
        holder: *tenant,
        covers: *prefix,
        digest: sha256(rsc_der),
        verified: true,
    })
}
