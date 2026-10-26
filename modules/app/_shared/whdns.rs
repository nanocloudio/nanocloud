// `service_dns` from the region controller's signed projection (rfc_lb_vip
// §13.6, §22 "Nanocloud: service_dns composition from the projection"):
// the controller's 224-byte projection, signed verbatim by Kagi
// (`service_dns_sign`, a `KSDN` record), is opened offline with Kagi's own
// codec (`service_dns.rs`: `Record::parse`, `verify` against the
// `SERVICE_DNS` key the resolver was given) and handed to Wormhole's own
// check (`upstream_core.rs` `dns_answers`: another service's, stale against
// the newest global generation already seen, expired, or with no healthy
// region the client may be answered with — each refused) for the
// addresses a resolver answers with, in preference order. Nothing here
// restates either format. I/O-free and allocation-free; needs
// `_shared/wormhole_portfolio.rs` and Kagi's `service_dns.rs` (as `ksdn`)
// included first, and the includer's signature check.

/// One `SERVICE_DNS` verifying key.
#[derive(Clone, Copy)]
pub struct DnsKey<'a> {
    pub issuer: &'a [u8],
    pub kid: &'a [u8],
    pub suite: u16,
    pub public: &'a [u8],
}

/// Open a `KSDN` record under `keys` at `now_ms`: the projection bytes it
/// vouches for, or why not.
pub fn ksdn_open(
    signed: &[u8],
    keys: &[DnsKey<'_>],
    verify: ksdn::VerifyFn,
    now_ms: u64,
) -> Result<[u8; traffic::SERVICE_DNS_LEN], traffic::DnsRefusal> {
    use traffic::DnsRefusal as R;
    let rec = ksdn::Record::parse(signed).map_err(|_| R::Malformed)?;
    rec.verify(
        |issuer, kid| {
            keys.iter()
                .find(|k| k.issuer == issuer && k.kid == kid)
                .map(|k| (k.suite, k.public))
        },
        verify,
        now_ms,
    )
    .map_err(|r| match r {
        ksdn::Refusal::Expired | ksdn::Refusal::NotYetValid => R::Expired,
        ksdn::Refusal::Malformed | ksdn::Refusal::WindowInvalid => R::Malformed,
        _ => R::Signature,
    })?;
    rec.projection_bytes.try_into().map_err(|_| R::Malformed)
}

/// What a resolver answers a service's name with: the addresses of the
/// healthy regions the client may reach (`allowed_regions`, a bit per
/// region), in preference order, and the global generation seen (the
/// next query's floor, so an older projection is never answered after a
/// newer one).
pub struct DnsAnswerSet {
    pub n: usize,
    pub answers: [traffic::DnsAnswer; traffic::DNS_ANSWERS_MAX],
    pub global_generation: u64,
    pub ttl_s: u32,
}

#[allow(
    clippy::too_many_arguments,
    reason = "one argument per input the check takes: the record, its keys and verifier, the service, the floor, the clock and the client's admission"
)]
pub fn service_dns_answer(
    signed: &[u8],
    keys: &[DnsKey<'_>],
    verify: ksdn::VerifyFn,
    service_id: &[u8; 16],
    min_global_generation: u64,
    now_ms: u64,
    allowed_regions: u32,
) -> Result<DnsAnswerSet, traffic::DnsRefusal> {
    let open = |s: &[u8], now: u64| ksdn_open(s, keys, verify, now);
    let mut out = [traffic::DnsAnswer::default(); traffic::DNS_ANSWERS_MAX];
    let n = traffic::dns_answers(
        signed,
        &open,
        service_id,
        min_global_generation,
        now_ms,
        allowed_regions,
        &mut out,
    )?;
    // The checks passed: the projection's own generation and TTL.
    let bytes = open(signed, now_ms)?;
    let p = traffic::ServiceDnsProjection::decode(&bytes)
        .map_err(|_| traffic::DnsRefusal::Malformed)?;
    Ok(DnsAnswerSet {
        n,
        answers: out,
        global_generation: p.global_generation,
        ttl_s: p.ttl_s,
    })
}
