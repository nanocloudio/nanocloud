// The regional fence provider's decision (rfc_lb_vip §13.6, §15.1, §22
// "Nanocloud: the regional fence provider (upstream cut, power)"): what
// Nanocloud observes of a region's isolation becomes the `RegionFence`
// confirmation the region controller acts on (`region_ctl.v1` 0xB4) — or
// nothing. I/O-free and allocation-free; needs
// `_shared/wormhole_portfolio.rs` first.
//
// A confirmation is a positive fact, never an absence of news:
//
// - **Upstream cut**: every transit link of the region is known, and every
//   one reports its cut acknowledged by the upstream (the session down
//   *and* the region's prefixes withdrawn on the upstream's side). A link
//   merely unreachable from here proves nothing; one link still up refuses.
// - **Power**: every power feed of the site reports its outlets off,
//   acknowledged by the power controller.
//
// The confirmation carries the provider's custody generation for the
// region (minted when it took custody of the region's actuators), which
// the controller checks, and when it was observed. The provider also
// presents its own Kagi FENCE scope (REGIONAL_CONFIRM, global) to the
// controller; Kagi refuses that scope to any class but the fence
// provider's.
//
// The actuation itself — cutting a region's transit, removing its power —
// is hardware this core only observes: a real cut **needs hardware**.

/// One transit link of a region as the provider observes it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct UpstreamLink {
    /// The session is down.
    pub session_down: bool,
    /// The upstream acknowledged the cut: the region's prefixes withdrawn on
    /// its side.
    pub cut_acknowledged: bool,
}

/// One power feed of a site.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct PowerFeed {
    pub outlets_off: bool,
    pub acknowledged: bool,
}

/// Why no confirmation was made.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FenceUnproven {
    /// No link or feed is known: nothing can be proven.
    Unknown,
    /// A link is up, or its cut not acknowledged.
    LinkUp,
    /// A feed still has power, or is unacknowledged.
    Powered,
    /// The provider holds no custody of the region.
    NoCustody,
}

/// An upstream-cut confirmation for `region`.
pub fn fence_upstream_cut(
    region: u32,
    links: &[UpstreamLink],
    custody_generation: u32,
    observed_ms: u64,
) -> Result<traffic::RegionFence, FenceUnproven> {
    if custody_generation == 0 {
        return Err(FenceUnproven::NoCustody);
    }
    if links.is_empty() {
        return Err(FenceUnproven::Unknown);
    }
    if links
        .iter()
        .any(|l| !(l.session_down && l.cut_acknowledged))
    {
        return Err(FenceUnproven::LinkUp);
    }
    Ok(traffic::RegionFence {
        region,
        kind: traffic::region_fence::UPSTREAM_CUT,
        custody_generation,
        observed_ms,
    })
}

/// A power confirmation for `region`.
pub fn fence_power(
    region: u32,
    feeds: &[PowerFeed],
    custody_generation: u32,
    observed_ms: u64,
) -> Result<traffic::RegionFence, FenceUnproven> {
    if custody_generation == 0 {
        return Err(FenceUnproven::NoCustody);
    }
    if feeds.is_empty() {
        return Err(FenceUnproven::Unknown);
    }
    if feeds.iter().any(|f| !(f.outlets_off && f.acknowledged)) {
        return Err(FenceUnproven::Powered);
    }
    Ok(traffic::RegionFence {
        region,
        kind: traffic::region_fence::POWER,
        custody_generation,
        observed_ms,
    })
}

/// The confirmation as the `region_ctl.v1` record the controller reads;
/// the length written.
pub fn fence_record(f: &traffic::RegionFence, out: &mut [u8]) -> Option<usize> {
    let mut b = [0u8; traffic::REGION_FENCE_LEN];
    f.encode(&mut b);
    let id = traffic::region_identity(f.region);
    traffic::encode_rg_envelope(traffic::RgKind::RegionFence, 0, &id, &b, out).ok()
}
