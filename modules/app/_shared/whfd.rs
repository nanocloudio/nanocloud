// The failure-domain controller's Nanocloud side (rfc_lb_vip §14.5, §22
// "Nanocloud: the preflight projection, acting on Placement"): what
// Nanocloud knows about a zone becomes Wormhole's `FdPreflight` proof, and
// the controller's `Placement` permission becomes the node actions that
// move workloads out of the zone and back. I/O-free and allocation-free;
// needs `_shared/wormhole_portfolio.rs` (the `traffic` module) included
// first. Every record is Wormhole's own codec (`fd_ctl.v1`).
//
// **The preflight.** Spare credit is the schedulable room Nanocloud can
// prove: allocatable minus requested CPU on ready, schedulable nodes —
// outside the zone for an evacuation, inside it for a restore. The demand
// is what must move: the requests of the zone's own pods (evacuate), or
// what returns to it (restore, the caller's figure). Quorum holds when the
// control plane's voters outside the zone are a strict majority of all of
// them. The readiness generation is the zone's own (the caller's: the
// store revision its nodes' readiness was last written at), so a restore is
// proved only on readiness newer than the evacuation saw. The remaining
// proofs — standbys, route capacity, keys, sovereignty, fence readiness,
// conflicting evidence, each move's binding epoch — are facts the caller
// read from Wormhole's and Clustor's own records and passes through.
//
// **Placement.** `EVACUATE_WORKLOADS` cordons every node of the zone (no
// new pod lands there) and marks it for drain; `RESTORE_WORKLOADS`
// uncordons them. The controller only sends either after its dial reached
// zero and the zone's protected sessions drained (or after the restore
// hold): Nanocloud never moves a workload ahead of the traffic.

/// One node as the preflight weighs it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct FdNode {
    /// `zone` as Wormhole names it (`whemit.rs`'s zone id).
    pub zone: u32,
    pub ready: bool,
    /// `spec.unschedulable`.
    pub cordoned: bool,
    /// A voter of the control plane's store group.
    pub voter: bool,
    pub cpu_allocatable_m: u64,
    pub cpu_requested_m: u64,
}

/// What the caller read beside the nodes.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct FdFacts {
    /// Wormhole endpoints the zone holds (its scope).
    pub endpoints_in_zone: u32,
    /// The zone's readiness generation.
    pub readiness_generation: u64,
    /// Restore only: the demand that returns to the zone.
    pub returning_demand_m: u64,
    pub standbys_ok: bool,
    pub route_ok: bool,
    pub keys_ok: bool,
    pub sovereignty_ok: bool,
    pub fence_ok: bool,
    pub conflicting: bool,
    pub epochs: [u64; traffic::FD_MOVES_MAX],
}

pub const FD_PHASE_EVACUATE: u8 = 1;
pub const FD_PHASE_RESTORE: u8 = 2;

/// The preflight proof for `zone` in `phase`.
pub fn fd_preflight(phase: u8, zone: u32, nodes: &[FdNode], f: &FdFacts) -> traffic::FdPreflight {
    let schedulable = |n: &FdNode| n.ready && !n.cordoned;
    let room = |n: &FdNode| n.cpu_allocatable_m.saturating_sub(n.cpu_requested_m);
    let restore = phase == FD_PHASE_RESTORE;
    let mut spare: u64 = 0;
    let mut demand: u64 = 0;
    let mut voters = 0u32;
    let mut outside_voters = 0u32;
    for n in nodes {
        let inside = n.zone == zone;
        if n.voter {
            voters += 1;
            if !inside && n.ready {
                outside_voters += 1;
            }
        }
        // A restore proves room in the zone, whose nodes are cordoned by
        // the evacuation: their readiness is what counts there.
        let usable = if restore && inside {
            n.ready
        } else {
            schedulable(n)
        };
        if inside == restore && usable {
            spare = spare.saturating_add(room(n));
        }
        if inside && !restore {
            demand = demand.saturating_add(n.cpu_requested_m);
        }
    }
    if restore {
        demand = f.returning_demand_m;
    }
    traffic::FdPreflight {
        phase,
        quorum_ok: voters > 0 && outside_voters * 2 > voters,
        standbys_ok: f.standbys_ok,
        route_ok: f.route_ok,
        keys_ok: f.keys_ok,
        sovereignty_ok: f.sovereignty_ok,
        fence_ok: f.fence_ok,
        conflicting: f.conflicting,
        scope: f.endpoints_in_zone,
        spare_credits: spare,
        domain_demand: demand,
        readiness_generation: f.readiness_generation,
        epochs: f.epochs,
    }
}

/// The preflight as the `fd_ctl.v1` record the controller reads on its
/// `ctl_in`; the length written.
pub fn fd_preflight_record(p: &traffic::FdPreflight, out: &mut [u8]) -> Option<usize> {
    let mut b = [0u8; traffic::FD_PREFLIGHT_LEN];
    p.encode(&mut b);
    traffic::encode_fd_envelope(traffic::FdKind::Preflight, 0, &[0; 16], &b, out).ok()
}

/// What a `Placement` asks of Nanocloud.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FdPlacement {
    pub action: u8,
    pub level: u8,
    pub domain: u32,
    pub generation: u64,
    pub operation: [u8; 16],
}

/// A `Placement` (0xA6) from the controller's `status_out`, strictly:
/// anything else (a status, an outcome) is `None`.
pub fn fd_placement_decode(rec: &[u8]) -> Option<FdPlacement> {
    let env = traffic::decode_fd_envelope(rec).ok()?;
    if env.kind != traffic::FdKind::Placement || env.payload.len() != traffic::FD_PLACEMENT_LEN {
        return None;
    }
    let p = env.payload;
    let action = p[0];
    if action != traffic::fd_placement::EVACUATE_WORKLOADS
        && action != traffic::fd_placement::RESTORE_WORKLOADS
    {
        return None;
    }
    Some(FdPlacement {
        action,
        level: p[1],
        domain: u32::from_le_bytes([p[4], p[5], p[6], p[7]]),
        generation: u64::from_le_bytes([p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15]]),
        operation: env.operation_id,
    })
}

/// Whether the placement acts on `node` (a node of the domain).
pub fn fd_placement_targets(p: &FdPlacement, node: &FdNode) -> bool {
    node.zone == p.domain
}

/// The node patch a placement makes: a merge patch of `spec.unschedulable`
/// and the drain mark (an annotation naming the operation, so a restore
/// undoes only what this operation did). The length written.
pub fn fd_node_patch(p: &FdPlacement, out: &mut [u8]) -> Option<usize> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let evac = p.action == traffic::fd_placement::EVACUATE_WORKLOADS;
    let mut op = [0u8; 32];
    for (i, b) in p.operation.iter().enumerate() {
        op[2 * i] = HEX[usize::from(b >> 4)];
        op[2 * i + 1] = HEX[usize::from(b & 15)];
    }
    let parts: [&[u8]; 5] = if evac {
        [
            b"{\"metadata\":{\"annotations\":{\"wormhole.nanocloud.io/evacuated-by\":\"",
            &op,
            b"\"}},\"spec\":{\"unschedulable\":true}}",
            b"",
            b"",
        ]
    } else {
        [
            b"{\"metadata\":{\"annotations\":{\"wormhole.nanocloud.io/evacuated-by\":null}},",
            b"\"spec\":{\"unschedulable\":false}}",
            b"",
            b"",
            b"",
        ]
    };
    let mut n = 0;
    for part in parts {
        let end = n + part.len();
        out.get_mut(n..end)?.copy_from_slice(part);
        n = end;
    }
    Some(n)
}
