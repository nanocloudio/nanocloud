// A Service's load-balancer address as Wormhole's VIP authority is asked
// for it: `/lb-addr/<ns>/<svc>` (svc_alloc's allocation) into the
// `failover_request` records Wormhole's `failover_coordinator` runs
// (rfc_lb_vip §11.1, §13.5). Pure; the I/O is `wormhole_vip`'s.
//
// - **One move per allocation.** A new `/lb-addr/<ns>/<svc>` = `ip=<a>`
//   becomes one `VipMove` of the Service's address (`service_id` and the
//   address, /32) to the node whose own hold claims it (`/net-hold/<addr>`
//   = `node=`, so the controller there adopts it in place), or the first
//   ready node when nothing claims it. No previous owner, so no fence.
// - **One release per allocation that goes.** When the row goes (or names
//   another address), a `VipRelease` of the old one from the node whose
//   controller holds it (`/net-vip/<addr>`): graceful, the only fence a
//   release has.
// - **The ledger.** Before a request is sent its whole intent is written at
//   `/wh-vip-op/<ns>/<svc>` (`wv_ledger`): the address, the allocation's
//   revision, the binding epoch it expects and the node it names. A
//   restarted module sends from the ledger, so it re-sends byte-identical
//   requests, which the coordinator answers from the record it already has.
// - **Operation ids** are derived, never stored as a mapping:
//   `H("nc-vip-move" | "nc-vip-release", service_id, address, revision)` —
//   the Service and the address, and the allocation's store revision, so a
//   Service deleted and created again with the same address is a new
//   operation, not the finished one answered again.
// - **Never a takeover.** A move is sent only for a binding with no owner
//   (absent, UNASSIGNED or RELEASED, read from Wormhole's published
//   `VipBinding`); an owned binding is the controllers' already. Moving an
//   owned address off an unresponsive node needs an external fence, which
//   this projection does not request (the coordinator refuses one without
//   an admitted provider).

const WV_LB: &[u8] = b"/lb-addr/";
const WV_HOLDS: &[u8] = b"/net-hold/";
const WV_MARKS: &[u8] = b"/net-vip/";
const WV_LEDGER: &[u8] = b"/wh-vip-op/";
const WV_NODES: &[u8] = b"/nodes/";
/// A move or release completes or fails inside this (§7.6, a duration the
/// coordinator starts when it adopts the record).
const WV_DEADLINE_MS: u32 = 60_000;
/// The largest request frame: the §9.4 envelope around a request.
const WV_FRAME_MAX: usize =
    traffic::HEADER_LEN + traffic::FAILOVER_REQUEST_LEN + traffic::DIGEST_LEN;
/// A ledger row.
const WV_ROW_MAX: usize = 160;

/// What a ledger row says was asked.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct WvIntent {
    /// 0: a move; 1: a release.
    release: bool,
    addr: [u8; 4],
    rev: u64,
    epoch: u64,
    /// The node named (the move's destination, the release's source);
    /// empty when nothing is to be sent (the binding is owned already).
    node: [u8; 64],
    node_len: u8,
}

impl WvIntent {
    fn node(&self) -> &[u8] {
        &self.node[..self.node_len as usize]
    }
}

fn wv_u32(a: [u8; 4]) -> u32 {
    u32::from_be_bytes(a)
}

fn wv_dec(out: &mut [u8], at: usize, v: u64) -> Option<usize> {
    wh_dec(out, at, v)
}

/// `op=<move|release>;ip=<u32>;rev=<r>;epoch=<e>;node=<n>`.
fn wv_ledger(i: &WvIntent, out: &mut [u8]) -> Option<usize> {
    let p = wh_put(
        out,
        0,
        if i.release {
            b"op=release;ip="
        } else {
            b"op=move;ip="
        },
    )?;
    let p = wv_dec(out, p, u64::from(wv_u32(i.addr)))?;
    let p = wh_put(out, p, b";rev=")?;
    let p = wv_dec(out, p, i.rev)?;
    let p = wh_put(out, p, b";epoch=")?;
    let p = wv_dec(out, p, i.epoch)?;
    let p = wh_put(out, p, b";node=")?;
    wh_put(out, p, i.node())
}

fn wv_parse_ledger(row: &[u8]) -> Option<WvIntent> {
    let op = wh_field(row, b"op=");
    let release = match op {
        b"move" => false,
        b"release" => true,
        _ => return None,
    };
    let ip = wh_u64(wh_field(row, b"ip="))?;
    let node = wh_field(row, b"node=");
    if ip > u64::from(u32::MAX) || node.len() > 64 {
        return None;
    }
    let mut n = [0u8; 64];
    n[..node.len()].copy_from_slice(node);
    Some(WvIntent {
        release,
        addr: (ip as u32).to_be_bytes(),
        rev: wh_u64(wh_field(row, b"rev="))?,
        epoch: wh_u64(wh_field(row, b"epoch="))?,
        node: n,
        node_len: node.len() as u8,
    })
}

fn wv_intent(release: bool, addr: [u8; 4], rev: u64, epoch: u64, node: &[u8]) -> Option<WvIntent> {
    if node.len() > 64 {
        return None;
    }
    let mut n = [0u8; 64];
    n[..node.len()].copy_from_slice(node);
    Some(WvIntent {
        release,
        addr,
        rev,
        epoch,
        node: n,
        node_len: node.len() as u8,
    })
}

fn wv_target(ns: &[u8], name: &[u8], addr: [u8; 4]) -> traffic::VipTarget {
    traffic::VipTarget {
        service_id: wh_service_id(ns, name),
        prefix: traffic::NetAddr::v4(addr),
        prefix_len: 32,
    }
}

fn wv_operation_id(ns: &[u8], name: &[u8], i: &WvIntent) -> [u8; 16] {
    let service = wh_service_id(ns, name);
    let label: &[u8] = if i.release {
        b"nc-vip-release"
    } else {
        b"nc-vip-move"
    };
    wh_id16(&wh_digest(
        label,
        &[&service, &i.addr, &i.rev.to_le_bytes()],
    ))
}

/// The boot incarnation a request names for a node. The controller claims
/// under its own admitted boot incarnation; the record keeps this as the
/// identity the projection named, which nanocloud does not observe.
fn wv_boot(node: &[u8]) -> [u8; 16] {
    wh_id16(&wh_digest(b"nc-node-boot", &[node]))
}

/// The coordinator's `failover_request` for `i`, enveloped (§9.4) into
/// `out`; its length. None for an intent with nothing to send.
fn wv_request(ns: &[u8], name: &[u8], i: &WvIntent, out: &mut [u8]) -> Option<usize> {
    if i.node_len == 0 {
        return None;
    }
    let host = wh_host_id(i.node());
    let (kind, fence, source, dest) = if i.release {
        (
            traffic::FailoverKind::VipRelease,
            traffic::FenceRequirement::Graceful,
            host,
            [0u8; 16],
        )
    } else {
        (
            traffic::FailoverKind::VipMove,
            traffic::FenceRequirement::None,
            [0u8; 16],
            host,
        )
    };
    let boot = wv_boot(i.node());
    let req = traffic::FailoverRequest {
        action: traffic::failover_action::START,
        kind: kind as u8,
        fence: fence as u8,
        ready: 1,
        source_fault_domain: 0,
        target: wv_target(ns, name, i.addr),
        expected_epoch: i.epoch,
        deadline_ms: WV_DEADLINE_MS,
        allowed_interfaces: 0,
        recovery_generation: 0,
        source_host: source,
        source_boot: if i.release { boot } else { [0; 16] },
        dest_host: dest,
        dest_boot: if i.release { [0; 16] } else { boot },
        policy: [0; 16],
        actor: wh_id16(&wh_digest(b"nc-actor", &[b"wormhole_vip"])),
    };
    let mut p = [0u8; traffic::FAILOVER_REQUEST_LEN];
    req.encode(&mut p).ok()?;
    let op = wv_operation_id(ns, name, i);
    traffic::encode_vip_envelope(traffic::VipCtlKind::FailoverRequest, 0, &op, &p, out).ok()
}

/// What Wormhole's published binding says of an address: whether it has
/// an owner (a move would be a takeover), and the epoch a request expects.
fn wv_binding(rec: &[u8], service: &[u8; 16], addr: [u8; 4]) -> Option<(bool, u64)> {
    let env = traffic::decode_envelope(rec).ok()?;
    if env.kind != traffic::RecordKind::VipBinding {
        return None;
    }
    let b = traffic::VipBinding::decode(env.payload).ok()?;
    if b.service_id != *service {
        return None;
    }
    if b.prefix != traffic::NetAddr::v4(addr) {
        // The Service's previous address: Clustor keys a binding by its
        // address, so this one has none yet.
        return Some((false, 0));
    }
    let unowned = matches!(
        b.phase,
        traffic::VipPhase::Unassigned | traffic::VipPhase::Released
    );
    Some((!unowned, b.vip_epoch))
}

/// `ip=<a.b.c.d>` of an `/lb-addr/` row.
fn wv_lb_addr(row: &[u8]) -> Option<[u8; 4]> {
    wh_v4(wh_field(row, b"ip="))
}
