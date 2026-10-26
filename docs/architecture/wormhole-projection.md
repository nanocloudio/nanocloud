# The Wormhole projection

Wormhole owns traffic: load balancing, service addresses, their authority and
session continuity (`../wormhole/.context/rfc_lb_vip.md`). Nanocloud owns the
Kubernetes API. This is the seam between them (RFC §6.1, "Nanocloud/Kubernetes
projection"): Kubernetes intent is mapped into Wormhole's own records, and
Wormhole's readiness is mapped back into Kubernetes status. No Kubernetes
object is packet-path state, and nanocloud restates none of Wormhole's
formats: the records are encoded and decoded with Wormhole's code, the
`traffic` cores of its published `wormhole-common` tree.

```
 /services/ ─┐                                   ┌─> /wh/<t>/<s>/gen/<g>/…  (records)
             ├─ wormhole.plan (wh_) ─> /wh-intent/ ─┤   /wh/<t>/<s>/active   (pointer)
 /endpointslices/ (endpoints.plan) ─> /wh-intent-ep/ ┘   /wh-status/<ns>/<svc>
                                 wormhole_emit ──┘

 /wh/<t>/<s>/vip/<4|6>  (Wormhole's VIP controller writes)
     └─ wormhole_status ─> /wh-vip/<ns>/<svc> ─ wh_status ─> /services/… status.loadBalancer

 /wh-demand/<cell>  (Wormhole writes) ─ wh_cell.plan (wc_) ─> /deployments.apps/wormhole-system/<cell>
```

Everything here is in the control plane's Service plane
(`packaging/debian/fluxor-wormhole.yaml`, spliced by `nanocloud-graph.sh`).

## Which Services

A Service is Wormhole's when its `spec.loadBalancerClass` is **unset** —
Wormhole is nanocloud's default load balancer — or is
**`wormhole.nanocloud.io/lb`**. Any other class belongs to another controller:
it is not declared, svc_alloc gives it node ports but no pool address, and its
status is never touched. A headless Service (`clusterIP: None`), an
ExternalName one and one without ports are not declared.

## Endpoints: the EndpointSlice lifecycle

`endpoints.plan` keeps one row per (Service, Pod) at
`/endpointslices/<ns>/<svc>/<pod>`:

```
ip=<ip>;state=<ready|serving|terminating>;zone=<z>;node=<n>;gen=<u32>;af=<4|6>;uid=<uid>[;at=<moment>];grace=<ms>
```

| Field | Meaning |
| --- | --- |
| `state` | `ready`: Ready (the readiness probe's verdict, else the Pod's condition) and not being deleted. `serving`: Ready while `metadata.deletionTimestamp` is set. `terminating`: being deleted and no longer Ready, or gone, or deselected. A Pod that is merely not Ready (not being deleted) has no row: that is health, not drain. |
| `zone` | the node's `topology.kubernetes.io/zone` label, empty without one (`node` is beside it) |
| `gen` | moves on whenever the endpoint is **reused**: another IP, another Pod uid under the same name, or a row that was not ready (absent or draining) becoming ready — RFC §9.2, so a late result for an older generation cannot touch its replacement |
| `af` | 4 or 6 |
| `grace` | the drain an endpoint is owed: `deletionGracePeriodSeconds`, else `terminationGracePeriodSeconds`, else 30 s, in ms; known before the drain begins. An announced drain is never shortened. |
| `at` | the moment the drain began (`_shared/moment.rs`: trusted Unix ms, else a monotonic stamp of this boot), only while draining |

Removal is drain-first: a row goes `terminating` and the expiry chain
(`ttl.plan` over `at!due+grace`, the kernel timer armed for it — nothing
polls) deletes it when its grace has run. A Service deleted outright takes
its rows at once. The DNS, the proxy's DNAT, the edge and node_net take
**ready** rows only.

## Intent rows

`wormhole.plan` writes flat rows the emitter encodes (it decides which
Services, the emitter decides nothing):

- `/wh-intent/<ns>/<svc>` = `kind=svc;type=…;cip=…;cips=<JSON list>;fam=<JSON list>;lbip=…;ports=…;protos=…;tports=…;etp=…;itp=…;aff=…;afft=…;src=<JSON list>`
  — `lbip` is svc_alloc's `/lb-addr/<ns>/<svc>`, never the status.
- `/wh-intent-ep/<ns>/<svc>/<pod>` = `kind=ep;<the slice>;pnames=…;pnums=…`
  — keyed by the slice's own tail, so its prune finds the slice by the same
  key (a `/wh-intent/<ns>/<svc>/ep/<pod>` key would put the endpoint rows
  under the Service row's prefix, where a same-key lookup cannot reach the
  slice).

## Identities

128-bit, derived, never stored as a mapping (`_shared/whemit.rs`):

```
H(label, parts…) = SHA-256(label ‖ 0x00 ‖ part₁ ‖ 0x00 ‖ part₂ …)
tenant_id     = H("nc-tenant",    ns)[..16]
service_id    = H("nc-service",   ns, name)[..16]
endpoint_id   = H("nc-endpoint",  ns, svc, pod)[..16]
host_id       = H("nc-node",      node)[..16]          zero: no node
zone          = H("nc-zone",      zone)[..4] as u32 BE  0: no zone (a digest of 0 is 1)
operation_id  = H("nc-operation", service_id, gen as u64 LE)[..16]
hash_seed_ref = H("nc-hash-seed", tenant_id, service_id)[..16]
```

A namespace is a tenant. A name holds no NUL, so the encoding is unambiguous;
a name past 63 bytes is refused, never truncated. `hash_seed_ref` names the
route's flow-hash secret; nanocloud mints no secret — Wormhole resolves it.

## Records and keys

One generation `g` of a Service, under `/wh/<tenant hex>/<service hex>/gen/<g>/`:

| Key | Record | Notes |
| --- | --- | --- |
| `r<i>-<4\|6>/ep/<endpoint hex>` | `EndpointRecord` (WHR1 kind 2) | one per (route, endpoint); `port` the route's target port on that endpoint (a named `targetPort` resolved against the Pod; an endpoint that has no such port is not in that route); `generation` the slice's `gen`; `weight` 1; `observed_route_epoch` `g` |
| `r<i>-<4\|6>/manifest` | `EndpointManifest` (kind 3) | `revision` `g`; `digest` SHA-256 over the endpoint records' content digests in identity order |
| `policy` | `LocalityPolicy` (kind 0x80) | the normalized locality/affinity inputs, 8 bytes: `ext_local u8, int_local u8, client_ip u8, 0, affinity_timeout_s u32 LE` |
| `admission` | `SourceAdmission` (kind 0x81) | only when the Service has `loadBalancerSourceRanges`: `service_id[16] count u16 LE 0u16` then per range `family u8 prefix_len u8 0u16 address[16]` (≤ 16 ranges, each masked to its prefix; a bad one refuses the generation) |
| `r<i>-<4\|6>/route` | `RouteRecord` (kind 1) | one per (Service port, listener family) |
| `seal` | `GenerationSeal` (kind 0x82) | `service_id[16] generation u64 records u32 routes u16 0u16 digest[32]` — the digest SHA-256 over `(key length u16 LE ‖ key ‖ content digest)` of every other record of the generation in lexicographic key order |

`i` is the Service port's index (≤ 8 ports); the family is the listener's.
Every record is in the §9.4 envelope with `record_generation` = `g` and the
operation id above; every one fits Lattice's 4096-byte value ceiling, and
every key its 256-byte one.

The route record:

| Field | Value |
| --- | --- |
| `service_id`, `tenant_id`, `route_epoch` | as above; `route_epoch` = `g` |
| `listener` | IPv4: the load-balancer address of a LoadBalancer Service that has one, else the ClusterIP; IPv6: the Service's IPv6 ClusterIP. A dual-stack Service (`ipFamilies`, `clusterIPs`) has a route per family, linked by the one `service_id`. |
| `prefix_len` | 32 / 128 |
| `transport`, `port` | the Service port's protocol (TCP, UDP; SCTP has no route) and port |
| `termination` | Passthrough |
| `selection` | Maglev past 16 endpoints, else Rendezvous |
| `continuity` | DrainOnly for `sessionAffinity: ClientIP`, else Reroutable |
| `return_mode` | Proxy |
| `affinity` | ApplicationKey for ClientIP (the key is the client address; the timeout in the policy record), else Flow |
| `source_identity` | TransportPeer |
| `policies.locality` | SHA-256(policy payload)[..16] |
| `policies.admission` | SHA-256(admission payload)[..16]; zero without ranges |
| other policies | zero (none) |
| `endpoint_manifest_id` | the manifest record's content digest[..16] |
| `provenance_digest` | SHA-256 of the service intent row |

Endpoint state mapping (§14.2):

| Slice `state` | `EndpointState` |
| --- | --- |
| `ready` | Ready |
| `serving` | Degraded (accepts new flows, as upstream sends to serving endpoints when no ready one remains) |
| `terminating` | Draining |
| (no row) | no record |

A route with no endpoint is not written: Wormhole's `EndpointManifest`
decoder refuses `count = 0`, so a listener with no endpoints is a generation
without that route.

## Publication, activation and collection

`wormhole_emit` publishes in the §9.5 order — every endpoint record, every
manifest, the policy and admission, every route, the seal — then advances

```
/wh/<t>/<s>/active = gen=<g>;intent=<hex>;seal=<hex>;records=<n>;routes=<r>;svc=<ns>/<name>
```

by a PUT conditional on the revision it read (absent: the absent
precondition). `g` is the pointer's generation plus one and is never reused
once active. `intent` is a digest of the inputs: equal to the current intent,
nothing is published. A publisher that loses the compare-and-swap removes
what it staged and stages after the winner; a staging whose intent moves is
removed and staged again. A burst of rows settles (250 ms) into one
generation.

After an activation every generation but the active one and the one before it
(the flows pinned to it, §9.3) is deleted. A Service no longer declared is
withdrawn — `gen=<g+1>;intent=;seal=;records=0;routes=0;svc=…;withdrawn=1` —
its generations deleted and its status removed; the pointer stays, so a
Service that returns continues its sequence.

`/wh-status/<ns>/<svc>` = `gen=<g>;written=<records>;routes=<r>;endpoints=<e>`,
or `error=<name|malformed|too-many|buffer|write>` (the last good generation
stands).

## Load-balancer status, gated on the VIP

A Service's `status.loadBalancer` says its address is reachable; only the
VIP's admitted readiness can say that (§6.1). So:

- **svc_alloc** allocates the pool address and records it by Service at
  `/lb-addr/<ns>/<svc>` = `ip=<a.b.c.d>` (claimed at `/ipam/lb/<u32>`). It no
  longer writes the status. The proxy's DNAT, node_net's link hold and the
  intent row read `/lb-addr/`.
- **Wormhole's VIP controller** writes the Service's `VipBinding` (WHR1
  kind 5, the §11.6 layout) per family at
  **`/wh/<tenant hex>/<service hex>/vip/<4|6>`** — Wormhole's key: nanocloud
  writes only `…/gen/` and `…/active` under a Service.
- **wormhole_status** decodes it with Wormhole's strict decoder (the binding
  must be this Service's and of the key's family) into
  `/wh-vip/<ns>/<svc>` = `p4=<phase>;a4=<addr>;p6=<phase>;a6=<addr>`.
- **wh_status** PATCHes `status.loadBalancer` to
  `{"ingress":[{"ip":…}…]}` for each family whose phase is **ACTIVE (6) or
  DRAINING (7)**, and to `{}` before that and once WITHDRAWN, EMISSION_DISABLED
  or RELEASED.

Without a Wormhole VIP controller the address still works (node_net holds it
on the link, every node DNATs it) and the status stays empty.

## The address backend: node_net under the VIP controller

node_net is the single writer of the host's held-address table (net.route
HOLD). Wormhole's `vip_controller`, when one runs on the node, drives an
address through the same Fluxor `addr_ctl` contract it sends `ip` on bare
metal — ADDR_ADD (disarmed), ADDR_ARM and ADDR_FENCE under the install token
node_net mints, ADDR_DEL — wired to node_net's `vip_ctl` input, answered on
its `vip_evt` output (named apart from the network stack's `addr_ctl`, which
Fluxor keeps to the kernel's one writer on a workload node). One place decides every address
(`modules/app/_shared/vip_hold.rs`), so no address has two owners, and none
loses its owner except across a fence:

- **The mark.** An address a controller drives is marked
  `/net-vip/<addr>` = `node=<name>`. While a mark stands, no node's own hold
  makes a new claim on the address; a claim already held keeps answering.
- **Adoption in place.** ADDR_ADD on the node whose claim
  (`/net-hold/<addr>`) holds the address adopts it: the row stays armed and
  the claim stays, so the upgrade from the own hold to the controller has no
  gap and no second install. The coordinator places an address's first
  owner on the node already holding it.
- **Arming elsewhere waits for the release.** ADDR_ADD on another node
  installs the row fenced; its ADDR_ARM waits until no claim stands, then
  claims under ABSENT and arms. The holder releases only through its own
  controller's ADDR_FENCE then ADDR_DEL — the fenced handover, whose
  cutoff node_net reports as the hand-off to the host provider — or by no
  longer being eligible, or by being taken over as not ready.
- **Falling back to the own hold.** ADDR_DEL without a fence (a controller
  restart's clean slate, or one that let the address go uncut) removes the
  mark and leaves the row and claim as they are. A controller that goes
  silent tears nothing down, and a mark whose node is not ready is removed,
  so the own-hold rules resume.

`tests/harness/tests/vip_hold.rs` drives three modelled nodes and their
controllers through adoption, an unfenced delete, controller loss and a
cross-node fenced move, and counts owners after every step: never two, and
never none where the address must stay owned.

## The VIP authority and its requests

The control plane carries Clustor's VIP authority as a raft group of its own
(`fluxor-controlplane.yaml`: `consensus_vip`, `durability_vip` and
`session_directory`, partition 1, over the store's voters and peer links;
`peer_router` routes two partitions there). Its `vip/<address>` namespace
decides who may answer for an address; its `fo/<operation_id>` namespace
keeps the failover coordinator's records.

On the plane's first replica `nanocloud-graph.sh` renders Wormhole's VIP
stack beside node_net (`NANOCLOUD_VIP=0` leaves it out):

| Module | Does |
| --- | --- |
| `vip_controller` | `host_id` = H("nc-node", node), `tenant` = H("nc-tenant", `NANOCLOUD_VIP_NAMESPACE`), no static `vip`: a binding per address, driving node_net's `vip_ctl`/`vip_evt` |
| `failover_coordinator` | runs each move and release; no fence provider is wired, so an external-fence move (an unresponsive owner) is refused `NO_FENCE_PROVIDER`, never attempted |
| `failover_store` | the coordinator's records in the `fo/` namespace, replayed at start |
| `binding_writer` | each `VipBinding` publication at `/wh/<t>/<s>/vip/4`, greatest generation winning — what `wormhole_status` gates the load-balancer status on |
| `wormhole_vip` | svc_alloc's `/lb-addr/<ns>/<svc>` into the coordinator's requests |

`wormhole_vip` (`_shared/whvip.rs`) asks for a Service's address:

- **a new allocation** is one `VipMove` of (`service_id`, the address /32) to
  the node whose own hold claims it (`/net-hold/<u32>`), so its controller
  adopts it in place, or with no claim to this node while it is ready; never
  for a binding that already has an owner (that would be a takeover);
- **an allocation that goes** is one graceful `VipRelease` from the node
  whose controller holds it (`/net-vip/<u32>`);
- **the ledger** `/wh-vip-op/<ns>/<svc>` =
  `op=<move|release>;ip=<u32>;rev=<r>;epoch=<e>;node=<n>` is written before a
  request is sent and the request built from it, so a restart re-sends the
  same bytes, which the coordinator answers from the record it keeps;
- **operation ids** are H("nc-vip-move"|"nc-vip-release", service_id,
  address, the allocation's store revision).

**One coordinator, beside one controller.** There is no channel between
nodes, and a coordinator adopts every record its store replays, so the stack
runs once per cluster and asks only for this node. Every other node keeps
node_net's own hold; with no controller, or one not yet ACTIVE, node_net
keeps holding, and the load-balancer status stays empty until the published
binding is ACTIVE. Only `NANOCLOUD_VIP_NAMESPACE`'s Services are asked for,
since a controller publishes under one tenant.

`tests/harness/tests/vip_e2e.rs` runs two nodes across the seam with the real
cores — the coordinator, both controllers, Clustor's registry, node_net's
hold — through adoption in place, a release and a move to the other node
that arms only after the release, and a controller killed and restarted
falling back to the own hold, counting owners after every step: never two,
and never none where the address must stay owned.

## Ingress-cell capacity

Wormhole publishes what a cell needs at **`/wh-demand/<cell>`** =
`hosts=<n>;profile=<pi5|server>;image=<ref>`. `wh_cell.plan` runs it through
the workload path nanocloud already has: the `wormhole-system` Namespace and a
Deployment `wormhole-system/<cell>` of `hosts` replicas, one per node
(required pod anti-affinity on `kubernetes.io/hostname`), on the host
network, requests by profile (pi5: 1 CPU, 512Mi; server: 4 CPU, 4Gi),
`env` `WORMHOLE_CELL` and `WORMHOLE_PROFILE`, labelled
`app.kubernetes.io/managed-by: nanocloud-wh-cell`. Created once, then held by
`spec.replicas` and `spec.template` (put-if-changed); deleted with its demand,
or at 0 hosts. The cell name is the Deployment's: a DNS label.

## Traffic dials

A Service's traffic dial — Wormhole's `TrafficDial` (WHR1 kind 0x84), as the
failure-domain controller's `dial_out` or an operator writes it — lives at
**`/wh-dial/<ns>/<svc>`** (one record). `wormhole_emit` subscribes to that
prefix beside the intents, reads it with the Service (`_shared/whemit.rs`
`wh_parse_dial`: the record strictly decoded, for this Service only), and
folds it into every generation: the record is written as `gen/<g>/dial`
between the policies and the routes (in the seal like every other record,
its key sorting between `admission` and `policy`), and each route names it
by its content address (`policies.traffic_dial` = SHA-256 of its
payload[..16]). A dial is part of the intent digest, so a new dial is a new
generation; with no dial the digest is what it always was. Proof:
`tests/harness/tests/wormhole_portfolio.rs`.

## The failure-domain controller's preflight and placement

`_shared/whfd.rs` is Nanocloud's side of Wormhole's `failure_domain_controller`:

- **Preflight** (`fd_preflight`, the `fd_ctl.v1` Preflight record): spare
  credit is allocatable minus requested CPU on ready, schedulable nodes —
  outside the zone for an evacuation, inside it (its readiness, the zone
  cordoned) for a restore; demand is the zone's requests (evacuate) or what
  returns (restore); quorum holds when the store group's voters outside the
  zone are a strict majority; scope, readiness generation, standbys, route,
  keys, sovereignty, fence readiness, conflicting evidence and each move's
  binding epoch are the caller's facts from Wormhole's and Clustor's records.
- **Placement** (`fd_placement_decode`, `fd_node_patch`): the controller's
  `EVACUATE_WORKLOADS` becomes a merge patch cordoning each node of the zone
  (`spec.unschedulable: true`) with the operation named in
  `wormhole.nanocloud.io/evacuated-by`; `RESTORE_WORKLOADS` undoes exactly
  that. The controller sends either only after its dial reached zero and
  the zone drained (or after the restore hold).

The module is `wormhole_fd`, between the controller's ports and the store
for one failure domain (`zone`, the nodes' `topology.kubernetes.io/zone`):
the controller's `status_out` on `status_in`, its answers on `ctl_out`
(the controller's `ctl_in`). A Status waiting for a preflight is answered
with `fd_preflight` over the nodes under `/nodes/` (ready, cordoned,
allocatable CPU, `nanocloud.io/store-voter`), the CPU requests of the Pods
bound to them (`/pods/`, `spec.nodeName`) and the facts Wormhole's and
Clustor's projections wrote at `/wh-fd/<zone>/facts`
(`endpoints=…;rgen=…;returning=…;standbys=…;route=…;keys=…;sovereignty=…;fence=…;conflicting=…;epochs=…`).
A Status waiting for an observation is answered with the `FdObservation`
for the dial generation it names — its sources: the zone's demand (its
Pods' requests) and readiness generation (the newest revision of its
nodes' rows), each dial arm's spare room (`arms`, the zone of each arm in
the dial's order) from the same nodes, and the health the director's
aggregation wrote at `/wh-fd/<zone>/health`
(`error_ppm=…;breaks=…;pinned=…;alarm=…`). A Placement is merged into every
node of the zone (`fd_node_patch`). Each request is answered once per
operation and generation; a missing or unparsable source answers nothing
and is logged, the controller's own deadlines deciding what silence means.
Proof: the cores in `wormhole_portfolio.rs`, the module in
`scripts/wormhole-modules-e2e.sh` (a preflight and an observation read back
from the store's nodes, Pods and rows; the zone's nodes cordoned and
marked, the others untouched).

## Usage, `service_dns`, the regional fence, RPKI and explain

- **Usage ingest** (`_shared/whusage.rs`): `usage_record.v1` applied by
  Wormhole's own ledger (in order; duplicates, conflicts and gaps told
  apart; a gap past the replay ring closed from totals and marked), each
  stream projected to `/wh-usage/<tenant>/<subject>/<exporter>` =
  `seq=…;chain=…;last=…;connections=…;bytes_in=…;bytes_out=…;refusals=…;gaps=…;from_totals=…`,
  and taken back from that row after a restart. The module is
  `wormhole_usage`: `usage_record.v1` on `usage_in` from Wormhole's
  exporters, the streams taken back from their rows at start, each record
  applied and its stream's row written before it is taken off the port (a
  write the store refuses leaves the ledger as it was, and the record is
  applied again).
- **`service_dns`** (`_shared/whdns.rs`): the region controller's
  projection signed by Kagi (`KSDN`) is opened with Kagi's own codec under
  the `SERVICE_DNS` key and checked by Wormhole's `dns_answers` (another
  service's, stale against the newest global generation seen, expired, or
  no healthy region the client may reach — refused); the answers come in
  preference order with the projection's TTL. `cluster_dns` serves from it:
  given the `SERVICE_DNS` key (`dns_issuer`, `dns_kid`, `dns_public`), a
  Service whose signed projection is stored at `/wh-dns/<service hex>` (its
  Wormhole identity) is answered with the projection's healthy regions'
  IPv4 addresses in preference order at its TTL, the newest global
  generation it answered kept per Service as the floor; a projection that
  is refused is SERVFAIL, never the local records in its place. The rows
  are `wormhole_dns`'s: the deployment's Kagi signer hands each signed
  projection on `ksdn_in` (`[len u16][KSDN]`), and every one that opens under
  the key is written verbatim at its Service's row when it is newer (global
  generation, then projection generation) than the row holds.
- **The regional fence provider** (`_shared/whfence.rs`): an upstream cut
  is confirmed only when every transit link of the region is down *and*
  its cut acknowledged by the upstream, power only when every feed reports
  its outlets off and acknowledged, under the provider's custody
  generation, as the `region_ctl.v1` RegionFence record; silence proves
  nothing. The module is `wormhole_fence` (one region, `mechanism`
  upstream or power): every `poll_ms` it reads `/wh-fence/<region>/custody`
  (`gen=…`) and the region's `link/<name>` (`down=…;ack=…`) or
  `feed/<name>` (`off=…;ack=…`) rows its observers write, and once per
  custody generation, only on proof, sends the RegionFence record on
  `fence_out` (the region controller's `ctl_in`) and writes
  `/wh-fence/<region>/confirmed`; anything less is logged and confirms
  nothing (`scripts/wormhole-modules-e2e.sh`: unproven while one cut is
  unacknowledged, confirmed once after). The actuation itself (cutting
  transit, removing power) **needs hardware**.
- **RPKI** (`_shared/rpki.rs`): a ROA (RFC 9582, a CMS signed object)
  validated against a configured trust anchor — the anchor self-signed and
  valid, the EE certificate under it and inside its RFC 3779 resources
  (or `inherit`), the signed attributes' content type and message digest,
  the signature, every prefix inside the EE's resources and every max
  length in range — into Wormhole's `Vrp`s, which `address_core`'s RFC 6811
  validation answers over. Fixture-tested (`tests/harness/tests/rpki.rs`,
  `fixtures/rpki/`). **Fetching** is `rpki_fetch`: RRDP (RFC 8182, read by
  `_shared/rrdp.rs`) over the net pair — the notification at `notify`, and
  when its session or serial moved the snapshot it names, taken only when
  its bytes hash to the notification's hash and it names the same session
  and serial; every `.roa` in it validated against the anchor at
  `/wh-rpki/anchor`; the payloads replace `/wh-rpki/vrps` whole and
  `/wh-rpki/status` says what was taken and refused
  (`tests/harness/tests/rrdp.rs`, `scripts/rpki-fetch-e2e.sh`: a repository
  served in a namespace, the fixtures' good ROAs' payloads stored and the
  bad ones counted, a tampered snapshot refused with the rows kept). Not
  done: rsync, RRDP deltas, CRLs and manifests,
  intermediate CA certificates, AS resources, address ranges.
- **Ownership evidence** (`_shared/ownership.rs`): whether a tenant holds a
  prefix it asks to import is proven by an RPKI Signed Checklist (RFC
  9323) under the trust anchor, its resources covering the prefix and its
  checklist signing the SHA-256 of that tenant's and prefix's
  `wormhole-ownership.v1` challenge; only then is it Wormhole's
  `OwnershipEvidence` (`verified`, the RSC's digest), which `address_core`'s
  import admission requires (`tests/harness/tests/ownership.rs`: another
  tenant, another prefix, resources outside the EE certificate, another
  anchor, an expired or altered checklist each refused by name).
- **Explain** (`nanocloud explain failovers <service>`,
  `_shared/whexplain.rs`): the region controller's `FailoverExplain` ledger
  rows (`/wh-ledger/<service hex>/<seq>`, each an `explain.v1` record) read
  in order, each checked against its predecessor with Wormhole's
  `LedgerCheck` and rendered one line each; the first gap or fork ends the
  listing and is named. `wormhole_ledger` writes them: the controller's
  `ledger_out` on `ledger_in`, every `FailoverExplain` Wormhole's decoder
  accepts written verbatim at its sequence's row (a replay of the same
  bytes left as it is, a row holding other bytes never overwritten —
  refused and counted, the CLI names the fork), DrStatus consumed. Not yet:
  explaining a placement (which needs the tier images the store does not
  hold).

## What Wormhole must do

1. **Mounting** (done in Wormhole): `traffic/cores.rs` lists the cores for a
   consumer that holds its own SDK SHA-256; `_shared/wormhole_traffic.rs`
   includes it.
2. **Record kinds** (done in Wormhole): 0x80 `LocalityPolicy`, 0x81
   `SourceAdmission` and 0x82 `GenerationSeal`, with `SealDigest` to verify a
   generation, are v1 kinds under these numbers. Wormhole refuses a source
   range with host bits past its prefix, so nanocloud masks each range to its
   prefix on emit (`10.0.0.5/8` is written as `10.0.0.0/8`).
3. **Read** `/wh/<t>/<s>/active`, then the sealed generation it names,
   verifying the seal before building a table.
4. **Write** the VIP bindings at `/wh/<t>/<s>/vip/<4|6>` and the cell demand
   at `/wh-demand/<cell>`.
5. **Empty endpoint sets** (decided): a manifest never has `count = 0`. A
   listener with no endpoints has no route in the generation, so a director
   holds no route for it and new flows take the off-route outcome.

## Limits

8 ports, 2 families, 64 endpoints, 16 source ranges per Service; past them
the Service is refused (`error=too-many`), never truncated. `sessionAffinity`
is Wormhole's to enforce: the nodes' nftables DNAT balances per connection.

## Proof

- `tests/harness/tests/wormhole_emit.rs`: identities against independent
  vectors; every record through Wormhole's decoders; the publication over a
  modelled store that answers EINPROGRESS (order, byte-identical re-asks, a
  lost race); the VIP binding's strict decode and the readiness gate.
- `scripts/wormhole-emit-e2e.sh`: the shipped graph; the records read back by
  an independent WHR1 reader (`scripts/support/whr1.py`); one active write per
  change; the status through ADVERTISED, ACTIVE/DRAINING and
  WITHDRAWN/RELEASED bindings; withdrawal.
- `scripts/endpoints-chronicle-e2e.sh`: ready → serving → terminating, a drain
  expired by its grace, zone, generations on reuse.
- `scripts/wormhole-cell-e2e.sh`: a demand provisioned, scaled, rolled and
  removed, and the shipped Deployment chain projecting its ReplicaSet.
- `scripts/wormhole-modules-e2e.sh`: each of `wormhole_ledger`,
  `wormhole_usage`, `wormhole_dns`, `wormhole_fence` and `wormhole_fd` in a
  graph of its own over the host store, fed what its producer sends (the
  harness's `emit_module_inputs_for_the_e2e`, Wormhole's and Kagi's own
  codecs): the ledger's rows verbatim and a replay rewriting nothing; a
  repeated usage record applied once; the signed projection written, its
  repeat and a forgery not; the fence confirmed only on proof, once; the
  preflight, the observation and the placement against the store.
- `scripts/rpki-fetch-e2e.sh`: `rpki_fetch` over RRDP from a repository in a
  namespace of its own.
