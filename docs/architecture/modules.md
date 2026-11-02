# Nanocloud as a Fluxor Graph

Nanocloud is a graph of cooperating [fluxor](../../../fluxor/) nodes. The
organizing idea: **a Kubernetes control plane is mostly APIs, and an API is a
pure transform** — request in, store reads and writes, response out. A pure
transform over records is what a rules VM does, so everything that is *logic*
— API handling, admission, scheduling, reconcile loops, policy compilation, DNS
projection, edge routing — is **rules**: authored in
`modules/app/_chronicle/*.plan` and compiled to the params that generic
Chronicle engines carry. Everything that is a *host fact* — sockets,
namespaces, cgroups, packet policy, durable disk, private keys — is reached
only through fluxor **capability surfaces**: kernel providers addressed by
contract class, which admission can gate.

Between the two sit the `.fmod` modules, and the control plane has
twenty-two of them. Twenty are **connectors**: each crosses one seam a rules VM
cannot — the store, a socket, a container, the packet filter, a mount, a key
service — and decides nothing, performing the key, field and op it is told
and reporting what happened. The other two are the CLI, an operator's entry
point rather than part of any chain, and `node_pki`, the node's certificate
and join service, which writes the bootstrap objects a joining operator needs
(its Namespace, the `cluster-admin` ClusterRole and binding, its Device). No
`.fmod` in this tree holds what a Deployment is, what `/api/v1` names, or
when a Pod may restart.

Three rules hold everywhere:

1. **Decision in rules, effect behind a surface.** A chain computes; a
   connector acts. The rules that decide packet policy publish rows to the
   store; encoding them as a `net.policy` table and replacing it is
   `net_apply`'s job, and realizing that table is the platform's.
2. **A connector holds no Kubernetes meaning.** It is told which key, which
   field, which op. Give one a special case for a resource and the next
   resource needs one too.
3. **A module holds no host facts of its own.** No module calls libc. One that
   needs a namespace, a socket or a durable byte asks a provider whose contract
   its manifest declares.

---

## 1. The store — the spine of the graph

Everything meets at the control-plane store, reached through two standard
fluxor contracts:

| Contract | Class | What it gives |
| -------- | ----- | ------------- |
| `storage.object` | `0x14` | keyed byte values, compare-and-swap on a precondition, ranged reads, delete |
| `storage.namespace` | `0x13` | prefix LIST with a cursor, SUBSCRIBE — a ring on the subscriber's own kernel event — and CHANGES, the paged change feed since a revision |

The Kubernetes object model rides on it as keys: `/pods/<ns>/<name>`,
`/services/<ns>/<name>`, `/deployments.apps/<ns>/<name>`, one prefix per
resource. A grouped resource is keyed group-qualified (`deployments.apps`) so it
never collides with a same-named resource in another group; core/v1 stays bare.
Objects are stored as the nested Kubernetes JSON the client sent; a few internal
projections (`/nodes/`, `/hpa-metrics/`, `/job-status/`, sandbox keys) use a
compact `k=v;k=v` form.

LIST + SUBSCRIBE is the idiom the whole plane is built from, and it is also
exactly the Kubernetes watch contract: list at a fence revision, then the
changes since it.

The store is **single-writer**. That is why the HTTP edge lives *inside* the
graph: the front terminates the socket and the request path writes the store
from within the runtime, rather than a second process appending to the same
file.

## 2. The API plane

The apiserver is a chain, not a module. Wave's `http` terminates HTTP/1.1
behind `tls` and hands each request to its application routes as an exchange
(fluxor's exchange contract: `http` the requester, the route's provider
answering); `http_chain` is that provider, and turns each exchange into one
request envelope for the chain and the chain's reply envelopes into its
response. Every stage after that is a decision carrying params, and every node
between the decisions moves bytes and decides nothing:

```
tls → http ⇄ http_chain → kube_decode → kube_route → admit → kagi_verify ⇄ token_verify
    → ident → fence → fenced → rbac → esc → escd → scope → disc → scoped → gate
    → ad_pol → ad_cnt → ad_dec → se_probe → se_act
    → reply → enc_http → http_chain ⇄ http
```

| Node | Type | Rules | What it does |
| ---- | ---- | ----- | ------- |
| `http_chain` | provider | — | each exchange one request envelope; the reply envelopes its response |
| `kube_decode` | pipeline | `kube.rd`, `kube.plan kd_pass` | projects the envelope into a record |
| `kube_route` | decision | `kube.plan kd_route` | which resource, namespace and name a path shape names |
| `admit` | decision | `apiplane.plan admit` | which credential to verify, the request riding as carry |
| `kagi_verify` | connector | — | the signature check, which is kagi's |
| `ident` | decision | `apiplane.plan ident` | the identity, namespace, verb and resource to authorize |
| `fence` | store_effect | `apiplane_fence.plan fence`, `fence_newer` | one COUNT of the caller's committed authority epochs newer than its `Nanocloud-Authority-Epoch` header (`/authority-epochs/<subject>/<epoch>`) |
| `fenced` | decision | `apiplane_fence.plan fenced` | a managed credential's write behind a committed epoch is 409, without the header 403; the record `ident` made, for `rbac` |
| `rbac` | store_effect | `apiplane.plan authz` | one `EXISTS_ANY` probe of the grant index |
| `esc` | store_big | `apiplane_esc.plan esc` | a write of a Role, ClusterRole, (Cluster)RoleBinding or ManagementBinding: `COVER` (`_shared/rbac_cover.rs`) — every permission it would grant held by the writer, or `escalate` / `bind` held |
| `escd` | decision | `apiplane_esc.plan escd` | a refusal (403, 404 for a binding's missing role, 422, 413, 503) as the verdict; RBAC's answer back for `scope` |
| `gate` | decision | `apiplane.plan gate` | admitted, or the refusal and its status |
| `ad_pol` | store_effect | `admission.plan ad_pol` | reads `/admission-policy/<resource>` |
| `ad_cnt` | store_effect | `admission.plan ad_cnt` | COUNTs live objects when a create is quota'd |
| `ad_dec` | pipeline | `admission.rd` + `admission.plan ad_dec` | the policy applied: defaults in, required paths looked up |
| `ad_ver` | decision | `admission.plan ad_dec` | the object as admitted, or 403 |
| `ad_q` … `ad_fin` | store_effect, decision | `admission_k8s.plan` | after `se_body`: a create past a ResourceQuota is 403; a `pods/eviction` spends its PodDisruptionBudget's disruption (a tested JSON Patch) or is 429, else becomes the Pod's delete |
| `se_probe` | store_effect | `apiplane.plan probe` | GETs the object or LISTs the collection |
| `se_act` | store_effect | `apiplane.plan act` | the PUT, DELETE or NOOP the verb implied |
| `reply` | decision | `apiplane.plan reply` | the status and body the client receives |
| `enc_http` | pipeline | `apiplane.plan encode` | renders `http_chain`'s reply envelope |

What a path names, which verb a method implies, whether a credential is good
enough, what a refusal answers, which store operation to run: all of it is
params compiled from `modules/app/_chronicle/apiplane.plan`.

Authorization is ONE store probe. Real `rbac.authorization.k8s.io/v1` Roles,
ClusterRoles, RoleBindings and ClusterRoleBindings are expanded by the `rb_`
chains into a grant index, `/rbac-g/<identity>/<scope>|<verb>|<group>|<resource>|<name>`
(`resourceNames` fill the last part; a `nonResourceURLs` grant is
`~|<verb>|~|<url>|~`, cluster scope only), and the `rbac` node asks whether a
grant covers the request — or the same with the namespace, verb, group,
resource or name as `*`, never widening a `~` — in one `EXISTS_ANY`. An
`aggregationRule` is `rbac_agg.plan`: every binding of an aggregating
ClusterRole is mirrored as a binding of each ClusterRole its selectors match
(depth ≤ 4), so no ClusterRole object is ever rewritten. Every derived row is
pruned when the object it came from no longer produces it, so revocation is as
fast as those chains. No node in the path holds Kubernetes meaning in compiled
code — save one, the escalation check below, whose rule cannot be a probe.

Privilege escalation is refused as upstream refuses it
(pkg/registry/rbac/*/policybased). Authorization answered whether the caller
may write a Role, ClusterRole, RoleBinding, ClusterRoleBinding or
ManagementBinding at all; `esc` (store_big op `COVER`, `_shared/rbac_cover.rs`)
then asks whether the object grants only what the writer already HOLDS:

- a Role's (ClusterRole's) rules, each verb × apiGroup × resource ×
  resourceName (every name `*` when it names none) and each nonResourceURL
  (cluster-wide only), probed in the writer's own grant index exactly as the
  `rbac` node would probe the request it grants — in the Role's namespace,
  cluster-wide for a ClusterRole — or the writer holds `escalate` on
  `roles` / `clusterroles` with that name. An `aggregationRule` gathers
  anything, so it needs full authority (`*` on `*.*` and every URL), as
  upstream's;
- a binding's role — read from the store, its rules held in the binding's
  namespace (cluster-wide for a ClusterRoleBinding); an aggregating
  ClusterRole's with every role its selectors gather (its /rbac-agg/ rows, to
  the mirror's depth) — or the writer holds `bind` on that role's name, in
  the binding's namespace. A roleRef naming no role is 404;
- a ManagementBinding is a ClusterRole and its ClusterRoleBinding in one
  (management.plan derives both): spec.rules and its own two rules held
  cluster-wide, or both `escalate` and `bind` on the ClusterRole
  `nanocloud-management:<b>`.

A patch (merge, strategic, JSON Patch, apply) is checked on its RESULT: the
stored object patched there as the store will patch it, so a patch that keeps
rules the writer does not hold is refused, as upstream checks the whole
object. A delete is not checked. Rules that grant nothing in the index (no
verbs, resources without apiGroups, a Role's URLs) need nothing. A body a
reader could read two ways — a duplicated or escaped member key — is 422;
past the check's bounds (4096 permissions, 32 gathered roles) 413: refused,
never admitted on a partial read. The grant index is the derived state the
`rbac` node trusts, so the check carries its lag: a permission granted the
writer a moment ago may not be held yet (retry), and an aggregating role's
gathered set is its /rbac-agg/ rows as they stand, as upstream's controller's
rules are. `scripts/rbac-escalation-e2e.sh` proves the refusals and both
exceptions through the API.

Revocation is bounded, not cached: nothing between a request and the grant
index holds a grant — every request probes the store — and the `rb_` chains
follow the RBAC objects by watch, so a role emptied, a binding deleted or a
subject removed is re-derived and its rows pruned at once, a few chained store
writes after the write that revoked. On the packaged control plane that is
under one request's round trip (the first request after the revoking write is
denied, ~30-50 ms on a Pi 5); `scripts/rbac-revoke-e2e.sh` holds it to
`NC_RBAC_REVOKE_MS` (2000 ms, the bound this documents, room for a loaded
host) for each of the three, and asserts that a denial, once seen, never flaps
back to a grant. Like upstream's informer-fed authorizer it is eventually
consistent, never stale for longer than the derivation takes. The one
difference in the other direction: a role that is edited is re-derived whole
(its rows are keyed by its digest), so a verb the edit keeps can be denied for
the same few writes, between the old rows' prune and the new rows' grant —
fail-closed, never wider than either the old or the new role.

The authority-epoch FENCE sits before authorization (`apiplane_fence.plan`):
a management plane's credential in a managed cell (a ManagementBinding's,
below) names the authority epoch it acts under on every write, and the cell
refuses a write behind the newest epoch it has committed for that credential
— 409 Conflict, so a stale management writer is stopped by the cell, not only
by its own compare-and-swap. The committed epochs are keys,
`/authority-epochs/<subject>/<epoch>`; the committed epoch is the greatest, a
set that only grows. `fence` COUNTs those newer than the header's (a
`count_where` predicate over the key tails), `fenced` decides. Reads and
callers with no committed epoch are not fenced. An epoch is committed only
through the binding's own path (`POST …/managementbindings/<b>/epoch`, where
`fenced` names the key and `probe` writes it).
[docs/design/managed-cells.md](../design/managed-cells.md) is the contract.

Admission is two store reads, a decode and one decision, all params
(`admission.plan`, `admission.rd`): `ad_pol` reads
`/admission-policy/<resource>`, `ad_cnt` COUNTs the live objects under
`/<resource>/<ns>/` when a create is quota'd, `ad_dec`'s decode program applies
the policy's defaults to the object (`JSONDEF`) and then looks each required
path up in the DEFAULTED object (`JSONAT`) — mutate, then validate, upstream's
order — and `ad_ver` answers 403 `missing <path>` for the first absent required
field, 403 `quota exceeded`, else 200 with the defaulted object. JSON is read
and written by the decode program; no decision parses it.

`http_chain` is the chain's door. It takes each exchange whole — its body
streamed on the credit it grants, up to `max_body_kib` (413 past it) — and
hands the chain ONE request envelope under a cid of its own:
`[cid u32][method u8][flags u8][target_len u16][hdr_len u16][body_len u16]`
then the target, headers and body, and the connection's verified mTLS peer as
a trailer behind flag `0x02`. A body past `body_inline_max` is staged in the
store at `<body_ref_prefix>sha256/<hex>` (its expiry beside it, for
`gc_blob`) and handed on as `sha256:<hex>` behind flag `0x04`. The chain
answers with reply envelopes —
`[cid u32][status u16][flags u8][ct_len u8][hdr_len u16][body_len u16]` then
the content type, headers and body; flag `0x01` says more follow, `0x04`
holds the response open (a watch) — and `http_chain` sends them as the
exchange's response within the server's credit, holding what the server has
not yet taken (up to 4 MiB an exchange, past which the exchange is aborted as
too slow, never holding another up). A reply to cid `0xFFFFFFFF`, or to an
exchange already gone, answers no one.

`kube_decode` is the chain's entry: a `pipeline` node whose Chronicle decode
program (`modules/app/_chronicle/kube.rd`, assembled by `scripts/rdasm.py`)
projects `http_chain`'s request envelope into a record — method, body, the
bearer credential with its scheme stripped, the raw target and query, the
verified peer identity from the trailer, the path segments and a store-key
candidate per path shape. `kube_route` (`kube.plan`) picks the shape:
resource, namespace, name and key.

`kagi_verify` is the chain's other connector — the role `store_source` and
`store_effect` play for the store, played here for kagi's
`token_verify`. A record arrives, the credential goes out as one exchange
(`kagi_verify` the requester, `token_verify` the provider of kagi's typed
operation: the body one `MSG_VERIFY_REQ` envelope of kagi's own `auth_wire`),
a typed `VerifiedIdentity` comes back in a 200, and the request's identity
rides across as carry-through in fields 30..=39. It decides nothing either — the
200/401/503 ladder is params (`modules/app/_chronicle/apiplane_auth.plan`), and
the signature check is kagi's, because a second JOSE verifier here is a second
answer to "is this credential genuine". It holds one request in flight, its
exchange id the correlation; an exchange that ends without a typed answer is
status 255, refused like any credential that did not verify, and a call the
verifier loses to `LINK DOWN` is sent again after `LINK UP`.
`scripts/apiplane-auth-e2e.sh` drives the seam on its own.

This chain is what `packaging/debian/fluxor-controlplane.yaml` boots, and
`packaging/debian/fluxor-apiplane.yaml` is the same path on its own for a test
to drive. `scripts/apiplane-e2e.sh` covers the whole surface: the CRUD verbs, the Kubernetes `List` and
`Status` envelopes, the discovery documents (literals in a decision — API
surface is data), an unauthenticated refusal, an unauthorized one, and
admission's defaulting and quota.

A collection GET — LIST or `?watch=true` — is served by a second chain
(`apiplane_watch.plan`) merging into the same reply stage. The request chain
registers the connection at `/watch-reg/<cid>` with the facts `kube_sel.rd` and
`kube_facts.rd` parsed from the query (resume revision, `timeoutSeconds`,
bookmarks, `limit`/`continue`, label and field selectors); the watch connector
(`store_source` `events`) serves every registration and runs the selector
filter — a Chronicle pipeline, `ir_stages` — on each candidate:

| | Served as |
| --- | --- |
| LIST | a stream: the typed envelope's head (`PodList`, …), the items that pass, then `metadata` with the revision the listing was read at and `continue` when `limit` cut it |
| WATCH | the store's change feed strictly after the registration's cursor: ADDED/MODIFIED/DELETED (DELETED carries the last state, from store_effect's `tomb`), BOOKMARK when asked, 410 Expired past the history, the end at `timeoutSeconds`; the exchange is HELD open between events |

`watch` is checked as its own RBAC verb, as Kubernetes does. Known limit: a
watch filtered by a label selector sees nothing when an object's labels stop
matching (upstream sends DELETED) — the change feed carries no prior value.

Identity is a host fact and stays one. `tls` runs `peer_auth` against the
cluster CA, so a client certificate that does not chain never opens a
connection at all; an accepted leaf's public key is hashed into a 32-byte SVID
and emitted on `peer_identity`, which wave's `http` carries on each exchange
and `http_chain` puts in the request envelope's trailer. The decode program lands it on the record, and the RBAC walk
authorizes that SVID. Absent a verified peer, the `Authorization` bearer token
is the credential, verified through `kagi_verify`. `scripts/apiplane-mtls-e2e.sh`
proves both gates: an unissued certificate is refused at the handshake, and an
issued one with no binding is refused at authorization.

## 3. Controllers

A controller is a pure function of store state, re-applied on change: it
SUBSCRIBEs its inputs, recomputes, and writes its outputs back — guarding every
write with a read-compare, so a settled cluster spends no revisions and a
controller that writes under a prefix it watches does not wake itself forever.
Every controller is params. That includes allocation, where uniqueness is the
store's create-only precondition rather than a module's private bitmap — an
allocator with no state of its own cannot disagree with the store about what it
handed out.

Packet policy is the same shape with one more connector. The chains write
rows; `net_apply` encodes them as a `net.policy` table and replaces it whole,
and the platform realizes it — nftables on Linux, the packet filter on bare
metal, a recorded no-op on wasm — with whether it is ENFORCED written to
`/np-status/<table>`. The chains run on the control plane; every node that
runs Pods runs the two `net_apply`s for itself, its status in its own records
(`/~node/<node>/np-status/<table>`), its name and address rendered in, so a
`Local` traffic policy keeps to its own backends and its host ports are its
own. NetworkPolicy is on by default (`NANOCLOUD_NETPOLICY=0` leaves it out).
A table is `enforced=1` only once its enforcer has applied exactly the
generation written (`pending` until then); the platform's tables are named
`fx_<instance>_<table>`:

| Prefix | Params | Rows |
| ------ | ------ | ---- |
| `px_` | `proxy.plan` | `/services/` JOIN their `/endpoint-rows/` → each backend's Pod read for its named ports → one DNAT row per (Service, READY backend — a notready row has none, and a serving or terminating one is draining and has none), ports as aligned lists (with the node ports and the load-balancer address, svc_alloc's `/lb-addr/`), a named targetPort resolved against that backend's Pod; the platform balances a VIP's rows. Pruned when the ClusterIP or the row moves, or the row stops being ready. `px_alloc` (`svc_alloc`) gives NodePort and LoadBalancer Services their node ports and pool address, claimed create-only at `/ipam/nodeport/<port>` and `/ipam/lb/<u32>`, the address recorded by Service at `/lb-addr/<ns>/<svc>` and said in its `status.loadBalancer`, released when no longer held; a Service of a `loadBalancerClass` gets node ports only. Each row carries the Service's traffic policies and the backend's node (net_apply keeps a `Local` policy to that node's backends); ClusterIP translates the source on hairpin, a `Cluster` node port or load-balancer address always, `Local` never |
| `hp_` | `proxy.plan` | `/pods/` one record per container → a row per container with hostPorts, for the node its Pod runs on, at `/np-rows/hostport/`; that node DNATs its address at each hostPort to the Pod. Pruned with the Pod |
| `np_` | `netpolicy.plan` | `/networkpolicies/` JOIN `/pods/` by podSelector → `/np-sel/` selections → per-rule ALLOW rows and a per-direction ISOLATE (Kubernetes' policyTypes reading); a selector peer allows nothing it cannot name. Selections and rows pruned by digest and pod address, conditional on the revision judged |

### The controller chains

Every workload controller is a **Chronicle chain**: `store_source` turns a
prefix subscription into record frames, a generic `decision` engine evaluates
the rules one record at a time, and `store_effect` performs the reads and
writes. The rules are authored in `modules/app/_chronicle/*.plan` and compiled
to the hex params a graph carries.

| Chain | Rules | Does |
| ----- | ----- | ---- |
| `dp_` | `deployment.plan` | Deployment → the owned ReplicaSet, the archived previous revision, the scaling event |
| `rs_` | `replicaset.plan` | ReplicaSet → exactly `replicas` Pods, ordinal by ordinal, rolled on a template-hash change |
| `ds_` | `daemonset.plan` | one node-pinned Pod per ready Node, pruned when a Node goes not-ready |
| `st_` | `statefulset.plan` | ordered identities: `<sts>-<i>` only once `<sts>-<i-1>` is ready; scale down highest-first |
| `jb_` | `job.plan`, `job_launch.plan`, `job_status.plan` | Jobs to Kubernetes' semantics: the Job's state at `/job-ctl/` (its Pods counted, the start/finish/backoff moments as deadlines on the timer), Pods started within parallelism and completions (NonIndexed, Indexed), backoffLimit, activeDeadlineSeconds, ttlSecondsAfterFinished, suspend, and the `.status` `kubectl wait` reads |
| `cj_` | `cronjob.plan`, `cronjob_status.plan` | CronJobs: the schedule judged by the source (`cron`) against the trusted clock only, concurrencyPolicy, startingDeadlineSeconds, history limits, status active / lastScheduleTime / lastSuccessfulTime |
| `pdb_` | `pdb.plan` | PodDisruptionBudget status (the selector judged by `count_sel`) |
| `q_` | `quota.plan` | ResourceQuota usage (`{}` expansion over `spec.hard`, a COUNT that sums) and status |
| `pc_` | `priority.plan` | PriorityClass: system classes, the global default, a Pod's priority resolved |
| `zn_`, `zl_`, `zc_`, `zs_` | `zone_node.plan`, `zone_summary.plan` | each Node → `/zone-node/<node>` (its zone, readiness, schedulability, voter label, allocatable, its Pods' requests) → per zone the sums → `/zonesummaries.nanocloud.io/<zone>`, rewritten only when a figure moves, deleted with the zone's last Node ([zone-summary.md](zone-summary.md)) |
| `te_` | `scheduler.plan` | the taint manager: NoExecute evictions, now or after `tolerationSeconds` |
| `hp_` | `hpa.plan` | `desired = ceil(R × currentCPU / targetCPU)` clamped `[min,max]`, patched onto the target |
| `gc_` | `gc.plan` | the ownerRef cascade, plus the Event count cap |
| `ns_` | `namespace.plan` | sweep every namespaced prefix of a Terminating namespace, then finalize |
| `sc_` | `scheduler.plan` | bind the first placeable Pod of the highest priority to the node the source's pick (`sched`, `_shared/sched.rs`: cordon, affinity, taints, resources, pod (anti-)affinity, spread) chose, one per pass; FailedScheduling; preemption |
| `rb_` | `rbac_rules.plan`, `rbac_grants.plan`, `rbac_prune.plan` | Kubernetes RBAC: roles → per-rule and per-(verb × resource) rows, bindings → per-subject rows, their join → the grant index the API plane probes; stale rows pruned by the source object's digest |
| `ep_` | `endpoints.plan`, `endpoints_doc.plan`, `ttl.plan` | Services JOIN Pods by selector → per-pod rows at `/endpoint-rows/<ns>/<svc>/<pod>` = `ip=…;state=<ready\|notready\|serving\|terminating>;zone=…;node=…;gen=…;af=…;uid=…[;at=…];grace=…` (the Pod read for its grace, its node for its `topology.kubernetes.io/zone`, the row for its generation, which moves on whenever the endpoint is reused) → the folded `/endpoint-lists/<ns>/<svc>` (a summary bounded by one record — cut at a whole entry past about 100 rows; no reader depends on it being whole), and the API's documents, discovery.k8s.io/v1 EndpointSlices and a v1 Endpoints at `/endpoints/<ns>/<svc>`, rendered from the Service and its rows by store_effect's `EP_DOCS` (`_shared/epdoc.rs`: conditions, targetRef, nodeName, zone, the Service's ports with a named targetPort resolved against each Pod; managed-by labels, each slice owned by its Service) and kept put-if-changed. The slices are cut as upstream's controller cuts them: one group per address family the Service serves (`spec.ipFamilies`, else its clusterIP's; a dual-stack Pod's other address from its `status.podIPs`) and set of resolved ports, each group in slices of at most 100 endpoints — the primary group's first named after the Service (`/endpointslices/<ns>/<svc>`), every other `<svc>-<digest>[-<n>]`, a slice no longer made deleted; the Endpoints holds the first family's addresses, at most 1000 (past them `endpoints.kubernetes.io/over-capacity: truncated`). `EP_DOCS` reads a Service's rows outside its record's journal (they outnumber its 96 calls), remembered across the serves of one held record, and its writes through it. Removal drains first: a deleted, deselected or vanished Pod's row is `terminating` from the moment its drain began, and the expiry chain (`ttl.plan` over `at!due+grace`) deletes it when its grace has run (a `notready` row goes at once); the rows of a gone Service, its fold, and the documents this controller manages go at once — a selectorless Service's own Endpoints and anyone's EndpointSlices are never touched |
| `pj_`, `pl_`, `pr_` | `pod_project.plan`, `pod_lifecycle.plan`, `pod_effect.plan` | the kubelet's decision half: the sandbox artifact, the lifecycle state machine and its writes, and the prune of a runtime record whose spec is gone |
| `pb_` | `probe.plan`, `probe_collect.plan` | liveness/readiness probes: per-probe state at `/probe-rt-<l\|r>/<uid>` whose `due`/`deadline` arm the kernel timer, exec attempts over the `/sandbox-exec/` seam, verdicts at `/probe-status/` (the lifecycle chain folds `live=0` into restart+backoff; the endpoints chain gates on `ready`) |
| `pi_` | `pod_image.plan` | an `image=` spec resolved to a rootfs and an argv: the pull trigger, the two gates, the answer |
| `sn_` | `snapshot.plan` | VolumeSnapshot → a bound VolumeSnapshotContent, marked ready |
| `dv_` | `device.plan` | a Device naming its public key → certified once by kagi's `certificate_endpoint` (via `http_effect`) → `/deviceidentities…/` and `/ca-cert` |
| `mb_` | `management.plan`, `management_sweep.plan` | a ManagementBinding (a managed cell's record of one management plane) → the Namespace `nanocloud-management`, the plane's ServiceAccount, its ClusterRole (its own rules + `spec.rules`, spliced) and ClusterRoleBinding, `spec.authorityEpoch` committed (`/authority-epochs/`), and the enrollment offer `/enrollment-offers/<b>/<grant>/<cell UID>/<digest>`, made once per grant, ever (CREATED only when a COUNT of the grant's offers, live or spent, is 0 — one write; an enrollment CLAIMS it, store_effect op 22, leaving it spent); an offer the binding no longer states (another grant, UID or digest; deleting; gone) claimed — withdrawn (`mb_wd`); a gone binding's ClusterRoleBinding, ClusterRole and ServiceAccount collected, its committed epochs and spent offers kept (a credential names the subject, so a binding made again under the name must fence its predecessor's) |
| `tk_` | `sa_token.plan` | a `/token-req/` → kagi's `token_mint` (via `kagi_mint`) → `/token-resp/` `token=<JWT>` |
| `is_` | `ipam_svc.plan` | ClusterIPs from 10.96.0.0/16: next-fit over a cursor, each address claimed create-only at `/ipam/svc/<u32>` (409 = taken, skip), PATCHed into `spec.clusterIP`; held addresses are claimed before allocation starts; a deleted Service's claim is released |
| `ip_` | `ipam_pod.plan`, `ipam_pod_release.plan` | pod IPs from the `/ipam-pool` CIDR: the same claims, committed as a create-only `/ipam-lease/<id>`; a request whose sandbox is gone releases its lease, request and claim |
| `wh_` (webhook graph) | `webhook_validator.plan` | Webhook validation: five comparisons in spec order as a first-hit decision, its message text as much the contract as its verdict → `/webhook-status/` |
| — | `route.plan` | the edge-route compile, below |
| — | `route_validator.plan` | first-error Route validation → `/route-status/` |
| — | `apiplane*.plan` | the API request path (the API plane, above) |
| — | `ttl.plan` | delete a record once its `deadline=` passes, armed on the kernel timer |

Params are baked into each graph because a `.deb` ships no compiler:
`scripts/chronicle-params.sh` compiles a `.plan` entry at test time, and
`scripts/chronicle-param-drift-e2e.sh` fails if a baked param has drifted from
the source it was compiled from.

### The edge-route compile

`route.plan` (graph `fluxor-route-compiler.yaml`) turns `/routes/` +
`/route-status/` + `/endpoint-rows/` into one row per route at
`/dataplane/edge/`, which the `http` edge SUBSCRIBEs for its DynRoute table. It
takes three passes, because the cardinality changes twice and the VM has no
iteration — each change of shape is a store round trip whose fan-out a SOURCE
performs:

| pass | from | to |
| --- | --- | --- |
| A | `/routes/` | `/route-be/<ns>/<svc>` — rekeyed BY SERVICE |
| B | `/route-be/` JOIN `/endpoint-rows/<ns>/<svc>/<pod>` | `/edge-be/<ns>/<svc>/<pod>` |
| C | `/route-be/` + `list_children` over `/edge-be/` | `/dataplane/edge/<ns>/<route>` |

The rekey in pass A is what makes it work: keying by service lets pass B scope
its join to the object (`join_scoped: 1`) and get one record per backend without
matching a middle key segment, which a decision cannot do. Reading the per-backend
`/endpoint-rows/` rather than the folded `/endpoint-lists/` document is what makes
the per-element work a JOIN rather than a map over a list.

## 4. The node plane

Running a container is two different things, and they sit either side of the
store:

- **decision** — which namespaces, which mounts, which image layers, in what
  order: a state machine over pod specs. That is the pod-lifecycle chain
  (`pl_`/`pj_`/`pr_`, params). It watches `/pod-specs/` and `/sandbox-status/`,
  projects `/sandboxes/`, and maps sandbox state to a pod phase — restart policy
  with exponential backoff (`min(1s × 2^(n-1), 300s)`, reset after a run
  survives 600s), two-phase kill with a grace deadline, and pause as a
  distinguishable non-terminal phase. Every deadline is a field on
  `/pod-runtime/<uid>` rather than private module state, and the source arms the
  kernel timer for whichever comes first.
- **execution** — the `unshare`/`clone3`, `pivot_root` and mount assembly,
  cgroup2 limits, veth and address realization, spawning PID 1 and reaping it.
  That is the fluxor `workload` contract (`0x1A`), reached through the
  `workload_effect` connector.

WHEN a sandbox is created, started, signalled, paused, exec'd into or torn down
is params (`sb_`, `sandbox.plan`): the desired `/sandboxes/` against the
observed `/sandbox-status/`, the IPAM lease, the pod's bound volumes
(`vb_`, `volume.plan` → workload `mount`/`bind` options) and the runtime's boot
(a status recorded under another boot is a workload that no longer exists).
`workload_effect` performs the op and owns only what cannot live in the store:
the live handles and their continuous observation (WAIT, READ, the PTY pump),
reported as events the chain writes back. A run's start is one of them: START
stamps the moment it starts the process, and every status of that run carries
it (`/sandbox-status/<uid>` `state=;code=;started=`), its exit included. The
Pod's report (`pod_report.plan`) takes a container's `startedAt` — and what
counts as a new run — from it, never from having observed the run running, so
a container that ends before any pass sees it run still reports a terminated
state with both its start and its finish. Nanocloud owns the image→rootfs and
pod-spec→argv mapping, and the backend reads no OCI format.

`platform_raw` is the tree's privileged grant, and exactly five modules carry
it — each because its one effect rewrites the host, and each on a module that
decides nothing:

| Module | What it rewrites |
| ------ | ---------------- |
| `workload_effect` | namespaces, cgroups and the processes in them (`workload`) |
| `net_apply` | the host's packet policy (`net_policy`) |
| `node_net` | the node's segment, peer routes and held addresses (`net_route`) |
| `mount_effect` | the mount table (`host_mount`, `proc`) |
| `volume_host` | a node's block devices and their unlocked mappings (`workload`, `proc`) |

The `workload` contract itself has two holders, `workload_effect` and
`volume_host`, so namespace surgery stays a grant a graph asks for by name.

The image plane sits beside them, and is chains too: `image.plan` turns
`/image-requests/` into one work queue `/fetch-q/`, then a manifest (through an
index: the arm64/linux entry), the runtime config and each missing layer, into
`/image-manifests/`, `/image-config/`, `/blobs/` and `/image-layer-ok/`;
`image_asm.plan` waits for every layer to be cached, runs the assembly job over
the sandbox seam, and writes `/image-rootfs/<name> state=ready|failed`. The
bytes move through the `http_fetch` connector.

## 5. The connectors

The control plane's `.fmod` modules are connectors: each crosses one seam a
rules VM cannot, and each decides nothing — told which key, which field, which
op, it performs it and reports what happened. `nanocloud_cli` is the
exception, and the only module a chain never talks to: it is the operator's
way into the same store. The encrypted-volume connectors are the same kind of
thing; `docs/design/encrypted-volumes.md` sets out the pipelines they serve.

| Module | Seam | Role |
| ------ | ---- | ---- |
| `store_source` | `storage.namespace`, `timer` | subscription-driven entry into a chain: a prefix's changes and timer deadlines become record frames, optionally walking each object's children (`list_children`, `list_values`) and decorating them as it goes |
| `store_effect` | `storage.object`, `storage.namespace` | the reads and writes a chain asks for — GET, CREATE, guarded PUT, PATCH, MERGE (conditional on a revision when one is given), REPLACE, APPEND, DELETE, LIST, COUNT (optionally of the children a `count_where` decision keeps), EXISTS and `EXISTS_ANY`, and `EP_DOCS` (a Service's EndpointSlice and Endpoints rendered from its rows, `_shared/epdoc.rs`) — over a key assembled from record fields; a fused decision builds the request from the arriving record, given the wall clock when `wall` is set |
| `workload_effect` | `workload` (`0x1A`) | op records (create/start/destroy/signal/pause/resume/exec/pty) → the workload contract; lifecycle, output, exec and pty events back. Six request inputs, so the runtime's chains feed it without a merge |
| `net_apply` | `net_policy` (`0x1E`) | rows under a prefix → one `net.policy` table, replaced whole (a row set that does not fit is not applied); the platform's ENFORCED bit and the generation at `/np-status/<table>` |
| `cluster_dns` | the net pair (datagram) | the cluster's DNS: `<svc>.<ns>.svc.cluster.local` answered from the store as the API keeps it — a Service's ClusterIP, a headless one's ready backends (its `/endpoint-rows/<ns>/<svc>/` rows, a paged walk, `ready` rows only: the first 32 in an answer, a backend's `<a-b-c-d>` name found among all of them), an ExternalName's CNAME, a named port's SRV, NXDOMAIN for no such Service — and every other name forwarded to the node's resolver |
| `svc_alloc` | `storage.object`, `storage.namespace` | NodePort and LoadBalancer allocation: node ports and a pool address claimed create-only, the node ports written into the Service conditional on its revision and the address recorded at `/lb-addr/<ns>/<svc>` and said in the Service's `status.loadBalancer` (the cell is the load balancer of a Service of no `loadBalancerClass`; one of any class gets node ports only, its address and status its class's external controller's, written through the API), released when no longer held (one module rather than two `is_`-shaped chains: a chain per allocator costs ~3.5 KiB of the runtime's config arena, this ~50 bytes); and every Node's Pod CIDR, a /24 of the cluster CIDR claimed at `/ipam/podcidr/<index>` and written into `spec.podCIDR`/`podCIDRs` |
| `node_net` | `net_route` (`0x20`), `storage.object`, `storage.namespace` | a node's one `net_route` writer: its segment from its Node's podCIDR (gateway the CIDR's first address, cluster prefixes the Pod and Service ranges), a peer route to every other node's podCIDR via its InternalIP, and the LoadBalancer addresses it holds the claim of (svc_alloc's `/lb-addr/`; `/net-hold/<u32>`, one node each, taken over from a node out of service; `Local` only where a ready endpoint is); what took effect (READ) at `/node-net/<node>` and `/sandbox-net`; its graph-backed Pods' address requests (`graph_pods`, bare metal) and the release of its Pods' leases once Pod and sandbox are gone |
| `http_fetch` | net pair, `fs` | one GET at a time (behind `tls`, or plaintext); the answer comes back as the JSON members the record asked for (`a[].b` collect, `a[k=v].b` select), or lands as a sha256-verified file (optionally in Range chunks); a request whose result already exists is answered 304 without a byte on the wire |
| `http_chain` | Wave's `http` (an exchange PROVIDER on an application route) | each exchange → one request envelope for a chain, a large body staged in the store; the chain's reply envelopes → the exchange's response, a watch held open, within the server's credit |
| `http_effect` | an HTTP endpoint, in the graph or through Wave's `http` client (an exchange REQUESTER) | a record → one exchange (kagi's `certificate_endpoint`) → the answer, with chosen top-level JSON members lifted, as a record |
| `kagi_verify` | kagi's `token_verify` (an exchange REQUESTER of the typed operation) | a record's bearer credential → a typed identity record; loads the key sets named by `key_paths` (default `/authn-keys/sa`) and forwards them as ONE keyset (`MSG_KEYSET_SNAPSHOT`); `issuer` one or several |
| `kagi_mint` | kagi's `token_mint` (an exchange REQUESTER of the typed operation) | a record → the signed token as a record; names the signing key by vault label and stores the public half the mint announces at `/authn-keys/sa` |
| `store_doc` | `storage.object` | the API writer's record adapter in front of lattice's JSON document dialect: each write leaves as one document request, and an object too large for the reply record is sent empty so the answer becomes a GET the watch chain serves |
| `store_stamped` | `storage.object`, `storage.namespace` | `store_effect`'s source built with a small heap, for a controller's writer: whole-object writes that follow a document `schema` are stamped with a uid and creationTimestamp, kept when rewritten, as an object the API created; and a controller's reader or deleter of objects past a record (its projections, its COUNT predicates, a delete's tomb read whole) |
| `store_big` | `storage.object`, `storage.namespace` | `store_effect`'s source built with a heap, for the few nodes handling a document past a record: a body the HTTP front staged, and an object projected from a heap copy. Every other `store_effect` stays bounded |
| `store_watch` | `storage.namespace`, `storage.object`, `timer` | `store_source`'s `events` mode with a heap — the watch connector: objects past one record streamed across records, and change batches past `EV_BUF` |
| `kagi_storage_effect` | Kagi's storage-key service | a volume-key effect in, storage-key requests out, the typed outcome back as a record |
| `loam_effect` | Loam's admin wire, `fs` | a volume effect in, admin-wire jobs out, the typed outcome back as a record |
| `volume_relay` | `storage.object`, `storage.namespace` | carries the encrypted-volume effects between the control plane and a node over the replicated store, so neither addresses the other |
| `mount_effect` | `host_mount`, `proc` | the claim chain's mount actuator: a `Mnt` record in, the raw facts of what ran back |
| `volume_files` | `host_mount`, `fs`, `storage.object` | a Pod's own volumes on its node (emptyDir, ConfigMap, Secret, projected, downwardAPI): made, projected and removed with the Pod. Projections are upstream's AtomicWriter layout (a `..data` link swapped, the whole set atomic for a reader), for a container and a graph-backed Pod alike (the runtime follows a link that stays beneath the volume) |
| `volume_host` | `workload`, `proc` | a node's volume effector: the volume's subgraph staged, unlocked through Kagi, published over NBD, and torn down |
| `node_pki` | kagi's `certificate_endpoint`, `tls`'s identity ports | the node's certificate: tls's identity request answered by the authority; with `join: 1`, the operator's first credential minted behind a one-time join secret, and the bootstrap objects it needs |
| `nanocloud_cli` | the host's argv and stdio | a cli-applet: argv in, the store read and written directly, bytes to stdout and a latched exit code — so the CLI works with no HTTP edge running |

The token mint and the certificate issuer sign under the same vault key,
`nanocloud.ca`: generated inside the vault on first open, sealed under the
node's `FLUXOR_SEAL_KEY` (written once by the package's postinst), and never
exported. That symmetric key and that signing key are the trust roots; no
private key is issued or written to the store. (The Debian postinst also
writes a self-signed node TLS key and the peer/operator keys under
/etc/nanocloud.io — `gen_node_cert`, `gen_peer_pki` — for the default `pins`
peer trust, `NANOCLOUD_PEER_TRUST` in packaging/debian/nanocloud-graph.sh.)

## 6. Capability-surface inventory

The complete host-fact boundary. Everything above it is an app module; nothing
below it is debt — it is the platform working as designed.

| Surface | Class | Provides |
| ------- | ----- | -------- |
| `storage.object` | `0x14` | keyed byte values, CAS, ranged read, delete |
| `storage.namespace` | `0x13` | prefix LIST with cursor, change SUBSCRIBE (an event ring), paged CHANGES since a revision |
| `workload` | `0x1A` | isolated workload lifecycle: typed CREATE header (identity, posture, resource intents, network identity) + a backend-opaque options envelope; CREATE/START/READ/SIGNAL/WAIT/DESTROY plus exec and TTY |
| `fs` | `0x09` | files — the blob cache `http_fetch` lands image layers in |
| `net_policy` | `0x1E` | the host's packet policy as a table, replaced whole; the platform realizes it and reports whether it is enforced |
| `host_mount` | `0x1F` | the host's mount table: a filesystem mounted and unmounted where a claim asks for it |
| `proc` | `0x16` | a host process run to completion, its output and status returned — a host-scoped class, so a graph asks for it by name |
| key vault | `0x0010` | private keys held by the kernel, signed by handle |
| `timer` | `0x06` | deadline wakeups for the Chronicle store source |
| `linux_net` / `tls` | channel contract | leased sockets and TLS termination, delivered as a `net_proto` stream |

A module's manifest declares what it needs; a graph that asks for a surface it
was not granted fails at admission rather than at runtime. The clocks are not a
contract: a module reads uptime and wall time through `dev_millis` /
`dev_unix_millis` and declares a `timer_class` so the scheduler knows whether a
relaxed tick is safe for it.

## 7. Hot paths, as wiring

```
API write:   linux_net → tls → http → decode → route → verify → authorize
             → admission → store PUT (CAS) ─→ every watcher's change stream

List/Watch:  the request chain registers the connection; the watch connector
             streams the listing, or reads the change feed from the
             registration's cursor, filters by the selectors, and writes each
             item or event into the reply stage on the connection wave holds

Reconcile:   ep_ chain: /services JOIN /pods → per-pod slices (ready,
             serving, terminating; drained for their grace) → the folded
             /endpoints document, which cluster_dns answers headless names
             from; px_ rows → each node's net_apply → its `proxy`
             net.policy table; svc_alloc → /lb-addr/ and the
             status.loadBalancer of a Service of no class; zn_/zs_ → each
             zone's ZoneSummary

Pod start:   apps/v1 PUT → dp_ → rs_ → Pods → sc_ binds → pj_/pl_ project
             /sandboxes/ → the im_ chains over http_fetch materialise the
             rootfs → sb_ chain → workload_effect CREATE/START → status written back →
             the watch chain serves it
```

## 8. Repository layout and workflow

```
fluxor.toml / fluxor.lock       project shape; registry-pinned dependencies
modules/app/<name>/             one module: mod.rs + manifest.toml
modules/app/_shared/            include-only helpers: JSON walk, store ops,
                                compact fields, events, key paths
modules/app/_chronicle/         controller rules authored for Chronicle params
target/fluxor/fluxor-abi/sdk/   the SDK the modules compile against
target/fluxor/aarch64/modules/  the built .fmod artefacts
packaging/debian/               graphs, bundle sources and the .deb build
scripts/*-e2e.sh                live graph E2Es — the behaviour gate
```

```bash
fluxor update && fluxor sync             # resolve + materialise
make build                               # build modules/app/* for bcm2712
scripts/endpoints-reconciler-e2e.sh      # prove one loop live
make test                                # every E2E
packaging/debian/build.sh                # the .deb: runtime + fmods + graphs
```

This checkout is a member of the user-local fluxor workspace
(`~/.fluxor/workspace.toml`), so dependencies resolve live from the sibling
fluxor tree during development and the lockfile pins take over outside it.

## 9. Adding to the graph

### A new controller is a chain, not a module

Start from the rules. A controller that watches prefixes, decides one record at
a time and writes the answer back needs no new `.fmod`: author its rules in
`modules/app/_chronicle/<name>.plan`, wire `store_source → decision →
store_effect` in the graph, and bake the compiled params in. Give the nodes a
short shared prefix (`dp_`, `gc_`, …) so a graph of a hundred nodes still reads.
A change of cardinality — one object becoming N records — is a store round trip
whose fan-out a SOURCE performs, because the VM has no iteration by
construction.

Write a module only for what a rules VM cannot express: a walk over a set, a
signature check, bytes moved to something outside the graph. Those are
connectors and they decide nothing — `store_effect`, `kagi_verify`,
`http_fetch` and `workload_effect` are the pattern.

### A new module

A new module is a directory under `modules/app/` with `mod.rs` and
`manifest.toml`, and it follows the conventions the existing ones share:

- **`no_std`, no relocations.** A PIC module does not relocate the inner
  pointers of a nested const table, so a `&[(&[u8], &[u8])]` of prefixes
  dereferences garbage. Keep such tables as direct statics used inline, or
  return them from a `match`.
- **Bounded buffers, unbounded prefixes.** Use the shared `ListWalk`: it holds
  one provider page at a time and ends only when the provider says the listing
  is complete. Listing a prefix whole into a fixed buffer truncates silently and
  reads as convergence.
- **Watch inputs, never outputs.** SUBSCRIBE the prefixes you read; a
  subscription on a prefix you write wakes you forever. Where a controller must
  watch a prefix it writes (a ReplicaSet self-healing its Pods), every write is
  guarded so the settled state is quiet.
- **Guard every write.** Read-compare before PUT, and delete only what exists.
  A quiet cluster should spend no revisions.
- **Declare your cadence.** `timer_class = "agnostic"` for a module that only
  polls its watches; `"wall_clock"` for one that measures elapsed time, so it
  stays correct under an adaptive tick.
- **Own your prefixes.** One writer per prefix, named in the manifest header.
  Two modules writing one key is a design error, not a race to fix.
- **Subscribe on one kernel event, and keep a ring owed until a pass ends.**
  The store writes no channel of a subscriber's: a module creates ONE event
  (`_shared/store.rs` `store_event`, the manifest's `event` resource) and
  names it in every SUBSCRIBE; each step `store_take` asks which
  subscriptions rang. A ring is coalesced — one per burst — so the pass it
  owes stays owed (`dirty`) until a pass COMPLETES: one the store answered
  `EAGAIN`, `ENOMEM` or a refusal is run again on the module's timer, never
  left for a next change that may not come. Declare the demand: a
  `store_subscriptions` constant, or `demand_from_params` counting the
  params that name prefixes, and `store_subscribers = 1`; a subscriber never
  attests `resume_after_fault`.
- **Prove it live.** Add a `scripts/<name>-e2e.sh` that boots the real `.fmod`
  in a runtime and asserts on the store. Inline `#[cfg(test)]` is forbidden
  under `modules/`: in a `no_std` module it compiles away silently.
