# Nanocloud as a Fluxor Graph

Nanocloud is a graph of cooperating [fluxor](../../../fluxor/) nodes. The
organizing idea: **a Kubernetes control plane is mostly APIs, and an API is a
pure transform** — request in, store reads and writes, response out. A pure
transform over records is what a rules VM does, so everything that is *logic*
— API handling, admission, scheduling, reconcile loops, policy compilation, DNS
projection, edge routing — is **rules**: authored in
`modules/app/_chronicle/*.uproc` and compiled to the params that generic
Chronicle engines carry. Everything that is a *host fact* — sockets,
namespaces, cgroups, packet policy, durable disk, private keys — is reached
only through fluxor **capability surfaces**: kernel providers addressed by
contract class, which admission can gate.

Between the two sit the `.fmod` modules, and the control plane has twenty
of them. Eighteen are **connectors**: each crosses one seam a rules VM
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
| `storage.namespace` | `0x13` | prefix LIST with a cursor, and SUBSCRIBE — a pushed change stream with a revision fence |

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
    → ident → rbac → gate → ad_pol → ad_cnt → ad_dec → se_probe → se_act
    → reply → enc_http → http_chain ⇄ http
```

| Node | Type | Rules | What it does |
| ---- | ---- | ----- | ------- |
| `http_chain` | provider | — | each exchange one request envelope; the reply envelopes its response |
| `kube_decode` | pipeline | `kube.rd`, `kube.uproc kd_pass` | projects the envelope into a record |
| `kube_route` | decision | `kube.uproc kd_route` | which resource, namespace and name a path shape names |
| `admit` | decision | `apiplane.uproc admit` | which credential to verify, the request riding as carry |
| `kagi_verify` | connector | — | the signature check, which is kagi's |
| `ident` | decision | `apiplane.uproc ident` | the identity, namespace, verb and resource to authorize |
| `rbac` | store_effect | `apiplane.uproc authz` | one `EXISTS_ANY` probe of the grant index |
| `gate` | decision | `apiplane.uproc gate` | admitted, or the refusal and its status |
| `ad_pol` | store_effect | `admission.uproc ad_pol` | reads `/admission-policy/<resource>` |
| `ad_cnt` | store_effect | `admission.uproc ad_cnt` | COUNTs live objects when a create is quota'd |
| `ad_dec` | pipeline | `admission.rd` + `admission.uproc ad_dec` | the policy applied: defaults in, required paths looked up |
| `ad_ver` | decision | `admission.uproc ad_dec` | the object as admitted, or 403 |
| `ad_q` … `ad_fin` | store_effect, decision | `admission_k8s.uproc` | after `se_body`: a create past a ResourceQuota is 403; a `pods/eviction` spends its PodDisruptionBudget's disruption (a tested JSON Patch) or is 429, else becomes the Pod's delete |
| `se_probe` | store_effect | `apiplane.uproc probe` | GETs the object or LISTs the collection |
| `se_act` | store_effect | `apiplane.uproc act` | the PUT, DELETE or NOOP the verb implied |
| `reply` | decision | `apiplane.uproc reply` | the status and body the client receives |
| `enc_http` | pipeline | `apiplane.uproc encode` | renders `http_chain`'s reply envelope |

What a path names, which verb a method implies, whether a credential is good
enough, what a refusal answers, which store operation to run: all of it is
params compiled from `modules/app/_chronicle/apiplane.uproc`.

Authorization is ONE store probe. Real `rbac.authorization.k8s.io/v1` Roles,
ClusterRoles, RoleBindings and ClusterRoleBindings are expanded by the `rb_`
chains into a grant index, `/rbac-g/<identity>/<scope>|<verb>|<group>|<resource>|<name>`
(`resourceNames` fill the last part; a `nonResourceURLs` grant is
`~|<verb>|~|<url>|~`, cluster scope only), and the `rbac` node asks whether a
grant covers the request — or the same with the namespace, verb, group,
resource or name as `*`, never widening a `~` — in one `EXISTS_ANY`. An
`aggregationRule` is `rbac_agg.uproc`: every binding of an aggregating
ClusterRole is mirrored as a binding of each ClusterRole its selectors match
(depth ≤ 4), so no ClusterRole object is ever rewritten. Every derived row is
pruned when the object it came from no longer produces it, so revocation is as
fast as those chains. No node in the path holds Kubernetes meaning in compiled
code.

Admission is two store reads, a decode and one decision, all params
(`admission.uproc`, `admission.rd`): `ad_pol` reads
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
candidate per path shape. `kube_route` (`kube.uproc`) picks the shape:
resource, namespace, name and key.

`kagi_verify` is the chain's other connector — the role `store_source` and
`store_effect` play for the store, played here for kagi's
`token_verify`. A record arrives, the credential goes out as one exchange
(`kagi_verify` the requester, `token_verify` the provider of kagi's typed
operation: the body one `MSG_VERIFY_REQ` envelope of kagi's own `auth_wire`),
a typed `VerifiedIdentity` comes back in a 200, and the request's identity
rides across as carry-through in fields 30..=39. It decides nothing either — the
200/401/503 ladder is params (`modules/app/_chronicle/apiplane_auth.uproc`), and
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
(`apiplane_watch.uproc`) merging into the same reply stage. The request chain
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
(`/~node/<node>/np-status/<table>`). NetworkPolicy is opt-in
(`NANOCLOUD_NETPOLICY=1`): its chain does not fit beside the image pull in a
single node's runtime config arena (fluxor's `CONFIG_ARENA_SIZE`, 256 KiB):

| Prefix | Params | Rows |
| ------ | ------ | ---- |
| `px_` | `proxy.uproc` | `/services/` JOIN their `/endpointslices/` → each backend's Pod read for its named ports → one DNAT row per (Service, ready backend), ports as aligned lists (with the node ports and the load-balancer address), a named targetPort resolved against that backend's Pod; the platform balances a VIP's rows. Pruned when the ClusterIP or the slice moves. `px_alloc` (`svc_alloc`) gives NodePort and LoadBalancer Services their node ports and pool address, claimed create-only at `/ipam/nodeport/<port>` and `/ipam/lb/<u32>`, released when no longer held |
| `np_` | `netpolicy.uproc` | `/networkpolicies/` JOIN `/pods/` by podSelector → `/np-sel/` selections → per-rule ALLOW rows and a per-direction ISOLATE (Kubernetes' policyTypes reading); a selector peer allows nothing it cannot name. Selections and rows pruned by digest and pod address, conditional on the revision judged |

### The controller chains

Every workload controller is a **Chronicle chain**: `store_source` turns a
prefix subscription into record frames, a generic `decision` engine evaluates
the rules one record at a time, and `store_effect` performs the reads and
writes. The rules are authored in `modules/app/_chronicle/*.uproc` and compiled
to the hex params a graph carries.

| Chain | Rules | Does |
| ----- | ----- | ---- |
| `dp_` | `deployment.uproc` | Deployment → the owned ReplicaSet, the archived previous revision, the scaling event |
| `rs_` | `replicaset.uproc` | ReplicaSet → exactly `replicas` Pods, ordinal by ordinal, rolled on a template-hash change |
| `ds_` | `daemonset.uproc` | one node-pinned Pod per ready Node, pruned when a Node goes not-ready |
| `st_` | `statefulset.uproc` | ordered identities: `<sts>-<i>` only once `<sts>-<i-1>` is ready; scale down highest-first |
| `jb_` | `job.uproc`, `job_launch.uproc`, `job_status.uproc` | Jobs to Kubernetes' semantics: the Job's state at `/job-ctl/` (its Pods counted, the start/finish/backoff moments as deadlines on the timer), Pods started within parallelism and completions (NonIndexed, Indexed), backoffLimit, activeDeadlineSeconds, ttlSecondsAfterFinished, suspend, and the `.status` `kubectl wait` reads |
| `cj_` | `cronjob.uproc`, `cronjob_status.uproc` | CronJobs: the schedule judged by the source (`cron`) against the trusted clock only, concurrencyPolicy, startingDeadlineSeconds, history limits, status active / lastScheduleTime / lastSuccessfulTime |
| `pdb_` | `pdb.uproc` | PodDisruptionBudget status (the selector judged by `count_sel`) |
| `q_` | `quota.uproc` | ResourceQuota usage (`{}` expansion over `spec.hard`, a COUNT that sums) and status |
| `pc_` | `priority.uproc` | PriorityClass: system classes, the global default, a Pod's priority resolved |
| `te_` | `scheduler.uproc` | the taint manager: NoExecute evictions, now or after `tolerationSeconds` |
| `hp_` | `hpa.uproc` | `desired = ceil(R × currentCPU / targetCPU)` clamped `[min,max]`, patched onto the target |
| `gc_` | `gc.uproc` | the ownerRef cascade, plus the Event count cap |
| `ns_` | `namespace.uproc` | sweep every namespaced prefix of a Terminating namespace, then finalize |
| `sc_` | `scheduler.uproc` | bind the first placeable Pod of the highest priority to the node the source's pick (`sched`, `_shared/sched.rs`: cordon, affinity, taints, resources, pod (anti-)affinity, spread) chose, one per pass; FailedScheduling; preemption |
| `rb_` | `rbac_rules.uproc`, `rbac_grants.uproc`, `rbac_prune.uproc` | Kubernetes RBAC: roles → per-rule and per-(verb × resource) rows, bindings → per-subject rows, their join → the grant index the API plane probes; stale rows pruned by the source object's digest |
| `ep_` | `endpoints.uproc` | Services JOIN Pods by selector → per-pod slices → the folded `/endpoints/` document; a slice whose Pod or Service is gone, and the document of a gone Service, pruned |
| `pj_`, `pl_`, `pr_` | `pod_project.uproc`, `pod_lifecycle.uproc`, `pod_effect.uproc` | the kubelet's decision half: the sandbox artifact, the lifecycle state machine and its writes, and the prune of a runtime record whose spec is gone |
| `pb_` | `probe.uproc`, `probe_collect.uproc` | liveness/readiness probes: per-probe state at `/probe-rt-<l\|r>/<uid>` whose `due`/`deadline` arm the kernel timer, exec attempts over the `/sandbox-exec/` seam, verdicts at `/probe-status/` (the lifecycle chain folds `live=0` into restart+backoff; the endpoints chain gates on `ready`) |
| `pi_` | `pod_image.uproc` | an `image=` spec resolved to a rootfs and an argv: the pull trigger, the two gates, the answer |
| `sn_` | `snapshot.uproc` | VolumeSnapshot → a bound VolumeSnapshotContent, marked ready |
| `dv_` | `device.uproc` | a Device naming its public key → certified once by kagi's `certificate_endpoint` (via `http_effect`) → `/deviceidentities…/` and `/ca-cert` |
| `tk_` | `sa_token.uproc` | a `/token-req/` → kagi's `token_mint` (via `kagi_mint`) → `/token-resp/` `token=<JWT>` |
| `is_` | `ipam_svc.uproc` | ClusterIPs from 10.96.0.0/16: next-fit over a cursor, each address claimed create-only at `/ipam/svc/<u32>` (409 = taken, skip), PATCHed into `spec.clusterIP`; held addresses are claimed before allocation starts; a deleted Service's claim is released |
| `ip_` | `ipam_pod.uproc`, `ipam_pod_release.uproc` | pod IPs from the `/ipam-pool` CIDR: the same claims, committed as a create-only `/ipam-lease/<id>`; a request whose sandbox is gone releases its lease, request and claim |
| `wh_` | `webhook_validator.uproc` | Webhook validation: five comparisons in spec order as a first-hit decision, its message text as much the contract as its verdict → `/webhook-status/` |
| — | `route.uproc` | the edge-route compile, below |
| — | `route_validator.uproc` | first-error Route validation → `/route-status/` |
| — | `apiplane*.uproc` | the API request path (the API plane, above) |
| — | `ttl.uproc` | delete a record once its `deadline=` passes, armed on the kernel timer |

Params are baked into each graph because a `.deb` ships no compiler:
`scripts/chronicle-params.sh` compiles a `.uproc` entry at test time, and
`scripts/chronicle-param-drift-e2e.sh` fails if a baked param has drifted from
the source it was compiled from.

### The edge-route compile

`route.uproc` (graph `fluxor-route-compiler.yaml`) turns `/routes/` +
`/route-status/` + `/endpointslices/` into one row per route at
`/dataplane/edge/`, which the `http` edge SUBSCRIBEs for its DynRoute table. It
takes three passes, because the cardinality changes twice and the VM has no
iteration — each change of shape is a store round trip whose fan-out a SOURCE
performs:

| pass | from | to |
| --- | --- | --- |
| A | `/routes/` | `/route-be/<ns>/<svc>` — rekeyed BY SERVICE |
| B | `/route-be/` JOIN `/endpointslices/<ns>/<svc>/<pod>` | `/edge-be/<ns>/<svc>/<pod>` |
| C | `/route-be/` + `list_children` over `/edge-be/` | `/dataplane/edge/<ns>/<route>` |

The rekey in pass A is what makes it work: keying by service lets pass B scope
its join to the object (`join_scoped: 1`) and get one record per backend without
matching a middle key segment, which a decision cannot do. Reading the per-backend
`/endpointslices/` rather than the folded `/endpoints/` document is what makes
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
is params (`sb_`, `sandbox.uproc`): the desired `/sandboxes/` against the
observed `/sandbox-status/`, the IPAM lease, the pod's bound volumes
(`vb_`, `volume.uproc` → workload `mount`/`bind` options) and the runtime's boot
(a status recorded under another boot is a workload that no longer exists).
`workload_effect` performs the op and owns only what cannot live in the store:
the live handles and their continuous observation (WAIT, READ, the PTY pump),
reported as events the chain writes back. Nanocloud owns the image→rootfs
and pod-spec→argv mapping, and the backend reads no OCI format.

`platform_raw` is the tree's privileged grant, and exactly four modules carry
it — each because its one effect rewrites the host, and each on a module that
decides nothing:

| Module | What it rewrites |
| ------ | ---------------- |
| `workload_effect` | namespaces, cgroups and the processes in them (`workload`) |
| `net_apply` | the host's packet policy (`net_policy`) |
| `mount_effect` | the mount table (`host_mount`, `proc`) |
| `volume_host` | a node's block devices and their unlocked mappings (`workload`, `proc`) |

The `workload` contract itself has two holders, `workload_effect` and
`volume_host`, so namespace surgery stays a grant a graph asks for by name.

The image plane sits beside them, and is chains too: `image.uproc` turns
`/image-requests/` into one work queue `/fetch-q/`, then a manifest (through an
index: the arm64/linux entry), the runtime config and each missing layer, into
`/image-manifests/`, `/image-config/`, `/blobs/` and `/image-layer-ok/`;
`image_asm.uproc` waits for every layer to be cached, runs the assembly job over
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
| `store_effect` | `storage.object`, `storage.namespace` | the reads and writes a chain asks for — GET, CREATE, guarded PUT, PATCH, MERGE (conditional on a revision when one is given), REPLACE, APPEND, DELETE, LIST, COUNT (optionally of the children a `count_where` decision keeps), EXISTS and `EXISTS_ANY` — over a key assembled from record fields; a fused decision builds the request from the arriving record, given the wall clock when `wall` is set |
| `workload_effect` | `workload` (`0x1A`) | op records (create/start/destroy/signal/pause/resume/exec/pty) → the workload contract; lifecycle, output, exec and pty events back. Six request inputs, so the runtime's chains feed it without a merge |
| `net_apply` | `net_policy` (`0x1E`) | rows under a prefix → one `net.policy` table, replaced whole (a row set that does not fit is not applied); the platform's ENFORCED bit and the generation at `/np-status/<table>` |
| `cluster_dns` | the net pair (datagram) | the cluster's DNS: `<svc>.<ns>.svc.cluster.local` answered from the store as the API keeps it — a Service's ClusterIP, a headless one's ready backends (its `/endpoints/` document), an ExternalName's CNAME, a named port's SRV, NXDOMAIN for no such Service — and every other name forwarded to the node's resolver |
| `svc_alloc` | `storage.object`, `storage.namespace` | NodePort and LoadBalancer allocation: node ports and a pool address claimed create-only, written into the Service conditional on its revision, released when no longer held (one module rather than two `is_`-shaped chains: a chain per allocator costs ~3.5 KiB of the runtime's config arena, this ~50 bytes) |
| `http_fetch` | net pair, `fs` | one GET at a time (behind `tls`, or plaintext); the answer comes back as the JSON members the record asked for (`a[].b` collect, `a[k=v].b` select), or lands as a sha256-verified file (optionally in Range chunks); a request whose result already exists is answered 304 without a byte on the wire |
| `http_chain` | Wave's `http` (an exchange PROVIDER on an application route) | each exchange → one request envelope for a chain, a large body staged in the store; the chain's reply envelopes → the exchange's response, a watch held open, within the server's credit |
| `http_effect` | an HTTP endpoint, in the graph or through Wave's `http` client (an exchange REQUESTER) | a record → one exchange (kagi's `certificate_endpoint`) → the answer, with chosen top-level JSON members lifted, as a record |
| `kagi_verify` | kagi's `token_verify` (an exchange REQUESTER of the typed operation) | a record's bearer credential → a typed identity record; loads the verification key from `/authn-keys/sa` |
| `kagi_mint` | kagi's `token_mint` (an exchange REQUESTER of the typed operation) | a record → the signed token as a record; names the signing key by vault label and stores the public half the mint announces at `/authn-keys/sa` |
| `store_doc` | `storage.object` | the API writer's record adapter in front of lattice's JSON document dialect: each write leaves as one document request, and an object too large for the reply record is sent empty so the answer becomes a GET the watch chain serves |
| `store_stamped` | `storage.object`, `storage.namespace` | `store_effect`'s source built with a small heap, for a controller's writer: whole-object writes that follow a document `schema` are stamped with a uid and creationTimestamp, kept when rewritten, as an object the API created; and a controller's reader or deleter of objects past a record (its projections, its COUNT predicates, a delete's tomb read whole) |
| `store_big` | `storage.object`, `storage.namespace` | `store_effect`'s source built with a heap, for the few nodes handling a document past a record: a body the HTTP front staged, and an object projected from a heap copy. Every other `store_effect` stays bounded |
| `store_watch` | `storage.namespace`, `storage.object`, `timer` | `store_source`'s `events` mode with a heap — the watch connector: objects past one record streamed across records, and change batches past `EV_BUF` |
| `kagi_storage_effect` | Kagi's storage-key service | a volume-key effect in, storage-key requests out, the typed outcome back as a record |
| `loam_effect` | Loam's admin wire, `fs` | a volume effect in, admin-wire jobs out, the typed outcome back as a record |
| `volume_relay` | `storage.object`, `storage.namespace` | carries the encrypted-volume effects between the control plane and a node over the replicated store, so neither addresses the other |
| `mount_effect` | `host_mount`, `proc` | the claim chain's mount actuator: a `Mnt` record in, the raw facts of what ran back |
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
| `storage.namespace` | `0x13` | prefix LIST with cursor, change SUBSCRIBE with a revision fence |
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

Reconcile:   ep_ chain: /services JOIN /pods → per-pod slices → the folded
             /endpoints document, which cluster_dns answers headless names
             from; px_ rows → each node's net_apply → its `proxy`
             net.policy table

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
`modules/app/_chronicle/<name>.uproc`, wire `store_source → decision →
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
- **Take your change sink from a self-edge.** A module cannot open a channel,
  so a graph gives it one by wiring its own `status` output back to its
  `changes` input (a one-node cycle); the module hands that channel to
  SUBSCRIBE and drains the pushed events. Declare both ports even though
  nothing writes `status`.
- **Prove it live.** Add a `scripts/<name>-e2e.sh` that boots the real `.fmod`
  in a runtime and asserts on the store. Inline `#[cfg(test)]` is forbidden
  under `modules/`: in a `no_std` module it compiles away silently.
