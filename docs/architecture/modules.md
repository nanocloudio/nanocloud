# Nanocloud as a Module Graph

Nanocloud is a graph of cooperative [fluxor](../../../fluxor/) modules. The
organizing idea: **a Kubernetes control plane is mostly APIs, and an API is a
pure transform** — request in, store reads and writes, response out. Pure
transforms are exactly what fluxor app modules are. Everything that is *logic*
— API handling, admission, scheduling, reconcile loops, policy compilation, DNS
projection, edge routing — is a `no_std` PIC module wired over channels and the
control-plane store. Everything that is a *host fact* — sockets, namespaces,
cgroups, nftables, durable disk, private keys — is reached only through fluxor
**capability surfaces**: kernel providers addressed by contract class, which
admission can gate.

Two rules hold everywhere in this tree:

1. **Decision in a module, effect behind a surface.** A controller computes; a
   backend acts. The compiler that decides an nftables ruleset publishes it to
   the store; programming nft is a node concern. The kubelet's decision half
   projects desired sandboxes; the runner creates them through the `workload`
   contract.
2. **A module holds no host facts of its own.** No module calls libc. One that
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
graph: `api_ingress` terminates the socket and writes the store from within the
runtime, rather than a second process appending to the same file.

## 2. The API plane

`api_ingress` is the apiserver's front door: it terminates HTTP/1.1 directly off
the `net_proto` stream that `tls` (or `linux_net`, in the cleartext graph) hands
it, serves the static discovery surface itself, and drives every other request
through the pipeline. It marshals Kubernetes JSON to the store's request form
and back, and it is the store's single writer on the request path.

The pipeline stages are separate modules, reached over a request/response seam
in the store — `api_ingress` writes `/<lane>-req/<corr>` and reads back
`/<lane>-resp/<corr>`. Because the responders run in the same cooperative
scheduler, each connection is a state machine that advances one store round-trip
at a time, yielding so the responders run.

| Module | Lane | Decision |
| ------ | ---- | -------- |
| `authn` | `/authn-req/` → `/authn-resp/` | credential → Kubernetes identity: a verified mTLS peer (remapped through `/peer-ids/` when a binding exists), or a bearer JWS verified against the published keyset |
| `rbac_gate` | `/authz-req/` → `/authz-resp/` | `(identity, verb, resource)` → allow/deny, resolving RoleBindings → Roles → rules, `*` wildcards, deny by default |
| `api_admission` | `/admit-req/` → `/admit-resp/` | required fields, defaulting (it mutates the object), and per-resource quota against live counts |
| `core_api` | `/core-req/` → `/core-resp/` | get / list / create / update / delete against `/<resource>/<ns>/<name>` |
| `watch_streamer` | `/watch-req/` → `/watch-resp/` | `?watch=true`: `since=0` is a full snapshot, `since>0` drains the change stream as `PUT`/`DELETE` events plus the new fence |
| `api_responder` | `/api-req/` → `/api-resp/` | the small ops the edge answers without a full CRUD round-trip (`healthz`, `count:<prefix>`, `getjson:<key>`) |

`kube_decode` sits beside them for the Chronicle graphs: it projects a wave
`HttpRequest` envelope into a flat record frame. It decides nothing — Chronicle's
VM cannot split a variable-arity path, so the reshaping happens in a domain
module and the routing meaning stays in params.

Identity is a host fact and stays one: `tls` verifies the client certificate and
emits a per-session peer-identity envelope carrying an SVID (a hash of the peer
key). `api_ingress` maps connection to SVID and presents `peer=<svid>` as the
credential; without a verified peer it falls back to the `Authorization` bearer
token.

## 3. Controllers

A controller is a pure function of store state, re-applied on change. Each one
SUBSCRIBEs its inputs, recomputes, and writes its outputs back — guarding every
write with a read-compare, so a settled cluster spends no revisions and a
controller that writes under a prefix it watches does not wake itself forever.

| Controller | Watches → writes |
| ---------- | ---------------- |
| `deployment_reconciler` | `/deployments.apps/` → the owned ReplicaSet, plus rollout history and a scaling event |
| `replicaset_reconciler` | `/replicasets.apps/`, `/pods/` → exactly `replicas` Pods; create-if-absent, delete the surplus |
| `daemonset_reconciler` | `/daemonsets.apps/`, `/nodes/`, `/pods/` → one node-pinned Pod per ready Node, pruned when a Node goes not-ready |
| `statefulset_reconciler` | `/statefulsets.apps/`, `/pods/` → ordered identities: bring `<sts>-<i>` up only once `<sts>-<i-1>` is ready, tear down highest-first, one change per pass |
| `job_reconciler` | `/jobs.batch/`, `/pods/` → run-to-completion: launch `completions` Pods once, track Succeeded, write `/job-status/` |
| `hpa_reconciler` | `/hpa/`, `/hpa-metrics/` → the target Deployment's `replicas`, `desired = ceil(R × currentCPU / targetCPU)` clamped to `[min, max]` |
| `garbage_collector` | the workload prefixes → deletes any object whose owner no longer resolves; over successive passes the cascade walks Deployment → ReplicaSet → Pods |
| `namespace_gc` | `/namespaces/` → on `phase=Terminating`, sweep every namespaced prefix, then remove the namespace record |
| `scheduler` | `/pods/`, `/nodes/` → binds each unbound Pod to the least-loaded ready Node |
| `endpoints_reconciler` | `/services/`, `/pods/`, `/probe-status/` → the selector-matched ready backends at `/endpoints/` |
| `service_ipam` | `/services/` → the lowest free ClusterIP from 10.96.0.0/16 |
| `service_dns` | `/endpoints/` → an A-record set at `<svc>.<ns>.svc.cluster.local` |
| `webhook_validator` | `/webhooks/` → `/webhook-status/`, per-object validation |
| `snapshot_reconciler` | VolumeSnapshots → a bound VolumeSnapshotContent, marked ready |
| `device_reconciler` | `/devices.nanocloud.io/` → a `/cert-req/` per Device, then the provisioned SPIFFE identity once the mint returns |

The compilers are the same shape with a dataplane output:

| Compiler | Watches → publishes |
| -------- | ------------------- |
| `netpolicy_compiler` | `/networkpolicies/`, `/pods/` → the filter ruleset at `/dataplane/netpolicy` (per-pod allow chains aggregated across policies, then drop; a pod no policy selects gets no chain) |
| `proxy_compiler` | `/services/`, `/endpoints/` → the NAT ruleset at `/dataplane/proxy` (ClusterIP DNAT, several backends load-balanced with `numgen random mod N map`) |
| `route_compiler` | `/routes/`, `/route-status/`, `/endpoints/` → one row per route at `/dataplane/edge/`, carrying the complete backend set so a backend change is a single atomic PUT |
| `cni_ipam` | `/ipam-request/` → the pod's address at `/ipam-lease/`, lowest free host in the pool CIDR |

### Controllers as params

A controller's rules can also be carried as **Chronicle params** rather than
compiled into a bespoke module: `store_source` turns a prefix subscription into
record frames, a generic `decision` engine evaluates the rules, and
`store_effect` performs the reads and writes. The rules are authored in
`modules/app/_chronicle/*.uproc` and compiled to the hex params a graph carries.

The packaged control plane runs the params form for Deployment, ReplicaSet,
DaemonSet, StatefulSet, Job, HPA, the ownerRef cascade, namespace teardown,
scheduling and snapshots; the store, the keys and the observable behaviour are
the same either way, which is what the paired E2Es assert. Params are baked into
the graph because a `.deb` ships no compiler — `scripts/chronicle-params.sh`
compiles them at test time, and `scripts/chronicle-param-drift-e2e.sh` fails if
a baked param has drifted from its `.uproc`.

## 4. The node plane

Running a container is two different things, and they are two different modules:

- **decision** — which namespaces, which mounts, which image layers, in what
  order: a state machine over pod specs. That is `pod_lifecycle`. It watches
  `/pod-specs/` and `/sandbox-status/`, projects `/sandboxes/`, and maps sandbox
  state to a pod phase, including restart policy with exponential backoff
  (`min(1s × 2^(n-1), 300s)`, reset after a run survives 600s), two-phase kill
  with a grace deadline, and pause as a distinguishable non-terminal phase.
- **execution** — the `unshare`/`clone3`, `pivot_root` and mount assembly,
  cgroup2 limits, veth and address realization, spawning PID 1 and reaping it.
  That is the fluxor `workload` contract (`0x1A`), driven by `sandbox_runner`.

`sandbox_runner` and `kubelet` are the only two modules that hold
`requires_contract = "workload"` plus `platform_raw`, so namespace surgery is
two auditable grants and nothing else in the tree can ask for it.
`sandbox_runner` composes the CREATE header and explicit spawn params —
nanocloud owns the image→rootfs and pod-spec→argv mapping; the backend reads no
OCI format.

`kubelet` is its sibling for bare metal: on bcm2712 a "pod" is not a container
image but a composition of flash fmod modules wired into an owned subgraph with
its own IP identity. It drives the metal backend of the same `0x1A` contract,
composing the subgraph off-node and handing it inline in the CREATE source-ref.
A pod spec names a pre-registered template rather than enumerating modules.

Around them:

| Module | Role |
| ------ | ---- |
| `probe_runner` | liveness/readiness probes: exec probes through the `/sandbox-exec/` seam, verdicts at `/probe-status/`. `pod_lifecycle` folds `live=0` into restart+backoff; `endpoints_reconciler` gates membership on `ready` |
| `image_puller` | the pull *decision*: manifest layers minus the blobs already cached → `/image-pull-plan/` |
| `image_fetcher` | the pull *effect*: HTTP/1.1 plus the OCI distribution API on its own net pair, multi-arch index → platform manifest → config → layers, digest-verified into the blob cache via `fs` |
| `image_assembler` | rootfs assembly: projects an extraction job through `sandbox_runner` and maps its terminal status to `/image-rootfs/<name> state=ready\|failed` |
| `volume_manager` | mount planning: resolve each mount's claim to its bound device and filesystem, unbound claims into `pending=` |

## 5. Identity and crypto

| Module | Role |
| ------ | ---- |
| `crypto_signer` | the mechanism: generate a P-256 keypair, deposit the private scalar in the kernel key vault (contract `0x0010`), wipe the in-module copy, and sign by handle. A conformance fixture, not a service — it signs anything written to `/sign-req/`, so it too refuses to construct without `development: 1` |
| `cert_manager` | one-shot minting: a `/cert-req/` yields a fresh leaf keypair and a real X.509 certificate built by an in-module ASN.1 DER encoder, signed by handle. Development-only — it refuses to construct without `development: 1`, since it regenerates its CA at every start and returns leaf private keys on the store |
| `sa_token` | ServiceAccount tokens: an ES256 JWS with a real one-hour `iat`/`exp`, signed by a key opened by label so a restart is not a new issuer, with the verification key published for `authn` at `/authn-keys/sa` |

## 6. Capability-surface inventory

The complete host-fact boundary. Everything above it is an app module; nothing
below it is debt — it is the platform working as designed.

| Surface | Class | Provides |
| ------- | ----- | -------- |
| `storage.object` | `0x14` | keyed byte values, CAS, ranged read, delete |
| `storage.namespace` | `0x13` | prefix LIST with cursor, change SUBSCRIBE with a revision fence |
| `workload` | `0x1A` | isolated workload lifecycle: typed CREATE header (identity, posture, resource intents, network identity) + a backend-opaque options envelope; CREATE/START/READ/SIGNAL/WAIT/DESTROY plus exec and TTY |
| `fs` | `0x09` | files — the blob cache the image fetcher lands bytes in |
| key vault | `0x0010` | private keys held by the kernel, signed by handle |
| `timer` | — | deadline wakeups for the Chronicle store source |
| `linux_net` / `tls` | channel contract | leased sockets and TLS termination, delivered as a `net_proto` stream |

A module's manifest declares what it needs; a graph that asks for a surface it
was not granted fails at admission rather than at runtime. The clocks are not a
contract: a module reads uptime and wall time through `dev_millis` /
`dev_unix_millis` and declares a `timer_class` so the scheduler knows whether a
relaxed tick is safe for it.

## 7. Hot paths, as wiring

```
API write:   linux_net → tls → api_ingress → authn → rbac_gate → admission
             → core_api → store PUT (CAS) ─→ every watcher's change stream

Watch:       watch_streamer: LIST at the fence → the changes since it → a
             batch of JSON events + the new fence the client resumes from

Reconcile:   endpoints_reconciler: DRAIN(/services,/pods) → match → PUT
             /endpoints → service_dns projects the zone; proxy_compiler
             compiles the NAT ruleset → the node programs nft

Pod start:   apps/v1 PUT → deployment → replicaset → Pods → scheduler binds →
             pod_lifecycle projects /sandboxes/ → image_fetcher +
             image_assembler materialise the rootfs → sandbox_runner CREATE/
             START → status written back → watch_streamer serves it
```

## 8. Repository layout and workflow

```
fluxor.toml / fluxor.lock       project shape; registry-pinned dependencies
modules/app/<name>/             one module: mod.rs + manifest.toml
modules/app/_shared/            include-only helpers: JSON walk, store ops,
                                compact fields, events, key paths
modules/app/_chronicle/         controller rules authored for Chronicle params
target/fluxor/fluxor-abi/sdk/   the SDK the modules compile against
target/fluxor/bcm2712/modules/  the built .fmod artefacts
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

## 9. Writing a module

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
