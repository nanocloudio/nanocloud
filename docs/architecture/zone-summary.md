# ZoneSummary: the cell's nodes and capacity, per zone

`ZoneSummary` (`nanocloud.io/v1`, plural `zonesummaries`, cluster-scoped) is
the cell's own read-only summary of one zone: how many of its Nodes there are,
how many are ready and schedulable, the CPU and memory they offer and the
requests bound to them, and where the store's voters stand. It is named by the
zone (the value of a Node's `topology.kubernetes.io/zone` label). The cell
derives it from `/nodes/` and `/pods/`; no client writes it. Any API client
with a grant reads, lists and watches it — a scheduler extension, a dashboard,
an operator deciding whether a zone can be drained, or an external controller
placing traffic by zone. Draining a zone itself is the standard Node API
(`kubectl cordon`, `kubectl drain`): nothing beyond it is needed.

| Resource | Group/version | Scope | Verbs | API path | Store key |
| --- | --- | --- | --- | --- | --- |
| `zonesummaries` (ZoneSummary) | `nanocloud.io/v1` | cluster | get, list, watch | `/apis/nanocloud.io/v1/zonesummaries[/<zone>]` | `/zonesummaries.nanocloud.io/<zone>` |

Each summary is one JSON document at its key, as for every grouped resource
(`/graphs.nanocloud.io/<name>`, `/deployments.apps/<ns>/<name>`), its figures
under `status`.

## Wiring

- Discovery lives in `apiplane_admit.plan`: `/apis/nanocloud.io/v1` lists
  `zonesummaries` (get, list, watch).
- `kube_facts.rd` gives the typed List kind (`ZoneSummaryList`).
- `doc_json_dialect` `id_paths` names `zonesummaries.nanocloud.io` as
  cluster-scoped.
- `ws_evt` `table` sets the columns `kubectl get zonesummaries` prints: Name,
  Ready (`ready/total`).
- `nanocloud get zonesummaries` lists them from the store.
- The derivation runs on the control plane (`zn_`/`zl_`/`zc_`/`zs_` in
  `fluxor-controlplane.yaml`, below).

## RBAC

A grant names resource `zonesummaries` in group `nanocloud.io` (verbs get,
list, watch), as upstream. The base ships no default `view`/`edit`/`admin`
ClusterRoles to aggregate into, so until a grant exists only a `*` grant
(cluster-admin) reaches it. A `view` role written as an aggregated ClusterRole
(`rbac_agg.plan` serves `aggregationRule`) would carry get, list and watch on
it.

Every write — create, update, patch, delete, and the same through `/status` —
is refused **405 MethodNotAllowed** for every caller, cluster-admin included,
after RBAC allowed the verb (`apiplane.plan` `probe`), as upstream refuses a
verb a resource does not serve. The cell's own plan writes the store directly.

## ZoneSummary

```json
{"apiVersion":"nanocloud.io/v1","kind":"ZoneSummary","metadata":{"name":"<zone>"},
 "status":{"readinessGeneration":<u64>,
           "nodes":{"total":n,"ready":n,"schedulable":n},
           "cpuMilli":{"allocatableSchedulable":n,"allocatableReady":n,"requested":n},
           "memoryBytes":{"allocatableSchedulable":n,"allocatableReady":n,"requested":n},
           "voters":{"inside":n,"outside":n,"outsideReady":n},
           "observedAtMs":<unix ms>}}
```

The summary is derived from `/nodes/` and `/pods/` by two plans:

**`zone_node.plan`** keeps one row per Node:
`/zone-node/<node>` = `zone=;ready=;sched=;voter=;ca=;ma=;cr=;mr=`.

| Row field | Meaning |
| --- | --- |
| zone | the `topology.kubernetes.io/zone` label, `""` when absent |
| ready | 1 when the Node's `Ready` condition is `True` |
| sched | 1 when the Node is ready and not `spec.unschedulable` |
| voter | 1 when labelled `nanocloud.io/store-voter: "true"` |
| ca, ma | `status.allocatable` cpu (milli) and memory (bytes); `status.capacity` when the Node states no allocatable |
| cr, mr | the summed requests of the Pods bound to the Node (`spec.nodeName`) that are not `Succeeded`/`Failed` (`!reqcpu`, `!reqmem`: a container's limit stands in for an unstated request; an init container counts when it is larger) |

A row whose Node is gone is deleted, with its readiness record. The plan
also keeps `/zone-ready/<node>` = `<zone>|<0|1>`, written put-if-changed, so
the store itself says (200 rather than 204) when a Node joined a zone or its
readiness flipped. On that answer the plan merges `/zone-list/<zone>`
`rg=<rev>`, where `rev` is the store revision of that Node write. Revisions
only grow, so this is the revision at which the zone's readiness was last
written: the **readinessGeneration**. A Node's heartbeat does not move it.

**`zone_summary.plan`** runs one pass per zone, driven by `/zone-list/` and by
any change to a row. It makes six COUNT sums over the rows:

- **nodes and voters**: packed ten bits apiece, so a zone holds at most 1023
  Nodes.
- **cpu allocatable**: on ready Nodes and on schedulable Nodes, packed 31 bits
  apiece (at most about 2.1 million cores).
- **cpu requested**.
- **memory allocatable**: on schedulable Nodes, then on ready Nodes.
- **memory requested**.

For voters:

- `inside` counts the zone's voters.
- `outside` counts the voters in every other zone and the voters with no zone.
- `outsideReady` counts the ready ones among the outside voters.

The figures are MERGEd into `/zone-list/<zone>` `f=`. The summary is PUT only
when they move, so `observedAtMs` (the wall clock at that write) changes only
when a figure does. When a zone's last Node goes, its summary and its marker
are deleted. A Node with no zone label is never summarised. It still counts
as a voter outside every zone.

The derivation is level-triggered:

- Any change to a Node or a Pod re-reads each Node's Pods: two counts per Node.
- Any change to a row re-sums each zone.

This suits a cell's scale: tens of Nodes and hundreds of Pods.

**Operator note:** nothing in the base sets `nanocloud.io/store-voter`. Label
the Nodes that hold store voters.

## Proof

`scripts/zone-summary-e2e.sh` runs the shipped control plane and proves:

- discovery: `nanocloud.io/v1` lists `zonesummaries` (get, list, watch);
- the figures from seeded Nodes and Pods: two zones, a Node with no zone, a
  NotReady Node, a cordoned one, a terminal Pod, voters inside and outside;
- the summary kept current and watched: a readiness flip moves
  `readinessGeneration`, an uncordon, a Pod deleted, a zone's last Node
  deleted;
- 405 on every write;
- 403 without a grant, and a `zonesummaries` grant that reads them and
  nothing else.

`scripts/chronicle-param-drift-e2e.sh` keeps the baked params equal to the
plans.
