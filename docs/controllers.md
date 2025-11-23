# Nanocloud Controllers vs Kubernetes Controllers

This note captures how Nanocloud’s controllers now line up with core Kubernetes controller behaviours.

## Controller Runtime & Queues
- Controllers share a keyed work queue (namespace/name/kind) so duplicate events coalesce and reconciliation stays ordered.
- Keyspace watchers fan into the queue for Bundles, VolumeSnapshots, and NetworkPolicies; StatefulSets continue to use the same runtime executor.
- Reconciler loops set status/conditions directly after each run, matching the Kubernetes pattern of `reconcile(obj)` + status update.

## Events
- Controllers emit Kubernetes-style `Event` objects via the in-memory registry:
  - Bundles: success/failure of reconciliation, including install/start/update failures.
  - VolumeSnapshots: Ready/Failed transitions.
  - NetworkPolicies: reconcile results for policy or pod triggers.
- Events carry `involvedObject`, `reason`, `type` (`Normal`/`Warning`), and timestamps so they are watchable/listable through `/api/v1/events`.

## Resource Lifecycles & Conditions
- **Pod status:** Phases now stick to `Pending | Running | Succeeded | Failed | Unknown`; conditions reported are `PodScheduled`, `Initialized`, `ContainersReady`, and `Ready`.
- **Bundle lifecycle:** Phases map to `Installing | Running | Updating | Failed | Uninstalling`.
  - Conditions: `InstallReady`, `BindingsReady`, `BackupHealthy` with reasons for Pending/Ready/Failed/Uninstalling.
- **VolumeSnapshot:** Reconcile sets phase and emits events; owner references are attached back to the owning Bundle when available.

## Finalizers & Garbage Collection
- Bundles carry the `nanocloud.io/bundle-cleanup` finalizer.
  - API deletes set `deletionTimestamp` and keep the Bundle until cleanup finishes.
  - Controller drives `container::uninstall` (prunes backups, detaches volumes) and then removes the finalizer via `finalize_delete`.
  - Workload Pods already carry `ownerReferences` to the Bundle; snapshots inherit ownership to enable GC.

## Scheduling Semantics (Single-Node)
- `spec.nodeName` and `spec.nodeSelector` are validated against the single available node (hostname or `NANOCLOUD_NODE_NAME`).
- Unsupported selectors or mismatched node names fail reconciliation with clear condition reasons and Events, mirroring how Kubernetes surfaces unschedulable pods.

## Behavioral Parity Notes
- Discovery and status fields use the same shapes (`metadata.uid/resourceVersion/annotations/labels`, condition timestamps).
- Watch/event ordering follows Kubernetes watch semantics via the shared runtime queue and the EventRegistry.
