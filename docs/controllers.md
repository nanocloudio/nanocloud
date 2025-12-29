# Nanocloud Controllers vs Kubernetes Controllers

This note captures how Nanocloud’s controllers now line up with core Kubernetes controller behaviours.

## Controller Runtime & Queues
- Controllers share a keyed work queue (namespace/name/kind) so duplicate events coalesce and reconciliation stays ordered.
- Keyspace watchers fan into the queue for Bundles, VolumeSnapshots, and NetworkPolicies; StatefulSets continue to use the same runtime executor.
- Reconciler loops set status/conditions directly after each run, matching the Kubernetes pattern of `reconcile(obj)` + status update.

## Bundle Controller
- Watches `/apis/nanocloud.io/v1/bundles` for ADD/MODIFIED/DELETE, persists desired state, and manages the `nanocloud.io/bundle-cleanup` finalizer before the API server removes the object.
- Coordinates Dockyard option resolution, binding execution, profile persistence, and secret distribution and only flips the `Ready` condition once all prerequisites succeed.
- Emits Kubernetes Events at each phase (`ProfilePending`, `BindingsCompleted`, `WorkloadFailed`, etc.) and writes detailed `status.conditions` for CLI/`kubectl` consumers.
- Drives lifecycle subresources (`/actions/start|stop|restart|uninstall`) so imperative requests reuse the same reconciliation flow that spec updates trigger.

## StatefulSet & Replica Controllers
- `src/nanocloud/controller/statefulset.rs` mirrors Kubernetes’ StatefulSet algorithm: the reconciler computes deterministic revision hashes, keeps a bounded revision history, reuses ReplicaSets during rollbacks, and limits the number of in-flight pods according to `spec.updateStrategy.rollingUpdate`.
- ReplicaSet reconcilers persist `ReplicaSetDesiredState` entries, annotate pods with template hashes/owner refs, and hand `ReplicaSetPodDesiredState` plans to the kubelet via the shared controller-runtime channel.
- The ReplicaSet scheduler enforces bounded concurrency per StatefulSet and deduplicates work items so rapid updates do not thrash the kubelet.
- Both controllers track observed pods and update `status.{readyReplicas,currentReplicas}` to keep `/apis/apps/v1/statefulsets|replicasets` responses in sync with the actual runtime state.

## ReplicaSet-to-Kubelet Bridge
- The kubelet registers for ReplicaSet plan broadcasts and acknowledges each plan, mirroring how kubelet mirrors objects stored in etcd.
- Pod registrations track UID, namespace, restart counts, and desired running state; when a controller prunes a ReplicaSet the kubelet tears down the matching containers and updates pod status.
- Event payloads include the same `involvedObject` references as upstream so operators can correlate controller actions with kubelet status transitions.

## NetworkPolicy & Snapshot Controllers
- NetworkPolicy reconciler translates policy specs into nftables chains and ensures watch subscribers receive the resulting rules (helpful for `nanocloud network-policy debug` flows).
- Snapshot controller watches `VolumeSnapshot` CRs, orchestrates CSI `NodePublishVolume` calls, and emits Events for `SnapshotReady`/`SnapshotFailed`, mirroring the Kubernetes CSI snapshotter state machine.

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
