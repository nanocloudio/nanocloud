# Networking, Storage, and RBAC

## Services and Endpoints
- Core/v1 Services now support create/list/get/delete plus `watch=true` with resourceVersion/continue semantics.
- ClusterIP addresses are allocated automatically and programmed via iptables; endpoints are reconciled from pod selectors and updated on pod/service changes.
- Endpoints are persisted and watchable, exposing ready pod IPs and service ports.

## PersistentVolumeClaims
- PVCs are exposed as read-only views of CSI volumes provisioned for each service. They surface storage size, access modes, and owning service annotations.
- List/get mirrors Kubernetes list pagination fields; watches are not yet supported.

## VolumeSnapshots
- VolumeSnapshot resources are exposed under `nanocloud.io/v1` with create/list/get/delete. New snapshots start in `Pending` and are reconciled by the snapshot controller.
- Watches are not yet supported; discovery verbs reflect the current API surface.

## RBAC
- Static `Role` and `RoleBinding` resources are published in `nanocloud.io/v1` (admin, viewer, device, service-account) with predefined policy rules and bindings.
- Endpoints are read-only (list/get) and intended for clients that need to introspect available roles; enforcement continues to rely on existing authentication scopes.
