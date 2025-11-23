## Kubernetes-Aligned API Surface

- Core resources exposed at Kubernetes-style paths: `GET/POST /api/v1/namespaces/{namespace}/pods`, `/configmaps`, `/secrets`, `/events`, plus cluster-wide list endpoints where applicable.
- Nanocloud CRDs retained under `nanocloud.io/v1` (Bundles, Devices, VolumeSnapshots, ephemeral certificates); discovery surfaces groups at `/api`, `/apis`, `/api/v1`, `/apis/nanocloud.io/v1`, and `/apis/apps/v1`.
- Objects model Kubernetes shapes (metadata with uid/resourceVersion/labels/annotations/ownerReferences, status.conditions with `type/status/reason/message/lastTransitionTime`), and Pods follow core/v1 field casing (e.g., `initContainers`, `nodeSelector`, `volumeMounts`).

## Watch Semantics

- `watch=true` supported across list/get where applicable with `resourceVersion`, `timeoutSeconds`, `allowWatchBookmarks`, and standard ADDED/MODIFIED/DELETED ordering.
- `resourceVersionMatch` and pagination (`limit`, `continue`) honored on list endpoints; watches reject incompatible combinations (e.g., `limit` + `watch=true`).

## Error Model

- Errors serialized as Kubernetes `Status` objects: `{kind:"Status", apiVersion:"v1", status:"Failure", message, reason, code}` with standard reasons (`NotFound`, `AlreadyExists`, `Conflict`, `Forbidden`, etc.).
- Apply conflicts returned via `conflicts` details; HTTP status codes align with Kubernetes expectations (404 NotFound, 409 AlreadyExists/Conflict, 400 BadRequest, 401 Unauthorized, 403 Forbidden, 410 Gone).

## Ownership and GC Semantics

- Bundle-generated Pods carry `ownerReferences` back to the owning `Bundle` (`nanocloud.io/v1`) enabling Kubernetes-style garbage-collection behavior expectations.
