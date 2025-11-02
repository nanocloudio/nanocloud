# Bundle Schema (draft v1alpha1)

The Bundle resource combines Kubernetes-style metadata with Nanocloud-specific
specification fields. This document records the fields that must appear in the
JSON Schema so admission and offline tooling remain consistent.

| JSON Pointer | Type | Description |
| ------------ | ---- | ----------- |
| `/apiVersion` | string | Must equal `nanocloud.io/v1`. |
| `/kind` | string | Must equal `Bundle`. |
| `/metadata/name` | string | Stable bundle identifier; defaults to `spec.service` when omitted. |
| `/metadata/namespace` | string | Logical namespace; defaults to `default`. |
| `/spec/service` | string | Target OCI manifest name (e.g. `postgres`). |
| `/spec/namespace` | string? | Explicit namespace override; defaults to metadata namespace. |
| `/spec/options` | object | Free-form key/value pairs surfaced to Dockyard option processors; keys must be RFC1123-compatible. |
| `/spec/key` | string? | Base64-wrapped profile encryption key. |
| `/spec/snapshot/source` | string | URI referencing profile/volume snapshot. |
| `/spec/snapshot/mediaType` | string? | Media type for snapshot payload (`application/x-tar` default). |
| `/spec/start` | bool | When false, reconciler prepares resources but leaves workloads stopped. Defaults to `true`. |
| `/spec/update` | bool | Force refresh of cached image layers. Defaults to `false`. |

Although bindings are derived from Dockyard labels rather than explicit spec
fields, the `options` map enforces the decorator semantics described in
`README.md` (macros, `*requires_option`, `&binding`). The forthcoming schema
captures those constraints via patterns and enum fragments so invalid option
shapes are rejected before persistence.

See `v1alpha1.json` for the machine-readable schema definition.
