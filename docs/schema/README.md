# Nanocloud Schema Catalog

This directory contains versioned JSON Schemas for Nanocloud custom
resources. Schemas are grouped by resource type and versioned independently
so controllers, CLIs, and offline tooling can validate payloads without
inspecting server-side Rust types.

- `bundle/` &mdash; validation artifacts for `Bundle` resources.
  - `v1alpha1.json` describes the current `nanocloud.io/v1` Bundle surface.
  - Future versions will live alongside (`v1beta1.json`, etc.) and remain
    immutable once released.

Each schema follows the draft 2020‑12 spec and keeps a flat structure that
mirrors the serialized API fields. Field comments live directly inside the
schema to avoid drift from `src/nanocloud/api/types.rs`.
