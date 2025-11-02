# OCI Conformance Checkpoint

- **Date:** 2025-11-12
- **Scope:** Clean + cached runs of the `oci_conformance` suite (image pull cache & macro expansion cases).

Executed commands:
- `cargo clean`
- `make test-conformance`
- `make test-conformance`

Artifacts were published under `target/oci-conformance/` for both runs, showing
the cached summaries remained stable across executions.
