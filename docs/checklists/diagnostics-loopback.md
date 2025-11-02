# Diagnostics Loopback Probe Checkpoint

- **Date:** 2025-11-12
- **Scope:** CLI summary/hint mapping for the loopback probe using the fake Dockyard image reference (`dockyard.nanocloud.io/diagnostics/loopback:latest`).

Validated command:
- `cargo test outcome_`

The aggregated outcome tests exercise all combinations of DNS/volume pass-fail
states and ensure the CLI surfaces the correct exit code plus remediation hints.
This checkpoint can be rerun in CI to confirm regressions are caught without
launching real workloads.
