# OCI Runtime Conformance Matrix

The Nanocloud runtime exercises a focused subset of OCI behaviors to keep the
single-binary deployment lightweight while still guaranteeing deterministic
container execution. The conformance harness documents the cases we must cover
in CI before accepting runtime changes.

## Harness Overview

- **HarnessContext** – bundles shared fixtures (fake Dockyard registry path,
  scratch Keyspace directory, temporary filesystem roots) so each case can run
  in isolation.
- **ConformanceCase** – describes one behavior with `setup`, `action`, and
  `assertions` closures. Cases run sequentially to avoid port or file clashes.
- **Fixtures**
  - `tests/support/oci_harness.rs` provides helpers to load fake images,
    program restart policies, and stub macros.
  - Fake Dockyard image (`tests/data/oci/fake-loopback:latest`) contains
    deterministic layers plus embedded scripts for DNS/macros/exec exercises.
  - Test keyspace snapshots seed bundle/spec data so controllers do not need to
    talk to external stores during CI.

## Environment Requirements

- Runs entirely in user space (no privileged daemons) so GitHub Actions runners
  can execute the suite.
- Requires the Nanocloud runtime features already used by integration tests:
  image store on the local filesystem, tokio runtime, and access to the same
  macro expansion code paths as production workloads.
- Uses the in-process event bus to capture restart policy / signal emissions.

## Case Matrix

| Case | Setup | Expected Behavior |
| ---- | ----- | ----------------- |
| `image_pull_cache` | Pull the fake Dockyard image twice with caching enabled. | First pull downloads all layers and increments `image_pulls_total`; second pull hits the cache, reports zero downloads, and bumps `image_cache_hits_total`. |
| `entrypoint_macros` | Launch container with `command: ["print-env"]` and inject options that trigger `!dns`, `!rand`, `!tls`, and `!if`. | Expanded environment variables include resolved DNS/IP, deterministic random token (seeded), TLS cert fingerprint, and branch from the conditional macro. |
| `signals_restart_policy` | Run a container whose entrypoint exits with non-zero status repeatedly while restart policy is `OnFailure` with `maxRestarts=2`. | Runtime sends SIGTERM, escalates to SIGKILL on timeout, enforces the restart limit, and emits controller events reflecting each restart + final failure. |
| `log_and_exec_flows` | Start long-running container writing to stdout/stderr and expose an exec endpoint. | Harness can tail logs via the existing streaming API and invoke `exec` to run `/bin/true` inside the container; both operations must succeed without interfering with the workload. |

Each case records artifacts (logs, event stream transcript) under
`target/oci-conformance/<case>/` so CI can upload them on failure.

## Reporting

- The harness prints a summary table with per-case status plus pointers to the
  artifact directory.
- Failures produce structured JSON containing the case name, phase, and error
  message, allowing follow-up tooling to surface regressions inline with PRs.
- A convenience target (`make test-conformance`) wraps `cargo test
  --package nanocloud --test oci_conformance` to keep CI wiring simple.

## Running the Suite

```
make test-conformance
```

The target:
- Clears and recreates `target/oci-conformance/`.
- Sets `OCI_CONFORMANCE_DIR` so tests can emit summaries.
- Forces single-threaded execution for deterministic artifact names.
- Runs the dedicated `oci_conformance` integration test binary with `--nocapture`.

Each case writes `summary.txt` beneath `target/oci-conformance/<case>/`, making
it easy for CI to persist and display outcomes alongside build logs.

## Extending Coverage

1. Add a new test to `tests/nanocloud/oci/conformance.rs` and instantiate
   `HarnessArtifacts::new("<case>")` to collect notes or links to auxiliary
   logs.
2. Update the matrix above with the new case so reviewers know what behavior is
   validated and why it matters.
3. Introduce additional fake registry fixtures under
   `tests/data/fake_registry/` when a scenario needs unique manifests/layers.
4. Keep the suite deterministic: prefer tempdirs rooted in
   `NANOCLOUD_IMAGE_ROOT`, generate secure assets via `SecureAssets::generate`,
   and avoid global state wherever possible.
5. Run `make test-conformance` and attach the resulting summary files to the PR
   description so reviewers can see what ran and why it passed or failed.
