# Nanocloud Specification

This document captures the user-visible contracts that the single `nanocloud`
binary enforces. It complements `docs/schema/**` (machine-readable artifacts)
with prose that explains how operators consume the API and CLI surfaces.

## Bundle Schema (v1alpha1)

### Version Manifest
- Schema files live under `docs/schema/bundle/` and are embedded in the binary
  at build time. The manifest (`manifest.json`) lists every version, checksum,
  and release note so automation can verify downloads before use.
- The current entry is:

| Version | API version | Status  | SHA256                                   | Description                                           |
| ------- | ----------- | ------- | ---------------------------------------- | ----------------------------------------------------- |
| v1alpha1 | nanocloud.io/v1 | current | `b5d9aacb0ee44c2e5952990ec2cfd61a5ba831b85615f158da118308ecef91bf` | Initial schema covering metadata, spec, snapshot, and status blocks. |

The checksum above is the value shipped in `manifest.json` and can be verified
with `sha256sum docs/schema/bundle/v1alpha1.json`. Future versions will append
new rows without mutating existing entries.

### Default Semantics

The admission controller applies deterministic defaults after schema validation.

| Field | Default rule |
| ----- | ------------ |
| `metadata.namespace` | Falls back to the namespace segment in the request URL; empty values resolve to `default`. |
| `metadata.name` | Defaults to `spec.service` when `metadata.name` is omitted. |
| `spec.namespace` | Mirrors `metadata.namespace` unless explicitly overridden; blank values are cleared so reconciler input stays canonical. |
| `spec.start` | Defaults to `true`; when set to `false` reconciliation prepares workloads but leaves them stopped. |
| `spec.update` | Defaults to `false`; setting it to `true` forces the runtime to re-pull images even if cached layers exist. |
| `spec.snapshot.mediaType` | Defaults to `application/x-tar` when a snapshot source is provided without an explicit media type. |
| `spec.options` | Keys are treated as case-sensitive strings; validation rejects characters outside the RFC1123 label set, so operators should stick to `[a-z0-9-]`. |

These defaults mirror the serde annotations in `src/nanocloud/api/types.rs` and
are persisted so read/modify/write flows do not churn `resourceVersion`.

### CLI Usage

- `nanocloud install <service> [--namespace <ns>] [--option key=value]` wraps
  the Bundle schema: the command builds a valid manifest, issues
  `POST /apis/nanocloud.io/v1/namespaces/<ns>/bundles`, and prints schema
  violations returned by the API (the human-readable messages originate from
  `format_bundle_error_summary`).
- `nanocloud bundle export --profile <service> [--namespace <ns>] (--output PATH | --stdout) [--include-secrets]`
  invokes `POST /apis/nanocloud.io/v1/namespaces/<ns>/bundles/<service>/exportProfile`
  and writes a tarball containing `manifest.json` plus `profile.json`. The CLI
  refuses to run without either `--output` or `--stdout`, prevents both flags
  from being set simultaneously, and forwards `--include-secrets` when operators
  are authorized to capture encrypted secret payloads. `--curl` prints the exact
  POST request so automation can capture artifacts without invoking the CLI.
- `nanocloud install --curl …` emits the exact HTTP request so operators can
  capture Bundle specs for review or pipeline usage before submitting them.
- Validation happens on every create/update. When a spec fails the JSON Schema
  checks, the server responds with `400 Bad Request` and field-targeted errors
  (e.g., `spec.options.database` → `unknown field`) that the CLI surfaces
  verbatim.

Shipments that need offline validation can consume the bundled schema directly
(`docs/schema/bundle/v1alpha1.json`) and run it through any JSON Schema tooling
draft 2020-12 compliant; the manifest described above ensures the bits match the
build embedded in the binary.

### Server-side Apply

- `nanocloud bundle apply <service> -f bundle.yaml --field-manager cli.nanocloud/v1`
  issues a server-side apply (`PATCH /bundles/<service>`) that records the provided
  field manager alongside every JSON pointer touched in `spec.*`.
- Conflicts return `409` and the CLI prints the paths plus their current owner.
  Re-run with `--force` only when the operator intentionally takes ownership of
  those fields.
- Use `--dry-run` to validate a manifest fragment without persisting it; the CLI
  forwards the request with `dryRun=true` so admission, schema validation, and SSA
  conflicts are still reported.
- `--curl` prints the exact PATCH request, including the `Content-Type`
  (`application/apply-patch+json` or `...+yaml` inferred from the file extension),
  so automation can replay the same SSA flow outside of the CLI.

## Bundle Status Conditions

Nanocloud mirrors Kubernetes-style readiness reporting with a small, fixed set
of condition types. Every condition carries a `status` (True/False/Unknown),
`reason`, `message`, and `lastTransitionTime`.

| Condition | True when | False when | Notes |
| --------- | --------- | ---------- | ----- |
| `Ready` | Workload objects are applied and either running or intentionally kept stopped (`start=false`). | Workload application failed, start failed, or prerequisites are still pending. | Aggregates the `Bound` condition. |
| `Bound` | Controller rendered workloads, reconciled persistent resources, and persisted the bundle profile. | Workload rendering or persistence failed. | Depends on `ProfilePrepared` and `SecretsProvisioned`. |
| `SecretsProvisioned` | Profile encryption keys, bindings, and secret material are available to workloads. | Secrets/bindings are pending or failed. | Depends on `ProfilePrepared`. |
| `ProfilePrepared` | Dockyard options, snapshots, and profile metadata resolved successfully. | Input validation failed, snapshot download failed, or Dockyard metadata could not be fetched. | Foundation for all other conditions. |

### Reason Vocabulary

Each condition uses a small vocabulary so automation can react predictably:

| Reason | Meaning |
| ------ | ------- |
| `ProfilePending`, `ProfilePersisted`, `ProfileFailed` | Option resolution/snapshot staging started, succeeded, or failed. |
| `SecretsPending`, `SecretsDistributed`, `SecretsFailed` | Secrets or encryption material are being generated, finished, or failed. |
| `BindingsPending`, `BindingsCompleted`, `BindingsFailed` | Binding execution (if required) is queued, succeeded, or failed. |
| `WorkloadPending`, `WorkloadApplied`, `WorkloadFailed` | Workload manifests are pending application, applied, or failed. |
| `StartPending`, `StartCompleted`, `StartSkipped`, `StartFailed` | Workload start requests are in-flight, done, intentionally skipped (`start=false`), or failed. |

Controllers should only advance a condition to `True` when all dependencies are
`True` (e.g., `Ready=True` implies `Bound=True`, which implies both
`ProfilePrepared=True` and `SecretsProvisioned=True`). CLI table output mirrors
this ordering so operators can immediately spot the phase that needs attention.

## Binding Execution Envelope

Bindings declared via Dockyard labels (`&bindings.<id>`) run inside a deterministic
envelope after the profile is persisted and before `BindingsCompleted=True`. The
envelope isolates filesystem access, drops privileges, and records a durable
idempotency marker so controllers neither re-run commands unnecessarily nor leak
privileges into workload namespaces.

| Field | Default | Notes |
| ----- | ------- | ----- |
| Namespace set | Target workload's `mnt`, `uts`, `ipc`, `pid`, and (when enabled) `net` namespaces. | Reuses the kubelet runtime container namespaces so bindings see the same filesystem, DNS, and processes as the dependent workload. |
| UID/GID | `65534:65534` (`nobody`). | Overridable via `NANOCLOUD_BINDING_UID` / `NANOCLOUD_BINDING_GID`. The controller impersonates the configured identity immediately before spawning the binding command. |
| Seccomp profile | `/etc/nanocloud/policies/bindings-default.json` embedded hash. | Operators can point `NANOCLOUD_BINDING_SECCOMP` to a custom JSON profile; hashes are logged so CI can fail when the on-disk profile diverges. |
| AppArmor profile | `nanocloud-bindings`. | If the kernel supports AppArmor the controller loads the named profile; set `NANOCLOUD_BINDING_APPARMOR=disable` to opt out (still recorded in events). |
| Timeout | 90 seconds per binding step. | Tunable via `NANOCLOUD_BINDING_TIMEOUT=5m` (or CLI flag `nanocloud server --binding-timeout 5m` once available). Timeouts kill the process group and emit `BindingTimeout` events. |
| Idempotency key | `<bundle>/<binding_id>` | Stored in the bundle profile once a command exits successfully; subsequent reconciles skip the binding unless `nanocloud install --force-bindings` or a profile reset is requested. |

### Namespace & IO Guarantees
- Namespace entry happens before privileges are dropped, ensuring the binding sees
  the same view of `/var/run`, `/var/lib`, and `/etc` as the workload. Host networking
  is respected: when `hostNetwork=true` the net namespace join is skipped.
- `stdin` is closed, `stdout/stderr` are captured and attached to the binding event
  stream, and the environment is sanitized to a small allowlist
  (`PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin`, `LANG=C.UTF-8`,
  `NANOCLOUD_BINDING_ID`, `NANOCLOUD_SERVICE`, `NANOCLOUD_NAMESPACE`).
- Signals are forwarded to the binding process group so operators can cancel a
  reconcile via `nanocloud controller abort --bundle <name>` without leaving
  orphaned processes pinned to a namespace.

### Idempotency & Retries
- Every binding command must expose a stable identifier via the Dockyard label
  suffix (e.g., `&bindings.create_user`). The controller hashes `{bundle, profile, binding_id}`
  with an incrementing attempt counter and stores the resulting token plus exit
  metadata in the bundle profile.
- Successful tokens cause future reconciles to short-circuit the binding. Operators
  can request a re-run by deleting the profile entry or issuing
  `nanocloud install <service> --force-bindings`, which clears tokens before the
  next reconcile cycle.

### Events & Observability
- The controller emits `BindingStarted`, `BindingCompleted`, `BindingFailed`, and
  `BindingTimeout` events with fields `{binding_id, service, attempt, duration_ms}`.
  Event IDs are mirrored into the bundle profile so CLI workflows can correlate
  logs with stored history.
- Metrics (`nanocloud_binding_executions_total{service,result}`) increment before
  and after each envelope execution. A per-binding duration histogram surfaces in
  `/metrics` for operators that scrape the control plane.

#### Event stream API contract

Nanocloud reuses the Kubernetes-style Events API so existing tooling can follow
the control-plane timeline:

- Endpoints: `GET /api/v1/events` (cluster-wide) and
  `GET /api/v1/namespaces/{namespace}/events` (scoped).
- Query parameters:
  - `watch=true` – upgrade to a streaming response (`application/json`) where
    each line is a single `EventWatchEvent`.
  - `since=<duration|RFC3339>` – trim the initial event list to entries newer
    than the supplied timestamp or duration (e.g., `since=5m`). Internally this
    maps to `resourceVersionMatch=NotOlderThan` with a server-side time gate.
  - `level=Normal|Warning` – filter by event `type` (warnings typically map to
    controller failures or policy violations).
  - `reason=ReasonA,ReasonB` – comma-separated list of `reason` strings; unknown
    values are ignored and simply return an empty stream.
  - Scope filters – clients reuse the existing `fieldSelector` support
    (`metadata.namespace=<ns>`, `involvedObject.name=<bundle>`) to drill down to
    a single workload.
  - Standard watch knobs `resourceVersion`, `resourceVersionMatch`,
    `allowWatchBookmarks`, `timeoutSeconds`, and pagination parameters apply in
    non-watch mode.
- Payload schema:
  ```json
  {
    "type": "Warning",
    "reason": "SecurityPolicyViolation",
    "message": "Capability 'CAP_SYS_ADMIN' is not allowed",
    "eventTime": "2025-01-13T18:05:31Z",
    "reportingComponent": "bundle-controller",
    "metadata": {
      "name": "nanocloud.1a2b3c",
      "namespace": "default",
      "resourceVersion": "341"
    },
    "involvedObject": {
      "kind": "Bundle",
      "namespace": "default",
      "name": "postgres"
    }
  }
  ```
- Clients resume streams by capturing `metadata.resourceVersion` from the last
  received object and reissuing the request with
  `resourceVersionMatch=NotOlderThan`.

Refer to `docs/guides/bindings.md` for authoring guidance and detailed examples.

## Exec API

- The API exposes the `/api/v1/namespaces/{namespace}/pods/{name}/exec` and
  `/api/v1/pods/{name}/exec` routes. Both endpoints require HTTP/1.1 upgrades
  to WebSocket via `Upgrade: websocket`, `Connection: Upgrade`, `Sec-WebSocket-Version: 13`,
  and `Sec-WebSocket-Protocol`.
- The server negotiates `v5.channel.k8s.io`, then `v4.channel.k8s.io`, and finally
  `channel.k8s.io`; it mirrors the selected subprotocol in the `X-Stream-Protocol-Version`
  response header. Clients can toggle `stdin`, `stdout`, `stderr`, `tty`, `container`,
  and compute multiple `command` entries with repeated query parameters.
- The binary emits structured metadata for exec sessions: `trace_id`, `span_id`,
  session/command labels, and exit status frames on channel `3`. Resize events
  (JSON payloads on channel `4`) only apply when `tty=true`, and the server rejects
  HTTP/2 requests (status `426 Upgrade Required`).
- Authorization mirrors Kubernetes semantics: TLS client certs (Nanocloud/device),
  bootstrap tokens, or bearer tokens must present at least the `pods.exec` scope.

## Device Management API

- Device resources live under `apis/nanocloud.io/v1/namespaces/{namespace}/devices`.
- Supported verbs:
  - `GET` (collection) returns `DeviceList`.
  - `GET /{name}` returns a single `Device`, enforcing that device certificates can
    only inspect their own record.
  - `POST` creates a `Device` with a `DeviceSpec` (`hash`, `certificateSubject`, optional `description`).
  - `DELETE /{name}` removes the device entry.
  - `POST /devices/certificates` accepts a `CertificateRequest` (CSR PEM) and returns
    a `CertificateResponse` with `CertificateStatus` (signed cert, CA bundle, expiration).
- These APIs power the `nanocloud device` CLI subcommands (`list`, `get`, `create`, `delete`, `issue-certificate`),
  which wrap the shared API client so they honor TLS, bearer tokens, and owner headers.

## CSI Storage Layout

- The default root for the CSI provider is `/var/lib/nanocloud.io/storage/csi`.
- Subdirectories:
  - `volumes/` – stores created volume roots named after the volume ID.
  - `publish/` – tracks published mount points for cleanup.
  - `snapshots/` – stores snapshot archives as `<snapshot_id>.tar`.
- Deployments may override the root with `NANOCLOUD_CSI_ROOT`, which the runtime
  and tests honor so operator-specific storage locations can coexist with defaults.

## Runtime Security Profile

Nanocloud hardens workloads by default:

- Containers launch without any ambient/permitted capabilities even when running as UID 0.
- A built-in seccomp profile blocks namespace manipulation, kernel module loading, ptrace, and other privileged syscalls.
- Admission rejects attempts to request sensitive capabilities unless the bundle explicitly opts in.

Bundles can override these defaults through the `spec.security` block:

| Field | Type | Default | Notes |
| ----- | ---- | ------- | ----- |
| `allowPrivileged` | boolean | `false` | Required to request capabilities outside the `CAP_NET_*` allowlist (e.g., `CAP_SYS_ADMIN`). |
| `extraCapabilities[]` | array\<string\> | `[]` | List of capability names (`CAP_NET_ADMIN`, `CAP_NET_RAW`, etc.). Values are normalized to uppercase and deduplicated. |
| `seccompProfile.type` | enum | `Baseline` | `Baseline` uses the embedded profile, `RuntimeDefault` defers to the host runtime, `Localhost` loads `/etc/nanocloud/policies/<localhostProfile>.json`. |
| `seccompProfile.localhostProfile` | string | _required for `Localhost`_ | File name (without extension) of the policy to load when `type=Localhost`. |

### Enforcement and Events

During container creation Nanocloud reads the `PodSpec.security` block (persisted alongside pod manifests) and:

1. Drops all Linux capabilities not requested.
2. Applies the baseline seccomp profile unless a custom profile is specified.
3. Emits controller events when enforcement blocks the operation:
   - `SecurityPolicyViolation` – unknown/unsupported capabilities or missing `allowPrivileged=true`.
   - `PrivilegeEscalationDenied` – the host refused to apply the requested capability set (e.g., insufficient privileges or kernel policy).

These events include actionable hints (e.g., “Capability 'CAP_SYS_ADMIN' is not allowed”, “Failed to configure effective capabilities...”, or “Defaults drop all caps; set spec.security.allowPrivileged/extraCapabilities to opt-in”) so operators do not have to inspect controller logs to understand why a bundle failed.

The event payload mirrors the bundle namespace/name and surfaces under `nanocloud bundle events --reason SecurityPolicyViolation`, allowing automation to alert on misconfigured workloads.

## Profile Export Artifacts

Nanocloud bundles can be exported as signed tarballs so operators can reapply
the exact set of options, encrypted secrets, and binding history during
reinstall or disaster-recovery workflows.

### API and CLI contract
- Endpoint: `POST /apis/nanocloud.io/v1/namespaces/{namespace}/bundles/{name}/exportProfile`.
- Authorization: callers must hold the `bundles.backup` scope; the endpoint
  returns `404` when the bundle no longer exists or the recorded profile is
  missing.
- CLI: `nanocloud bundle export --profile <service> [--namespace <ns>] (--output PATH | --stdout) [--include-secrets]`.
  The command streams the HTTP response to disk (or STDOUT) and mirrors any
  server-side validation errors verbatim. `--include-secrets` forwards an
  `includeSecrets=true` query parameter so operators with sufficient privileges
  can capture encrypted secret payloads alongside non-secret options.

### Tarball layout
Every export contains two files:

1. `manifest.json` matches `BundleProfileExportManifest` and includes the bundle
   metadata plus a `digest` field (hex-encoded SHA256) computed from the profile
   payload described below.
2. `profile.json` matches `BundleProfileArtifact` and contains:
   - `metadata`: copy of the bundle metadata (name, namespace, service, resourceVersion).
   - `createdAt`: RFC3339 timestamp generated at export time.
   - `profileKey`: wrapped encryption key used to decrypt the stored secrets.
   - `options`: map containing every non-secret option (e.g., `region`,
     `replicas`). Keys prefixed with `secret_` are intentionally omitted.
   - `secrets`: array of `{name, cipherText, keyId}` entries for every secret
     option when `include-secrets` is enabled. The ciphertext is the exact value
     persisted in the bundle profile; the `keyId` records which KMS/identity was
     used to encrypt it.
   - `bindings`: serialized `BindingHistoryEntry` list so auditors can confirm
     when each binding last ran and why it succeeded or failed.
   - `integrity.sha256`: hex-encoded SHA256 of the canonical JSON serialization
     of the `BundleProfileArtifactData` payload (everything except the integrity
     block). This value always matches `manifest.digest`.

The manifest and profile payload are deterministic: hashing `profile.json`'s
data section with `sha256sum` produces the same digest stored in both files,
enabling tamper detection even when artifacts are distributed outside of
Nanocloud.

### Relationship to uninstall snapshots
- Running `nanocloud uninstall <service> --snapshot /backups/<service>-volumes.tar`
  now triggers a profile export *before* workloads are removed. The CLI writes
  the profile tarball to `/<backups>/<service>-volumes.profile.tar`, then fetches
  the storage snapshot requested by `--snapshot`.
- Both files are required for a complete reinstall: the profile export preserves
  the wrapped encryption key, option values, secret ciphertext, and binding
  history while the volume snapshot captures block/device state. Automation can
  reconstruct the original `spec.options` map by merging `profile.json.options`
  with the encrypted entries listed under `profile.json.secrets` and, when
  desired, re-serializing `profile.json.bindings` back into the reserved
  `.nanocloud.bindings.v1` profile key.
- See `docs/guides/backups.md` for step-by-step export, verification, and
  uninstall workflows.

## Diagnostics Loopback Probe

The `nanocloud diagnostics` command is extended with a loopback probe that
validates host networking (CNI) and storage (CSI) wiring without requiring users
to craft their own workloads.

- CLI flags:
  - `--loopback/--no-loopback` toggle the probe (defaults to enabled).
  - `--loopback-image <ref>` overrides the default Dockyard image
    (`dockyard.nanocloud.io/diagnostics/loopback:latest`).
  - `--loopback-timeout 90s` caps the end-to-end execution time; the CLI fails with exit
    code `2` when the probe exceeds this threshold.
- `LoopbackProbeConfig` fields exposed via the diagnostics module:
  - `image: String` – fully-qualified image reference pulled without using the
    OCI layer cache so misconfigured registries are surfaced.
  - `timeout: Duration` – overall probe timeout, defaulting to 90 seconds.
- When invoked the probe:
  1. Launches a pod attached to `nanocloud0` (host network namespace) so DNS and
     routing behave exactly like regular workloads.
  2. Mounts an ephemeral CSI volume at `/mnt/nanocloud-loopback` and writes a
     sentinel file to confirm read/write access.
  3. Runs DNS lookups against control-plane endpoints plus the `!dns` macro to
     ensure template expansion behaves as expected.
 4. Emits a structured JSON payload
     (`LoopbackProbeResult { dns_ok, volumes_ok, logs, duration_ms }`) that the
     CLI converts into human-readable output with remediation hints and persists
     a terse log line under `$NANOCLOUD_DIAGNOSTICS_LOG_DIR`
     (default `/var/log/nanocloud/diagnostics`).

### Exit codes

| Code | Meaning |
| ---- | ------- |
| `0`  | All phases succeeded (image pull, CNI attach, DNS check, CSI check). |
| `1`  | Probe completed but at least one check (`dns_ok` or `volumes_ok`) failed (CLI prints hints). |
| `2`  | Fatal runtime failure (image pull, network attach, CSI mount, timeout). |

Partial failures still produce a JSON report and print remediation hints so
operators can inspect logs immediately. Fatal failures emit the underlying
runtime error and skip the summary table.

### Automation hooks
- Setting `NANOCLOUD_DIAGNOSTICS_LOOPBACK=1` before launching
  `nanocloud server` runs the same probe as part of startup diagnostics. Results
  are logged via `tracing` and surfaced through the CLI command for later review.
- Future CI jobs can import the probe module directly (`diagnostics::loopback`)
  to run the same checks programmatically.

See `docs/guides/diagnostics.md` for end-user output samples and operational
guidance.
