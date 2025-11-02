# Binding Execution Guide

Dockyard images declare binding steps with `&bindings.<id>` tokens inside the
`io.nanocloud.options` label. Each token expands into a command array that runs
after options, secrets, and profiles have been materialized but before the
containing bundle is marked `Bound`. This guide summarizes the contract enforced
by the binding execution envelope and the knobs that image authors or operators
can tune during development.

## How bindings are resolved
1. During reconciliation the controller evaluates every binding template emitted
   by the Dockyard contract parser and orders them deterministically by service
   name and binding identifier (the suffix after `&bindings.`).
2. Each binding is executed sequentially per service so that commands can rely on
   previous steps (e.g., `create_user` before `grant_privileges`). Cross-service
   bindings are independent; the controller can run bindings for different target
   services in parallel.
3. On success the controller writes a `BindingRecord` to the bundle profile that
   includes the binding identifier, the command that was invoked, timestamps,
   and an idempotency token.

## Envelope behavior
- **Namespaces** – Bindings inherit the dependent workload's `mnt`, `uts`, `ipc`,
  `pid`, and, when appropriate, `net` namespaces. Commands therefore interact with
  the same filesystems, DNS, and sockets as the running container without exposing
  host resources.
- **Privileges** – Immediately before spawning the process the controller drops to
  `uid=65534` and `gid=65534` (nobody). Override these defaults by defining
  `NANOCLOUD_BINDING_UID` / `NANOCLOUD_BINDING_GID` (either as decimal integers or
  as `user:group` pairs) before launching `nanocloud server`.
- **Seccomp & AppArmor** – The default seccomp profile is embedded in the binary
  and installed at `/etc/nanocloud/policies/bindings-default.json`. Override it
  by pointing `NANOCLOUD_BINDING_SECCOMP` at a JSON file that follows the OCI
  runtime seccomp schema. AppArmor enforcement uses the profile name stored in
  `NANOCLOUD_BINDING_APPARMOR` (set to `disable` to opt out on kernels that lack
  AppArmor support).
- **Timeouts** – Every binding receives 90 seconds to finish. Set
  `NANOCLOUD_BINDING_TIMEOUT=5m` (or use the `nanocloud server --binding-timeout`
  flag once available) to adjust the limit. Expired bindings are killed with
  SIGTERM followed by SIGKILL and recorded as `BindingTimeout`.
- **Environment** – The process sees a minimal environment:
  `PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin`,
  `LANG=C.UTF-8`, `NANOCLOUD_BINDING_ID`, `NANOCLOUD_SERVICE`,
  `NANOCLOUD_NAMESPACE`, and any option-derived environment variables the runtime
  writes to `/var/run/nanocloud.io/env`. STDOUT/STDERR are captured and attached
  to binding events for troubleshooting.

## Idempotency and force re-runs
- The `binding_id` (e.g., `create_user`) becomes part of the idempotency key
  persisted in the bundle profile. When the exact binding finishes successfully
  future reconciles skip it automatically.
- `nanocloud install <service> --force-bindings` clears stored tokens before the
  next reconcile, forcing every binding to run again.
- Operators can also delete individual records via
  `nanocloud profile edit <service> --remove-binding <binding_id>` (planned),
  which is useful when only one step needs to be re-run.

## Monitoring bindings
- Use `nanocloud events --reason BindingStarted,BindingCompleted,BindingFailed`
  to watch the live stream of binding activity. Each entry includes the binding
  identifier, attempt counter, exit code (if any), and duration in milliseconds.
- `nanocloud status --profile <service>` prints the binding history stored in
  the bundle profile so auditors can confirm when a step last ran and why.
- Prometheus scrapes of `/metrics` expose
  `nanocloud_binding_executions_total{service,binding,result}` and
  `nanocloud_binding_duration_seconds_bucket` so dashboards can highlight slow or
  frequently failing steps.

## Operational checks

1. **Surface binding history in the CLI** – `nanocloud status <bundle>`
   prints a dedicated `Bindings:` section listing each service/binding pair,
   its most recent status, attempt count, exit code, and timestamps. Use this
   output to confirm that automation ran the expected hooks before `Ready=True`.
2. **Tail binding events** – the `controller/bindings.lifecycle` event topic emits
   `BindingStarted`, `BindingCompleted`, and `BindingFailed` records containing the
   same metadata stored in the bundle profile. The CLI helper
   `nanocloud events --topic controller/bindings.lifecycle` bridges the stream
   for live troubleshooting sessions.
3. **Exercise the envelope in CI** – the asynchronous test
   `cargo test --lib nanocloud::engine::container::tests::binding_envelope_emits_events`
   runs a fake Dockyard binding command end-to-end and asserts that profile
   history plus event publication behave as expected. The test sets
   `NANOCLOUD_BINDING_SKIP_ISOLATION=1` to bypass namespace/AppArmor/seccomp
   when running inside unprivileged CI sandboxes.
