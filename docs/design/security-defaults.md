# Runtime Security Defaults

Nanocloud phases 5–7 require predictable runtime hardening so bundle authors
understand the exact guardrails enforced by the kubelet/runtime pair. This
design captures the default capability set, the seccomp baseline, and how
bundle authors explicitly opt into more permissive profiles.

## Goals
- Ship a deterministic seccomp filter that blocks filesystem/mount, module
  loading, ptrace, and other privileged syscalls by default.
- Drop *all* inheritable/permitted/ambient Linux capabilities unless a bundle
  explicitly opts into additional privileges.
- Provide a small vocabulary of workload classes that explains which
  capabilities must be added and why.
- Surface security denials as controller events so operators see remediation
  hints without reading logs.

## Workload Classes & Capabilities
The runtime enforces the following capability policy. Bundle authors map their
service to a class via `spec.security.allow_privileged` or by enumerating
`spec.security.extraCapabilities`.

| Class | Capabilities | Example workloads | Notes |
| ----- | ------------ | ----------------- | ----- |
| `baseline` | *(none)* | Web/UI services, control-plane sidecars, jobs that only need regular syscalls. | Processes still run as the requested UID/GID but have zero kernel capabilities. |
| `network-admin` | `CAP_NET_ADMIN`, `CAP_NET_RAW`, `CAP_NET_BIND_SERVICE` | CNI helpers, ingress controllers needing low ports or IP rules. | Requires `spec.security.extraCapabilities=["CAP_NET_ADMIN","CAP_NET_RAW"]`; `CAP_NET_BIND_SERVICE` may be requested independently. |
| `node-maintenance` | `CAP_SYS_ADMIN`, `CAP_SYS_CHROOT`, `CAP_DAC_OVERRIDE`, `CAP_SYS_PTRACE` | Backup/restore helpers that mount snapshots or inspect other processes. | Requires `allow_privileged=true`. Enabling this class keeps the seccomp baseline but skips capability drops. |

Workload classes are documentation helpers—at the API level bundle authors list
the additional capabilities they need. Anything outside the table is rejected
during admission.

## SecurityProfile Shape
Bundle specs gain an optional `security` block:

```yaml
spec:
  security:
    allowPrivileged: false            # default
    extraCapabilities: []             # optional, validated against allowPrivileged
    seccompProfile:
      type: Baseline                  # Baseline | RuntimeDefault | Localhost
      localhostProfile: ""            # required when type=Localhost
```

- `allowPrivileged=true` lifts the capability drop *after* validating the caller
  is allowed to request privileged access. When `false`, requesting `SYS_*`
  capabilities fails admission.
- `extraCapabilities` supports the `network-admin` use case; the controller
  ensures duplicates are removed and converts strings to the canonical kernel
  form (`CAP_FOWNER`).
- `seccompProfile.type=Baseline` forces the embedded profile shipped under
  `assets/security/seccomp-baseline.json`. `RuntimeDefault` defers to the host
  runtime (if applicable) and `Localhost` loads a profile from
  `/etc/nanocloud/policies/<name>.json`.

## Baseline Seccomp Profile
The embedded profile denies high-risk syscalls and only allows the following
groups:
- `prctl`, `arch_prctl`, `setrlimit`, `rt_sig*`, `futex`, basic scheduling.
- File IO, directory traversal, and networking syscalls (`socket`, `connect`,
  `accept`, `sendto`, `recvfrom`, `epoll_*`).
- Memory management (`mmap`, `mprotect`, `brk`, `munmap`) and process control
  (`clone3` limited to user namespaces, `wait4`, `exit_group`).
- Timekeeping and randomness (`clock_gettime`, `getrandom`, `nanosleep`).

Everything else returns `EPERM`, which covers: mounting/remounting, module
loading, `ptrace`, kexec, raw IO, and perf events. The filter is applied by the
runtime immediately before `execve` so the init process and its descendants
inherit the policy.

## Controller Events
When defaults block a workload:
- `SecurityPolicyViolation` – the requested spec contradicts the default (e.g.
  `extraCapabilities` requested without `allowPrivileged`).
- `PrivilegeEscalationDenied` – the runtime failed to apply the requested
  capability/seccomp configuration; the event message explains how to adjust
  the bundle spec.

Events reference the bundle namespace/name and include prescriptive hints:
“Set `spec.security.allowPrivileged=true` or remove `CAP_SYS_ADMIN`”.
