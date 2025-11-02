# Runtime Security Guide

Nanocloud ships secure-by-default workloads. Containers run without any ambient
capabilities and the embedded seccomp profile rejects namespace/ptrace/module
operations. Use the guidance below when a workload legitimately requires
additional privileges.

## Declaring Capabilities

The bundle schema exposes the `spec.security` block:

```yaml
apiVersion: nanocloud.io/v1
kind: Bundle
metadata:
  name: net-debug
spec:
  service: net-debug
  security:
    extraCapabilities:
      - CAP_NET_ADMIN      # adds iptables privileges
      - CAP_NET_RAW        # raw sockets for traceroute
```

Capabilities are normalized to the canonical `CAP_*` form and deduplicated.
Values outside the `CAP_NET_*` allowlist require an explicit acknowledgement:

```yaml
spec:
  security:
    allowPrivileged: true
    extraCapabilities:
      - CAP_SYS_ADMIN
      - CAP_CHOWN
```

Without `allowPrivileged`, admission rejects the bundle with a descriptive
validation error.

## Custom Seccomp Profiles

Set `seccompProfile.type` to override the baseline filter:

```yaml
spec:
  security:
    seccompProfile:
      type: Localhost
      localhostProfile: kube-proxy
```

`Localhost` profiles are loaded from `/etc/nanocloud/policies/<name>.json` and
should follow the standard libseccomp JSON format. Use `RuntimeDefault` to
delegate to the host runtime or keep `Baseline` for Nanocloud’s built-in policy.

## Troubleshooting Events

Security enforcement emits controller events with actionable hints. Tail them via
`nanocloud bundle events`:

```
$ nanocloud bundle events --bundle net-debug --since 10m
TIME                TYPE    REASON                      MESSAGE
2025-01-13T18:05Z   Error   SecurityPolicyViolation     Capability 'CAP_SYS_ADMIN' is not allowed (set spec.security.allowPrivileged=true)
2025-01-13T18:06Z   Error   PrivilegeEscalationDenied   Failed to configure effective capabilities: Operation not permitted
```

Common remediation steps:

- **SecurityPolicyViolation** – update `spec.security` with the required
  `extraCapabilities` and, if necessary, set `allowPrivileged=true`.
- **PrivilegeEscalationDenied** – verify the host kernel permits the requested
  capability set (e.g., disable conflicting LSM policies) or remove the
  capability from the bundle.

Events also appear in the reconciliation summary (`reason` field) so CI pipelines
and alerting systems can react without scraping controller logs.
