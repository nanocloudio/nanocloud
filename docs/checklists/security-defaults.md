# Security Defaults Checkpoint

## Test Run

- `cargo test --lib`

## Sample Events

```
$ nanocloud bundle events --bundle net-debug --since 5m
TIME                TYPE    REASON                      MESSAGE
2025-01-13T18:05Z   Error   SecurityPolicyViolation     Capability 'CAP_SYS_ADMIN' is not allowed (set spec.security.allowPrivileged=true)
2025-01-13T18:06Z   Error   PrivilegeEscalationDenied   Failed to configure effective capabilities: Operation not permitted
```
