# Event Stream CLI Checkpoint

## Test Run

- `cargo test --lib`

## Sample Command

```
$ nanocloud events --namespace prod --bundle payments --since 5m --level warning --follow
TIME                     TYPE     REASON                       OBJECT                MESSAGE
2025-01-14T11:32:18Z     Warning  SecurityPolicyViolation      prod/payments         Capability 'CAP_SYS_ADMIN' is not allowed
2025-01-14T11:32:21Z     Warning  PrivilegeEscalationDenied    prod/payments         Failed to configure effective capabilities: Operation not permitted
```
