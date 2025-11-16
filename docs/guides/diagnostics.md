# Diagnostics Guide

Nanocloud ships a loopback probe that exercises the same CNI and CSI plumbing
used by real workloads. This guide explains how to run the probe, interpret its
output, and customize the experience for CI or on-host validation.

## Running the loopback probe

```bash
nanocloud diagnostics --loopback
```

Sample output:

```
Loopback probe image: dockyard.nanocloud.io/diagnostics/loopback:latest
  DNS check: OK (api.nanocloud.local → 10.0.0.10)
  Volume check: OK (/mnt/nanocloud-loopback/test.txt round-tripped)
  Duration: 4.2s
  Summary: PASS
```

Exit codes:
- `0` – all checks passed.
- `1` – probe finished but at least one check failed (see summary + hints).
- `2` – fatal runtime error (image pull failure, timeout, CNI/CSI attach error).

The CLI always writes the raw JSON payload to the verbose log so it can be
inspected later:

```json
{
  "dnsOk": true,
  "volumesOk": true,
  "logs": "/var/log/nanocloud/diagnostics/loopback-demo.log",
  "durationMs": 4217
}
```

Additionally, the probe appends a single-line summary to
`$NANOCLOUD_DIAGNOSTICS_LOG_DIR` (defaults to `/var/log/nanocloud/diagnostics`),
making it easy to correlate CLI output with historical diagnostics runs.

## Customizing the probe

Flags:
- `--loopback/--no-loopback` – explicitly enable or skip the probe
  (enabled by default).
- `--loopback-image <ref>` – pull and run a different diagnostic image
  (e.g., a staging build).
- `--loopback-timeout 2m` – adjust the global timeout (default 90 seconds).

Environment hooks:
- `NANOCLOUD_DIAGNOSTICS_LOOPBACK=0` – server startup skips the loopback probe.
- `NANOCLOUD_DIAGNOSTICS_LOOPBACK=1` – server startup runs the probe and logs the
  summary before accepting API traffic.

## Interpreting failures

| Phase | Symptoms | Next steps |
| ----- | -------- | ---------- |
| Image pull | Fatal error before the summary table, exit code `2`. | Verify Dockyard credentials or override `--loopback-image` to point at a private registry. |
| DNS check | Summary shows `DNS check: FAIL`; the CLI prints a DNS hint and exits with `1`. | Inspect `/var/log/nanocloud/diagnostics/*.log`; confirm host resolv.conf and the `nanocloud0` interface routes traffic correctly. |
| Volume check | Summary shows `Volume check: FAIL`; the CLI prints a volume hint and exits with `1`. | Validates CSI loopback writes failed. Examine the probe log for mount errors and confirm host volume permissions. |
| Timeout | CLI exits `2` after reporting `Probe exceeded timeout`. | Increase `--loopback-timeout`, verify that the runtime can schedule pods, and inspect runtime logs for hung attach/detach calls. |

When a check fails, the CLI now surfaces `Loopback probe hints` on stderr so
operators immediately see recommended next steps without consulting reference
material.

## CI integration

Tests can run the probe module directly:

```rust
use nanocloud::diagnostics::loopback::{run_loopback_probe, LoopbackProbeConfig};

let config = LoopbackProbeConfig {
    image: "dockyard.nanocloud.io/diagnostics/loopback:latest".into(),
    timeout: Duration::from_secs(90),
};
let result = run_loopback_probe(config).await?;
assert!(result.dns_ok && result.volumes_ok);
```

When using the CLI in CI, combine `--loopback` with `--json` (planned) or capture
STDOUT so results can be archived with other logs.
