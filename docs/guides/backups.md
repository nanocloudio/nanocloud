# Backup & Export Guide

Nanocloud treats bundle profiles as first-class artifacts so operators can take
consistent backups alongside their volume snapshots. This guide walks through
the `nanocloud bundle export --profile` command, explains how the uninstall
workflow cooperates with exports, and highlights a few verification tips.

## Exporting a profile

Run the CLI against a live bundle:

```bash
nanocloud bundle export --profile demo \
  --namespace prod \
  --output /backups/demo-profile.tar
```

- `--output PATH` writes the tarball to disk. Use `--stdout` instead to stream
  directly into `tar` or an object store upload.
- `--include-secrets` forwards `includeSecrets=true` to the API and is required
  when you want encrypted `secret_*` options included in the artifact. The
  server enforces RBAC, so operators without `bundles.backup` scope receive a
  `403/404`.
- `--curl` prints the exact POST request against
  `/apis/nanocloud.io/v1/namespaces/<ns>/bundles/<name>/exportProfile` so CI/CD
  systems can capture artifacts via plain `curl`.

The resulting tarball always contains two files:

1. `manifest.json` – `BundleProfileExportManifest` with bundle metadata and the
   hex-encoded SHA256 digest of the payload.
2. `profile.json` – `BundleProfileArtifact`, which holds `metadata`, `profileKey`,
   `options`, `secrets`, `bindings`, and an `integrity.sha256` hash identical to
   the manifest digest.

Verify the export by recomputing the digest:

```bash
tar -xf /backups/demo-profile.tar profile.json
jq '.integrity.sha256' profile.json
python - <<'PY'
import hashlib, json, sys
data = json.load(open("profile.json"))
payload = json.dumps({k: data[k] for k in data if k != "integrity"}, separators=(",", ":"), sort_keys=True).encode()
print(hashlib.sha256(payload).hexdigest())
PY
```

## Streaming exports

Batch workflows can avoid temporary files entirely:

```bash
nanocloud bundle export --profile demo --stdout \
  | tee demo.profile.tar \
  | sha256sum
```

STDOUT carries the raw tar stream, so route progress or log messages to STDERR.

## Uninstall snapshots

The uninstall workflow now captures the profile automatically:

```bash
nanocloud uninstall demo \
  --snapshot /backups/demo-volumes.tar
```

1. The CLI first calls `/apis/nanocloud.io/v1/namespaces/<ns>/bundles/<service>/exportProfile`
   with `includeSecrets=true` and writes the profile tarball next to the requested snapshot file. For the example above the
   export becomes `/backups/demo-volumes.profile.tar`.
2. After the profile is secured the CLI waits for workloads to terminate and
   then downloads the storage snapshot to `/backups/demo-volumes.tar`.

Keep both files: the profile export preserves the wrapped encryption key,
non-secret options, encrypted `secret_*` values, and binding history while the
volume tarball captures persistent data. During reinstall you can merge
`profile.json.options` with `profile.json.secrets` to rehydrate the original
`spec.options` map before running `nanocloud install`.

For more details on the artifact schema see `docs/specification.md#profile-export-artifacts`.
