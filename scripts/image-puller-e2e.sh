#!/usr/bin/env bash
# Live E2E for nanocloud's image_puller module (modules/app/image_puller) — the
# pull-scheduling decision over the control-plane store.
#
# Consumed through the standard storage contracts (`storage.object` 0x14 +
# `storage.namespace` 0x13). The store is
# single-process, owned by the fluxor-linux runtime, and seeded from its durable
# append-log at init. This script (playing nanocloud after it read the manifests
# and populated the content store) seeds /image-manifests/ (layer digests) and
# /blobs/ (present content) into `$D/store.log`; at boot the image_puller fmod
# cold-starts (SUBSCRIBE /image-manifests/ + /blobs/ onto its `changes` input,
# then a full LIST reconcile), computes each image's missing layers, and writes
# /image-pull-plan/. `image_fetcher` performs the fetch the plan names.
#
# Compact format:
#   /image-manifests/<image> = "layers=<digest>,<digest>,..."
#   /blobs/<digest>          = <present marker>
#   /image-pull-plan/<image> = "pull=<digest>,..."   (missing; empty = complete)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-imagepull.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/image_puller.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgp-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

store_last() { # store_last <key>  — last-writer value of a key in the log
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want:
        out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project two manifests + a partially-populated content store =="
store_put "/image-manifests/nginx" "layers=sha256:aaa,sha256:bbb,sha256:ccc"
store_put "/image-manifests/redis" "layers=sha256:xxx"
store_put "/blobs/sha256:aaa" "1"   # present
store_put "/blobs/sha256:ccc" "1"   # present   (bbb is missing)
store_put "/blobs/sha256:xxx" "1"   # redis fully present

echo "== 3. run the pull scheduler =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify each image's pull plan (manifest layers − present blobs) =="
NGINX="$(store_last /image-pull-plan/nginx)"
REDIS="$(store_last /image-pull-plan/redis)"
[ "$NGINX" = "pull=sha256:bbb" ] || fail "nginx plan wrong: got '$NGINX', want 'pull=sha256:bbb'"
[ "$REDIS" = "pull=" ]           || fail "redis plan wrong: got '$REDIS', want 'pull=' (complete)"

echo "== E2E green: pull scheduling over the shared store =="
echo "   nginx: $NGINX   (only the missing layer)   redis: $REDIS (complete)"
