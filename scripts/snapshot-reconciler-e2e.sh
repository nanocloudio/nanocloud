#!/usr/bin/env bash
# Live E2E for the VolumeSnapshot chain (`sn_*`, Chronicle params) as it ships
# in packaging/debian/fluxor-snapshot.yaml. For a VolumeSnapshot with a source
# PVC it provisions a bound VolumeSnapshotContent and marks the pair
# readyToUse. The physical data snapshot is a node-backend effect; this proves
# the control-plane binding over the store.
#
#   /volumesnapshots.snapshot.storage.k8s.io/<ns>/<name>          spec.source.persistentVolumeClaimName
#   /volumesnapshotcontents.snapshot.storage.k8s.io/snapcontent-<name>   (created)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-snapshot.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-snap-e2e-XXXXXX)"
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
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}
store_last() { # store_last <key>
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want: out = data[p+15+kl:p+15+kl+vl] if op == 1 else b""
    p += 15 + kl + vl
print(out.decode() if out else "")
PY
}

echo "== 1. build config + module table =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. create a VolumeSnapshot of PVC 'data' =="
store_put "/volumesnapshots.snapshot.storage.k8s.io/default/snap1" \
  '{"metadata":{"name":"snap1"},"spec":{"source":{"persistentVolumeClaimName":"data"}}}'

echo "== 3. run snapshot_reconciler =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. the VolumeSnapshot is bound + ready =="
VS="$(store_last /volumesnapshots.snapshot.storage.k8s.io/default/snap1)"
case "$VS" in
  *'"readyToUse":true'*'"boundVolumeSnapshotContentName":"snapcontent-snap1"'*) : ;;
  *) fail "VolumeSnapshot not bound/ready: got '$VS'" ;;
esac
echo "   snapshot: $VS"

echo "== 5. a bound VolumeSnapshotContent was provisioned =="
VSC="$(store_last /volumesnapshotcontents.snapshot.storage.k8s.io/snapcontent-snap1)"
if [[ "$VSC" == *'"readyToUse":true'* && "$VSC" == *'"volumeHandle":"data"'* && "$VSC" == *'"name":"snap1"'* ]]; then
  echo "   content: $VSC"
else
  fail "VolumeSnapshotContent wrong: got '$VSC'"
fi

echo "== E2E green: VolumeSnapshot -> bound, ready VolumeSnapshotContent =="
