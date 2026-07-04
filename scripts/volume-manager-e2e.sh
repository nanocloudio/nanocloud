#!/usr/bin/env bash
# Live E2E for nanocloud's volume_manager module (modules/app/volume_manager) —
# the mount-planning decision over the control-plane store.
#
# Consumed through the standard storage contracts (`storage.object` 0x14 +
# `storage.namespace` 0x13). The store is
# single-process, owned by the fluxor-linux runtime, and seeded from its durable
# append-log at init. This script (playing the PV/PVC binder) seeds
# /volumes/ (bound claim → device+fs) and /volume-requests/ (a pod's mounts)
# into `$D/store.log`; at boot the volume_manager fmod cold-starts (SUBSCRIBE
# /volume-requests/ + /volumes/ onto its `changes` input, then a full LIST
# reconcile), resolves each mount's claim, and writes the mount plan to
# /volume-plan/. Unbound claims land in pending=. The node's storage backend
# performs the mount from the plan.
#
# Compact format:
#   /volume-requests/<pod> = "vols=<mount>=<claim>,<mount>=<claim>,..."
#   /volumes/<claim>       = "device=<path>;fs=<type>"
#   /volume-plan/<pod>     = "mount=<mount>:<device>:<fs>,...;pending=<mount>,..."
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-volume.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/volume_manager.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-vol-e2e-XXXXXX)"
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

echo "== 2. bind two claims + two pod volume requests (one with an unbound claim) =="
store_put "/volumes/pvc-a" "device=/dev/sda1;fs=ext4"
store_put "/volumes/pvc-b" "device=/dev/sdb1;fs=xfs"
store_put "/volume-requests/pod1" "vols=data=pvc-a,config=pvc-b"
store_put "/volume-requests/pod2" "vols=data=pvc-a,logs=pvc-missing"

echo "== 3. run the mount planner =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the resolved mount plans =="
P1="$(store_last /volume-plan/pod1)"
P2="$(store_last /volume-plan/pod2)"
[ "$P1" = "mount=data:/dev/sda1:ext4,config:/dev/sdb1:xfs;pending=" ] \
  || fail "pod1 plan wrong: got '$P1'"
[ "$P2" = "mount=data:/dev/sda1:ext4;pending=logs" ] \
  || fail "pod2 plan wrong: got '$P2' (pvc-missing should be pending)"

echo "== E2E green: mount planning over the shared store =="
echo "   pod1: all bound.   pod2: $P2"
