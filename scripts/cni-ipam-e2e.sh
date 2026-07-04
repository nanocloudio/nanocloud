#!/usr/bin/env bash
# Live E2E for nanocloud's cni_ipam module (modules/app/cni_ipam) — pod IP
# allocation over the fluxor-native control-plane store, consumed through the
# standard storage contracts (storage.object 0x14 + storage.namespace 0x13). The
# store lives inside the
# runtime and is seeded from its durable append-log ($D/store.log) at boot.
#
# This script (playing the CNI plugin's request side) seeds the pool + a
# pre-existing lease + three requests into the store log; the cni_ipam fmod,
# inside the fluxor-linux runtime, allocates the lowest free host for each and
# writes the lease. Proves deterministic lowest-free allocation, the free-scan
# (skipping the pre-existing lease), and lease stability.
#
# Compact format:
#   /ipam-pool          = "cidr=<a.b.c.0>/<prefix>"
#   /ipam-request/<uid> = ""
#   /ipam-lease/<uid>   = "<ip>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-ipam.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/cni_ipam.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-ipam-e2e-XXXXXX)"
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
    f.flush()
    os.fsync(f.fileno())
PY
}

store_last() { # store_last <key>
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
        out = v
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. seed pool + a pre-existing lease (.1) + three requests =="
store_put "/ipam-pool" "cidr=10.244.0.0/24"
store_put "/ipam-lease/pre" "10.244.0.1"       # already allocated — must be skipped + kept
store_put "/ipam-request/pod1" ""
store_put "/ipam-request/pod2" ""
store_put "/ipam-request/pod3" ""

echo "== 3. run the allocator =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify deterministic lowest-free allocation (skipping .1) =="
expect() { # expect <uid> <want>
  local got; got="$(store_last "/ipam-lease/$1")"
  [ "$got" = "$2" ] || fail "lease for $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect pre  "10.244.0.1"   # untouched
expect pod1 "10.244.0.2"   # .1 skipped
expect pod2 "10.244.0.3"
expect pod3 "10.244.0.4"

echo "== E2E green: deterministic pod IP allocation over the shared store =="