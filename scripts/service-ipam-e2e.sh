#!/usr/bin/env bash
# Live E2E for nanocloud's service_ipam module (modules/app/service_ipam) —
# ClusterIP allocation over the fluxor-native control-plane store, consumed
# through the standard storage contracts (storage.object 0x14 + storage.namespace
# 0x13). the store lives inside
# the runtime and is seeded from its durable append-log ($D/store.log) at boot.
#
# Proves IPAM on real binaries: Services without a clusterIP are seeded into the
# store log; the allocator fmod assigns each the lowest free address from the
# service CIDR (10.96.0.0/16), skipping IPs already held by other Services, and
# leaves already-allocated Services alone.
#
#   /services/<ns>/<name> = "sel=k=v[;clusterIP=<ip>]"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-service-ipam.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/service_ipam.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
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
    f.flush(); os.fsync(f.fileno())
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
    if data[p+15:p+15+kl] == want:
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else b""
    p += 15 + kl + vl
print(out.decode() if out else "")
PY
}

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. two unallocated Services + one pre-allocated (holds 10.96.0.5) =="
store_put "/services/default/a"   '{"metadata":{"name":"a"},"spec":{"selector":{"app":"a"}}}'
store_put "/services/default/b"   '{"metadata":{"name":"b"},"spec":{"selector":{"app":"b"}}}'
store_put "/services/default/pre" '{"metadata":{"name":"pre"},"spec":{"selector":{"app":"pre"},"clusterIP":"10.96.0.5"}}'

echo "== 3. run the allocator =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify lowest-free allocation, skipping the held IP =="
expect() { # expect <svc> <clusterIP>
  local got; got="$(store_last "/services/default/$1")"
  case "$got" in *"\"clusterIP\":\"$2\""*) echo "   $1 -> $2"; return 0;; *) fail "$1 wrong: got '$got' want clusterIP=$2";; esac
  [ "$got" = "$2" ] || fail "$1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
# a and b get 10.96.0.1 and 10.96.0.2 (byte order); .5 is skipped (held by pre).
expect a   10.96.0.1
expect b   10.96.0.2
# the pre-allocated Service is left untouched.
expect pre 10.96.0.5

echo "== E2E green: ClusterIP allocation (lowest-free, collision-safe) over the store =="
