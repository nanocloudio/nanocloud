#!/usr/bin/env bash
# Live E2E for the full workload chain: both
# deployment_reconciler and replicaset_reconciler run in ONE fluxor graph on the
# shared control-plane store, so a single Deployment cascades all the way to Pods
# — Deployment → ReplicaSet → Pods — with no orchestration beyond the store.
#
# The store is single-process; the API plane's one Deployment is seeded into the
# durable log ($D/store.log) and replayed at boot. deployment_reconciler projects
# the ReplicaSet; its PUT synchronously pushes a namespace.change onto
# replicaset_reconciler's sink, which materialises the Pods — the whole cascade
# settles IN ONE PROCESS — cooperating modules over one store, end to end.
#
#   /deployments.apps/<ns>/<name> = "replicas=<N>;image=<img>"
#   /replicasets.apps/<ns>/<name> = "replicas=<N>;image=<img>;owner=<dep>"
#   /pods/<ns>/<name>-<i>         = "image=<img>;owner=<rs>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-workload.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-wlc-e2e-XXXXXX)"
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

echo "== 1. build the config + module table from the 2-module workload graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. create ONE Deployment (replicas=2) — the only external write =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'

echo "== 3. run the workload chain; the cascade settles over the store =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the cascade: Deployment → ReplicaSet → Pods =="
RS="$(store_last /replicasets.apps/default/web)"
case "$RS" in *replicas*2*) true ;; *) false ;; esac \
  || fail "replicaset wrong: got '$RS'"
echo "   replicaset: $RS"
for i in 0 1; do
  v="$(store_last /pods/default/web-$i)"
  case "$v" in *nginx*) : ;; *) fail "pod web-$i wrong: got '$v'";; esac
  echo "   pod web-$i: $v"
done

echo "== E2E green: one Deployment cascades to Pods through cooperating modules =="
