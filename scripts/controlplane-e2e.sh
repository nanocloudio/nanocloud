#!/usr/bin/env bash
# Live E2E for the COMPLETE fluxor-native control plane, as the node boots it:
# the API request path (a Chronicle chain behind tls + http, with the RBAC grant index and
# admission as params), every workload chain
# (deployment/replicaset/daemonset/statefulset/job/hpa/gc/namespace/scheduler/
# pod-lifecycle, IPAM) — composed into a
# single graph over the shared control-plane store.
#
# Proves the full cascade in one process: a Deployment projected into the store
# flows Deployment -> ReplicaSet -> Pods -> scheduled onto a node, entirely
# through cooperating graph nodes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-controlplane.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
[ -e "$FLUXOR_RUNTIME" ] || { echo "FAIL: missing runtime $FLUXOR_RUNTIME"; exit 1; }
[ -e "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }

D="$(mktemp -d /tmp/nc-cp-e2e-XXXXXX)"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  rm -rf "$D"
}
trap cleanup EXIT
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

echo "== 1. build config + module table from the control-plane graph =="
# The packaged control-plane graph is rendered per node (its replica of the
# store's group, its peer PKI) and references /etc/nanocloud.io/certs/...
# (which postinst provisions). For the test: a one-voter group, a P-256 API
# cert, and a reader — so this exercises the REAL production graph (TLS, the
# replicated store).
nc_controlplane_graph "$D" "$D/graph.yaml" || fail "the control plane did not render"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 2. project one Deployment (replicas=2) + a ready node (the API plane's role) =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
store_put "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'

echo "== 3. run the whole control plane; the cascade settles over the store =="
# The store is lattice's: the runtime runs in the state directory, which the
# store (lattice/) and the seeded host log (store.log, imported) are in.
nc_lattice_prepare "$D"
env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$D" || fail "the store never answered"
grep -q "store ready at construction" "$D/run.log" || fail "the lattice store never became ready"
for _ in $(seq 60); do
  case "$(nc_store_get "$D" /pods/default/web-1)" in *'"nodeName"'*) break ;; esac
  sleep 0.5
done

echo "== 4. verify Deployment -> ReplicaSet -> Pods -> scheduled =="
RS="$(nc_store_get "$D" /replicasets.apps/default/web)"
case "$RS" in *'"replicas":2'*'"image":"nginx"'*) ;; *) fail "replicaset wrong: got '$RS'";; esac
echo "   replicaset: $RS"
for i in 0 1; do
  POD="$(nc_store_get "$D" /pods/default/web-$i)"
  case "$POD" in
    *'"nodeName":"node-1"'*) echo "   pod web-$i: scheduled" ;;
    *) fail "pod web-$i not materialised+scheduled: '$POD'" ;;
  esac
done

echo "== E2E green: the complete fluxor-native control plane cascades Deployment -> scheduled Pods in one graph =="
