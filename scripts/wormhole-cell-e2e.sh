#!/usr/bin/env bash
# Wormhole's ingress-cell capacity, provisioned through nanocloud's own
# workload path (wh_cell.plan, in packaging/debian/fluxor-wormhole.yaml):
#
#   /wh-demand/<cell> = hosts=<n>;profile=<pi5|server>;image=<ref>   Wormhole's
#   -> /namespaces/wormhole-system, /deployments.apps/wormhole-system/<cell>
#
# Proves: the Deployment is created at the demand's size, one per node
# (required podAntiAffinity on kubernetes.io/hostname), on the host network,
# sized by the profile; a new size scales it and a new image rolls its
# template, without rewriting what is unchanged; another Deployment in the
# namespace is left alone; a demand of 0 hosts, then a removed one, deletes
# it. Then the shipped Deployment chain (fluxor-deployment.yaml) runs on the
# same store and projects the cell's ReplicaSet: the workload path takes it
# from there as it does any Deployment.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-wormhole.yaml"
DEPLOY="$ROOT/packaging/debian/fluxor-deployment.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_stamped.fmod" "$GRAPH" "$DEPLOY"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-whcell-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; grep -v WARN "$D/run.log" 2>/dev/null | tail -10 || true; exit 1; }
. "$ROOT/scripts/support/np.sh"

run() { # run <config> <modules> <seconds>
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$3" "$FLUXOR_RUNTIME" \
    --config "$1" --modules "$2" >>"$D/run.log" 2>&1 || true
}
cell() { run "$D/cell.cfg" "$D/cell.mod" "$1"; }
DEP=/deployments.apps/wormhole-system/edge-a
# One field of the Deployment, as JSON.
dq() { python3 -c 'import json,sys
o = json.loads(sys.argv[1]) if sys.argv[1] else None
for k in sys.argv[2].split("."):
    o = o[int(k)] if isinstance(o, list) else (o or {}).get(k)
print(json.dumps(o, separators=(",", ":")))' "$(np_last "$1")" "$2"; }
writes() { python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}

echo "== 1. build the shipped Wormhole graph and the shipped Deployment graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/cell.cfg" "$D/cell.mod" >/dev/null || fail "the Wormhole graph did not build"
nc_build_workload "$ROOT" "$DEPLOY" "$D/dep.cfg" "$D/dep.mod" >/dev/null || fail "the Deployment graph did not build"

echo "== 2. a demand of three pi5 hosts; another Deployment in the namespace =="
np_put "/wh-demand/edge-a" "hosts=3;profile=pi5;image=registry.nanocloud.io:5000/wormhole/ingress:1"
np_put "/deployments.apps/wormhole-system/other" '{"metadata":{"name":"other","namespace":"wormhole-system"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"c","image":"busybox"}]}}}}'
cell 8
[ -n "$(np_last /namespaces/wormhole-system)" ] || fail "the namespace was not created"
[ "$(dq $DEP spec.replicas)" = 3 ] || fail "replicas: $(dq $DEP spec.replicas)"
[ "$(dq $DEP metadata.labels)" = '{"app.kubernetes.io/managed-by":"nanocloud-wh-cell","wormhole.nanocloud.io/cell":"edge-a"}' ] || fail "labels: $(dq $DEP metadata.labels)"
[ "$(dq $DEP spec.template.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution.0.topologyKey)" = '"kubernetes.io/hostname"' ] \
  || fail "one per node: $(dq $DEP spec.template.spec.affinity)"
[ "$(dq $DEP spec.template.spec.hostNetwork)" = true ] || fail "the host's network"
[ "$(dq $DEP spec.template.spec.containers.0.image)" = '"registry.nanocloud.io:5000/wormhole/ingress:1"' ] || fail "image"
[ "$(dq $DEP spec.template.spec.containers.0.resources.requests)" = '{"cpu":"1","memory":"512Mi"}' ] || fail "pi5 requests: $(dq $DEP spec.template.spec.containers.0.resources.requests)"
[ "$(dq $DEP metadata.uid)" != null ] || fail "created as an object the API would have created (no uid)"
echo "   wormhole-system/edge-a: 3 replicas, one per node, host network, pi5 (1 CPU, 512Mi)"

echo "== 3. settled: nothing rewritten =="
N="$(writes $DEP)"
cell 5
[ "$(writes $DEP)" = "$N" ] || fail "a settled cell was rewritten ($N -> $(writes $DEP) writes)"
echo "   $N write(s), none since"

echo "== 4. the demand grows to five server hosts with a new image: scaled and rolled =="
np_put "/wh-demand/edge-a" "hosts=5;profile=server;image=registry.nanocloud.io:5000/wormhole/ingress:2"
cell 8
[ "$(dq $DEP spec.replicas)" = 5 ] || fail "replicas: $(dq $DEP spec.replicas)"
[ "$(dq $DEP spec.template.spec.containers.0.image)" = '"registry.nanocloud.io:5000/wormhole/ingress:2"' ] || fail "image not rolled"
[ "$(dq $DEP spec.template.spec.containers.0.resources.requests)" = '{"cpu":"4","memory":"4Gi"}' ] || fail "server requests"
[ "$(dq /deployments.apps/wormhole-system/other spec.replicas)" = 1 ] || fail "another Deployment was touched"
echo "   5 replicas, ingress:2, server (4 CPU, 4Gi); wormhole-system/other untouched"

echo "== 5. the workload path takes it from there: the shipped Deployment chain projects its ReplicaSet =="
run "$D/dep.cfg" "$D/dep.mod" 3
RS="$(np_last /replicasets.apps/wormhole-system/edge-a)"
case "$RS" in *'"replicas":5'*'"wormhole.nanocloud.io/cell":"edge-a"'*|*'"wormhole.nanocloud.io/cell":"edge-a"'*'"replicas":5'*) ;;
  *) fail "no ReplicaSet of 5 for the cell: '${RS:0:300}'" ;; esac
grep -q 'ingress:2' <<<"$RS" || fail "the ReplicaSet does not carry the cell's image"
echo "   /replicasets.apps/wormhole-system/edge-a: 5 replicas of ingress:2"

echo "== 6. 0 hosts: deleted; the other Deployment stays =="
np_put "/wh-demand/edge-a" "hosts=0;profile=server;image=registry.nanocloud.io:5000/wormhole/ingress:2"
cell 6
[ -z "$(np_last $DEP)" ] || fail "a 0-host cell's Deployment remains"
[ -n "$(np_last /deployments.apps/wormhole-system/other)" ] || fail "the other Deployment was deleted"
echo "   edge-a deleted; other kept"

echo "== 7. a demand removed outright: its Deployment, recreated meanwhile, goes =="
np_put "/wh-demand/edge-b" "hosts=2;profile=pi5;image=registry.nanocloud.io:5000/wormhole/ingress:1"
cell 6
[ "$(dq /deployments.apps/wormhole-system/edge-b spec.replicas)" = 2 ] || fail "edge-b was not provisioned"
np_put "/wh-demand/edge-b" "-DELETE-"
cell 6
[ -z "$(np_last /deployments.apps/wormhole-system/edge-b)" ] || fail "edge-b outlived its demand"
echo "   edge-b provisioned (2), its demand removed, deleted"

echo "== E2E green: Wormhole's ingress-cell demand provisioned as a Deployment, through the workload path =="
