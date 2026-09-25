#!/usr/bin/env bash
# Live integration E2E the fluxor-native CLI and
# the fluxor-native control plane cooperating over the shared store — every piece
# a PIC fmod. A Deployment is reconciled to its pods; the `nanocloud scale`
# cli-applet mutates desired state; the control-plane graph reconciles the CLI's
# change to the new pod count. No host code, no HTTP — just fmods over
# the store, which both run as the node's one replica of it (one at a time).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
CLI_GRAPH="$ROOT/packaging/cli/linux.yaml"
CP_GRAPH="$ROOT/packaging/debian/fluxor-controlplane.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_source.fmod" "$MODULES_DIR/store_effect.fmod" "$MODULES_DIR/decision.fmod" "$MODULES_DIR/nanocloud_cli.fmod" \
         "$CLI_GRAPH" "$CP_GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-cascade-XXXXXX)"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  rm -rf "$D"
}
trap cleanup EXIT
# Both graphs open the store (lattice's) in $D, one after the other, as the
# same replica of it.
nc_lattice_prepare "$D"
fail() { echo "FAIL: $1"; tail -20 "$D/cp.log" 2>/dev/null || true; exit 1; }

count_pods() { nc_store_keys "$D" /pods/ | grep -c . || true; }

echo "== 1. seed a Deployment (replicas=2) + a ready node =="
python3 - "$D/store.log" <<'PY'
import struct, os
def rec(f,rev,k,v): f.write(struct.pack("<QBHI",rev,1,len(k),len(v))+k+v)
with open(os.sys.argv[1],"wb") as f:
    rec(f,1,b"/deployments.apps/default/web",b'{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}')
    rec(f,2,b"/nodes/node-1",b'{"metadata":{"name":"node-1"},"status":{"ready":true}}')
PY

echo "== 2. build both graphs (control plane + cli), rendered as this node's replica =="
# Both templates render as replica 0 of a one-voter group, with one PKI —
# what postinst provisions on a node.
nc_controlplane_graph "$D" "$D/cp-graph.yaml" || fail "the control plane did not render"
nc_render_graph "$D" "$CLI_GRAPH" "$D/cli-graph.yaml" || fail "the CLI did not render"
nc_build_workload "$ROOT" "$D/cp-graph.yaml" "$D/cp.bin" "$D/cp.mods"
nc_build_workload "$ROOT" "$D/cli-graph.yaml" "$D/cli.bin" "$D/cli.mods"

# cp_until <pods>: run the control plane until it reconciles to <pods>, then
# stop it (the CLI holds the replica next).
cp_until() {
  env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" --config "$D/cp.bin" --modules "$D/cp.mods" >>"$D/cp.log" 2>&1 &
  RUNTIME_PID=$!
  nc_store_ready "$D" || fail "the store never answered"
  for _ in $(seq 60); do
    [ "$(count_pods)" = "$1" ] && break
    sleep 0.5
  done
  N="$(count_pods)"
  kill "$RUNTIME_PID"; wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}
cli()     { env -C "$D"           "$FLUXOR_RUNTIME" --config "$D/cli.bin" --modules "$D/cli.mods" -- "$@" 2>/dev/null; }

echo "== 3. control plane reconciles the Deployment → pods =="
cp_until 2
[ "$N" = "2" ] || fail "expected 2 pods after initial reconcile, got $N"
echo "   pods: $N"

echo "== 4. the CLI scales desired state: nanocloud scale deployments default/web 5 =="
out="$(cli scale deployments default/web 5)"
echo "   $out"
echo "$out" | grep -q "scaled to 5" || fail "scale command failed"

echo "== 5. control plane reconciles the CLI's change → pods scale up =="
cp_until 5
[ "$N" = "5" ] || fail "expected 5 pods after scale reconcile, got $N"
echo "   pods: $N"

echo "== E2E green: fluxor-native CLI (scale) → fluxor-native control plane (reconcile) → 5 pods, all fmods =="
