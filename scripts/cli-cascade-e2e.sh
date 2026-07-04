#!/usr/bin/env bash
# Live integration E2E the fluxor-native CLI and
# the fluxor-native control plane cooperating over the shared store — every piece
# a PIC fmod. A Deployment is reconciled to its pods; the `nanocloud scale`
# cli-applet mutates desired state; the control-plane graph reconciles the CLI's
# change to the new pod count. No host code, no HTTP — just fmods over
# the store.
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
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/nanocloud_cli.fmod" \
         "$MODULES_DIR/deployment_reconciler.fmod" "$CLI_GRAPH" "$CP_GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-cascade-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; exit 1; }

count_pods() {
  python3 - "$D/store.log" <<'PY'
import struct, sys
d = open(sys.argv[1], "rb").read(); p = 0; last = {}
while p + 15 <= len(d):
    r, o, kl, vl = struct.unpack("<QBHI", d[p:p+15])
    k = d[p+15:p+15+kl].decode("utf8", "replace"); last[k] = o
    p += 15 + kl + vl
print(sum(1 for k, o in last.items() if k.startswith("/pods/") and o == 1))
PY
}

echo "== 1. seed a Deployment (replicas=2) + a ready node =="
python3 - "$D/store.log" <<'PY'
import struct, os
def rec(f,rev,k,v): f.write(struct.pack("<QBHI",rev,1,len(k),len(v))+k+v)
with open(os.sys.argv[1],"wb") as f:
    rec(f,1,b"/deployments.apps/default/web",b'{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}')
    rec(f,2,b"/nodes/node-1",b'{"metadata":{"name":"node-1"},"status":{"ready":true}}')
PY

echo "== 2. build both graphs (control plane + cli) =="
nc_build_workload "$ROOT" "$CP_GRAPH" "$D/cp.bin" "$D/cp.mods"
nc_build_workload "$ROOT" "$CLI_GRAPH" "$D/cli.bin" "$D/cli.mods"

cp_run()  { FLUXOR_STORE_DIR="$D" timeout 3 "$FLUXOR_RUNTIME" --config "$D/cp.bin"  --modules "$D/cp.mods"  >/dev/null 2>&1 || true; }
cli()     { FLUXOR_STORE_DIR="$D"           "$FLUXOR_RUNTIME" --config "$D/cli.bin" --modules "$D/cli.mods" -- "$@" 2>/dev/null; }

echo "== 3. control plane reconciles the Deployment → pods =="
cp_run
n="$(count_pods)"; [ "$n" = "2" ] || fail "expected 2 pods after initial reconcile, got $n"
echo "   pods: $n"

echo "== 4. the CLI scales desired state: nanocloud scale deployments default/web 5 =="
out="$(cli scale deployments default/web 5)"
echo "   $out"
echo "$out" | grep -q "scaled to 5" || fail "scale command failed"

echo "== 5. control plane reconciles the CLI's change → pods scale up =="
cp_run
n="$(count_pods)"; [ "$n" = "5" ] || fail "expected 5 pods after scale reconcile, got $n"
echo "   pods: $n"

echo "== E2E green: fluxor-native CLI (scale) → fluxor-native control plane (reconcile) → 5 pods, all fmods =="
