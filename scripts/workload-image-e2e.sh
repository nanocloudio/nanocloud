#!/usr/bin/env bash
# Live E2E: a Pod of a WORKLOAD IMAGE — `runtimeClassName: fluxor` naming an
# OCI reference — on a node in the bare-metal shape (scripts/metal-plane-e2e.sh):
# the node pulls the image, holds the Pod Pending until every layer is in its
# blob cache, then creates the workload from the modules it brought
# (SOURCE_FMOD_PACK, each pinned by its layer digest). The module is NOT in
# the node's image: only the pack can have run it.
#
#   fluxor build --emit=workload  -> graph.flxa + program_probe.fmod
#   a registry stub serves them as `fluxor publish workload` lays them out
#   (artifactType application/vnd.nanocloud.fluxor.workload-image.v1)
#   POST Pod image=nanocloud/probe:1 -> pull -> /image-manifests/ kind=workload
#     -> /image-rootfs/ path=pack:… -> /pod-resolved/ -> /metal-sandboxes/
#     pack=… -> CREATE (pack) -> Running, and the module's own log line
#   DELETE the Pod -> its sandbox destroyed
#   a second image that SERVES: redis_edge_anchor wired to the node's network
#   through a lane of its own (`net:0`), answering PING on its port
#
# PASS = Running, `[prog]` in the node's log, the module absent from the
# node's image, the delete settled, and the second Pod answering on its port.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
REG_PORT=5394
SERVE_PORT=16380

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/workload-node.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-wimg-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }


echo "== 1. the workload images: fluxor build --emit=workload, served as a registry would =="
cat >"$D/probe.yaml" <<'YAML'
target: linux
tick_us: 1000
modules: []
wiring: []
workloads:
  - modules:
      - name: prog
        type: program_probe
YAML
wn_publish probe "$D/probe.yaml"
# A Pod that serves: the anchor answers PING itself, over a lane of its own.
cat >"$D/front.yaml" <<YAML
target: linux
tick_us: 1000
modules: []
wiring: []
workloads:
  - modules:
      - name: anchor
        type: redis_edge_anchor
        listen_port: $SERVE_PORT
    wiring:
      - from: "net:0"
        to: anchor
      - from: anchor
        to: "net:0"
YAML
wn_publish front "$D/front.yaml"
wn_registry "$REG_PORT"

echo "== 2. seed the operator's identity: SA key, cluster-admin binding, a ready node =="
wn_seed_identity

echo "== 3. render the node: the plane + the kubelet's graph-pod slice + its image plane, on one scoped store =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
wn_splice "$D/graph.yaml" node-1 "$D/blobs" "localhost:$REG_PORT" || fail "the splice did not apply"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
python3 - "$D/modules.bin" <<'PY' || fail "the node's image holds program_probe: the test would prove nothing"
import struct, sys
b = open(sys.argv[1], "rb").read()
n = b[5]
fnv = lambda s: __import__("functools").reduce(lambda h, c: ((h ^ c) * 0x01000193) & 0xffffffff, s, 0x811c9dc5)
hashes = {struct.unpack_from("<I", b, 16 + 16 * i)[0] for i in range(n)}
sys.exit(1 if fnv(b"program_probe") in hashes else 0)
PY
echo "   the node's image does not hold program_probe"

echo "== 4. run it; wait for the API over TLS =="
env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac

echo "== 5. a Pod of the workload image is created through the API =="
POD='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"nanocloud/probe:1"}]}}'
for _ in $(seq 90); do
  r="$(api POST /api/v1/namespaces/default/pods "$POD")"
  case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
done
case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST pod: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
echo "   uid $UID_"
dump() {
  local k
  for k in "/~node/node-1/pod-specs/$UID_" "/~node/node-1/image-requests/nanocloud_probe" \
           "/~node/node-1/image-manifests/nanocloud_probe" "/~node/node-1/image-rootfs/nanocloud_probe" \
           "/~node/node-1/pod-resolved/$UID_" "/~node/node-1/metal-sandboxes/$UID_" \
           "/~node/node-1/sandbox-status/$UID_" "/~node/node-1/pod-lifecycle-status/$UID_" "/podstatus/$UID_"; do
    printf '   %-56s %s\n' "$k" "$(nc_store_get "$STORE" "$k" 2>/dev/null | cut -c1-110)"
  done
  tail -5 "$D/reg.log" | sed 's/^/   registry: /'
  nc_store_keys "$STORE" 2>/dev/null | grep -aE "fetch-q|/image-|/blobs/" | head -12 | sed 's/^/   key: /'
  grep -a "http_fetch\]" "$D/run.log" | tail -3 | cut -c1-140
}

echo "== 6. the node pulls it and runs the modules it brought; the phase reaches the object =="
phase=""
for _ in $(seq 120); do
  r="$(api GET /api/v1/namespaces/default/pods/probe)"
  phase="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("status",{}).get("phase",""))
except Exception: print("")' 2>/dev/null)"
  [ "$phase" = "Running" ] && grep -aq "\[prog\]" "$D/run.log" && break
  sleep 1
done
echo "   phase=${phase:-}"
if [ "$phase" != "Running" ]; then dump; fail "the Pod never reached Running (phase='${phase:-}')"; fi
grep -aq "\[prog\]" "$D/run.log" || { dump; fail "Running, but program_probe never logged: the pack did not run"; }
echo "   $(grep -ao '\[prog\][^"]*' "$D/run.log" | head -1 | cut -c1-80)"
man="$(nc_store_get "$STORE" /~node/node-1/image-manifests/nanocloud_probe)"
case "$man" in *";kind=workload;"*) echo "   manifest: kind=workload" ;; *) fail "manifest not marked a workload image: $man" ;; esac
case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_")" in
  *"pack=nanocloud_probe"*) echo "   metal sandbox names its pack" ;;
  *) dump; fail "the metal sandbox does not name the pack" ;;
esac

echo "== 7. the Pod is deleted; its workload goes =="
r="$(api DELETE /api/v1/namespaces/default/pods/probe)"
case "$r" in 20[0-9]\|*) echo "   DELETE -> ${r%%|*}" ;; *) fail "DELETE pod: ${r:0:200}" ;; esac
st=""
for _ in $(seq 60); do
  st="$(nc_store_get "$STORE" "/~node/node-1/sandbox-status/$UID_" 2>/dev/null || true)"
  case "$st" in ""|*destroyed*) break ;; esac
  sleep 1
done
case "$st" in ""|*destroyed*) echo "   sandbox: ${st:-gone}" ;; *) dump; fail "the workload was not destroyed: $st" ;; esac

echo "== 8. a Pod that serves: its module listens on the node's network through a lane of its own =="
FRONT='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"front","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"front","image":"nanocloud/front:1"}]}}'
r="$(api POST /api/v1/namespaces/default/pods "$FRONT")"
case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST front: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
pong=""
for _ in $(seq 120); do
  pong="$(redis-cli -p "$SERVE_PORT" PING 2>/dev/null || true)"
  [ "$pong" = "PONG" ] && break
  sleep 1
done
[ "$pong" = "PONG" ] || { dump; fail "the serving Pod never answered on :$SERVE_PORT"; }
echo "   PING :$SERVE_PORT -> PONG (a pulled module, listening through its lane)"

echo "== E2E green: Pods of workload images pulled, pinned, staged as the modules they brought, run, serving, and deleted =="
