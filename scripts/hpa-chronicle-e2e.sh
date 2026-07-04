#!/usr/bin/env bash
# HPA as a Chronicle graph: the closed loop, as params. Three reads and one
# arithmetic decision — desired = ceil(current * cpu / target) clamped to
# [min, max] — ending in a PATCH of spec.replicas on the target Deployment.
# The VM computes the number (it now divides); the connector renders it and
# places it in the document. Integer projections on both sides, so numbers
# are compared as numbers.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-hpachron-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}
store_last() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}
# how many records were written for <key> (PUT-if-changed must not spend revisions)
store_writes() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}

DEC_RD="$(nc_decision "$ROOT/modules/app/_chronicle/hpa.uproc" rd)"
DEC_MT="$(nc_decision "$ROOT/modules/app/_chronicle/hpa.uproc" mt)"
DEC_SC="$(nc_decision "$ROOT/modules/app/_chronicle/hpa.uproc" sc)"
for v in "$DEC_RD" "$DEC_MT" "$DEC_SC"; do
  [ -n "$v" ] || { echo "FAIL: an hpa.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: hpas
    type: store_source
    prefix: "/hpa/"
    paths: "spec.scaleTargetRef.name"
    ints: "spec.minReplicas,spec.maxReplicas,spec.targetCPUUtilizationPercentage"
    # The metric wakes it; its OWN write must not. Watching the Deployment
    # would re-evaluate against the new count with the old metric, and climb
    # to the ceiling one pass at a time. So the watches are /hpa/ and
    # /hpa-metrics/, and nothing else.
    watch: "/hpa-metrics/"
  - name: dep
    type: decision
    params:
      decision: "${DEC_RD}"
  - name: depget
    type: store_effect
    key_prefix: "/deployments.apps/"
    ints: "spec.replicas"
  - name: met
    type: decision
    params:
      decision: "${DEC_MT}"
  - name: metget
    type: store_effect
    key_prefix: "/hpa-metrics/"
    flat: 1
    ints: "cpu"
  - name: scale
    type: decision
    params:
      decision: "${DEC_SC}"
  - name: patch
    type: store_effect
    key_prefix: "/deployments.apps/"
wiring:
  - from: hpas.status
    to: hpas.changes
  - from: hpas.record_out
    to: dep.record_in
  - from: dep.result_out
    to: depget.request_in
    buffer_group: 1
  - from: depget.response_out
    to: met.record_in
  - from: met.result_out
    to: metget.request_in
    buffer_group: 2
  - from: metget.response_out
    to: scale.record_in
  - from: scale.result_out
    to: patch.request_in
    buffer_group: 3
YAML

run() { # run <seconds>
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  sleep "$1"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}
replicas() { store_last /deployments.apps/default/web | sed -n 's/.*"replicas":\([0-9]*\).*/\1/p'; }

echo "== 1. Deployment replicas=2, HPA 50% CPU in [1,10], load 90%: ceil(2*90/50)=4 =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
store_put "/hpa/default/web-hpa" '{"metadata":{"name":"web-hpa"},"spec":{"scaleTargetRef":{"name":"web"},"minReplicas":1,"maxReplicas":10,"targetCPUUtilizationPercentage":50}}'
store_put "/hpa-metrics/default/web" "cpu=90"
run 5
[ "$(replicas)" = "4" ] || fail "want replicas=4, got '$(store_last /deployments.apps/default/web)'"
case "$(store_last /deployments.apps/default/web)" in *'"image":"nginx"'*) : ;; *) fail "the PATCH lost the rest of the document";; esac
echo "   replicas 2 -> 4; the rest of the Deployment intact"

echo "== 2. load falls to 20%: ceil(4*20/50)=2 =="
store_put "/hpa-metrics/default/web" "cpu=20"
run 5
[ "$(replicas)" = "2" ] || fail "want replicas=2, got $(replicas)"
echo "   replicas 4 -> 2"

echo "== 3. clamps: 5% load would want 1 (floor holds at minReplicas=1); 900% wants 36, ceiling 10 =="
store_put "/hpa-metrics/default/web" "cpu=5"
run 5
[ "$(replicas)" = "1" ] || fail "want floor 1, got $(replicas)"
store_put "/hpa-metrics/default/web" "cpu=900"
run 5
[ "$(replicas)" = "10" ] || fail "want ceiling 10, got $(replicas)"
echo "   floor 1, ceiling 10"

echo "== 4. steady state spends no revision: at target, no PATCH =="
store_put "/hpa-metrics/default/web" "cpu=50"
run 4
W1="$(python3 - "$D/store.log" <<'PY2'
import struct,sys
d=open(sys.argv[1],"rb").read();p=n=0
while p+15<=len(d):
    rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15])
    if d[p+15:p+15+kl]==b"/deployments.apps/default/web" and op==1: n+=1
    p+=15+kl+vl
print(n)
PY2
)"
run 4
W2="$(python3 - "$D/store.log" <<'PY2'
import struct,sys
d=open(sys.argv[1],"rb").read();p=n=0
while p+15<=len(d):
    rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15])
    if d[p+15:p+15+kl]==b"/deployments.apps/default/web" and op==1: n+=1
    p+=15+kl+vl
print(n)
PY2
)"
[ "$W1" = "$W2" ] || fail "a quiet pass rewrote the Deployment ($W1 -> $W2)"
echo "   $W1 writes before, $W2 after"

echo "== 5. no metric yet: hold =="
store_put "/deployments.apps/default/api" '{"metadata":{"name":"api"},"spec":{"replicas":3,"template":{"spec":{}}}}'
store_put "/hpa/default/api-hpa" '{"metadata":{"name":"api-hpa"},"spec":{"scaleTargetRef":{"name":"api"},"minReplicas":1,"maxReplicas":10,"targetCPUUtilizationPercentage":50}}'
run 4
case "$(store_last /deployments.apps/default/api)" in *'"replicas":3'*) : ;; *) fail "scaled without a metric";; esac
echo "   api held at 3"

echo "== E2E green: HPA as params — three reads, integer arithmetic with division, a PATCH; clamped and quiet at steady state =="
