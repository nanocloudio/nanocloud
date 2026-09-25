#!/usr/bin/env bash
# HPA as a Chronicle graph, STABILISED (T1.8): three reads and one arithmetic
# decision per metric sample — recommendation = ceil(current * cpu / target)
# clamped to [min, max] — folded by an aggregation (sliding window, max per
# target) and applied as a PATCH of spec.replicas on the target Deployment.
#
# Driven by a SYNTHETIC metric (scripts/support/hpa_synth.uproc) inside the
# graph: a sample every 200 ms, 90% CPU until SW ms, 20% after. One run,
# polled live. Proven: the scale-up reaches the ceiling at once; the scale-down
# is HELD for the window (without stabilisation it would follow the first low
# sample); then it walks to the floor. A target with no metric is never scaled.
# The window is shortened for the test (3000 ms, step 500 ms; the shipped graph
# uses 300000/60000).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision aggregation; do
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

WIN=3000
STEP=500
SW=6000
# hpa.uproc with the test's step (apply acts on the window ending within a step).
sed "s/enum STEP = [0-9]*;/enum STEP = $STEP;/; s/enum WIN = [0-9]*;/enum WIN = $WIN;/" "$ROOT/modules/app/_chronicle/hpa.uproc" >"$D/hpa.uproc"
DEC_RD="$(nc_decision "$D/hpa.uproc" rd)"
DEC_MT="$(nc_decision "$D/hpa.uproc" mt)"
DEC_RC="$(nc_decision "$D/hpa.uproc" rc)"
DEC_AP="$(nc_decision "$D/hpa.uproc" ap)"
DEC_SP="$(nc_decision "$ROOT/scripts/support/hpa_synth.uproc" sy_put)"
DEC_SA="$(nc_decision "$ROOT/scripts/support/hpa_synth.uproc" sy_adv)"
AGG="$(bash "$ROOT/scripts/hpa-agg.sh" "$WIN" "$STEP")"
for v in "$DEC_RD" "$DEC_MT" "$DEC_RC" "$DEC_AP" "$DEC_SP" "$DEC_SA" "$AGG"; do
  [ -n "$v" ] || { echo "FAIL: a param did not compile"; exit 1; }
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
    watch: "/hpa-metrics/"
    now: 1
  - name: depget
    type: store_effect
    decision: "${DEC_RD}"
    key_prefix: "/deployments.apps/"
    ints: "spec.replicas"
  - name: metget
    type: store_effect
    decision: "${DEC_MT}"
    key_prefix: "/hpa-metrics/"
    flat: 1
    ints: "cpu"
  - name: rec
    type: decision
    params:
      decision: "${DEC_RC}"
  - name: stab
    type: aggregation
    params:
      ir_def: "${AGG}"
  - name: patch
    type: store_effect
    decision: "${DEC_AP}"
    key_prefix: "/deployments.apps/"
  # The synthetic metric.
  - name: synth
    type: store_source
    prefix: "/synth/"
    ints: "next,hi,lo,sw,period"
    now: 1
    due: "next"
  - name: synth_put
    type: store_effect
    decision: "${DEC_SP}"
    key_prefix: "/hpa-metrics/"
  - name: synth_adv
    type: store_effect
    decision: "${DEC_SA}"
    key_prefix: "/synth/"
wiring:
  - from: hpas.status
    to: hpas.changes
  - from: hpas.record_out
    to: depget.request_in
  - from: depget.response_out
    to: metget.request_in
  - from: metget.response_out
    to: rec.record_in
  - from: rec.result_out
    to: stab.record_in
  - from: stab.result_out
    to: patch.request_in
  - from: synth.status
    to: synth.changes
  - from: synth.record_out
    to: synth_put.request_in
  - from: synth_put.response_out
    to: synth_adv.request_in
YAML

replicas() { store_last /deployments.apps/default/web | sed -n 's/.*"replicas":\([0-9]*\).*/\1/p'; }

echo "== 1. seed: web at 2 replicas, HPA 50% CPU in [1,10], a synthetic metric (90% until ${SW} ms, then 20%); api with no metric =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
store_put "/hpa/default/web" '{"metadata":{"name":"web"},"spec":{"scaleTargetRef":{"name":"web"},"minReplicas":1,"maxReplicas":10,"targetCPUUtilizationPercentage":50}}'
store_put "/synth/default/web" '{"next":0,"hi":90,"lo":20,"sw":'"$SW"',"period":200}'
store_put "/deployments.apps/default/api" '{"metadata":{"name":"api"},"spec":{"replicas":3,"template":{"spec":{}}}}'
store_put "/hpa/default/api" '{"metadata":{"name":"api"},"spec":{"scaleTargetRef":{"name":"api"},"minReplicas":1,"maxReplicas":10,"targetCPUUtilizationPercentage":50}}'

echo "== 2. one live run, polled =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
RUNTIME_PID=$!
T0=$(date +%s%3N)
TRACE=""
while :; do
  now=$(( $(date +%s%3N) - T0 ))
  r="$(replicas)"
  TRACE="$TRACE $now:$r"
  [ "$now" -gt 20000 ] && break
  sleep 0.1
done
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""
python3 - "$SW" "$WIN" "$STEP" $TRACE <<'PY' || fail "the stabilised loop"
import sys
sw, win, step = map(int, sys.argv[1:4])
pts = [tuple(map(int, t.split(":"))) for t in sys.argv[4:] if t.split(":")[1]]
first = lambda cond: next((t for t, r in pts if cond(t, r)), None)
up = first(lambda t, r: r == 10)
assert up is not None and up < sw, f"never reached the ceiling 10 before the switch: {pts[:40]}"
down = first(lambda t, r: t > up and r < 10)
assert down is not None, f"never scaled down: {pts[-20:]}"
# The first low sample lands just after the switch; stabilisation holds the
# ceiling until every high recommendation has left the window.
assert down >= sw + win - 2 * step, f"scaled down at {down} ms, before the window ({sw}+{win}): not stabilised"
floor = first(lambda t, r: r == 1)
assert floor is not None, f"never reached the floor 1: {pts[-20:]}"
print(f"   up to 10 at {up} ms; held until {down} ms (switch {sw}, window {win}); floor 1 at {floor} ms")
PY
case "$(store_last /deployments.apps/default/api)" in *'"replicas":3'*) : ;; *) fail "api was scaled without a metric";; esac
echo "   api (no metric): held at 3"
case "$(store_last /deployments.apps/default/web)" in *'"image":"nginx"'*) : ;; *) fail "the patch lost the rest of the Deployment";; esac

echo "== E2E green: HPA as params — per-sample recommendations, a stabilisation window on the aggregation engine, a PATCH =="
