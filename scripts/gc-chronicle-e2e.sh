#!/usr/bin/env bash
# The ownerRef cascade as a Chronicle graph.
#
# garbage_collector's sweep, expressed as params: an object whose ownerRef no
# longer resolves is collected; an object with no ownerRef never is. This is
# read-then-decide — probe the owner, then act on the answer AND on the object
# that provoked it — so it rides on the carry-through seam rather than on an
# effect call/join Chronicle does not have.
#
#   store_source  watch /pods/, project metadata.ownerReferences.0.name
#                 and metadata.namespace
#   probe         decision: EXISTS /replicasets.apps/<ns>/<owner>, carrying the
#                 pod's own key; no ownerRef -> NOOP
#   owner         store_effect, key_prefix /replicasets.apps/
#   sweep         decision: 200 the owner lives -> NOOP; 404 -> DELETE the
#                 carried key; 204 is the NOOP coming back
#   collect       store_effect, key_prefix /pods/, delete-if-present
#
# The module does three things, and all three are here:
#   A. pods        <- replicasets   the chain above
#   B. replicasets <- deployments   the SAME chain re-parameterised (prefixes)
#   C. the Events count-cap         store_source in `count` mode stamps each
#                                   record with its listing position and the
#                                   total; `cap` deletes below the survivors'
#                                   floor. A fold over a set, as a per-record
#                                   decision — because the source did the count.
#
# No nanocloud module knows what an ownerRef is. The three that run move bytes.
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

D="$(mktemp -d /tmp/nc-gcchron-XXXXXX)"
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
store_del() { # store_del <key> — append a tombstone (log op 2, no value)
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys, os
path, key = sys.argv[1], sys.argv[2].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
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

echo "== 1. seed: one live owner, one orphan, one object with no owner =="
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":1}}'
store_put "/pods/default/web-0" '{"metadata":{"name":"web-0","namespace":"default","ownerReferences":[{"kind":"ReplicaSet","name":"web"}]},"spec":{}}'
store_put "/pods/default/orphan-0" '{"metadata":{"name":"orphan-0","namespace":"default","ownerReferences":[{"kind":"ReplicaSet","name":"gone"}]},"spec":{}}'
store_put "/pods/default/standalone" '{"metadata":{"name":"standalone","namespace":"default"},"spec":{}}'
# B: a ReplicaSet whose Deployment lives, one whose Deployment is gone. The
# live owner `web` above has no ownerRef and must survive as a top-level object.
store_put "/deployments.apps/default/api" '{"metadata":{"name":"api","namespace":"default"},"spec":{"replicas":1}}'
store_put "/replicasets.apps/default/api-1" '{"metadata":{"name":"api-1","namespace":"default","ownerReferences":[{"kind":"Deployment","name":"api"}]},"spec":{"replicas":1}}'
store_put "/replicasets.apps/default/old-1" '{"metadata":{"name":"old-1","namespace":"default","ownerReferences":[{"kind":"Deployment","name":"old"}]},"spec":{"replicas":1}}'
# C: 230 Events — 30 over the cap. Zero-padded so key order is numeric order,
# which makes "the lowest 30 go, the highest 200 stay" checkable by name.
for i in $(seq -f '%03g' 1 230); do
  store_put "/events/default/pod-$i.Scheduled.0" '{"metadata":{"name":"pod-'$i'.Scheduled.0","namespace":"default"},"reason":"Scheduled","count":1}'
done

echo "== 2. write the graph =="
DEC_PROBE="$(nc_decision "$ROOT/modules/app/_chronicle/gc.uproc" ask)"
[ -n "$DEC_PROBE" ] || { echo "FAIL: could not compile gc.uproc ask"; exit 1; }
DEC_SWEEP="$(nc_decision "$ROOT/modules/app/_chronicle/gc.uproc" act)"
[ -n "$DEC_SWEEP" ] || { echo "FAIL: could not compile gc.uproc act"; exit 1; }
DEC_CAP="$(nc_decision "$ROOT/modules/app/_chronicle/gc.uproc" evcap)"
[ -n "$DEC_CAP" ] || { echo "FAIL: could not compile gc.uproc evcap"; exit 1; }
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: store_source
    prefix: "/pods/"
    paths: "metadata.ownerReferences.0.name"
    # The cascade: when a ReplicaSet is collected, its Pods are orphans NOW,
    # and nothing under /pods/ changed to say so. Watch the owners' prefix.
    watch: "/replicasets.apps/"
  - name: probe
    type: decision
    params:
      decision: "${DEC_PROBE}"
  - name: owner
    type: store_effect
    key_prefix: "/replicasets.apps/"
  - name: sweep
    type: decision
    params:
      decision: "${DEC_SWEEP}"
  - name: collect
    type: store_effect
    key_prefix: "/pods/"
  # B — the same two decisions over different prefixes. Nothing recompiled.
  - name: rs_source
    type: store_source
    prefix: "/replicasets.apps/"
    paths: "metadata.ownerReferences.0.name"
    watch: "/deployments.apps/"
  - name: rs_probe
    type: decision
    params:
      decision: "${DEC_PROBE}"
  - name: rs_owner
    type: store_effect
    key_prefix: "/deployments.apps/"
  - name: rs_sweep
    type: decision
    params:
      decision: "${DEC_SWEEP}"
  - name: rs_collect
    type: store_effect
    key_prefix: "/replicasets.apps/"
  # C — the cap. count: 1 is the whole difference.
  - name: ev_source
    type: store_source
    prefix: "/events/"
    count: 1
  - name: ev_cap
    type: decision
    params:
      decision: "${DEC_CAP}"
  - name: ev_collect
    type: store_effect
    key_prefix: "/events/"
  - name: dbg
    type: debug
    mode: 1
wiring:
  - from: store_source.status
    to: store_source.changes
  - from: store_source.record_out
    to: probe.record_in
  - from: probe.result_out
    to: owner.request_in
    buffer_group: 1
  - from: owner.response_out
    to: sweep.record_in
  - from: sweep.result_out
    to: collect.request_in
    buffer_group: 2
  - from: collect.response_out
    to: dbg.data
    buffer_group: 3
  - from: rs_source.status
    to: rs_source.changes
  - from: rs_source.record_out
    to: rs_probe.record_in
  - from: rs_probe.result_out
    to: rs_owner.request_in
    buffer_group: 4
  - from: rs_owner.response_out
    to: rs_sweep.record_in
  - from: rs_sweep.result_out
    to: rs_collect.request_in
    buffer_group: 5
  - from: ev_source.status
    to: ev_source.changes
  - from: ev_source.record_out
    to: ev_cap.record_in
  - from: ev_cap.result_out
    to: ev_collect.request_in
    buffer_group: 6
YAML

echo "== 3. run =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
sleep 5
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""

echo "== 4. only the orphan is collected =="
[ -n "$(store_last /pods/default/web-0)" ]     || fail "web-0 was collected but its owner is alive"
echo "   web-0      kept      (owner /replicasets.apps/default/web resolves)"
[ -z "$(store_last /pods/default/orphan-0)" ]  || fail "orphan-0 should have been collected (owner 'gone' does not resolve)"
echo "   orphan-0   COLLECTED (owner 'gone' does not resolve)"
[ -n "$(store_last /pods/default/standalone)" ] || fail "standalone has no ownerRef and must never be collected"
echo "   standalone kept      (no ownerRef — user-owned, never collected)"
[ -n "$(store_last /replicasets.apps/default/web)" ] || fail "the owner itself must not be touched"
echo "   the owner itself untouched"

echo "== 5. B: the second sweep, same decisions, different prefixes =="
[ -n "$(store_last /replicasets.apps/default/api-1)" ] || fail "api-1 collected but its Deployment lives"
[ -z "$(store_last /replicasets.apps/default/old-1)" ]  || fail "old-1 should be collected (Deployment 'old' is gone)"
[ -n "$(store_last /replicasets.apps/default/web)" ]    || fail "web has no ownerRef and must survive the ReplicaSet sweep too"
echo "   api-1 kept, old-1 COLLECTED, web (no ownerRef) kept"

echo "== 6. C: the Events cap — 230 in, at most 200 survive, the LOWEST evicted =="
# Not "exactly 200": the count pass of a re-scan can run while the previous
# pass's deletes are still in flight through the effect channel, so it may
# count one or two that are already condemned and evict that many more. The
# cap is a growth bound on advisory objects and the module's own is coarse
# too; what must hold is the bound, the direction, and convergence.
python3 - "$D/store.log" <<'PY2' || exit 1
import struct, sys
d = open(sys.argv[1], "rb").read(); p = 0; live = set()
while p + 15 <= len(d):
    rev, op, kl, vl = struct.unpack("<QBHI", d[p:p+15])
    if p + 15 + kl + vl > len(d): break
    k = d[p+15:p+15+kl]
    if k.startswith(b"/events/default/"):
        live.add(k) if op == 1 else live.discard(k)
    p += 15 + kl + vl
names = sorted(live)
n = len(names)
if not (190 <= n <= 200):
    print(f"   FAIL {n} events survive, want at most 200 and no fewer than 190"); sys.exit(1)
lo, hi = names[0].decode(), names[-1].decode()
first = int(lo.rsplit("pod-", 1)[1][:3])
if "pod-230." not in hi or first != 231 - n:
    print(f"   FAIL survivors are not the highest {n} keys: {lo} .. {hi}"); sys.exit(1)
print(f"   {n} survive: {lo.rsplit('/',1)[1]} .. {hi.rsplit('/',1)[1]} — the lowest {230-n} evicted, as the module does")
PY2

echo "== 7. the cascade: delete the Deployment, and both tiers below it go =="
# Deleting a Deployment collects its ReplicaSet on one pass and the ReplicaSet's
# Pods on the next — B wakes on /deployments.apps/, A wakes on /replicasets.apps/.
store_put "/pods/default/api-1-0" '{"metadata":{"name":"api-1-0","namespace":"default","ownerReferences":[{"kind":"ReplicaSet","name":"api-1"}]},"spec":{}}'
store_del "/deployments.apps/default/api"
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
RUNTIME_PID=$!
sleep 5
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""
[ -z "$(store_last /replicasets.apps/default/api-1)" ] || fail "api-1 should be collected once its Deployment is gone"
[ -z "$(store_last /pods/default/api-1-0)" ]           || fail "api-1-0 should be collected once its ReplicaSet is gone"
echo "   Deployment gone -> api-1 collected -> api-1-0 collected: two tiers, two chains, no module"

echo "== E2E green: the ownerRef cascade as a compiled decision — both sweeps and the Events cap, same verdicts as garbage_collector, no ownerRef knowledge in any .fmod =="
