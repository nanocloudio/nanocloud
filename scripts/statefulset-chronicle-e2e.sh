#!/usr/bin/env bash
# StatefulSet as a Chronicle graph: ordered bring-up (i waits for i-1 READY),
# highest-first teardown (i goes only when i+1 is absent), stable identity.
# The ReplicaSet chain with the gate asked of the NEIGHBOUR that matters:
# the predecessor in range, the successor for a surplus.
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

D="$(mktemp -d /tmp/nc-stschron-XXXXXX)"
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

write_graph() {
DEC_NB="$(nc_decision "$ROOT/modules/app/_chronicle/statefulset.uproc" nb)"
DEC_ASK="$(nc_decision "$ROOT/modules/app/_chronicle/statefulset.uproc" ask)"
DEC_ACT="$(nc_decision "$ROOT/modules/app/_chronicle/statefulset.uproc" act)"
for v in "$DEC_NB" "$DEC_ASK" "$DEC_ACT"; do
  [ -n "$v" ] || { echo "FAIL: a statefulset.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: store_source
    prefix: "/statefulsets.apps/"
    paths: "spec.template.spec,spec.replicas"
    expand: "spec.replicas"
    expand_tail: 2
    expand_over: "/pods/"
    watch: "/pods/"
  - name: nb
    type: decision
    params:
      decision: "${DEC_NB}"
  - name: nbget
    type: store_effect
    key_prefix: "/pods/"
    paths: "status.ready"
  - name: probe
    type: decision
    params:
      decision: "${DEC_ASK}"
  - name: selfget
    type: store_effect
    key_prefix: "/pods/"
  - name: act
    type: decision
    params:
      decision: "${DEC_ACT}"
  - name: apply
    type: store_effect
    key_prefix: "/pods/"
wiring:
  - from: store_source.status
    to: store_source.changes
  - from: store_source.record_out
    to: nb.record_in
  - from: nb.result_out
    to: nbget.request_in
    buffer_group: 1
  - from: nbget.response_out
    to: probe.record_in
  - from: probe.result_out
    to: selfget.request_in
    buffer_group: 2
  - from: selfget.response_out
    to: act.record_in
  - from: act.result_out
    to: apply.request_in
    buffer_group: 3
YAML
}
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
present() { [ -n "$(store_last "$1")" ]; }
absent()  { [ -z "$(store_last "$1")" ]; }
mark_ready() { # mark_ready <ordinal> — what the kubelet would write
  store_put "/pods/default/db-$1" '{"metadata":{"name":"db-'$1'","namespace":"default","ownerReferences":[{"kind":"StatefulSet","name":"db"}]},"spec":{"containers":[{"name":"db","image":"postgres"}]},"status":{"ready":true}}'
}
STS='{"metadata":{"name":"db","namespace":"default"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'

echo "== 1. replicas=3: only db-0 is created; db-1 waits for db-0 READY =="
store_put "/statefulsets.apps/default/db" "$STS"
write_graph
run 5
present /pods/default/db-0 || fail "db-0 should be created"
case "$(store_last /pods/default/db-0)" in *'"kind":"StatefulSet"'*'"image":"postgres"'*) : ;; *) fail "db-0 wrong: $(store_last /pods/default/db-0)";; esac
absent  /pods/default/db-1 || fail "db-1 must wait until db-0 is ready"
echo "   db-0 created, owned; db-1 held"

echo "== 2. db-0 ready -> db-1; db-2 still waits =="
mark_ready 0
run 5
present /pods/default/db-1 || fail "db-1 should follow a ready db-0"
absent  /pods/default/db-2 || fail "db-2 must wait until db-1 is ready"
echo "   db-1 created; db-2 held"

echo "== 3. db-1 ready -> db-2 =="
mark_ready 1
run 5
present /pods/default/db-2 || fail "db-2 should follow a ready db-1"
echo "   db-2 created"

echo "== 4. scale to 1: db-2 then db-1 go, HIGHEST FIRST, one per pass =="
mark_ready 2
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db","namespace":"default"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
run 6
present /pods/default/db-0 || fail "db-0 should remain"
absent  /pods/default/db-1 || fail "db-1 should be torn down"
absent  /pods/default/db-2 || fail "db-2 should be torn down"
python3 - "$D/store.log" <<'PY2' || exit 1
import struct, sys
d = open(sys.argv[1], "rb").read(); p = 0; order = []
while p + 15 <= len(d):
    rev, op, kl, vl = struct.unpack("<QBHI", d[p:p+15]); k = d[p+15:p+15+kl]
    if op == 2 and k.startswith(b"/pods/default/db-"): order.append(k.decode()[-1])
    p += 15 + kl + vl
if order[-2:] != ["2", "1"]:
    print("   FAIL teardown order was %s, want 2 then 1" % order); sys.exit(1)
print("   torn down in order: db-2, then db-1 — the successor gate, no budget")
PY2

echo "== 5. scale-down across a HOLE far above the count: 6 -> 1 with db-3 missing =="
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db","namespace":"default"},"spec":{"replicas":6,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
for i in 0 1 2 3 4; do mark_ready $i; done
run 6
present /pods/default/db-5 || fail "db-5 should exist at replicas=6"
python3 - "$D/store.log" <<'PY2'
import struct, sys, os
path = sys.argv[1]; key = b"/pods/default/db-3"
with open(path, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2); f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
PY2
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db","namespace":"default"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
run 8
present /pods/default/db-0 || fail "db-0 should remain"
for i in 1 2 4 5; do absent /pods/default/db-$i || fail "db-$i survived scale-down past the hole at db-3"; done
echo "   db-5, db-4 above the hole, then db-2, db-1: all collected; db-0 kept"

echo "== E2E green: StatefulSet as params — ordered bring-up on readiness, highest-first teardown, holes covered =="
