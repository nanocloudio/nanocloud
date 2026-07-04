#!/usr/bin/env bash
# The scheduler as a Chronicle graph: least-loaded placement, one Pod per pass.
#   bind   the FIRST unbound Pod (where spec.nodeName=, index 0) JOIN
#          /ready-nodes/ picked by the fewest Pods bound to it -> PATCH
#          spec.nodeName; then the Scheduled event
# The argmin is a fold over the nodes, measured from the Pods themselves at
# pick time; it lives in the source (join_pick), and the decision sees one
# record with the winner in it.
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

D="$(mktemp -d /tmp/nc-schedchron-XXXXXX)"
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

for e in bd rc; do
  v="$(nc_decision "$ROOT/modules/app/_chronicle/scheduler.uproc" $e)"
  [ -n "$v" ] || { echo "FAIL: scheduler.uproc $e did not compile"; exit 1; }
  eval "DEC_$(echo $e | tr a-z A-Z)=\"$v\""
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  # the ready-nodes projection (the DaemonSet chain's, shared in production)
  - name: nodes
    type: store_source
    prefix: "/nodes/"
    paths: "status.ready"
  - name: ready
    type: decision
    params:
      decision: "$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" rdy)"
  - name: mark
    type: store_effect
    key_prefix: "/ready-nodes/"
  # bind: the first unbound pod, the least-loaded ready node
  - name: unbound
    type: store_source
    prefix: "/pods/"
    where: "spec.nodeName="
    count: 1
    join: "/ready-nodes/"
    join_pick: "mincount:/pods/:spec.nodeName"
    watch: "/ready-nodes/"
  - name: bind
    type: decision
    params:
      decision: "${DEC_BD}"
  - name: patch
    type: store_effect
    key_prefix: "/pods/"
  - name: record
    type: decision
    params:
      decision: "${DEC_RC}"
  - name: events
    type: store_effect
    key_prefix: "/events/"
wiring:
  - from: nodes.status
    to: nodes.changes
  - from: nodes.record_out
    to: ready.record_in
  - from: ready.result_out
    to: mark.request_in
    buffer_group: 1
  - from: unbound.status
    to: unbound.changes
  - from: unbound.record_out
    to: bind.record_in
  - from: bind.result_out
    to: patch.request_in
    buffer_group: 2
  - from: patch.response_out
    to: record.record_in
  - from: record.result_out
    to: events.request_in
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
node_of() { store_last "/pods/default/$1" | sed -n 's/.*"nodeName":"\([^"]*\)".*/\1/p'; }

echo "== 1. two ready nodes, one not; four unbound Pods and one pre-bound to the dead node =="
store_put "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":true}}'
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":false}}'
for p in a b c d; do store_put "/pods/default/pod-$p" '{"metadata":{"name":"pod-'$p'"},"spec":{"containers":[{"image":"x"}]}}'; done
store_put "/pods/default/pod-z" '{"metadata":{"name":"pod-z"},"spec":{"nodeName":"node-3","containers":[{"image":"x"}]}}'
run 10
for p in a b c d; do n="$(node_of pod-$p)"; [ -n "$n" ] || fail "pod-$p unbound"; [ "$n" != "node-3" ] || fail "pod-$p bound to a not-ready node"; echo "   pod-$p -> $n"; done
[ "$(node_of pod-z)" = "node-3" ] || fail "pod-z's binding must not be touched"
N1=0; N2=0; for p in a b c d; do case "$(node_of pod-$p)" in node-1) N1=$((N1+1));; node-2) N2=$((N2+1));; esac; done
[ "$N1" = 2 ] && [ "$N2" = 2 ] || fail "not spread: node-1=$N1 node-2=$N2"
echo "   spread 2/2 across the ready nodes; pod-z untouched"
case "$(store_last /events/default/pod-a.Scheduled.0)" in *'"reason":"Scheduled"'*) : ;; *) fail "no Scheduled event for pod-a";; esac
echo "   Scheduled events recorded"

echo "== 2. a new Pod lands on the less-loaded node after one is deleted =="
python3 - "$D/store.log" <<'PY2'
import struct, sys
path = sys.argv[1]; key = b"/pods/default/pod-a"
with open(path, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2); f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
PY2
store_put "/pods/default/pod-e" '{"metadata":{"name":"pod-e"},"spec":{"containers":[{"image":"x"}]}}'
run 8
E="$(node_of pod-e)"; [ -n "$E" ] || fail "pod-e unbound"
# pod-a is gone from its node; pod-e must land THERE (the load is measured from the Pods)
N1=0; N2=0; for p in b c d e; do case "$(node_of pod-$p)" in node-1) N1=$((N1+1));; node-2) N2=$((N2+1));; esac; done
[ "$N1" = 2 ] && [ "$N2" = 2 ] || fail "after a delete the spread should be 2/2 again: node-1=$N1 node-2=$N2"
echo "   pod-e -> $E; spread 2/2 again"

echo "== E2E green: the scheduler as params — an argmin join, one bind per pass, a spread that emerges =="
