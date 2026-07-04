#!/usr/bin/env bash
# DaemonSet as a Chronicle graph: one Pod per ready Node, as params. Three
# chains:
#
#   ready   /nodes/ -> /ready-nodes/<node>       the readiness projection
#   place   /daemonsets.apps/ JOIN /ready-nodes/ one record per (ds, node);
#           EXISTS the pod; create-if-absent, node-pinned
#   prune   /pods/ owned by a DaemonSet; GET /ready-nodes/<nodeName>;
#           404 -> DELETE
#
# The cross product is the SOURCE's (`join`) — the set-shaped sibling of the
# count `expand` gave ReplicaSet — and nodeName is spliced into the template
# through a `.*` members projection: the VM cannot open a brace, the source
# can, once.
# so "spec equal and hash equal" is "document equal".
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

D="$(mktemp -d /tmp/nc-dschron-XXXXXX)"
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

DEC_RDY="$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" rdy)"
DEC_PRB="$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" prb)"
DEC_MK="$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" mk)"
DEC_ASK="$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" ask)"
DEC_PRN="$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" prn)"
for v in "$DEC_RDY" "$DEC_PRB" "$DEC_MK" "$DEC_ASK" "$DEC_PRN"; do
  [ -n "$v" ] || { echo "FAIL: a daemonset.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  # ready: the readiness projection
  - name: nodes
    type: store_source
    prefix: "/nodes/"
    paths: "status.ready"
  - name: ready
    type: decision
    params:
      decision: "${DEC_RDY}"
  - name: mark
    type: store_effect
    key_prefix: "/ready-nodes/"
  # place: DaemonSets x ready Nodes
  - name: ds_source
    type: store_source
    prefix: "/daemonsets.apps/"
    paths: "spec.template.spec.*"
    join: "/ready-nodes/"
    watch: "/ready-nodes/,/pods/"
  - name: probe
    type: decision
    params:
      decision: "${DEC_PRB}"
  - name: exists
    type: store_effect
    key_prefix: "/pods/"
  - name: create
    type: decision
    params:
      decision: "${DEC_MK}"
  - name: apply
    type: store_effect
    key_prefix: "/pods/"
  # prune: DaemonSet Pods on Nodes no longer ready
  - name: pods
    type: store_source
    prefix: "/pods/"
    paths: "metadata.ownerReferences.0.kind,spec.nodeName"
    watch: "/ready-nodes/"
  - name: ask
    type: decision
    params:
      decision: "${DEC_ASK}"
  - name: known
    type: store_effect
    key_prefix: "/ready-nodes/"
  - name: prune
    type: decision
    params:
      decision: "${DEC_PRN}"
  - name: collect
    type: store_effect
    key_prefix: "/pods/"
wiring:
  - from: nodes.status
    to: nodes.changes
  - from: nodes.record_out
    to: ready.record_in
  - from: ready.result_out
    to: mark.request_in
    buffer_group: 1
  - from: ds_source.status
    to: ds_source.changes
  - from: ds_source.record_out
    to: probe.record_in
  - from: probe.result_out
    to: exists.request_in
    buffer_group: 2
  - from: exists.response_out
    to: create.record_in
  - from: create.result_out
    to: apply.request_in
    buffer_group: 3
  - from: pods.status
    to: pods.changes
  - from: pods.record_out
    to: ask.record_in
  - from: ask.result_out
    to: known.request_in
    buffer_group: 4
  - from: known.response_out
    to: prune.record_in
  - from: prune.result_out
    to: collect.request_in
    buffer_group: 5
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
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== 1. a DaemonSet and three Nodes, two ready =="
store_put "/daemonsets.apps/default/log" '{"metadata":{"name":"log"},"spec":{"template":{"spec":{"containers":[{"name":"log","image":"fluentd"}]}}}}'
store_put "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":true}}'
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":false}}'
# a Pod that is NOT a DaemonSet's, on the not-ready node: prune must leave it
store_put "/pods/default/web-0" '{"metadata":{"name":"web-0","namespace":"default","ownerReferences":[{"kind":"ReplicaSet","name":"web"}]},"spec":{"nodeName":"node-3"}}'
run 6
[ "$(store_last /ready-nodes/node-1)" = "1" ] && [ "$(store_last /ready-nodes/node-2)" = "1" ] || fail "ready projection missing"
[ -z "$(store_last /ready-nodes/node-3)" ] || fail "node-3 is not ready and must not be projected"
echo "   /ready-nodes/: node-1, node-2 (node-3 absent)"
P1="$(store_last /pods/default/log-node-1)"
has "$P1" '"name":"log-node-1"' '"namespace":"default"' '"kind":"DaemonSet"' '"name":"log"' '"nodeName":"node-1"' '"image":"fluentd"' \
  || fail "log-node-1 wrong: '$P1'"
has "$(store_last /pods/default/log-node-2)" '"nodeName":"node-2"' '"image":"fluentd"' || fail "log-node-2 missing"
[ -z "$(store_last /pods/default/log-node-3)" ] || fail "log-node-3 must not exist (node-3 not ready)"
python3 -c "import json,sys; d=json.loads(sys.argv[1]); assert d['spec']['nodeName']=='node-1' and d['spec']['containers'][0]['image']=='fluentd'" "$P1" \
  || fail "log-node-1 is not valid JSON with nodeName spliced into spec: '$P1'"
echo "   log-node-1, log-node-2 created, node-pinned, template spliced (valid JSON); log-node-3 absent"
[ -n "$(store_last /pods/default/web-0)" ] || fail "prune touched a Pod that is not a DaemonSet's"
echo "   web-0 (a ReplicaSet's Pod on the dead node) untouched"

echo "== 2. node-2 goes not-ready: its daemon Pod is pruned; node-1's remains =="
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":false}}'
run 6
[ -z "$(store_last /ready-nodes/node-2)" ] || fail "node-2 still projected ready"
[ -z "$(store_last /pods/default/log-node-2)" ] || fail "log-node-2 should be pruned"
[ -n "$(store_last /pods/default/log-node-1)" ] || fail "log-node-1 should remain"
echo "   log-node-2 pruned, log-node-1 kept"

echo "== 3. node-3 becomes ready: a daemon Pod appears on it =="
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":true}}'
run 6
has "$(store_last /pods/default/log-node-3)" '"nodeName":"node-3"' || fail "log-node-3 should be created once node-3 is ready"
echo "   log-node-3 created"

echo "== 4. a daemon Pod deleted out of band is recreated (create-if-absent, woken by /pods/) =="
python3 - "$D/store.log" <<'PY2'
import struct, sys, os
path = sys.argv[1]; key = b"/pods/default/log-node-1"
with open(path, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
PY2
run 6
has "$(store_last /pods/default/log-node-1)" '"nodeName":"node-1"' || fail "log-node-1 not recreated"
echo "   log-node-1 recreated"

echo "== E2E green: DaemonSet as params — readiness projection, join-expanded create-if-absent, prune on node loss =="
