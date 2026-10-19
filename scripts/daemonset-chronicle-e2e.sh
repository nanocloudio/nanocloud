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
# count `expand` gave ReplicaSet. The decisions carry names and the
# template's digest only; the writer SPLICES the template's labels,
# annotations and spec (nodeName first) from the stored DaemonSet, whatever
# their size.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_scan store_effect store_big decision; do
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

# The chain as production runs it: the ds_ nodes of fluxor-controlplane.yaml
# and their baked params (checked against daemonset.uproc by
# chronicle-param-drift-e2e.sh).
python3 "$ROOT/scripts/extract-fixture.py" "$D/graph.yaml" - ds_ >/dev/null
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

echo "== 5. a REAL template (> 4 KiB): each Pod carries it whole, labelled as upstream labels them =="
BIG="$(python3 - <<'PYB'
import json
env = [{"name": "AGENT_SETTING_%02d" % i, "value": "value-%02d-abcdefghijklmnopqrstuvwxyz0123456789" % i} for i in range(48)]
tmpl = {"metadata": {"labels": {"app": "agent", "tier": "node"},
                     "annotations": {"example.com/note-%d" % i: "annotation %d of the agent" % i for i in range(8)}},
        "spec": {"containers": [{"name": "agent", "image": "agent", "env": env,
                                 "resources": {"requests": {"cpu": "50m", "memory": "32Mi"}}}],
                 "tolerations": [{"operator": "Exists"}]}}
assert len(json.dumps(tmpl, separators=(",", ":"))) >= 4096
print(json.dumps({"apiVersion": "apps/v1", "kind": "DaemonSet",
                  "metadata": {"name": "agent", "namespace": "default", "uid": "a9e5d000-0000-4000-8000-00000000a9e5"},
                  "spec": {"selector": {"matchLabels": {"app": "agent"}}, "template": tmpl}},
                 separators=(",", ":")))
PYB
)"
store_put "/daemonsets.apps/default/agent" "$BIG"
run 8
python3 - "$(store_last /pods/default/agent-node-1)" "$(store_last /pods/default/agent-node-3)" "$BIG" <<'PYC' || fail "the large DaemonSet's Pods are not its template whole: $(store_last /pods/default/agent-node-1 | head -c 400)"
import json, sys
a, b, ds = json.loads(sys.argv[1]), json.loads(sys.argv[2]), json.loads(sys.argv[3])
t = ds["spec"]["template"]
hashes = set()
for p, node in ((a, "node-1"), (b, "node-3")):
    m = p["metadata"]
    h = m["labels"].pop("controller-revision-hash")
    assert len(h) == 8, h
    hashes.add(h)
    assert m["labels"] == t["metadata"]["labels"], m["labels"]
    assert m["annotations"] == t["metadata"]["annotations"], m["annotations"]
    r = m["ownerReferences"][0]
    assert (r["kind"], r["name"], r["uid"], r["controller"]) == ("DaemonSet", "agent", ds["metadata"]["uid"], True), r
    spec = dict(p["spec"])
    assert spec.pop("nodeName") == node
    assert spec == t["spec"], spec
assert len(hashes) == 1
print("   controller-revision-hash", hashes.pop())
PYC
echo "   agent-node-1, agent-node-3: the whole template (48 env, tolerations), its labels + controller-revision-hash, its annotations, owned"
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":false}}'
run 6
[ -z "$(store_last /pods/default/agent-node-3)" ] || fail "agent-node-3 (a Pod past 4 KiB) was not pruned when node-3 went not-ready"
[ -n "$(store_last /pods/default/agent-node-1)" ] || fail "agent-node-1 should remain"
echo "   node-3 not ready: agent-node-3 (past 4 KiB) pruned, agent-node-1 kept"

echo "== E2E green: DaemonSet as params — readiness projection, join-expanded create-if-absent, prune on node loss =="
