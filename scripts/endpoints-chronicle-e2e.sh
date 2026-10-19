#!/usr/bin/env bash
# Endpoints as a Chronicle graph. Three chains:
#   slice   Services JOIN the namespace's Pods, the label selector as the
#           join predicate; the probe runner's readiness overrides the static
#           field when it has a verdict -> /endpointslices/<ns>/<svc>/<pod>
#   prune   a slice whose Pod is gone
#   doc     list_children over the slices -> /endpoints/<ns>/<svc> =
#           "<pod>=<ip>,…" — the document the four consumers parse, unchanged
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_scan store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-epchron-XXXXXX)"
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

for e in ak sl ex pr dc gn; do
  v="$(nc_decision "$ROOT/modules/app/_chronicle/endpoints.uproc" $e)"
  [ -n "$v" ] || { echo "FAIL: endpoints.uproc $e did not compile"; exit 1; }
  eval "DEC_$(echo $e | tr a-z A-Z)=\"$v\""
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: pairs
    type: store_scan
    prefix: "/services/"
    paths: "spec.selector"
    join: "/pods/"
    join_scoped: 2
    join_match: "metadata.labels:spec.selector"
    join_paths: "status.conditions[type=Ready].status,status.podIP,metadata.uid"
    watch: "/pods/,/probe-status/"
  - name: probe
    type: store_effect
    decision: {hex: "${DEC_AK}"}
    key_prefix: "/probe-status/"
    flat: 1
    paths: "ready"
  - name: slices
    type: store_effect
    decision: {hex: "${DEC_SL}"}
    key_prefix: "/endpointslices/"
  - name: sl_source
    type: store_source
    prefix: "/endpointslices/"
    related: "/services/^1:metadata.name"
    watch: "/pods/,/services/"
  - name: podexists
    type: store_effect
    decision: {hex: "${DEC_EX}"}
    key_prefix: "/pods/"
  - name: collect
    type: store_effect
    decision: {hex: "${DEC_PR}"}
    key_prefix: "/endpointslices/"
  - name: svcs
    type: store_source
    prefix: "/services/"
    paths: "spec.selector"
    list_children: "/endpointslices/"
    child_sep: "/"
    watch: "/endpointslices/"
  - name: write
    type: store_effect
    decision: {hex: "${DEC_DC}"}
    key_prefix: "/endpoints/"
  - name: docs
    type: store_source
    rev: 1
    prefix: "/endpoints/"
    related: "/services/:metadata.name"
    watch: "/services/"
  - name: gone
    type: store_effect
    decision: {hex: "${DEC_GN}"}
    key_prefix: "/endpoints/"
wiring:
  - from: pairs.status
    to: pairs.changes
  - from: pairs.record_out
    to: probe.request_in
  - from: probe.response_out
    to: slices.request_in
  - from: sl_source.status
    to: sl_source.changes
  - from: sl_source.record_out
    to: podexists.request_in
  - from: podexists.response_out
    to: collect.request_in
  - from: svcs.status
    to: svcs.changes
  - from: svcs.record_out
    to: write.request_in
  - from: docs.status
    to: docs.changes
  - from: docs.record_out
    to: gone.request_in
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
store_del() {
  python3 - "$D/store.log" "$1" <<'PY2'
import struct, sys
path, key = sys.argv[1], sys.argv[2].encode()
with open(path, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2); f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
PY2
}

echo "== 1. two Services, four Pods (web-2 not ready, db-1 matches nothing) =="
store_put "/services/default/web" '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"}}}'
store_put "/services/default/api" '{"metadata":{"name":"api"},"spec":{"selector":{"app":"api","tier":"backend"}}}'
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","conditions":[{"type":"Ready","status":"True"}]}}'
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","conditions":[{"type":"Ready","status":"False"}]}}'
store_put "/pods/default/api-1" '{"metadata":{"name":"api-1","labels":{"app":"api","tier":"backend"}},"status":{"podIP":"10.0.0.3","conditions":[{"type":"Ready","status":"True"}]}}'
store_put "/pods/default/db-1"  '{"metadata":{"name":"db-1","labels":{"app":"db"}},"status":{"podIP":"10.0.0.4","conditions":[{"type":"Ready","status":"True"}]}}'
run 7
[ "$(store_last /endpoints/default/web)" = "web-1=10.0.0.1" ] || fail "web: '$(store_last /endpoints/default/web)'"
[ "$(store_last /endpoints/default/api)" = "api-1=10.0.0.3" ] || fail "api: '$(store_last /endpoints/default/api)' (db-1 must not match; multi-label selector)"
echo "   web: web-1=10.0.0.1 (web-2 not ready); api: api-1=10.0.0.3 (db-1 excluded)"

echo "== 2. web-2 becomes ready: included, in key order =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","conditions":[{"type":"Ready","status":"True"}]}}'
run 6
[ "$(store_last /endpoints/default/web)" = "web-1=10.0.0.1,web-2=10.0.0.2" ] || fail "web after flip: '$(store_last /endpoints/default/web)'"
echo "   web: web-1=10.0.0.1,web-2=10.0.0.2"

echo "== 3. the probe runner overrides: web-1 has a uid and a failing probe; web-3 a passing one =="
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","uid":"u1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","conditions":[{"type":"Ready","status":"True"}]}}'
store_put "/probe-status/u1" "live=1;ready=0"
store_put "/pods/default/web-3" '{"metadata":{"name":"web-3","uid":"u3","labels":{"app":"web"}},"status":{"podIP":"10.0.0.5","conditions":[{"type":"Ready","status":"False"}]}}'
store_put "/probe-status/u3" "live=1;ready=1"
run 6
[ "$(store_last /endpoints/default/web)" = "web-2=10.0.0.2,web-3=10.0.0.5" ] || fail "probe override: '$(store_last /endpoints/default/web)'"
echo "   web: web-2=10.0.0.2,web-3=10.0.0.5 — the probe's verdict in AND out"

echo "== 4. a label change and a deletion: web-2 relabelled, web-3 deleted =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"other"}},"status":{"podIP":"10.0.0.2","conditions":[{"type":"Ready","status":"True"}]}}'
store_del "/pods/default/web-3"
run 7
[ -z "$(store_last /endpointslices/default/web/web-3)" ] || fail "web-3's slice should be pruned"
[ -z "$(store_last /endpoints/default/web)" ] || fail "web should have no endpoints now: '$(store_last /endpoints/default/web)'"
echo "   web-2 dropped (selector), web-3 pruned (gone): the document removed"

echo "== 5. a Service is deleted: its slices and its Endpoints document go with it =="
[ -n "$(store_last /endpointslices/default/api/api-1)" ] || fail "api-1's slice is missing before the delete"
store_del "/services/default/api"
run 7
[ -z "$(store_last /endpointslices/default/api/api-1)" ] || fail "a deleted Service's slice was left behind"
[ -z "$(store_last /endpoints/default/api)" ] || fail "a deleted Service's Endpoints document was left behind: '$(store_last /endpoints/default/api)'"
echo "   api deleted: /endpointslices/default/api/api-1 and /endpoints/default/api removed"

echo "== 6. a Pod past 4 KiB (a real spec) is selected like any other =="
BIGPOD="$(python3 - <<'PYB'
import json
env = [{"name": "SETTING_%02d" % i, "value": "value-%02d-abcdefghijklmnopqrstuvwxyz0123456789" % i} for i in range(64)]
pod = {"metadata": {"name": "big-1", "uid": "ub1", "labels": {"app": "big", "tier": "web"},
                    "annotations": {"example.com/note-%d" % i: "annotation %d" % i for i in range(8)}},
       "spec": {"containers": [{"name": "c", "image": "busybox", "env": env}]},
       "status": {"podIP": "10.0.0.9", "conditions": [{"type": "Ready", "status": "True"}]}}
s = json.dumps(pod, separators=(",", ":"))
assert len(s) > 4096
print(s)
PYB
)"
store_put "/services/default/big" '{"metadata":{"name":"big"},"spec":{"selector":{"app":"big"}}}'
store_put "/pods/default/big-1" "$BIGPOD"
store_put "/pods/default/big-2" '{"metadata":{"name":"big-2","labels":{"app":"big"}},"status":{"podIP":"10.0.0.10","conditions":[{"type":"Ready","status":"True"}]}}'
run 7
[ "$(store_last /endpoints/default/big)" = "big-1=10.0.0.9,big-2=10.0.0.10" ] || fail "big: '$(store_last /endpoints/default/big)' (big-1 is $(printf '%s' "$BIGPOD" | wc -c) bytes)"
echo "   big: big-1=10.0.0.9 ($(printf '%s' "$BIGPOD" | wc -c) bytes),big-2=10.0.0.10"

echo "== E2E green: endpoints as params — a scoped join with a selector predicate, the probe override, a materialised list =="
