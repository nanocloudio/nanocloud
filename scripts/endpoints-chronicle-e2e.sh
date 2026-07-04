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

for e in ak sl ex pr dc; do
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
    type: store_source
    prefix: "/services/"
    paths: "spec.selector"
    join: "/pods/"
    join_scoped: 2
    join_match: "metadata.labels:spec.selector"
    join_paths: "status.ready,status.podIP,metadata.uid"
    watch: "/pods/,/probe-status/"
  - name: ask
    type: decision
    params:
      decision: "${DEC_AK}"
  - name: probe
    type: store_effect
    key_prefix: "/probe-status/"
    flat: 1
    paths: "ready"
  - name: slice
    type: decision
    params:
      decision: "${DEC_SL}"
  - name: slices
    type: store_effect
    key_prefix: "/endpointslices/"
  - name: sl_source
    type: store_source
    prefix: "/endpointslices/"
    watch: "/pods/"
  - name: exists
    type: decision
    params:
      decision: "${DEC_EX}"
  - name: podexists
    type: store_effect
    key_prefix: "/pods/"
  - name: prune
    type: decision
    params:
      decision: "${DEC_PR}"
  - name: collect
    type: store_effect
    key_prefix: "/endpointslices/"
  - name: svcs
    type: store_source
    prefix: "/services/"
    paths: "spec.selector"
    list_children: "/endpointslices/"
    child_sep: "/"
    watch: "/endpointslices/"
  - name: doc
    type: decision
    params:
      decision: "${DEC_DC}"
  - name: write
    type: store_effect
    key_prefix: "/endpoints/"
wiring:
  - from: pairs.status
    to: pairs.changes
  - from: pairs.record_out
    to: ask.record_in
  - from: ask.result_out
    to: probe.request_in
    buffer_group: 1
  - from: probe.response_out
    to: slice.record_in
  - from: slice.result_out
    to: slices.request_in
    buffer_group: 2
  - from: sl_source.status
    to: sl_source.changes
  - from: sl_source.record_out
    to: exists.record_in
  - from: exists.result_out
    to: podexists.request_in
    buffer_group: 3
  - from: podexists.response_out
    to: prune.record_in
  - from: prune.result_out
    to: collect.request_in
    buffer_group: 4
  - from: svcs.status
    to: svcs.changes
  - from: svcs.record_out
    to: doc.record_in
  - from: doc.result_out
    to: write.request_in
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
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","ready":true}}'
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","ready":false}}'
store_put "/pods/default/api-1" '{"metadata":{"name":"api-1","labels":{"app":"api","tier":"backend"}},"status":{"podIP":"10.0.0.3","ready":true}}'
store_put "/pods/default/db-1"  '{"metadata":{"name":"db-1","labels":{"app":"db"}},"status":{"podIP":"10.0.0.4","ready":true}}'
run 7
[ "$(store_last /endpoints/default/web)" = "web-1=10.0.0.1" ] || fail "web: '$(store_last /endpoints/default/web)'"
[ "$(store_last /endpoints/default/api)" = "api-1=10.0.0.3" ] || fail "api: '$(store_last /endpoints/default/api)' (db-1 must not match; multi-label selector)"
echo "   web: web-1=10.0.0.1 (web-2 not ready); api: api-1=10.0.0.3 (db-1 excluded)"

echo "== 2. web-2 becomes ready: included, in key order =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","ready":true}}'
run 6
[ "$(store_last /endpoints/default/web)" = "web-1=10.0.0.1,web-2=10.0.0.2" ] || fail "web after flip: '$(store_last /endpoints/default/web)'"
echo "   web: web-1=10.0.0.1,web-2=10.0.0.2"

echo "== 3. the probe runner overrides: web-1 has a uid and a failing probe; web-3 a passing one =="
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","uid":"u1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","ready":true}}'
store_put "/probe-status/u1" "live=1;ready=0"
store_put "/pods/default/web-3" '{"metadata":{"name":"web-3","uid":"u3","labels":{"app":"web"}},"status":{"podIP":"10.0.0.5","ready":false}}'
store_put "/probe-status/u3" "live=1;ready=1"
run 6
[ "$(store_last /endpoints/default/web)" = "web-2=10.0.0.2,web-3=10.0.0.5" ] || fail "probe override: '$(store_last /endpoints/default/web)'"
echo "   web: web-2=10.0.0.2,web-3=10.0.0.5 — the probe's verdict in AND out"

echo "== 4. a label change and a deletion: web-2 relabelled, web-3 deleted =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"other"}},"status":{"podIP":"10.0.0.2","ready":true}}'
store_del "/pods/default/web-3"
run 7
[ -z "$(store_last /endpointslices/default/web/web-3)" ] || fail "web-3's slice should be pruned"
[ -z "$(store_last /endpoints/default/web)" ] || fail "web should have no endpoints now: '$(store_last /endpoints/default/web)'"
echo "   web-2 dropped (selector), web-3 pruned (gone): the document removed"

echo "== E2E green: endpoints as params — a scoped join with a selector predicate, the probe override, a materialised list =="
