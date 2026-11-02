#!/usr/bin/env bash
# Endpoints as a Chronicle graph. Five chains:
#   slice   Services JOIN the namespace's Pods, the label selector as the
#           join predicate; the probe runner's readiness overrides the static
#           field when it has a verdict; the Pod read for its grace, its node
#           for its zone, the row for its generation ->
#           /endpoint-rows/<ns>/<svc>/<pod> = ip=…;state=<ready|notready|
#           serving|terminating>;zone=…;node=…;gen=…;af=…;uid=…[;at=…;grace=…]
#   prune   a row whose Pod is gone drains (terminating, its grace; a
#           notready one goes), one whose Service is gone goes
#   expiry  a draining row is deleted when its grace has run (ttl.plan)
#   doc     list_children over the rows -> /endpoint-lists/<ns>/<svc> =
#           "<pod>=<row>,…" — the fold the Service plane parses
#   api     store_stamped EP_DOCS -> the API's JSON: a discovery.k8s.io/v1
#           EndpointSlice /endpointslices/<ns>/<svc> and a v1 Endpoints
#           /endpoints/<ns>/<svc>; this controller's deleted with their
#           Service, anyone else's never touched
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_scan store_effect store_stamped decision; do
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

for e in ak dl pd nd pv sl ex pr; do
  v="$(nc_decision "$ROOT/modules/app/_chronicle/endpoints.plan" $e)"
  [ -n "$v" ] || { echo "FAIL: endpoints.plan $e did not compile"; exit 1; }
  eval "DEC_$(echo $e | tr a-z A-Z)=\"$v\""
done
for e in dc gn ad ag; do
  v="$(nc_decision "$ROOT/modules/app/_chronicle/endpoints_doc.plan" $e)"
  [ -n "$v" ] || { echo "FAIL: endpoints_doc.plan $e did not compile"; exit 1; }
  eval "DEC_$(echo $e | tr a-z A-Z)=\"$v\""
done
DEC_TTL="$(nc_decision "$ROOT/modules/app/_chronicle/ttl.plan" ex)"
[ -n "$DEC_TTL" ] || { echo "FAIL: ttl.plan ex did not compile"; exit 1; }
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
    join_paths: "status.conditions[type=Ready].status,status.podIP,metadata.uid,spec.nodeName,metadata.deletionTimestamp"
    watch: "/pods/,/probe-status/,/pod-probes/,/nodes/"
  - name: probe
    type: store_effect
    decision: {hex: "${DEC_AK}"}
    key_prefix: "/probe-status/"
    flat: 1
    paths: "ready"
  - name: declared
    type: store_effect
    decision: {hex: "${DEC_DL}"}
    key_prefix: "/pod-probes/"
    flat: 1
    paths: "rp_cmd"
  - name: pod
    type: store_stamped
    decision: {hex: "${DEC_PD}"}
    key_prefix: "/pods/"
    ints: "spec.terminationGracePeriodSeconds,metadata.deletionGracePeriodSeconds"
  - name: node
    type: store_effect
    decision: {hex: "${DEC_ND}"}
    key_prefix: "/nodes/"
    paths: "metadata.labels.\"topology.kubernetes.io/zone\""
  - name: prev
    type: store_effect
    decision: {hex: "${DEC_PV}"}
    key_prefix: "/endpoint-rows/"
    flat: 1
    paths: "ip,state,uid,at"
    ints: "gen,grace"
  - name: slices
    type: store_effect
    decision: {hex: "${DEC_SL}"}
    key_prefix: "/endpoint-rows/"
    wall: 2
  - name: sl_source
    type: store_source
    prefix: "/endpoint-rows/"
    flat: 1
    paths: "ip,state,zone,node,gen,af,uid,at,grace"
    related: "/services/^1!json:metadata.name,spec.selector"
    watch: "/pods/,/services/"
  - name: podexists
    type: store_effect
    decision: {hex: "${DEC_EX}"}
    key_prefix: "/pods/"
  - name: collect
    type: store_effect
    decision: {hex: "${DEC_PR}"}
    key_prefix: "/endpoint-rows/"
    wall: 2
  - name: expiry_src
    type: store_source
    prefix: "/endpoint-rows/"
    flat: 1
    ints: "at!due+grace"
    now: 1
    due: "at!due+grace"
  - name: expire
    type: store_effect
    decision: {hex: "${DEC_TTL}"}
    key_prefix: "/endpoint-rows/"
  - name: svcs
    type: store_source
    prefix: "/services/"
    paths: "spec.selector"
    list_children: "/endpoint-rows/"
    child_sep: "/"
    watch: "/endpoint-rows/"
  - name: write
    type: store_effect
    decision: {hex: "${DEC_DC}"}
    key_prefix: "/endpoint-lists/"
  - name: docs
    type: store_source
    rev: 1
    prefix: "/endpoint-lists/"
    related: "/services/:metadata.name"
    watch: "/services/"
  - name: gone
    type: store_effect
    decision: {hex: "${DEC_GN}"}
    key_prefix: "/endpoint-lists/"
  - name: api_src
    type: store_scan
    prefix: "/services/"
    watch: "/endpoint-rows/"
  - name: api
    type: store_stamped
    decision: {hex: "${DEC_AD}"}
    ep_docs: "/services/|/endpoint-rows/|/pods/|/endpointslices/|/endpoints/"
  - name: api_slices
    type: store_scan
    prefix: "/endpointslices/"
    paths: "metadata.labels.\"endpointslice.kubernetes.io/managed-by\""
    related: "/services/:metadata.name"
    watch: "/services/"
  - name: api_sgone
    type: store_stamped
    decision: {hex: "${DEC_AG}"}
    ep_docs: "/services/|/endpoint-rows/|/pods/|/endpointslices/|/endpoints/"
  - name: api_eps
    type: store_scan
    prefix: "/endpoints/"
    paths: "metadata.labels.\"endpoints.kubernetes.io/managed-by\""
    related: "/services/:metadata.name"
    watch: "/services/"
  - name: api_egone
    type: store_stamped
    decision: {hex: "${DEC_AG}"}
    ep_docs: "/services/|/endpoint-rows/|/pods/|/endpointslices/|/endpoints/"
wiring:
  - from: pairs.record_out
    to: probe.request_in
  - from: probe.response_out
    to: declared.request_in
  - from: declared.response_out
    to: pod.request_in
  - from: pod.response_out
    to: node.request_in
  - from: node.response_out
    to: prev.request_in
  - from: prev.response_out
    to: slices.request_in
  - from: sl_source.record_out
    to: podexists.request_in
  - from: podexists.response_out
    to: collect.request_in
  - from: expiry_src.record_out
    to: expire.request_in
  - from: svcs.record_out
    to: write.request_in
  - from: docs.record_out
    to: gone.request_in
  - from: api_src.record_out
    to: api.request_in
  - from: api_slices.record_out
    to: api_sgone.request_in
  - from: api_eps.record_out
    to: api_egone.request_in
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

# A ready slice as the chain writes one:
# `ip=…;state=ready;zone=…;node=…;gen=<g>;af=4;uid=<uid>;grace=<ms>`.
sl() { # sl <ip> [gen] [uid] [zone] [node] [grace]
  printf 'ip=%s;state=ready;zone=%s;node=%s;gen=%s;af=4;uid=%s;grace=%s' "$1" "${4:-}" "${5:-}" "${2:-1}" "${3:-}" "${6:-30000}"
}
# A notready row: addressed, not Ready, not being deleted.
nr() { # nr <ip> [gen] [uid]
  printf 'ip=%s;state=notready;zone=;node=;gen=%s;af=4;uid=%s;grace=30000' "$1" "${2:-1}" "${3:-}"
}
# The API's document at <key>, through jq: `jqk <key> <filter>` prints the
# filter's answer (`-c`), empty when there is no document.
jqk() {
  local v; v="$(store_last "$1")"
  [ -n "$v" ] || return 0
  printf '%s' "$v" | jq -c "$2"
}
# One field of a slice value.
sf() { # sf <value> <field>
  python3 -c 'import sys; d=dict(kv.split("=",1) for kv in sys.argv[1].split(";") if "=" in kv); print(d.get(sys.argv[2],""))' "$1" "$2"
}

echo "== 1. two Services, four Pods (web-2 not ready, db-1 matches nothing) =="
store_put "/services/default/web" '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"}}}'
store_put "/services/default/api" '{"metadata":{"name":"api"},"spec":{"selector":{"app":"api","tier":"backend"}}}'
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","conditions":[{"type":"Ready","status":"True"}]}}'
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","conditions":[{"type":"Ready","status":"False"}]}}'
store_put "/pods/default/api-1" '{"metadata":{"name":"api-1","labels":{"app":"api","tier":"backend"}},"status":{"podIP":"10.0.0.3","conditions":[{"type":"Ready","status":"True"}]}}'
store_put "/pods/default/db-1"  '{"metadata":{"name":"db-1","labels":{"app":"db"}},"status":{"podIP":"10.0.0.4","conditions":[{"type":"Ready","status":"True"}]}}'
run 7
[ "$(store_last /endpoint-lists/default/web)" = "web-1=$(sl 10.0.0.1),web-2=$(nr 10.0.0.2)" ] || fail "web: '$(store_last /endpoint-lists/default/web)'"
[ "$(store_last /endpoint-lists/default/api)" = "api-1=$(sl 10.0.0.3)" ] || fail "api: '$(store_last /endpoint-lists/default/api)' (db-1 must not match; multi-label selector)"
echo "   web: web-1=10.0.0.1 ready, gen 1, web-2 notready; api: api-1=10.0.0.3 (db-1 excluded)"
# The API's documents, JSON, read with jq.
[ "$(jqk /endpoints/default/web '[.apiVersion,.kind,.metadata.name,.metadata.namespace,.metadata.labels["endpoints.kubernetes.io/managed-by"]]')" = '["v1","Endpoints","web","default","endpoint-controller"]' ] \
  || fail "/endpoints/default/web is not the controller's v1 Endpoints: '$(store_last /endpoints/default/web)'"
[ "$(jqk /endpoints/default/web '[.subsets[0].addresses[].ip, (.subsets[0].notReadyAddresses[].ip)]')" = '["10.0.0.1","10.0.0.2"]' ] \
  || fail "web's Endpoints: addresses [10.0.0.1], notReadyAddresses [10.0.0.2]: '$(store_last /endpoints/default/web)'"
[ "$(jqk /endpoints/default/web '.subsets | length')" = 1 ] && [ "$(jqk /endpoints/default/web '.subsets[0].notReadyAddresses | length')" = 1 ] \
  || fail "web's Endpoints subsets: '$(store_last /endpoints/default/web)'"
[ "$(jqk /endpointslices/default/web '[.apiVersion,.kind,.addressType,.metadata.labels["kubernetes.io/service-name"],.metadata.labels["endpointslice.kubernetes.io/managed-by"]]')" = '["discovery.k8s.io/v1","EndpointSlice","IPv4","web","endpointslice-controller.k8s.io"]' ] \
  || fail "/endpointslices/default/web: '$(store_last /endpointslices/default/web)'"
[ "$(jqk /endpointslices/default/web '[.endpoints[] | [.addresses[0], .conditions.ready, .conditions.serving, .conditions.terminating]]')" = '[["10.0.0.1",true,true,false],["10.0.0.2",false,false,false]]' ] \
  || fail "web's EndpointSlice endpoints: '$(store_last /endpointslices/default/web)'"
[ "$(jqk /endpointslices/default/api '[.endpoints[].addresses[0]]')" = '["10.0.0.3"]' ] || fail "api's EndpointSlice: '$(store_last /endpointslices/default/api)'"
echo "   /endpoints/default/web (jq): addresses [10.0.0.1], notReadyAddresses [10.0.0.2]; /endpointslices/default/web: 10.0.0.1 ready, 10.0.0.2 not"

echo "== 2. web-2 becomes ready: included, in key order =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","conditions":[{"type":"Ready","status":"True"}]}}'
run 6
[ "$(store_last /endpoint-lists/default/web)" = "web-1=$(sl 10.0.0.1),web-2=$(sl 10.0.0.2 2)" ] || fail "web after flip: '$(store_last /endpoint-lists/default/web)'"
[ "$(jqk /endpoints/default/web '[.subsets[0].addresses[].ip, (.subsets[0].notReadyAddresses // [] | length)]')" = '["10.0.0.1","10.0.0.2",0]' ] \
  || fail "web's Endpoints after the flip: '$(store_last /endpoints/default/web)'"
echo "   web: web-1=10.0.0.1,web-2=10.0.0.2 (gen 2: notready -> ready); its Endpoints both addresses"

echo "== 3. the probe runner overrides: web-1 has a uid and a failing probe; web-3 a passing one =="
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","uid":"u1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","conditions":[{"type":"Ready","status":"True"}]}}'
store_put "/probe-status/u1" "live=1;ready=0"
store_put "/pods/default/web-3" '{"metadata":{"name":"web-3","uid":"u3","labels":{"app":"web"}},"status":{"podIP":"10.0.0.5","conditions":[{"type":"Ready","status":"False"}]}}'
store_put "/probe-status/u3" "live=1;ready=1"
run 6
[ "$(store_last /endpoint-lists/default/web)" = "web-1=$(nr 10.0.0.1 2 u1),web-2=$(sl 10.0.0.2 2),web-3=$(sl 10.0.0.5 1 u3)" ] || fail "probe override: '$(store_last /endpoint-lists/default/web)'"
[ "$(jqk /endpoints/default/web '[[.subsets[0].addresses[].targetRef.name], [.subsets[0].notReadyAddresses[].targetRef | [.kind,.namespace,.name,.uid]]]')" = '[[null,"web-3"],[["Pod","default","web-1","u1"]]]' ] \
  || fail "web's Endpoints with the probe's verdicts: '$(store_last /endpoints/default/web)'"
echo "   web: web-1 notready (its probe fails), web-2, web-3=10.0.0.5 ready — the probe's verdict in AND out; targetRef where the Pod has a uid"

echo "== 4. a label change and a deletion: web-2 relabelled, web-3 deleted — both DRAIN first =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"other"}},"status":{"podIP":"10.0.0.2","conditions":[{"type":"Ready","status":"True"}]}}'
store_del "/pods/default/web-3"
run 7
V2="$(store_last /endpoint-rows/default/web/web-2)"; V3="$(store_last /endpoint-rows/default/web/web-3)"
[ "$(sf "$V2" state)" = terminating ] && [ -n "$(sf "$V2" at)" ] && [ "$(sf "$V2" grace)" = 30000 ] \
  || fail "web-2 (deselected) should be terminating with the moment and the 30 s default grace: '$V2'"
[ "$(sf "$V3" state)" = terminating ] && [ -n "$(sf "$V3" at)" ] && [ "$(sf "$V3" grace)" = 30000 ] && [ "$(sf "$V3" ip)" = 10.0.0.5 ] \
  || fail "web-3 (gone) should be terminating with the moment and the 30 s default grace: '$V3'"
DOC="$(store_last /endpoint-lists/default/web)"
case "$DOC" in *"web-2=ip=10.0.0.2;state=terminating;"*"web-3=ip=10.0.0.5;state=terminating;"*) ;; *) fail "the document carries the draining slices: '$DOC'" ;; esac
[ "$(jqk /endpointslices/default/web '[.endpoints[] | [.addresses[0], .conditions.ready, .conditions.serving, .conditions.terminating]]')" = '[["10.0.0.1",false,false,false],["10.0.0.2",false,false,true],["10.0.0.5",false,false,true]]' ] \
  || fail "web's EndpointSlice while draining: '$(store_last /endpointslices/default/web)'"
[ "$(jqk /endpoints/default/web '[(.subsets[0].addresses // [] | length), [.subsets[0].notReadyAddresses[].ip]]')" = '[0,["10.0.0.1"]]' ] \
  || fail "web's Endpoints leave the terminating out: '$(store_last /endpoints/default/web)'"
echo "   web-2 (deselected) and web-3 (gone): terminating, at=<moment>, grace=30000 — still in the fold and the EndpointSlice (terminating true), out of the Endpoints"
# Their drain is 30 s: not waited for here (step 8 proves the expiry with a
# short grace); both are removed so the rest reads as before.
store_del "/endpoint-rows/default/web/web-2"
store_del "/endpoint-rows/default/web/web-3"
run 5
[ "$(store_last /endpoint-lists/default/web)" = "web-1=$(nr 10.0.0.1 2 u1)" ] || fail "web should have web-1 alone now: '$(store_last /endpoint-lists/default/web)'"
echo "   (removed by hand: web-1, notready, is all that is left)"

echo "== 5. a Service is deleted: its rows, its fold, and its EndpointSlice and Endpoints go with it =="
[ -n "$(store_last /endpointslices/default/api)" ] && [ -n "$(store_last /endpoints/default/api)" ] || fail "api's documents are missing before the delete"
[ -n "$(store_last /endpoint-rows/default/api/api-1)" ] || fail "api-1's slice is missing before the delete"
store_del "/services/default/api"
run 7
[ -z "$(store_last /endpoint-rows/default/api/api-1)" ] || fail "a deleted Service's slice was left behind"
[ -z "$(store_last /endpoint-lists/default/api)" ] || fail "a deleted Service's fold was left behind: '$(store_last /endpoint-lists/default/api)'"
[ -z "$(store_last /endpointslices/default/api)" ] || fail "a deleted Service's EndpointSlice was left behind"
[ -z "$(store_last /endpoints/default/api)" ] || fail "a deleted Service's Endpoints was left behind"
echo "   api deleted: its row, /endpoint-lists/default/api, /endpointslices/default/api and /endpoints/default/api removed"

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
[ "$(store_last /endpoint-lists/default/big)" = "big-1=$(sl 10.0.0.9 1 ub1),big-2=$(sl 10.0.0.10)" ] || fail "big: '$(store_last /endpoint-lists/default/big)' (big-1 is $(printf '%s' "$BIGPOD" | wc -c) bytes)"
[ "$(jqk /endpointslices/default/big '[.endpoints[] | [.addresses[0], (.targetRef.uid // "")]]')" = '[["10.0.0.9","ub1"],["10.0.0.10",""]]' ] \
  || fail "big's EndpointSlice: '$(store_last /endpointslices/default/big)'"
echo "   big: big-1=10.0.0.9 ($(printf '%s' "$BIGPOD" | wc -c) bytes),big-2=10.0.0.10; its EndpointSlice too"

echo "== 7. a Service WITHOUT a selector is left alone: its own row stays, no Pod is joined to it =="
# As kube-system/kube-dns is (cluster_dns writes its one endpoint itself).
store_put "/services/default/manual" '{"metadata":{"name":"manual"},"spec":{"clusterIP":"10.96.0.10","ports":[{"port":53,"protocol":"UDP"}]}}'
store_put "/endpoint-rows/default/manual/cluster-dns-10-0-0-50" "$(sl 10.0.0.50)"
run 7
[ "$(store_last /endpoint-rows/default/manual/cluster-dns-10-0-0-50)" = "$(sl 10.0.0.50)" ] \
  || fail "a selectorless Service's own slice was pruned (it names no Pod)"
for p in web-1 web-2 api-1 db-1 big-1 big-2; do
  [ -z "$(store_last "/endpoint-rows/default/manual/$p")" ] || fail "$p was joined to a Service with no selector"
done
[ -z "$(store_last /endpoint-lists/default/manual)" ] || fail "a selectorless Service got a fold"
[ "$(jqk /endpoints/default/manual '[.subsets[0].addresses[] | [.ip, (.targetRef // "none")]]')" = '[["10.0.0.50","none"]]' ] \
  || fail "a selectorless Service's own row is in its API Endpoints: '$(store_last /endpoints/default/manual)'"
echo "   manual: its row kept, no Pod joined, no fold; its API Endpoints and EndpointSlice carry its own row (as kube-dns's do)"

echo "== 7b. a user's own Endpoints and EndpointSlice are never touched, not even when their Service goes =="
USER_EP='{"apiVersion":"v1","kind":"Endpoints","metadata":{"name":"byhand","namespace":"default"},"subsets":[{"addresses":[{"ip":"192.0.2.7"}],"ports":[{"port":5432}]}]}'
USER_SL='{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"byhand-x1","namespace":"default","labels":{"kubernetes.io/service-name":"byhand"}},"addressType":"IPv4","endpoints":[{"addresses":["192.0.2.7"]}],"ports":[{"port":5432}]}'
USER_SL2='{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"byhand","namespace":"default","labels":{"kubernetes.io/service-name":"byhand"}},"addressType":"IPv4","endpoints":[],"ports":[]}'
store_put "/services/default/byhand" '{"metadata":{"name":"byhand"},"spec":{"ports":[{"port":5432}]}}'
store_put "/endpoints/default/byhand" "$USER_EP"
store_put "/endpointslices/default/byhand-x1" "$USER_SL"
store_put "/endpointslices/default/byhand" "$USER_SL2"
run 6
[ "$(store_last /endpoints/default/byhand)" = "$USER_EP" ] || fail "a user's Endpoints was rewritten: '$(store_last /endpoints/default/byhand)'"
[ "$(store_last /endpointslices/default/byhand-x1)" = "$USER_SL" ] || fail "a user's EndpointSlice was rewritten"
[ "$(store_last /endpointslices/default/byhand)" = "$USER_SL2" ] || fail "a user's EndpointSlice named after its Service was rewritten"
store_del "/services/default/byhand"
run 6
[ "$(store_last /endpoints/default/byhand)" = "$USER_EP" ] || fail "a user's Endpoints was deleted with its Service"
[ "$(store_last /endpointslices/default/byhand-x1)" = "$USER_SL" ] && [ "$(store_last /endpointslices/default/byhand)" = "$USER_SL2" ] \
  || fail "a user's EndpointSlice was deleted with its Service"
echo "   byhand: the user's Endpoints and EndpointSlices unchanged with their selectorless Service, and after it is deleted"

echo "== 8. a graceful deletion: serving while ready, terminating when not, its drain moment kept =="
store_put "/nodes/node-z" '{"metadata":{"name":"node-z","labels":{"topology.kubernetes.io/zone":"zone-a"}}}'
store_put "/services/default/drain" '{"metadata":{"name":"drain"},"spec":{"selector":{"app":"drain"}}}'
store_put "/pods/default/d-1" '{"metadata":{"name":"d-1","uid":"ud1","labels":{"app":"drain"}},"spec":{"nodeName":"node-z","terminationGracePeriodSeconds":45},"status":{"podIP":"10.0.0.21","conditions":[{"type":"Ready","status":"True"}]}}'
run 7
[ "$(store_last /endpoint-rows/default/drain/d-1)" = "$(sl 10.0.0.21 1 ud1 zone-a node-z 45000)" ] \
  || fail "d-1 ready, with its node's zone and its grace: '$(store_last /endpoint-rows/default/drain/d-1)'"
echo "   d-1: ready, zone=zone-a (the node's label), node=node-z, gen=1, grace=45000 (terminationGracePeriodSeconds)"
# Deleted gracefully: deletionTimestamp set, the deleter's grace 30 s; still Ready.
store_put "/pods/default/d-1" '{"metadata":{"name":"d-1","uid":"ud1","labels":{"app":"drain"},"deletionTimestamp":"2026-10-26T10:00:00Z","deletionGracePeriodSeconds":30},"spec":{"nodeName":"node-z","terminationGracePeriodSeconds":45},"status":{"podIP":"10.0.0.21","conditions":[{"type":"Ready","status":"True"}]}}'
run 6
V="$(store_last /endpoint-rows/default/drain/d-1)"
[ "$(sf "$V" state)" = serving ] && [ "$(sf "$V" grace)" = 45000 ] && [ -n "$(sf "$V" at)" ] && [ "$(sf "$V" gen)" = 1 ] \
  || fail "d-1 being deleted while Ready should be serving, the moment recorded, the longer grace kept (45000): '$V'"
AT="$(sf "$V" at)"
echo "   d-1: serving, at=$AT, grace=45000 (an announced drain is never shortened by the deleter's 30 s), gen kept"
# Its containers stopped: not Ready, still being deleted — terminating, the SAME moment.
store_put "/pods/default/d-1" '{"metadata":{"name":"d-1","uid":"ud1","labels":{"app":"drain"},"deletionTimestamp":"2026-10-26T10:00:00Z","deletionGracePeriodSeconds":30},"spec":{"nodeName":"node-z","terminationGracePeriodSeconds":45},"status":{"podIP":"10.0.0.21","conditions":[{"type":"Ready","status":"False"}]}}'
run 6
V="$(store_last /endpoint-rows/default/drain/d-1)"
[ "$(sf "$V" state)" = terminating ] && [ "$(sf "$V" at)" = "$AT" ] && [ "$(sf "$V" grace)" = 45000 ] \
  || fail "d-1 not Ready while being deleted should be terminating, its drain moment kept: '$V'"
echo "   d-1: terminating, at=$AT kept, grace=45000"
store_del "/pods/default/d-1"
store_del "/endpoint-rows/default/drain/d-1"

echo "== 8b. a Pod gone outright: its slice drains for the Pod's grace (2 s), then is deleted =="
store_put "/pods/default/d-2" '{"metadata":{"name":"d-2","uid":"ud2","labels":{"app":"drain"}},"spec":{"nodeName":"node-z","terminationGracePeriodSeconds":2},"status":{"podIP":"10.0.0.23","conditions":[{"type":"Ready","status":"True"}]}}'
run 6
[ "$(store_last /endpoint-rows/default/drain/d-2)" = "$(sl 10.0.0.23 1 ud2 zone-a node-z 2000)" ] \
  || fail "d-2 ready: '$(store_last /endpoint-rows/default/drain/d-2)'"
store_del "/pods/default/d-2"
run 9
[ -z "$(store_last /endpoint-rows/default/drain/d-2)" ] || fail "d-2's slice should have expired 2 s after its drain began: '$(store_last /endpoint-rows/default/drain/d-2)'"
H="$(python3 - "$D/store.log" <<'PYH'
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0; out = []
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if data[p+15:p+15+kl] == b"/endpoint-rows/default/drain/d-2":
        v = data[p+15+kl:p+15+kl+vl].decode()
        out.append("DELETE" if op == 2 else v.split(";")[1])
    p += 15 + kl + vl
print(" ".join(out))
PYH
)"
[ "$H" = "state=ready state=terminating DELETE" ] || fail "d-2's slice should go ready -> terminating -> deleted, went: '$H'"
[ -z "$(store_last /endpoint-lists/default/drain)" ] || fail "drain's document should be gone: '$(store_last /endpoint-lists/default/drain)'"
echo "   d-2 gone: ready -> terminating (at=<moment>, grace=2000) -> deleted by its grace; the document removed"

echo "== 9. an endpoint reused: another IP, then another Pod uid under the same name — the generation moves on =="
store_put "/pods/default/d-1" '{"metadata":{"name":"d-1","uid":"ud1","labels":{"app":"drain"}},"spec":{"nodeName":"node-z"},"status":{"podIP":"10.0.0.21","conditions":[{"type":"Ready","status":"True"}]}}'
run 7
[ "$(sf "$(store_last /endpoint-rows/default/drain/d-1)" gen)" = 1 ] || fail "a fresh slice is generation 1: '$(store_last /endpoint-rows/default/drain/d-1)'"
store_put "/pods/default/d-1" '{"metadata":{"name":"d-1","uid":"ud1","labels":{"app":"drain"}},"spec":{"nodeName":"node-z"},"status":{"podIP":"10.0.0.22","conditions":[{"type":"Ready","status":"True"}]}}'
run 6
V="$(store_last /endpoint-rows/default/drain/d-1)"
[ "$(sf "$V" ip)" = 10.0.0.22 ] && [ "$(sf "$V" gen)" = 2 ] || fail "an IP change bumps the generation: '$V'"
N="$(store_writes /endpoint-rows/default/drain/d-1)"
run 5
[ "$(store_writes /endpoint-rows/default/drain/d-1)" = "$N" ] || fail "a settled slice was rewritten (PUT-if-changed spent a revision)"
store_put "/pods/default/d-1" '{"metadata":{"name":"d-1","uid":"ud2","labels":{"app":"drain"}},"spec":{"nodeName":"node-z"},"status":{"podIP":"10.0.0.22","conditions":[{"type":"Ready","status":"True"}]}}'
run 6
V="$(store_last /endpoint-rows/default/drain/d-1)"
[ "$(sf "$V" uid)" = ud2 ] && [ "$(sf "$V" gen)" = 3 ] || fail "a Pod replaced under the same name bumps the generation: '$V'"
N="$(store_writes /endpointslices/default/drain)"; NE="$(store_writes /endpoints/default/drain)"
run 5
[ "$(store_writes /endpointslices/default/drain)" = "$N" ] && [ "$(store_writes /endpoints/default/drain)" = "$NE" ] \
  || fail "a settled EndpointSlice or Endpoints was rewritten (put-if-changed spent a revision)"
[ "$(jqk /endpointslices/default/drain '[.endpoints[] | [.addresses[0], .nodeName, .zone, .targetRef.uid]]')" = '[["10.0.0.22","node-z","zone-a","ud2"]]' ] \
  || fail "drain's EndpointSlice: '$(store_last /endpointslices/default/drain)'"
echo "   d-1: gen 1 -> 2 (10.0.0.21 -> 10.0.0.22) -> 3 (uid ud1 -> ud2); settled, no revision spent (rows, EndpointSlice, Endpoints); nodeName and zone in the slice"

echo "== E2E green: endpoints as params — a scoped join with a selector predicate, the probe override, ready/notready/serving/terminating with a drain expired by its grace, generations on reuse, a materialised list, the API's EndpointSlice and Endpoints as JSON (a user's never touched) =="
