#!/usr/bin/env bash
# The Wormhole projection as it ships (packaging/debian/fluxor-wormhole.yaml):
# Services and their endpoint slices become intent rows (wormhole.plan), and
# wormhole_emit publishes them as Wormhole's v1 records, a sealed generation
# at a time, activated by compare-and-swap.
#
#   /wh-intent/<ns>/<svc>                    kind=svc;… (wormhole.plan)
#   /wh-intent-ep/<ns>/<svc>/<pod>           kind=ep;<slice>;pnames=…;pnums=…
#   /wh/<tenant>/<service>/gen/<g>/…         WHR1 records (wormhole_emit)
#   /wh/<tenant>/<service>/active            gen=<g>;intent=…;seal=…;…
#   /wh-status/<ns>/<svc>                    gen=<g>;written=<n>;routes=<r>;endpoints=<e>
#
# The records are read back by a small reader of the WHR1 layout written
# here from the layout alone (scripts/support/whr1.py): every envelope's
# digest, the endpoint and route fields, the manifest's endpoint digest, and
# the seal over the sorted keys. Proves: the records appear; the state
# mapping (ready, serving, terminating -> Ready, Degraded, Draining); one
# route per Service port and listener family, the listener the LB address;
# a Service of another loadBalancerClass is not declared; the active pointer
# advances exactly once per change and not at all for none; a removed
# Service is withdrawn and its generations collected; and the Service's
# status.loadBalancer names its address only while Wormhole's VIP binding
# (/wh/<t>/<s>/vip/<4|6>) is ACTIVE or DRAINING.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-wormhole.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/wormhole_emit.fmod" "$MODULES_DIR/store_source.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-wh-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; grep -v WARN "$D/run.log" 2>/dev/null | tail -10 || true; exit 1; }
. "$ROOT/scripts/support/np.sh"
WHR="python3 $ROOT/scripts/support/whr1.py $D/store.log"

run() { # run <seconds>
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$1" "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}

echo "== 1. build the shipped graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"

SL() { # SL <ip> <state> <gen> <af>
  printf 'ip=%s;state=%s;zone=zone-a;node=node-a;gen=%s;af=%s;uid=u;grace=30000' "$1" "$2" "$3" "$4"
}
echo "== 2. a dual-stack LoadBalancer Service (ClientIP affinity, source ranges), three endpoints; one of another class =="
np_put "/services/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"type":"LoadBalancer","selector":{"app":"web"},"clusterIP":"10.96.0.10","clusterIPs":["10.96.0.10","fd00::10"],"ipFamilies":["IPv4","IPv6"],"ports":[{"port":80,"targetPort":"http"},{"port":53,"protocol":"UDP","targetPort":5353}],"externalTrafficPolicy":"Local","sessionAffinity":"ClientIP","sessionAffinityConfig":{"clientIP":{"timeoutSeconds":600}},"loadBalancerSourceRanges":["192.0.2.0/24"]}}'
# The address svc_alloc gave it (its status is the VIP's to say).
np_put "/lb-addr/default/web" "ip=10.250.0.1"
for p in web-1 web-2 web-3; do
  np_put "/pods/default/$p" "{\"metadata\":{\"name\":\"$p\"},\"spec\":{\"containers\":[{\"name\":\"c\",\"ports\":[{\"name\":\"http\",\"containerPort\":8080}]}]}}"
done
np_put "/endpointslices/default/web/web-1" "$(SL 10.0.0.1 ready 1 4)"
np_put "/endpointslices/default/web/web-2" "$(SL 10.0.0.2 serving 1 4)"
np_put "/endpointslices/default/web/web-3" "$(SL fd00::3 ready 2 6)"
np_put "/services/default/other" '{"metadata":{"name":"other","namespace":"default"},"spec":{"type":"LoadBalancer","loadBalancerClass":"example.com/lb","clusterIP":"10.96.0.11","ports":[{"port":80}]}}'
np_put "/endpointslices/default/other/o-1" "$(SL 10.0.0.9 ready 1 4)"
run 8

R="$(np_last /wh-intent/default/web)"
case "$R" in "kind=svc;type=LoadBalancer;cip=10.96.0.10;"*";lbip=10.250.0.1;ports=80,53;protos=-,UDP;tports=http,5353;etp=Local;itp=;aff=ClientIP;afft=600;src="*192.0.2.0/24*) ;;
  *) fail "the service intent row: '$R'" ;; esac
case "$(np_last /wh-intent-ep/default/web/web-1)" in "kind=ep;ip=10.0.0.1;state=ready;"*";pnames=http;pnums=8080") ;;
  *) fail "an endpoint row: '$(np_last /wh-intent-ep/default/web/web-1)'" ;; esac
[ -z "$(np_last /wh-intent/default/other)" ] && [ -z "$(np_keys /wh-intent-ep/default/other/)" ] \
  || fail "a Service of another loadBalancerClass was declared"
echo "   intent: web (both families, its affinity and ranges) and its three endpoints; 'other' (example.com/lb) not declared"

echo "== 3. the records, read back from the WHR1 layout =="
OUT="$($WHR check default web)" || fail "the records did not verify: $OUT"
printf '%s\n' "$OUT" | sed 's/^/   /'
# The rows converge over a few chain steps; the emitter waits for quiet, so
# this is generation 1 unless a row landed late — what is proven exactly is
# what follows: one generation per change, one active write per generation.
G0="$(sed -n 's/^active gen=\([0-9]*\) routes=4 records=17$/\1/p' <<<"$OUT")"
[ -n "$G0" ] || fail "want four routes (80 and 53 per family), seventeen records"
grep -qx "route r0-4 tcp 10.250.0.1:80 rendezvous drain_only application_key endpoints=2" <<<"$OUT" || fail "the IPv4 port-80 route (the LB address)"
grep -qx "route r0-6 tcp fd00::10:80 rendezvous drain_only application_key endpoints=1" <<<"$OUT" || fail "the IPv6 port-80 route (its ClusterIP)"
grep -qx "route r1-4 udp 10.250.0.1:53 rendezvous drain_only application_key endpoints=2" <<<"$OUT" || fail "the IPv4 port-53 route"
grep -qx "endpoint r0-4 10.0.0.1:8080 ready gen=1" <<<"$OUT" || fail "web-1: Ready, its named port resolved"
grep -qx "endpoint r0-4 10.0.0.2:8080 degraded gen=1" <<<"$OUT" || fail "web-2: serving -> Degraded"
grep -qx "endpoint r1-6 fd00::3:5353 ready gen=2" <<<"$OUT" || fail "web-3: the IPv6 route, the numeric targetPort"
grep -qx "admission 192.0.2.0/24" <<<"$OUT" || fail "the source ranges"
[ "$(np_last /wh-status/default/web)" = "gen=$G0;written=17;routes=4;endpoints=3" ] || fail "status: '$(np_last /wh-status/default/web)'"
echo "   status: $(np_last /wh-status/default/web)"

ACTIVE="$($WHR active-key default web)"
writes() { python3 - "$D/store.log" "$ACTIVE" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if data[p+15:p+15+kl] == want: n += 1
    p += 15 + kl + vl
print(n)
PY
}
[ "$(writes)" = "$G0" ] || fail "the active pointer was written $(writes) times for $G0 generation(s)"

echo "== 4. nothing changes: no generation, no revision =="
run 5
[ "$(writes)" = "$G0" ] || fail "an unchanged intent advanced the active pointer"
echo "   the active pointer still generation $G0, written once per generation"

echo "== 5. web-1 drains (terminating): one new generation, the previous kept =="
np_put "/endpointslices/default/web/web-1" "$(SL 10.0.0.1 terminating 1 4);at=1;grace=30000"
run 8
OUT="$($WHR check default web)" || fail "generation 2 did not verify: $OUT"
grep -qx "active gen=$((G0 + 1)) routes=4 records=17" <<<"$OUT" || fail "want generation $((G0 + 1)): $OUT"
grep -qx "endpoint r0-4 10.0.0.1:8080 draining gen=1" <<<"$OUT" || fail "terminating -> Draining"
[ "$(writes)" = "$((G0 + 1))" ] || fail "one change, $(writes) active writes after $G0"
[ -n "$(np_keys "$($WHR gen-prefix default web "$G0")")" ] || fail "the previous generation was collected (it is kept for pinned flows)"
echo "   generation $((G0 + 1)), web-1 Draining; one more active write; generation $G0 kept"

echo "== 6. another change: generation 3, and generation 1 collected =="
np_put "/endpointslices/default/web/web-2" "-DELETE-"
run 8
OUT="$($WHR check default web)" || fail "generation 3 did not verify: $OUT"
grep -qx "active gen=$((G0 + 2)) routes=4 records=15" <<<"$OUT" || fail "want generation $((G0 + 2)) with web-2 gone: $OUT"
[ -z "$(np_keys "$($WHR gen-prefix default web "$G0")")" ] || fail "generation $G0 should have been collected"
[ -n "$(np_keys "$($WHR gen-prefix default web "$((G0 + 1))")")" ] || fail "generation $((G0 + 1)) is the one before: kept"
[ "$(writes)" = "$((G0 + 2))" ] || fail "$(writes) active writes for $((G0 + 2)) generations"
echo "   web-2's slice gone: generation $((G0 + 2)); generation $G0 collected, $((G0 + 1)) kept"

LBS() { python3 -c 'import json,sys; o=json.loads(sys.argv[1]); print(json.dumps((o.get("status") or {}).get("loadBalancer"), separators=(",",":")))' "$(np_last /services/default/web)"; }
echo "== 6b. status.loadBalancer is gated on the VIP: none while it advertises, the address once ACTIVE, none once WITHDRAWN =="
[ "$(LBS)" = "{}" ] || fail "no VIP binding: status.loadBalancer should be empty, is $(LBS)"
$WHR put-vip default web 4 5 10.250.0.1
run 6
[ "$(np_last /wh-vip/default/web)" = "p4=5;a4=10.250.0.1;p6=0;a6=" ] || fail "the decoded binding: '$(np_last /wh-vip/default/web)'"
[ "$(LBS)" = "{}" ] || fail "ADVERTISED is not ready: status.loadBalancer is $(LBS)"
echo "   ADVERTISED (5): status.loadBalancer {}"
$WHR put-vip default web 4 6 10.250.0.1
$WHR put-vip default web 6 7 fd00::10
run 6
[ "$(LBS)" = '{"ingress":[{"ip":"10.250.0.1"},{"ip":"fd00::10"}]}' ] || fail "ACTIVE (IPv4) and DRAINING (IPv6): status.loadBalancer is $(LBS)"
echo "   IPv4 ACTIVE (6), IPv6 DRAINING (7): status.loadBalancer $(LBS)"
$WHR put-vip default web 4 8 10.250.0.1
$WHR put-vip default web 6 10 fd00::10
run 6
[ "$(LBS)" = "{}" ] || fail "WITHDRAWN and RELEASED: status.loadBalancer should be empty, is $(LBS)"
echo "   IPv4 WITHDRAWN (8), IPv6 RELEASED (10): status.loadBalancer {} again"

echo "== 7. the Service deleted: withdrawn (the next generation, empty), collected, its status gone =="
np_put "/services/default/web" "-DELETE-"
run 8
A="$(np_last "$ACTIVE")"
case "$A" in "gen=$((G0 + 3));intent=;seal=;records=0;routes=0;svc=default/web;withdrawn=1") ;; *) fail "withdrawn pointer: '$A'" ;; esac
[ -z "$(np_keys "$($WHR gen-prefix default web "$((G0 + 1))")")" ] && [ -z "$(np_keys "$($WHR gen-prefix default web "$((G0 + 2))")")" ] \
  || fail "a withdrawn Service's generations remain"
[ -z "$(np_last /wh-intent/default/web)" ] || fail "the intent row outlived its Service"
[ -z "$(np_last /wh-status/default/web)" ] || fail "the status outlived the withdrawal"
echo "   active: $A"

echo "== E2E green: Services declared to Wormhole as its own records, generation by generation =="
