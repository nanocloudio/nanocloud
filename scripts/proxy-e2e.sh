#!/usr/bin/env bash
# Service proxying as params over net.policy (packaging/debian/fluxor-proxy.yaml).
# Services and their endpoint slices become rows under /np-rows/proxy/, and
# net_apply replaces the `proxy` table from them. The platform runs in DRY RUN:
# the rendered nftables script is checked, then handed to a real kernel in a
# private network namespace; the status says the table is NOT enforced, as a
# dry run must.
#
#   /services/<ns>/<svc>             = Service JSON (clusterIP, ports[])
#   /endpointslices/<ns>/<svc>/<pod> = <ready backend address>
#   /pods/<ns>/<pod>                 = Pod JSON (its container's named ports)
#   /np-rows/proxy/<ns>/<svc>/<pod>  = kind=dnat;vip=…;vports=…;tports=…;protos=…;backend=…;pnames=…;pnums=…
#   /np-status/proxy                 = enforced=0|1;rules=<n>;generation=<g>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-proxy.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/net_apply.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-px-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; echo "--- rendered ---"; cat "$D/np/proxy.nft" 2>/dev/null; tail -10 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/np.sh"

echo "== 1. build: the shipped graph, given a node address and a load-balancer pool as a render gives them =="
python3 - "$GRAPH" "$D/graph.yaml" <<'PY'
import sys
g = open(sys.argv[1]).read()
for a, b in (('    table: "proxy"\n', '    table: "proxy"\n    node_address: "192.0.2.10"\n'),
             ('    node_ports: "30000-32767"\n', '    node_ports: "30000-32767"\n    lb_pool: "10.250.0.0/24"\n')):
    assert g.count(a) == 1, a
    g = g.replace(a, b)
open(sys.argv[2], "w").write(g)
PY
GRAPH="$D/graph.yaml"
np_build

echo "== 2. four Services: two backends, one backend on two ports, headless, no backends =="
np_put "/services/default/web"      '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"},"clusterIP":"10.96.0.10","ports":[{"port":80,"targetPort":8080}]}}'
np_put "/endpointslices/default/web/web-1" "10.0.0.1"
np_put "/endpointslices/default/web/web-2" "10.0.0.2"
np_put "/services/default/multi"    '{"metadata":{"name":"multi"},"spec":{"clusterIP":"10.96.0.20","ports":[{"port":443,"targetPort":8443},{"port":53,"protocol":"UDP"}]}}'
np_put "/endpointslices/default/multi/m-1" "10.0.0.3"
np_put "/services/default/headless" '{"metadata":{"name":"headless"},"spec":{"clusterIP":"None","ports":[{"port":80}]}}'
np_put "/endpointslices/default/headless/h-1" "10.0.0.4"
np_put "/services/default/noep"     '{"metadata":{"name":"noep"},"spec":{"clusterIP":"10.96.0.30","ports":[{"port":80}]}}'
# Named targetPorts: each resolves against the backend's own Pod — n-1 and
# n-2 name `http` differently, and only n-1 has `metrics`.
np_put "/services/default/named"    '{"metadata":{"name":"named"},"spec":{"clusterIP":"10.96.0.40","ports":[{"port":80,"targetPort":"http"},{"port":9090,"targetPort":"metrics"}]}}'
np_put "/endpointslices/default/named/n-1" "10.0.0.5"
np_put "/endpointslices/default/named/n-2" "10.0.0.6"
np_put "/pods/default/n-1" '{"metadata":{"name":"n-1"},"spec":{"containers":[{"name":"c","ports":[{"name":"http","containerPort":8080},{"name":"metrics","containerPort":9100}]}]}}'
np_put "/pods/default/n-2" '{"metadata":{"name":"n-2"},"spec":{"containers":[{"name":"c","ports":[{"name":"http","containerPort":8081}]}]}}'
np_run 5

echo "== 3. the rows =="
ROWS="$(np_keys /np-rows/proxy/)"
[ "$(printf '%s\n' "$ROWS" | grep -c .)" = 5 ] || fail "want 5 rows, got: $ROWS"
[ "$(np_last /np-rows/proxy/default/multi/m-1)" = "kind=dnat;vip=10.96.0.20;vports=443,53;tports=8443,-;protos=-,UDP;backend=10.0.0.3;pnames=;pnums=;nports=-,-;lbip=" ] \
  || fail "multi row: '$(np_last /np-rows/proxy/default/multi/m-1)'"
[ "$(np_last /np-rows/proxy/default/named/n-1)" = "kind=dnat;vip=10.96.0.40;vports=80,9090;tports=http,metrics;protos=-,-;backend=10.0.0.5;pnames=http,metrics;pnums=8080,9100;nports=-,-;lbip=" ] \
  || fail "named row: '$(np_last /np-rows/proxy/default/named/n-1)'"
echo "   web-1, web-2, m-1, n-1, n-2 — headless and backend-less Services have none; a named row carries its Pod's ports"

echo "== 4. the rendered table =="
RS="$(cat "$D/np/proxy.nft" 2>/dev/null)" || fail "no proxy table rendered"
grep -q "ip daddr 10.96.0.10 tcp dport 80 dnat ip to numgen random mod 2 map { 0 : 10.0.0.1 . 8080, 1 : 10.0.0.2 . 8080 }" <<<"$RS" \
  || fail "web: two balanced backends"
grep -q "ip daddr 10.96.0.20 tcp dport 443 dnat to 10.0.0.3:8443" <<<"$RS" || fail "multi: 443 -> 8443"
grep -q "ip daddr 10.96.0.20 udp dport 53 dnat to 10.0.0.3:53" <<<"$RS"   || fail "multi: udp 53, targetPort defaulted"
grep -q "10.96.0.30" <<<"$RS" && fail "noep has no backends and no rule"
grep -q "ip daddr 10.96.0.40 tcp dport 80 dnat ip to numgen random mod 2 map { 0 : 10.0.0.5 . 8080, 1 : 10.0.0.6 . 8081 }" <<<"$RS" \
  || fail "named: targetPort http resolved per backend Pod (8080, 8081)"
grep -q "ip daddr 10.96.0.40 tcp dport 9090 dnat to 10.0.0.5:9100" <<<"$RS" \
  || fail "named: targetPort metrics leads to n-1 only (n-2 has no such port)"
[ "$(np_last /np-status/proxy)" = "enforced=0;rules=7;generation=$(np_last /np-status/proxy | sed 's/.*generation=//')" ] \
  || fail "status: '$(np_last /np-status/proxy)' (a dry run enforces nothing, and says so)"
echo "   balanced, multi-port, UDP, named targetPorts per backend; status: $(np_last /np-status/proxy)"
np_kernel_check proxy

echo "== 5. a backend leaves its slice; a Service is deleted =="
np_put "/endpointslices/default/web/web-2" "-DELETE-"
np_put "/services/default/multi" "-DELETE-"
np_put "/services/default/named" "-DELETE-"
np_run 5
ROWS="$(np_keys /np-rows/proxy/)"
[ "$ROWS" = "/np-rows/proxy/default/web/web-1" ] || fail "after: want only web-1, got: $ROWS"
RS="$(cat "$D/np/proxy.nft")"
grep -q "ip daddr 10.96.0.10 tcp dport 80 dnat to 10.0.0.1:8080" <<<"$RS" || fail "web now has one backend"
grep -q "10.96.0.20" <<<"$RS" && fail "multi is gone"
grep -q "10.96.0.40" <<<"$RS" && fail "named is gone"
echo "   the table follows: web -> 10.0.0.1 only; multi and named gone"

echo "== 6. NodePort and LoadBalancer: ports and an address allocated and claimed, DNATed at the node and the pool =="
np_put "/services/default/np" '{"metadata":{"name":"np","namespace":"default"},"spec":{"type":"NodePort","clusterIP":"10.96.0.50","ports":[{"port":80,"targetPort":8080},{"port":443,"targetPort":8443,"nodePort":31443}]}}'
np_put "/endpointslices/default/np/np-1" "10.0.0.7"
np_put "/services/default/lb" '{"metadata":{"name":"lb","namespace":"default"},"spec":{"type":"LoadBalancer","clusterIP":"10.96.0.60","ports":[{"port":80,"targetPort":8080}]}}'
np_put "/endpointslices/default/lb/lb-1" "10.0.0.8"
np_run 6
NP="$(np_last /services/default/np)"; LB="$(np_last /services/default/lb)"
read -r P0 P1 < <(python3 -c 'import json,sys; print(*[p.get("nodePort",0) for p in json.loads(sys.argv[1])["spec"]["ports"]])' "$NP")
[ "$P1" = 31443 ] || fail "np's own nodePort was not kept: $NP"
[ "$P0" -ge 30000 ] && [ "$P0" -le 32767 ] && [ "$P0" != 31443 ] || fail "np's port 80 was given no nodePort in range: $NP"
read -r LP LIP < <(python3 -c 'import json,sys; o=json.loads(sys.argv[1]); print(o["spec"]["ports"][0].get("nodePort",0), (o.get("status",{}).get("loadBalancer",{}).get("ingress") or [{}])[0].get("ip",""))' "$LB")
[ "$LIP" = 10.250.0.1 ] || fail "lb was given no address of the pool: $LB"
[ "$LP" -ge 30000 ] && [ "$LP" != "$P0" ] && [ "$LP" != 31443 ] || fail "lb's nodePort collides or is missing: $LB"
for c in "/ipam/nodeport/$P0=default/np" "/ipam/nodeport/31443=default/np" "/ipam/nodeport/$LP=default/lb" "/ipam/lb/$(python3 -c 'print(int.from_bytes(bytes([10,250,0,1]),"big"))')=default/lb"; do
  [ "$(np_last "${c%%=*}")" = "${c#*=}" ] || fail "claim ${c%%=*} = '$(np_last "${c%%=*}")', want ${c#*=}"
done
echo "   np: nodePorts $P0 (allocated), 31443 (its own); lb: nodePort $LP, ingress $LIP; each claimed under /ipam/"
RS="$(cat "$D/np/proxy.nft")"
for r in "ip daddr 192.0.2.10 tcp dport $P0 dnat to 10.0.0.7:8080" "ip daddr 192.0.2.10 tcp dport 31443 dnat to 10.0.0.7:8443" \
         "ip daddr 10.250.0.1 tcp dport 80 dnat to 10.0.0.8:8080" "ip daddr 192.0.2.10 tcp dport $LP dnat to 10.0.0.8:8080"; do
  grep -qF "$r" <<<"$RS" || fail "no rule '$r'"
done
echo "   the node's address at each nodePort, the pool address at the port: DNATed to the backends"
np_kernel_check proxy
np_put "/services/default/np" '{"metadata":{"name":"np","namespace":"default"},"spec":{"clusterIP":"10.96.0.50","ports":[{"port":80,"targetPort":8080}]}}'
np_put "/services/default/lb" "-DELETE-"
np_run 6
for c in "/ipam/nodeport/$P0" /ipam/nodeport/31443 "/ipam/nodeport/$LP"; do
  [ -z "$(np_last "$c")" ] || fail "$c was not released"
done
RS="$(cat "$D/np/proxy.nft")"
grep -q "192.0.2.10\|10.250.0.1" <<<"$RS" && fail "a released port or address is still DNATed: $RS"
echo "   np made a ClusterIP Service, lb deleted: their ports and address released, their rules gone"

echo "== E2E green: Service proxying as params — rows to a net.policy table the kernel accepts =="
