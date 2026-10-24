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
#   /np-rows/proxy/<ns>/<svc>/<pod>  = kind=dnat;vip=…;vports=…;tports=…;protos=…;backend=…;pnames=…;pnums=…;
#                                      nports=…;lbip=…;node=…;etp=…;itp=…
#   /np-rows/hostport/<ns>/<pod>/<i> = kind=hostport;hports=…;tports=…;protos=…;hostips=…;backend=…;node=…
#   /np-status/proxy                 = enforced=0|1|pending;rules=<n>;generation=<g>
#
# What net_apply writes is read back by the platform's own decoder (the
# contract's `dnat()`), so the rendered rules prove the record layout:
# `[proto][vip][vport][n][flags][backend port]×n` — the backends where they
# are, and each record's translate_source (ClusterIP hairpin, a `Cluster`
# node port always, `Local` never) and its node-local backends.
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
for a, b in (('    table: "proxy"\n', '    table: "proxy"\n    node_address: "192.0.2.10"\n    node_name: "node-a"\n'),
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
np_put "/pods/default/n-1" '{"metadata":{"name":"n-1"},"spec":{"nodeName":"node-a","containers":[{"name":"c","ports":[{"name":"http","containerPort":8080},{"name":"metrics","containerPort":9100}]}]}}'
np_put "/pods/default/n-2" '{"metadata":{"name":"n-2"},"spec":{"nodeName":"node-b","containers":[{"name":"c","ports":[{"name":"http","containerPort":8081}]}]}}'
np_run 5

echo "== 3. the rows =="
ROWS="$(np_keys /np-rows/proxy/)"
[ "$(printf '%s\n' "$ROWS" | grep -c .)" = 5 ] || fail "want 5 rows, got: $ROWS"
[ "$(np_last /np-rows/proxy/default/multi/m-1)" = "kind=dnat;vip=10.96.0.20;vports=443,53;tports=8443,-;protos=-,UDP;backend=10.0.0.3;pnames=;pnums=;nports=-,-;lbip=;node=;etp=;itp=" ] \
  || fail "multi row: '$(np_last /np-rows/proxy/default/multi/m-1)'"
[ "$(np_last /np-rows/proxy/default/named/n-1)" = "kind=dnat;vip=10.96.0.40;vports=80,9090;tports=http,metrics;protos=-,-;backend=10.0.0.5;pnames=http,metrics;pnums=8080,9100;nports=-,-;lbip=;node=node-a;etp=;itp=" ] \
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
# Each ClusterIP translates the source of a client that is its own backend
# (hairpin): the platform read that from the record's flags byte.
for b in 10.0.0.1 10.0.0.2; do
  grep -qF "ct status dnat ct original ip daddr 10.96.0.10 ct original protocol tcp ct original proto-dst 80 ip saddr $b ip daddr $b masquerade" <<<"$RS" \
    || fail "web: no hairpin translation for backend $b"
done
grep -q "ct original ip daddr 10.96.0.20.*masquerade" <<<"$RS" || fail "multi: no hairpin translation"
grep -qF "ct original ip daddr 10.96.0.10 ct original protocol tcp ct original proto-dst 80 masquerade" <<<"$RS" \
  && fail "web: a ClusterIP translates every source (always), not only a hairpin"
echo "   balanced, multi-port, UDP, named targetPorts per backend, each ClusterIP hairpinned; status: $(np_last /np-status/proxy)"
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
# externalTrafficPolicy Cluster (the default): every node port and the pool
# address translate the source, so the reply comes back through this node.
for a in "ip daddr 192.0.2.10 ct original protocol tcp ct original proto-dst $P0 masquerade" \
         "ip daddr 10.250.0.1 ct original protocol tcp ct original proto-dst 80 masquerade"; do
  grep -qF "ct status dnat ct original $a" <<<"$RS" || fail "no source translation '$a' (externalTrafficPolicy Cluster is always)"
done
echo "   the node's address at each nodePort, the pool address at the port: DNATed to the backends, the source translated (Cluster)"
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

echo "== 7. traffic policies Local: only this node's backends; the client's address kept =="
# node-a is this node (node_name). loc's two backends: l-1 here, l-2 on node-b.
np_put "/services/default/loc" '{"metadata":{"name":"loc","namespace":"default"},"spec":{"type":"NodePort","clusterIP":"10.96.0.70","externalTrafficPolicy":"Local","internalTrafficPolicy":"Local","ports":[{"port":80,"targetPort":8080,"nodePort":31080}]}}'
np_put "/endpointslices/default/loc/l-1" "10.0.0.9"
np_put "/endpointslices/default/loc/l-2" "10.0.0.10"
np_put "/pods/default/l-1" '{"metadata":{"name":"l-1"},"spec":{"nodeName":"node-a","containers":[{"name":"c"}]}}'
np_put "/pods/default/l-2" '{"metadata":{"name":"l-2"},"spec":{"nodeName":"node-b","containers":[{"name":"c"}]}}'
# clu: the same backends, Cluster: both, from any node.
np_put "/services/default/clu" '{"metadata":{"name":"clu","namespace":"default"},"spec":{"type":"NodePort","clusterIP":"10.96.0.71","ports":[{"port":80,"targetPort":8080,"nodePort":31081}]}}'
np_put "/endpointslices/default/clu/l-1" "10.0.0.9"
np_put "/endpointslices/default/clu/l-2" "10.0.0.10"
np_run 6
[ "$(np_last /np-rows/proxy/default/loc/l-2)" = "kind=dnat;vip=10.96.0.70;vports=80;tports=8080;protos=-;backend=10.0.0.10;pnames=;pnums=;nports=31080;lbip=;node=node-b;etp=Local;itp=Local" ] \
  || fail "loc row: '$(np_last /np-rows/proxy/default/loc/l-2)'"
RS="$(cat "$D/np/proxy.nft")"
for r in "ip daddr 10.96.0.70 tcp dport 80 dnat to 10.0.0.9:8080" "ip daddr 192.0.2.10 tcp dport 31080 dnat to 10.0.0.9:8080" \
         "ip daddr 10.96.0.71 tcp dport 80 dnat ip to numgen random mod 2 map { 0 : 10.0.0.9 . 8080, 1 : 10.0.0.10 . 8080 }" \
         "ip daddr 192.0.2.10 tcp dport 31081 dnat ip to numgen random mod 2 map { 0 : 10.0.0.9 . 8080, 1 : 10.0.0.10 . 8080 }"; do
  grep -qF "$r" <<<"$RS" || fail "no rule '$r'"
done
grep -q "proto-dst 31080 masquerade" <<<"$RS" && fail "externalTrafficPolicy Local translates the client's address"
grep -qF "proto-dst 31081 masquerade" <<<"$RS" || fail "externalTrafficPolicy Cluster does not translate the source"
echo "   loc: ClusterIP and nodePort 31080 -> l-1 only (node-a's), source kept; clu: both backends, source translated"
np_put "/endpointslices/default/loc/l-1" "-DELETE-"
np_put "/endpointslices/default/clu/l-1" "-DELETE-"
np_run 5
RS="$(cat "$D/np/proxy.nft")"
grep -qF "ip daddr 10.96.0.70 tcp dport 80 reject" <<<"$RS" || fail "loc with no backend here does not reject at its ClusterIP: $RS"
grep -qF "ip daddr 192.0.2.10 tcp dport 31080 reject" <<<"$RS" || fail "loc with no backend here does not reject at its nodePort"
grep -q "10.0.0.10:8080" <<<"$(grep '10.96.0.70\|31080' <<<"$RS")" && fail "loc reaches node-b's backend"
grep -qF "ip daddr 10.96.0.71 tcp dport 80 dnat to 10.0.0.10:8080" <<<"$RS" || fail "clu lost its remote backend"
echo "   l-1 gone: loc rejects at its ClusterIP and nodePort (none of its backends is here); clu leads to node-b's"
np_kernel_check proxy

echo "== 8. hostPort: the node's address at the hostPort, on the Pod's own node only =="
np_put "/pods/default/hp-1" '{"metadata":{"name":"hp-1"},"spec":{"nodeName":"node-a","containers":[{"name":"a","ports":[{"containerPort":8080,"hostPort":18080},{"containerPort":9000}]},{"name":"b","ports":[{"containerPort":5353,"hostPort":15353,"protocol":"UDP","hostIP":"192.0.2.99"}]}]},"status":{"podIP":"10.0.0.20"}}'
np_put "/pods/default/hp-2" '{"metadata":{"name":"hp-2"},"spec":{"nodeName":"node-b","containers":[{"name":"a","ports":[{"containerPort":8080,"hostPort":18081}]}]},"status":{"podIP":"10.0.0.21"}}'
np_put "/pods/default/nohp" '{"metadata":{"name":"nohp"},"spec":{"nodeName":"node-a","containers":[{"name":"a","ports":[{"containerPort":8080}]}]},"status":{"podIP":"10.0.0.22"}}'
np_run 6
[ "$(np_last /np-rows/hostport/default/hp-1/0)" = "kind=hostport;hports=18080,-;tports=8080,9000;protos=-,-;hostips=-,-;backend=10.0.0.20;node=node-a" ] \
  || fail "hp-1 row: '$(np_last /np-rows/hostport/default/hp-1/0)'"
[ -z "$(np_keys /np-rows/hostport/default/nohp)" ] || fail "a Pod with no hostPort has a row"
RS="$(cat "$D/np/proxy.nft")"
for r in "ip daddr 192.0.2.10 tcp dport 18080 dnat to 10.0.0.20:8080" "ip daddr 192.0.2.99 udp dport 15353 dnat to 10.0.0.20:5353"; do
  grep -qF "$r" <<<"$RS" || fail "no hostPort rule '$r'"
done
grep -q "18081\|9000" <<<"$RS" && fail "node-b's hostPort, or a port with none, is DNATed here"
echo "   hp-1: 192.0.2.10:18080 -> 10.0.0.20:8080, 192.0.2.99:15353/udp (its hostIP) -> :5353; hp-2 (node-b's) not here"
np_kernel_check proxy
np_put "/pods/default/hp-1" "-DELETE-"
np_run 5
[ -z "$(np_keys /np-rows/hostport/default/hp-1/)" ] || fail "hp-1's rows outlived it: $(np_keys /np-rows/hostport/)"
grep -q "18080" <<<"$(cat "$D/np/proxy.nft")" && fail "hp-1's hostPort is still DNATed"
echo "   hp-1 deleted: its rows and rules gone"

echo "== E2E green: Service proxying as params — rows to a net.policy table the kernel accepts =="
