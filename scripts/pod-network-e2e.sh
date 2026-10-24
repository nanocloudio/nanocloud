#!/usr/bin/env bash
# Live E2E: Pods with addresses of their own, reaching each other across
# nodes — the Pod network as the packaged renders build it, through fluxor's
# net.route (0x20) and its Linux realization, in real kernels.
#
#   lan     one link (a bridge in a network domain of its own): node-a
#           192.168.77.1, node-b .2, a client C .100 — two machines and a
#           host on their network, nothing else routes
#   node-a  replica 0, the voter: the packaged control plane rendered with
#           Pods (NANOCLOUD_PODS=1) — its kubelet, its node_net
#   node-b  replica 1: the packaged worker, joined to node-a's authority
#           (rig-join.sh, its slot's credential), its kubelet, its node_net
#   Each runtime runs as root in its node's network domain, with a state
#   directory and a registry of its own (FLUXOR_STATE_DIR, FLUXOR_RUN_DIR):
#   two hosts. NetworkPolicy is left out (NANOCLOUD_NETPOLICY=0): a control
#   plane that runs Pods with it asks lattice's store for more subscriptions
#   than it holds (256, lattice store_provider.rs SUBSCRIPTIONS), and the
#   last consumers of the render are refused.
#
# Each step fails without the change it proves:
#   1. podCIDR    each Node is given its own /24 of 10.244.0.0/16
#                 (svc_alloc, Node.spec.podCIDR), carried by its renewals
#                 (node_lease); without either, a Node has none
#   2. segment    each node's node_net states its segment in place
#                 (/node-net/<node> seg=1, from READ) and routes the other's
#                 /24 to it (routes=1/1); without node_net nothing is written
#                 and no Pod gets an address of its own
#   3. Pod IPs    a Pod is given an address from its node's /24, never the
#                 gateway (.1, ipam_pod), and status.podIP is that lease
#                 (pod_resolve's request, the sandbox's identity on segment
#                 1); without them its podIP is its node's
#   4. probes     a server Pod's httpGet readiness probe dials its own
#                 address: it serves in its own network domain only, so it
#                 is Ready only because the probe targets the Pod IP
#   5. Pod-to-Pod a Pod on node-b fetches from the server Pod on node-a by
#                 its address: through node-b's segment gateway, its peer
#                 route to node-a, and node-a's attachment
#   6. hostNetwork a hostNetwork Pod shares its node's network: it sees the
#                 node's address, and its podIP is its hostIP
#   7. LB         a LoadBalancer address (svc_alloc's pool, 192.168.77.240/29)
#                 is answered on the link by exactly one node: for `Local`
#                 the node with the endpoint (node-b); kept by it when the
#                 Service turns `Cluster`; and, when node-b goes (its link
#                 down, its runtime stopped), taken over by node-a once the
#                 plane marks node-b not ready — answered by node-a alone
#   8. release    a deleted Pod's address goes back: once the Pod and its
#                 sandbox are gone its node releases its lease and request
#                 (node_net; the plane's own release ignores a request that
#                 names its node), and with them the address's claim
#
# Needs passwordless sudo (network domains, the runtimes as root), python3,
# kubectl and redis-cli. Without sudo it skips; NANOCLOUD_REQUIRE_E2E=1
# makes that a failure. NC_KEEP=1 keeps the run's directory.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
for c in fluxor kubectl openssl curl redis-cli python3 ip; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not found"; exit 1; }
done
sudo -n true 2>/dev/null || skip "needs passwordless sudo (network domains, the runtimes as root)"
KUBECTL="$(command -v kubectl)"
PY="$(command -v python3)"

D="$(mktemp -d /tmp/nc-podnet-XXXXXX)"
chmod 755 "$D"
TAG="pn$$"
NS_LAN="${TAG}lan"
NS_A="${TAG}a"
NS_B="${TAG}b"
NS_C="${TAG}c"
declare -A NS=([plane]="$NS_A" [worker]="$NS_B")
declare -A RID=([plane]=0 [worker]=1)
declare -A NODE=([plane]=node-a [worker]=node-b)
declare -A ADDR=([plane]=192.168.77.1 [worker]=192.168.77.2)
LB_POOL=192.168.77.240/29
API="192.168.77.1:7443"
PEERS="0=192.168.77.1:7450 1=192.168.77.2:7451"
READER_PORT=7460

stop() { # stop <replica>
  local pf="$D/$1/pid"
  [ -s "$pf" ] || return 0
  sudo -n kill "$(cat "$pf")" 2>/dev/null || true
  for _ in $(seq 100); do
    sudo -n kill -0 "$(cat "$pf")" 2>/dev/null || break
    sleep 0.1
  done
  sudo -n kill -9 "$(cat "$pf")" 2>/dev/null || true
  sudo -n rm -f "$pf"
}
cleanup() {
  stop worker
  stop plane
  for n in "$NS_A" "$NS_B" "$NS_C" "$NS_LAN"; do sudo -n ip netns del "$n" 2>/dev/null || true; done
  if [ -n "${NC_KEEP:-}" ]; then echo "kept $D"; else sudo -n rm -rf "$D"; fi
}
trap cleanup EXIT
fail() {
  echo "FAIL: $1"
  for r in plane worker; do
    echo "--- $r"
    grep -a "node_net\|net_route\|\[pod_resolve\]\|ERROR\|FAULT" "$D/$r/run.log" 2>/dev/null | grep -v MON_ | tail -15 || true
    for k in "/node-net/${NODE[$r]}"; do echo "   $k: $(get "$r" "$k")"; done
  done
  exit 1
}

# ── the link: a bridge, three hosts on it ──
echo "== 0. the lan: node-a .1, node-b .2, client .100 on one link =="
for n in "$NS_LAN" "$NS_A" "$NS_B" "$NS_C"; do
  sudo -n ip netns add "$n"
  sudo -n ip -n "$n" link set lo up
done
sudo -n ip -n "$NS_LAN" link add br0 type bridge
sudo -n ip -n "$NS_LAN" link set br0 up
i=0
for pair in "$NS_A:192.168.77.1" "$NS_B:192.168.77.2" "$NS_C:192.168.77.100"; do
  n="${pair%%:*}"; a="${pair#*:}"
  sudo -n ip link add "${TAG}u$i" netns "$n" type veth peer name "${TAG}p$i" netns "$NS_LAN"
  sudo -n ip -n "$NS_LAN" link set "${TAG}p$i" master br0 up
  sudo -n ip -n "$n" link set "${TAG}u$i" name eth0
  sudo -n ip -n "$n" addr add "$a/24" dev eth0
  sudo -n ip -n "$n" link set eth0 up
  i=$((i + 1))
done
in_ns() { local n="$1"; shift; sudo -n ip netns exec "$n" "$@"; }
in_ns "$NS_C" ping -c1 -W2 192.168.77.1 >/dev/null || fail "the lan does not carry"
MAC_A="$(in_ns "$NS_A" cat /sys/class/net/eth0/address)"
MAC_B="$(in_ns "$NS_B" cat /sys/class/net/eth0/address)"
echo "   node-a $MAC_A, node-b $MAC_B"

# ── the renders ──
for r in plane worker; do mkdir -p "$D/$r/pki" "$D/$r/state" "$D/$r/run"; nc_lattice_prepare "$D/$r"; done
# The runtimes run as root, and a runtime keeps its host registry only in a
# directory private to it (owned by it or root, not group- or world-writable):
# one the invoking user made is refused, and host networking with it.
for r in plane worker; do sudo -n chown root:root "$D/$r/run"; sudo -n chmod 0755 "$D/$r/run"; done
render() { # render <replica> <role> <template> <out>
  NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PKI="$D/$1/pki" NANOCLOUD_ROLE="$2" \
    NANOCLOUD_PODS=1 NANOCLOUD_PEER_TRUST=authority NANOCLOUD_PLANE_API="$API" \
    NANOCLOUD_REPLICA_ID="${RID[$1]}" NANOCLOUD_NODE_NAME="${NODE[$1]}" NANOCLOUD_VOTERS=1 \
    NANOCLOUD_NODE_ADDRESS="${ADDR[$1]}" NANOCLOUD_LB_POOL="$LB_POOL" \
    NANOCLOUD_PEER_PORT="$((7450 + ${RID[$1]}))" NANOCLOUD_PEERS="$PEERS" \
    NANOCLOUD_DNS_UPSTREAM= NANOCLOUD_REGISTRY_TLS=0 NANOCLOUD_NETPOLICY=0 \
    NANOCLOUD_REPLICA_FRAGMENT="$ROOT/packaging/debian/fluxor-replica.yaml" \
    NANOCLOUD_KUBELET_FRAGMENT="$ROOT/packaging/debian/fluxor-podlifecycle.yaml" \
    sh "$ROOT/packaging/debian/nanocloud-graph.sh" "$3" "$4"
}
splice() { # splice <graph> <modules> <wiring>
  python3 - "$@" <<'PY'
import sys
path, mods_add, wire_add = sys.argv[1:4]
mods, wiring = open(path).read().split("\nwiring:\n", 1)
open(path, "w").write(mods.rstrip("\n") + "\n" + mods_add.rstrip("\n") + "\n\nwiring:\n"
                      + wiring.rstrip("\n") + "\n" + wire_add.rstrip("\n") + "\n")
PY
}
reader() { # reader <with-router>: the test's GET surface, in the node's domain
  printf '  - name: redis_edge_anchor\n    params:\n      listen_port: %s\n' "$READER_PORT"
  [ "$1" = 1 ] && printf '  - name: kv_request_router\n    params:\n      lin_reads: 0\n      replicated: 1\n'
  return 0
}
reader_wiring() { # reader_wiring <with-router>
  cat <<'YAML'
  - from: linux_net.net_out
    to: redis_edge_anchor.net_in
    buffer_bytes: 65536
  - from: redis_edge_anchor.net_out
    to: linux_net.net_in
  - from: redis_edge_anchor.kv_out
    to: kv_request_router.redis_in
  - from: kv_request_router.redis_out
    to: redis_edge_anchor.kv_in
YAML
  [ "$1" = 1 ] && cat <<'YAML'
  - from: kv_request_router.kv_out
    to: kv_state_worker_store.commands
  - from: kv_state_worker_store.responses
    to: kv_request_router.kv_in
  - from: kv_request_router.proposal_out
    to: gateway.client_requests
  - from: gateway.client_responses
    to: kv_request_router.gateway_reject
YAML
  return 0
}
SEAL_plane="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
SEAL_worker="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
start() { # start <replica>: as root in its node's domain, a host of its own
  local seal="SEAL_$1"
  mkdir -p "$D/$1/vault"
  sudo -n ip netns exec "${NS[$1]}" sh -c 'd="$1"; shift; echo $$ > "$d/pid"; cd "$d"; exec env "$@"' sh "$D/$1" \
    FLUXOR_SEAL_KEY="${!seal}" FLUXOR_VAULT_DIR="$D/$1/vault" \
    FLUXOR_STATE_DIR="$D/$1/state" FLUXOR_RUN_DIR="$D/$1/run" \
    RUST_LOG="${NC_LOG:-warn}" "$FLUXOR_RUNTIME" --config "$D/$1/config.bin" --modules "$D/$1/modules.bin" \
    >>"$D/$1/run.log" 2>&1 &
  for _ in $(seq 50); do [ -s "$D/$1/pid" ] && return 0; sleep 0.1; done
  fail "the $1 did not start"
}
alive() { [ -s "$D/$1/pid" ] && sudo -n kill -0 "$(cat "$D/$1/pid")" 2>/dev/null; }
wait_for() { # wait_for <seconds> <what> <cmd...>
  local secs="$1" what="$2" r; shift 2
  local end=$((SECONDS + secs))
  until "$@"; do
    [ "$SECONDS" -lt "$end" ] || fail "$what (after ${secs}s)"
    for r in $LIVE; do alive "$r" || fail "the $r exited"; done
    sleep 0.5
  done
}
get() { in_ns "${NS[$1]}" redis-cli -p "$READER_PORT" --raw GET "$2" 2>/dev/null || true; }
LIVE="plane"

echo "== 1a. node-a: the control plane with Pods, its node_net, its own authority =="
render plane plane "$ROOT/packaging/debian/fluxor-controlplane.yaml" "$D/plane/graph.yaml" \
  || fail "the plane did not render"
grep -q "^    type: node_net$" "$D/plane/graph.yaml" || fail "the plane's render has no node_net"
splice "$D/plane/graph.yaml" "$(reader 0)" "$(reader_wiring 0)"
nc_build_workload "$ROOT" "$D/plane/graph.yaml" "$D/plane/config.bin" "$D/plane/modules.bin" >/dev/null \
  || fail "the plane did not build"
start plane
wait_for 90 "the plane announced no join secret" grep -aq '\[join\] secret=' "$D/plane/run.log"
JOIN="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/plane/run.log" | tail -1)"
SECRET="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
CA_SHA="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
KC="$D/operator/kubeconfig"
mkdir -p "$D/operator"
in_ns "$NS_C" env HOME="$D/operator" bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "$API" "$KC" >/dev/null \
  || fail "the operator's join did not complete"
k() { in_ns "$NS_C" env HOME="$D/operator" "$KUBECTL" --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get namespaces >/dev/null || fail "the operator's kubeconfig does not work"
echo "   joined: the operator's kubeconfig, node-1's credential"

echo "== 1b. node-b: the worker, its slot's credential =="
sudo -n cp "$D/operator/ca.der" "$D/worker/pki/plane-ca.der"
sudo -n cp "$D/operator/node-1.token" "$D/worker/pki/node.token"
sudo -n chown "$(id -u):$(id -g)" "$D/worker/pki/plane-ca.der" "$D/worker/pki/node.token"
render worker node "$ROOT/packaging/debian/fluxor-worker.yaml" "$D/worker/graph.yaml" \
  || fail "the worker did not render"
grep -q "^    type: node_net$" "$D/worker/graph.yaml" || fail "the worker's render has no node_net"
splice "$D/worker/graph.yaml" "$(reader 1)" "$(reader_wiring 1)"
nc_build_workload "$ROOT" "$D/worker/graph.yaml" "$D/worker/config.bin" "$D/worker/modules.bin" >/dev/null \
  || fail "the worker did not build"
start worker
LIVE="plane worker"
nodes_ready() {
  [ "$(k get nodes -o jsonpath='{range .items[*]}{.metadata.name}={.status.conditions[?(@.type=="Ready")].status} {end}' 2>/dev/null)" \
    = "node-a=True node-b=True " ]
}
wait_for 120 "both nodes never became Ready ($(k get nodes 2>&1 | tr '\n' ' '))" nodes_ready
echo "   node-a and node-b Ready"

echo "== 1. each node is given its own Pod CIDR =="
cidr_of() { k get node "$1" -o jsonpath='{.spec.podCIDR}' 2>/dev/null; }
both_cidrs() { [ -n "$(cidr_of node-a)" ] && [ -n "$(cidr_of node-b)" ]; }
wait_for 60 "a node has no podCIDR" both_cidrs
CIDR_A="$(cidr_of node-a)"; CIDR_B="$(cidr_of node-b)"
[ "$CIDR_A" != "$CIDR_B" ] || fail "both nodes hold $CIDR_A"
for c in "$CIDR_A" "$CIDR_B"; do
  [[ "$c" =~ ^10\.244\.[0-9]+\.0/24$ ]] || fail "a podCIDR '$c' is not a /24 of 10.244.0.0/16"
done
[ "$(k get node node-a -o jsonpath='{.spec.podCIDRs[0]}')" = "$CIDR_A" ] || fail "node-a's podCIDRs is not its podCIDR"
# Kept across renewals (each a whole-record write): still there 12 s on.
sleep 12
[ "$(cidr_of node-a)" = "$CIDR_A" ] && [ "$(cidr_of node-b)" = "$CIDR_B" ] || fail "a renewal lost a podCIDR"
echo "   node-a $CIDR_A, node-b $CIDR_B (kept across renewals)"

echo "== 2. each node's segment is in place, and its peer route to the other =="
seg_up() { # seg_up <replica>
  local v; v="$(get plane "/node-net/${NODE[$1]}")"
  case "$v" in "seg=1;"*";routes=1/1"*) return 0 ;; esac
  return 1
}
wait_for 60 "node-a's segment is not in place: '$(get plane /node-net/node-a)'" seg_up plane
wait_for 60 "node-b's segment is not in place: '$(get plane /node-net/node-b)'" seg_up worker
GW_A="${CIDR_A%.0/24}.1"
in_ns "$NS_A" ip -4 addr show | grep -q "inet $GW_A/32" || fail "node-a does not hold its gateway $GW_A"
echo "   /node-net/node-a: $(get plane /node-net/node-a)"
echo "   /node-net/node-b: $(get plane /node-net/node-b)"

pod() { # pod <name> <node> <spec members>
  printf '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"%s","namespace":"default","labels":{"app":"%s"}},"spec":{"nodeName":"%s",%s}}' \
    "$1" "$1" "$2" "$3" | k create --validate=false -f - >/dev/null || fail "could not create Pod $1"
}
SERVE='"containers":[{"name":"c","command":["'"$PY"'","-c","import http.server,socket,sys\nclass H(http.server.BaseHTTPRequestHandler):\n  def do_GET(s):\n    b=(socket.gethostname()+\" \"+s.client_address[0]+\"\\n\").encode();s.send_response(200);s.send_header(\"Content-Length\",str(len(b)));s.end_headers();s.wfile.write(b)\n  def log_message(s,*a): pass\nhttp.server.HTTPServer((\"0.0.0.0\",8080),H).serve_forever()"],"ports":[{"containerPort":8080}],"readinessProbe":{"httpGet":{"path":"/","port":8080},"periodSeconds":1}}]'
pod_ip() { k get pod "$1" -o jsonpath='{.status.podIP}' 2>/dev/null; }
pod_ready() { [ "$(k get pod "$1" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = True ]; }
in_cidr() { python3 -c "import ipaddress,sys; sys.exit(0 if ipaddress.ip_address(sys.argv[1]) in ipaddress.ip_network(sys.argv[2]) else 1)" "$1" "$2"; }

echo "== 3. a Pod is given an address from its node's CIDR, and status.podIP is its lease =="
pod srv-a node-a "$SERVE"
has_ip() { local ip; ip="$(pod_ip "$1")"; [ -n "$ip" ] && [ "$ip" != "${ADDR[$2]}" ]; }
wait_for 90 "srv-a was given no address of its own (podIP '$(pod_ip srv-a)')" has_ip srv-a plane
IP_A="$(pod_ip srv-a)"
UID_A="$(k get pod srv-a -o jsonpath='{.metadata.uid}')"
in_cidr "$IP_A" "$CIDR_A" || fail "srv-a's $IP_A is not in node-a's $CIDR_A"
[ "$IP_A" != "$GW_A" ] || fail "srv-a was leased the gateway"
[ "$(get plane "/ipam-lease/$UID_A")" = "$IP_A" ] || fail "srv-a's podIP $IP_A is not its lease '$(get plane "/ipam-lease/$UID_A")'"
[ "$(k get pod srv-a -o jsonpath='{.status.hostIP}')" = 192.168.77.1 ] || fail "srv-a's hostIP is not node-a's"
echo "   srv-a: podIP $IP_A (its lease, in $CIDR_A), hostIP 192.168.77.1"

echo "== 4. its readiness probe dials its own address =="
wait_for 60 "srv-a never became Ready (probe '$(get plane "/~node/node-a/pod-probes/$UID_A")')" pod_ready srv-a
in_ns "$NS_A" curl -s -m 3 "http://192.168.77.1:8080/" >/dev/null && fail "something serves on node-a's own address: the probe proves nothing"
echo "   srv-a Ready; nothing listens on node-a's address"

echo "== 5. a Pod on node-b reaches srv-a on node-a by its address =="
pod cli-b node-b '"restartPolicy":"Never","containers":[{"name":"c","command":["'"$PY"'","-c","import urllib.request,time\nfor i in range(60):\n  try:\n    print(\"OK \"+urllib.request.urlopen(\"http://'"$IP_A"':8080/\",timeout=2).read().decode().strip(),flush=True); break\n  except Exception as e:\n    time.sleep(1)\nelse:\n  print(\"FAILED\",flush=True)"]}]'
wait_for 90 "cli-b was given no address of its own" has_ip cli-b worker
IP_B="$(pod_ip cli-b)"
in_cidr "$IP_B" "$CIDR_B" || fail "cli-b's $IP_B is not in node-b's $CIDR_B"
got_ok() { k logs cli-b 2>/dev/null | grep -q '^OK \|^FAILED'; }
wait_for 90 "cli-b printed nothing" got_ok
LOG="$(k logs cli-b)"
# srv-a answers `<its hostname> <the client address it saw>`: cli-b's own.
case "$LOG" in "OK "*" $IP_B") ;; *) fail "cli-b did not reach srv-a across nodes as itself: '$LOG'" ;; esac
echo "   cli-b ($IP_B on node-b) -> srv-a ($IP_A on node-a): '$LOG' (srv-a saw cli-b's own address)"

echo "== 6. a hostNetwork Pod keeps its node's network =="
IPBIN="$(command -v ip)"
pod host-b node-b '"hostNetwork":true,"restartPolicy":"Never","containers":[{"name":"c","command":["'"$IPBIN"'","-4","-o","addr","show"]}]'
host_done() { k logs host-b 2>/dev/null | grep -q "inet "; }
wait_for 90 "host-b printed nothing" host_done
k logs host-b | grep -q "inet 192.168.77.2/24" || fail "host-b does not see node-b's address: $(k logs host-b | tr '\n' ' ')"
[ "$(pod_ip host-b)" = 192.168.77.2 ] || fail "host-b's podIP is '$(pod_ip host-b)', not its node's"
echo "   host-b sees 192.168.77.2; podIP = hostIP = 192.168.77.2"

echo "== 7. a LoadBalancer address: one node answers, and another when it goes =="
pod srv-b node-b "$SERVE"
wait_for 90 "srv-b never became Ready" pod_ready srv-b
printf '%s' '{"apiVersion":"v1","kind":"Service","metadata":{"name":"lb","namespace":"default"},"spec":{"type":"LoadBalancer","externalTrafficPolicy":"Local","selector":{"app":"srv-b"},"ports":[{"port":80,"targetPort":8080}]}}' \
  | k create --validate=false -f - >/dev/null || fail "could not create the LoadBalancer Service"
lb_ip() { k get service lb -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null; }
lb_given() { [ -n "$(lb_ip)" ]; }
wait_for 60 "the Service was given no load-balancer address" lb_given
LB="$(lb_ip)"
in_cidr "$LB" "$LB_POOL" || fail "the load-balancer address $LB is not of the pool"
# Who answers for it on the link: every ARP reply to the client, by MAC.
who() {
  in_ns "$NS_C" python3 - "$LB" <<'PY'
import socket, struct, sys, time
target = socket.inet_aton(sys.argv[1])
mac = open("/sys/class/net/eth0/address").read().strip()
me = bytes.fromhex(mac.replace(":", ""))
src = socket.inet_aton("192.168.77.100")
s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(0x0806))
s.bind(("eth0", 0))
req = b"\xff" * 6 + me + b"\x08\x06" + struct.pack("!HHBBH", 1, 0x0800, 6, 4, 1) + me + src + b"\x00" * 6 + target
seen = set()
for _ in range(3):
    s.send(req)
    end = time.time() + 0.4
    s.settimeout(0.1)
    while time.time() < end:
        try:
            f = s.recv(128)
        except socket.timeout:
            continue
        if len(f) >= 42 and f[12:14] == b"\x08\x06" and struct.unpack("!H", f[20:22])[0] == 2 and f[28:32] == target:
            seen.add(f[22:28].hex(":"))
print(" ".join(sorted(seen)))
PY
}
answered_by() { [ "$(who)" = "$1" ]; }
wait_for 60 "Local: $LB is not answered by node-b alone (by '$(who)'; /node-net/node-b: $(get plane /node-net/node-b))" answered_by "$MAC_B"
H="$(get plane "/net-hold/$(python3 -c "import ipaddress,sys;print(int(ipaddress.ip_address(sys.argv[1])))" "$LB")")"
[ "$H" = "node=node-b" ] || fail "the address's claim is '$H', not node-b's"
echo "   Local, its endpoint on node-b: $LB answered by node-b alone ($MAC_B); claim $H"
k patch service lb --type=merge -p '{"spec":{"externalTrafficPolicy":"Cluster"}}' >/dev/null || fail "could not patch the Service"
sleep 5
answered_by "$MAC_B" || fail "Cluster: the address moved off a holder that may keep it (by '$(who)')"
echo "   Cluster: node-b keeps it"
# node-b goes: off the link, its runtime stopped. The plane takes it out of
# service after three missed renewals (30 s); node-a's node_net then takes
# the claim over and answers.
in_ns "$NS_B" ip link set eth0 down
stop worker
LIVE="plane"
wait_for 90 "node-b's address was not taken over (answered by '$(who)'; /node-net/node-a: $(get plane /node-net/node-a))" answered_by "$MAC_A"
H="$(get plane "/net-hold/$(python3 -c "import ipaddress,sys;print(int(ipaddress.ip_address(sys.argv[1])))" "$LB")")"
[ "$H" = "node=node-a" ] || fail "the address's claim is '$H', not node-a's"
echo "   node-b gone: $LB answered by node-a alone ($MAC_A); claim $H"

echo "== 8. a deleted Pod's address goes back =="
N_A="$(python3 -c "import ipaddress,sys;print(int(ipaddress.ip_address(sys.argv[1])))" "$IP_A")"
[ -n "$(get plane "/ipam/pod/$N_A")" ] || fail "srv-a's address holds no claim"
k delete pod srv-a --wait=false >/dev/null || fail "could not delete srv-a"
released() {
  [ -z "$(get plane "/ipam-lease/$UID_A")" ] && [ -z "$(get plane "/ipam-request/$UID_A")" ] \
    && [ -z "$(get plane "/ipam/pod/$N_A")" ]
}
wait_for 150 "srv-a's address was not released (lease '$(get plane "/ipam-lease/$UID_A")', request '$(get plane "/ipam-request/$UID_A")', claim '$(get plane "/ipam/pod/$N_A")'; Pod '$(k get pod srv-a -o name 2>&1)', sandbox '$(get plane "/~node/node-a/sandboxes/$UID_A")', its status '$(get plane "/~node/node-a/sandbox-status/$UID_A")')" released
echo "   srv-a deleted: its lease, its request and the claim on $IP_A gone"

echo "== E2E green: per-node Pod CIDRs, Pod IPs from them, probes to the Pod IP, Pod-to-Pod across nodes, hostNetwork, one announcer per load-balancer address with failover, addresses released =="
