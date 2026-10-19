#!/usr/bin/env bash
# Live E2E: the INSTALLED node's dataplane — a fresh install's single node,
# rendered exactly as the deb renders it, serving a Deployment behind a
# ClusterIP Service, by name and by its virtual address, and enforcing a
# NetworkPolicy in the kernel.
#
#   1. the deb's /etc/nanocloud.io/fluxor, staged from packaging/debian/
#      build.sh's own install lines, and the cluster.env postinst writes on a
#      fresh install (NANOCLOUD_PODS=1)
#   2. the renders: the control plane carries the Service plane (endpoints,
#      the cluster DNS, the proxy's and NetworkPolicy's chains); every node
#      — the single node and a worker — carries the enforcement of it (the
#      proxy's and NetworkPolicy's net_apply), and only that
#   3. the node runs as root, as the deb's service does, in a network
#      namespace of its own (one address, 10.200.0.1, its default route), so
#      its nftables tables and its DNS on :53 are the test's alone
#   4. a Deployment of a Pod serving HTTP on 8080 (its port named `http`),
#      and two ClusterIP Services: `web` (80 -> 8080) and `web-named`
#      (81 -> the name `http`). The Pod's Endpoints, then:
#        - web.default.svc.cluster.local resolves, through the node's DNS,
#          to web's ClusterIP, and `curl http://web.default.svc.cluster.local/`
#          is answered by the Pod — DNAT in the kernel (nft table nc_proxy);
#        - web-named's ClusterIP:81 is answered too: the name resolved against
#          the Pod's own ports;
#        - `kubectl logs` of the Pod reads what it wrote, from the node's own
#          records
#   5. a NetworkPolicy isolating the Pod is enforced (nc_netpolicy drops what
#      is addressed to it), then one allowing 10.0.0.0/8 to its port opens
#      that and only that; removing both lifts the isolation. The rules are
#      read back from the kernel, and the node's /np-status/ says enforced=1.
#      (A Pod sharing its node's network — every Pod here, until Pods get
#      network namespaces of their own — is reached through the node's INPUT
#      path, which the forward-hook policy does not see: as upstream, a
#      NetworkPolicy does not constrain a hostNetwork Pod's traffic. What is
#      proven is that the policy is computed for the Pod's address and the
#      kernel holds it.)
#
# Needs passwordless sudo (a network namespace, nftables, :53, the node as
# root); without it the suite skips, and NANOCLOUD_REQUIRE_E2E=1 makes that a
# failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
NODE_IP=10.200.0.1

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
sudo -n true 2>/dev/null || skip "passwordless sudo unavailable (the node runs as root)"
for c in nft ip dig curl redis-cli; do
  command -v "$c" >/dev/null || skip "$c not found"
done

D="$(mktemp -d /tmp/nc-dataplane-XXXXXX)"
chmod 755 "$D"
STORE="$D/store"
mkdir -p "$STORE" "$D/etc/fluxor" "$D/lib" "$D/owners"
nc_lattice_prepare "$STORE"
. "$ROOT/scripts/support/workload-node.sh"
NS="nc-dp-$$"
NSX="sudo -n ip netns exec $NS"
RUNTIME_PID=""
RPORT=6380
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then
    sudo -n pkill -f "$D/[c]onfig.bin" 2>/dev/null || true
    for _ in $(seq 20); do pgrep -f "$D/[c]onfig.bin" >/dev/null || break; sleep 0.5; done
    sudo -n pkill -KILL -f "$D/[c]onfig.bin" 2>/dev/null || true
  fi
  # The Pod's server, whatever outlived the node.
  sudo -n pkill -f "$D/[w]ww" 2>/dev/null || true
  sudo -n ip netns del "$NS" 2>/dev/null || true
  sudo -n rm -rf "/etc/netns/$NS" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT
fail() {
  echo "FAIL: $1"
  if [ -n "$RUNTIME_PID" ]; then
    local k
    for k in $($NSX redis-cli -p "$RPORT" --raw KEYS '*' 2>/dev/null | sed 1d | grep -a "/web\|/~node/node-a/\(pod-\|sandbox\)" | sort | head -40); do
      printf '   %-64s %s\n' "$k" "$($NSX redis-cli -p "$RPORT" --raw GET "$k" 2>/dev/null | head -c 160)"
    done
  fi
  grep -av "MON_\|DEBUG\|INFO" "$D/run.log" 2>/dev/null | tail -30 || true
  exit 1
}
DEB="$ROOT/packaging/debian"

echo "== 1. the deb's tree and its fresh cluster.env =="
python3 - "$DEB/build.sh" "$DEB" "$D/etc/fluxor" <<'PY'
import re, shutil, sys, os
build, deb, out = sys.argv[1:]
for src, dst in re.findall(r'install -Dm644 "\$\{SCRIPT_DIR\}/([^"]+)"\s+"\$\{STAGING_ROOT\}/etc/nanocloud\.io/fluxor/([^"]+)"',
                           open(build).read()):
    shutil.copy(os.path.join(deb, src), os.path.join(out, dst))
for need in ("controlplane.yaml", "worker.yaml", "endpoints.yaml", "dns.yaml", "proxy.yaml", "netpolicy.yaml"):
    if not os.path.exists(os.path.join(out, need)):
        sys.exit(f"build.sh does not install {need}")
print("   staged as installed: " + " ".join(sorted(os.listdir(out))))
PY
cp "$DEB/nanocloud-graph.sh" "$D/lib/nanocloud-graph.sh"
sed -n '/cat >"\$ENV" <<EOF/,/^EOF$/p' "$DEB/postinst" | sed '1d;$d' | sed "s/\$HOST/node-a/g" >"$D/cluster.env"
grep -qx 'NANOCLOUD_PODS=1' "$D/cluster.env" || fail "a fresh install's cluster.env does not run Pods"
SVC_ENV="$( { sed -n 's/^Environment=//p' "$DEB/nanocloud.service"; grep -E '^[A-Z_]+=' "$DEB/nanocloud.default"; } )"
nc_node_pki "$STORE"

render() { # render <role> <out>: nanocloud-graph.sh on the installed tree, as the unit runs it
  env -i PATH="$PATH" HOME="$HOME" $(printf '%s\n' "$SVC_ENV" | grep -v '^FLUXOR_' | tr '\n' ' ') \
    NANOCLOUD_CLUSTER_ENV="$D/cluster.env" NANOCLOUD_ROLE="$1" \
    NANOCLOUD_PKI="$STORE/pki" NANOCLOUD_FLUXOR_ABI="$(sh "$ROOT/scripts/fluxor-abi.sh" "$ROOT")" \
    NANOCLOUD_NODE_ADDRESS="$NODE_IP" NANOCLOUD_DNS_UPSTREAM="" ${NP:+NANOCLOUD_NETPOLICY=$NP} \
    NANOCLOUD_LB_POOL=10.250.0.0/24 \
    sh "$D/lib/nanocloud-graph.sh" "$D/etc/fluxor" "$2"
}
has() { grep -qx "  - name: $2" "$1"; }

echo "== 2. the renders: the Service plane on the control plane, its enforcement on every node =="
# As installed: NetworkPolicy off (its chain does not fit beside the image
# pull in the runtime's config arena), the rest on.
NP="" render plane "$D/default.rendered" 2>/dev/null || fail "the plane did not render"
for m in ep_pairs cluster_dns px_src px_apply; do has "$D/default.rendered" "$m" || fail "$m is not in the default render"; done
grep -q '^  - name: np_' "$D/default.rendered" && fail "NetworkPolicy is rendered by default"
# The operator's opt-in (cluster.env NANOCLOUD_NETPOLICY=1), from here on.
NP=1
render node "$D/worker.yaml" 2>/dev/null || fail "the worker did not render"
for m in px_apply np_apply; do has "$D/worker.yaml" "$m" || fail "$m is not in the worker's graph"; done
for m in ep_pairs cluster_dns px_src np_selsrc; do
  has "$D/worker.yaml" "$m" && fail "the worker runs the cluster's $m"
done
render plane "$D/plane.rendered" 2>/dev/null || fail "the plane did not render"
for m in ep_pairs ep_write cluster_dns px_src px_pod px_rows px_prune np_selsrc np_allow px_apply np_apply; do
  has "$D/plane.rendered" "$m" || fail "$m is not in the single node's graph"
done
[ "$(grep -c '^    type: net_apply$' "$D/plane.rendered")" = 2 ] || fail "the single node does not enforce exactly once"
grep -q '^    port: 53$' "$D/plane.rendered" || fail "the cluster DNS is not on :53"
grep -q '/np-status/' "$D/plane.rendered" || fail "the node's dataplane status is not in its scope"
echo "   worker: px_apply, np_apply only; the single node: the Service plane and its enforcement ($(grep -c '^  - name: ' "$D/plane.rendered") modules)"
python3 - "$D/plane.rendered" "$D/graph.yaml" "$STORE" "$RPORT" <<'PY'
import sys
src, out, store, rport = sys.argv[1:]
g = open(src).read()
for a, b in (("/etc/nanocloud.io/certs/server.der", f"{store}/server.der"),
             ("/etc/nanocloud.io/certs/server.key.der", f"{store}/server.key.der")):
    assert g.count(a) == 1, a
    g = g.replace(a, b)
mods, wiring = g.split("\nwiring:\n", 1)
mods += ("\n  - name: redis_edge_anchor\n    params:\n      listen_port: " + rport + "\n")
wiring = wiring.rstrip("\n") + (
    "\n  - from: linux_net.net_out\n    to: redis_edge_anchor.net_in\n    buffer_bytes: 65536\n"
    "  - from: redis_edge_anchor.net_out\n    to: linux_net.net_in\n"
    "  - from: redis_edge_anchor.kv_out\n    to: kv_request_router.redis_in\n"
    "  - from: kv_request_router.redis_out\n    to: redis_edge_anchor.kv_in\n")
open(out, "w").write(mods + "\nwiring:\n" + wiring)
PY
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" || fail "the single node's graph did not build"

echo "== 3. the node, as root, in a network namespace of its own ($NODE_IP) =="
sudo -n ip netns add "$NS"
$NSX ip link set lo up
$NSX ip link add dummy0 type dummy
$NSX ip addr add "$NODE_IP/24" dev dummy0
$NSX ip link set dummy0 up
$NSX ip route add default dev dummy0
# Inside the namespace the resolver is the node's: `ip netns exec` binds it
# over /etc/resolv.conf, with the cluster's search path.
sudo -n mkdir -p "/etc/netns/$NS"
printf 'nameserver %s\nsearch default.svc.cluster.local svc.cluster.local cluster.local\n' "$NODE_IP" \
  | sudo -n tee "/etc/netns/$NS/resolv.conf" >/dev/null
WN_NO_NODE=1 wn_seed_identity
$NSX env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" FLUXOR_VAULT_DIR="$STORE/vault" RUST_LOG=info \
  "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
api() { # api <method> <path> [json-body] -> "<status>|<body>", inside the namespace
  $NSX python3 - "$PORT" "$D/token.admin" "$1" "$2" "${3:-}" <<'PY'
import socket, ssl, sys
port, tokf, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
tok = open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
try:
    s = ctx.wrap_socket(socket.create_connection(("127.0.0.1", port), timeout=10), server_hostname="localhost")
except OSError as e:
    print(f"000|{e}"); sys.exit(0)
b = body.encode()
s.sendall((f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n"
           f"Content-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b)
buf = b""
s.settimeout(10)
try:
    while True:
        c = s.recv(4096)
        if not c: break
        buf += c
except (socket.timeout, ssl.SSLError):
    pass
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}
sget() { $NSX redis-cli -p "$RPORT" --raw GET "$1" 2>/dev/null || true; }
jget() { python3 -c 'import json,sys
o = json.loads(sys.stdin.read().split("|", 1)[1] or "{}")
for k in sys.argv[1].split("."):
    o = o.get(k, {}) if isinstance(o, dict) else {}
print(o if o != {} else "")' "$1"; }
wait_for() { # wait_for <seconds> <what> <cmd...>
  local secs="$1" what="$2"; shift 2
  local end=$((SECONDS + secs))
  until "$@"; do
    [ "$SECONDS" -lt "$end" ] || fail "$what (after ${secs}s)"
    pgrep -f "$D/[c]onfig.bin" >/dev/null || fail "the node exited"
    sleep 1
  done
}
up() { case "$(api GET /version)" in 200\|*) return 0 ;; *) return 1 ;; esac; }
wait_for 90 "no API" up
ready() { [ "$(api GET /api/v1/nodes/node-a | jget status.ready)" = True ]; }
wait_for 60 "node-a never registered itself Ready" ready
grep -aq "\[cluster_dns\] serving" "$D/run.log" || fail "the cluster DNS is not serving"
echo "   API up; node-a Ready; the cluster DNS serving on $NODE_IP:53"

echo "== 4. a Deployment behind two ClusterIP Services =="
# What the Pod serves (its command is short: a Deployment's template rides
# the controllers' records whole).
mkdir -p "$D/www"
echo "hello from the web Pod" >"$D/www/index.html"
DEPLOY="$(python3 - "$D/www" <<'PY'
import json, sys
print(json.dumps({"apiVersion": "apps/v1", "kind": "Deployment",
  "metadata": {"name": "web", "namespace": "default"},
  "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "web"}},
    "template": {"metadata": {"labels": {"app": "web"}},
      "spec": {"containers": [{"name": "web",
        "command": ["python3", "-u", "-m", "http.server", "8080", "--directory", sys.argv[1]],
        "ports": [{"name": "http", "containerPort": 8080}]}]}}}}))
PY
)"
r="$(api POST /apis/apps/v1/namespaces/default/deployments "$DEPLOY")"
case "$r" in 20[01]\|*) ;; *) fail "POST deployment: ${r:0:200}" ;; esac
svc() { # svc <name> <port> <targetPort-json>
  api POST /api/v1/namespaces/default/services \
    "{\"apiVersion\":\"v1\",\"kind\":\"Service\",\"metadata\":{\"name\":\"$1\",\"namespace\":\"default\"},\"spec\":{\"selector\":{\"app\":\"web\"},\"ports\":[{\"port\":$2,\"targetPort\":$3}]}}"
}
r="$(svc web 80 8080)"; case "$r" in 20[01]\|*) ;; *) fail "POST service web: ${r:0:200}" ;; esac
r="$(svc web-named 81 '"http"')"; case "$r" in 20[01]\|*) ;; *) fail "POST service web-named: ${r:0:200}" ;; esac
cip() { api GET "/api/v1/namespaces/default/services/$1" | jget spec.clusterIP; }
CIP=""; CIP2=""
for _ in $(seq 30); do CIP="$(cip web)"; CIP2="$(cip web-named)"; [ -n "$CIP" ] && [ -n "$CIP2" ] && break; sleep 1; done
[ -n "$CIP" ] && [ -n "$CIP2" ] || fail "no ClusterIPs: web='$CIP' web-named='$CIP2'"
echo "   web -> $CIP:80 (8080); web-named -> $CIP2:81 (http)"
POD=""
podname() { POD="$(api GET "/api/v1/namespaces/default/pods" | python3 -c 'import json,sys
o = json.loads(sys.stdin.read().split("|", 1)[1])
print(next((p["metadata"]["name"] for p in o.get("items", []) if p["metadata"]["name"].startswith("web-")
            and p.get("status", {}).get("phase") == "Running"), ""))')"; [ -n "$POD" ]; }
wait_for 90 "the Deployment's Pod never ran" podname
eps() { case "$(sget /endpoints/default/web)" in *"=$NODE_IP"*) return 0 ;; *) return 1 ;; esac; }
wait_for 60 "web's Endpoints never held the Pod ($(sget /endpoints/default/web))" eps
echo "   $POD Running; /endpoints/default/web = $(sget /endpoints/default/web)"

echo "== 5. by name, through the ClusterIP: DNS, then DNAT in the kernel =="
R="$($NSX dig +short +time=2 +tries=2 "@$NODE_IP" web.default.svc.cluster.local A)"
[ "$R" = "$CIP" ] || fail "web.default.svc.cluster.local -> '$R', want $CIP"
echo "   dig web.default.svc.cluster.local -> $R"
enforced() { case "$(sget "/~node/node-a/np-status/$1")" in enforced=1\;*) return 0 ;; *) return 1 ;; esac; }
wait_for 30 "the proxy table was never enforced: '$(sget /~node/node-a/np-status/proxy)'" enforced proxy
answered() { case "$($NSX curl -s --max-time 3 "$1" 2>/dev/null)" in "hello from the web Pod") return 0 ;; *) return 1 ;; esac; }
wait_for 30 "http://web.default.svc.cluster.local/ was never answered by $POD ($($NSX nft list table ip nc_proxy 2>&1 | grep dnat))" \
  answered http://web.default.svc.cluster.local/
answered "http://web/" || fail "the search path's short name was not answered"
echo "   curl http://web.default.svc.cluster.local/ (and http://web/) -> $POD's page"
wait_for 30 "web-named's ClusterIP:81 was never answered" answered "http://$CIP2:81/"
$NSX nft list table ip nc_proxy | grep -q "ip daddr $CIP2 tcp dport 81 dnat to $NODE_IP:8080" \
  || fail "web-named's rule does not lead to the Pod's port named http: $($NSX nft list table ip nc_proxy)"
echo "   curl http://$CIP2:81/ -> $POD's page (targetPort http = the Pod's 8080); /np-status/proxy: $(sget /~node/node-a/np-status/proxy)"

[ "$(sget /cluster-dns)" = "$NODE_IP" ] || fail "/cluster-dns = '$(sget /cluster-dns)': the Pods' nameserver is not the node's DNS"
echo "   /cluster-dns = $NODE_IP (the nameserver a Pod's resolv.conf names)"

echo "== 5b. NodePort and LoadBalancer: allocated, then answered at the node's port and the pool's address =="
svct() { # svct <name> <type> <port>
  api POST /api/v1/namespaces/default/services \
    "{\"apiVersion\":\"v1\",\"kind\":\"Service\",\"metadata\":{\"name\":\"$1\",\"namespace\":\"default\"},\"spec\":{\"type\":\"$2\",\"selector\":{\"app\":\"web\"},\"ports\":[{\"port\":$3,\"targetPort\":8080}]}}"
}
r="$(svct web-np NodePort 82)"; case "$r" in 20[01]\|*) ;; *) fail "POST web-np: ${r:0:200}" ;; esac
r="$(svct web-lb LoadBalancer 83)"; case "$r" in 20[01]\|*) ;; *) fail "POST web-lb: ${r:0:200}" ;; esac
nodeport() { api GET "/api/v1/namespaces/default/services/$1" | python3 -c 'import json,sys
o = json.loads(sys.stdin.read().split("|", 1)[1])
print((o.get("spec", {}).get("ports") or [{}])[0].get("nodePort", ""))'; }
lbip() { api GET /api/v1/namespaces/default/services/web-lb | python3 -c 'import json,sys
o = json.loads(sys.stdin.read().split("|", 1)[1])
print(((o.get("status", {}).get("loadBalancer", {}) or {}).get("ingress") or [{}])[0].get("ip", ""))'; }
NPORT=""; LPORT=""; LBIP=""
for _ in $(seq 30); do NPORT="$(nodeport web-np)"; LPORT="$(nodeport web-lb)"; LBIP="$(lbip)"
  [ -n "$NPORT" ] && [ -n "$LPORT" ] && [ -n "$LBIP" ] && break; sleep 1; done
[ -n "$NPORT" ] && [ "$NPORT" -ge 30000 ] && [ "$NPORT" -le 32767 ] || fail "web-np was given no nodePort: '$NPORT'"
[ "$LBIP" = 10.250.0.1 ] || fail "web-lb was given no address of the pool: '$LBIP'"
[ -n "$LPORT" ] && [ "$LPORT" != "$NPORT" ] || fail "web-lb's nodePort: '$LPORT'"
wait_for 30 "web-np's nodePort $NPORT was never answered" answered "http://$NODE_IP:$NPORT/"
wait_for 30 "web-lb's $LBIP:83 was never answered" answered "http://$LBIP:83/"
answered "http://$NODE_IP:$LPORT/" || fail "web-lb's own nodePort $LPORT was not answered"
echo "   web-np: $NODE_IP:$NPORT -> the Pod; web-lb: $LBIP:83 (status.loadBalancer.ingress) and $NODE_IP:$LPORT -> the Pod"

echo "== 5c. a Service deleted: its Endpoints and slices go with it =="
[ -n "$(sget /endpoints/default/web-named)" ] || fail "web-named has no Endpoints before its delete"
r="$(api DELETE /api/v1/namespaces/default/services/web-named)"
case "$r" in 200\|*|202\|*) ;; *) fail "DELETE web-named: ${r:0:200}" ;; esac
gone() { [ -z "$(sget /endpoints/default/web-named)" ] && [ -z "$(sget "/endpointslices/default/web-named/$POD")" ]; }
wait_for 30 "web-named's Endpoints or slice was left behind" gone
r="$(api GET /api/v1/namespaces/default/endpoints/web-named)"
case "$r" in 404\|*) ;; *) fail "GET endpoints/web-named after its Service's delete: ${r:0:120}" ;; esac
echo "   /endpoints/default/web-named and its slice removed; GET endpoints/web-named -> 404"

echo "== 6. kubectl logs of the Pod: its node's record of what it wrote =="
r="$(api GET "/api/v1/namespaces/default/pods/$POD/log")"
case "$r" in 200\|"Serving HTTP on "*) echo "   GET pods/$POD/log -> ${r#*|}" ;; *) fail "the Pod's log: '${r:0:200}'" ;; esac

echo "== 7. a NetworkPolicy isolates the Pod; another allows 10.0.0.0/8 to its port =="
policy() { # policy <name> <spec-json>
  api POST /apis/networking.k8s.io/v1/namespaces/default/networkpolicies \
    "{\"apiVersion\":\"networking.k8s.io/v1\",\"kind\":\"NetworkPolicy\",\"metadata\":{\"name\":\"$1\",\"namespace\":\"default\"},\"spec\":$2}"
}
nptable() { $NSX nft list table ip nc_netpolicy 2>/dev/null || true; }
r="$(policy deny-web '{"podSelector":{"matchLabels":{"app":"web"}},"policyTypes":["Ingress"]}')"
case "$r" in 20[01]\|*) ;; *) fail "POST deny-web: ${r:0:200}" ;; esac
drops() { nptable | grep -q "ip daddr $NODE_IP drop"; }
wait_for 60 "the isolation never reached the kernel: $(sget /~node/node-a/np-status/netpolicy)" drops
nptable | grep -q "dport.*accept" && fail "an isolated Pod with no allowing policy has an accept rule: $(nptable)"
enforced netpolicy || fail "the node does not say it enforces the policy: $(sget /~node/node-a/np-status/netpolicy)"
echo "   deny-web: nc_netpolicy drops what is addressed to $NODE_IP; $(sget /~node/node-a/np-status/netpolicy)"
r="$(policy allow-web '{"podSelector":{"matchLabels":{"app":"web"}},"policyTypes":["Ingress"],"ingress":[{"from":[{"ipBlock":{"cidr":"10.0.0.0/8"}}],"ports":[{"port":8080,"protocol":"TCP"}]}]}')"
case "$r" in 20[01]\|*) ;; *) fail "POST allow-web: ${r:0:200}" ;; esac
allows() { nptable | grep -q "ip daddr $NODE_IP ip saddr 10.0.0.0/8 tcp dport 8080 accept"; }
wait_for 60 "the allow never reached the kernel: $(nptable)" allows
drops || fail "the allow lifted the isolation of everything else"
echo "   allow-web: 10.0.0.0/8 -> :8080 accepted ahead of the drop"
for p in deny-web allow-web; do
  r="$(api DELETE "/apis/networking.k8s.io/v1/namespaces/default/networkpolicies/$p")"
  case "$r" in 200\|*|202\|*) ;; *) fail "DELETE $p: ${r:0:200}" ;; esac
done
lifted() { ! nptable | grep -q "ip daddr $NODE_IP"; }
wait_for 60 "removing the policies never lifted the isolation: $(nptable)" lifted
answered "http://$CIP/" || fail "the Service stopped answering"
echo "   both removed: no rule addresses the Pod; the Service still answers"

echo "== E2E green: an installed node's dataplane — Endpoints, cluster DNS, ClusterIP DNAT (named targetPorts too), logs and NetworkPolicy, enforced in its kernel =="
