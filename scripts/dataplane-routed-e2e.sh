#!/usr/bin/env bash
# Live E2E: the Service dataplane in real kernels, between routed Pods on two
# nodes — what each node's `net_apply` writes, realized by fluxor's net.policy
# provider as nftables, and judged by traffic.
#
#   lan   a client C (192.168.77.100), node A (.1) and node B (.2) on one
#         link; nothing else routes
#   A     the control plane's chains (fluxor-proxy.yaml, fluxor-netpolicy.yaml)
#         and the cluster DNS (fluxor-dns.yaml, fronted by its ClusterIP
#         Service), and A's enforcement (`net_apply`, node_name node-a), over
#         Services, slices and Pods seeded in its store
#   B     a worker's dataplane only — the proxy's and NetworkPolicy's
#         `net_apply`, node_name node-b — over the rows A computed (its
#         store a copy of A's, as a replica holds them)
#   Pods  network namespaces with a /32 address each, a veth to their node,
#         the node's side holding the Pod gateway; node A routes 10.244.2/24
#         to B and B 10.244.1/24 to A. Built HERE, by the test: per-node
#         CIDRs, `net=own` and route rows are the pod network's own work
#         (nc-net), not this dataplane's. Pod a1, a2 on A; b1, b2 on B; each
#         serves HTTP on 8080, answering "<pod> <client address>"
#
# Each step fails without the change it proves:
#   1. hairpin  a1 -> a ClusterIP whose one backend is a1: answered only
#               because the record says `hairpin` (else a1 gets its own
#               address as the source, a martian, and drops it)
#   2. NodePort Cluster, C -> A:31001 -> b1 on B: answered, b1 sees A's
#               address — `always` (without it b1 answers C directly, from an
#               address C never dialled)
#   3. NodePort Local, C -> A:31002 with backends a1 (here) and b1 (on B):
#               every answer from a1, which sees C's own address (per-node
#               endpoint filtering and `never`)
#   4. DNS      a1 and b1 resolve through the kube-dns ClusterIP (10.96.0.10)
#               — the Service cluster_dns publishes, DNATed by each node to it
#   5. hostPort C -> A:18080 -> a2:8080 (a2's hostPort), and not on B
#   6. NetworkPolicy on the WORKER: isolating b1 (ingress), B drops b2 -> b1
#               and still forwards the replies b1's own flows get; B's
#               /np-status/netpolicy says enforced=1
#
# Needs passwordless sudo (network namespaces, nftables); without it the
# suite skips, and NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEB="$ROOT/packaging/debian"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
sudo -n true 2>/dev/null || skip "passwordless sudo unavailable (network namespaces, nftables)"
for c in nft ip dig curl; do command -v "$c" >/dev/null || skip "$c not found"; done

D="$(mktemp -d "${TMPDIR:-/tmp}/nc-dprt-XXXXXX")"
chmod 755 "$D"
DA="$D/a"; DB="$D/b"
mkdir -p "$DA" "$DB"
# The nodes' host registry (FLUXOR_RUN_DIR): the runtimes run as root, and
# the registry refuses a directory another user owns or may write.
sudo -n install -d -m 0755 "$D/run"
P="ncrt$$"
LAN="${P}l"; NA="${P}a"; NB="${P}b"; NC="${P}c"
# Instance tags: distinct in their first six characters (a tag's stem names
# its links, so two live tags may not share one).
TAGA="a$$"; TAGB="b$$"
PODS="a1 a2 b1 b2"
cleanup() {
  for d in "$DA" "$DB"; do sudo -n pkill -f "$d/[c]onfig.bin" 2>/dev/null || true; done
  sudo -n pkill -f "$D/[s]erve.py" 2>/dev/null || true
  for n in $PODS; do sudo -n ip netns del "${P}$n" 2>/dev/null || true; done
  for n in "$LAN" "$NA" "$NB" "$NC"; do sudo -n ip netns del "$n" 2>/dev/null || true; done
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT
in_ns() { local n="$1"; shift; sudo -n ip netns exec "$n" "$@"; }
fail() {
  echo "FAIL: $1"
  local n t
  for n in "$NA:$TAGA" "$NB:$TAGB"; do
    for t in proxy netpolicy; do
      echo "--- ${n%%:*} fx_${n#*:}_$t"; in_ns "${n%%:*}" nft list table ip "fx_${n#*:}_$t" 2>&1 | head -40 || true
    done
  done
  for d in "$DA" "$DB"; do echo "--- $d/run.log"; grep -av "MON_\|DEBUG" "$d/run.log" 2>/dev/null | tail -${NC_LOG_LINES:-15} || true; done
  exit 1
}
D_SAVE="$D"
. "$ROOT/scripts/support/np.sh"   # np_put / np_last / np_keys over $D/store.log
on() { D="$1"; shift; "$@"; local r=$?; D="$D_SAVE"; return $r; }

echo "== 0. the graphs: A's chains, DNS and enforcement; B's enforcement alone =="
python3 - "$DEB" "$D/a.yaml" "$D/b.yaml" <<'PY'
import re, sys
deb, out_a, out_b = sys.argv[1:]
def parts(path):
    s = open(f"{deb}/{path}").read()
    mods, wiring = s.split("\nwiring:\n", 1)
    mods = mods.split("\nmodules:\n", 1)[1]
    blocks = [b for b in re.split(r"\n(?=  - name: )", "\n" + mods) if b.strip()]
    edges = re.findall(r"  - from: (\S+)\n    to: (\S+)\n", wiring + "\n")
    return blocks, edges
def name(b): return re.search(r"  - name: (\S+)", b).group(1)
def is_apply(b): return "\n    type: net_apply" in b
A, B, EA, EB = [], [], [], []
for g in ("fluxor-proxy.yaml", "fluxor-netpolicy.yaml", "fluxor-dns.yaml"):
    blocks, edges = parts(g)
    for b in blocks:
        b = b.strip("\n")
        if is_apply(b):
            for who, node, addr in ((A, "node-a", "192.168.77.1"), (B, "node-b", "192.168.77.2")):
                extra = f'\n    node_name: "{node}"\n    node_address: "{addr}"' if '"proxy"' in b else ""
                who.append(b + extra)
        else:
            if name(b) == "cluster_dns":
                b += '\n    address: "192.168.77.1"\n    service_ip: "10.96.0.10"'
            A.append(b)
    applies = {name(b) for b in blocks if is_apply(b)}
    for f, t in edges:
        EA.append((f, t))
        if f.split(".")[0] in applies and t.split(".")[0] in applies:
            EB.append((f, t))
head = "target: linux\ntick_us: 1000\n\nplatform:\n  net: {}\n\nscheduler:\n  accept_cycles: true\n\nmodules:\n"
def write(path, mods, edges, net):
    h = head if net else head.replace("platform:\n  net: {}\n\n", "")
    w = "".join(f"  - from: {f}\n    to: {t}\n" for f, t in edges)
    # A graph with no edges (B: two enforcers, each its own watcher) says
    # so: an empty `wiring:` is not a list.
    open(path, "w").write(h + "\n".join(mods) + ("\n\nwiring:\n" + w if w else "\n\nwiring: []\n"))
write(out_a, A, EA, True)
write(out_b, B, EB, False)
print("   A: %d modules; B: %d (%s)" % (len(A), len(B), ", ".join(name(b) for b in B)))
PY
nc_build_workload "$ROOT" "$D/a.yaml" "$DA/config.bin" "$DA/modules.bin" >/dev/null || fail "A's graph did not build"
nc_build_workload "$ROOT" "$D/b.yaml" "$DB/config.bin" "$DB/modules.bin" >/dev/null || fail "B's graph did not build"

echo "== 1. the network: a link, two nodes, four Pods routed by the test =="
for n in "$LAN" "$NA" "$NB" "$NC"; do sudo -n ip netns add "$n"; in_ns "$n" ip link set lo up; done
in_ns "$LAN" ip link add br0 type bridge
in_ns "$LAN" ip link set br0 up
lan_port() { # lan_port <ns> <addr>
  sudo -n ip link add "${1}u" netns "$LAN" type veth peer name eth0 netns "$1"
  in_ns "$LAN" ip link set "${1}u" master br0 up
  in_ns "$1" ip addr add "$2/24" dev eth0
  in_ns "$1" ip link set eth0 up
}
lan_port "$NA" 192.168.77.1
lan_port "$NB" 192.168.77.2
lan_port "$NC" 192.168.77.100
for n in "$NA" "$NB"; do in_ns "$n" sysctl -qw net.ipv4.ip_forward=1; done
in_ns "$NA" ip route add 10.244.2.0/24 via 192.168.77.2
in_ns "$NB" ip route add 10.244.1.0/24 via 192.168.77.1
# A Pod's route is plain: the cluster DNS on A answers each query from the
# address it was sent to (fluxor's linux_net replies with the query's own
# destination as its source), so a Pod on A resolves through its gateway's
# route as one on B does through the uplink.
pod_net() { # pod_net <pod> <node ns> <addr> <gateway>
  local ns="${P}$1"
  sudo -n ip netns add "$ns"
  in_ns "$ns" ip link set lo up
  sudo -n ip link add "v$1" netns "$2" type veth peer name eth0 netns "$ns"
  in_ns "$2" ip addr add "$4/32" dev "v$1"
  in_ns "$2" ip link set "v$1" up
  in_ns "$2" ip route add "$3/32" dev "v$1"
  in_ns "$ns" ip addr add "$3/32" dev eth0
  in_ns "$ns" ip link set eth0 up
  in_ns "$ns" ip route add "$4" dev eth0
  in_ns "$ns" ip route add default via "$4"
}
pod_net a1 "$NA" 10.244.1.2 10.244.1.1
pod_net a2 "$NA" 10.244.1.3 10.244.1.1
pod_net b1 "$NB" 10.244.2.2 10.244.2.1
pod_net b2 "$NB" 10.244.2.3 10.244.2.1
cat >"$D/serve.py" <<'PY'
import http.server, sys
name = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        b = f"{name} {self.client_address[0]}".encode()
        self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers()
        self.wfile.write(b)
    def log_message(self, *a): pass
http.server.ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
PY
for n in $PODS; do in_ns "${P}$n" python3 "$D/serve.py" "$n" >/dev/null 2>&1 & done
get() { # get <ns> <url> -> body, or empty
  in_ns "$1" curl -s --max-time 2 "$2" 2>/dev/null || true
}
for _ in $(seq 20); do [ -n "$(get "${P}a1" http://10.244.2.2:8080/)" ] && break; sleep 0.5; done
[ "$(get "${P}a1" http://10.244.2.2:8080/)" = "b1 10.244.1.2" ] || fail "Pods are not routed between the nodes"
echo "   a1 -> b1 directly: '$(get "${P}a1" http://10.244.2.2:8080/)'"

echo "== 2. the cluster's objects in A's store =="
pod() { # pod <name> <node> <ip> [ports-json]
  local ports='[{"containerPort":8080}]'
  [ -z "${4:-}" ] || ports="$4"
  np_put "/pods/default/$1" "{\"metadata\":{\"name\":\"$1\",\"namespace\":\"default\",\"labels\":{\"app\":\"$1\"}},\"spec\":{\"nodeName\":\"$2\",\"containers\":[{\"name\":\"c\",\"ports\":$ports}]},\"status\":{\"podIP\":\"$3\",\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\"}]}}"
}
svc() { # svc <name> <clusterIP> <extra spec members>
  np_put "/services/default/$1" "{\"metadata\":{\"name\":\"$1\",\"namespace\":\"default\"},\"spec\":{\"clusterIP\":\"$2\"$3}}"
}
seed() {
  pod a1 node-a 10.244.1.2
  pod a2 node-a 10.244.1.3 '[{"containerPort":8080,"hostPort":18080}]'
  pod b1 node-b 10.244.2.2
  pod b2 node-b 10.244.2.3
  svc self 10.96.0.20 ',"ports":[{"port":80,"targetPort":8080}]'
  np_put /endpointslices/default/self/a1 10.244.1.2
  svc rem 10.96.0.30 ',"type":"NodePort","ports":[{"port":80,"targetPort":8080,"nodePort":31001}]'
  np_put /endpointslices/default/rem/b1 10.244.2.2
  svc loc 10.96.0.40 ',"type":"NodePort","externalTrafficPolicy":"Local","ports":[{"port":80,"targetPort":8080,"nodePort":31002}]'
  np_put /endpointslices/default/loc/a1 10.244.1.2
  np_put /endpointslices/default/loc/b1 10.244.2.2
}
on "$DA" seed

# start <dir> <ns> <tag>: the node's runtime, as root in its namespace.
start() {
  in_ns "$2" env FLUXOR_INSTANCE="$3" FLUXOR_RUN_DIR="$D/run" FLUXOR_STORE_DIR="$1" RUST_LOG="${NC_RUST_LOG:-warn}" \
    "$FLUXOR_RUNTIME" --config "$1/config.bin" --modules "$1/modules.bin" >>"$1/run.log" 2>&1 &
}
stop() { # stop <dir>
  sudo -n pkill -f "$1/[c]onfig.bin" 2>/dev/null || true
  for _ in $(seq 40); do pgrep -f "$1/[c]onfig.bin" >/dev/null || return 0; sleep 0.25; done
  sudo -n pkill -KILL -f "$1/[c]onfig.bin" 2>/dev/null || true
}
wait_for() { # wait_for <seconds> <what> <cmd...>
  local secs="$1" what="$2"; shift 2
  local end=$((SECONDS + secs))
  until "$@"; do [ "$SECONDS" -lt "$end" ] || fail "$what (after ${secs}s)"; sleep 0.5; done
}
enforced() { # enforced <dir> <table>
  case "$(on "$1" np_last "/np-status/$2")" in enforced=1\;*) return 0 ;; *) return 1 ;; esac
}
# grep reads nft's listing whole (no -q): under pipefail a grep that quits at
# its first match leaves nft writing into a closed pipe, and the SIGPIPE fails
# the check once the table outgrows the pipe's buffer.
rule() { in_ns "$1" nft list table ip "fx_${2}_proxy" 2>/dev/null | grep -F "$3" >/dev/null; }

start "$DA" "$NA" "$TAGA"
wait_for 60 "A's proxy table was never enforced: '$(on "$DA" np_last /np-status/proxy)'" enforced "$DA" proxy
wait_for 30 "A never DNATed the kube-dns ClusterIP" rule "$NA" "$TAGA" "ip daddr 10.96.0.10 udp dport 53 dnat to 192.168.77.1:53"
wait_for 30 "A never DNATed rem's nodePort" rule "$NA" "$TAGA" "ip daddr 192.168.77.1 tcp dport 31001 dnat to 10.244.2.2:8080"
wait_for 30 "A never DNATed a2's hostPort" rule "$NA" "$TAGA" "ip daddr 192.168.77.1 tcp dport 18080 dnat to 10.244.1.3:8080"
echo "   A: $(on "$DA" np_last /np-status/proxy); /cluster-dns = $(on "$DA" np_last /cluster-dns)"
# B holds what the cluster computed: A's store, as its replica would.
replicate() {
  stop "$DB"
  cp "$DA/store.log" "$DB/store.log"
  # A node's status is its own (scoped under /~node/ in a cluster).
  on "$DB" np_put /np-status/proxy -DELETE-
  on "$DB" np_put /np-status/netpolicy -DELETE-
  start "$DB" "$NB" "$TAGB"
}
replicate
wait_for 60 "B's proxy table was never enforced: '$(on "$DB" np_last /np-status/proxy)'" enforced "$DB" proxy
echo "   B: $(on "$DB" np_last /np-status/proxy)"

echo "== 3. hairpin: a1 through a ClusterIP to itself =="
R="$(get "${P}a1" http://10.96.0.20/)"
[ "$R" = "a1 10.244.1.1" ] || fail "a1 -> self (10.96.0.20) -> a1: '$R' (want its answer, the source translated to the gateway)"
echo "   a1 -> 10.96.0.20 -> a1: '$R'"
R="$(get "${P}a2" http://10.96.0.20/)"
[ "$R" = "a1 10.244.1.3" ] || fail "a2 -> self -> a1: '$R' (another client keeps its address)"
echo "   a2 -> 10.96.0.20 -> a1: '$R' (not a hairpin: its own address kept)"

echo "== 4. NodePort, externalTrafficPolicy Cluster: C -> A -> b1 on B =="
R="$(get "$NC" http://192.168.77.1:31001/)"
[ "$R" = "b1 192.168.77.1" ] || fail "C -> A:31001 -> b1: '$R' (want b1, seeing A's address)"
echo "   C -> 192.168.77.1:31001: '$R'"
R="$(get "${P}a1" http://10.96.0.30/)"
[ "${R%% *}" = b1 ] || fail "a1 -> rem's ClusterIP -> b1 (another node): '$R'"
echo "   a1 -> 10.96.0.30 (rem) -> b1 on B: '$R'"

echo "== 5. NodePort, externalTrafficPolicy Local: only A's own backend, C's address kept =="
for i in $(seq 12); do
  R="$(get "$NC" http://192.168.77.1:31002/)"
  [ "$R" = "a1 192.168.77.100" ] || fail "C -> A:31002 (Local), try $i: '$R' (want a1 seeing C)"
done
echo "   12 of 12 from a1, seeing 192.168.77.100"
R="$(get "$NC" http://192.168.77.2:31002/)"
[ "$R" = "b1 192.168.77.100" ] || fail "C -> B:31002 (Local): '$R' (want B's own backend b1)"
echo "   C -> 192.168.77.2:31002: '$R' (B's own)"

echo "== 6. DNS through the kube-dns ClusterIP, from both nodes' Pods =="
dns() { in_ns "$1" dig +short +time=2 +tries=2 "@${3:-10.96.0.10}" "$2" A 2>/dev/null || true; }
R="$(dns "${P}a1" rem.default.svc.cluster.local 192.168.77.1)"
[ "$R" = 10.96.0.30 ] || fail "a1: dig @192.168.77.1 (the resolver itself) rem.default.svc.cluster.local -> '$R'; from A itself: '$(dns "$NA" rem.default.svc.cluster.local 192.168.77.1)'; sockets: $(in_ns "$NA" ss -uanp 2>&1 | tr '\n' ' ')"
R="$(dns "${P}a1" rem.default.svc.cluster.local)"
[ "$R" = 10.96.0.30 ] || fail "a1: dig @10.96.0.10 rem.default.svc.cluster.local -> '$R'"
R2="$(dns "${P}b1" loc.default.svc.cluster.local)"
[ "$R2" = 10.96.0.40 ] || fail "b1 (on B): dig @10.96.0.10 loc.default.svc.cluster.local -> '$R2'"
[ "$(on "$DA" np_last /cluster-dns)" = 10.96.0.10 ] || fail "/cluster-dns is '$(on "$DA" np_last /cluster-dns)', not the ClusterIP"
case "$(on "$DA" np_last /services/kube-system/kube-dns)" in *'"clusterIP":"10.96.0.10"'*) ;; *) fail "no kube-dns Service" ;; esac
echo "   a1: rem -> $R; b1: loc -> $R2 — through 10.96.0.10 (kube-system/kube-dns -> 192.168.77.1:53)"

echo "== 7. hostPort: C -> A:18080 -> a2, on a2's node only =="
R="$(get "$NC" http://192.168.77.1:18080/)"
[ "$R" = "a2 192.168.77.100" ] || fail "C -> A:18080 -> a2: '$R'"
[ -z "$(get "$NC" http://192.168.77.2:18080/)" ] || fail "B answers a2's hostPort"
echo "   C -> 192.168.77.1:18080: '$R'; B: nothing"

echo "== 8. NetworkPolicy on the worker: b1 isolated, B drops b2 -> b1 =="
[ "$(get "${P}b2" http://10.244.2.2:8080/)" = "b1 10.244.2.3" ] || fail "b2 -> b1 before any policy"
stop "$DA"
on "$DA" np_put /networkpolicies/default/deny-b1 '{"metadata":{"name":"deny-b1","namespace":"default"},"spec":{"podSelector":{"matchLabels":{"app":"b1"}},"policyTypes":["Ingress"]}}'
start "$DA" "$NA" "$TAGA"
isolated() { case "$(on "$DA" np_keys /np-rows/netpolicy/)" in *10.244.2.2*|*b1*) return 0 ;; *) return 1 ;; esac; }
wait_for 60 "A never computed b1's isolation: $(on "$DA" np_keys /np-rows/netpolicy/)" isolated
sleep 2
replicate
wait_for 60 "B's netpolicy table was never enforced: '$(on "$DB" np_last /np-status/netpolicy)'" enforced "$DB" netpolicy
in_ns "$NB" nft list table ip "fx_${TAGB}_netpolicy" | grep -F "ip daddr 10.244.2.2 drop" >/dev/null || fail "B's kernel holds no isolation for b1"
R="$(get "${P}b2" http://10.244.2.2:8080/)"
[ -z "$R" ] || fail "b2 -> b1 answered under deny-b1: '$R'"
R="$(get "${P}b1" http://10.244.2.3:8080/)"
[ "$R" = "b2 10.244.2.2" ] || fail "b1's own flow to b2 lost its reply: '$R'"
echo "   B: $(on "$DB" np_last /np-status/netpolicy); b2 -> b1 dropped; b1 -> b2 answered (replies pass)"

echo "== E2E green: the Service dataplane between routed Pods — hairpin, NodePort Cluster and Local, kube-dns, hostPort, NetworkPolicy on a worker =="
