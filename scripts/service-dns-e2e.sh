#!/usr/bin/env bash
# Live E2E for the cluster's DNS (modules/app/cluster_dns, the graph
# packaging/debian/fluxor-dns.yaml): real queries over UDP to the running
# graph, answered from the store as the API keeps it.
#
#   <svc>.<ns>.svc.cluster.local  A  -> the Service's ClusterIP
#                                       a headless Service: its ready
#                                       backends (/endpoints/<ns>/<svc>)
#                                       an ExternalName Service: a CNAME
#   _<port>._<proto>.<svc>.<ns>.svc.cluster.local  SRV
#                                    -> the named port: at the Service's
#                                       name, or (headless) each backend's
#                                       `<a-b-c-d>.<svc>.<ns>.svc…` at its
#                                       targetPort — which resolves too
#   a name in the domain that is no Service        -> NXDOMAIN
#   AAAA of a Service                               -> NOERROR, no answer
#   a name outside the domain                       -> forwarded upstream,
#                                                      the client's id restored
#
# The store is the host store seeded from its append-log ($D/store.log) at
# boot; phase B appends a deleted Service and a changed ClusterIP and boots
# again: the answers follow the store, with nothing projected in between.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GRAPH="$ROOT/packaging/debian/fluxor-dns.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
[ -e "$FLUXOR_RUNTIME" ] || { echo "FAIL: missing $FLUXOR_RUNTIME"; exit 1; }

D="$(mktemp -d /tmp/nc-dns-e2e-XXXXXX)"
PID=""
UP_PID=""
cleanup() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  [ -n "$UP_PID" ] && kill "$UP_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/np.sh"   # np_put: the store's append-log

DNS_PORT=$((25300 + ($$ % 400)))
UP_PORT=$((DNS_PORT + 400))

# q <name> [type] -> "<rcode> aa=<0|1> id=<ok|BAD> <records…>": an A as its
# address, a CNAME as CNAME:<name>, an SRV as SRV:<port>:<target>
q() {
  python3 - "$DNS_PORT" "$1" "${2:-A}" <<'PY'
import random, socket, struct, sys
port, name, qt = int(sys.argv[1]), sys.argv[2], {"A": 1, "AAAA": 28, "SRV": 33, "CNAME": 5}[sys.argv[3]]
qid = random.randrange(65536)
pkt = struct.pack(">HHHHHH", qid, 0x0100, 1, 0, 0, 0)
for l in name.split("."):
    pkt += bytes([len(l)]) + l.encode()
pkt += b"\0" + struct.pack(">HH", qt, 1)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(2)
try:
    s.sendto(pkt, ("127.0.0.1", port))
    r, _ = s.recvfrom(1500)
except socket.timeout:
    print("TIMEOUT"); sys.exit(0)
rid, fl, qd, an = struct.unpack(">HHHH", r[:8])
rc = {0: "NOERROR", 2: "SERVFAIL", 3: "NXDOMAIN", 5: "REFUSED"}.get(fl & 0xF, str(fl & 0xF))
at = 12
while r[at]: at += 1 + r[at]
at += 5
ips = []
for _ in range(an):
    if r[at] & 0xC0 == 0xC0: at += 2
    else:
        while r[at]: at += 1 + r[at]
        at += 1
    t, c, ttl, rl = struct.unpack(">HHIH", r[at:at+10]); at += 10
    def name(i):
        out = []
        while r[i]:
            out.append(r[i+1:i+1+r[i]].decode()); i += 1 + r[i]
        return ".".join(out)
    if t == 1: ips.append(".".join(str(b) for b in r[at:at+4]))
    if t == 5: ips.append("CNAME:" + name(at))
    if t == 33: ips.append("SRV:%d:%s" % (struct.unpack(">H", r[at+4:at+6])[0], name(at + 6)))
    at += rl
print(rc, "aa=%d" % ((fl >> 10) & 1), "id=" + ("ok" if rid == qid else "BAD"), *sorted(ips))
PY
}

boot() {
  PID=""
  env FLUXOR_STORE_DIR="$D" RUST_LOG=info "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  PID=$!
  for _ in $(seq 100); do
    grep -q "\[cluster_dns\] serving" "$D/run.log" && return 0
    kill -0 "$PID" 2>/dev/null || fail "the runtime exited"
    sleep 0.1
  done
  fail "cluster_dns never bound :$DNS_PORT"
}
halt() { kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true; PID=""; }

echo "== 1. the shipped graph, on a test port, forwarding to a stub upstream =="
python3 - "$UP_PORT" >"$D/up.log" 2>&1 <<'PY' &
import socket, struct, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", int(sys.argv[1])))
while True:
    q, a = s.recvfrom(1500)
    qid = q[:2]
    at = 12
    while q[at]: at += 1 + q[at]
    qend = at + 5
    ans = qid + struct.pack(">HHHHH", 0x8180, 1, 1, 0, 0) + q[12:qend] \
        + b"\xc0\x0c" + struct.pack(">HHIH", 1, 1, 60, 4) + bytes([192, 0, 2, 1])
    s.sendto(ans, a)
    print("asked", q[12:qend - 4], flush=True)
PY
UP_PID=$!
python3 - "$GRAPH" "$D/graph.yaml" "$DNS_PORT" "$UP_PORT" <<'PY'
import sys
g, out, port, up = sys.argv[1:]
g = open(g).read()
assert g.count("    port: 53\n") == 1
g = g.replace("    port: 53\n", f"    port: {port}\n    upstream: \"127.0.0.1\"\n    upstream_port: {up}\n    address: \"127.0.0.1\"\n")
open(out, "w").write(g)
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" || fail "graph did not build"

echo "== 2. a Service, a headless Service with two ready backends, one with none =="
np_put "/services/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"selector":{"app":"web"},"clusterIP":"10.96.0.10","ports":[{"name":"http","port":80,"targetPort":8080},{"name":"dns","port":53,"protocol":"UDP"}]}}'
np_put "/services/default/db" '{"metadata":{"name":"db","namespace":"default"},"spec":{"selector":{"app":"db"},"clusterIP":"None","ports":[{"name":"pg","port":5432,"targetPort":5433}]}}'
np_put "/services/default/ext" '{"metadata":{"name":"ext","namespace":"default"},"spec":{"type":"ExternalName","externalName":"db.example.com"}}'
np_put "/endpoints/default/db" "db-0=10.244.0.5,db-1=10.244.0.6"
np_put "/services/other/lonely" '{"metadata":{"name":"lonely","namespace":"other"},"spec":{"clusterIP":"None"}}'
boot

echo "== 3. phase A: the answers =="
R="$(q web.default.svc.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok 10.96.0.10" ] || fail "web: '$R' (want its ClusterIP, authoritative)"
echo "   web.default.svc.cluster.local      -> $R"
R="$(q WEB.Default.SVC.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok 10.96.0.10" ] || fail "names are case-insensitive: '$R'"
R="$(q db.default.svc.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok 10.244.0.5 10.244.0.6" ] || fail "headless db: '$R' (want both backends)"
echo "   db.default.svc.cluster.local       -> $R (headless: its backends)"
R="$(q lonely.other.svc.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok" ] || fail "headless with no backends: '$R' (the name exists, with no address)"
R="$(q web.default.svc.cluster.local AAAA)"
[ "$R" = "NOERROR aa=1 id=ok" ] || fail "AAAA of web: '$R'"
R="$(q nope.default.svc.cluster.local)"
[ "$R" = "NXDOMAIN aa=1 id=ok" ] || fail "no such Service: '$R'"
R="$(q web.default.pod.cluster.local)"
[ "$R" = "NXDOMAIN aa=1 id=ok" ] || fail "not a Service name: '$R'"
echo "   no addresses, AAAA, no such Service, not a Service name: NOERROR / NOERROR / NXDOMAIN / NXDOMAIN"
R="$(q ext.default.svc.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok CNAME:db.example.com" ] || fail "ExternalName: '$R'"
echo "   ext.default.svc.cluster.local      -> $R (ExternalName)"
R="$(q _http._tcp.web.default.svc.cluster.local SRV)"
[ "$R" = "NOERROR aa=1 id=ok SRV:80:web.default.svc.cluster.local" ] || fail "SRV http/tcp: '$R'"
R="$(q _dns._udp.web.default.svc.cluster.local SRV)"
[ "$R" = "NOERROR aa=1 id=ok SRV:53:web.default.svc.cluster.local" ] || fail "SRV dns/udp: '$R'"
R="$(q _http._udp.web.default.svc.cluster.local SRV)"
[ "$R" = "NXDOMAIN aa=1 id=ok" ] || fail "SRV of a port the Service has not on that protocol: '$R'"
echo "   _http._tcp.web… -> SRV 80 web.default.svc.cluster.local; _dns._udp -> 53; _http._udp -> NXDOMAIN"
R="$(q _pg._tcp.db.default.svc.cluster.local SRV)"
[ "$R" = "NOERROR aa=1 id=ok SRV:5433:10-244-0-5.db.default.svc.cluster.local SRV:5433:10-244-0-6.db.default.svc.cluster.local" ] \
  || fail "headless SRV: '$R'"
R="$(q 10-244-0-6.db.default.svc.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok 10.244.0.6" ] || fail "a headless backend's name: '$R'"
R="$(q 10-244-0-9.db.default.svc.cluster.local)"
[ "$R" = "NXDOMAIN aa=1 id=ok" ] || fail "an address that is no backend: '$R'"
echo "   _pg._tcp.db… -> each backend at 5433 (10-244-0-5.db…, 10-244-0-6.db…), which resolve; another address does not"
R="$(q example.org)"
[ "$R" = "NOERROR aa=0 id=ok 192.0.2.1" ] || fail "example.org forwarded: '$R'"
grep -q "example" "$D/up.log" || fail "the upstream was never asked"
grep -q "cluster" "$D/up.log" && fail "a cluster name was forwarded"
echo "   example.org                        -> $R (forwarded, the client's id restored)"
halt
[ "$(np_last /cluster-dns)" = 127.0.0.1 ] || fail "/cluster-dns = '$(np_last /cluster-dns)', want the resolver's address"
echo "   /cluster-dns = 127.0.0.1 (what a Pod's resolv.conf names)"

echo "== 4. phase B: web's ClusterIP changes, db is deleted — the answers follow the store =="
np_put "/services/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"selector":{"app":"web"},"clusterIP":"10.96.0.11","ports":[{"port":80}]}}'
np_put "/services/default/db" "-DELETE-"
boot
R="$(q web.default.svc.cluster.local)"
[ "$R" = "NOERROR aa=1 id=ok 10.96.0.11" ] || fail "web after: '$R'"
R="$(q db.default.svc.cluster.local)"
[ "$R" = "NXDOMAIN aa=1 id=ok" ] || fail "a deleted Service still resolves: '$R'"
echo "   web -> 10.96.0.11; db -> NXDOMAIN (its Endpoints document left behind changes nothing)"
halt

echo "== E2E green: the cluster's DNS — Service names answered from the store, the rest forwarded =="
