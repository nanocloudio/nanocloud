#!/usr/bin/env bash
# Live E2E: a Pod's status, as its node reports it, becomes the Pod object's
# status as Kubernetes states it — and what every client reads from it.
#
# The node's status chain (pod_report.uproc) reports under /podstatus/<uid>,
# a prefix the node does not scope, so the record lands in the plane's store
# as written; pod_status.uproc writes it onto the Pod: phase, startTime,
# hostIP/podIP (the node's InternalIP — the Pod shares its network), the
# conditions PodScheduled, Initialized, ContainersReady and Ready with their
# lastTransitionTime, and containerStatuses[0] (name, image, imageID, ready,
# started, restartCount, state, lastState) — only when the Pod there is the
# record's, by uid: records an earlier incarnation of a name left behind
# patch nothing. The ReplicaSet and Deployment count Ready Pods
# (replicaset_status.uproc, deployment_status.uproc).
#
# Runs on one store: the first run creates the Pods through the API; between
# runs the node's reports are appended to the plane's durable log, as a
# worker's write would arrive. PASS = the Pod JSON field by field, `kubectl
# get pods` READY/STATUS/RESTARTS (1/1 Running, 0/1 CrashLoopBackOff), the
# ReplicaSet/Deployment counts, a settled status written no more, and
# `kubectl wait --for=condition=Ready` and `kubectl rollout status` once the
# second Pod is Ready.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-dep-api-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <hex-value>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
import struct, sys, os, binascii
path, key, val = sys.argv[1], sys.argv[2].encode(), binascii.unhexlify(sys.argv[3])
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
put_text() { store_put "$1" "$(printf '%s' "$2" | python3 -c 'import sys,binascii;print(binascii.hexlify(sys.stdin.buffer.read()).decode())')"; }

echo "== 1. seed the operator's identity: SA key, cluster-admin binding, a ready node =="
python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
ISSUER, KID = b"https://kubernetes.default.svc", b"sa-1"
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1)
        + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex","w").write(
    binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"
now = int(time.time())
claims = {"iss": ISSUER.decode(), "sub": "system:serviceaccount:default:admin",
          "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/token.admin","w").write((signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode())
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
put_text "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
# The node advertises its address, as its lease writes it (node_lease.uproc):
# the hostIP of the Pods it runs.
put_text "/nodes/node-1" '{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1"},"status":{"ready":true,"addresses":[{"type":"InternalIP","address":"10.0.0.9"},{"type":"Hostname","address":"node-1"}],"conditions":[{"type":"Ready","status":"True"}]}}'

echo "== 2. render the SHIPPED control plane for this node, build it =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
# The node's own mint announces apart from the seeded operator key, as the
# bare-metal render (rig-render-controlplane.sh) has it: both at
# /authn-keys/sa and the node's would replace the one this test signs with.
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 3. run it; wait for the API over TLS =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"

api() { # api <method> <path> [json-body] -> "<status>|<body>"
  python3 - "$PORT" "$D/token.admin" "$1" "$2" "${3:-}" <<'PY'
import socket, ssl, sys
port, tokf, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
tok = open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
raw = socket.create_connection(("127.0.0.1", port), timeout=10)
s = ctx.wrap_socket(raw, server_hostname="localhost")
b = body.encode()
req = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n"
       f"Content-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b
s.sendall(req)
buf = b""
s.settimeout(10)
try:
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
except (socket.timeout, ssl.SSLError):
    pass
s.close()
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac

echo "== 4. POST a Deployment through the API, as kubectl apply does =="
DEP='{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"default"},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"web"}},"template":{"metadata":{"labels":{"app":"web"}},"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
# The seeded key reaches the verifier through the replicated log: a 503
# ("no verification key") in the first seconds is that import, not a verdict.
for _ in $(seq 90); do
  r="$(api POST /apis/apps/v1/namespaces/default/deployments "$DEP")"
  case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
done
case "$r" in 20[01]\|*) echo "   POST -> ${r%%|*}" ;; *) fail "POST deployment: $r" ;; esac
# The key the controllers watch — not a bare `/deployments/`.
STORED="$(nc_store_get "$STORE" /deployments.apps/default/web)"
case "$STORED" in *'"name":"web"'*) echo "   stored at /deployments.apps/default/web" ;;
  *) echo "   /deployments.apps/default/web: '$STORED'"; echo "   /deployments/default/web: '$(nc_store_get "$STORE" /deployments/default/web)'"; fail "the API stored the Deployment under a key no controller watches" ;;
esac

# A DaemonSet and a StatefulSet beside it: their update strategy is not
# stated, which the API server defaults (RollingUpdate), as upstream's does.
DS='{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"agent","namespace":"default"},"spec":{"selector":{"matchLabels":{"app":"agent"}},"template":{"metadata":{"labels":{"app":"agent"}},"spec":{"containers":[{"name":"agent","image":"busybox"}]}}}}'
STS='{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"db","namespace":"default"},"spec":{"replicas":2,"serviceName":"db","selector":{"matchLabels":{"app":"db"}},"template":{"metadata":{"labels":{"app":"db"}},"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
r="$(api POST /apis/apps/v1/namespaces/default/daemonsets "$DS")"
case "$r" in 20[01]\|*) echo "   POST daemonset agent -> ${r%%|*}" ;; *) fail "POST daemonset: ${r:0:300}" ;; esac
r="$(api POST /apis/apps/v1/namespaces/default/statefulsets "$STS")"
case "$r" in 20[01]\|*) echo "   POST statefulset db -> ${r%%|*}" ;; *) fail "POST statefulset: ${r:0:300}" ;; esac
for k in daemonsets.apps/default/agent statefulsets.apps/default/db; do
  case "$(nc_store_get "$STORE" "/$k")" in *'"updateStrategy":{"type":"RollingUpdate"}'*) ;;
    *) fail "/$k: its update strategy was not defaulted: $(nc_store_get "$STORE" "/$k" | head -c 400)" ;; esac
done
echo "   both stored with spec.updateStrategy.type RollingUpdate (defaulted)"

echo "== 5. the controllers act: a ReplicaSet, then Pods =="
for _ in $(seq 60); do
  case "$(nc_store_get "$STORE" /replicasets.apps/default/web)" in *'"replicas":2'*) break ;; esac
  sleep 0.5
done
RS="$(nc_store_get "$STORE" /replicasets.apps/default/web)"
case "$RS" in *'"replicas":2'*'"image":"nginx"'*) echo "   replicaset: ${RS:0:110}..." ;; *) fail "no ReplicaSet for web: '$RS'" ;; esac
# The ReplicaSet is one a client can decode and select: kind, and the
# template's labels beside the hash (kubectl get rs -l app=web).
case "$RS" in *'"kind":"ReplicaSet"'*) ;; *) fail "ReplicaSet lacks its kind: ${RS:0:200}" ;; esac
case "$RS" in *'"app":"web"'*) echo "   kind ReplicaSet, label app=web" ;; *) fail "ReplicaSet lacks the template's labels: ${RS:0:200}" ;; esac
for _ in $(seq 60); do
  case "$(nc_store_get "$STORE" /pods/default/web-1)" in *'"nodeName"'*) break ;; esac
  sleep 0.5
done
for i in 0 1; do
  POD="$(nc_store_get "$STORE" /pods/default/web-$i)"
  case "$POD" in *'"nodeName":"node-1"'*) echo "   pod web-$i: scheduled" ;; *) fail "pod web-$i not scheduled" ;; esac
  case "$POD" in *'"kind":"Pod"'*) ;; *) fail "pod web-$i lacks its kind: ${POD:0:200}" ;; esac
  case "$POD" in *'"app":"web"'*) ;; *) fail "pod web-$i lacks the template's labels: ${POD:0:200}" ;; esac
  case "$POD" in *'"pod-template-hash"'*) ;; *) fail "pod web-$i lacks pod-template-hash: ${POD:0:200}" ;; esac
done

# The DaemonSet's Pods — one per ready Node, named <ds>-<node> — and the
# StatefulSet's first.
# The ready Nodes: the seeded node-1, and the plane's own (node-a, as the
# render names it), when it keeps a lease.
AGENTS=""; for _ in $(seq 60); do
  AGENTS=""; NREADY=0
  for n in node-1 node-a; do
    [ -n "$(nc_store_get "$STORE" "/ready-nodes/$n")" ] || continue
    NREADY=$((NREADY + 1))
    case "$(nc_store_get "$STORE" "/pods/default/agent-$n")" in *'"DaemonSet"'*) AGENTS="$AGENTS agent-$n" ;; esac
  done
  case "$(nc_store_get "$STORE" /pods/default/db-0)" in *nodeName*) DB0OK=1 ;; *) DB0OK=0 ;; esac
  [ "$NREADY" -ge 1 ] && [ "$(echo $AGENTS | wc -w)" = "$NREADY" ] && [ "$DB0OK" = 1 ] && break
  sleep 1
done
[ -n "$AGENTS" ] || fail "the DaemonSet made no Pod"
case "$(nc_store_get "$STORE" /pods/default/db-0)" in *nodeName*) ;; *) fail "the StatefulSet made no db-0: $(nc_store_get "$STORE" /pods/default/db-0 | head -c 600)" ;; esac
echo "   daemonset agent: $AGENTS; statefulset db: db-0 (db-1 waits for it to be Ready)"

echo "== 6. the node reports web-0 Running and Ready, web-1 in its restart backoff; a restart replays both onto the Pods =="
uid_of() { nc_store_get "$STORE" "/pods/default/$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])'; }
UID0="$(uid_of web-0)"; UID1="$(uid_of web-1)"
[ -n "$UID0" ] && [ -n "$UID1" ] || fail "the Pods carry no uid"
boot2() { # restart the runtime over the same store
  kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
  "$@"
  env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run2.log" 2>&1 &
  RUNTIME_PID=$!
  nc_store_ready "$STORE" 60 || fail "the store never answered after the restart"
  # The verifier's key arrives through the replicated log: a 503 ("no
  # verification key") in the first seconds is that import, not a verdict.
  local r=""
  for _ in $(seq 90); do
    r="$(api GET /api/v1/namespaces/default/pods/web-0 2>/dev/null || true)"
    case "$r" in 200\|*) return 0 ;; esac
    sleep 1
  done
  fail "no API after the restart: ${r:0:200}"
}
# The node's report (pod_report.uproc), keyed by the Pod's uid; its moments
# are the node's wall clock in Unix ms.
T0=1760000000000; TR=1760000005000; TRDY=1760000006000; LTR=1760000001000; LTF=1760000004000
DIG=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
ts() { python3 -c 'import sys,datetime; print(datetime.datetime.fromtimestamp(int(sys.argv[1])//1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))' "$1"; }
running() { # running <uid> <pod> <restarts> — Running, Ready, after one crash
  put_text "/podstatus/$1" "phase=Running;pod=default/$2;rst=$3;ready=true;started=true;cr=True;crr=;st=running;why=;msg=;code=;tr=$TR;tf=;lcode=3;lwhy=Error;ltr=$LTR;ltf=$LTF;t0=$T0;trdy=$TRDY;digest=$DIG"
}
DSU=""; for a in $AGENTS; do DSU="$DSU $a=$(uid_of "$a")"; done
DB0="$(uid_of db-0)"
boot2 eval '
  for au in $DSU; do running "${au#*=}" "${au%%=*}" 0; done
  running "$DB0" db-0 0
  running "$UID0" web-0 1
  put_text "/podstatus/$UID1" "phase=Running;pod=default/web-1;rst=4;ready=false;started=false;cr=False;crr=ContainersNotReady;st=waiting;why=CrashLoopBackOff;msg=back-off restarting failed container;code=;tr=;tf=;lcode=3;lwhy=Error;ltr=$LTR;ltf=$LTF;t0=$T0;trdy=$T0;digest=$DIG"
  # What an EARLIER incarnation of each name left behind: a Pod deleted and
  # made again keeps its name, not its uid. These name web-0 and web-1 but
  # are not theirs, and must patch neither — the source replays every record
  # on any change, so a stale one would win on its turn.
  put_text "/podstatus/00000000-0000-0000-0000-00000000dead" "phase=Failed;pod=default/web-0;rst=0;ready=false;started=false;cr=False;crr=PodCompleted;st=terminated;why=Error;msg=;code=1;tr=$T0;tf=$T0;lcode=;lwhy=;ltr=;ltf=;t0=$T0;trdy=$T0;digest=$DIG"
  put_text "/podstatus/00000000-0000-0000-0000-00000000beef" "phase=Failed;pod=default/web-1;rst=0;ready=false;started=false;cr=False;crr=PodCompleted;st=terminated;why=Error;msg=;code=1;tr=$T0;tf=$T0;lcode=;lwhy=;ltr=;ltf=;t0=$T0;trdy=$T0;digest=$DIG"
'
pod_json() { api GET "/api/v1/namespaces/default/pods/$1" 2>/dev/null | sed 's/^[^|]*|//'; }
for _ in $(seq 90); do
  case "$(pod_json web-0)" in *'"containerStatuses"'*'"restartCount":1'*) case "$(pod_json web-1)" in *CrashLoopBackOff*) break ;; esac ;; esac
  sleep 1
done
check() { # check <pod> <python assertions over p (the Pod)>
  pod_json "$1" | python3 -c '
import json, sys
p = json.load(sys.stdin); st = p.get("status", {})
cond = {c["type"]: c for c in st.get("conditions", [])}
cs = (st.get("containerStatuses") or [{}])[0]
exec(sys.argv[1])
' "$2" || fail "$1: $(pod_json "$1" | head -c 1500)"
}
check web-0 "
assert st['phase'] == 'Running', st
assert st['startTime'] == '$(ts $T0)', st.get('startTime')
assert st['hostIP'] == '10.0.0.9' and st['hostIPs'] == [{'ip': '10.0.0.9'}], st
assert st['podIP'] == '10.0.0.9' and st['podIPs'] == [{'ip': '10.0.0.9'}], st
for t in ('PodScheduled', 'Initialized'):
    assert cond[t]['status'] == 'True' and cond[t]['lastTransitionTime'] == '$(ts $T0)', cond[t]
for t in ('ContainersReady', 'Ready'):
    assert cond[t]['status'] == 'True' and cond[t]['lastTransitionTime'] == '$(ts $TRDY)', cond[t]
assert cs['name'] == 'web' and cs['image'] == 'nginx' and cs['imageID'] == 'sha256:$DIG', cs
assert cs['ready'] is True and cs['started'] is True and cs['restartCount'] == 1, cs
assert cs['state'] == {'running': {'startedAt': '$(ts $TR)'}}, cs['state']
assert cs['lastState'] == {'terminated': {'exitCode': 3, 'reason': 'Error', 'startedAt': '$(ts $LTR)', 'finishedAt': '$(ts $LTF)'}}, cs['lastState']
"
echo "   web-0: Running; Ready/ContainersReady True since $(ts $TRDY); hostIP=podIP 10.0.0.9 (the node's: it shares its network); startTime $(ts $T0)"
echo "   web-0: containerStatuses[0] ready, started, restartCount 1, running since $(ts $TR), lastState terminated exit 3 (Error), imageID sha256:${DIG:0:12}…"
check web-1 "
assert st['phase'] == 'Running', st
assert cond['Ready']['status'] == 'False' and cond['Ready']['reason'] == 'ContainersNotReady', cond['Ready']
assert cs['ready'] is False and cs['restartCount'] == 4, cs
assert cs['state'] == {'waiting': {'reason': 'CrashLoopBackOff', 'message': 'back-off restarting failed container'}}, cs['state']
assert cs['lastState']['terminated']['exitCode'] == 3, cs
"
echo "   web-1: waiting CrashLoopBackOff, not Ready (ContainersNotReady), restartCount 4"

echo "== 7. kubectl: READY, STATUS and RESTARTS; the ReplicaSet and the Deployment count Ready Pods =="
cat >"$D/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters:
- name: node
  cluster:
    server: https://127.0.0.1:$PORT
    insecure-skip-tls-verify: true
users:
- name: admin
  user:
    token: $(cat "$D/token.admin")
contexts:
- name: node
  context: {cluster: node, user: admin, namespace: default}
current-context: node
KC
k() { kubectl --kubeconfig "$D/kubeconfig" --request-timeout=20s "$@"; }
row() { k get pods --no-headers 2>&1 | awk -v n="$1" '$1 == n { print $2, $3, $4 }'; }
r0="$(row web-0)"; r1="$(row web-1)"
[ "$r0" = "1/1 Running 1" ] || { k get pods 2>&1 | sed 's/^/   /'; fail "kubectl get pods web-0: '$r0', want '1/1 Running 1'"; }
[ "$r1" = "0/1 CrashLoopBackOff 4" ] || { k get pods 2>&1 | sed 's/^/   /'; fail "kubectl get pods web-1: '$r1', want '0/1 CrashLoopBackOff 4'"; }
k get pods 2>&1 | sed 's/^/   /'
for _ in $(seq 60); do
  rs="$(k get rs web -o jsonpath='{.status.replicas}/{.status.readyReplicas}/{.status.availableReplicas}' 2>/dev/null || true)"
  dp="$(k get deploy web -o jsonpath='{.status.replicas}/{.status.updatedReplicas}/{.status.readyReplicas}/{.status.availableReplicas}' 2>/dev/null || true)"
  [ "$rs" = "2/1/1" ] && [ "$dp" = "2/2/1/1" ] && break
  sleep 1
done
[ "$rs" = "2/1/1" ] || fail "ReplicaSet status replicas/ready/available '$rs', want 2/1/1: $(nc_store_get "$STORE" /replicasets.apps/default/web | head -c 600)"
[ "$dp" = "2/2/1/1" ] || fail "Deployment status replicas/updated/ready/available '$dp', want 2/2/1/1: $(nc_store_get "$STORE" /deployments.apps/default/web | head -c 800)"
avail="$(k get deploy web -o jsonpath='{.status.conditions[?(@.type=="Available")].status}')"
[ "$avail" = "False" ] || fail "Deployment Available is '$avail' with 1 of 2 available (maxUnavailable 25% of 2 is 0)"
echo "   rs/web 2 replicas, 1 ready, 1 available; deploy/web 2 updated, 1 ready, Available False"
k get deploy web 2>&1 | sed 's/^/   /'
if timeout 8 kubectl --kubeconfig "$D/kubeconfig" rollout status deployment/web --timeout=5s >"$D/rollout.out" 2>&1; then
  fail "rollout status reports done with web-1 in CrashLoopBackOff: $(cat "$D/rollout.out")"
fi
echo "   kubectl rollout status deployment/web: still waiting ($(tail -1 "$D/rollout.out"))"

want="$(echo $AGENTS | wc -w)"
if ! k rollout status daemonset/agent --timeout=60s >"$D/ds.out" 2>&1; then
  fail "kubectl rollout status daemonset/agent: $(cat "$D/ds.out") — $(nc_store_get "$STORE" /daemonsets.apps/default/agent | head -c 800)"
fi
echo "   $(tail -1 "$D/ds.out")"
dss="$(k get ds agent -o jsonpath='{.status.desiredNumberScheduled}/{.status.currentNumberScheduled}/{.status.numberReady}/{.status.numberAvailable}/{.status.updatedNumberScheduled}')"
[ "$dss" = "$want/$want/$want/$want/$want" ] || fail "DaemonSet status desired/current/ready/available/updated '$dss', want $want each"
echo "   ds/agent: desired/current/ready/available/updated = $dss"
for _ in $(seq 60); do case "$(nc_store_get "$STORE" /pods/default/db-1)" in *nodeName*) break ;; esac; sleep 1; done
case "$(nc_store_get "$STORE" /pods/default/db-1)" in *nodeName*) ;; *) fail "db-1 was not made once db-0 was Ready" ;; esac
sts="$(k get sts db -o jsonpath='{.status.replicas}/{.status.readyReplicas}')"
for _ in $(seq 30); do [ "$sts" = "2/1" ] && break; sleep 1; sts="$(k get sts db -o jsonpath='{.status.replicas}/{.status.readyReplicas}')"; done
[ "$sts" = "2/1" ] || fail "StatefulSet status replicas/readyReplicas '$sts', want 2/1 (db-1 not reported yet)"
if timeout 8 kubectl --kubeconfig "$D/kubeconfig" rollout status statefulset/db --timeout=5s >"$D/sts.out" 2>&1; then
  fail "rollout status reports the StatefulSet done with db-1 not Ready: $(cat "$D/sts.out")"
fi
echo "   sts/db: db-0 Ready made db-1; replicas/ready $sts; kubectl rollout status statefulset/db still waiting"

echo "== 8. an earlier incarnation's record patches no Pod of its name; a settled status writes nothing =="
rv() { pod_json "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["resourceVersion"])'; }
RV0="$(rv web-0)"; RV1="$(rv web-1)"
# Give the stale records every chance: the source replays every record on
# any change under the prefix, so a settle covers many of their turns.
for _ in $(seq 8); do
  sleep 1
  r0="$(pod_json web-0)"; r1="$(pod_json web-1)"
  case "$r0" in *'"phase":"Failed"'*) fail "web-0 took an earlier incarnation's Failed: ${r0:0:300}" ;; esac
  case "$r1" in *'"phase":"Failed"'*) fail "web-1 took an earlier incarnation's Failed: ${r1:0:300}" ;; esac
done
[ "$(rv web-0)" = "$RV0" ] && [ "$(rv web-1)" = "$RV1" ] \
  || fail "a settled status was written again: web-0 rv $RV0 -> $(rv web-0), web-1 rv $RV1 -> $(rv web-1)"
echo "   neither Pod took a record not its own; their resourceVersions stayed $RV0/$RV1 across the replays"

echo "== 9. web-1 runs and is Ready: kubectl wait and rollout status complete =="
DB1="$(uid_of db-1)"
boot2 eval 'running "$UID1" web-1 4; running "$DB1" db-1 0'
if ! k wait --for=condition=Ready pod/web-1 --timeout=60s >"$D/wait.out" 2>&1; then
  fail "kubectl wait --for=condition=Ready pod/web-1: $(cat "$D/wait.out") — $(pod_json web-1 | head -c 600)"
fi
echo "   $(cat "$D/wait.out")"
if ! k rollout status deployment/web --timeout=60s >"$D/rollout.out" 2>&1; then
  fail "kubectl rollout status deployment/web: $(cat "$D/rollout.out") — $(nc_store_get "$STORE" /deployments.apps/default/web | head -c 800)"
fi
echo "   $(tail -1 "$D/rollout.out")"
k wait --for=condition=Available deployment/web --timeout=30s >/dev/null 2>&1 || fail "the Deployment never became Available"
[ "$(row web-1)" = "1/1 Running 4" ] || fail "kubectl get pods web-1: '$(row web-1)', want '1/1 Running 4'"
if ! k rollout status statefulset/db --timeout=60s >"$D/sts.out" 2>&1; then
  fail "kubectl rollout status statefulset/db: $(cat "$D/sts.out") — $(nc_store_get "$STORE" /statefulsets.apps/default/db | head -c 800)"
fi
echo "   $(tail -1 "$D/sts.out")"
sts="$(k get sts db -o jsonpath='{.status.replicas}/{.status.readyReplicas}/{.status.currentReplicas}/{.status.updatedReplicas}/{.status.availableReplicas}')"
[ "$sts" = "2/2/2/2/2" ] || fail "StatefulSet status '$sts', want 2/2/2/2/2"
k get pods 2>&1 | sed 's/^/   /'
k get deploy,rs,ds,sts 2>&1 | sed 's/^/   /'

echo "== E2E green: a node's report becomes the Pod's status (phase, conditions, containerStatuses, IPs); kubectl's columns, wait and rollout status read it =="
