#!/usr/bin/env bash
# Live E2E: a Pod's phase, as its node reports it, reaches the Pod object.
#
# The node's lifecycle chain reports `phase=<phase>;pod=<ns>/<name>` under
# /podstatus/<uid>, a prefix the node does not scope, so the record lands in
# the plane's store as written; pod_status.uproc patches the Pod's
# status.phase from it, which is what `kubectl get pods` shows. Two runs on
# one store: the first creates the Pods through the API; between the runs the
# node's report is appended to the plane's durable log, as a worker's write
# would arrive; the second run must serve the Pod as Running.
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
put_text "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'

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

echo "== 6. the node reports web-0 Running; a restart replays it onto the Pod =="
kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
put_text "/podstatus/report-1" 'phase=Running;pod=default/web-0'
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run2.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered after the restart"
for _ in $(seq 60); do
  r="$(api GET /api/v1/namespaces/default/pods/web-0 2>/dev/null || true)"
  case "$r" in 200\|*'"phase":"Running"'*) break ;; esac
  sleep 1
done
case "$r" in 200\|*'"phase":"Running"'*) echo "   pod web-0: status.phase Running" ;;
  *) fail "web-0 did not reach Running through the API: ${r:0:200} store='$(nc_store_get "$STORE" /pods/default/web-0)'" ;; esac
r="$(api GET /api/v1/namespaces/default/pods/web-1)"
case "$r" in 200\|*'"phase":"Running"'*) fail "web-1 was never reported and must not be Running" ;; *) echo "   pod web-1: not Running (never reported)" ;; esac

echo "== E2E green: a node's phase report patches the Pod object; kubectl sees the phase the node reports =="
