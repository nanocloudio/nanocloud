#!/usr/bin/env bash
# Live E2E for ?watch=true served by the Chronicle graph.
#
# A watch is the one API surface that is not one-request-one-response, and the
# claim under test is that it needs no new construct: wave already streams
# (FLAG_MORE_BODY, close-delimited with no Content-Length), so a watch is a
# SECOND chain — store-change driven — merging into the same reply stage.
#
#   request chain: … -> act  (registers the watcher, answers the listing with
#                             MORE_BODY so wave holds the connection)
#   watch chain:   store_source(/pods/, watch) -> event -> store_effect
#                  -> reg -> store_effect -> emit -> reply -> http
#
# Unlike the other E2Es this one holds ONE runtime for the whole test and drives
# two connections at once, because that is the only way to observe a stream.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7444

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kube_decode kagi_verify rbac_gate api_admission store_effect store_source token_verify decision pipeline http; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apiwatch-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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

echo "== 1. seed identity, an RBAC role that grants watch, and one pod =="
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
def mint(sa):
    now = int(time.time())
    claims = {"iss": ISSUER.decode(), "sub": f"system:serviceaccount:default:{sa}",
              "aud": "nanocloud", "iat": now, "exp": now + 3600}
    signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    return (signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode()
open(f"{D}/token.admin","w").write(mint("admin"))
open(f"{D}/token.lister","w").write(mint("lister"))
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
put_text "/rolebindings/admins"  "subjects=system:serviceaccount:default:admin;role=admin"
put_text "/rolebindings/listers" "subjects=system:serviceaccount:default:lister;role=listonly"
put_text "/roles/admin"    "rules=*:*"
# `list` but NOT `watch` — the two are distinct Kubernetes verbs.
put_text "/roles/listonly" "rules=list:pods,get:pods"
put_text "/pods/default/web-0" '{"metadata":{"name":"web-0","namespace":"default"}}'
echo "   seeded"

echo "== 2. build the SHIPPED graph =="
[ -f "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }
cp "$GRAPH" "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 3. start ONE runtime and hold it for the whole test =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

echo "== 4. RBAC: 'watch' is its own verb — a list-only role cannot watch =="
CODE="$(python3 - "$PORT" "$(cat "$D/token.lister")" <<'PY'
import socket, sys
port, tok = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port), timeout=6)
s.sendall(("GET /api/v1/namespaces/default/pods?watch=true HTTP/1.1\r\n"
           "Host: x\r\nAuthorization: Bearer %s\r\n\r\n" % tok).encode())
data = s.recv(4096); s.close()
print(data.split(b"\r\n")[0].split(b" ")[1].decode())
PY
)"
[ "$CODE" = "403" ] || fail "list-only role watching: got '$CODE' want 403"
echo "   403 — a role granting list does NOT grant watch"

echo "== 5. WATCH: the stream opens, then a create arrives as an event =="
python3 - "$PORT" "$(cat "$D/token.admin")" "$D" <<'PY' || exit 1
import socket, sys, time, json
port, tok, D = int(sys.argv[1]), sys.argv[2], sys.argv[3]

w = socket.create_connection(("127.0.0.1", port), timeout=20)
w.sendall(("GET /api/v1/namespaces/default/pods?watch=true HTTP/1.1\r\n"
           "Host: x\r\nAuthorization: Bearer %s\r\n\r\n" % tok).encode())
w.settimeout(10)
head = b""
while b"\r\n\r\n" not in head:
    b = w.recv(4096)
    if not b:
        print("FAIL: watch connection closed before headers"); sys.exit(1)
    head += b
status = head.split(b"\r\n")[0].split(b" ")[1].decode()
if status != "200":
    print("FAIL: watch status %s want 200" % status); sys.exit(1)
if b"content-length" in head.lower():
    print("FAIL: a stream must not declare Content-Length"); sys.exit(1)
first = head.split(b"\r\n\r\n", 1)[1]
print("   watch opened: 200, no Content-Length (close-delimited stream)")
print("   first slice   : %s" % first[:90].decode(errors="replace"))

# A second connection creates a pod. The watch chain should turn the store
# change into an event on the FIRST connection.
c = socket.create_connection(("127.0.0.1", port), timeout=10)
body = json.dumps({"metadata": {"name": "web-9", "namespace": "default"}}).encode()
c.sendall(b"POST /api/v1/namespaces/default/pods HTTP/1.1\r\nHost: x\r\n"
          + ("Authorization: Bearer %s\r\n" % tok).encode()
          + b"Content-Type: application/json\r\n"
          + ("Content-Length: %d\r\n\r\n" % len(body)).encode() + body)
resp = c.recv(4096); c.close()
code = resp.split(b"\r\n")[0].split(b" ")[1].decode()
if code != "201":
    print("FAIL: create during watch: %s want 201" % code); sys.exit(1)
print("   created web-9 on a second connection -> 201")

deadline = time.time() + 12
seen = b""
while time.time() < deadline:
    try:
        chunk = w.recv(4096)
    except socket.timeout:
        break
    if not chunk:
        break
    seen += chunk
    if b"web-9" in seen:
        break
w.close()
if b"web-9" not in seen:
    print("FAIL: no event for web-9 arrived on the watch stream")
    print("      got: %r" % seen[:400]); sys.exit(1)
line = [l for l in seen.split(b"\n") if b"web-9" in l][0]
print("   event received: %s" % line[:110].decode(errors="replace"))
if b'"type":' not in line or b'"object":' not in line:
    print("FAIL: the event is not a watch envelope"); sys.exit(1)
print("   the store change reached the watching connection as an event")
PY

echo "== E2E green: ?watch=true is a Chronicle chain — a store change streams to a registered connection =="
