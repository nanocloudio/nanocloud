#!/usr/bin/env bash
# Anonymous access is a DEPLOYMENT CHOICE, and this proves both halves of it.
#
# The shipped graph answers 401 to a request with no credential. Adding the
# `anon` node — one decision, from modules/app/_chronicle/apiplane_anon.uproc —
# maps that request to `system:anonymous` and lets RBAC decide. The node is
# absent from the shipped graph on purpose: flipping who can reach the API
# should be visible in the file, not inherited from a default.
#
# Both cases run against the SAME shipped graph, once unmodified and once with
# the node spliced in, so the difference under test is exactly the choice.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7446

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kube_decode kagi_verify rbac_gate api_admission store_effect decision pipeline http; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apianon-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

put_text() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
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

store_put() { # store_put <key> <hex>
  python3 - "$D/store.log" "$1" "$2" <<'PYS'
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
PYS
}

echo "== 0. a verification key, so a missing credential is 401 and not 503 =="
# Without one the chain answers 503 "no verification key" — the SERVER's fault,
# not the caller's. That is the right answer for an unconfigured server and the
# wrong thing to assert here: this test is about a caller with no credential.
python3 - "$D" <<'PYK'
import struct, sys, binascii
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
D = sys.argv[1]
pub = ec.generate_private_key(ec.SECP256R1()).public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(b"https://kubernetes.default.svc") + struct.pack("<H", 1) + f8(b"sa-1")
        + struct.pack("<H", 1) + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex","w").write(
    binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
PYK
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"

echo "== 1. seed a pod, and bind system:anonymous to a READ-ONLY role =="
# Read-only on purpose: "anonymous is allowed in" and "anonymous may do
# anything" are different decisions, and conflating them is how an opt-in
# becomes an open door.
put_text "/pods/default/web-0" '{"metadata":{"name":"web-0","namespace":"default"}}'
put_text "/roles/reader" "rules=get:pods,list:pods"
put_text "/rolebindings/anon" "subjects=system:anonymous;role=reader"

run_case() { # run_case <graph> -> "<code>|<body>"
  nc_build_workload "$ROOT" "$1" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  for _ in $(seq 80); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
    sleep 0.1
  done
  local out
  out="$(curl -s -i --max-time 6 "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/web-0" || true)"
  kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
  printf '%s|%s' \
    "$(printf '%s' "$out" | head -1 | tr -d '\r' | awk '{print $2}')" \
    "$(printf '%s' "$out" | tr -d '\r' | awk 'BEGIN{b=0} /^$/{b=1;next} b{print}')"
}

echo "== 2. the SHIPPED graph refuses an anonymous request =="
sed 's/port: 7444/port: '"$PORT"'/' "$GRAPH" >"$D/shipped.yaml"
R="$(run_case "$D/shipped.yaml")"
[ "${R%%|*}" = "401" ] || fail "shipped graph: got '${R%%|*}' want 401"
case "${R#*|}" in *'"reason":"Unauthorized"'*) ;; *) fail "shipped body: ${R#*|}";; esac
echo "   GET /pods/default/web-0 (no credential) -> 401 'no usable credential'"

echo "== 3. the SAME graph plus the `anon` node admits it, and RBAC decides =="
ANON="$(nc_decision "$ROOT/modules/app/_chronicle/apiplane_anon.uproc" anon)"
[ -n "$ANON" ] || { echo "FAIL: apiplane_anon.uproc did not compile"; exit 1; }
python3 - "$D/shipped.yaml" "$D/anon.yaml" "$ANON" <<'PY'
import sys
src, dst, param = sys.argv[1], sys.argv[2], sys.argv[3]
g = open(src).read()
g = g.replace("""  - name: admit
    type: decision""", """  - name: anon
    type: decision
    params:
      decision: "%s"

  - name: admit
    type: decision""" % param)
g = g.replace("""  - from: kube_decode.record_out
    to: admit.record_in""", """  - from: kube_decode.record_out
    to: anon.record_in
  - from: anon.result_out
    to: admit.record_in""")
open(dst, "w").write(g)
PY
R="$(run_case "$D/anon.yaml")"
[ "${R%%|*}" = "200" ] || fail "anon graph: got '${R%%|*}' want 200 (${R#*|})"
echo "   GET /pods/default/web-0 (no credential) -> 200 (as system:anonymous, get:pods allowed)"

echo "== 4. admitted is not authorized: anonymous cannot do what its role forbids =="
python3 - "$D/anon.yaml" <<'PY'
import sys
p = sys.argv[1]
g = open(p).read()
# Same graph, no edit — the refusal must come from RBAC, not from the graph.
open(p, "w").write(g)
PY
nc_build_workload "$ROOT" "$D/anon.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  sleep 0.1
done
OUT="$(curl -s -i --max-time 6 -X DELETE "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/web-0" || true)"
kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
CODE="$(printf '%s' "$OUT" | head -1 | tr -d '\r' | awk '{print $2}')"
[ "$CODE" = "403" ] || fail "anonymous DELETE: got '$CODE' want 403"
echo "   DELETE /pods/default/web-0 (anonymous) -> 403 (the role grants get+list only)"

echo "== E2E green: anonymous access is opt-in by a node, and still governed by RBAC =="
