#!/usr/bin/env bash
# The join exchange on the SHIPPED API graph: `POST
# /apis/nanocloud.io/v1/join` with a node's
# single-use join secret as the bearer credential answers what the node stored
# under /join/<secret> (node_pki: the admin credential and the CA) and spends
# the secret in the same store step (store_doc TAKE). A second presentation, a
# wrong secret, or none, is 401; the secret is not a token kagi verifies and
# grants nothing else.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7447

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kagi_verify store_effect store_source decision pipeline http http_chain; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apijoin-XXXXXX)"
# The store is lattice's (the graph's `kv_state_worker_store`): the runtime
# runs in $D, where the store lives (lattice/) and the seeded host log
# (store.log) is imported from.
nc_lattice_prepare "$D"
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

SECRET="0123456789abcdef0123456789abcdef"
ANSWER='{"token":"eyJhbGciOiJFUzI1NiJ9.e30.c2ln","ca":"MIIBkjCCATg="}'
echo "== 1. a join secret, as the node stores it =="
put_text "/join/$SECRET" "$ANSWER"

sed 's/port: 7444/port: '"$PORT"'/' "$GRAPH" >"$D/shipped.yaml"
nc_build_workload "$ROOT" "$D/shipped.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"
env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done
req() { # req <method> <auth-or-empty> -> "<code>|<body>"
  local out args=(-s -i --max-time 6 -X "$1")
  [ "$1" = POST ] && args+=(--data "")
  [ -n "$2" ] && args+=(-H "Authorization: Bearer $2")
  out="$(curl "${args[@]}" "http://127.0.0.1:$PORT/apis/nanocloud.io/v1/join" || true)"
  printf '%s|%s' \
    "$(printf '%s' "$out" | head -1 | tr -d '\r' | awk '{print $2}')" \
    "$(printf '%s' "$out" | tr -d '\r' | awk 'BEGIN{b=0} /^$/{b=1;next} b{print}')"
}

echo "== 2. a wrong secret, and none, are 401 — and spend nothing =="
R="$(req POST "fedcba9876543210fedcba9876543210")"
[ "${R%%|*}" = "401" ] || fail "wrong secret: got '${R%%|*}' want 401 (${R#*|})"
case "${R#*|}" in *'"reason":"Unauthorized"'*) ;; *) fail "wrong secret body: ${R#*|}";; esac
R="$(req POST "")"
[ "${R%%|*}" = "401" ] || fail "no secret: got '${R%%|*}' want 401"
echo "   wrong -> 401, none -> 401"

echo "== 3. only a POST joins: a GET presenting the secret is not one, and spends nothing =="
R="$(req GET "$SECRET")"
[ "${R%%|*}" != "200" ] || fail "GET joined: ${R#*|}"
echo "   GET -> ${R%%|*}"

echo "== 4. the secret answers what was stored under it =="
R="$(req POST "$SECRET")"
[ "${R%%|*}" = "200" ] || fail "join: got '${R%%|*}' want 200 (${R#*|})"
[ "${R#*|}" = "$ANSWER" ] || fail "join answered '${R#*|}'"
echo "   POST …/join -> 200, the stored credential"

echo "== 5. ... once: a second presentation is 401 =="
R="$(req POST "$SECRET")"
[ "${R%%|*}" = "401" ] || fail "replayed join: got '${R%%|*}' want 401"
echo "   replay -> 401"

echo "== E2E green: a join secret answers once, and only with what the node stored =="
