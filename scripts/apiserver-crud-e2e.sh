#!/usr/bin/env bash
# Live E2E for the full apiserver graph — api_ingress terminates HTTP, converts JSON⇄compact, and drives the
# authn→rbac→admission→core pipeline over the store, all as PIC. No host code in
# the path: linux_net → api_ingress → {authn,rbac_gate,api_admission,core_api}.
#
# The only seed is provisioning that any apiserver needs — an RBAC role+binding
# so requests are authorized. It is written BEFORE the graph starts (single
# writer, runtime down); the HTTP client itself writes nothing.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiserver.yaml"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in api_ingress authn rbac_gate api_admission core_api api_responder; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done
[ -e "$FLUXOR_RUNTIME" ] || { echo "FAIL: missing $FLUXOR_RUNTIME"; exit 1; }

D="$(mktemp -d /tmp/nc-apisrv-crud-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value> — seed the durable store log (runtime down).
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

# http <method> <path> [body] -> "<status>|<body>"
http() {
  python3 - "$PORT" "$1" "$2" "${3:-}" <<'PY'
import socket, sys
port, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
req = f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
if body:
    req += f"Content-Length: {len(body)}\r\n"
req += "\r\n" + body
s = socket.create_connection(("127.0.0.1", port), timeout=5)
s.sendall(req.encode())
buf = b""; s.settimeout(5)
try:
    while True:
        c = s.recv(4096)
        if not c: break
        buf += c
except socket.timeout:
    pass
s.close()
head, _, rbody = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n",1)[0].split(b" ",2)[1].decode() if head else "?"
print(f"{status}|{rbody.decode(errors='replace')}")
PY
}

echo "== 1. build config + module table =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. seed RBAC provisioning (role + binding for anonymous) =="
wal_put "/roles/admin" "rules=*:*"
wal_put "/rolebindings/anon" "subjects=anonymous;role=admin"

echo "== 3. start the apiserver graph =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 60); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

echo "== 4. full CRUD lifecycle, JSON in and out, pipeline in PIC =="
check() { # check <method> <path> <body> <want-status> <want-substr>
  local out st body; out="$(http "$1" "$2" "$3")"; st="${out%%|*}"; body="${out#*|}"
  [ "$st" = "$4" ] || fail "$1 $2 status: got '$st' want '$4' (body: $body)"
  case "$body" in *"$5"*) ;; *) fail "$1 $2 body: got '$body' want substr '$5'";; esac
  echo "   $1 $2 -> $st ${body:0:72}"
}
echo "== 3b. discovery documents (static, unauthenticated) =="
check GET /version  '' 200 '"gitVersion"'
check GET /api      '' 200 '"kind":"APIVersions"'
check GET /apis     '' 200 '"kind":"APIGroupList"'
check GET /api/v1   '' 200 '"kind":"APIResourceList"'
check GET /apis/apps/v1 '' 200 '"statefulsets"'
check GET /openapi/v2 "" 200 '"swagger"'
check GET /metrics "" 200 nanocloud_apiserver_up

CM=/api/v1/namespaces/default/configmaps
check POST   "$CM"      '{"metadata":{"name":"cm1"},"key":"value1"}' 201 '"name":"cm1"'
check GET    "$CM/cm1"  ''                                          200 '"value1"'
check POST   "$CM"      '{"metadata":{"name":"cm2"},"key":"v2"}'    201 '"name":"cm2"'
check GET    "$CM"      ''                                          200 '"kind":"List"'
# Watch (long-poll) from resourceVersion=0 → the current entries as ADDED events.
check GET    "$CM?watch=true&resourceVersion=0" ''                  200 '"type":"ADDED"'
check PUT    "$CM/cm1"  '{"metadata":{"name":"cm1"},"key":"value2"}' 200 '"value2"'
check DELETE "$CM/cm1"  ''                                          200 '"status":"Success"'
check GET    "$CM/cm1"  ''                                          404 '"status":"Failure"'

echo "== 5. a REAL nested k8s object round-trips, verbatim =="
DEP=/apis/apps/v1/namespaces/default/deployments
NESTED='{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":3,"selector":{"matchLabels":{"app":"web"}},"template":{"metadata":{"labels":{"app":"web"}},"spec":{"containers":[{"name":"web","image":"nginx:1.25","ports":[{"containerPort":80}]}]}}}}'
out="$(http POST "$DEP" "$NESTED")"; st="${out%%|*}"; body="${out#*|}"
[ "$st" = "201" ] || fail "nested POST: got '$st' ($body)"
echo "   POST deployment (nested spec) -> $st"
out="$(http GET "$DEP/web" "")"; st="${out%%|*}"; body="${out#*|}"
[ "$st" = "200" ] || fail "nested GET: got '$st'"
# The full nested structure must survive verbatim.
for want in '"replicas":3' '"image":"nginx:1.25"' '"containerPort":80' '"matchLabels"'; do
  case "$body" in *"$want"*) ;; *) fail "nested object lost '$want': $body";; esac
done
echo "   GET deployment/web -> $st (nested spec intact: replicas, containers, ports, selector)"

echo "== E2E green: full k8s CRUD served by the fmod graph — HTTP+JSON+pipeline, no host code =="
