#!/usr/bin/env bash
# Live E2E for api_ingress as an HTTP terminator — it proves the module parses real HTTP/1.1 off the net stream, routes
# it, runs a store round-trip, and writes an HTTP response. No host code in the
# path: the graph is linux_net → api_ingress → api_responder.
#
# This is the transport proof; the authenticated CRUD pipeline + JSON
# marshalling are covered by apiserver-crud-e2e.sh (a real curl to
# /api/v1/... returns 501 in this graph).
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
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/api_ingress.fmod" "$MODULES_DIR/api_responder.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apisrv-http-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

# http_get <path> — one HTTP/1.1 GET; prints "<status>|<body>".
http_get() {
  python3 - "$PORT" "$1" <<'PY'
import socket, sys
port, path = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port), timeout=5)
s.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
buf = b""
s.settimeout(5)
try:
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
except socket.timeout:
    pass
s.close()
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}

echo "== 1. build the config + module table =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. start the apiserver graph (cleartext HTTP) =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 60); do
  python3 -c "import socket,sys;\
socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

echo "== 3. real HTTP/1.1 requests, terminated by api_ingress in PIC =="
expect() { # expect <path> <want-status> <want-body-substr>
  local out st body; out="$(http_get "$1")"; st="${out%%|*}"; body="${out#*|}"
  [ "$st" = "$2" ] || fail "$1 status: got '$st' want '$2' (body: $body)"
  case "$body" in *"$3"*) ;; *) fail "$1 body: got '$body' want substr '$3'";; esac
  echo "   GET $1 -> $st $body"
}
expect /healthz 200 ok
expect /livez 200 ok
expect "/apis/nanocloud.io/v1/counts/pods" 200 0
expect /nonsense 404 "not found"
# The CRUD pipeline runs (authn→authz→…); with no RBAC seeded, an anonymous
# request is authorized-denied — a 403 k8s Status. Full CRUD is proven with
# RBAC seeded in scripts/apiserver-crud-e2e.sh.
expect "/api/v1/namespaces/default/configmaps/cm1" 403 '"status":"Failure"'

echo "== E2E green: api_ingress terminates HTTP/1.1 + the pipeline as PIC — no host code =="
