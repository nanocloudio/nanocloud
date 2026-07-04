#!/usr/bin/env bash
# Live E2E for the control-plane graph at boot — the node path that runs the
# full k8s API plane as a fmod graph, single-writer over the control-plane
# store.
#
# It exercises the spawn contract the node boot uses — `fluxor run
# <control-plane-graph>` with `FLUXOR_STORE_DIR` pointed at a dedicated store —
# against the real fluxor-controlplane.yaml (all 23 fmods,
# api_ingress fronting the store on :7443). It proves the graph the bootstrap
# spawns actually comes up and serves the API over the socket, with the store
# written by exactly one process.
#
# As with the ingress E2E, this script is a pure TCP client: it never writes
# the store. Single-writer is the invariant under test.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-controlplane.yaml"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/api_ingress.fmod" "$MODULES_DIR/core_api.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-cp-boot-XXXXXX)"
STORE="$D/store"          # the control-plane store (FLUXOR_STORE_DIR)
STATE="$D/state"          # the runtime pidfile + log (state_root)
mkdir -p "$STORE" "$STATE"
RUNTIME_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true
  rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$STATE/control-plane.log" 2>/dev/null || true; exit 1; }

http_get() { # http_get <path> -> "<status>|<body>" — over TLS (the graph is HTTPS)
  python3 - "$PORT" "$1" <<'PY'
import socket, ssl, sys
port, path = int(sys.argv[1]), sys.argv[2]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
raw = socket.create_connection(("127.0.0.1", port), timeout=8)
s = ctx.wrap_socket(raw, server_hostname="localhost")
s.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
buf = b""
s.settimeout(8)
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

echo "== 1. generate a node cert + template the graph's cert paths to it =="
# The packaged control-plane graph references /etc/nanocloud.io/certs/... (which
# postinst provisions). For the test, generate a P-256 cert and point a copy of
# the graph at it — so this exercises the REAL production graph structure (TLS).
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$D/k.pem" -out "$D/c.pem" -days 1 -subj "/CN=nanocloud" 2>/dev/null
openssl x509 -in "$D/c.pem" -outform DER -out "$D/server.der" 2>/dev/null
openssl ec   -in "$D/k.pem" -outform DER -out "$D/server.key.der" 2>/dev/null
sed -e "s#/etc/nanocloud.io/certs/server.der#$D/server.der#" \
    -e "s#/etc/nanocloud.io/certs/server.key.der#$D/server.key.der#" \
    "$GRAPH" > "$D/graph.yaml"

echo "== 1b. build the config + module table from the control-plane graph =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 2. spawn the graph exactly as ControlPlaneRuntime::ensure_running does =="
# FLUXOR_STORE_DIR -> the sole-writer control-plane store; pidfile in state_root.
FLUXOR_STORE_DIR="$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$STATE/control-plane.log" 2>&1 &
RUNTIME_PID=$!
echo "$RUNTIME_PID" >"$STATE/control-plane.pid"

for _ in $(seq 60); do
  if python3 -c "
import socket,sys
try:
    socket.create_connection(('127.0.0.1', $PORT), timeout=0.2).close()
except OSError:
    sys.exit(1)
" 2>/dev/null; then
    break
  fi
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "control-plane graph exited before binding :$PORT"
  sleep 0.1
done

echo "== 3. the API plane answers real HTTP, terminated by api_ingress in PIC =="
expect() { # expect <path> <want-status> <want-body-substr>
  local out st body; out="$(http_get "$1")"; st="${out%%|*}"; body="${out#*|}"
  [ "$st" = "$2" ] || fail "$1 status: got '$st' want '$2' (body: $body)"
  case "$body" in *"$3"*) ;; *) fail "$1 body: got '$body' want substr '$3'";; esac
  echo "   GET $1 -> $st $body"
}
expect /healthz 200 ok
expect "/apis/nanocloud.io/v1/counts/pods" 200 0
# CRUD runs the full pipeline; with no RBAC seeded here it is authorized-denied
# (403). Full CRUD is proven in scripts/apiserver-crud-e2e.sh.
expect "/api/v1/namespaces/default/configmaps/cm1" 403 '"status":"Failure"'

echo "== 4. the pidfile names the live runtime (adopt-on-restart contract) =="
PIDFILE_PID="$(cat "$STATE/control-plane.pid")"
[ "$PIDFILE_PID" = "$RUNTIME_PID" ] || fail "pidfile $PIDFILE_PID != runtime $RUNTIME_PID"
kill -0 "$PIDFILE_PID" 2>/dev/null || fail "pidfile pid not alive"
echo "   pidfile -> $PIDFILE_PID (alive)"

echo "== 5. the control-plane store was written only by the graph =="
[ -f "$STORE/store.log" ] || fail "no $STORE/store.log — the graph never wrote the store"
python3 - "$STORE/store.log" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
p, seam = 0, 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    key = data[p+15:p+15+kl].decode(errors="replace")
    if key.startswith("/api-req/") or key.startswith("/api-resp/"):
        seam += 1
    p += 15 + kl + vl
if seam == 0:
    print("FAIL: no API seam records in the control-plane store")
    sys.exit(1)
print(f"   {seam} seam record(s), all written in-process by the graph")
PY

echo "== E2E green: the bootstrap's control-plane graph serves the API, single-writer =="
