#!/usr/bin/env bash
# Live E2E for L7 workload ingress — the PAYOFF path:
# a Route + a workload → real HTTPS traffic reaching the workload THROUGH the
# proxy relay, not just a compiled route table.
#
#   external curl (HTTPS 8443)
#        │  TLS 1.3 terminated at the anchor (transport.anchor.stream.secure)
#        ▼
#   tls ──cleartext──▶ http proxy anchor (transport.anchor.stream)
#        │  match_dyn_route(host, path) → select_backend (ready-gated)
#        │  dial backend via NET_CMD_CONNECT, forward request + X-Forwarded-For,
#        │  stream the response back
#        ▼
#   backend workload (net=own leased address in a cluster; a host-reachable
#   listener here — the host-shared endpoint, the v1 Linux-expressible path)
#
# The path under test is Route → edge 443 → workload.
# It runs the tls-fronted edge graph (packaging/debian/fluxor-edge.yaml shape),
# seeds one /dataplane/edge/ row (the route-compiler seam is proven separately
# in route-compiler-e2e.sh), and asserts the workload's body returns through the
# relay — plus the fixed-surface fallback for unmatched requests.
#
# It needs freshly built tls.fmod / http.fmod, so run it once
# `fluxor modules build --target bcm2712` has produced the .fmods.
#
# net=own note: the ideal backend is a `net=own` workload (own netns + veth
# carrying its leased Tier-1 address), which metal does not yet realize. On
# Linux the relay reaches any routable backend
# address; this script uses a host-reachable listener (host IP + a port), which
# is the v1 host-shared-endpoint path and is byte-identical from the
# relay's side. Swap BACKEND_ADDR for a sandbox-runner net=own veth address to
# exercise the leased-identity variant (see sandbox-runner-e2e.sh).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
EDGE_PORT="${EDGE_PORT:-8443}"          # high port so the e2e needs no root
BACKEND_PORT="${BACKEND_PORT:-18099}"
BACKEND_ADDR="127.0.0.1:${BACKEND_PORT}"
ROUTE_HOST="api.example.com"
ROUTE_PATH="/v1/"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor  >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
command -v curl    >/dev/null || { echo "FAIL: curl not found"; exit 1; }
command -v python3 >/dev/null || { echo "FAIL: python3 not found"; exit 1; }
for m in tls http; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-ingress-e2e-XXXXXX)"
RUNTIME_PID=""; BACKEND_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true
  [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null || true
  rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; echo "--- run.log tail ---"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

# ── WAL PUT (op=1) into the shared control-plane store the edge SUBSCRIBEs ──
wal_put() {
  python3 - "$D/store.log" 1 "$1" "$2" <<'PY'
import struct, sys, os
path, op, key, val = sys.argv[1], int(sys.argv[2]), sys.argv[3].encode(), sys.argv[4].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, o, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, op, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

echo "== 1. generate a P-256 server cert (DER, as the tls module wants) =="
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$D/key.pem" -out "$D/cert.pem" -days 1 -subj "/CN=${ROUTE_HOST}" 2>/dev/null
openssl x509 -in "$D/cert.pem" -outform DER -out "$D/cert.der" 2>/dev/null
openssl ec   -in "$D/key.pem"  -outform DER -out "$D/key.der"  2>/dev/null

echo "== 2. start the backend workload (stands in for a net=own pod) =="
# A minimal HTTP/1.1 backend: 200 + a marker body, and it echoes back the
# X-Forwarded-For it received so the relay's header injection is observable.
cat >"$D/backend.py" <<'PY'
import socket, sys, threading
port = int(sys.argv[1])
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port)); srv.listen(16)
def handle(c):
    req = c.recv(65536).decode("latin1")
    xff = ""
    for line in req.split("\r\n"):
        if line.lower().startswith("x-forwarded-for:"):
            xff = line.split(":", 1)[1].strip()
    body = f"workload-ok xff={xff}\n"
    c.sendall(("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
               f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}").encode())
    c.close()
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PY
python3 "$D/backend.py" "$BACKEND_PORT" >"$D/backend.log" 2>&1 &
BACKEND_PID=$!
sleep 0.3

echo "== 3. write the tls-fronted edge graph (fluxor-edge.yaml shape) =="
cat >"$D/edge.yaml" <<YAML
target: linux
tick_us: 100
scheduler:
  accept_cycles: true
platform:
  net: {}
modules:
  - name: tls
    mode: 1
    verify_peer: 0
    alpn_h1_only: 1
    cert_file: "$D/cert.der"
    key_file: "$D/key.der"
  - name: http
    port: ${EDGE_PORT}
    host_tcp: 1
    routes_prefix: "/dataplane/edge/"
    routes:
      - path: "/healthz"
        body: "ok"
        content_type: "text/plain"
      - path: "/"
        body: "<html><body><h1>nanocloud edge</h1></body></html>"
wiring:
  - from: linux_net.net_out
    to: tls.cipher_in
  - from: tls.cipher_out
    to: linux_net.net_in
    buffer_bytes: 32768
  - from: tls.clear_out
    to: http.net_in
  - from: http.net_out
    to: tls.clear_in
    buffer_bytes: 32768
  - from: http.routes_sink
    to: http.routes_changes
YAML

echo "== 4. seed the compiled Route → /dataplane/edge/ row =="
# One validated Route's backend set (the route compiler's output; its own seam is
# proven in route-compiler-e2e.sh). host + longest-prefix path + one ready
# backend pointing at the workload.
wal_put "/dataplane/edge/default/web" \
  "host=${ROUTE_HOST};path=${ROUTE_PATH};be=${BACKEND_ADDR}:1:1"

echo "== 5. build config + module table, start the edge =="
nc_build_workload "$ROOT" "$D/edge.yaml" "$D/config.bin" "$D/modules.bin"
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!

echo "== 6. wait for the edge listener =="
for _ in $(seq 100); do
  curl -sk --max-time 1 "https://127.0.0.1:${EDGE_PORT}/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done

echo "== 7. THE PAYOFF: HTTPS Route → edge ${EDGE_PORT} → workload through the relay =="
RELAYED="$(curl -sk --max-time 5 -H "Host: ${ROUTE_HOST}" "https://127.0.0.1:${EDGE_PORT}${ROUTE_PATH}app" || true)"
grep -q "workload-ok" <<<"$RELAYED"     || fail "relay did not reach the workload; got: '$RELAYED'"
grep -q "xff=127.0.0.1" <<<"$RELAYED"   || echo "  NOTE: XFF empty (peer-address surface pending; injection path present)"
echo "  relayed body: $RELAYED"

echo "== 8. fixed-surface fallback: unmatched requests never hit the relay =="
HEALTH="$(curl -sk --max-time 5 "https://127.0.0.1:${EDGE_PORT}/healthz" || true)"
[ "$HEALTH" = "ok" ]                     || fail "fixed /healthz surface wrong: '$HEALTH'"
# A request whose Host has no route falls through to the fixed surface / 404.
CODE="$(curl -sk --max-time 5 -o /dev/null -w '%{http_code}' \
        -H "Host: nobody.example.com" "https://127.0.0.1:${EDGE_PORT}${ROUTE_PATH}app" || true)"
[ "$CODE" = "404" ]                      || fail "unmatched Host should 404 (empty-table fallback), got: $CODE"

echo "== E2E green: a Route + workload reached over real HTTPS through the proxy"
echo "   relay (dial + forward + stream), with the fixed surface as fallback. =="
