#!/usr/bin/env bash
# Live E2E for the apiserver graph behind TLS —
# `linux_net → tls → api_ingress`: fluxor's tls module terminates TLS 1.3 and
# hands cleartext HTTP to api_ingress, which serves it as PIC. Proves the tls⇄
# ingress wiring, bind-through-tls, and HTTPS termination. No host code.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
for m in tls api_ingress api_responder; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apisrv-https-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. generate a P-256 server cert (DER, as the tls module wants) =="
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$D/key.pem" -out "$D/cert.pem" -days 1 -subj "/CN=apiserver" 2>/dev/null
openssl x509 -in "$D/cert.pem" -outform DER -out "$D/cert.der" 2>/dev/null
openssl ec   -in "$D/key.pem"  -outform DER -out "$D/key.der"  2>/dev/null

echo "== 2. write the TLS apiserver graph =="
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
platform:
  net: {}
scheduler:
  accept_cycles: true
modules:
  - name: tls
    mode: 1
    verify_peer: 0
    cert_file: "$D/cert.der"
    key_file: "$D/key.der"
  - name: api_ingress
  - name: api_responder
wiring:
  - from: linux_net.net_out
    to: tls.cipher_in
  - from: tls.cipher_out
    to: linux_net.net_in
  - from: tls.clear_out
    to: api_ingress.net_in
  - from: api_ingress.net_out
    to: tls.clear_in
  - from: api_responder.status
    to: api_responder.changes
YAML

echo "== 3. build config + module table =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 4. start the graph =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

echo "== 5. HTTPS GET, terminated by tls → api_ingress =="
https_get() {
  python3 - "$PORT" "$1" <<'PY'
import socket, ssl, sys
port, path = int(sys.argv[1]), sys.argv[2]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
raw = socket.create_connection(("127.0.0.1", port), timeout=8)
s = ctx.wrap_socket(raw, server_hostname="localhost")
s.sendall(f"GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode())
buf = b""; s.settimeout(8)
try:
    while True:
        c = s.recv(4096)
        if not c: break
        buf += c
except (socket.timeout, ssl.SSLError):
    pass
s.close()
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n",1)[0].split(b" ",2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}
out="$(https_get /healthz)"; st="${out%%|*}"; body="${out#*|}"
[ "$st" = "200" ] || fail "GET /healthz over TLS: got '$st' (body: $body)"
case "$body" in *ok*) ;; *) fail "GET /healthz body: '$body'";; esac
echo "   HTTPS GET /healthz -> $st $body"

echo "== E2E green: TLS 1.3 terminated by the graph, served by api_ingress — no host code =="
