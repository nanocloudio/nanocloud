#!/usr/bin/env bash
# Live E2E for dynamic listeners via mid-life bind: a listener row appearing
# in the store makes the edge bind a NEW port MID-LIFE (outside the init bind
# path)
# and route traffic on it through the relay — and a listener removed from the
# compiled set is no longer bound.
#
#   external curl (HTTP <DYN_PORT>)      external curl (HTTP <HEALTH_PORT>/healthz)
#        │  cleartext (tcp/tls=0; http drives linux_net directly)
#        ▼                                    ▼
#   http anchor ── mid-life CMD_BIND(<DYN_PORT>) from the pre-leased pool
#        │  the STATIC listener is <HEALTH_PORT> (init path); <DYN_PORT> is
#        │  bound by the listener table_consumer's reconciler AFTER bound=1,
#        │  i.e. the new mid-life mechanism, not the Init→Binding→WaitBound path
#        │  match_dyn_route(host, path) → dial backend → forward → stream back
#        ▼
#   backend workload (host-shared endpoint — the Linux-expressible path)
#
# GRANT MODEL: a pre-leased port POOL. In a real nanocloud node the edge
# owner's plan carries one endpoint lease per pool port (an `export` per port
# → tools/compose.rs PlanLease); the kernel gate enforces "bind ∈ lease set", so mid-life bind just draws
# from the pool. This STANDALONE `fluxor run` stages no plan, so the lease
# gate is Ungated and any pooled bind succeeds — the pool is expressed by the
# /dataplane/edge-listeners/ rows the (unstaged) edge owner is allowed to bind.
# The lease-REFUSED path (unleased port → MSG_BIND_REFUSED, no listener) is
# proven deterministically in the harness (http_listeners.rs), which stages
# the enforcement the durable-log standalone store cannot.
#
# SCOPE: cleartext (tls=0) edge; the tls-fronted dynamic listener (tls=1) is
# a deferred follow-on. Run once `fluxor modules build --target bcm2712` has
# produced http.fmod.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
HEALTH_PORT="${HEALTH_PORT:-18081}"      # static listener (init path), no root
DYN_PORT="${DYN_PORT:-18090}"            # mid-life-bound listener
BACKEND_PORT="${BACKEND_PORT:-18099}"
BACKEND_ADDR="127.0.0.1:${BACKEND_PORT}"
ROUTE_HOST="api.example.com"
ROUTE_PATH="/v1/"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor  >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v curl    >/dev/null || { echo "FAIL: curl not found"; exit 1; }
command -v python3 >/dev/null || { echo "FAIL: python3 not found"; exit 1; }
[ -x "$FLUXOR_RUNTIME" ] || { echo "FAIL: fluxor-linux runtime not built ($FLUXOR_RUNTIME)"; exit 1; }
[ -e "$MODULES_DIR/http.fmod" ] || { echo "FAIL: missing http.fmod (fluxor modules build --target bcm2712)"; exit 1; }

D="$(mktemp -d /tmp/nc-ingress-dynlisten-XXXXXX)"
RUNTIME_PID=""; BACKEND_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true
  [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null || true
  rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; echo "--- run.log tail ---"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

# WAL op into the shared control-plane store (op 1 = PUT, 2 = DELETE).
wal_op() {
  python3 - "$D/store.log" "$1" "$2" "${3:-}" <<'PY'
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

echo "== 1. start the backend workload (host-shared endpoint) =="
cat >"$D/backend.py" <<'PY'
import socket, sys, threading
port = int(sys.argv[1])
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port)); srv.listen(16)
def handle(c):
    c.recv(65536)
    body = "workload-ok\n"
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

echo "== 2. write the CLEARTEXT edge graph (http ⇄ linux_net directly) =="
cat >"$D/edge.yaml" <<YAML
target: linux
tick_us: 100
scheduler:
  accept_cycles: true
platform:
  net: {}
modules:
  - name: http
    port: ${HEALTH_PORT}
    host_tcp: 1
    routes_prefix: "/dataplane/edge/"
    listeners_prefix: "/dataplane/edge-listeners/"
    routes:
      - path: "/healthz"
        body: "ok"
        content_type: "text/plain"
wiring:
  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in
    buffer_bytes: 32768
  - from: http.routes_sink
    to: http.routes_changes
  - from: http.listeners_sink
    to: http.listeners_changes
YAML

echo "== 3. seed the Route → /dataplane/edge/ and the desired listener =="
wal_op 1 "/dataplane/edge/default/web" \
  "host=${ROUTE_HOST};path=${ROUTE_PATH};be=${BACKEND_ADDR}:1:1"
# The compiled dynamic listener the edge mid-life-binds (the route compiler's
# /listeners/ → /dataplane/edge-listeners/ seam is proven separately).
wal_op 1 "/dataplane/edge-listeners/${DYN_PORT}" "proto=tcp;tls=0"

echo "== 4. build config + module table, start the edge =="
nc_build_workload "$ROOT" "$D/edge.yaml" "$D/config.bin" "$D/modules.bin"
start_edge() {
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
}
start_edge

echo "== 5. wait for the STATIC listener (init bind path) =="
for _ in $(seq 100); do
  curl -s --max-time 1 "http://127.0.0.1:${HEALTH_PORT}/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done
HEALTH="$(curl -s --max-time 5 "http://127.0.0.1:${HEALTH_PORT}/healthz" || true)"
[ "$HEALTH" = "ok" ] || fail "static /healthz surface wrong: '$HEALTH'"
echo "  static listener up on ${HEALTH_PORT}"

echo "== 6. THE PAYOFF: the MID-LIFE-bound ${DYN_PORT} routes through the relay =="
# ${DYN_PORT} is NOT in the init bind path — it was bound by the listener
# reconciler after bound=1. A request on it must reach the backend.
for _ in $(seq 100); do
  curl -s --max-time 1 -H "Host: ${ROUTE_HOST}" \
    "http://127.0.0.1:${DYN_PORT}${ROUTE_PATH}app" >/dev/null 2>&1 && break
  sleep 0.1
done
RELAYED="$(curl -s --max-time 5 -H "Host: ${ROUTE_HOST}" \
           "http://127.0.0.1:${DYN_PORT}${ROUTE_PATH}app" || true)"
grep -q "workload-ok" <<<"$RELAYED" \
  || fail "mid-life-bound listener did not relay to the workload; got: '$RELAYED'"
echo "  relayed body on the dynamic port: $RELAYED"

echo "== 7. remove the listener from the compiled set → the port stops serving =="
# The durable-log standalone store does not tail external appends, so removal
# is applied across a restart (the in-process runtime withdrawal is proven in
# the harness: http_listeners.rs::withdrawn_listener_is_torn_down). Delete the
# listener key, restart, and assert ${DYN_PORT} is no longer bound.
kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
wal_op 2 "/dataplane/edge-listeners/${DYN_PORT}"
start_edge
for _ in $(seq 100); do
  curl -s --max-time 1 "http://127.0.0.1:${HEALTH_PORT}/healthz" >/dev/null 2>&1 && break
  sleep 0.1
done
CODE="$(curl -s --max-time 3 -o /dev/null -w '%{http_code}' \
        -H "Host: ${ROUTE_HOST}" "http://127.0.0.1:${DYN_PORT}${ROUTE_PATH}app" 2>/dev/null || true)"
# A refused connection yields an empty/000 code (curl connect error).
[ "$CODE" = "000" ] || [ -z "$CODE" ] \
  || fail "removed listener still accepted on ${DYN_PORT} (http_code=$CODE)"
# The static surface still serves — only the withdrawn listener stopped.
HEALTH2="$(curl -s --max-time 5 "http://127.0.0.1:${HEALTH_PORT}/healthz" || true)"
[ "$HEALTH2" = "ok" ] || fail "static surface regressed after listener removal: '$HEALTH2'"
echo "  ${DYN_PORT} no longer accepts (http_code='${CODE:-<none>}'); static surface intact"

echo "== E2E green: a mid-life-bound listener carried real traffic through the"
echo "   relay, and a removed listener stopped accepting (cleartext / tls=0). =="
