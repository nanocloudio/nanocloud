#!/usr/bin/env bash
# Live E2E for `kube_decode` — the typed seam between wave's HTTP fan-out and
# Chronicle's engines.
#
# Chronicle's VM cannot split a variable-arity URL path (no iteration, by
# construction) and its record frame is flat, so a Kubernetes request must be
# projected into decided-on fields before a `decision` can route it. This module
# does that projection, and this script proves it emits a Chronicle v1 record
# frame that is byte-for-byte what `pipeline_core.rs` would decode.
#
# The assertion is EXACT: the script builds the expected frame independently, in
# python, from the documented layout —
#   [count:u8] then count x [number:u8][type:u8][len:u16 LE][payload]
#   TY_BYTES = 0, TY_I64 = 1 (payload 8 bytes LE)
# — and requires those exact bytes to appear in what the sink received. A frame
# that is merely "close" fails.
#
# Chain: curl -> linux_net -> http (wave) -> kube_decode -> debug (the sink,
# which logs the raw bytes it is handed).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=5394

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in kube_decode http debug; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-kubedec-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. write the decode graph =="
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
platform:
  net: {}
scheduler:
  accept_cycles: true
modules:
  - name: http
    port: $PORT
    host_tcp: 1
    max_body_kib: 64
    routes:
      - path: "/api/"
        app: true
      - path: "/apis/"
        app: true
      # `/api/` is a PREFIX and does not cover `/version`: a path with no route
      # never reaches the app at all, and wave answers its own 404.
      - path: "/version"
        app: true
  - name: kube_decode
    # Non-resource paths reach the graph only with this on — the case the
    # apiplane graph relies on for discovery documents.
    passthrough: 1
  - name: debug
    mode: 1        # plaintext: log the bytes verbatim, not a hash of them
wiring:
  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in
  - from: http.req_out
    to: kube_decode.req_in
    buffer_group: 1
  - from: kube_decode.record_out
    to: debug.data
    # `record_out` is declared framed, so the graph build requires mailbox mode
    # here — a byte FIFO would fragment these records under load.
    buffer_group: 2
YAML

echo "== 2. build =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

# One request per graph instance, deliberately. An `app: true` route hands the
# request to the graph and waits for an answer on `resp_in`; nothing here
# answers, so http services exactly ONE request per boot. That is the correct
# behaviour for a half-wired app route, and running a fresh instance per case
# keeps each assertion unambiguous rather than depending on request ordering.
drive() { # drive <log> <curl args...>
  local log="$1"; shift
  FLUXOR_STORE_DIR="$D" RUST_LOG=info "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$log" 2>&1 &
  RUNTIME_PID=$!
  for _ in $(seq 80); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
    sleep 0.1
  done
  curl -s --max-time 2 "$@" >/dev/null 2>&1 || true
  sleep 0.4
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}

echo "== 3. a namespaced GET (core group, object) =="
drive "$D/get.log" "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/web-0"

echo "== 4. a POST with a body (named group, collection — no name) =="
drive "$D/post.log" -X POST --data-binary '{"metadata":{"name":"web"}}' \
  "http://127.0.0.1:$PORT/apis/apps/v1/namespaces/kube-system/deployments"

echo "== 4b. a GET carrying a bearer credential =="
drive "$D/auth.log" -H "Authorization: Bearer abc.def.ghi" \
  "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/web-0"

echo "== 4c. a collection GET with ?watch=true =="
drive "$D/watch.log" "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods?watch=true&resourceVersion=0"

echo "== 4d. a NON-resource path, under passthrough =="
drive "$D/pass.log" "http://127.0.0.1:$PORT/version"

echo "== 5. assert the exact Chronicle v1 frames =="
python3 - "$D/get.log" "$D/post.log" "$D/auth.log" "$D/watch.log" "$D/pass.log" <<'PY'
import struct, sys
logs = [open(a, "rb").read() for a in sys.argv[1:]]

TY_BYTES, TY_I64 = 0, 1

def field(num, ty, payload):
    return bytes([num, ty]) + struct.pack("<H", len(payload)) + payload

def frame(wave_id_known, method, resource, namespace, name, body, key,
          credential=b"", target=b"", query=b"", watch=0, peer=b""):
    """Every field except the correlation, which the graph assigns."""
    return (field(2, TY_I64, struct.pack("<q", method))
            + field(3, TY_BYTES, resource)
            + field(4, TY_BYTES, namespace)
            + field(5, TY_BYTES, name)
            + field(6, TY_BYTES, body)
            # Field 7 is the STORE KEY, assembled here because the Chronicle VM
            # has no string concatenation (ADD is integer-only).
            + field(7, TY_BYTES, key)
            # Field 8 is the bearer CREDENTIAL with its scheme stripped, and 9
            # the raw request target. Both are extracted for the same reason as
            # the key: the VM cannot scan a header block or split on a space.
            # An absent Authorization header is an EMPTY field, never a missing
            # one — "no credential" is an authorization answer downstream, not
            # an unroutable request here.
            + field(8, TY_BYTES, credential)
            + field(9, TY_BYTES, target)
            # The QUERY, split off before the path is parsed, and `watch=true`
            # as a NUMBER — the VM has no substring test, so the flag cannot be
            # a comparison downstream.
            + field(10, TY_BYTES, query)
            + field(11, TY_I64, struct.pack("<q", watch))
            # The verified mTLS peer, hex. Empty on plaintext — which is every
            # case here, and the point: the field is always PRESENT, because a
            # consumer indexes by number and an absent field is not an empty one.
            + field(12, TY_BYTES, peer))

# wave wire/method.rs: GET = 1, POST = 3 (the registry's uproc pins GET = 1).
cases = [
    ("GET  /api/v1/namespaces/default/pods/web-0",
     frame(None, 1, b"pods", b"default", b"web-0", b"", b"/pods/default/web-0",
           b"", b"/api/v1/namespaces/default/pods/web-0")),
    # A CREATE names its object in the BODY: the path has no name, so the key
    # is addressed from `metadata.name`. Path name wins when there is one.
    ("POST /apis/apps/v1/namespaces/kube-system/deployments",
     frame(None, 3, b"deployments", b"kube-system", b"",
           b'{"metadata":{"name":"web"}}', b"/deployments/kube-system/web",
           b"", b"/apis/apps/v1/namespaces/kube-system/deployments")),
    ("GET  .../pods/web-0 with Authorization: Bearer",
     frame(None, 1, b"pods", b"default", b"web-0", b"", b"/pods/default/web-0",
           b"abc.def.ghi", b"/api/v1/namespaces/default/pods/web-0")),
    # The query is split off BEFORE the path is parsed: without that, the
    # resource here is literally named `pods?watch=true`.
    ("GET  .../pods?watch=true (collection watch)",
     frame(None, 1, b"pods", b"default", b"", b"", b"/pods/default/",
           b"", b"/api/v1/namespaces/default/pods",
           b"watch=true&resourceVersion=0", 1)),
    # Passthrough: resource, namespace, name and key all EMPTY, the target
    # carried so a decision can answer by path. The field COUNT must match the
    # resource case exactly — a frame that is shorter on one branch makes the
    # decision reading it produce nothing at all.
    ("GET  /version (passthrough, not a resource path)",
     frame(None, 1, b"", b"", b"", b"", b"", b"", b"/version")),
]

fails = []
for (label, tail), log in zip(cases, logs):
    at = log.find(tail)
    if at < 0:
        fails.append(label)
        continue
    # The correlation field and the count byte precede the tail: verify the
    # whole frame, not just the part that was easy to predict.
    corr_at = at - 12          # field 1: [1][TY_I64][8,0] + 8 payload bytes
    count_at = corr_at - 1
    if count_at < 0 or log[count_at] != 12:
        fails.append(label + " (field count != 12)")
        continue
    if log[corr_at] != 1 or log[corr_at + 1] != TY_I64:
        fails.append(label + " (field 1 is not the i64 correlation)")
        continue
    corr = struct.unpack("<q", log[corr_at + 4:corr_at + 12])[0]
    print("   ok  %-52s corr=0x%08x, 12 fields, byte-exact" % (label, corr))

if fails:
    for f in fails:
        print("   FAIL " + f)
    sys.exit(1)
PY
[ $? -eq 0 ] || fail "frame assertion failed"

echo "== E2E green: kube_decode projects a Kubernetes request into a byte-exact Chronicle v1 record frame =="
