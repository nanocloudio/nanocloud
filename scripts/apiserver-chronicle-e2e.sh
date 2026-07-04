#!/usr/bin/env bash
# Live E2E for the apiserver READ PATH served by a Chronicle graph.
#
# The claim under test: a Kubernetes read falls out of generic engines carrying
# compiled params plus two nanocloud-owned providers, with no bespoke module
# holding routing or storage MEANING.
#
#   curl -> linux_net -> http (wave)      terminates HTTP, HANDLER_APP fan-out
#        -> kube_decode                   envelope -> Request record + store key
#        -> route      (decision)         apiserver.uproc's `to_store`, as params
#        -> store_effect                  GET, answering with the STORE's status
#        -> enc_http   (pipeline)         ser bytecode: StoreReply -> HttpResponse
#        -> http -> curl
#
# Only `kube_decode` and `store_effect` are nanocloud code, and both only
# reshape bytes: the routing decision is a param compiled from
# modules/app/_chronicle/apiserver.uproc, and the response framing is a `ser`
# program. Nothing about /api/v1 is baked into an .fmod.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=5396

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in kube_decode store_effect decision pipeline http; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apichron-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
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

echo "== 1. seed a Pod object =="
POD='{"metadata":{"name":"web-0","namespace":"default"},"spec":{"containers":[{"name":"web","image":"nginx"}]}}'
store_put "/pods/default/web-0" "$POD"

echo "== 2. write the graph — engines + params, two nanocloud providers =="
DEC_ROUTE="$(nc_decision "$ROOT/modules/app/_chronicle/apiserver.uproc" serve)"
[ -n "$DEC_ROUTE" ] || { echo "FAIL: could not compile apiserver.uproc serve"; exit 1; }
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
  - name: kube_decode
  - name: route
    type: decision
    params:
      # apiserver.uproc 'to_store': method -> store op, cid and key through.
      decision: "${DEC_ROUTE}"
  - name: store_effect
  - name: enc_http
    type: pipeline
    params:
      # apiserver.uproc 'pass_reply' — a codec-only node fails CLOSED.
      ir_stages: "01ff250005000101000000120100000005000102000000120200000005000103000000120300000013"
      # StoreReply -> HttpResponse. The cid is emitted as ONE 4-byte LE int, so
      # conn_id and stream_id land whole — a codec that hardcodes stream_id=0
      # does not satisfy wave's app contract.
      encode: "01000201000000620401010002020000006202016001000060010010600200000001000203000000646202016010006170706c69636174696f6e2f6a736f6e01000203000000616a"
wiring:
  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in
  - from: http.req_out
    to: kube_decode.req_in
    buffer_group: 1
  # NO buffer_group on the engine edges. A Chronicle engine self-frames over a
  # byte stream (io_core::admit_frame: peek, read the length from the header,
  # consume exactly that), so mailbox mode breaks it — the registry example
  # mailboxes only its http edges for the same reason.
  - from: kube_decode.record_out
    to: route.record_in
  # store_effect reads a whole record per step and DECLARES its input framed,
  # so this edge is mailboxed — the build enforces it.
  - from: route.result_out
    to: store_effect.request_in
    buffer_group: 3
  - from: store_effect.response_out
    to: enc_http.record_in
  - from: enc_http.result_out
    to: http.resp_in
    buffer_group: 4
YAML

echo "== 3. build =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

# One request per boot. Even with `resp_in` wired, an `app: true` route services
# exactly one request per run here; a fresh instance per case keeps each
# assertion unambiguous rather than depending on request ordering. Worth chasing
# separately — it is a property of the fan-out, not of this chain, and both
# cases pass when each is the first request.
drive() { # drive <url> -> "<code>|<body>"
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  for _ in $(seq 80); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
    sleep 0.1
  done
  local out code body
  out="$(curl -s -i --max-time 6 "$1" || true)"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
  code="$(printf '%s' "$out" | head -1 | tr -d '\r' | awk '{print $2}')"
  body="$(printf '%s' "$out" | tr -d '\r' | awk 'BEGIN{b=0} /^$/{b=1;next} b{print}')"
  printf '%s|%s' "$code" "$body"
}

echo "== 4. GET the object through the graph =="
R="$(drive "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/web-0")"
CODE="${R%%|*}"; BODY="${R#*|}"
[ "$CODE" = "200" ] || fail "status: got '$CODE' want 200"
[ "$BODY" = "$POD" ] || fail "body mismatch:
  got:  $BODY
  want: $POD"
echo "   GET /api/v1/namespaces/default/pods/web-0 -> 200, body byte-identical to the stored object"

echo "== 5. an absent object gets the STORE's 404, not the graph's opinion =="
R="$(drive "http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/nope")"
CODE="${R%%|*}"
[ "$CODE" = "404" ] || fail "absent object: got '$CODE' want 404"
echo "   GET .../pods/nope -> 404 (the store's, never learned by the graph)"

echo "== E2E green: the apiserver read path served by Chronicle engines + compiled params — routing is data, not an .fmod =="
