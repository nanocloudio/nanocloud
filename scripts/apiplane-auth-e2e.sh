#!/usr/bin/env bash
# Live E2E for the API plane's AUTH STAGE served by a Chronicle graph.
#
# The claim under test: a request can leave the graph mid-chain, be answered by
# somebody ELSE's module, and come back still knowing which request it was —
# with the routing and the refusal ladder as params, not as an .fmod.
#
#   curl -> linux_net -> http (wave)      terminates HTTP, HANDLER_APP fan-out
#        -> http_chain                    the exchange -> one request envelope
#        -> kube_decode                   envelope -> Request record + credential
#        -> admit      (decision)         apiplane_auth.uproc `admit`, as params
#        -> kagi_verify                   the kagi seam: credential out, identity back
#             <-> token_verify (KAGI)     the signature check, kagi's module
#        -> decide     (decision)         apiplane_auth.uproc `decide`, as params
#        -> enc_http   (pipeline)         the READ PATH'S ser program, byte-identical
#        -> http_chain -> http -> curl
#
# The stage under test is the FILTER: one that calls out of the graph and
# correlates back. The only nanocloud code in it is `kagi_verify`, which decides
# nothing — the 200/401/503 ladder is in
# modules/app/_chronicle/apiplane_auth.uproc, and the response encoder is the
# read path's, because a response is data too.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=5397

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in kagi_verify token_verify decision pipeline http http_chain; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apiauth-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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
PY
}

echo "== 1. mint an ES256 keypair, a kagi MSG_KEY_ADD frame and a real JWT =="
# The shapes kagi's `token_mint` announces (a VERIFY MSG_KEY_ADD, stored at
# /authn-keys/sa by kagi_mint) and mints (an ES256 JWS). Generated here rather than by running
# the live mint so the test can also produce an EXPIRED token, which a live issuer
# will not do.
python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization

D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
assert len(pub) == 65

ISSUER = b"https://kubernetes.default.svc"
KID = b"sa-1"
def f8(b):  return bytes([len(b)]) + b
def f16(b): return struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1)
        + bytes([1])          # KEY_STATE_ACTIVE
        + bytes([0x01])       # KEY_USE_VERIFY
        + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0)
        + f16(pub))
frame = bytes([0x22]) + struct.pack("<H", len(body)) + body
open(f"{D}/keyframe.hex", "w").write(binascii.hexlify(frame).decode())

def b64u(b): return base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"

def mint(exp_offset):
    now = int(time.time())
    claims = {
        "iss": ISSUER.decode(),
        "sub": "system:serviceaccount:default:builder",
        "aud": "nanocloud",
        "kubernetes.io": {"namespace": "default",
                          "serviceaccount": {"name": "builder"}},
        "iat": now if exp_offset > 0 else now - 7200,
        "exp": now + exp_offset,
    }
    payload = json.dumps(claims, separators=(",", ":")).encode()
    signing = HDR + b"." + b64u(payload)
    der = key.sign(signing, ec.ECDSA(hashes.SHA256()))
    r, s = utils.decode_dss_signature(der)
    raw = r.to_bytes(32, "big") + s.to_bytes(32, "big")   # JOSE, not DER
    return (signing + b"." + b64u(raw)).decode()

open(f"{D}/token.good", "w").write(mint(3600))
open(f"{D}/token.expired", "w").write(mint(-3600))
print("   keypair, MSG_KEY_ADD frame, valid token and expired token written")
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
echo "   /authn-keys/sa seeded with the kagi key frame"

echo "== 2. write the graph — engines + params, ONE new nanocloud provider =="
DEC_ADMIT="$(nc_decision "$ROOT/modules/app/_chronicle/apiplane_auth.uproc" admit)"
DEC_DECIDE="$(nc_decision "$ROOT/modules/app/_chronicle/apiplane_auth.uproc" decide)"
[ -n "$DEC_ADMIT" ]  || { echo "FAIL: could not compile apiplane_auth.uproc admit"; exit 1; }
[ -n "$DEC_DECIDE" ] || { echo "FAIL: could not compile apiplane_auth.uproc decide"; exit 1; }
# The request decoder, as the apiplane graph carries it (kube.rd, kube.uproc).
. "$ROOT/scripts/chronicle-params.sh"
KD_DECODE="$(python3 "$ROOT/scripts/rdasm.py" "$ROOT/modules/app/_chronicle/kube.rd")"
KD_PASS="$(nc_stages "$ROOT/modules/app/_chronicle/kube.uproc" kd_pass)"
KD_ROUTE="$(nc_decision "$ROOT/modules/app/_chronicle/kube.uproc" kd_route)"
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
    routes:
      - path: "/api/"
        app: true
  # The provider behind the http application routes: each exchange one
  # request envelope for kube_decode.
  - name: http_chain
  - name: kube_decode
    type: pipeline
    params:
      decode: "${KD_DECODE}"
      ir_stages: "${KD_PASS}"
  - name: kube_route
    type: decision
    params:
      decision: "${KD_ROUTE}"
  - name: admit
    type: decision
    params:
      # apiplane_auth.uproc 'admit': credential to field 8, request to carry 30..35.
      decision: "${DEC_ADMIT}"
  - name: kagi_verify
    audience: "nanocloud"
    issuer: "https://kubernetes.default.svc"
    key_path: "/authn-keys/sa"
  - name: token_verify
  - name: decide
    type: decision
    params:
      # apiplane_auth.uproc 'decide': verify_err -> HTTP status + body.
      decision: "${DEC_DECIDE}"
  - name: enc_http
    type: pipeline
    params:
      # BYTE-IDENTICAL to the read path's encoder (apiserver-chronicle-e2e.sh).
      # The reply shape is the same three fields, so the response leg needed no
      # new bytecode and no new module.
      ir_stages: "01ff250005000101000000120100000005000102000000120200000005000103000000120300000013"
      encode: "01000201000000620401010002020000006202016001000060010010600200000001000203000000646202016010006170706c69636174696f6e2f6a736f6e01000203000000616a"
wiring:
  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in
  - from: http.request_out
    to: http_chain.request_in
  - from: http_chain.response_out
    to: http.response_in
  # A decode-mode pipeline reads one envelope per read: mailbox.
  - from: http_chain.record_out
    to: kube_decode.record_in
    buffer_group: 1
  # No buffer_group on engine edges: a Chronicle engine self-frames over a byte
  # stream, so mailbox mode breaks it. The edges into modules that read a whole
  # record per step and DECLARE their input framed are mailboxed.
  - from: kube_decode.result_out
    to: kube_route.record_in
  - from: kube_route.result_out
    to: admit.record_in
  - from: admit.result_out
    to: kagi_verify.request_in
    buffer_group: 3
  # The kagi seam, both directions plus the key: one exchange per credential,
  # kagi_verify the requester and token_verify the provider. Exchange edges
  # are framed by their content type; the build gives them their mailbox.
  - from: kagi_verify.key_out
    to: token_verify.verify_key
  - from: kagi_verify.request_out
    to: token_verify.request_in
  - from: token_verify.response_out
    to: kagi_verify.response_in
  - from: kagi_verify.response_out
    to: decide.record_in
  - from: decide.result_out
    to: enc_http.record_in
  - from: enc_http.result_out
    to: http_chain.reply_in
    buffer_group: 5
YAML

echo "== 3. build =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

# One request per boot, as in the read-path E2E: an `app: true` route services
# exactly one request per run there too. A fresh instance per case keeps each
# assertion unambiguous.
drive() { # drive <auth-header-or-empty> -> "<code>|<body>"
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  for _ in $(seq 80); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
    sleep 0.1
  done
  local out code body url="http://127.0.0.1:$PORT/api/v1/namespaces/default/pods/web-0"
  if [ -n "$1" ]; then
    out="$(curl -s -i --max-time 6 -H "Authorization: $1" "$url" || true)"
  else
    out="$(curl -s -i --max-time 6 "$url" || true)"
  fi
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
  code="$(printf '%s' "$out" | head -1 | tr -d '\r' | awk '{print $2}')"
  body="$(printf '%s' "$out" | tr -d '\r' | awk 'BEGIN{b=0} /^$/{b=1;next} b{print}')"
  printf '%s|%s' "$code" "$body"
}

echo "== 4. a valid ServiceAccount token is verified by kagi and admitted =="
R="$(drive "Bearer $(cat "$D/token.good")")"
CODE="${R%%|*}"; BODY="${R#*|}"
[ "$CODE" = "200" ] || fail "valid token: got '$CODE' want 200"
[ "$BODY" = "system:serviceaccount:default:builder" ] \
  || fail "subject: got '$BODY' want 'system:serviceaccount:default:builder'"
echo "   200, body is the SUBJECT kagi established — the identity crossed the seam and came back"

echo "== 5. no credential -> 401, by the same path as a bad one =="
R="$(drive "")"
CODE="${R%%|*}"; BODY="${R#*|}"
[ "$CODE" = "401" ] || fail "no credential: got '$CODE' want 401"
[ "$BODY" = "no usable credential" ] || fail "no credential body: got '$BODY'"
echo "   401 'no usable credential' — the absent case reached kagi rather than being shortcut"

echo "== 6. an expired token is refused as EXPIRED, not as a bad signature =="
R="$(drive "Bearer $(cat "$D/token.expired")")"
CODE="${R%%|*}"; BODY="${R#*|}"
[ "$CODE" = "401" ] || fail "expired token: got '$CODE' want 401"
[ "$BODY" = "credential expired" ] \
  || fail "expired body: got '$BODY' want 'credential expired' (a valid signature outside its window)"
echo "   401 'credential expired' — kagi's typed reason survived as a distinct answer"

echo "== 7. a tampered token is refused =="
GOOD="$(cat "$D/token.good")"
R="$(drive "Bearer ${GOOD%?}X")"
CODE="${R%%|*}"
[ "$CODE" = "401" ] || fail "tampered token: got '$CODE' want 401"
echo "   401 — a flipped signature byte does not verify"

echo "== 8. a token the LIVE issuer mints (sa_token chain -> kagi token_mint) is admitted =="
# The packaged sa-token graph over the same store: it opens the vault root,
# stores its announced key at /authn-keys/sa (replacing the one seeded above)
# and answers a /token-req/. The API graph, restarted, loads that key.
store_put "/token-req/live" "$(printf 'ns=team;sa=deployer' | xxd -p | tr -d '\n')"
nc_build_workload "$ROOT" "$ROOT/packaging/debian/fluxor-sa-token.yaml" "$D/sat-config.bin" "$D/sat-modules.bin"
SEAL="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
FLUXOR_SEAL_KEY="$SEAL" FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 4 "$FLUXOR_RUNTIME" \
  --config "$D/sat-config.bin" --modules "$D/sat-modules.bin" >>"$D/run.log" 2>&1 || true
LIVE="$(python3 - "$D/store.log" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0; out = b""
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == b"/token-resp/live" and op == 1: out = data[p+15+kl:p+15+kl+vl]
    p += 15 + kl + vl
print(out.decode().removeprefix("token="))
PY
)"
[ -n "$LIVE" ] || fail "the live issuer did not answer /token-req/live"
R="$(drive "Bearer $LIVE")"
CODE="${R%%|*}"; BODY="${R#*|}"
[ "$CODE" = "200" ] || fail "live token: got '$CODE' want 200 ($BODY)"
[ "$BODY" = "system:serviceaccount:team:deployer" ] || fail "live subject: got '$BODY'"
R="$(drive "Bearer $(cat "$D/token.good")")"
[ "${R%%|*}" = "401" ] || fail "a token under the replaced key must no longer verify: got '${R%%|*}'"
echo "   200 for the minted token; the old key's token now 401 — one issuer, one key"

echo "== E2E green: the API plane's auth stage is a Chronicle filter — one connector, zero decisions in .fmod =="
