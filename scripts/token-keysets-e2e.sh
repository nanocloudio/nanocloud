#!/usr/bin/env bash
# Live E2E: the API verifies tokens under more than one key set, and a
# ServiceAccount renews its own token — and only its own.
#
# The control plane as packaged, rendered as the rigs render it: a
# provisioned ServiceAccount issuer's key seeded at /authn-keys/sa (the key
# this test signs the operator's token with), and the cell's own mint
# announcing at /authn-keys/node, beside it. kagi_verify's `key_paths` holds
# both as ONE keyset (MSG_KEYSET_SNAPSHOT), so:
#   - a TokenRequest (`kubectl create token`) answers a token the cell's
#     mint signed, and that token reads /api/v1/…/services: 200;
#   - the ServiceAccount renews with it (a TokenRequest for itself, granted
#     by a Role restricted to its own name — resourceNames on the
#     serviceaccounts/token subresource), and the renewed token reads too;
#     a TokenRequest for ANOTHER ServiceAccount is 403;
#   - a second configured issuer (`issuer` a list, a third key path) verifies;
#     a token naming an issuer not configured is 401, and so is one signed
#     by a key no path holds, and one claiming one configured issuer under
#     the other's key (keys are selected by issuer and kid);
#   - the key at a path is replaced while the plane runs: the new key's
#     tokens verify and the old key's are refused — the set is replaced,
#     not accumulated.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
command -v redis-cli >/dev/null || { echo "FAIL: redis-cli not found"; exit 1; }

D="$(mktemp -d /tmp/nc-keysets-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() {
  echo "FAIL: $1"
  if [ -f "$STORE/read.port" ]; then
    echo "   key sets in the store: $(nc_store_keys "$STORE" /authn-keys/ | tr '\n' ' ')"
  fi
  tail -30 "$D/run.log" 2>/dev/null || true
  exit 1
}

store_put() { # store_put <key> <hex-value>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
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
put_text() { store_put "$1" "$(printf '%s' "$2" | python3 -c 'import sys,binascii;print(binascii.hexlify(sys.stdin.buffer.read()).decode())')"; }

ISS_A="https://kubernetes.default.svc"
ISS_B="https://issuer-b.example"

echo "== 1. keys: the provisioned issuer's, a second issuer's (and its successor), a stranger's =="
# Each key's MSG_KEY_ADD frame as a mint announces it, and tokens signed
# under it: <name>.frame (hex), tok.<name>.
python3 - "$D" "$ISS_A" "$ISS_B" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D, ISS_A, ISS_B = sys.argv[1:4]
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
def frame(iss, kid, key):
    pub = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    body = (f8(iss.encode()) + struct.pack("<H", 1) + f8(kid.encode()) + struct.pack("<H", 1)
            + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1) + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
    return binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode()
def token(key, kid, iss, sub):
    now = int(time.time())
    hdr = b64u(json.dumps({"alg": "ES256", "typ": "JWT", "kid": kid}, separators=(",", ":")).encode())
    claims = {"iss": iss, "sub": sub, "aud": "nanocloud", "iat": now, "exp": now + 3600}
    signing = hdr + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    return (signing + b"." + b64u(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode()
keys = {n: ec.generate_private_key(ec.SECP256R1()) for n in ("a", "b", "b2", "z")}
open(f"{D}/a.frame", "w").write(frame(ISS_A, "sa-1", keys["a"]))
open(f"{D}/b.frame", "w").write(frame(ISS_B, "b-1", keys["b"]))
open(f"{D}/b2.frame", "w").write(frame(ISS_B, "b-2", keys["b2"]))
x = "system:serviceaccount:default:x"
open(f"{D}/tok.admin", "w").write(token(keys["a"], "sa-1", ISS_A, "system:serviceaccount:default:admin"))
open(f"{D}/tok.bx", "w").write(token(keys["b"], "b-1", ISS_B, x))
open(f"{D}/tok.b2x", "w").write(token(keys["b2"], "b-2", ISS_B, x))
# Signed under a configured key, naming an issuer nobody configured.
open(f"{D}/tok.unknown-iss", "w").write(token(keys["b"], "b-1", "https://unknown.example", x))
# A configured issuer's name, signed by a key no path holds.
open(f"{D}/tok.stranger", "w").write(token(keys["z"], "z-1", ISS_A, x))
# Issuer A's name, signed by issuer B's key and naming its kid: a key is
# selected by (issuer, kid), so B's key verifies nothing A claims.
open(f"{D}/tok.cross", "w").write(token(keys["b"], "b-1", ISS_A, x))
PY
store_put "/authn-keys/sa" "$(cat "$D/a.frame")"
store_put "/authn-keys/extra" "$(cat "$D/b.frame")"
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
put_text "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
echo "   /authn-keys/sa (issuer A, sa-1), /authn-keys/extra (issuer B, b-1) seeded"

echo "== 2. render the SHIPPED control plane: the mint at /authn-keys/node, a second issuer =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
grep -q 'key_paths: "/authn-keys/sa,/authn-keys/node"' "$D/graph.yaml" \
  || fail "the shipped plane does not verify both the issuer's and the mint's key sets"
# As the rigs render it: the provisioned issuer owns /authn-keys/sa, so the
# node's mint announces beside it.
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
# A second configured issuer, and its key set — the verifier's (the mint
# keeps signing as the cell's one issuer).
python3 - "$D/graph.yaml" "$ISS_A" "$ISS_B" <<'PY' || fail "the second issuer did not render"
import sys
path, a, b = sys.argv[1:]
g = open(path).read()
at = g.index("\n  - name: kagi_verify\n")
end = g.index("\n  - name:", at + 1)
blk = g[at:end]
new = blk.replace(f'issuer: "{a}"', f'issuer: "{a},{b}"').replace(
    'key_paths: "/authn-keys/sa,/authn-keys/node"',
    'key_paths: "/authn-keys/sa,/authn-keys/node,/authn-keys/extra"')
assert new.count(b) == 1 and "/authn-keys/extra" in new, blk
open(path, "w").write(g[:at] + new + g[end:])
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"

echo "== 3. run it; wait for the API =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"

api() { # api <token-file> <method> <path> [json-body] -> "<status>|<body>"
  python3 - "$PORT" "$1" "$2" "$3" "${4:-}" <<'PY'
import socket, ssl, sys
port, tokf, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
tok = open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
try:
    raw = socket.create_connection(("127.0.0.1", port), timeout=10)
    s = ctx.wrap_socket(raw, server_hostname="localhost")
except OSError as e:
    print(f"?|{e}"); sys.exit(0)
b = body.encode()
req = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n"
       f"Content-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b
s.sendall(req)
buf = b""
s.settimeout(10)
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
# until <seconds> <want-status-glob> <api args…>: ask until the answer matches
until_status() {
  local secs="$1" want="$2" r=""; shift 2
  for _ in $(seq "$secs"); do
    r="$(api "$@")"
    # shellcheck disable=SC2254
    case "${r%%|*}" in $want) printf '%s' "$r"; return 0 ;; esac
    sleep 1
  done
  printf '%s' "$r"; return 1
}
# The seeded key reaches the verifier through the replicated log: a 503 (no
# verification key) in the first seconds is that import, not a verdict.
r="$(until_status 90 200 "$D/tok.admin" GET /api/v1/namespaces/default/services)" \
  || fail "the provisioned issuer's token was not admitted: ${r:0:300}"
echo "   the provisioned issuer's operator token: GET services -> 200"

echo "== 4. two ServiceAccounts; x may mint its own token only, and read services =="
api "$D/tok.admin" POST /api/v1/namespaces/default/serviceaccounts '{"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"x"}}' >/dev/null
api "$D/tok.admin" POST /api/v1/namespaces/default/serviceaccounts '{"apiVersion":"v1","kind":"ServiceAccount","metadata":{"name":"y"}}' >/dev/null
r="$(api "$D/tok.admin" POST /apis/rbac.authorization.k8s.io/v1/namespaces/default/roles \
  '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"token-self","namespace":"default"},"rules":[{"apiGroups":[""],"resources":["serviceaccounts/token"],"resourceNames":["x"],"verbs":["create"]},{"apiGroups":[""],"resources":["services"],"verbs":["get","list"]}]}')"
case "$r" in 201\|*) ;; *) fail "Role token-self: ${r:0:300}" ;; esac
r="$(api "$D/tok.admin" POST /apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings \
  '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"x-token","namespace":"default"},"subjects":[{"kind":"ServiceAccount","name":"x","namespace":"default"}],"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"token-self"}}')"
case "$r" in 201\|*) ;; *) fail "RoleBinding x-token: ${r:0:300}" ;; esac
echo "   ServiceAccounts x, y; Role token-self {serviceaccounts/token, resourceNames [x], create} + services get,list -> x"

TR='{"apiVersion":"authentication.k8s.io/v1","kind":"TokenRequest","spec":{"audiences":["nanocloud"],"expirationSeconds":3600}}'
token_of() { python3 -c 'import json,sys;print(json.loads(sys.argv[1])["status"]["token"])' "$1"; }

echo "== 5. a TokenRequest answers a token the cell's mint signed, and it reads =="
r="$(api "$D/tok.admin" POST /api/v1/namespaces/default/serviceaccounts/x/token "$TR")"
case "$r" in 201\|*) ;; *) fail "TokenRequest for x as admin: ${r:0:300}" ;; esac
token_of "${r#*|}" >"$D/tok.x" || fail "no status.token: ${r:0:300}"
python3 - "$(cat "$D/tok.x")" <<'PY' || fail "the minted token is not x's, or not the mint's"
import base64, json, sys
h, p, _ = sys.argv[1].split(".")
d = lambda s: json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)))
hdr, c = d(h), d(p)
assert c["sub"] == "system:serviceaccount:default:x", c
assert hdr["kid"] != "sa-1", hdr     # not the provisioned issuer's key: the mint's
PY
r="$(until_status 60 200 "$D/tok.x" GET /api/v1/namespaces/default/services)" \
  || fail "the cell-minted token does not read services: ${r:0:300}"
echo "   TokenRequest -> 201; the minted token (the mint's key, at /authn-keys/node): GET services -> 200"

echo "== 6. renewal: x mints its own next token with the one it holds; not another's =="
# The mint's claims are second-granular (iat/exp) and its signature is
# deterministic, so a renewal within the second the held token was minted
# in is that same token. A renewal is asked for once that second has
# passed, and must answer a token that expires later.
exp_of() { python3 -c 'import sys,json,base64;p=open(sys.argv[1]).read().strip().split(".")[1];p+="="*(-len(p)%4);print(json.loads(base64.urlsafe_b64decode(p))["exp"])' "$1"; }
python3 -c 'import time;time.sleep(1.05-(time.time()%1)+1)'
r="$(api "$D/tok.x" POST /api/v1/namespaces/default/serviceaccounts/x/token "$TR")"
case "$r" in 201\|*) ;; *) fail "x renewing its own token: ${r:0:300}" ;; esac
token_of "${r#*|}" >"$D/tok.x2" || fail "no status.token on renewal: ${r:0:300}"
[ "$(cat "$D/tok.x2")" != "$(cat "$D/tok.x")" ] || fail "the renewal answered the same token"
[ "$(exp_of "$D/tok.x2")" -gt "$(exp_of "$D/tok.x")" ] || fail "the renewed token does not expire later"
r="$(api "$D/tok.x2" GET /api/v1/namespaces/default/services)"
case "$r" in 200\|*) ;; *) fail "the renewed token does not read services: ${r:0:300}" ;; esac
echo "   x -> serviceaccounts/x/token: 201; the renewed token: GET services -> 200"
r="$(api "$D/tok.x" POST /api/v1/namespaces/default/serviceaccounts/y/token "$TR")"
case "$r" in 403\|*) ;; *) fail "x minted y's token (want 403): ${r:0:300}" ;; esac
echo "   x -> serviceaccounts/y/token: 403 (resourceNames [x] grants x's own only)"

echo "== 7. a second configured issuer verifies; an unknown issuer and an unknown key do not =="
r="$(api "$D/tok.bx" GET /api/v1/namespaces/default/services)"
case "$r" in 200\|*) ;; *) fail "issuer B's token: ${r:0:300}" ;; esac
echo "   issuer B (b-1, /authn-keys/extra): GET services -> 200"
r="$(api "$D/tok.unknown-iss" GET /api/v1/namespaces/default/services)"
case "$r" in 401\|*) ;; *) fail "a token naming an unconfigured issuer (want 401): ${r:0:300}" ;; esac
echo "   a configured key, an unconfigured issuer: 401"
r="$(api "$D/tok.stranger" GET /api/v1/namespaces/default/services)"
case "$r" in 401\|*) ;; *) fail "a token under a key no path holds (want 401): ${r:0:300}" ;; esac
echo "   a key no path holds: 401"
r="$(api "$D/tok.cross" GET /api/v1/namespaces/default/services)"
case "$r" in 401\|*) ;; *) fail "a token claiming issuer A under issuer B's key (want 401): ${r:0:300}" ;; esac
echo "   issuer A's name under issuer B's key (its kid): 401"

echo "== 8. the key at a path is replaced while the plane runs: the set follows =="
xxd -r -p "$D/b2.frame" >"$D/b2.bin"
redis-cli -p "$(cat "$STORE/read.port")" -x SET /authn-keys/extra <"$D/b2.bin" >"$D/set.out" 2>&1 \
  || fail "could not write /authn-keys/extra: $(cat "$D/set.out")"
r="$(until_status 60 200 "$D/tok.b2x" GET /api/v1/namespaces/default/services)" \
  || fail "the new key's token was not admitted after the refresh: ${r:0:300}"
echo "   issuer B's new key (b-2): GET services -> 200"
r="$(api "$D/tok.bx" GET /api/v1/namespaces/default/services)"
case "$r" in 401\|*) ;; *) fail "the replaced key still verifies (want 401): ${r:0:300}" ;; esac
r="$(api "$D/tok.x2" GET /api/v1/namespaces/default/services)"
case "$r" in 200\|*) ;; *) fail "replacing one path's key dropped another's: ${r:0:300}" ;; esac
echo "   the replaced key (b-1): 401; the mint's token still 200 — one keyset, replaced whole"

echo "== E2E green: tokens verify under every configured key set, and a ServiceAccount renews its own =="
