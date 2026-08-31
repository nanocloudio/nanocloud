#!/usr/bin/env bash
# Live E2E for the WHOLE Kubernetes API request path served by a Chronicle graph.
#
# The claim under test: decode, authenticate, authorize, read, write and answer
# are all data. Not one of /api/v1's meanings — which verb a method is, what a
# refusal answers, when a create is a conflict — lives in an .fmod.
#
#   curl -> linux_net -> http (wave)     terminates HTTP, HANDLER_APP fan-out
#        -> kube_decode                  envelope -> Request record
#        -> admit   (decision)           apiplane.uproc, as params
#        -> kagi_verify <-> token_verify  KAGI: the signature check
#        -> authz   (decision)           verify_err -> verdict; method -> VERB
#        -> rbac_gate                    the binding -> role -> rule SET walk
#        -> probe   (decision)           -> GET the object / LIST the collection
#        -> store_effect
#        -> act     (decision)           what was found + the verb -> the write
#        -> store_effect
#        -> reply   (decision)           -> (cid, status, body)
#        -> enc_http (pipeline)          the read path's ser program, unchanged
#        -> http -> curl
#
# The only nanocloud modules are kube_decode, kagi_verify, rbac_gate and
# store_effect, and every one of them only moves bytes or walks a set — the two
# things Chronicle's VM cannot do by construction.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7444

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in kube_decode kagi_verify rbac_gate store_effect token_verify decision pipeline http; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apiplane-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <hex-value>
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
put_text() { store_put "$1" "$(printf '%s' "$2" | python3 -c 'import sys,binascii;print(binascii.hexlify(sys.stdin.buffer.read()).decode())')"; }

echo "== 1. seed identity, RBAC and one object =="
python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
ISSUER, KID = b"https://kubernetes.default.svc", b"sa-1"
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1)
        + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex","w").write(
    binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"
def mint(sa):
    now = int(time.time())
    claims = {"iss": ISSUER.decode(), "sub": f"system:serviceaccount:default:{sa}",
              "aud": "nanocloud", "iat": now, "exp": now + 3600}
    signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    return (signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode()
open(f"{D}/token.admin","w").write(mint("admin"))
open(f"{D}/token.reader","w").write(mint("reader"))
open(f"{D}/token.nobody","w").write(mint("nobody"))
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
put_text "/rolebindings/admins" "subjects=system:serviceaccount:default:admin;role=admin"
put_text "/rolebindings/readers" "subjects=system:serviceaccount:default:reader;role=viewer"
put_text "/roles/admin"  "rules=*:*"
put_text "/roles/viewer" "rules=get:pods,list:pods"
POD='{"metadata":{"name":"web-0","namespace":"default"},"spec":{"replicas":1}}'
put_text "/pods/default/web-0" "$POD"
# Admission policy for configmaps: one required field, one default, a quota of 2.
# Pods deliberately have NO policy, so the CRUD cases above prove the
# unpoliced path still passes an object through verbatim.
put_text "/admission-policy/configmaps" "required=spec.image;defaults=spec.priority=0;quota=2"
echo "   key, 2 bindings, 2 roles and /pods/default/web-0 seeded"

echo "== 2. use the SHIPPED graph, unmodified =="
# The graph under test is the one that ships, not a copy written here: a test
# that builds its own topology proves the topology it invented, and the file in
# packaging/ is then only as correct as the last person to read it. The baked
# params are checked separately against their .uproc by
# chronicle-param-drift-e2e.sh.
[ -f "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }
cp "$GRAPH" "$D/graph.yaml"
echo "   $GRAPH"

echo "== 3. build =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

drive() { # drive <token-file|-> <curl args...> -> "<code>|<body>"
  local tok="$1"; shift
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  for _ in $(seq 80); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
    sleep 0.1
  done
  local out
  if [ "$tok" = "-" ]; then
    out="$(curl -s -i --max-time 6 "$@" || true)"
  else
    out="$(curl -s -i --max-time 6 -H "Authorization: Bearer $(cat "$D/$tok")" "$@" || true)"
  fi
  kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
  printf '%s|%s' \
    "$(printf '%s' "$out" | head -1 | tr -d '\r' | awk '{print $2}')" \
    "$(printf '%s' "$out" | tr -d '\r' | awk 'BEGIN{b=0} /^$/{b=1;next} b{print}')"
}
check() { # check <label> <want-code> <want-body-or-*> <result>
  local code="${4%%|*}" body="${4#*|}"
  [ "$code" = "$2" ] || fail "$1: status '$code' want '$2'"
  if [ "$3" != "*" ]; then [ "$body" = "$3" ] || fail "$1: body '$body' want '$3'"; fi
  echo "   $1 -> $code${3:+ }$([ "$3" != "*" ] && printf '%s' "'$body'")"
}

U="http://127.0.0.1:$PORT/api/v1/namespaces/default/pods"

echo "== 4. READ: an authorized get returns the stored object =="
check "GET  /pods/default/web-0 as reader" 200 "$POD" "$(drive token.reader "$U/web-0")"

echo "== 5. READ: the collection lists its children =="
check "GET  /pods/default (list)" 200 \
  '{"kind":"List","apiVersion":"v1","items":[{"metadata":{"name":"web-0","namespace":"default"},"spec":{"replicas":1}}]}' \
  "$(drive token.reader "$U")"

echo "== 6. AUTHZ: a subject with no binding is denied, and never reaches the store =="
check "GET  /pods/default/web-0 as nobody" 403 '{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"forbidden"}'  "$(drive token.nobody "$U/web-0")"

echo "== 7. AUTHZ: the viewer's role has no 'delete' rule =="
check "DELETE /pods/default/web-0 as reader" 403 '{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"forbidden"}'  "$(drive token.reader -X DELETE "$U/web-0")"

echo "== 8. AUTHN: no credential is refused before authorization runs =="
check "GET  /pods/default/web-0 anonymous" 401 '{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Unauthorized","code":401,"message":"no usable credential"}'  "$(drive - "$U/web-0")"

echo "== 9. WRITE: create returns 201 and the object is really in the store =="
NEW='{"metadata":{"name":"web-1","namespace":"default"},"spec":{"replicas":2}}'
check "POST /pods/default (create web-1)" 201 "$NEW" \
  "$(drive token.admin -X POST --data-binary "$NEW" "$U")"
grep -q "web-1" "$D/store.log" || fail "create: /pods/default/web-1 is not in the store"
echo "   /pods/default/web-1 present in the store — the write happened, not just the answer"

echo "== 10. WRITE: creating it again is a 409, not a silent overwrite =="
check "POST /pods/default (create web-1 again)" 409 \
  '{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","code":409}' \
  "$(drive token.admin -X POST --data-binary "$NEW" "$U")"

echo "== 11. WRITE: update replaces it =="
UPD='{"metadata":{"name":"web-1","namespace":"default"},"spec":{"replicas":9}}'
check "PUT  /pods/default/web-1" 200 "$UPD" \
  "$(drive token.admin -X PUT --data-binary "$UPD" "$U/web-1")"
check "GET  /pods/default/web-1 after update" 200 "$UPD" "$(drive token.reader "$U/web-1")"

echo "== 12. WRITE: update of an absent object is a 404, not a create =="
check "PUT  /pods/default/ghost" 404 \
  '{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}' \
  "$(drive token.admin -X PUT --data-binary '{"x":1}' "$U/ghost")"

echo "== 13. WRITE: delete removes it =="
check "DELETE /pods/default/web-1" 200 \
  '{"kind":"Status","apiVersion":"v1","status":"Success"}' \
  "$(drive token.admin -X DELETE "$U/web-1")"
check "GET  /pods/default/web-1 after delete" 404 \
  '{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}' \
  "$(drive token.reader "$U/web-1")"

CM="http://127.0.0.1:$PORT/api/v1/namespaces/default/configmaps"

echo "== 13b. DISCOVERY: static documents, unauthenticated, as decision literals =="
check "GET  /version   (anonymous)" 200 '{"major":"1","minor":"29","gitVersion":"v1.29.0-nanocloud"}' "$(drive - "http://127.0.0.1:$PORT/version")"
check "GET  /api       (anonymous)" 200 '{"kind":"APIVersions","versions":["v1"]}' "$(drive - "http://127.0.0.1:$PORT/api")"
check "GET  /api/v1    (anonymous)" 200 '{"kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"pods"},{"name":"configmaps"},{"name":"services"}]}' "$(drive - "http://127.0.0.1:$PORT/api/v1")"
R="$(drive - "http://127.0.0.1:$PORT/apis")"
case "${R#*|}" in *'"kind":"APIGroupList"'*) ;; *) fail "/apis: ${R#*|}";; esac
echo "   GET  /apis      (anonymous) -> ${R%%|*} APIGroupList"

echo "== 13b2. the remaining static documents the module apiserver served =="
check "GET  /openapi/v2 (anonymous)" 200 '{"swagger":"2.0","info":{"title":"nanocloud","version":"v1"},"paths":{}}' "$(drive - "http://127.0.0.1:$PORT/openapi/v2")"
check "GET  /healthz   (anonymous)" 200 "ok" "$(drive - "http://127.0.0.1:$PORT/healthz")"
R="$(drive - "http://127.0.0.1:$PORT/metrics")"
case "${R#*|}" in *nanocloud_apiserver_up*) ;; *) fail "/metrics: ${R#*|}";; esac
echo "   GET  /metrics   (anonymous) -> ${R%%|*} nanocloud_apiserver_up"

echo "== 13c. a path that is not a resource path at all is a 404 =="
# `/api/nope` is too short to be a Kubernetes resource path, so it arrives only
# because of `passthrough` — with every resource field empty. Answering 404 is
# what makes passthrough safe: the empty fields are never read as a resource.
# (`/api/v1/nope` is a DIFFERENT case: that parses as the collection `nope`, and
# an anonymous request for it is a 401 before anyone asks whether it exists.)
check "GET  /api/nope  (anonymous)" 404 "not found" "$(drive - "http://127.0.0.1:$PORT/api/nope")"

echo "== 14. ADMISSION: a missing required field is refused before the store =="
check "POST /configmaps (no spec.image)" 403 "missing spec.image" \
  "$(drive token.admin -X POST --data-binary '{"metadata":{"name":"c1"},"spec":{}}' "$CM")"
grep -q "configmaps/default/c1" "$D/store.log" && fail "refused create still wrote the object"
echo "   nothing was written — admission refused ahead of the store, not after it"

echo "== 15. ADMISSION: a default is applied, and it is what gets STORED =="
R="$(drive token.admin -X POST --data-binary '{"metadata":{"name":"c1"},"spec":{"image":"nginx"}}' "$CM")"
[ "${R%%|*}" = "201" ] || fail "policed create: status '${R%%|*}' want 201"
case "${R#*|}" in *'"priority":0'*) ;; *) fail "the answer does not carry the applied default: ${R#*|}";; esac
grep -q '"priority":0' "$D/store.log" || fail "the default was answered but not stored"
echo "   201, spec.priority=0 applied by admission and present in the store"

echo "== 16. ADMISSION: quota counts LIVE objects and refuses the one past it =="
R="$(drive token.admin -X POST --data-binary '{"metadata":{"name":"c2"},"spec":{"image":"nginx"}}' "$CM")"
[ "${R%%|*}" = "201" ] || fail "second create: status '${R%%|*}' want 201"
check "POST /configmaps (third, quota=2)" 403 "quota exceeded" \
  "$(drive token.admin -X POST --data-binary '{"metadata":{"name":"c3"},"spec":{"image":"nginx"}}' "$CM")"

echo "== E2E green: the whole Kubernetes API request path is Chronicle params — authn, authz, admission, CRUD =="
