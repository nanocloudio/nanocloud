#!/usr/bin/env bash
# Live E2E: managed-cell enrollment and authority-epoch admission (spec §9) —
# a management plane enrolls with a cell through the cell's API, and a stale
# management writer is stopped by the cell.
#
# The control plane as packaged. The cell's administrator records ONE
# management plane — a ManagementBinding: the cell UID and bootstrap-envelope
# digest the plane must prove, its one-time grant, the authority epoch the
# envelope carries (3), its public identity, its rules (ConfigMaps) — and the
# cell derives the rest (management.plan). Then, as the management plane:
#   1. the offer is made: wrong grant, wrong cell UID, wrong digest -> 403,
#      the offer untouched; the right three -> 201, a TokenRequest for
#      nanocloud-management/parent;
#   2. the grant is spent: the same three again -> 403, and so is another
#      cell UID; the offer stays, as its claim (spent, never made again);
#   3. the credential works under its rules: it reads its own binding, and a
#      ConfigMap write naming epoch 3 is created; one outside its rules (a
#      Secret) is 403;
#   4. epoch admission: a write with no epoch header -> 403, a malformed one
#      -> 400, epoch 2 (behind the committed 3) -> 409; epoch 5 (ahead) is
#      admitted and commits nothing (epoch 4 still writes); reads need no
#      header; the administrator (no binding) writes without one;
#   5. the epoch path commits epoch 6 and answers a fresh credential; the
#      writer still at epoch 4 or 5 -> 409, at 6 -> admitted; an advance to
#      2 -> 409, without a header -> 403, 6 again -> 201 (idempotent);
#   6. the binding cannot advance another binding's epoch (403);
#   7. a grant changed before it is spent supersedes the earlier: the earlier
#      offer is withdrawn (claimed) — its grant 403 — the new one enrolls, and
#      the earlier grant stated again is not offered again;
#   8. the binding deleted, its ClusterRoleBinding, ClusterRole and
#      ServiceAccount are collected and the credential's write is 403; its
#      committed epochs and spent offer stay, so the binding made again
#      under the same name fences the old credential and offers nothing for
#      the spent grant;
#   9. a binding the plane stopped at (killed) before its offer was made is
#      offered on the next boot: the offer is one write.
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

D="$(mktemp -d /tmp/nc-mgmt-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
has() { # has <key>: whether the store holds it (lattice's redis edge, one EXISTS under a deadline)
  local n
  for _ in 1 2 3; do
    n="$(timeout 10 redis-cli -p "$(cat "$STORE/read.port")" EXISTS "$1" 2>/dev/null)" && { [ "$n" = 1 ]; return; }
  done
  return 2
}
hasnt() { has "$1"; [ $? -eq 1 ]; } # the store answered, and does not hold it
spent() { # spent <key>: the store holds it as a take's claim (`\0nanocloud.take:<nonce>`)
  local v
  for _ in 1 2 3; do
    v="$(timeout 10 redis-cli --no-raw -p "$(cat "$STORE/read.port")" GET "$1" 2>/dev/null)" && {
      case "$v" in '"\x00nanocloud.take:'*) return 0 ;; *) return 1 ;; esac; }
  done
  return 2
}
fail() {
  echo "FAIL: $1"
  tail -30 "$D/run.log" 2>/dev/null || true
  exit 1
}

store_put() { # store_put <key> <text>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
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

ISS="https://kubernetes.default.svc"
CELL="5f0c2a8e-3d41-4b7a-9c1e-8a2b6d4f0e17"
DIGEST="sha256:9b7e1c0d4a3f5e2b8c6d1a0f9e8d7c6b5a4f3e2d1c0b9a8f7e6d5c4b3a2f1e0d"
GRANT="g7Qm2xV9pL4sT8wZ1nR6cY3hK5bJ0dFa"

echo "== 1. the administrator's key; the shipped control plane =="
python3 - "$D" "$ISS" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D, ISS = sys.argv[1:3]
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
body = (f8(ISS.encode()) + struct.pack("<H", 1) + f8(b"sa-1") + struct.pack("<H", 1)
        + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1) + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/a.frame", "w").write(binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
now = int(time.time())
hdr = b64u(json.dumps({"alg": "ES256", "typ": "JWT", "kid": "sa-1"}, separators=(",", ":")).encode())
claims = {"iss": ISS, "sub": "system:serviceaccount:default:admin", "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = hdr + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/tok.admin", "w").write((signing + b"." + b64u(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode())
PY
python3 - "$STORE/store.log" "$(cat "$D/a.frame")" <<'PY'
import struct, sys, os, binascii
path, val = sys.argv[1], binascii.unhexlify(sys.argv[2])
key = b"/authn-keys/sa"
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
PY
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
store_put "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
for n in fence fenced mb_src mb_ask mb_offer mb_wd mb_gc_collect mb_gcr_collect mb_gcs_collect; do
  grep -q "^  - name: $n$" "$D/graph.yaml" || fail "the shipped plane has no $n"
done
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"

echo "== 2. run it; wait for the API =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"

api() { # api <token-file|-> <epoch|-> <method> <path> [json-body] -> "<status>|<body>"
  python3 - "$PORT" "$@" <<'PY'
import socket, ssl, sys
port, tokf, epoch, method, path = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
body = sys.argv[6] if len(sys.argv) > 6 else ""
tok = sys.argv[2][1:] if tokf.startswith("=") else open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
try:
    raw = socket.create_connection(("127.0.0.1", port), timeout=10)
    s = ctx.wrap_socket(raw, server_hostname="localhost")
except OSError as e:
    print(f"?|{e}"); sys.exit(0)
b = body.encode()
eh = "" if epoch == "-" else f"Nanocloud-Authority-Epoch: {epoch}\r\n"
req = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n{eh}"
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
until_status() { # until_status <seconds> <want-glob> <api args…>
  local secs="$1" want="$2" r=""; shift 2
  for _ in $(seq "$secs"); do
    r="$(api "$@")"
    # shellcheck disable=SC2254
    case "${r%%|*}" in $want) printf '%s' "$r"; return 0 ;; esac
    sleep 1
  done
  printf '%s' "$r"; return 1
}
want() { # want <status> <what> <answer>
  case "$3" in "$1|"*) ;; *) fail "$2 (want $1): ${3:0:400}" ;; esac
}
token_of() { python3 -c 'import json,sys;print(json.loads(sys.argv[1])["status"]["token"])' "$1"; }
until_has() { # until_has <seconds> <key>
  for _ in $(seq "$1"); do
    has "$2" && return 0
    sleep 1
  done
  return 1
}

r="$(until_status 90 200 "$D/tok.admin" - GET /api/v1/namespaces/default/configmaps)" \
  || fail "the administrator's token was not admitted: ${r:0:300}"
echo "   the administrator: GET configmaps -> 200"

echo "== 3. the administrator records the management plane: a ManagementBinding =="
MB="$(python3 - "$CELL" "$DIGEST" "$GRANT" <<'PY'
import json, sys
cell, digest, grant = sys.argv[1:]
print(json.dumps({"apiVersion": "nanocloud.io/v1", "kind": "ManagementBinding",
  "metadata": {"name": "parent"},
  "spec": {"cellUID": cell, "envelopeDigest": digest, "grant": grant, "authorityEpoch": 3,
           "authority": {"name": "fabric-a", "issuer": "https://fabric-a.example"},
           "rules": [{"apiGroups": [""], "resources": ["configmaps"],
                      "verbs": ["get", "list", "create", "update", "patch", "delete"]}]}},
  separators=(",", ":")))
PY
)"
r="$(api "$D/tok.admin" - POST /apis/nanocloud.io/v1/managementbindings "$MB")"
want 201 "the ManagementBinding" "$r"
r="$(api "$D/tok.admin" - GET /apis/nanocloud.io/v1/managementbindings/parent)"
want 200 "GET the ManagementBinding" "$r"
r="$(api "$D/tok.admin" - GET /apis/nanocloud.io/v1)"
case "$r" in *'"managementbindings"'*'"managementbindings/enroll"'*'"managementbindings/epoch"'*) ;;
  *) fail "discovery does not serve managementbindings and its two subresources: ${r:0:300}" ;; esac
until_has 60 "/enrollment-offers/parent/$GRANT/$CELL/$DIGEST" || fail "the cell made no enrollment offer"
until_has 30 "/authority-epochs/system:serviceaccount:nanocloud-management:parent/3" || fail "spec.authorityEpoch 3 was not committed"
r="$(until_status 30 200 "$D/tok.admin" - GET /apis/rbac.authorization.k8s.io/v1/clusterrolebindings/nanocloud-management:parent)" || fail "no ClusterRoleBinding derived: ${r:0:300}"
r="$(until_status 30 200 "$D/tok.admin" - GET /api/v1/namespaces/nanocloud-management/serviceaccounts/parent)" || fail "no ServiceAccount derived: ${r:0:300}"
r="$(api "$D/tok.admin" - GET /apis/rbac.authorization.k8s.io/v1/clusterroles/nanocloud-management:parent)"
want 200 "the derived ClusterRole" "$r"
python3 - "${r#*|}" <<'PY2' || fail "the ClusterRole is not the binding's rules and its own: ${r:0:600}"
import json, sys
cr = json.loads(sys.argv[1])
rules = cr["rules"]
assert {"apiGroups": ["nanocloud.io"], "resources": ["managementbindings/epoch"], "resourceNames": ["parent"], "verbs": ["create"]} in rules, rules
assert any(r.get("resources") == ["configmaps"] for r in rules), rules
assert cr["metadata"]["ownerReferences"][0]["kind"] == "ManagementBinding", cr
PY2
echo "   ManagementBinding parent -> 201; offer, committed epoch 3, ServiceAccount, ClusterRole(Binding) derived"

echo "== 4. enrollment: the grant, the cell UID and the digest must all be the offer's =="
EN() { printf '{"apiVersion":"nanocloud.io/v1","kind":"Enrollment","spec":{"cellUID":"%s","envelopeDigest":"%s"}}' "$1" "$2"; }
ENROLL=/apis/nanocloud.io/v1/managementbindings/parent/enroll
r="$(api "=not-the-grant" - POST "$ENROLL" "$(EN "$CELL" "$DIGEST")")"
want 403 "an enrollment under a wrong grant" "$r"
r="$(api "=$GRANT" - POST "$ENROLL" "$(EN "00000000-0000-0000-0000-000000000000" "$DIGEST")")"
want 403 "an enrollment proving another cell UID" "$r"
r="$(api "=$GRANT" - POST "$ENROLL" "$(EN "$CELL" "sha256:0000")")"
want 403 "an enrollment proving another envelope digest" "$r"
r="$(api "=$GRANT" - POST "$ENROLL" "")"
want 403 "an enrollment proving nothing" "$r"
r="$(api "=$GRANT" - POST /apis/nanocloud.io/v1/managementbindings/other/enroll "$(EN "$CELL" "$DIGEST")")"
want 403 "the grant presented for another binding" "$r"
has "/enrollment-offers/parent/$GRANT/$CELL/$DIGEST" || fail "a refused enrollment spent the offer"
echo "   wrong grant / cell UID / digest / no proof / other binding: 403, the offer unspent"
r="$(api "=$GRANT" - POST "$ENROLL" "$(EN "$CELL" "$DIGEST")")"
want 201 "the enrollment" "$r"
token_of "${r#*|}" >"$D/tok.mgmt" || fail "no status.token: ${r:0:300}"
python3 - "$(cat "$D/tok.mgmt")" "${r#*|}" <<'PY' || fail "the credential is not the binding's"
import base64, json, sys
h, p, _ = sys.argv[1].split(".")
c = json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
assert c["sub"] == "system:serviceaccount:nanocloud-management:parent", c
a = json.loads(sys.argv[2])
assert a["kind"] == "TokenRequest" and a["metadata"] == {"name": "parent", "namespace": "nanocloud-management"}, a
PY
spent "/enrollment-offers/parent/$GRANT/$CELL/$DIGEST" || fail "the enrollment did not leave the offer spent"
echo "   the right grant, cell UID and digest: 201, a TokenRequest for nanocloud-management/parent; the offer spent (kept as its claim)"
r="$(api "=$GRANT" - POST "$ENROLL" "$(EN "$CELL" "$DIGEST")")"
want 403 "the spent grant presented again" "$r"
r="$(api "=$GRANT" - POST "$ENROLL" "$(EN "11111111-2222-3333-4444-555555555555" "$DIGEST")")"
want 403 "a re-enrollment for another cell UID" "$r"
echo "   the spent grant again: 403; a re-enrollment for another cell UID: 403"

echo "== 5. the credential, under its rules =="
r="$(until_status 60 200 "$D/tok.mgmt" - GET /apis/nanocloud.io/v1/managementbindings/parent)" \
  || fail "the credential does not read its own binding: ${r:0:300}"
r="$(api "$D/tok.mgmt" - GET /apis/nanocloud.io/v1/managementbindings/other)"
want 403 "the credential reading another binding" "$r"
CM() { printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s","namespace":"default"},"data":{"k":"v"}}' "$1"; }
r="$(until_status 60 201 "$D/tok.mgmt" 3 POST /api/v1/namespaces/default/configmaps "$(CM at-3)")" \
  || fail "a ConfigMap write at the committed epoch: ${r:0:300}"
r="$(api "$D/tok.mgmt" 3 POST /api/v1/namespaces/default/secrets '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"s","namespace":"default"}}')"
want 403 "a write outside the binding's rules" "$r"
r="$(api "$D/tok.mgmt" - GET /api/v1/namespaces/default/configmaps/at-3)"
want 200 "a read with no epoch header" "$r"
echo "   own binding: 200, another's: 403; ConfigMap at epoch 3: 201; a Secret: 403; a read, no header: 200"

echo "== 6. epoch admission =="
r="$(api "$D/tok.mgmt" - POST /api/v1/namespaces/default/configmaps "$(CM no-epoch)")"
want 403 "a managed write with no epoch" "$r"
case "$r" in *Nanocloud-Authority-Epoch*) ;; *) fail "the refusal does not name the header: ${r:0:300}" ;; esac
r="$(api "$D/tok.mgmt" 3x POST /api/v1/namespaces/default/configmaps "$(CM bad-epoch)")"
want 400 "a managed write with a malformed epoch" "$r"
r="$(api "$D/tok.mgmt" 2 POST /api/v1/namespaces/default/configmaps "$(CM at-2)")"
want 409 "a stale (epoch 2) write" "$r"
case "$r" in *'"reason":"Conflict"'*'stale authority epoch 2: this cell has committed epoch 3'*) ;; *) fail "the 409 is not the fence's: ${r:0:300}" ;; esac
r="$(api "$D/tok.mgmt" 2 DELETE /api/v1/namespaces/default/configmaps/at-3)"
want 409 "a stale (epoch 2) delete" "$r"
r="$(api "$D/tok.mgmt" 2 PATCH /api/v1/namespaces/default/configmaps/at-3 '{"data":{"k":"stale"}}')"
want 409 "a stale (epoch 2) patch" "$r"
r="$(api "$D/tok.mgmt" 5 POST /api/v1/namespaces/default/configmaps "$(CM at-5)")"
want 201 "a write ahead of the committed epoch" "$r"
r="$(api "$D/tok.mgmt" 4 POST /api/v1/namespaces/default/configmaps "$(CM at-4)")"
want 201 "a write at epoch 4 after one at 5 (a header commits nothing)" "$r"
r="$(api "$D/tok.admin" - POST /api/v1/namespaces/default/configmaps "$(CM admin)")"
want 201 "the administrator's write, no binding, no header" "$r"
r="$(api "$D/tok.mgmt" 3 GET /api/v1/namespaces/default/configmaps/at-3)"
case "$r" in *'"k":"v"'*) ;; *) fail "a refused patch changed the object: ${r:0:300}" ;; esac
echo "   no header: 403; '3x': 400; epoch 2 (create, delete, patch): 409; 5 then 4: 201, 201; the administrator: 201"

echo "== 7. the epoch path: commit 6, renew; the stale writer stopped =="
EP=/apis/nanocloud.io/v1/managementbindings/parent/epoch
r="$(api "$D/tok.mgmt" 6 POST "$EP" '{}')"
want 201 "the epoch path (6)" "$r"
token_of "${r#*|}" >"$D/tok.mgmt6" || fail "no renewed token: ${r:0:300}"
has "/authority-epochs/system:serviceaccount:nanocloud-management:parent/6" || fail "epoch 6 not committed"
r="$(api "$D/tok.mgmt" 5 POST /api/v1/namespaces/default/configmaps "$(CM stale-5)")"
want 409 "the writer at 5 after 6 committed" "$r"
r="$(api "$D/tok.mgmt" 4 PUT /api/v1/namespaces/default/configmaps/at-4 "$(CM at-4)")"
want 409 "the writer at 4 after 6 committed (update)" "$r"
r="$(api "$D/tok.mgmt6" 6 POST /api/v1/namespaces/default/configmaps "$(CM at-6)")"
want 201 "the renewed credential at 6" "$r"
r="$(api "$D/tok.mgmt" 6 POST /api/v1/namespaces/default/configmaps "$(CM at-6b)")"
want 201 "the first credential at 6" "$r"
r="$(api "$D/tok.mgmt6" 2 POST "$EP" '{}')"
want 409 "an advance behind the committed epoch" "$r"
r="$(api "$D/tok.mgmt6" - POST "$EP" '{}')"
want 403 "an advance with no epoch" "$r"
r="$(api "$D/tok.mgmt6" 6 POST "$EP" '{}')"
want 201 "the committed epoch again (idempotent)" "$r"
echo "   epoch 6 committed + renewed: 201; at 5, 4: 409; at 6 (either token): 201; advance to 2: 409, none: 403, 6 again: 201"

echo "== 8. one binding cannot move another's epoch =="
MB2="$(printf '%s' "$MB" | sed -e 's/"name":"parent"/"name":"other"/' -e 's/"grant":"[^"]*"/"grant":"other-grant-0123456789abcdef"/')"
r="$(api "$D/tok.admin" - POST /apis/nanocloud.io/v1/managementbindings "$MB2")"
want 201 "a second ManagementBinding" "$r"
until_has 60 "/authority-epochs/system:serviceaccount:nanocloud-management:other/3" || fail "other's epoch not committed"
r="$(api "$D/tok.mgmt6" 9 POST /apis/nanocloud.io/v1/managementbindings/other/epoch '{}')"
want 403 "parent advancing other's epoch" "$r"
hasnt "/authority-epochs/system:serviceaccount:nanocloud-management:other/9" || fail "parent committed an epoch for other"
echo "   parent -> other/epoch: 403, nothing committed"

echo "== 9. a grant changed before it is spent supersedes the earlier one =="
OG="other-grant-0123456789abcdef"
NG="other-grant-fedcba9876543210"
until_has 60 "/enrollment-offers/other/$OG/$CELL/$DIGEST" || fail "other's first offer was not made"
r="$(api "$D/tok.admin" - GET /apis/nanocloud.io/v1/managementbindings/other)"
want 200 "GET other" "$r"
MB2b="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys;d=json.load(sys.stdin);d["spec"]["grant"]=sys.argv[1];print(json.dumps(d,separators=(",",":")))' "$NG")"
r="$(api "$D/tok.admin" - PUT /apis/nanocloud.io/v1/managementbindings/other "$MB2b")"
want 200 "other's grant replaced" "$r"
until_has 60 "/enrollment-offers/other/$NG/$CELL/$DIGEST" || fail "the new grant was not offered"
ok=""; for _ in $(seq 60); do spent "/enrollment-offers/other/$OG/$CELL/$DIGEST" && { ok=1; break; }; sleep 1; done
[ -n "$ok" ] || fail "the superseded offer was not withdrawn"
r="$(api "=$OG" - POST /apis/nanocloud.io/v1/managementbindings/other/enroll "$(EN "$CELL" "$DIGEST")")"
want 403 "the superseded grant" "$r"
r="$(api "=$NG" - POST /apis/nanocloud.io/v1/managementbindings/other/enroll "$(EN "$CELL" "$DIGEST")")"
want 201 "the new grant" "$r"
# The earlier grant stated again: its offer was made once, and is not again.
r="$(api "$D/tok.admin" - GET /apis/nanocloud.io/v1/managementbindings/other)"
MB2c="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys;d=json.load(sys.stdin);d["spec"]["grant"]=sys.argv[1];print(json.dumps(d,separators=(",",":")))' "$OG")"
r="$(api "$D/tok.admin" - PUT /apis/nanocloud.io/v1/managementbindings/other "$MB2c")"
want 200 "other's grant set back" "$r"
sleep 3
spent "/enrollment-offers/other/$OG/$CELL/$DIGEST" || fail "the superseded offer was made again"
r="$(api "=$OG" - POST /apis/nanocloud.io/v1/managementbindings/other/enroll "$(EN "$CELL" "$DIGEST")")"
want 403 "the superseded grant stated again" "$r"
echo "   grant replaced: the earlier offer claimed (403), the new one offered (201); the earlier stated again: still 403"

echo "== 10. the binding deleted: its grants are revoked, what it made collected =="
r="$(api "$D/tok.admin" - DELETE /apis/nanocloud.io/v1/managementbindings/parent)"
want 200 "deleting the ManagementBinding" "$r"
r="$(until_status 60 404 "$D/tok.admin" - GET /apis/rbac.authorization.k8s.io/v1/clusterrolebindings/nanocloud-management:parent)" \
  || fail "the deleted binding's ClusterRoleBinding was not collected: ${r:0:300}"
r="$(until_status 60 404 "$D/tok.admin" - GET /apis/rbac.authorization.k8s.io/v1/clusterroles/nanocloud-management:parent)" \
  || fail "the deleted binding's ClusterRole was not collected: ${r:0:300}"
r="$(until_status 60 404 "$D/tok.admin" - GET /api/v1/namespaces/nanocloud-management/serviceaccounts/parent)" \
  || fail "the deleted binding's ServiceAccount was not collected: ${r:0:300}"
r="$(until_status 60 403 "$D/tok.mgmt6" 6 POST /api/v1/namespaces/default/configmaps "$(CM after-delete)")" \
  || fail "the deleted binding's credential still writes: ${r:0:300}"
has "/authority-epochs/system:serviceaccount:nanocloud-management:parent/6" || fail "the committed epoch 6 was not kept"
spent "/enrollment-offers/parent/$GRANT/$CELL/$DIGEST" || fail "the spent offer was not kept"
r="$(api "$D/tok.admin" - GET /apis/rbac.authorization.k8s.io/v1/clusterroles/nanocloud-management:other)"
want 200 "another binding's ClusterRole (untouched)" "$r"
echo "   ClusterRoleBinding, ClusterRole, ServiceAccount collected; the credential's write: 403; epochs and the spent offer kept"
# Made again under the same name (a new uid), at epoch 7: the old credential
# names the same subject, and the kept epochs fence it; the spent grant is
# not offered again.
MB4="$(printf '%s' "$MB" | sed -e 's/"authorityEpoch":3/"authorityEpoch":7/')"
r="$(api "$D/tok.admin" - POST /apis/nanocloud.io/v1/managementbindings "$MB4")"
want 201 "the binding made again" "$r"
until_has 60 "/authority-epochs/system:serviceaccount:nanocloud-management:parent/7" || fail "epoch 7 not committed"
r="$(until_status 60 200 "$D/tok.admin" - GET /apis/rbac.authorization.k8s.io/v1/clusterrolebindings/nanocloud-management:parent)" || fail "no ClusterRoleBinding re-derived: ${r:0:300}"
r="$(until_status 60 409 "$D/tok.mgmt6" 6 POST /api/v1/namespaces/default/configmaps "$(CM old-cred)")" \
  || fail "the predecessor's credential at epoch 6 is not fenced: ${r:0:300}"
r="$(api "=$GRANT" - POST "$ENROLL" "$(EN "$CELL" "$DIGEST")")"
want 403 "the spent grant, for the binding made again" "$r"
echo "   made again at epoch 7: the old credential at 6 -> 409; the spent grant -> 403"

echo "== 11. stopped before its offer: the next boot makes it =="
MB3="$(printf '%s' "$MB" | sed -e 's/"name":"parent"/"name":"third"/' -e 's/"grant":"[^"]*"/"grant":"third-grant-0123456789abcdef"/')"
r="$(api "$D/tok.admin" - POST /apis/nanocloud.io/v1/managementbindings "$MB3")"
want 201 "a third ManagementBinding" "$r"
kill -9 "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered after the restart"
until_has 90 "/enrollment-offers/third/third-grant-0123456789abcdef/$CELL/$DIGEST" || fail "the stopped binding was never offered"
r="$(until_status 60 201 "=third-grant-0123456789abcdef" - POST /apis/nanocloud.io/v1/managementbindings/third/enroll "$(EN "$CELL" "$DIGEST")")" \
  || fail "the offer made after the restart does not enroll: ${r:0:300}"
echo "   killed right after the binding was written; after the restart: offered, and it enrolls"

echo "== E2E green: enrollment proves the cell UID and envelope digest, and the cell stops a stale management writer =="
