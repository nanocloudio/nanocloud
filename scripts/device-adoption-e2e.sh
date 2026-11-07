#!/usr/bin/env bash
# Live E2E: a customer's Device adopted with a claim token, on the control
# plane as packaged (device_pki behind admit, kagi's authority behind it).
#
#   1. the operator creates a Device with a `spec.class`; the plane mints its
#      claim token (`<id>.<secret>`) and the Device reads Claimable, with its
#      SPIFFE identity, its broker and its topic prefix
#      (`t/<16 hex of the namespace uid's SHA-256>/devices/<name>/`);
#   2. the firmware subresource says what is missing, 501;
#   3. the device makes a P-256 key and a CSR with openssl and sends ONE
#      request — `POST certificatesigningrequests`, the token as Bearer — and
#      the 201 carries its certificate: it certifies that key, names the
#      Device's SPIFFE identity, and chains to the plane's authority;
#   4. the same request again is 409 AlreadyExists, and a GET under the same
#      token reads the CSR back; another CSR name under it is 403;
#   5. another key under the spent token is 403, a wrong secret 401;
#   6. the Device reads Enrolled, with the key's fingerprint and the
#      certificate's notAfter, and the token is gone from the plane;
#   7. a certificate is asked for the Device's `spec.certificate.lifetime`
#      (24h by default), clamped by the cell, and the status reports what the
#      certificate it got lasts;
#   8. the enrolled key re-enrolls with no token and no certificate (201) —
#      also when its certificate expired long ago, so a device is never
#      stranded; a key never enrolled is 403;
#   9. a Device recreated under its name is minted a new token, the first
#      one withdrawn (401), and the old Device's key re-enrolls to nothing;
#  10. a revoked Device (`spec.revoked`) re-enrolls nothing;
#  11. with `spec.certificate.maxOffline`, a key re-enrolls within it of its
#      certificate's expiry and is refused past it (the claim's recorded
#      notAfter set in the past through the store's RESP edge, as
#      token-keysets-e2e writes its store);
#  12. stock kubectl redeems a claim (`/apis` lists certificates.k8s.io, so
#      `create -f` maps the kind); a device renews under its own certificate, presented to the API's tls
#      (`verify_peer: 2`) — and once that certificate has REALLY expired, the
#      handshake refuses it and the device re-enrolls by its key alone.
#
# For 12 the cell's and the authority's floors are lowered in this graph
# (device_pki and certificate_endpoint `min_lifetime_s: 60`) so a 90 s
# certificate can expire while the test waits; the shipped floor is an hour.
#
# The token is read from the plane's store here, where the firmware
# subresource would carry it (not yet built: docs/design/devices.md).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
command -v redis-cli >/dev/null || { echo "FAIL: redis-cli not found"; exit 1; }

D="$(mktemp -d /tmp/nc-adopt-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
stop() { if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; fi; }
cleanup() { stop; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; grep -a "device_pki" "$D/run.log" | tail -5 || true; tail -15 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 0. the control plane, as packaged, joined by its operator =="
NANOCLOUD_PEER_TRUST=authority nc_controlplane_graph "$STORE" "$D/graph.yaml" \
  || fail "the control plane did not render"
python3 - "$D/graph.yaml" <<'PY' || fail "could not lower the lifetime floors"
import sys
p = sys.argv[1]; g = open(p).read()
a = "    min_lifetime_s: 3600\n"
if g.count(a) != 1: sys.exit("device_pki's floor is not where it was")
g = g.replace(a, "    min_lifetime_s: 60\n")
a = '    key_label: "nanocloud.ca"\n'
if g.count("  - name: certificate_endpoint\n") != 1: sys.exit("no single certificate_endpoint")
i = g.index("  - name: certificate_endpoint\n"); j = g.index(a, i) + len(a)
g = g[:j] + "    min_lifetime_s: 60\n" + g[j:]
open(p, "w").write(g)
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"
SEAL="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
mkdir -p "$D/vault"
env -C "$STORE" FLUXOR_SEAL_KEY="$SEAL" FLUXOR_VAULT_DIR="$D/vault" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
JOIN=""
for _ in $(seq 240); do
  JOIN="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/run.log" | tail -1 || true)"
  [ -n "$JOIN" ] && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the runtime exited"
  sleep 0.25
done
[ -n "$JOIN" ] || fail "no join secret announced in 60 s"
SECRET="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
CA_SHA="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
KC="$D/kubeconfig"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$KC" >/dev/null || fail "the join did not complete"
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get --raw /version >/dev/null || fail "kubectl could not reach the API"
kubectl --kubeconfig "$KC" config view --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' \
  | base64 -d >"$D/ca.pem"
nc_store_ready "$STORE" || fail "the store's reader never answered"
get() { nc_store_get "$STORE" "$1"; }
echo "   joined"

# api <token> <method> <path> [body-file] -> "<status> <body>" in $D/api.out;
# with CERT=<pem> KEY=<pem> set, the request presents that client certificate
api() {
  local tok="$1" method="$2" path="$3" body="${4:-}"
  local args=(-sk -o "$D/api.body" -w '%{http_code}' -X "$method"
    -H 'Content-Type: application/json' "https://127.0.0.1:$PORT$path")
  [ -n "$tok" ] && args+=(-H "Authorization: Bearer $tok")
  [ -n "${CERT:-}" ] && args+=(--cert "$CERT" --key "$KEY")
  [ -n "$body" ] && args+=(--data-binary "@$body")
  curl "${args[@]}" >"$D/api.code" || true
  cat "$D/api.code"
}
CSRS=/apis/certificates.k8s.io/v1/certificatesigningrequests

# fp <keyfile>: the SHA-256 of the key's 65-byte point, hex
fp() { openssl ec -in "$D/$1" -pubout -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1; }
# renewal <name> <keyfile>: a re-enrollment, named by the key it proves
renewal() { csr "$1" "$2" "$(fp "$2" | cut -c1-16)"; }
# leaf_of <answer-file> <out.pem>
leaf_of() { python3 -c 'import base64,json,sys;print(base64.b64decode(json.load(open(sys.argv[1]))["status"]["certificate"]).decode(),end="")' "$1" >"$2"; }
# span <pem>: notAfter - notBefore, seconds
span() {
  echo $(( $(date -u -d "$(openssl x509 -in "$1" -noout -enddate | cut -d= -f2)" +%s) \
         - $(date -u -d "$(openssl x509 -in "$1" -noout -startdate | cut -d= -f2)" +%s) ))
}
# status <ns> <name> <python expr over st> — waits up to 10 s for it to hold
status_holds() {
  for _ in $(seq 20); do
    k -n "$1" get device "$2" -o json >"$D/st.json" 2>/dev/null
    python3 -c "import json,sys; st=json.load(open(sys.argv[1])).get('status',{}); sys.exit(0 if ($3) else 1)" "$D/st.json" && return 0
    sleep 0.5
  done
  return 1
}

# expire_claim <ns> <name> <seconds-ago>: the claim's recorded notAfter set
# that long in the past, through the store's RESP edge — standing in for a
# certificate that lasted until then (the pinned authority issues 30 days).
expire_claim() {
  get "/device-claim-of/$1/$2" | python3 -c '
import json, sys, time
c = json.load(sys.stdin)
c["notAfter"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - int(sys.argv[1])))
sys.stdout.write(json.dumps(c, separators=(",", ":")))' "$3" >"$D/claim.json"
  redis-cli -p "$(cat "$STORE/read.port")" -x SET "/device-claim-of/$1/$2" <"$D/claim.json" >"$D/set.out" 2>&1 \
    || fail "could not set the claim's notAfter: $(cat "$D/set.out")"
  grep -q OK "$D/set.out" || fail "the store refused the claim's notAfter: $(cat "$D/set.out")"
}

# csr <name> <keyfile> <token-id> -> $D/<name>.json, a CSR object as a device sends it
csr() {
  openssl req -new -key "$D/$2" -subj "/CN=ignored" -out "$D/$1.csr" 2>/dev/null
  python3 - "$D/$1.csr" "$3" >"$D/$1.json" <<'PY'
import base64, json, sys
req = base64.b64encode(open(sys.argv[1], "rb").read()).decode()
print(json.dumps({"apiVersion": "certificates.k8s.io/v1", "kind": "CertificateSigningRequest",
                  "metadata": {"name": "device-" + sys.argv[2]},
                  "spec": {"signerName": "nanocloud.io/device", "request": req,
                           "usages": ["digital signature", "client auth"]}}))
PY
}

echo "== 1. a Device with a class is minted a claim token =="
k create namespace workshop >/dev/null || fail "create namespace"
cat >"$D/device.json" <<'J'
{"apiVersion":"nanocloud.io/v1","kind":"Device","metadata":{"name":"sensor-1","namespace":"workshop"},
 "spec":{"class":"pico2w","wifi":{"secretRef":{"name":"workshop-wifi"}}}}
J
k create --validate=false -f "$D/device.json" >/dev/null || fail "create Device"
for _ in $(seq 60); do
  PH="$(k -n workshop get device sensor-1 -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [ "$PH" = Claimable ] && break
  sleep 0.5
done
[ "$PH" = Claimable ] || fail "the Device never read Claimable (phase '$PH')"
k -n workshop get device sensor-1 -o json >"$D/dev.json"
NSUID="$(k get namespace workshop -o jsonpath='{.metadata.uid}')"
OPAQUE="$(printf '%s' "$NSUID" | sha256sum | cut -c1-16)"
python3 - "$D/dev.json" "$OPAQUE" <<'PY' || fail "the Claimable status is not the contract's: $(head -c 400 "$D/dev.json")"
import json, sys
st = json.load(open(sys.argv[1]))["status"]
assert st["identity"]["spiffeId"] == "spiffe://nanocloud.local/device/workshop/sensor-1", st
assert st["mqtt"]["topicPrefix"] == "t/%s/devices/sensor-1/" % sys.argv[2], st
PY
grep -q "$(get /device-claim-of/workshop/sensor-1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')" "$D/dev.json" \
  && fail "the Device's status carries its token"
TOKEN="$(get /device-claim-of/workshop/sensor-1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')"
[[ "$TOKEN" =~ ^[a-z0-9]{6}\.[a-z0-9]{16}$ ]] || fail "the claim token is not bootstrap-shaped: '$TOKEN'"
TID="${TOKEN%%.*}"
OFFER="$(get "/device-claims/$TID")"
grep -q "${TOKEN#*.}" <<<"$OFFER" && fail "the offer holds the secret, not its hash"
echo "   Claimable; topicPrefix t/$OPAQUE/devices/sensor-1/; token $TID.<secret>, its offer hashed"

echo "== 2. the firmware subresource says what is missing =="
CODE="$(api "$(kubectl --kubeconfig "$KC" config view --raw -o jsonpath='{.users[0].user.token}')" GET \
  /apis/nanocloud.io/v1/namespaces/workshop/devices/sensor-1/firmware)"
[ "$CODE" = 501 ] || fail "firmware answered $CODE, not 501"
grep -q "sign a certificate request with its vault key" "$D/api.body" || fail "firmware's 501 does not say what is missing: $(cat "$D/api.body")"
echo "   501: $(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["message"])' "$D/api.body")"

echo "== 3. the device's one request: its key certified =="
openssl ecparam -name prime256v1 -genkey -noout -out "$D/dev.key" 2>/dev/null
csr req1 dev.key "$TID"
CODE="$(api "$TOKEN" POST "$CSRS" "$D/req1.json")"
[ "$CODE" = 201 ] || fail "redemption answered $CODE: $(cat "$D/api.body")"
cp "$D/api.body" "$D/issued.json"
python3 -c 'import base64,json,sys;print(base64.b64decode(json.load(open(sys.argv[1]))["status"]["certificate"]).decode(),end="")' \
  "$D/issued.json" >"$D/leaf.pem" || fail "no status.certificate"
openssl verify -CAfile "$D/ca.pem" "$D/leaf.pem" >/dev/null 2>&1 || fail "the certificate does not chain to the plane's authority"
[ "$(openssl x509 -in "$D/leaf.pem" -noout -pubkey | openssl pkey -pubin -outform DER | sha256sum)" = \
  "$(openssl ec -in "$D/dev.key" -pubout -outform DER 2>/dev/null | sha256sum)" ] || fail "the certificate is not for the device's key"
openssl x509 -in "$D/leaf.pem" -noout -ext subjectAltName | grep -q "URI:spiffe://nanocloud.local/device/workshop/sensor-1" \
  || fail "the certificate does not name the Device"
echo "   201; the leaf certifies the device's key as spiffe://nanocloud.local/device/workshop/sensor-1"

echo "== 4. the same request again, and a read under the token =="
CODE="$(api "$TOKEN" POST "$CSRS" "$D/req1.json")"
[ "$CODE" = 409 ] || fail "a repeated redemption answered $CODE, not 409: $(cat "$D/api.body")"
CODE="$(api "$TOKEN" GET "$CSRS/device-$TID")"
[ "$CODE" = 200 ] || fail "reading the CSR back answered $CODE"
cmp -s <(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["status"]["certificate"])' "$D/api.body") \
  <(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["status"]["certificate"])' "$D/issued.json") \
  || fail "the CSR read back carries another certificate"
CODE="$(api "$TOKEN" GET "$CSRS/device-zzzzzz")"
[ "$CODE" = 403 ] || fail "another CSR under the token answered $CODE, not 403"
echo "   409 AlreadyExists; GET reads the same certificate; another name 403"

echo "== 5. the spent token refuses another key; a wrong secret is refused =="
openssl ecparam -name prime256v1 -genkey -noout -out "$D/thief.key" 2>/dev/null
csr req2 thief.key "$TID"
CODE="$(api "$TOKEN" POST "$CSRS" "$D/req2.json")"
[ "$CODE" = 403 ] || fail "another key under the spent token answered $CODE, not 403"
CODE="$(api "$TID.aaaaaaaaaaaaaaaa" POST "$CSRS" "$D/req2.json")"
[ "$CODE" = 401 ] || fail "a wrong secret answered $CODE, not 401"
CODE="$(api "not-a-token" POST "$CSRS" "$D/req2.json")"
[ "$CODE" = 401 ] || fail "a bearer that is no claim token answered $CODE, not 401"
echo "   another key 403; a wrong secret 401"

echo "== 6. the Device reads Enrolled, and the token is gone =="
for _ in $(seq 20); do
  PH="$(k -n workshop get device sensor-1 -o jsonpath='{.status.phase}')"
  [ "$PH" = Enrolled ] && break
  sleep 0.5
done
[ "$PH" = Enrolled ] || fail "the Device reads '$PH', not Enrolled"
k -n workshop get device sensor-1 -o json >"$D/dev.json"
FP="$(openssl ec -in "$D/dev.key" -pubout -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1)"
END="$(date -u -d "$(openssl x509 -in "$D/leaf.pem" -noout -enddate | cut -d= -f2)" +%Y-%m-%dT%H:%M:%SZ)"
python3 - "$D/dev.json" "$FP" "$END" <<'PY' || fail "the Enrolled status is not the contract's: $(head -c 600 "$D/dev.json")"
import json, sys
st = json.load(open(sys.argv[1]))["status"]
assert st["identity"]["keyFingerprint"] == "sha256:" + sys.argv[2], st
assert st["identity"]["certificateNotAfter"] == sys.argv[3], st
assert {c["type"] for c in st["conditions"]} == {"Claimed", "CertificateValid"}, st
PY
[ -z "$(get /device-claim-of/workshop/sensor-1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')" ] \
  || fail "the redeemed token is still kept"
echo "   Enrolled; keyFingerprint sha256:${FP:0:12}…; notAfter $END; the token emptied"

echo "== 7. the certificate's lifetime: asked from the Device, reported as issued =="
# device_pki says so only when the authority issues another lifetime than asked.
grep -aq "\[device_pki\] device-$TID: asked 86400 s, the authority issued" "$D/run.log" \
  && fail "the authority issued another lifetime than the day asked"
SPAN="$(span "$D/leaf.pem")"
[ "$SPAN" = 86400 ] || fail "the authority issued $SPAN s, not the day asked"
status_holds workshop sensor-1 "st['identity']['certificateLifetime'] == '24h'" \
  || fail "the status does not report the lifetime the certificate has: $(head -c 600 "$D/st.json")"
echo "   asked 24h (the default), issued 24h, and the status says 24h"

echo "== 8. the enrolled key re-enrolls, with no token and no certificate =="
renewal ren1 dev.key
CODE="$(api "" POST "$CSRS" "$D/ren1.json")"
[ "$CODE" = 201 ] || fail "a re-enrollment by the enrolled key answered $CODE: $(cat "$D/api.body")"
leaf_of "$D/api.body" "$D/leaf_r.pem"
[ "$(openssl x509 -in "$D/leaf_r.pem" -noout -pubkey | openssl pkey -pubin -outform DER | sha256sum)" = \
  "$(openssl ec -in "$D/dev.key" -pubout -outform DER 2>/dev/null | sha256sum)" ] || fail "the re-enrolled certificate is not for the device's key"
openssl verify -CAfile "$D/ca.pem" "$D/leaf_r.pem" >/dev/null 2>&1 || fail "the re-enrolled certificate does not chain"
cmp -s "$D/leaf.pem" "$D/leaf_r.pem" && fail "re-enrollment answered the old certificate"
END_R="$(date -u -d "$(openssl x509 -in "$D/leaf_r.pem" -noout -enddate | cut -d= -f2)" +%Y-%m-%dT%H:%M:%SZ)"
status_holds workshop sensor-1 "st['identity']['certificateNotAfter'] == '$END_R'" \
  || fail "the status does not follow the re-enrolled certificate: $(head -c 600 "$D/st.json")"
openssl ecparam -name prime256v1 -genkey -noout -out "$D/other.key" 2>/dev/null
renewal ren2 other.key
CODE="$(api "" POST "$CSRS" "$D/ren2.json")"
[ "$CODE" = 403 ] || fail "a key never enrolled re-enrolled ($CODE)"
echo "   201: a fresh certificate for the same key; status follows; a key never enrolled 403"
# expired a year ago, and no maxOffline: re-enrolled all the same
expire_claim workshop sensor-1 $((365 * 86400))
CODE="$(api "" POST "$CSRS" "$D/ren1.json")"
[ "$CODE" = 201 ] || fail "a key whose certificate expired a year ago (no maxOffline) answered $CODE: $(cat "$D/api.body")"
echo "   its certificate expired a year ago, no maxOffline: 201, never stranded"

echo "== 9. a Device recreated under its name: a new token, the first withdrawn =="
k -n workshop delete device sensor-1 >/dev/null || fail "delete Device"
k create --validate=false -f "$D/device.json" >/dev/null || fail "recreate Device"
NEW=""
for _ in $(seq 60); do
  NEW="$(get /device-claim-of/workshop/sensor-1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])' 2>/dev/null || true)"
  [ -n "$NEW" ] && [ "$NEW" != "$TOKEN" ] && [ -n "$(get "/device-claims/${NEW%%.*}")" ] && break
  sleep 0.5
done
[ -n "$NEW" ] && [ "$NEW" != "$TOKEN" ] || fail "no new token for the recreated Device"
for _ in $(seq 20); do [ -z "$(get "/device-claims/$TID")" ] && break; sleep 0.5; done
CODE="$(api "$TOKEN" GET "$CSRS/device-$TID")"
[ "$CODE" = 401 ] || fail "the first token still answers ($CODE) after its Device was recreated"
renewal ren0 dev.key
CODE="$(api "" POST "$CSRS" "$D/ren0.json")"
[ "$CODE" = 403 ] || fail "the old Device's key re-enrolled to the recreated Device ($CODE)"
csr req3 dev.key "${NEW%%.*}"
CODE="$(api "$NEW" POST "$CSRS" "$D/req3.json")"
[ "$CODE" = 201 ] || fail "the new token's redemption answered $CODE: $(cat "$D/api.body")"
echo "   new token ${NEW%%.*}; the first 401; the old Device's key 403 by re-enrollment; the new token redeemed"
openssl ecparam -name prime256v1 -genkey -noout -out "$D/dev2.key" 2>/dev/null

echo "== 10. revoked: no re-enrollment =="
k -n workshop patch device sensor-1 --type=merge -p '{"spec":{"revoked":true}}' >/dev/null || fail "revoke"
status_holds workshop sensor-1 "st.get('phase') == 'Revoked'" || fail "the revoked Device does not read Revoked"
renewal ren3 dev.key
CODE="$(api "" POST "$CSRS" "$D/ren3.json")"
[ "$CODE" = 403 ] || fail "a revoked Device's key re-enrolled ($CODE)"
grep -q revoked "$D/api.body" || fail "the refusal does not say revoked: $(cat "$D/api.body")"
echo "   Revoked; its key's re-enrollment 403"

echo "== 11. maxOffline: within it of expiry, re-enrolled; past it, refused =="
cat >"$D/short.json" <<'J'
{"apiVersion":"nanocloud.io/v1","kind":"Device","metadata":{"name":"sensor-2","namespace":"workshop"},
 "spec":{"class":"pico2w","certificate":{"lifetime":"30s","maxOffline":"1h"}}}
J
k create --validate=false -f "$D/short.json" >/dev/null || fail "create sensor-2"
T2=""
for _ in $(seq 60); do
  T2="$(get /device-claim-of/workshop/sensor-2 | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])' 2>/dev/null || true)"
  [ -n "$T2" ] && [ -n "$(get "/device-claims/${T2%%.*}")" ] && break
  sleep 0.5
done
[ -n "$T2" ] || fail "no token for sensor-2"
csr s2 dev2.key "${T2%%.*}"
CODE="$(api "$T2" POST "$CSRS" "$D/s2.json")"
[ "$CODE" = 201 ] || fail "sensor-2's redemption answered $CODE: $(cat "$D/api.body")"
leaf_of "$D/api.body" "$D/leaf2.pem"
[ "$(span "$D/leaf2.pem")" = 60 ] || fail "a 30s lifetime was not clamped up to the cell's floor (60 s in this graph): $(span "$D/leaf2.pem") s"
status_holds workshop sensor-2 "st.get('phase') == 'Enrolled'" || fail "sensor-2 never read Enrolled"
renewal s2r dev2.key
expire_claim workshop sensor-2 600
CODE="$(api "" POST "$CSRS" "$D/s2r.json")"
[ "$CODE" = 201 ] || fail "expired 10 min ago, within maxOffline 1h, answered $CODE: $(cat "$D/api.body")"
k -n workshop patch device sensor-2 --type=merge -p '{"spec":{"certificate":{"lifetime":"9999h"}}}' >/dev/null
expire_claim workshop sensor-2 $((2 * 3600))
CODE="$(api "" POST "$CSRS" "$D/s2r.json")"
[ "$CODE" = 403 ] || fail "expired 2 h ago, past maxOffline 1h, answered $CODE"
grep -q maxOffline "$D/api.body" || fail "the refusal does not name maxOffline: $(cat "$D/api.body")"
expire_claim workshop sensor-2 1800
CODE="$(api "" POST "$CSRS" "$D/s2r.json")"
[ "$CODE" = 201 ] || fail "back within maxOffline, answered $CODE"
leaf_of "$D/api.body" "$D/leaf2c.pem"
[ "$(span "$D/leaf2c.pem")" = 7776000 ] || fail "a 9999h lifetime was not clamped down to the cell's ceiling (90 days): $(span "$D/leaf2c.pem") s"
echo "   30s asked as the floor (60 s here); expired 10 min: 201; expired 2 h: 403 maxOffline; 9999h asked as the ceiling 90 days"

echo "== 12. renewal under the device's own certificate; then that certificate really expires =="
k api-resources --api-group=certificates.k8s.io -o name 2>"$D/ar.err" | grep -qx "certificatesigningrequests.certificates.k8s.io" \
  || fail "kubectl does not discover certificatesigningrequests: $(cat "$D/ar.err")"
cat >"$D/brief.json" <<'J'
{"apiVersion":"nanocloud.io/v1","kind":"Device","metadata":{"name":"sensor-3","namespace":"workshop"},
 "spec":{"class":"pico2w","certificate":{"lifetime":"90s"}}}
J
k create --validate=false -f "$D/brief.json" >/dev/null || fail "create sensor-3"
T3=""
for _ in $(seq 60); do
  T3="$(get /device-claim-of/workshop/sensor-3 | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])' 2>/dev/null || true)"
  [ -n "$T3" ] && [ -n "$(get "/device-claims/${T3%%.*}")" ] && break
  sleep 0.5
done
[ -n "$T3" ] || fail "no token for sensor-3"
openssl ecparam -name prime256v1 -genkey -noout -out "$D/dev3.key" 2>/dev/null
csr s3 dev3.key "${T3%%.*}"
# This one by stock kubectl, its claim token the credential: /apis lists
# certificates.k8s.io, so `create -f` maps the kind, and the create response
# carries the certificate.
kubectl --kubeconfig /dev/null --server "https://127.0.0.1:$PORT" --certificate-authority "$D/ca.pem" \
  --tls-server-name kubernetes.default.svc.cluster.local --token "$T3" --request-timeout=20s \
  create --validate=false -f "$D/s3.json" -o jsonpath='{.status.certificate}' >"$D/kc3.out" 2>"$D/kc3.err" \
  || fail "kubectl create of a device CSR under its claim token: $(cat "$D/kc3.err")"
base64 -d <"$D/kc3.out" >"$D/leaf3.pem" || fail "kubectl printed no certificate: $(cat "$D/kc3.out")"
echo "   kubectl create -f csr under the claim token: the certificate in the answer"
SPAN="$(span "$D/leaf3.pem")"
[ "$SPAN" = 90 ] || fail "sensor-3's certificate lasts $SPAN s, not the 90 s asked"
# Renewal under the certificate: kubectl-style mTLS, no token.
renewal s3r dev3.key
CODE="$(CERT="$D/leaf3.pem" KEY="$D/dev3.key" api "" POST "$CSRS" "$D/s3r.json")"
[ "$CODE" = 201 ] || fail "a renewal presenting the device's own certificate answered $CODE: $(cat "$D/api.body")"
leaf_of "$D/api.body" "$D/leaf3r.pem"
cmp -s "$D/leaf3.pem" "$D/leaf3r.pem" && fail "the renewal answered the old certificate"
openssl verify -CAfile "$D/ca.pem" "$D/leaf3r.pem" >/dev/null 2>&1 || fail "the renewed certificate does not chain"
echo "   renewed under its own certificate: 201, a fresh 90 s certificate"
# Wait the renewed certificate out, on the wall clock.
END3="$(date -u -d "$(openssl x509 -in "$D/leaf3r.pem" -noout -enddate | cut -d= -f2)" +%s)"
W=$(( END3 - $(date +%s) + 3 ))
[ "$W" -gt 0 ] && { echo "   waiting ${W} s for it to expire"; sleep "$W"; }
FROM="$(wc -l <"$D/run.log")"
CODE="$(CERT="$D/leaf3r.pem" KEY="$D/dev3.key" api "" GET /version)"
[ "$CODE" = 000 ] || fail "an expired certificate was served ($CODE)"
tail -n +"$((FROM + 1))" "$D/run.log" | grep -aq "peer auth FAIL.*reason=expired" || fail "the handshake did not refuse the certificate as expired"
CODE="$(api "" POST "$CSRS" "$D/s3r.json")"
[ "$CODE" = 201 ] || fail "the key of a really expired certificate did not re-enroll ($CODE): $(cat "$D/api.body")"
leaf_of "$D/api.body" "$D/leaf3n.pem"
openssl x509 -in "$D/leaf3n.pem" -noout -checkend 30 >/dev/null || fail "the re-enrolled certificate is not current"
echo "   expired for real: refused at the handshake (expired); re-enrolled by its key: 201"

echo "== E2E green: a Device adopted by claim token and one CSR, on the packaged control plane =="
