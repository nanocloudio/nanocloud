#!/usr/bin/env bash
# Device identities as params over kagi's certificate_endpoint
# (packaging/debian/fluxor-device.yaml). A Device names the public key it holds;
# the chain has it certified — POST /pki/certificate through http_effect — and
# writes the identity back. OpenSSL proves the leaf is genuine: it certifies the
# DEVICE's key (whose private half never left the test), carries the SPIFFE URI
# SAN, and chains to the CA the authority answered with.
#
#   /devices.nanocloud.io/<ns>/<name>          = {"spec":{"publicKey":"<b64url>"}}  (input)
#   /deviceidentities.nanocloud.io/<ns>/<name>  = "spiffe=…;crt=<b64url DER>;ca=<b64url DER>"
#   /ca-cert                                    = the CA certificate, b64url DER
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-device.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl required for this E2E"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/http_effect.fmod" "$MODULES_DIR/certificate_endpoint.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-device-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

store_last() { # store_last <key>
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want:
        out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

b64u_to_pem() { # b64u_to_pem <b64url DER> <out.pem>
  python3 - "$1" "$2" <<'PY'
import sys, base64
v = sys.argv[1]
der = base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))
pem = b"-----BEGIN CERTIFICATE-----\n" + base64.encodebytes(der) + b"-----END CERTIFICATE-----\n"
open(sys.argv[2], "wb").write(pem)
PY
}

SEAL="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
run() { # run <seconds>
  FLUXOR_SEAL_KEY="$SEAL" FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$1" "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. alice holds a P-256 key and names its public half; bob names none =="
openssl ecparam -name prime256v1 -genkey -noout -out "$D/alice.key" 2>/dev/null
PK="$(openssl ec -in "$D/alice.key" -pubout -outform DER 2>/dev/null | tail -c 65 | python3 -c 'import sys,base64; print(base64.urlsafe_b64encode(sys.stdin.buffer.read()).rstrip(b"=").decode())')"
store_put "/devices.nanocloud.io/default/alice" "{\"spec\":{\"publicKey\":\"$PK\"}}"
store_put "/devices.nanocloud.io/default/bob" '{"spec":{}}'
run 5

SPIFFE_ID="spiffe://nanocloud.local/device/default/alice"

echo "== 3. the chain wrote alice's identity, and the CA =="
IDENT="$(store_last /deviceidentities.nanocloud.io/default/alice)"
[ -n "$IDENT" ] || fail "no identity for alice"
echo "$IDENT" | grep -q "^spiffe=$SPIFFE_ID;" || fail "identity missing the SPIFFE id: ${IDENT:0:80}"
CRT="${IDENT#*;crt=}"; CRT="${CRT%%;ca=*}"
CA="${IDENT##*;ca=}"
[ -n "$CRT" ] && [ -n "$CA" ] || fail "identity missing crt/ca"
[ "$(store_last /ca-cert)" = "$CA" ] || fail "/ca-cert is not the CA the identity chains to"
[ -z "$(store_last /deviceidentities.nanocloud.io/default/bob)" ] || fail "bob has no key and must not be certified"
echo "   alice certified; /ca-cert published; bob (no key) left alone"
b64u_to_pem "$CRT" "$D/leaf.pem"
b64u_to_pem "$CA" "$D/ca.pem"

echo "== 4. openssl: the leaf certifies ALICE's key, carries the SPIFFE SAN, and chains =="
SAN="$(openssl x509 -in "$D/leaf.pem" -noout -ext subjectAltName 2>/dev/null)"
echo "$SAN" | grep -q "URI:$SPIFFE_ID" || fail "leaf missing SPIFFE URI SAN; got: $SAN"
openssl verify -CAfile "$D/ca.pem" "$D/leaf.pem" >/dev/null 2>&1 \
  || fail "openssl verify rejected the leaf against the CA"
[ "$(openssl x509 -in "$D/leaf.pem" -noout -pubkey | openssl pkey -pubin -outform DER | sha256sum)" = \
  "$(openssl ec -in "$D/alice.key" -pubout -outform DER 2>/dev/null | sha256sum)" ] \
  || fail "the leaf does not certify alice's key"
echo "   leaf SAN = URI:$SPIFFE_ID; certifies alice's key; chains to nanocloud-ca"

echo "== 5. restart: certified once, and the CA is the same (a vault key) =="
PUTS="$(python3 - "$D/store.log" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == b"/deviceidentities.nanocloud.io/default/alice" and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
)"
[ "$PUTS" = 1 ] || fail "alice certified $PUTS times"
store_put "/devices.nanocloud.io/default/carol" "{\"spec\":{\"publicKey\":\"$PK\"}}"
run 4
CA2="$(store_last /deviceidentities.nanocloud.io/default/carol)"; CA2="${CA2##*;ca=}"
b64u_to_pem "$CA2" "$D/ca2.pem"
[ "$(openssl x509 -in "$D/ca.pem" -noout -pubkey)" = "$(openssl x509 -in "$D/ca2.pem" -noout -pubkey)" ] \
  || fail "the CA key changed across a restart"
echo "   alice not re-certified; carol's CA has the same key"

echo "== E2E green: Device identities as params — certified by kagi under the node's vault root =="
