#!/usr/bin/env bash
# Live E2E for the Device identity chain + cert_manager: a Device CR is admitted
# and a SPIFFE identity is provisioned end to end. The chain writes a /cert-req/
# for the Device, cert_manager mints a real X.509 leaf carrying the SPIFFE URI
# SAN, and the chain projects the identity at /deviceidentities…/<ns>/<name>.
# OpenSSL then proves the projected leaf is genuine, carries the family-canonical
# SPIFFE id, and chains to the minter's CA.
#
#   /devices.nanocloud.io/<ns>/<name>          = <DeviceSpec JSON>       (input)
#   /cert-req/<ns>-<name>                       = "cn=…;dns=…;spiffe=…"   (reconciler → minter)
#   /cert-resp/<ns>-<name>                      = "crt=<hex>;key=<hex>"   (minter → reconciler)
#   /deviceidentities.nanocloud.io/<ns>/<name>  = "spiffe=…;crt=<hex>;ca=<hex>"  (output)
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
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/cert_manager.fmod" "$GRAPH"; do
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

hex_to_pem() { # hex_to_pem <hex> <out.pem>
  python3 - "$1" "$2" <<'PY'
import sys, base64
der = bytes.fromhex(sys.argv[1])
pem = b"-----BEGIN CERTIFICATE-----\n" + base64.encodebytes(der) + b"-----END CERTIFICATE-----\n"
open(sys.argv[2], "wb").write(pem)
PY
}

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. admit a Device CR (default/alice) =="
store_put "/devices.nanocloud.io/default/alice" '{"spec":{"hash":"a1b2c3","certificateSubject":"device:alice"}}'

echo "== 3. run device_reconciler + cert_manager over the shared store =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

SPIFFE_ID="spiffe://nanocloud.local/device/default/alice"

echo "== 4. the chain wrote a SPIFFE-shaped cert-req =="
REQ="$(store_last /cert-req/default-alice)"
[ -n "$REQ" ] || fail "device_reconciler did not write /cert-req/default-alice"
echo "$REQ" | grep -q "spiffe=$SPIFFE_ID" || fail "cert-req missing the SPIFFE id: $REQ"
echo "   /cert-req/default-alice = $REQ"

echo "== 5. cert_manager minted a response =="
RESP="$(store_last /cert-resp/default-alice)"
[ -n "$RESP" ] || fail "cert_manager did not mint /cert-resp/default-alice"

echo "== 6. the chain projected the device identity =="
IDENT="$(store_last /deviceidentities.nanocloud.io/default/alice)"
[ -n "$IDENT" ] || fail "device_reconciler did not project the identity status"
echo "$IDENT" | grep -q "spiffe=$SPIFFE_ID" || fail "identity missing the SPIFFE id"
CRT_HEX="${IDENT#*;crt=}"; CRT_HEX="${CRT_HEX%%;ca=*}"
CA_HEX="${IDENT##*;ca=}"
[ -n "$CRT_HEX" ] && [ -n "$CA_HEX" ] || fail "identity missing crt/ca"
hex_to_pem "$CRT_HEX" "$D/leaf.pem"
hex_to_pem "$CA_HEX" "$D/ca.pem"

echo "== 7. openssl: the provisioned leaf carries the SPIFFE URI SAN and chains =="
SAN="$(openssl x509 -in "$D/leaf.pem" -noout -ext subjectAltName 2>/dev/null)"
echo "$SAN" | grep -q "URI:$SPIFFE_ID" || fail "provisioned leaf missing SPIFFE URI SAN; got: $SAN"
openssl verify -CAfile "$D/ca.pem" "$D/leaf.pem" >/dev/null 2>&1 \
  || fail "openssl verify rejected the provisioned leaf against the CA"
echo "   identity leaf SAN = URI:$SPIFFE_ID; chains to nanocloud-ca"

echo "== E2E green: Device admitted → SPIFFE identity provisioned (reconciler + cert_manager) =="
