#!/usr/bin/env bash
# Live E2E for nanocloud's cert_manager module (modules/app/cert_manager) — the
# one-shot certificate minting service. On start it self-generates a CA (private
# key in the kernel KEY_VAULT) and publishes /ca-cert; each /cert-req/ mints a
# leaf keypair, builds a real X.509 certificate in-module, signs the TBS with the
# CA key by handle, and returns /cert-resp/ with the cert + key.
#
# This script mints a leaf, then uses OPENSSL to prove the result is a genuine,
# chain-valid X.509 certificate: the CA cert and leaf both parse, the leaf's SAN
# matches the request, and `openssl verify` validates the leaf against the CA.
#
#   /ca-cert         = "<self-signed CA cert, DER hex>"
#   /cert-req/<id>   = "cn=<commonName>;dns=<dnsName>"
#   /cert-resp/<id>  = "crt=<leaf cert, DER hex>;key=<leaf P-256 scalar, hex>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-cert-manager.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl required for this E2E"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/cert_manager.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-cert-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

# Append one put record to the store's durable log (same wire the store writes:
# [rev:u64][op:u8=1][key_len:u16][val_len:u32][key][val]). Single-writer here —
# no flock, because nothing else has the file open (the runtime is not running).
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

store_last() { # store_last <key>  — last-writer value of a key in the log
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

echo "== 2. request a cert for web.default.svc.cluster.local =="
store_put "/cert-req/c1" "cn=web.default.svc;dns=web.default.svc.cluster.local"
# ... and a SPIFFE-shaped device identity (URI SAN, the family-canonical form).
SPIFFE_ID="spiffe://nanocloud.local/device/default/alice"
store_put "/cert-req/c2" "cn=device:alice;dns=alice.dev;spiffe=$SPIFFE_ID"

echo "== 3. run the minter (CA self-gen + vault, then mint the leaf) =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. extract the CA cert and the minted leaf =="
CA_HEX="$(store_last /ca-cert)"
[ -n "$CA_HEX" ] || fail "cert_manager did not publish /ca-cert"
RESP="$(store_last /cert-resp/c1)"
[ -n "$RESP" ] || fail "cert_manager did not mint /cert-resp/c1"
CRT_HEX="${RESP#crt=}"; CRT_HEX="${CRT_HEX%%;key=*}"
KEY_HEX="${RESP##*;key=}"
[ "${#KEY_HEX}" = 64 ] || fail "leaf private key must be 64 hex chars, got ${#KEY_HEX}"
hex_to_pem "$CA_HEX" "$D/ca.pem"
hex_to_pem "$CRT_HEX" "$D/leaf.pem"
echo "   /ca-cert: ${#CA_HEX} hex; leaf: ${#CRT_HEX} hex; key: 32 bytes"

echo "== 5. openssl: both certs parse; leaf carries the requested SAN =="
openssl x509 -in "$D/ca.pem" -noout -subject 2>/dev/null | grep -q 'nanocloud-ca' \
  || fail "CA cert did not parse / wrong subject"
LEAF_SUBJ="$(openssl x509 -in "$D/leaf.pem" -noout -subject 2>/dev/null)"
echo "$LEAF_SUBJ" | grep -q 'web.default.svc' || fail "leaf subject wrong: $LEAF_SUBJ"
openssl x509 -in "$D/leaf.pem" -noout -ext subjectAltName 2>/dev/null | grep -q 'web.default.svc.cluster.local' \
  || fail "leaf SAN missing the requested dNSName"
echo "   CA + leaf parse; leaf SAN = web.default.svc.cluster.local"

echo "== 6. openssl verify: the leaf chains to the self-signed CA =="
openssl verify -CAfile "$D/ca.pem" "$D/leaf.pem" >/dev/null 2>&1 \
  || fail "openssl verify rejected the leaf against the CA"
echo "   openssl verify: leaf.pem OK (chains to nanocloud-ca)"

echo "== 7. SPIFFE identity: the leaf carries the requested URI SAN and chains =="
RESP2="$(store_last /cert-resp/c2)"
[ -n "$RESP2" ] || fail "cert_manager did not mint /cert-resp/c2"
CRT2_HEX="${RESP2#crt=}"; CRT2_HEX="${CRT2_HEX%%;key=*}"
hex_to_pem "$CRT2_HEX" "$D/spiffe.pem"
SAN2="$(openssl x509 -in "$D/spiffe.pem" -noout -ext subjectAltName 2>/dev/null)"
echo "$SAN2" | grep -q "URI:$SPIFFE_ID" || fail "leaf missing SPIFFE URI SAN; got: $SAN2"
echo "$SAN2" | grep -q 'alice.dev' || fail "leaf missing the dNSName alongside the URI"
openssl verify -CAfile "$D/ca.pem" "$D/spiffe.pem" >/dev/null 2>&1 \
  || fail "openssl verify rejected the SPIFFE leaf against the CA"
echo "   SPIFFE leaf SAN = URI:$SPIFFE_ID (+ dNSName); chains to nanocloud-ca"

echo "== E2E green: a module minted a real, openssl-verified X.509 cert (dNSName + SPIFFE URI) — no new surface =="
