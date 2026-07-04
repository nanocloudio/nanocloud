#!/usr/bin/env bash
# Live E2E for nanocloud's crypto_signer module (modules/app/crypto_signer) —
# proof that a nanocloud APP module can do real cryptography on EXISTING fluxor
# capabilities (kernel KEY_VAULT + SDK p256), so cert_manager needs NO new
# capability surface.
#
# The module generates a P-256 keypair, deposits the private key in the kernel
# KEY_VAULT (private key never re-enters the module), publishes the public key,
# and signs each /sign-req/ by vault handle — self-verifying with ecdsa_verify.
# This script writes a message and checks the signature came back valid; it also
# independently verifies the ECDSA signature against the published public key
# with openssl (when available) — a fully external confirmation.
#
#   /signer-pubkey  = "<uncompressed P-256 point, 130 hex>"
#   /sign-req/<id>  = "<message>"
#   /sign-resp/<id> = "sig=<128 hex>;valid=<0|1>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-crypto-signer.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/crypto_signer.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-signer-e2e-XXXXXX)"
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

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. request a signature over a message =="
store_put "/sign-req/r1" "nanocloud-attestation-payload"

echo "== 3. run the signer (keygen → vault store → vault sign → verify) =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the module signed and self-verified =="
RESP="$(store_last /sign-resp/r1)"
echo "   /sign-resp/r1 = $RESP"
case "$RESP" in
  sig=*\;valid=1) : ;;
  *) fail "expected 'sig=<128hex>;valid=1', got '$RESP'" ;;
esac
SIG="${RESP#sig=}"; SIG="${SIG%;valid=1}"
[ "${#SIG}" = 128 ] || fail "signature must be 128 hex chars (64-byte ECDSA r||s), got ${#SIG}"
PUB="$(store_last /signer-pubkey)"
[ "${#PUB}" = 130 ] || fail "pubkey must be 130 hex chars (uncompressed P-256 point), got ${#PUB}"
echo "   module self-verified the signature (valid=1); pubkey published (65 bytes)"

echo "== 5. independent verification against the published pubkey (openssl) =="
if command -v openssl >/dev/null 2>&1; then
  python3 - "$PUB" "$SIG" "nanocloud-attestation-payload" "$D" <<'PY' && echo "   openssl: signature verifies against the published public key" || { echo "   openssl verify FAILED"; exit 1; }
import sys, subprocess, hashlib, os
pub_hex, sig_hex, msg, d = sys.argv[1:5]
pub = bytes.fromhex(pub_hex)          # 0x04 || X(32) || Y(32)
r = int(sig_hex[:64], 16); s = int(sig_hex[64:], 16)
# DER-encode ECDSA-Sig-Value SEQUENCE{INTEGER r, INTEGER s}
def der_int(x):
    b = x.to_bytes((x.bit_length()+7)//8 or 1, "big")
    if b[0] & 0x80: b = b"\x00" + b
    return b"\x02" + bytes([len(b)]) + b
body = der_int(r) + der_int(s)
der = b"\x30" + bytes([len(body)]) + body
open(d+"/sig.der","wb").write(der)
# Build a PEM SubjectPublicKeyInfo for the raw P-256 point (prime256v1).
spki_prefix = bytes.fromhex("3059301306072a8648ce3d020106082a8648ce3d030107034200")
import base64
spki = spki_prefix + pub
pem = b"-----BEGIN PUBLIC KEY-----\n" + base64.encodebytes(spki) + b"-----END PUBLIC KEY-----\n"
open(d+"/pub.pem","wb").write(pem)
open(d+"/msg.bin","wb").write(msg.encode())
rc = subprocess.run(["openssl","dgst","-sha256","-verify",d+"/pub.pem","-signature",d+"/sig.der",d+"/msg.bin"],
                    capture_output=True)
sys.exit(0 if rc.returncode == 0 else 1)
PY
else
  echo "   openssl not present — skipping external check (module self-verify already passed)"
fi

echo "== E2E green: an app module signs via the kernel KEY_VAULT — no new surface =="
