#!/usr/bin/env bash
# Live E2E for nanocloud's sa_token module (modules/app/sa_token) — the
# ServiceAccount token issuer. On start it self-generates a P-256 signing key
# (private scalar in the kernel KEY_VAULT) and publishes /sa-pubkey; each
# /token-req/ mints a signed JWS (ES256) for system:serviceaccount:<ns>:<name>
# and returns it at /token-resp/.
#
# This mints a token and proves it's a real JWT: three base64url parts, the
# header decodes to the ES256 header, the payload decodes to the right subject,
# and the signature is 64 bytes (P-256 r||s).
#
#   /sa-pubkey       = "<signing pubkey, 65-byte EC point, hex>"
#   /token-req/<id>  = "ns=<ns>;sa=<sa>[;aud=<aud>]"
#   /token-resp/<id> = "token=<JWT>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-sa-token.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/sa_token.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-sat-e2e-XXXXXX)"
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
    if data[p+15:p+15+kl] == want:
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else b""
    p += 15 + kl + vl
sys.stdout.buffer.write(out or b"")
PY
}

echo "== 1. build config + module table =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. request a token for system:serviceaccount:default:builder =="
store_put "/token-req/req1" "ns=default;sa=builder;aud=nanocloud"

echo "== 3. run sa_token: generate key, mint the token =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the signing pubkey was published =="
PUB="$(store_last /sa-pubkey)"
[ "${#PUB}" = 130 ] || fail "pubkey should be 130 hex chars (65-byte EC point): got ${#PUB}"
case "$PUB" in 04*) echo "   /sa-pubkey: ${PUB:0:24}... (65-byte uncompressed EC point)" ;; *) fail "pubkey not an uncompressed EC point: $PUB" ;; esac

echo "== 5. verify the minted token is a real JWT (ES256) =="
RESP="$(store_last /token-resp/req1)"
JWT="${RESP#token=}"
[ "$JWT" != "$RESP" ] || fail "no token= in response: '$RESP'"
python3 - "$JWT" <<'PY'
import sys, base64, json
jwt = sys.argv[1]
parts = jwt.split(".")
assert len(parts) == 3, f"JWT must have 3 parts, got {len(parts)}"
def b64u(s): return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
hdr = json.loads(b64u(parts[0]))
pl = json.loads(b64u(parts[1]))
sig = b64u(parts[2])
assert hdr.get("alg") == "ES256", f"alg not ES256: {hdr}"
assert hdr.get("typ") == "JWT", f"typ not JWT: {hdr}"
assert pl.get("sub") == "system:serviceaccount:default:builder", f"sub wrong: {pl}"
assert pl.get("aud") == "nanocloud", f"aud wrong: {pl}"
assert pl.get("kubernetes.io", {}).get("serviceaccount", {}).get("name") == "builder", f"k8s claim wrong: {pl}"
assert len(sig) == 64, f"ES256 signature must be 64 bytes (r||s), got {len(sig)}"
print(f"   header:  {hdr}")
print(f"   payload: sub={pl['sub']} aud={pl['aud']}")
print(f"   signature: {len(sig)} bytes (P-256 r||s)")
PY
[ $? -eq 0 ] || fail "JWT verification failed"

echo "== E2E green: sa_token mints a real ES256-signed ServiceAccount JWT =="
