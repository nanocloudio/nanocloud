#!/usr/bin/env bash
# ServiceAccount tokens as Chronicle params (sa_token.uproc) over kagi's
# token_mint, through the kagi_mint connector (packaging/debian/fluxor-sa-token.yaml).
#
#   /token-req/<id>  = "ns=<ns>;sa=<sa>[;aud=<aud>]"
#   /token-resp/<id> = "token=<JWT>"  |  "error=mint-refused"
#   /authn-keys/sa   = kagi MSG_KEY_ADD (VERIFY) — the key kagi_verify loads
#
# Proves the token is REAL: ES256, signed under the key the mint announced
# (checked with a real ECDSA verifier, not by shape), carrying the subject, the
# audience and the Kubernetes `kubernetes.io` claim. And that the key is a
# vault key: a restart announces the SAME key, and an answered request is not
# minted again.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-sa-token.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/kagi_mint.fmod" "$MODULES_DIR/token_mint.fmod" "$GRAPH"; do
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

# The deployment's seal key (postinst writes one per node): without it the
# vault has nowhere durable to keep the signing root, and step 6 would fail.
SEAL="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
run() { # run <seconds>
  FLUXOR_SEAL_KEY="$SEAL" FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$1" "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}
count_puts() { # count_puts <key>
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}
key_hex() { # the announced key frame, hex
  python3 - "$D/store.log" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0; out = ""
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == b"/authn-keys/sa":
        out = data[p+15+kl:p+15+kl+vl].hex() if op == 1 else ""
    p += 15 + kl + vl
print(out)
PY
}

echo "== 1. build the sa-token graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"

echo "== 2. three requests: explicit, defaulted, and one naming no ServiceAccount =="
store_put "/token-req/req1" "ns=default;sa=builder;aud=nanocloud"
store_put "/token-req/req2" "sa=bare"
store_put "/token-req/bad" "ns=team"
run 5

echo "== 3. the mint announced its vault key where kagi_verify loads it =="
KEY1="$(key_hex)"
[ -n "$KEY1" ] || fail "/authn-keys/sa was not written"

echo "== 4. each token verifies against THAT key, with the right claims =="
check() { # check <id> <sub> <aud> <ns> <sa>
  RESP="$(store_last "/token-resp/$1")"
  JWT="${RESP#token=}"
  [ "$JWT" != "$RESP" ] || fail "no token= in /token-resp/$1: '$RESP'"
  python3 - "$JWT" "$KEY1" "$2" "$3" "$4" "$5" <<'PY' || fail "token $1 did not verify"
import sys, base64, json
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes
jwt, frame, sub, aud, ns, sa = sys.argv[1:7]
frame = bytes.fromhex(frame)
# MSG_KEY_ADD: [type][len u16] then the record; the key is the trailing f16.
assert frame[0] == 0x22, f"not a MSG_KEY_ADD: {frame[:3].hex()}"
klen = int.from_bytes(frame[-67:-65], "little")
key = frame[-65:]
assert klen == 65 and key[0] == 4, "announced key is not a P-256 point"
pub = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), key)
def b64u(s): return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))
h, p, sg = jwt.split(".")
hdr, pl, sig = json.loads(b64u(h)), json.loads(b64u(p)), b64u(sg)
assert hdr["alg"] == "ES256", hdr
assert len(sig) == 64
der = utils.encode_dss_signature(int.from_bytes(sig[:32], "big"), int.from_bytes(sig[32:], "big"))
pub.verify(der, f"{h}.{p}".encode(), ec.ECDSA(hashes.SHA256()))
assert pl["iss"] == "https://kubernetes.default.svc", pl
assert pl["sub"] == sub, pl
assert pl["aud"] == aud, pl
assert pl["kubernetes.io"] == {"namespace": ns, "serviceaccount": {"name": sa}}, pl
assert 0 < pl["exp"] - pl["iat"] <= 3600, pl
print(f"   {sub}: signature verifies, aud={pl['aud']}, exp-iat={pl['exp']-pl['iat']}")
PY
}
check req1 "system:serviceaccount:default:builder" nanocloud default builder
check req2 "system:serviceaccount:default:bare" nanocloud default bare

echo "== 5. a request naming no ServiceAccount is answered, not left to time out =="
[ "$(store_last /token-resp/bad)" = "error=mint-refused" ] || fail "bad: got '$(store_last /token-resp/bad)'"
echo "   bad: error=mint-refused"

echo "== 6. restart: the SAME key (a vault key, not a per-process one), and no re-mint =="
run 4
[ "$(key_hex)" = "$KEY1" ] || fail "the key changed across a restart"
[ "$(count_puts /token-resp/req1)" = 1 ] || fail "req1 was minted again: $(count_puts /token-resp/req1) puts"
echo "   same key; req1 answered once"

echo "== E2E green: ServiceAccount tokens as params — minted by kagi under a vault key, verifiable =="
