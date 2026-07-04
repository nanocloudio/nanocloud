#!/usr/bin/env bash
# Live E2E for nanocloud's authn module (modules/app/authn) — mTLS/token identity
# extraction over the control-plane store.
#
# An EXTERNAL process (this script, playing fluxor tls + the API front) seeds the
# credential bindings and writes /authn-req/ with a peer identity (as fluxor tls
# would extract from a client cert) or a bearer token; the authn fmod resolves
# each to a Kubernetes identity and writes /authn-resp/. A verified mTLS peer
# always authenticates (remapped via /peer-ids/ or, absent a binding, as itself);
# an unknown bearer token or missing credential is anonymous (401).
#
# Compact format:
#   /authn-req/<reqid>  = "peer=<spiffe>" | "token=<bearer>"
#   /peer-ids/<spiffe>  = "id=<identity>"     (OPTIONAL mTLS peer remap)
#   /authn-keys/<n>     = <kagi MSG_KEY_ADD frame>   (verification key)
#   /authn-resp/<reqid> = "200;<identity>" | "401;anonymous"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-authn.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/authn.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-authn-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

# Seed a BINARY value, given as hex. Verification keys are kagi wire frames,
# not text, so they cannot go through `wal_put`.
wal_put_hex() { # wal_put_hex <key> <hex>
  python3 - "$D/store.log" "$1" "$2" <<'PYHEX'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), bytes.fromhex(sys.argv[3])
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last = rev
        p += 15 + kl + vl
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PYHEX
}

wal_put() { # wal_put <key> <value> — append a put to the store's durable log.
  # Single-writer: the runtime isn't running while we seed, so no flock needed.
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
    f.flush()
    os.fsync(f.fileno())
PY
}

wal_last() { # wal_last <key>
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
        out = v
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

# A real ES256 keypair and a real signed JWT. The bearer credential is now
# VERIFIED, not looked up, so this has to produce one that actually verifies —
# a string in the store no longer authenticates anything.
echo "== 2. mint an ES256 key + a signed ServiceAccount JWT =="
KEYDIR="$D/keys"; mkdir -p "$KEYDIR"
openssl ecparam -name prime256v1 -genkey -noout -out "$KEYDIR/k.pem" 2>/dev/null
JWT_AND_KEY="$(python3 "$ROOT/scripts/support/authn_fixture.py" "$KEYDIR/k.pem")"
JWT="$(echo "$JWT_AND_KEY" | sed -n 1p)"
KEYFRAME="$(echo "$JWT_AND_KEY" | sed -n 2p)"
EXPIRED="$(echo "$JWT_AND_KEY" | sed -n 3p)"

echo "== 3. seed the peer binding, the verification key, and the requests =="
wal_put "/peer-ids/spiffe://cluster/ns/default/sa/web" "id=system:serviceaccount:default:web"
wal_put_hex "/authn-keys/k1" "$KEYFRAME"
wal_put "/authn-req/r1" "peer=spiffe://cluster/ns/default/sa/web"   # bound mTLS peer (remapped)
wal_put "/authn-req/r2" "token=$JWT"                                # a genuinely signed JWT
wal_put "/authn-req/r3" "peer=spiffe://cluster/ns/default/sa/ghost" # unbound peer (self-identity)
wal_put "/authn-req/r4" "token=forged"                             # not a JWS at all
wal_put "/authn-req/r5" "garbage"                                  # no credential
wal_put "/authn-req/r6" "peer="                                    # empty peer
wal_put "/authn-req/r7" "token=$EXPIRED"                           # signed, but outside its window

echo "== 4. run the authenticator =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 5. verify each credential resolved (or was rejected) =="
expect() { # expect <reqid> <want>
  local got; got="$(wal_last "/authn-resp/$1")"
  [ "$got" = "$2" ] || fail "authn for $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect r1 "200;system:serviceaccount:default:web"    # bound peer → remapped identity
expect r2 "200;system:serviceaccount:default:api"    # SIGNATURE verified → sub is the identity
expect r3 "200;spiffe://cluster/ns/default/sa/ghost" # unbound peer → peer is the identity
expect r4 "401;anonymous"                            # not a JWS — nothing to verify
expect r5 "401;anonymous"                            # no credential
expect r6 "401;anonymous"                            # empty peer
expect r7 "401;anonymous"                            # correctly signed but expired

echo "== E2E green: mTLS peer (remapped or self) + a bearer JWT verified against a keyset =="