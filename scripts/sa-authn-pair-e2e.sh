#!/usr/bin/env bash
# The pair: sa_token MINTS a ServiceAccount JWT, authn VERIFIES that exact
# token. One store, two graphs, one credential.
#
# This is the composition the plan calls "the two incompatible authentication
# models collapse to one", and until now they genuinely could not meet:
# `sa_token` minted real ES256 JWTs while `authn` looked bearer credentials up
# in a `/sa-tokens/` table that nothing ever wrote to. Both modules passed
# their own e2e; the PAIR had no test, so nobody noticed the seam did not
# join.
#
# What makes it work now: `authn` verifies signatures instead of consulting a
# table, and `sa_token` publishes its verification key as a kagi
# `MSG_KEY_ADD` frame at `/authn-keys/sa`. Neither half is trusted by
# assertion — the token that comes out of step 1 is the exact bytes fed to
# step 2, and a wrong key, a wrong `kid` or a lapsed window all fail it.
#
# It is deliberately an END-TO-END check rather than two unit tests: a seam is
# the one thing a per-module test cannot cover.
set -euo pipefail
ROOT=/home/pi/Development/nanocloudio/nanocloud
cd "$ROOT"; . scripts/fluxor-env.sh
RT="$(nc_fluxor_runtime "$ROOT")"
D=$(mktemp -d /tmp/pair-XXXXXX)
seed() { python3 - "$D/store.log" "$1" "$2" <<'PYS'
import struct,sys,os
path,key,val=sys.argv[1],sys.argv[2].encode(),sys.argv[3].encode()
fd=os.open(path,os.O_RDWR|os.O_CREAT,0o644)
with os.fdopen(fd,"r+b") as f:
    d=f.read(); p=0; last=0
    while p+15<=len(d):
        rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15])
        if p+15+kl+vl>len(d): break
        last=rev; p+=15+kl+vl
    f.seek(0,2); f.write(struct.pack("<QBHI",last+1,1,len(key),len(val))+key+val)
    f.flush(); os.fsync(f.fileno())
PYS
}
last() { python3 - "$D/store.log" "$1" <<'PYL'
import struct,sys
d=open(sys.argv[1],'rb').read(); want=sys.argv[2].encode(); p=0; out=None
while p+15<=len(d):
    rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15])
    if p+15+kl+vl>len(d): break
    k=d[p+15:p+15+kl]; v=d[p+15+kl:p+15+kl+vl]
    if k==want: out=v if op==1 else None
    p+=15+kl+vl
sys.stdout.write(out.decode("utf-8","replace") if out is not None else "")
PYL
}
echo "== 1. sa_token mints a token and publishes its key =="
nc_build_workload "$ROOT" packaging/debian/fluxor-sa-token.yaml "$D/c1.bin" "$D/m1.bin" >/dev/null 2>&1
seed "/token-req/t1" "ns=default;sa=builder;aud=nanocloud"
FLUXOR_STORE_DIR="$D" RUST_LOG=error timeout 8 "$RT" --config "$D/c1.bin" --modules "$D/m1.bin" >/dev/null 2>&1 || true
TOK=$(last "/token-resp/t1"); TOK="${TOK#token=}"
KEYLEN=$(python3 - "$D/store.log" <<'PYK'
import struct,sys
d=open(sys.argv[1],'rb').read(); want=b"/authn-keys/sa"; p=0; out=None
while p+15<=len(d):
    rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15])
    if p+15+kl+vl>len(d): break
    if d[p+15:p+15+kl]==want: out=d[p+15+kl:p+15+kl+vl]
    p+=15+kl+vl
print(len(out) if out else 0)
PYK
)
echo "   token: ${#TOK} chars   key frame: $KEYLEN bytes published"
[ -n "$TOK" ] || { echo "FAIL: no token"; exit 1; }
[ "$KEYLEN" -gt 0 ] || { echo "FAIL: no key published"; exit 1; }
echo "== 2. authn verifies that same token, in the same store =="
nc_build_workload "$ROOT" packaging/debian/fluxor-authn.yaml "$D/c2.bin" "$D/m2.bin" >/dev/null 2>&1
seed "/authn-req/p1" "token=$TOK"
FLUXOR_STORE_DIR="$D" RUST_LOG=error timeout 8 "$RT" --config "$D/c2.bin" --modules "$D/m2.bin" >/dev/null 2>&1 || true
GOT=$(last "/authn-resp/p1")
echo "   authn says: $GOT"
case "$GOT" in
  "200;system:serviceaccount:default:builder") echo "== PAIR COMPOSES: sa_token mints, authn verifies ==" ;;
  *) echo "FAIL: expected 200;system:serviceaccount:default:builder"; exit 1 ;;
esac
rm -rf "$D"
