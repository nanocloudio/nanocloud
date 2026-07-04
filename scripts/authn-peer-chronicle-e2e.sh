#!/usr/bin/env bash
# The mTLS-peer half of authn as a Chronicle graph — the EFFECT-JOIN case.
#
# The verdict needs BOTH the store's answer and the request that provoked it:
# which reqid to answer, and the peer to fall back on when no binding exists.
# Chronicle has no effect call/join that retains the pre-effect record for a
# later stage, so the request's
# context rides THROUGH `store_effect` as opaque carry-through (fields 9..12)
# and comes back untouched — narrow on purpose, and not a general substitute.
#
#   store_source  watch /authn-req/, flat records, project `peer`
#   ask           decision: GET /peer-ids/<peer>, carrying reqid + peer
#   get           store_effect, key_prefix /peer-ids/
#   verdict       decision: the store's answer + the carried request -> status
#   put           store_effect, key_prefix /authn-resp/, guarded
#
# SCOPE: the peer path only. authn also verifies bearer tokens as compact JWS
# (signature, kid, suite, validity window, via kagi's fragments) — cryptography,
# which stays in the module. This is the peer half only, and the token cases
# from authn-e2e.sh are deliberately absent.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-anchron-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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
store_last() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. seed the peer binding and authn-e2e.sh's three peer requests =="
store_put "/peer-ids/spiffe://cluster/ns/default/sa/web" "id=system:serviceaccount:default:web"
store_put "/authn-req/r1" "peer=spiffe://cluster/ns/default/sa/web"    # bound   -> remapped
store_put "/authn-req/r3" "peer=spiffe://cluster/ns/default/sa/ghost"  # unbound -> itself
store_put "/authn-req/r6" "peer="                                      # none    -> anonymous

echo "== 2. write the graph — source, decision, effect, decision, writer =="
DEC_ASK="$(nc_decision "$ROOT/modules/app/_chronicle/authn_peer.uproc" ask)"
[ -n "$DEC_ASK" ] || { echo "FAIL: could not compile authn_peer.uproc ask"; exit 1; }
DEC_VERDICT="$(nc_decision "$ROOT/modules/app/_chronicle/authn_peer.uproc" verdict)"
[ -n "$DEC_VERDICT" ] || { echo "FAIL: could not compile authn_peer.uproc verdict"; exit 1; }
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: store_source
    prefix: "/authn-req/"
    flat: 1
    paths: "peer"
  - name: ask
    type: decision
    params:
      decision: "${DEC_ASK}"
  - name: get
    type: store_effect
    key_prefix: "/peer-ids/"
  - name: verdict
    type: decision
    params:
      decision: "${DEC_VERDICT}"
  - name: put
    type: store_effect
    key_prefix: "/authn-resp/"
  - name: dbg
    type: debug
    mode: 1
wiring:
  - from: store_source.status
    to: store_source.changes
  # Engine inputs are byte FIFOs: a Chronicle engine self-frames.
  - from: store_source.record_out
    to: ask.record_in
  - from: ask.result_out
    to: get.request_in
    buffer_group: 1
  - from: get.response_out
    to: verdict.record_in
  - from: verdict.result_out
    to: put.request_in
    buffer_group: 2
  - from: put.response_out
    to: dbg.data
    buffer_group: 3
YAML

echo "== 3. run =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
sleep 5
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""

echo "== 4. verdicts must match authn's, case for case =="
expect() { # expect <reqid> <want>
  got="$(store_last "/authn-resp/$1")"
  [ "$got" = "$2" ] || fail "$1: got '$got' want '$2'"
  echo "   $1 -> $got"
}
expect r1 "200;system:serviceaccount:default:web"
expect r3 "200;spiffe://cluster/ns/default/sa/ghost"
expect r6 "401;anonymous"

echo "== E2E green: read-then-decide across an effect, with the request carried through — effect-join without X-C1 =="
