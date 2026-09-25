#!/usr/bin/env bash
# The authentication ladders fail CLOSED on a verdict nobody established.
#
# A verify record's `status` is kagi's `verify_err` as the wire carries it, and
# kagi's OK is 0 — the errno convention, not this repo's number to choose. That
# is safe only because Chronicle never lets an ABSENT field read as 0: absence
# is a type error that fails the decision (and a decoder cannot synthesize a
# value for input that is not there — chronicle docs/conformance.md). This
# pins it on the two decisions that read the verdict — apiplane_auth.uproc
# `decide` and apiplane.uproc `authz` — through `chronicle decide`, the
# decision module's own evaluation path:
#   absent   -> the decision fails (no outcome: nothing is answered)
#   refusal  -> a refusal (401/503), never 200 and never a probe
#   OK (0)   -> the request proceeds
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
fail() { echo "FAIL: $1"; exit 1; }

DECIDE="$(nc_decision "$ROOT/modules/app/_chronicle/apiplane_auth.uproc" decide)"
AUTHZ="$(nc_decision "$ROOT/modules/app/_chronicle/apiplane.uproc" authz)"
[ -n "$DECIDE" ] && [ -n "$AUTHZ" ] || fail "the auth decisions did not compile"

# frame <num>:<i|s|x|m>:<value> ...  -> a Chronicle record frame, hex
# (x = raw hex bytes; m = a nested record, hex, typed as a message: TY_MSG 3)
frame() {
  python3 - "$@" <<'PY'
import struct, sys
out = b""
for f in sys.argv[1:]:
    n, t, v = f.split(":", 2)
    if t == "i":
        out += struct.pack("<BBH", int(n), 1, 8) + struct.pack("<q", int(v))
    elif t in ("x", "m"):
        b = bytes.fromhex(v)
        out += struct.pack("<BBH", int(n), 3 if t == "m" else 0, len(b)) + b
    else:
        b = v.encode()
        out += struct.pack("<BBH", int(n), 0, len(b)) + b
print((bytes([len(sys.argv) - 1]) + out).hex())
PY
}
decide() { fluxor exec chronicle -- decide "$1" "$2" 2>&1 || true; }
# the i64 field `n` of an outcome frame (after "rule i "/"default ")
field_i64() {
  python3 - "$1" "$2" <<'PY'
import struct, sys
line, want = sys.argv[1].split(), int(sys.argv[2])
b = bytes.fromhex(line[-1]); p = 1
for _ in range(b[0]):
    n, t, l = struct.unpack("<BBH", b[p:p+4]); v = b[p+4:p+4+l]; p += 4 + l
    if n == want and t == 1:
        print(struct.unpack("<q", v)[0]); break
PY
}

echo "== 1. apiplane_auth decide =="
CARRY=(30:i:7 31:i:1 32:s:pods 33:s:default 34:s: 35:s:/pods/default/)
REST=(1:i:1 3:s:alice 4:s:iss 5:s:aud 6:s: 7:i:0 8:i:0)
out="$(decide "$DECIDE" "$(frame "${REST[@]}" "${CARRY[@]}")")"
case "$out" in error:*) echo "   absent status  -> $out" ;; *) fail "absent status must fail the decision, got '$out'" ;; esac
for st in 1 2 5 99; do
  out="$(decide "$DECIDE" "$(frame 2:i:$st "${REST[@]}" "${CARRY[@]}")")"
  code="$(field_i64 "$out" 2)"
  case "$code" in 401|503) echo "   status $st -> $code" ;; *) fail "status $st must be refused, got '$out'" ;; esac
done
out="$(decide "$DECIDE" "$(frame 2:i:0 "${REST[@]}" "${CARRY[@]}")")"
[ "$(field_i64 "$out" 2)" = 200 ] || fail "status OK must answer 200, got '$out'"
echo "   status 0 (OK) -> 200"

echo "== 2. apiplane authz =="
# The request's context (field 254: {1: group}) rides beside the carry, as
# every real record's does.
ACARRY=(30:i:7 31:i:1 32:s:/pods/default/ 33:s:pods 34:s: 35:s: 36:s:default 37:i:0 38:s: 39:i:0 254:m:020100000002000000)
out="$(decide "$AUTHZ" "$(frame 1:i:1 3:s:alice "${ACARRY[@]}")")"
case "$out" in error:*) echo "   absent status  -> $out" ;; *) fail "absent status must fail authz, got '$out'" ;; esac
# the verdict (field 33 of AuthzCall's carry) must be a refusal; the probe op NOOP
for st in 1 2 5 99; do
  out="$(decide "$AUTHZ" "$(frame 1:i:1 2:i:$st 3:s:alice "${ACARRY[@]}")")"
  v="$(field_i64 "$out" 33)"
  case "$v" in 401|503) echo "   status $st -> verdict $v" ;; *) fail "status $st must be refused, got '$out'" ;; esac
done
out="$(decide "$AUTHZ" "$(frame 1:i:1 2:i:0 3:s:alice "${ACARRY[@]}")")"
[ "$(field_i64 "$out" 33)" = 0 ] || fail "status OK must leave the verdict pending for RBAC, got '$out'"
echo "   status 0 (OK) -> pending, probed"

echo "== E2E green: an unestablished verdict never authenticates =="
