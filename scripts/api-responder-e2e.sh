#!/usr/bin/env bash
# Live E2E for nanocloud's api_responder module (modules/app/api_responder) —
# the API-plane request/response seam over the control-plane store.
#
# Proves the API-plane shape on real binaries: an API endpoint is a pure
# transform — request in, store reads, response out — served by a fluxor
# module. An EXTERNAL process (this script, playing the HTTP front) writes /api-req/<id> ops into the shared WAL; the api_responder fmod, inside
# the fluxor-linux runtime, handles each and writes /api-resp/<id>. `count:<pfx>`
# is a real store transform (it LISTs the store); `healthz` is the trivial
# case.
#
# Compact format:
#   /api-req/<reqid>  = "<op>[:<arg>]"     e.g. "healthz" or "count:/pods/"
#   /api-resp/<reqid> = "<status>;<body>"  e.g. "200;ok" or "200;3"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-api.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/api_responder.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-api-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

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

echo "== 2. seed store state (3 pods) + four API requests =="
wal_put "/pods/default/a" "ip=10.0.0.1"
wal_put "/pods/default/b" "ip=10.0.0.2"
wal_put "/pods/default/c" "ip=10.0.0.3"
wal_put "/api-req/r1" "healthz"
wal_put "/api-req/r2" "count:/pods/"
wal_put "/api-req/r3" "count:/nope/"
wal_put "/api-req/r4" "bogus"
wal_put "/api-req/r5" "getjson:/pods/default/a"   # store read → JSON
wal_put "/api-req/r6" "getjson:/pods/default/nope"

echo "== 3. run the responder; it handles each request over the seam =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify each response (request -> store read -> response) =="
expect() { # expect <reqid> <want>
  local got; got="$(wal_last "/api-resp/$1")"
  [ "$got" = "$2" ] || fail "response for $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect r1 "200;ok"
expect r2 "200;3"     # count:/pods/ — a real store transform
expect r3 "200;0"     # empty prefix
expect r4 "404;unknown op"
expect r5 '200;{"ip":"10.0.0.1"}'   # getjson — compact record → JSON object
expect r6 "404;{}"                  # absent object

echo "== E2E green: API-as-store-transform (count + JSON object) served by a fluxor module =="