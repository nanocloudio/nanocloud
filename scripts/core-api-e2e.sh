#!/usr/bin/env bash
# Live E2E for nanocloud's core_api module (modules/app/core_api) — core/v1 CRUD
# over the control-plane store. A k8s API handler is a
# pure transform — request in, store read/CAS-write, response out.
#
# An EXTERNAL process (this script, playing the API front after admission) writes
# a sequence of /core-req/ ops; the core_api fmod performs each store operation
# on /<resource>/<ns>/<name> and writes /core-resp/. Request ids are
# zero-padded (r01..r10) so the store's byte-ordered LIST processes them in
# lifecycle order within one pass.
#
# Compact format:
#   /core-req/<reqid>       = "verb=<v>;resource=<r>;ns=<ns>;name=<n>;obj=<obj>"
#   /<resource>/<ns>/<name> = <object>
#   /core-resp/<reqid>      = "<status>;<body>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-coreapi.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/core_api.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-core-e2e-XXXXXX)"
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

echo "== 2. project a full CRUD lifecycle (r01..r10, in order) =="
wal_put "/core-req/r01" "verb=create;resource=configmaps;ns=default;name=cm1;obj=key=value1"
wal_put "/core-req/r02" "verb=get;resource=configmaps;ns=default;name=cm1"
wal_put "/core-req/r03" "verb=create;resource=configmaps;ns=default;name=cm1;obj=key=dup"
wal_put "/core-req/r04" "verb=create;resource=configmaps;ns=default;name=cm2;obj=k=v"
wal_put "/core-req/r05" "verb=list;resource=configmaps;ns=default"
wal_put "/core-req/r06" "verb=update;resource=configmaps;ns=default;name=cm1;obj=key=value2"
wal_put "/core-req/r07" "verb=get;resource=configmaps;ns=default;name=cm1"
wal_put "/core-req/r08" "verb=get;resource=configmaps;ns=default;name=ghost"
wal_put "/core-req/r09" "verb=delete;resource=configmaps;ns=default;name=cm1"
wal_put "/core-req/r10" "verb=get;resource=configmaps;ns=default;name=cm1"

echo "== 3. run core_api =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 4 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the CRUD lifecycle =="
expect() { # expect <reqid> <want>
  local got; got="$(wal_last "/core-resp/$1")"
  [ "$got" = "$2" ] || fail "core-resp $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect r01 "201;key=value1"       # create
expect r02 "200;key=value1"       # get
expect r03 "409;already exists"   # create dup
expect r04 "201;k=v"              # create cm2
expect r05 "200;cm1,cm2"          # list
expect r06 "200;key=value2"       # update
expect r07 "200;key=value2"       # get (updated)
expect r08 "404;not found"        # get missing
expect r09 "200;deleted"          # delete
expect r10 "404;not found"        # get after delete

echo "== E2E green: core/v1 CRUD as a store transform — the write path flows =="