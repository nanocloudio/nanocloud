#!/usr/bin/env bash
# Live E2E for route validation — a validation reconciler over the control-plane
# store, run by the production graph (packaging/debian/fluxor-route.yaml) as a
# Chronicle decision compiled from modules/app/_chronicle/route_validator.uproc.
#
# The store is consumed through the standard storage contracts (storage.object
# 0x14 + storage.namespace 0x13); it lives inside the runtime and is seeded
# from its durable append-log ($D/store.log) at boot.
#
# The validation runs on the store-watch seam: this script (playing the route
# projection) seeds
# Route specs into the store log; the validator fmod, inside the fluxor-linux
# runtime, validates each (first-error) and writes
# /route-status/<ns>/<name> = "ready=1;endpoint=<svc>:<port>" or
# "ready=0;msg=<first error>".
#
# Compact format:
#   /routes/<ns>/<name>       = "host=<h>;service=<s>;port=<n>;path=<prefix>"
#   /route-status/<ns>/<name> = "ready=1;endpoint=<s>:<n>" | "ready=0;msg=<err>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-route.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_source.fmod" "$MODULES_DIR/decision.fmod" \
         "$MODULES_DIR/store_effect.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-rt-e2e-XXXXXX)"
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
    f.flush()
    os.fsync(f.fileno())
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
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want:
        out = v
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project routes: valid, valid-no-path, and four first-error cases =="
store_put "/routes/default/ok"       '{"metadata":{"name":"ok"},"spec":{"host":"example.com","to":{"name":"web"},"port":80,"path":"/api"}}'
store_put "/routes/default/ok-nopath" '{"metadata":{"name":"ok-nopath"},"spec":{"host":"example.com","to":{"name":"web"},"port":80}}'
store_put "/routes/default/no-host"  '{"metadata":{"name":"no-host"},"spec":{"to":{"name":"web"},"port":80}}'
store_put "/routes/default/no-svc"   '{"metadata":{"name":"no-svc"},"spec":{"host":"example.com","port":80}}'
store_put "/routes/default/bad-port" '{"metadata":{"name":"bad-port"},"spec":{"host":"example.com","to":{"name":"web"},"port":0}}'
store_put "/routes/default/bad-path" '{"metadata":{"name":"bad-path"},"spec":{"host":"example.com","to":{"name":"web"},"port":80,"path":"api"}}'

echo "== 3. run the validator =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify each route's computed status =="
expect() { # expect <name> <want>
  local got; got="$(store_last "/route-status/default/$1")"
  [ "$got" = "$2" ] || fail "route '$1' status wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect ok        "ready=1;endpoint=web:80"
expect ok-nopath "ready=1;endpoint=web:80"
expect no-host   "ready=0;msg=host must be set"
expect no-svc    "ready=0;msg=service name must be set"
expect bad-port  "ready=0;msg=service port must be non-zero"
expect bad-path  "ready=0;msg=path prefix must start with '/'"

echo "== E2E green: route validation over the shared store =="
