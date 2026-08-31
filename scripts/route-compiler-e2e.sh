#!/usr/bin/env bash
# Live E2E for the route-compile chain (Chronicle params) as it ships in
# packaging/debian/fluxor-route-compiler.yaml — the /routes/ + /endpointslices/
# -> /dataplane/edge/ seam, verified UNPRIVILEGED over the control-plane store.
#
# An EXTERNAL process (this script, playing the API projection, the route
# validation's /route-status/ and the endpoints chain's slices) writes Routes,
# route-statuses and endpoint slices into the shared WAL; the chain, inside the
# fluxor-linux runtime, resolves each validated Route's service to its ready
# backends and PUBLISHES one key per route to
# /dataplane/edge/<ns>/<name> carrying the full backend set. That key IS the
# `http`-module edge's DynRoute table source (fluxor-edge.yaml `routes_prefix`).
#
# What this proves, and deliberately does NOT: it asserts the route TABLE is
# compiled and store-visible, not that traffic flows. The full Route → edge 443
# → workload data path is exercised by scripts/ingress-e2e.sh.
# The edge-side ingestion of these rows into the in-module DynRoute arena
# (in-module state, not store-visible) is unit-tested in the fluxor harness:
#   fluxor/tests/harness/tests/dyn_routes.rs (table_consumer_* + dyn_route_*).
#
# Data model:
#   /routes/<ns>/<name>       = {"spec":{"host":…,"to":{"name":…},"port":…,"path":…}}  (JSON)
#   /route-status/<ns>/<name> = "ready=1;endpoint=<svc>:<port>"   (route_validator gate)
#   /endpoints/<ns>/<svc>     = "<pod>=<addr>,<pod>=<addr>"       (compact)
#   /dataplane/edge/<ns>/<name> = host=<h>;path=<prefix>;be=<ip>:<port>:<w>:<r>,…
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-route-compiler.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_source.fmod" "$MODULES_DIR/store_effect.fmod" "$MODULES_DIR/decision.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-rc-e2e-XXXXXX)"
trap 'if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi' EXIT
fail() { echo "FAIL: $1"; echo "--- edge rows ---"; grep -a "/dataplane/edge/" "$D/store.log" 2>/dev/null | tr -c '[:print:]\n' '.'; tail -10 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value>   (op=1 PUT)
  python3 - "$D/store.log" 1 "$1" "$2" <<'PY'
import struct, sys, os
path, op, key, val = sys.argv[1], int(sys.argv[2]), sys.argv[3].encode(), sys.argv[4].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, o, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, op, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

wal_del() { # wal_del <key>   (op=2 DELETE)
  python3 - "$D/store.log" 2 "$1" "" <<'PY'
import struct, sys, os
path, op, key, val = sys.argv[1], int(sys.argv[2]), sys.argv[3].encode(), sys.argv[4].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, o, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, op, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

store_last() { # store_last <key> — latest value, empty if deleted/absent
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
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else None
    p += 15 + kl + vl
sys.stdout.buffer.write(out if out is not None else b"")
PY
}

run_compiler() {
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn \
    timeout 2.5 "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
    >"$D/run.log" 2>&1 || true
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project Routes + route-status + endpoints =="
# A validated, multi-backend route.
wal_put "/routes/default/web"        '{"spec":{"host":"api.example.com","to":{"name":"web"},"port":8080,"path":"/v1/"}}'
wal_put "/route-status/default/web"  "ready=1;endpoint=web:8080"
# One key per backend, which is what endpoints.uproc materialises on the way to
# building the /endpoints/ document. The compiler joins these rather than
# mapping over the folded list — the per-element work is already done.
wal_put "/endpointslices/default/web/web-1" "10.0.0.1"
wal_put "/endpointslices/default/web/web-2" "10.0.0.2"
wal_put "/endpoints/default/web"     "web-1=10.0.0.1,web-2=10.0.0.2"
# A route that failed validation (ready=0) — must NOT be compiled.
wal_put "/routes/default/bad"        '{"spec":{"to":{"name":"nohost"},"port":9090,"path":"/"}}'
wal_put "/route-status/default/bad"  "ready=0;msg=host must be set"
wal_put "/endpointslices/default/nohost/n-1" "10.0.0.9"
wal_put "/endpoints/default/nohost"  "n-1=10.0.0.9"

echo "== 3. run the compiler; it PUBLISHES one key per validated route =="
run_compiler

echo "== 4. verify the compiled /dataplane/edge/ rows =="
EDGE="$(store_last /dataplane/edge/default/web)"
[ -n "$EDGE" ] || fail "no edge row compiled for default/web"
grep -q "host=api.example.com" <<<"$EDGE"                          || fail "edge row missing host"
grep -q "path=/v1/" <<<"$EDGE"                                     || fail "edge row missing path"
grep -q "be=10.0.0.1:8080:1:1,10.0.0.2:8080:1:1" <<<"$EDGE"        || fail "backend set wrong: $EDGE"
[ -z "$(store_last /dataplane/edge/default/bad)" ]                 || fail "unvalidated route was compiled (gate leak)"

echo "== 5. route removal -> single DELETE of the edge row =="
wal_del "/routes/default/web"
wal_del "/route-status/default/web"
run_compiler
[ -z "$(store_last /dataplane/edge/default/web)" ]                 || fail "edge row not deleted after route removal"

echo "== E2E green: validated Route -> /dataplane/edge/ backend set compiled;"
echo "   unvalidated route gated out; route removal DELETEs the edge row."
echo "   (edge-side DynRoute ingestion proven in fluxor tests/harness/dyn_routes.rs)"
