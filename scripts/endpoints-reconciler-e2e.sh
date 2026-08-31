#!/usr/bin/env bash
# Live E2E for the endpoints chain (Chronicle params) as it ships in
# packaging/debian/fluxor-endpoints.yaml — selector matching over the
# control-plane store, consumed through the standard storage contracts
# (`storage.object` 0x14 + `storage.namespace` 0x13). The store is
# single-process, owned by the fluxor-linux runtime, and seeded from its
# durable append-log at init.
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# the projection's writes are replayed from `$D/store.log` at boot rather than
# raced in live. The chain cold-starts: its sources SUBSCRIBE /services/ and
# /pods/, then a full pass selector-matches ready pods per service and writes
# /endpoints/<ns>/<name>.
#
# Compact format:
#   /services/<ns>/<name>  = "sel=k=v,k=v"
#   /pods/<ns>/<name>      = "ip=<addr>;l=k=v,k=v;r=0|1"
#   /endpoints/<ns>/<name> = "<pod>=<addr>,<pod>=<addr>"
#
# Two phases prove durable reconcile: phase A seeds services + pods (web-2 NOT
# ready) and checks the projection; phase B appends web-2's readiness flip to the
# same log and re-runs (a fresh process replays the updated state) and checks the
# reconcile followed.
#
# Prereqs: `fluxor sync` (SDK + fmod palette + runtime pinned in fluxor.lock)
# and `fluxor modules build --target bcm2712` run in this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-endpoints.yaml"

# The runtime pinned by fluxor.lock (registry bin shelf), overridable.
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-epr-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

# Append one put record to the store's durable log (same wire the store writes:
# [rev:u64][op:u8=1][key_len:u16][val_len:u32][key][val]). Single-writer here —
# no flock, because nothing else has the file open (the runtime is not running).
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

store_last() { # store_last <key>  — last-writer value of a key in the log
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
        out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

run_runtime() { # run_runtime — boot the runtime over the seeded log for one window
  nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 2.5 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true
}

echo "== phase A: project two services, four pods (web-2 NOT ready), reconcile =="
store_put "/services/default/web" '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"}}}'
store_put "/services/default/api" '{"metadata":{"name":"api"},"spec":{"selector":{"app":"api","tier":"backend"}}}'
store_put "/pods/default/web-1"  '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","ready":true}}'
store_put "/pods/default/web-2"  '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","ready":false}}'  # NOT ready
store_put "/pods/default/api-1"  '{"metadata":{"name":"api-1","labels":{"app":"api","tier":"backend"}},"status":{"podIP":"10.0.0.3","ready":true}}'
store_put "/pods/default/db-1"   '{"metadata":{"name":"db-1","labels":{"app":"db"}},"status":{"podIP":"10.0.0.4","ready":true}}'  # matches nothing
run_runtime

WEB="$(store_last /endpoints/default/web)"
API="$(store_last /endpoints/default/api)"
[ "$WEB" = "web-1=10.0.0.1" ] \
  || fail "phase A web endpoints wrong: got '$WEB', want 'web-1=10.0.0.1' (web-2 not ready)"
[ "$API" = "api-1=10.0.0.3" ] \
  || fail "phase A api endpoints wrong: got '$API', want 'api-1=10.0.0.3' (db-1 must not match; multi-label selector)"
echo "   web: $WEB"
echo "   api: $API"

echo "== phase B: web-2 becomes ready, re-boot, reconcile follows =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","ready":true}}'  # readiness flips
run_runtime

WEB2="$(store_last /endpoints/default/web)"
[ "$WEB2" = "web-1=10.0.0.1,web-2=10.0.0.2" ] \
  || fail "phase B web endpoints wrong: got '$WEB2', want 'web-1=10.0.0.1,web-2=10.0.0.2' (readiness flip included)"
echo "   web: $WEB2"

echo "== E2E green: selector-matched endpoints over the fluxor-native store =="
