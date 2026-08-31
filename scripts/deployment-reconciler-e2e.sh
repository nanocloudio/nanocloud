#!/usr/bin/env bash
# Live E2E for the Deployment chain (`dp_*`, Chronicle params) as it ships in
# packaging/debian/fluxor-deployment.yaml — the Deployment → ReplicaSet
# projection over the control-plane store, consumed through the standard
# storage contracts (`storage.object` 0x14 + `storage.namespace` 0x13). The
# store is single-process, owned by the fluxor-linux runtime, and seeded from
# its durable append-log at init.
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# the API plane's writes are replayed from `$D/store.log` at boot rather than
# raced in live from an external writer. The chain cold-starts: `dp_source`
# SUBSCRIBEs /deployments.apps/, then a full pass projects the owned ReplicaSet
# at /replicasets.apps/<ns>/<name>.
#
#   /deployments.apps/<ns>/<name>  = "replicas=<N>;image=<img>"
#   /replicasets.apps/<ns>/<name>  = "replicas=<N>;image=<img>;owner=<name>"
#
# Two phases prove durable reconcile: phase A seeds web=3/api=1 and checks the
# projection; phase B appends web=5 to the same log and re-runs (a fresh process
# replays the updated desired state) and checks the reconcile followed.
#
# Prereqs: `fluxor sync` and `fluxor modules build --target bcm2712` in this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-deployment.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-depr-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

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
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 2.0 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true
}

# has <value> <substr...> — assert every substring is present in the value.
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== phase A: seed two JSON Deployments, boot, project JSON ReplicaSets =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
store_put "/deployments.apps/default/api" '{"metadata":{"name":"api"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"api","image":"redis"}]}}}}'
run_runtime

WEB="$(store_last /replicasets.apps/default/web)"
API="$(store_last /replicasets.apps/default/api)"
has "$WEB" '"replicas":3' '"image":"nginx"' '"kind":"Deployment"' '"name":"web"' \
  || fail "phase A web replicaset wrong: got '$WEB'"
has "$API" '"replicas":1' '"image":"redis"' '"kind":"Deployment"' \
  || fail "phase A api replicaset wrong: got '$API'"
echo "   web: $WEB"
echo "   api: $API"

echo "== phase B: scale web to 5, re-boot, reconcile follows desired state =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":5,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime

WEB2="$(store_last /replicasets.apps/default/web)"
has "$WEB2" '"replicas":5' '"image":"nginx"' \
  || fail "phase B web replicaset wrong: got '$WEB2'"
echo "   web: $WEB2"

echo "== E2E green: JSON Deployment → owned JSON ReplicaSet over the fluxor-native store =="
