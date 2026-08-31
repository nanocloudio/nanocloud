#!/usr/bin/env bash
# Live E2E for the full workload lifecycle:
# deployment_reconciler + replicaset_reconciler + the gc_ chain on ONE fluxor
# graph over the fluxor-native control-plane store, consumed through the standard
# storage contracts (`storage.object` 0x14 + `storage.namespace` 0x13). The
# store is single-process, owned by the fluxor-linux runtime, and seeded from its durable append-log at
# init. Creating a Deployment cascades forward to Pods; deleting it cascades
# backward — the GC collects the orphaned ReplicaSet, then the orphaned Pods —
# all through cooperating modules, no orchestration beyond the store.
#
#   /deployments.apps/<ns>/<name> = "replicas=<N>;image=<img>"
#   /replicasets.apps/<ns>/<name> = "replicas=<N>;image=<img>;owner=<dep>"
#   /pods/<ns>/<name>-<i>         = "image=<img>;owner=<rs>"
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# each phase replays `$D/store.log` at boot; the runtime's own writes persist
# back to the log, so phase B boots over phase A's projected state plus the
# appended delete.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-workload-lifecycle.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-wll-e2e-XXXXXX)"
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

store_delete() { # store_delete <key>  — append a tombstone (op=2)
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys, os
path, key = sys.argv[1], sys.argv[2].encode()
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
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
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

run_runtime() {
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 3 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}

echo "== 1. build the config + module table from the 3-module lifecycle graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. create a Deployment (replicas=2) — the forward cascade =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime

echo "== 3. verify Deployment → ReplicaSet → Pods =="
case "$(store_last /replicasets.apps/default/web)" in *replicas*2*) true ;; *) false ;; esac \
  || fail "replicaset not created"
case "$(store_last /pods/default/web-0)" in *nginx*) : ;; *) fail "pod web-0 not created";; esac
case "$(store_last /pods/default/web-1)" in *nginx*) : ;; *) fail "pod web-1 not created";; esac
echo "   created: replicaset web + pods web-0, web-1"

echo "== 4. delete the Deployment — the GC cascade collects the orphans =="
store_delete "/deployments.apps/default/web"
run_runtime

echo "== 5. verify the whole tree is collected =="
[ -z "$(store_last /replicasets.apps/default/web)" ] || fail "replicaset should be GC'd (owner Deployment gone)"
[ -z "$(store_last /pods/default/web-0)" ] || fail "pod web-0 should be GC'd (owner ReplicaSet gone)"
[ -z "$(store_last /pods/default/web-1)" ] || fail "pod web-1 should be GC'd (owner ReplicaSet gone)"
echo "   collected: replicaset web + pods web-0, web-1"

echo "== E2E green: full workload lifecycle — create cascade + ownerRef GC cascade =="
