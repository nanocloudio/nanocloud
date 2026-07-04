#!/usr/bin/env bash
# Live E2E for nanocloud's statefulset_reconciler module (modules/app/
# statefulset_reconciler) — ordered, stable-identity Pods over the fluxor-native
# control-plane store, consumed through the standard storage contracts
# (`storage.object` 0x14 + `storage.namespace` 0x13). The store is single-process, owned by the fluxor-linux
# runtime, and seeded from its durable append-log at init.
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# desired state and simulated kubelet readiness are replayed from `$D/store.log`
# at boot rather than raced in live. Each phase appends to the log and re-runs a
# fresh process, which proves the defining StatefulSet behaviour: Pods come up
# ONE AT A TIME in ordinal order — <sts>-<i> is not created until <sts>-<i-1> is
# ready — and scale-down removes the highest ordinal first.
#
#   /statefulsets.apps/<ns>/<name> = "replicas=<N>;image=<img>"
#   /pods/<ns>/<name>-<i>          = "image=<img>;owner=<sts>[;ready=1]"
#
# Prereqs: `fluxor sync` and `fluxor modules build --target bcm2712` in this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-statefulset.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/statefulset_reconciler.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-sts-e2e-XXXXXX)"
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
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 2 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true
}
store_delete() { # store_delete <key> — append a tombstone (log op 2, no value)
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

present() { [ -n "$(store_last "$1")" ]; }
absent()  { [ -z "$(store_last "$1")" ]; }

echo "== phase 1: StatefulSet replicas=3 → only db-0 is created (db-1 waits for db-0 ready) =="
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
run_runtime
present /pods/default/db-0 || fail "db-0 should be created"
absent  /pods/default/db-1 || fail "db-1 must WAIT until db-0 is ready"
echo "   db-0 created; db-1 held"

echo "== phase 2: db-0 becomes ready → db-1 is created; db-2 waits =="
store_put "/pods/default/db-0" '{"metadata":{"name":"db-0"},"spec":{"containers":[{"image":"postgres"}]},"status":{"ready":true}}'
run_runtime
present /pods/default/db-1 || fail "db-1 should be created once db-0 is ready"
absent  /pods/default/db-2 || fail "db-2 must WAIT until db-1 is ready"
echo "   db-1 created; db-2 held"

echo "== phase 3: db-1 becomes ready → db-2 is created =="
store_put "/pods/default/db-1" '{"metadata":{"name":"db-1"},"spec":{"containers":[{"image":"postgres"}]},"status":{"ready":true}}'
run_runtime
present /pods/default/db-2 || fail "db-2 should be created once db-1 is ready"
echo "   db-2 created — full ordered bring-up"

echo "== phase 4: scale down to replicas=1 → db-2, db-1 removed (highest first) =="
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
run_runtime
present /pods/default/db-0 || fail "db-0 should remain"
absent  /pods/default/db-1 || fail "db-1 should be scaled down"
absent  /pods/default/db-2 || fail "db-2 should be scaled down"
echo "   scaled down to db-0 only"

echo "== phase 5: scale-down across a HOLE — the surplus above a missing ordinal is still found =="
# Regression guard. Ordinals are NOT contiguous: a Pod deleted out of band
# leaves a hole. A reverse scan that stops at the first absent ordinal hides
# every HIGHER one from the "highest existing" search, and since each later
# pass stops at the same hole those Pods are orphaned permanently rather than
# torn down late.
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db"},"spec":{"replicas":4,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
run_runtime                                          # db-0 ready -> db-1 created
store_put "/pods/default/db-1" '{"metadata":{"name":"db-1"},"spec":{"containers":[{"image":"postgres"}]},"status":{"ready":true}}'
run_runtime                                          # db-1 ready -> db-2 created
store_put "/pods/default/db-2" '{"metadata":{"name":"db-2"},"spec":{"containers":[{"image":"postgres"}]},"status":{"ready":true}}'
run_runtime                                          # db-2 ready -> db-3 created
for i in 0 1 2 3; do
  present /pods/default/db-$i || fail "phase 5 setup: db-$i missing"
done
echo "   scaled up to 4: db-0..db-3 present"

store_delete "/pods/default/db-2"                     # the hole
store_put "/statefulsets.apps/default/db" '{"metadata":{"name":"db"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"db","image":"postgres"}]}}}}'
run_runtime                                          # one delete per pass, reverse order
present /pods/default/db-0 || fail "phase 5: db-0 should remain"
for i in 1 2 3; do
  absent /pods/default/db-$i || fail "phase 5: db-$i survived scale-down past the hole at db-2"
done
echo "   hole at db-2 did not hide db-3: all surplus torn down, highest-first"

echo "== E2E green: ordered StatefulSet bring-up + scale-down (incl. across a hole) over the fluxor-native store =="
