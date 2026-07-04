#!/usr/bin/env bash
# Live E2E for nanocloud's replicaset_reconciler module (modules/app/
# replicaset_reconciler) — the ReplicaSet → Pods reconcile over the fluxor-native
# control-plane store (storage.object + storage.namespace).
#
# The store is single-process, owned by the fluxor-linux runtime and seeded from
# its durable append-log at boot — so the API plane's projections are replayed
# from `$D/store.log` rather than raced in live from an external writer. The
# reconciler creates Pods /pods/<ns>/<rs>-<i> to match `replicas`, self-heals a
# deleted Pod, and scales down by deleting surplus Pods. Because external live
# mutation isn't visible mid-run, each mutation is a fresh boot over the updated
# log (three phases: create, self-heal, scale-down).
#
# Compact format:
#   /replicasets.apps/<ns>/<name> = "replicas=<N>;image=<img>"
#   /pods/<ns>/<name>-<i>         = "image=<img>;owner=<rs>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-replicaset.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/replicaset_reconciler.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-rsr-e2e-XXXXXX)"
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
    f.flush(); os.fsync(f.fileno())
PY
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

store_last() { # store_last <key>  (empty string if last record is a tombstone or absent)
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
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else b""
    p += 15 + kl + vl
print(out.decode() if out else "")
PY
}

run_runtime() { # boot the runtime over the seeded log for one window
  nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.0 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}

has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== phase A: project a ReplicaSet replicas=3, boot, create 3 pods =="
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime
for i in 0 1 2; do
  v="$(store_last /pods/default/web-$i)"
  has "$v" '"image":"nginx"' '"kind":"ReplicaSet"' '"name":"web-'"$i"'"' || fail "phase A pod web-$i wrong: got '$v'"
  echo "   web-$i: $v"
done
# Each pod creation records a SuccessfulCreate event on the ReplicaSet.
has "$(store_last /events/default/web.SuccessfulCreate.0)" '"reason":"SuccessfulCreate"' '"kind":"ReplicaSet"' \
  || fail "no SuccessfulCreate event: got '$(store_last /events/default/web.SuccessfulCreate.0)'"
echo "   event: ReplicaSet web SuccessfulCreate recorded"

echo "== phase B: a pod dies (web-1 tombstoned), re-boot, reconciler self-heals =="
store_delete "/pods/default/web-1"
run_runtime
has "$(store_last /pods/default/web-1)" '"image":"nginx"' \
  || fail "phase B web-1 not self-healed: got '$(store_last /pods/default/web-1)'"
echo "   web-1: self-healed"

echo "== phase C: scale down to replicas=1, re-boot, delete surplus =="
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime
has "$(store_last /pods/default/web-0)" '"image":"nginx"' || fail "web-0 should remain"
[ -z "$(store_last /pods/default/web-1)" ] || fail "web-1 should be deleted on scale-down"
[ -z "$(store_last /pods/default/web-2)" ] || fail "web-2 should be deleted on scale-down"
echo "   web-0: kept; web-1, web-2: deleted"

echo "== phase D: scale-down across a HOLE — surplus above a missing ordinal is still collected =="
# Regression guard. Ordinals are NOT contiguous: a Pod deleted out of band
# (kubectl, node loss, the GC) leaves a hole. A scale-down that stops at the
# first absent ordinal hides every HIGHER one, and since each later pass stops
# at the same hole those Pods are orphaned permanently rather than late.
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":4,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime
for i in 0 1 2 3; do
  has "$(store_last /pods/default/web-$i)" '"image":"nginx"' || fail "phase D setup: web-$i missing"
done
echo "   scaled up to 4: web-0..web-3 present"

store_delete "/pods/default/web-1"          # the hole, BELOW the surplus above it
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime
has "$(store_last /pods/default/web-0)" '"image":"nginx"' || fail "phase D: web-0 should remain"
for i in 1 2 3; do
  [ -z "$(store_last /pods/default/web-$i)" ] \
    || fail "phase D: web-$i survived scale-down past the hole at web-1"
done
echo "   hole at web-1 did not hide web-2/web-3: all surplus collected"

echo "== phase E: a prefix MANY pages long — 120 Pods created, then 119 collected =="
# The listing walk holds one 512-byte page at a time, so 120 keys is ten
# pages. Listing a prefix whole into a fixed buffer would cut it silently past
# ~45 objects, and a scale-down deletes WHILE walking, which an index-shaped
# cursor skips past. Both must converge here or the assertion fails: create all
# 120, then collect all but one.
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":120,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime
n=0; for i in $(seq 0 119); do [ -n "$(store_last /pods/default/web-$i)" ] && n=$((n+1)); done
[ "$n" = 120 ] || fail "phase E: only $n of 120 Pods exist — the listing walk stopped short"
echo "   120 Pods present across ten listing pages"
store_put "/replicasets.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run_runtime
n=0; for i in $(seq 1 119); do [ -n "$(store_last /pods/default/web-$i)" ] && n=$((n+1)); done
[ "$n" = 0 ] || fail "phase E: $n surplus Pods survived a paged scale-down"
has "$(store_last /pods/default/web-0)" '"image":"nginx"' || fail "phase E: web-0 should remain"
echo "   119 collected across pages while deleting mid-walk; web-0 kept"

echo "== E2E green: ReplicaSet → Pods (create, self-heal, scale-down, scale-down across a hole, paged) over the fluxor-native store =="
