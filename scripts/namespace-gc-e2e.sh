#!/usr/bin/env bash
# Live E2E for the namespace teardown chain (`ns_*`, Chronicle params) as it
# ships in packaging/debian/fluxor-namespace-gc.yaml — teardown over the
# control-plane store, consumed through the standard storage contracts
# (`storage.object` 0x14 + `storage.namespace` 0x13). The store is
# single-process, owned by the fluxor-linux runtime, and seeded from its
# durable append-log at init.
#
# Proves the namespace controller's teardown on real binaries: a namespace marked
# phase=Terminating has every object under it (across all namespaced resource
# prefixes) deleted, and its record finalized — while objects in OTHER namespaces
# are untouched.
#
#   /namespaces/<name>     = "phase=Active|Terminating"
#   /<resource>/<ns>/<obj> = …
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-namespace-gc.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-nsgc-e2e-XXXXXX)"
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
present() { [ -n "$(store_last "$1")" ]; }
absent()  { [ -z "$(store_last "$1")" ]; }

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. two namespaces; 'doomed' is Terminating with objects, 'default' Active =="
store_put "/namespaces/default" '{"metadata":{"name":"default"},"status":{"phase":"Active"}}'
store_put "/namespaces/doomed"  '{"metadata":{"name":"doomed"},"status":{"phase":"Terminating"}}'
# objects in the doomed namespace, across several resource kinds
store_put "/pods/doomed/p1"                 "image=x"
store_put "/deployments.apps/doomed/d1"     "replicas=1;image=x"
store_put "/configmaps/doomed/c1"           "k=v"
store_put "/services/doomed/s1"             "sel=app=x"
# objects in the default namespace — must be untouched
store_put "/pods/default/keep1"             "image=y"
store_put "/configmaps/default/keep2"       "k=w"

echo "== 3. run the namespace GC =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the doomed namespace was swept + finalized =="
for k in /pods/doomed/p1 /deployments.apps/doomed/d1 /configmaps/doomed/c1 /services/doomed/s1; do
  absent "$k" || fail "$k should be deleted (namespace terminating)"
done
absent /namespaces/doomed || fail "the namespace record should be finalized (removed)"
echo "   doomed: all objects deleted; namespace record finalized"

echo "== 5. verify the default namespace is untouched =="
present /namespaces/default   || fail "default namespace must remain"
present /pods/default/keep1    || fail "default pod must remain"
present /configmaps/default/keep2 || fail "default configmap must remain"
echo "   default: namespace + objects intact"

echo "== E2E green: namespaced teardown + finalize, other namespaces isolated =="
