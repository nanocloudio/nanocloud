#!/usr/bin/env bash
# Namespace teardown as a Chronicle graph. Two chains over /namespaces/:
#   sweep     a JOIN scoped to the namespace across every namespaced prefix —
#             one record per object it contains, its full key at 24 — DELETE
#             when the namespace is Terminating
#   finalize  count_children over the same prefixes; Terminating and 0 ->
#             DELETE the namespace record
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-nschron-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}
store_last() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}
# how many records were written for <key> (PUT-if-changed must not spend revisions)
store_writes() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}

DEC_SW="$(nc_decision "$ROOT/modules/app/_chronicle/namespace.uproc" sw)"
DEC_FIN="$(nc_decision "$ROOT/modules/app/_chronicle/namespace.uproc" fin)"
[ -n "$DEC_SW" ] && [ -n "$DEC_FIN" ] || { echo "FAIL: namespace.uproc did not compile"; exit 1; }
PREFIXES="/pods/,/services/,/endpoints/,/configmaps/,/secrets/,/serviceaccounts/,/pvcs/,/deployments.apps/,/replicasets.apps/,/daemonsets.apps/,/statefulsets.apps/,/jobs.batch/,/hpa/,/networkpolicies/"
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: objects
    type: store_source
    prefix: "/namespaces/"
    paths: "status.phase"
    join: "${PREFIXES}"
    join_scoped: 1
  - name: sweep
    type: decision
    params:
      decision: "${DEC_SW}"
  - name: collect
    type: store_effect
  - name: namespaces
    type: store_source
    prefix: "/namespaces/"
    paths: "status.phase"
    count_children: "${PREFIXES}"
    child_sep: "/"
    watch: "${PREFIXES}"
  - name: finalize
    type: decision
    params:
      decision: "${DEC_FIN}"
  - name: remove
    type: store_effect
    key_prefix: "/namespaces/"
wiring:
  - from: objects.status
    to: objects.changes
  - from: objects.record_out
    to: sweep.record_in
  - from: sweep.result_out
    to: collect.request_in
    buffer_group: 1
  - from: namespaces.status
    to: namespaces.changes
  - from: namespaces.record_out
    to: finalize.record_in
  - from: finalize.result_out
    to: remove.request_in
    buffer_group: 2
YAML

run() { # run <seconds>
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  sleep "$1"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}
present() { [ -n "$(store_last "$1")" ]; }
absent()  { [ -z "$(store_last "$1")" ]; }

echo "== 1. 'doomed' is Terminating with objects in four trees; 'default' is Active =="
store_put "/namespaces/default" '{"metadata":{"name":"default"},"status":{"phase":"Active"}}'
store_put "/namespaces/doomed"  '{"metadata":{"name":"doomed"},"status":{"phase":"Terminating"}}'
for k in /pods/doomed/p1 /pods/doomed/p2 /services/doomed/s1 /configmaps/doomed/c1 /deployments.apps/doomed/d1 /jobs.batch/doomed/j1; do
  store_put "$k" '{"metadata":{"name":"x"},"spec":{}}'
done
store_put /pods/default/keep1 '{"metadata":{"name":"keep1"}}'
store_put /configmaps/default/keep2 '{"metadata":{"name":"keep2"}}'
run 7
for k in /pods/doomed/p1 /pods/doomed/p2 /services/doomed/s1 /configmaps/doomed/c1 /deployments.apps/doomed/d1 /jobs.batch/doomed/j1; do
  absent "$k" || fail "$k should be swept"
done
echo "   six objects across four trees swept"
absent /namespaces/doomed || fail "doomed should be finalized once empty"
echo "   /namespaces/doomed finalized"
present /namespaces/default && present /pods/default/keep1 && present /configmaps/default/keep2 || fail "default must be untouched"
echo "   default and its objects untouched"

echo "== 2. an Active namespace with objects is never swept; a Terminating EMPTY one is finalized at once =="
store_put "/namespaces/empty" '{"metadata":{"name":"empty"},"status":{"phase":"Terminating"}}'
run 5
absent /namespaces/empty || fail "an empty Terminating namespace should be finalized"
present /pods/default/keep1 || fail "default swept"
echo "   empty finalized; default still whole"

echo "== E2E green: namespace teardown as params — a scoped join across every namespaced tree, a count to finalize =="
