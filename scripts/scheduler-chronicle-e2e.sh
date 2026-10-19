#!/usr/bin/env bash
# The scheduler as a Chronicle graph: least-loaded placement, one Pod per pass.
#   bind   the FIRST unbound Pod (where spec.nodeName=, index 0) JOIN
#          /ready-nodes/ picked by the scheduler's filters and scores
#          (store_source `sched`) -> merge-patch spec.nodeName; then the
#          Scheduled event
# The pick is a fold over the nodes, measured from the Pods themselves at
# pick time; it lives in the source, and the decision sees one record with
# the winner in it. The full surface (taints, affinity, resources,
# priority, preemption, cordon) is scripts/scheduler-api-e2e.sh's.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect store_big; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-schedchron-XXXXXX)"
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

# The production chain (sc_*) and the ready-nodes projection it reads,
# extracted from the shipped control plane.
python3 "$ROOT/scripts/extract-fixture.py" "$D/graph.yaml" - sc_ ds_nodes ds_mark >/dev/null
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
node_of() { store_last "/pods/default/$1" | sed -n 's/.*"nodeName":"\([^"]*\)".*/\1/p'; }

echo "== 1. two ready nodes, one not; four unbound Pods and one pre-bound to the dead node =="
store_put "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":true}}'
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":false}}'
for p in a b c d; do store_put "/pods/default/pod-$p" '{"metadata":{"name":"pod-'$p'"},"spec":{"containers":[{"image":"x"}]}}'; done
store_put "/pods/default/pod-z" '{"metadata":{"name":"pod-z"},"spec":{"nodeName":"node-3","containers":[{"image":"x"}]}}'
run 10
for p in a b c d; do n="$(node_of pod-$p)"; [ -n "$n" ] || fail "pod-$p unbound"; [ "$n" != "node-3" ] || fail "pod-$p bound to a not-ready node"; echo "   pod-$p -> $n"; done
[ "$(node_of pod-z)" = "node-3" ] || fail "pod-z's binding must not be touched"
N1=0; N2=0; for p in a b c d; do case "$(node_of pod-$p)" in node-1) N1=$((N1+1));; node-2) N2=$((N2+1));; esac; done
[ "$N1" = 2 ] && [ "$N2" = 2 ] || fail "not spread: node-1=$N1 node-2=$N2"
echo "   spread 2/2 across the ready nodes; pod-z untouched"
case "$(store_last /events/default/pod-a.Scheduled.0)" in *'"reason":"Scheduled"'*) : ;; *) fail "no Scheduled event for pod-a";; esac
echo "   Scheduled events recorded"

echo "== 2. a new Pod lands on the less-loaded node after one is deleted =="
python3 - "$D/store.log" <<'PY2'
import struct, sys
path = sys.argv[1]; key = b"/pods/default/pod-a"
with open(path, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2); f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
PY2
store_put "/pods/default/pod-e" '{"metadata":{"name":"pod-e"},"spec":{"containers":[{"image":"x"}]}}'
run 8
E="$(node_of pod-e)"; [ -n "$E" ] || fail "pod-e unbound"
# pod-a is gone from its node; pod-e must land THERE (the load is measured from the Pods)
N1=0; N2=0; for p in b c d e; do case "$(node_of pod-$p)" in node-1) N1=$((N1+1));; node-2) N2=$((N2+1));; esac; done
[ "$N1" = 2 ] && [ "$N2" = 2 ] || fail "after a delete the spread should be 2/2 again: node-1=$N1 node-2=$N2"
echo "   pod-e -> $E; spread 2/2 again"

echo "== 3. nodeSelector: a Pod lands only on a node whose labels it names; one that fits no node waits, and holds nobody behind it =="
store_put "/nodes/node-1" '{"metadata":{"name":"node-1","labels":{"kubernetes.io/arch":"arm64","nanocloud.io/fluxor-abi":"d1579ff4"}},"status":{"ready":true}}'
store_put "/nodes/node-2" '{"metadata":{"name":"node-2","labels":{"kubernetes.io/arch":"amd64","nanocloud.io/fluxor-abi":"d1579ff4"}},"status":{"ready":true}}'
# pod-0 sorts first among the unbound: an ABI no node has.
store_put "/pods/default/pod-0" '{"metadata":{"name":"pod-0"},"spec":{"nodeSelector":{"nanocloud.io/fluxor-abi":"00000000"},"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-f" '{"metadata":{"name":"pod-f"},"spec":{"nodeSelector":{"kubernetes.io/arch":"amd64"},"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-g" '{"metadata":{"name":"pod-g"},"spec":{"nodeSelector":{"kubernetes.io/arch":"arm64","nanocloud.io/fluxor-abi":"d1579ff4"},"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-h" '{"metadata":{"name":"pod-h"},"spec":{"nodeSelector":{},"containers":[{"image":"x"}]}}'
run 10
[ -z "$(node_of pod-0)" ] || fail "pod-0 names an ABI no node has, and was bound to $(node_of pod-0)"
[ "$(node_of pod-f)" = "node-2" ] || fail "pod-f selects amd64 (node-2), bound to '$(node_of pod-f)'"
[ "$(node_of pod-g)" = "node-1" ] || fail "pod-g selects arm64 + the ABI (node-1), bound to '$(node_of pod-g)'"
[ -n "$(node_of pod-h)" ] || fail "pod-h has an empty selector: any node fits, yet it is unbound"
echo "   pod-0 unbound (no node has its ABI); pod-f -> node-2, pod-g -> node-1, pod-h -> $(node_of pod-h)"
WHY="0/2 nodes are available: 2 node(s) didn't match Pod's node affinity/selector."
case "$(store_last /pods/default/pod-0)" in *'"type":"PodScheduled","status":"False","reason":"Unschedulable","message":"'"$WHY"'"'*) ;;
  *) fail "pod-0 lacks PodScheduled=False/Unschedulable: $(store_last /pods/default/pod-0)" ;; esac
case "$(store_last /events/default/pod-0.FailedScheduling.0)" in *'"reason":"FailedScheduling"'*"$WHY"*'"type":"Warning"'*) ;;
  *) fail "no FailedScheduling event for pod-0: $(store_last /events/default/pod-0.FailedScheduling.0)" ;; esac
echo "   pod-0: PodScheduled=False/Unschedulable, FailedScheduling: $WHY"

echo "== E2E green: the scheduler as params — a pick over the nodes, one bind per pass, a spread that emerges, nodeSelector fit, FailedScheduling =="
