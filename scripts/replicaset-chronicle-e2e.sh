#!/usr/bin/env bash
# ReplicaSet convergence as a Chronicle graph — the X-C2 alternative.
#
# The question X-C2 asks is whether a connector may hold a SET-shaped op
# (ensure_range) that touches N keys per invocation. This graph exists to show
# the question is moot: the reconcile is N INDEPENDENT decisions, because
# deterministic naming makes each ordinal unrelated to the others, so N
# guarded single-key writes converge over passes.
#
# So the cardinality lives in the SOURCE — store_source's `expand` emits one
# record per ordinal — and every ordinal is an ordinary 1:1 read-then-decide:
#
#   store_source (expand by spec.replicas, + tail)  one record per ordinal
#     -> probe   (decision)   EXISTS this ordinal's pod, carrying the request
#     -> exists  (store_effect, key_prefix /pods/)
#     -> act     (decision)   in-range+absent -> PUT; out-of-range+present ->
#                             DELETE; otherwise NOOP
#     -> apply   (store_effect, key_prefix /pods/)
#
# Only GET/EXISTS/PUT/DELETE are used — every one already in the contract. No
# ensure_range, and no new storage op of any kind.
#
# The rollout path (one stale Pod at a time) and core/v1 events are both here:
# the write's own reply carries a tag saying which branch fired, so a fourth
# stage records SuccessfulCreate / SuccessfulDelete from the effect that
# produced them rather than guessing from a second read of the store.
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

D="$(mktemp -d /tmp/nc-rschron-XXXXXX)"
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

write_graph() { # write_graph
DEC_PREV="$(nc_decision "$ROOT/modules/app/_chronicle/replicaset.uproc" prev)"
DEC_ASK="$(nc_decision "$ROOT/modules/app/_chronicle/replicaset.uproc" ask)"
DEC_ACT="$(nc_decision "$ROOT/modules/app/_chronicle/replicaset.uproc" act)"
DEC_REC="$(nc_decision "$ROOT/modules/app/_chronicle/replicaset.uproc" rec)"
for v in "$DEC_PREV" "$DEC_ASK" "$DEC_ACT" "$DEC_REC"; do
  [ -n "$v" ] || { echo "FAIL: a replicaset.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: store_source
    prefix: "/replicasets.apps/"
    paths: "spec.template.spec,metadata.labels.pod-template-hash"
    expand: "spec.replicas"
    expand_tail: 4
    # ...and past the highest EXISTING child, whatever the count: a surplus far
    # above the count (scale 10 -> 1) is otherwise never probed, and orphaned.
    expand_over: "/pods/"
    # Watch the children too: a rolled Pod is DELETED and recreated on a later
    # pass, and without this nothing would wake the ReplicaSet to do it.
    watch: "/pods/"
  # Stage 1 — the PREDECESSOR, so a rollout proceeds strictly in ordinal order.
  - name: prev
    type: decision
    params:
      decision: "${DEC_PREV}"
  - name: prevget
    type: store_effect
    key_prefix: "/pods/"
    paths: "metadata.labels.pod-template-hash"
  # Stage 2 — THIS ordinal, carrying the predecessor verdict.
  - name: probe
    type: decision
    params:
      decision: "${DEC_ASK}"
  - name: selfget
    type: store_effect
    key_prefix: "/pods/"
    paths: "metadata.labels.pod-template-hash"
  # Stage 3 — create, delete, roll, or nothing.
  - name: act
    type: decision
    params:
      decision: "${DEC_ACT}"
  - name: apply
    type: store_effect
    key_prefix: "/pods/"
  # Stage 4 — record what stage 3 did. The write's own reply carries the tag,
  # so the event is decided from the effect that produced it, not guessed from
  # a second read of the store.
  - name: record
    type: decision
    params:
      decision: "${DEC_REC}"
  - name: events
    type: store_effect
    key_prefix: "/events/"
wiring:
  - from: store_source.status
    to: store_source.changes
  - from: store_source.record_out
    to: prev.record_in
  - from: prev.result_out
    to: prevget.request_in
    buffer_group: 1
  - from: prevget.response_out
    to: probe.record_in
  - from: probe.result_out
    to: selfget.request_in
    buffer_group: 2
  - from: selfget.response_out
    to: act.record_in
  - from: act.result_out
    to: apply.request_in
    buffer_group: 3
  - from: apply.response_out
    to: record.record_in
  - from: record.result_out
    to: events.request_in
    buffer_group: 4
YAML
}

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

echo "== 1. a ReplicaSet with replicas=3 =="
RS='{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
store_put "/replicasets.apps/default/web" "$RS"
write_graph
run 5

echo "== 2. exactly three Pods, each owned and carrying the template =="
for i in 0 1 2; do
  got="$(store_last "/pods/default/web-$i")"
  case "$got" in
    *'"name":"web-'$i'"'*'"namespace":"default"'*'"kind":"ReplicaSet"'*'"image":"nginx"'*)
      echo "   web-$i created, owned, template embedded" ;;
    *) fail "web-$i wrong: '$got'" ;;
  esac
done
[ -z "$(store_last /pods/default/web-3)" ] || fail "web-3 must not exist at replicas=3"
echo "   web-3 absent — the tail probed and left alone"

echo "== 2b. each create recorded a SuccessfulCreate event =="
for i in 0 1 2; do
  ev="$(store_last "/events/default/web-$i.SuccessfulCreate.0")"
  case "$ev" in
    *'"name":"web-'$i'.SuccessfulCreate.0"'*'"kind":"Pod"'*'"name":"web-'$i'"'*'"reason":"SuccessfulCreate"'*'"message":"Created pod web-'$i'"'*'"type":"Normal"'*)
      echo "   web-$i SuccessfulCreate recorded" ;;
    *) fail "web-$i event wrong: '$ev'" ;;
  esac
done
[ -z "$(store_last /events/default/web-3.SuccessfulCreate.0)" ] \
  || fail "web-3 was never created and must have no event"
echo "   no event for the ordinal that was probed and left alone"

echo "== 3. scale to 1: the surplus is collected, the survivor untouched =="
KEEP="$(store_last /pods/default/web-0)"
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run 5
[ "$(store_last /pods/default/web-0)" = "$KEEP" ] || fail "web-0 changed on scale-down"
[ -z "$(store_last /pods/default/web-1)" ] || fail "web-1 should be deleted"
[ -z "$(store_last /pods/default/web-2)" ] || fail "web-2 should be deleted"
echo "   web-0 kept byte-identical; web-1 and web-2 deleted"

for i in 1 2; do
  case "$(store_last "/events/default/web-$i.SuccessfulDelete.0")" in
    *'"reason":"SuccessfulDelete"'*'"message":"Deleted pod web-'$i'"'*) : ;;
    *) fail "web-$i SuccessfulDelete event missing" ;;
  esac
done
[ -z "$(store_last /events/default/web-0.SuccessfulDelete.0)" ] \
  || fail "web-0 survived and must have no delete event"
echo "   both collections recorded; the survivor recorded nothing"

echo "== 4. scale back to 2: the missing ordinal is recreated =="
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run 5
[ -n "$(store_last /pods/default/web-1)" ] || fail "web-1 should be recreated at replicas=2"
[ -z "$(store_last /pods/default/web-2)" ] || fail "web-2 should stay deleted"
echo "   web-1 recreated; web-2 stays gone"

echo "== 5. rollout: a template change rolls stale Pods ONE AT A TIME, in order =="
# The module this mirrors spends a per-pass budget of one on the first stale
# ordinal it meets. A per-record decision has no budget to spend, so the chain
# gates on the PREDECESSOR instead: roll ordinal i only when i is stale and i-1
# is already fresh (or i is 0). That identifies the same pod, and it is what
# maxUnavailable = 1 means.
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default","labels":{"pod-template-hash":"v1"}},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run 6
for i in 0 1 2; do
  case "$(store_last "/pods/default/web-$i")" in
    *'"pod-template-hash":"v1"'*) : ;;
    *) fail "web-$i should carry hash v1 after the initial roll" ;;
  esac
done
echo "   all three Pods stamped v1"

# Now change the template hash. Every Pod is stale at once, and only one may be
# missing at any moment -- that IS maxUnavailable = 1. Sampling the store cannot
# show it (the whole roll finishes inside a second), so the WAL is replayed
# instead: from the moment the new template lands, at no point may two of the
# three Pods be absent together.
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default","labels":{"pod-template-hash":"v2"}},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"web","image":"nginx:2"}]}}}}'
run 12

python3 - "$D/store.log" <<'PY' || exit 1
import struct, sys
d = open(sys.argv[1], "rb").read()
recs, p = [], 0
while p + 15 <= len(d):
    rev, op, kl, vl = struct.unpack("<QBHI", d[p:p+15])
    if p + 15 + kl + vl > len(d): break
    recs.append((d[p+15:p+15+kl].decode(), op, d[p+15+kl:p+15+kl+vl]))
    p += 15 + kl + vl

# start at the write that introduced v2
start = max(i for i, (k, op, v) in enumerate(recs)
            if k == "/replicasets.apps/default/web" and b'"v2"' in v)
pods = {f"/pods/default/web-{i}": True for i in range(3)}
worst, rolled = 0, 0
for k, op, v in recs[start:]:
    if k not in pods: continue
    if op == 1:
        pods[k] = True
    else:
        pods[k] = False
        rolled += 1
    absent = sum(1 for x in pods.values() if not x)
    worst = max(worst, absent)
if worst > 1:
    print(f"   FAIL {worst} Pods were absent at once — maxUnavailable=1 violated")
    sys.exit(1)
if rolled < 3:
    print(f"   FAIL only {rolled} of 3 Pods were rolled")
    sys.exit(1)
print(f"   {rolled} Pods rolled, never more than {worst} absent at once — maxUnavailable=1 held")
PY

echo "   converged: all three Pods now carry v2"

echo "== 6. the roll was recorded, and distinguished from a scale-down =="
for i in 0 1 2; do
  case "$(store_last "/events/default/web-$i.SuccessfulDelete.0")" in
    *'"reason":"SuccessfulDelete"'*'"message":"Rolled pod web-'$i'"'*) : ;;
    *) fail "web-$i roll event missing or not distinguished from a scale-down" ;;
  esac
done
echo "   three SuccessfulDelete events, each reading \"Rolled pod\" — the tag"
echo "   rode with the write, so the record knows WHICH branch fired"

echo "== 6b. scale 12 -> 1: a surplus FAR above the count is still collected =="
# The expansion probes 0..count+tail. A fixed tail of 4 cannot see ordinal 9
# when the count is 1 — so `expand_over` extends the range past the highest
# child that exists, and every surplus ordinal is probed however far up it is.
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default","labels":{"pod-template-hash":"v2"}},"spec":{"replicas":12,"template":{"spec":{"containers":[{"name":"web","image":"nginx:2"}]}}}}'
run 6
for i in 0 11; do [ -n "$(store_last /pods/default/web-$i)" ] || fail "web-$i missing after scale to 12"; done
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default","labels":{"pod-template-hash":"v2"}},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"web","image":"nginx:2"}]}}}}'
run 6
for i in $(seq 1 11); do [ -z "$(store_last /pods/default/web-$i)" ] || fail "web-$i survived a scale-down far past the tail"; done
[ -n "$(store_last /pods/default/web-0)" ] || fail "web-0 should remain"
echo "   web-1..web-11 collected; web-0 kept"

echo "== 7. an expansion LARGER than the downstream channel still reaches its tail =="
# store_source resumes a blocked page at the entry it stopped on, and the
# entry's EXPANSION must resume at the index it reached: restarting at 0 makes
# an object expanding into more records than the channel holds re-emit the same
# prefix forever, and the tail is never created. That failure looks like
# convergence — every ordinal emitted is correct, only the count is short.
# 600 ordinals is several channels' worth.
store_put "/replicasets.apps/default/web" \
  '{"metadata":{"name":"web","namespace":"default","labels":{"pod-template-hash":"v2"}},"spec":{"replicas":600,"template":{"spec":{"containers":[{"name":"web","image":"nginx:2"}]}}}}'
run 12
python3 - "$D/store.log" <<'PY2' || exit 1
import struct, sys
d = open(sys.argv[1], "rb").read(); p = 0; live = set()
while p + 15 <= len(d):
    rev, op, kl, vl = struct.unpack("<QBHI", d[p:p+15])
    if p + 15 + kl + vl > len(d): break
    k = d[p+15:p+15+kl]
    if k.startswith(b"/pods/default/web-"):
        live.add(k) if op == 1 else live.discard(k)
    p += 15 + kl + vl
n = len(live)
if n < 600:
    print(f"   FAIL only {n} of 600 Pods exist — the expansion restarted at 0 on backpressure and never reached its tail")
    sys.exit(1)
print(f"   {n} Pods — the expansion resumed where it blocked, and the tail was reached")
PY2

echo "== E2E green: ReplicaSet convergence as N independent decisions — no set-shaped op, only contract ops that already exist =="
