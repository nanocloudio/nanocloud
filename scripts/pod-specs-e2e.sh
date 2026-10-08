#!/usr/bin/env bash
# A scheduled Pod as its node's ask, as params: the control plane projects
# /pods/<ns>/<name> bound to a node into the flat /pod-specs/<uid> record the
# node's pod-lifecycle chain runs, and a Pod deleted outright (its tomb under
# /~tomb/pods/) into desired=deleted. Each lands in its node's scope,
# /~node/<node>/pod-specs/<uid> (a worker's store names it /pod-specs/<uid>).
# Chains from modules/app/_chronicle/pod_specs.uproc.
#
#   spec   /pods/ (scheduled) -> /pod-specs/<uid>
#          node image restart grace desired cmd
#   gone   /~tomb/pods/        -> /pod-specs/<uid> desired=deleted
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-podspecs-XXXXXX)"
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

DEC_SPEC="$(nc_decision "$ROOT/modules/app/_chronicle/pod_specs.uproc" spec_p)"
DEC_GONE="$(nc_decision "$ROOT/modules/app/_chronicle/pod_specs.uproc" gone_p)"
for v in "$DEC_SPEC" "$DEC_GONE"; do
  [ -n "$v" ] || { echo "FAIL: a pod_specs.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: ps_pods
    type: store_source
    prefix: "/pods/"
    paths: "metadata.uid,spec.nodeName,spec.containers.0.command!jargv,spec.containers.0.args!jargv,spec.containers.0.image,spec.restartPolicy!lower,spec.terminationGracePeriodSeconds,metadata.deletionTimestamp,spec.runtimeClassName"
  - name: ps_spec
    type: store_effect
    decision: "${DEC_SPEC}"
    key_prefix: "/~node/"
  - name: ps_tombs
    type: store_source
    prefix: "/~tomb/pods/"
    paths: "obj.metadata.uid,obj.spec.nodeName,obj.spec.runtimeClassName,obj.spec.containers.0.image"
  - name: ps_gone
    type: store_effect
    decision: "${DEC_GONE}"
    key_prefix: "/~node/"
wiring:
  - from: ps_pods.status
    to: ps_pods.changes
  - from: ps_pods.record_out
    to: ps_spec.request_in
  - from: ps_tombs.status
    to: ps_tombs.changes
  - from: ps_tombs.record_out
    to: ps_gone.request_in
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
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== 1. four Pods: scheduled, unscheduled, being deleted; and one deleted outright =="
store_put "/pods/default/web" '{"metadata":{"name":"web","namespace":"default","uid":"uid-web"},"spec":{"nodeName":"node-a","restartPolicy":"OnFailure","terminationGracePeriodSeconds":5,"containers":[{"name":"web","image":"busybox:1.36","command":["sh","-c"],"args":["echo hi; sleep 9"]}]}}'
store_put "/pods/default/wait" '{"metadata":{"name":"wait","namespace":"default","uid":"uid-wait"},"spec":{"containers":[{"name":"wait","image":"busybox:1.36"}]}}'
store_put "/pods/default/bye" '{"metadata":{"name":"bye","namespace":"default","uid":"uid-bye","deletionTimestamp":"2026-10-03T00:00:00Z"},"spec":{"nodeName":"node-b","containers":[{"name":"bye","image":"busybox:1.36","command":["sleep","60"]}]}}'
store_put "/~tomb/pods/default/gone" '{"deadline":1,"obj":{"metadata":{"name":"gone","namespace":"default","uid":"uid-gone"},"spec":{"nodeName":"node-a"}}}'
run 5
WEB="$(store_last /~node/node-a/pod-specs/uid-web)"
[ "$WEB" = "node=node-a;image=busybox:1.36;restart=onfailure;grace=5;desired=running;cmd=sh -c echo%20hi%3B%20sleep%209;pod=default/web" ] \
  || fail "web's spec wrong: '$WEB'"
echo "   web:  argv kept its bounds (sh, -c, 'echo hi; sleep 9'), restartPolicy as the node spells it"
[ -z "$(store_last /~node//pod-specs/uid-wait)" ] || fail "an unscheduled Pod is no node's ask"
echo "   wait: unscheduled, no record"
BYE="$(store_last /~node/node-b/pod-specs/uid-bye)"
has "$BYE" "node=node-b" "grace=30" "desired=deleted" "cmd=sleep 60" || fail "bye's spec wrong: '$BYE'"
echo "   bye:  being deleted -> desired=deleted, the default 30 s grace"
GONE="$(store_last /~node/node-a/pod-specs/uid-gone)"
has "$GONE" "node=node-a" "grace=0" "desired=deleted" || fail "gone's spec wrong: '$GONE'"
echo "   gone: deleted outright (its tomb) -> desired=deleted, now"

echo "== 2. a pass with nothing changed spends no revision =="
W0="$(store_writes /~node/node-a/pod-specs/uid-web)"
run 3
[ "$(store_writes /~node/node-a/pod-specs/uid-web)" = "$W0" ] || fail "an unchanged Pod rewrote its spec"
echo "   uid-web: $W0 write(s), unchanged"

echo "== 3. the Pod's image changes; then it is marked for deletion =="
store_put "/pods/default/web" '{"metadata":{"name":"web","namespace":"default","uid":"uid-web"},"spec":{"nodeName":"node-a","restartPolicy":"OnFailure","terminationGracePeriodSeconds":5,"containers":[{"name":"web","image":"busybox:1.37","command":["sh","-c"],"args":["echo hi; sleep 9"]}]}}'
run 4
has "$(store_last /~node/node-a/pod-specs/uid-web)" "image=busybox:1.37" "desired=running" || fail "the new image did not reach the spec"
echo "   image=busybox:1.37"
store_put "/pods/default/web" '{"metadata":{"name":"web","namespace":"default","uid":"uid-web","deletionTimestamp":"2026-10-03T00:00:00Z"},"spec":{"nodeName":"node-a","restartPolicy":"OnFailure","terminationGracePeriodSeconds":5,"containers":[{"name":"web","image":"busybox:1.37","command":["sh","-c"],"args":["echo hi; sleep 9"]}]}}'
run 4
has "$(store_last /~node/node-a/pod-specs/uid-web)" "desired=deleted" "grace=5" || fail "the deletion did not reach the spec"
echo "   desired=deleted, its own 5 s grace"

echo "== E2E green: scheduled Pods projected to their node's ask, deletion carried, argv bounds kept =="
