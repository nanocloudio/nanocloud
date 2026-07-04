#!/usr/bin/env bash
# Job as a Chronicle graph: run-to-completion, as params. Three chains:
#   launch  /jobs.batch/ expanded by spec.completions -> create-if-absent,
#           never recreated
#   done    /pods/ owned by a Job and Succeeded -> /job-done/<ns>/<pod>
#   status  /jobs.batch/ plain, count_children over /job-done/ vs desired =
#           spec.completions -> /job-status/<ns>/<job>
# "How many of mine succeeded" is a per-object fold; the source performs it
# (count_children) and the decision states it.
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

D="$(mktemp -d /tmp/nc-jobchron-XXXXXX)"
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

DEC_PRB="$(nc_decision "$ROOT/modules/app/_chronicle/job.uproc" prb)"
DEC_MK="$(nc_decision "$ROOT/modules/app/_chronicle/job.uproc" mk)"
DEC_DN="$(nc_decision "$ROOT/modules/app/_chronicle/job.uproc" dn)"
DEC_ST="$(nc_decision "$ROOT/modules/app/_chronicle/job.uproc" st)"
for v in "$DEC_PRB" "$DEC_MK" "$DEC_DN" "$DEC_ST"; do
  [ -n "$v" ] || { echo "FAIL: a job.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: jobs
    type: store_source
    prefix: "/jobs.batch/"
    paths: "spec.template.spec,spec.completions"
    expand: "spec.completions"
    expand_tail: 0
    watch: "/pods/"
  - name: probe
    type: decision
    params:
      decision: "${DEC_PRB}"
  - name: exists
    type: store_effect
    key_prefix: "/pods/"
  - name: launch
    type: decision
    params:
      decision: "${DEC_MK}"
  - name: apply
    type: store_effect
    key_prefix: "/pods/"
  - name: pods
    type: store_source
    prefix: "/pods/"
    paths: "metadata.ownerReferences.0.kind,status.phase"
  - name: done
    type: decision
    params:
      decision: "${DEC_DN}"
  - name: mark
    type: store_effect
    key_prefix: "/job-done/"
  - name: jobstat
    type: store_source
    prefix: "/jobs.batch/"
    paths: "spec.completions"
    desired: "spec.completions"
    count_children: "/job-done/"
    watch: "/job-done/"
  - name: status
    type: decision
    params:
      decision: "${DEC_ST}"
  - name: write
    type: store_effect
    key_prefix: "/job-status/"
wiring:
  - from: jobs.status
    to: jobs.changes
  - from: jobs.record_out
    to: probe.record_in
  - from: probe.result_out
    to: exists.request_in
    buffer_group: 1
  - from: exists.response_out
    to: launch.record_in
  - from: launch.result_out
    to: apply.request_in
    buffer_group: 2
  - from: pods.status
    to: pods.changes
  - from: pods.record_out
    to: done.record_in
  - from: done.result_out
    to: mark.request_in
    buffer_group: 3
  - from: jobstat.status
    to: jobstat.changes
  - from: jobstat.record_out
    to: status.record_in
  - from: status.result_out
    to: write.request_in
    buffer_group: 4
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

echo "== 1. completions=2: two Pods launched, status incomplete =="
store_put "/jobs.batch/default/backup" '{"metadata":{"name":"backup"},"spec":{"completions":2,"template":{"spec":{"containers":[{"name":"backup","image":"busybox"}]}}}}'
run 6
has "$(store_last /pods/default/backup-0)" '"kind":"Job"' '"name":"backup"' '"image":"busybox"' '"namespace":"default"' || fail "backup-0 wrong: $(store_last /pods/default/backup-0)"
has "$(store_last /pods/default/backup-1)" '"image":"busybox"' || fail "backup-1 not launched"
[ -z "$(store_last /pods/default/backup-2)" ] || fail "backup-2 must not exist"
[ "$(store_last /job-status/default/backup)" = "succeeded=0;complete=0" ] || fail "status: '$(store_last /job-status/default/backup)'"
echo "   backup-0, backup-1 launched; succeeded=0;complete=0"

echo "== 2. one Pod succeeds: succeeded=1, still incomplete =="
store_put "/pods/default/backup-0" '{"metadata":{"name":"backup-0","namespace":"default","ownerReferences":[{"kind":"Job","name":"backup"}]},"spec":{"containers":[{"image":"busybox"}]},"status":{"phase":"Succeeded"}}'
run 6
[ "$(store_last /job-done/default/backup-0)" = "1" ] || fail "backup-0 not marked done"
[ "$(store_last /job-status/default/backup)" = "succeeded=1;complete=0" ] || fail "status: '$(store_last /job-status/default/backup)'"
echo "   succeeded=1;complete=0"

echo "== 3. both succeed: complete — and the Succeeded Pod is NOT recreated =="
store_put "/pods/default/backup-1" '{"metadata":{"name":"backup-1","namespace":"default","ownerReferences":[{"kind":"Job","name":"backup"}]},"spec":{"containers":[{"image":"busybox"}]},"status":{"phase":"Succeeded"}}'
run 6
[ "$(store_last /job-status/default/backup)" = "succeeded=2;complete=1" ] || fail "status: '$(store_last /job-status/default/backup)'"
has "$(store_last /pods/default/backup-0)" '"phase":"Succeeded"' || fail "backup-0 lost its Succeeded phase (recreated?)"
echo "   succeeded=2;complete=1; Pods keep their phase"

echo "== 4. a Job with no completions is not projected =="
store_put "/jobs.batch/default/bare" '{"metadata":{"name":"bare"},"spec":{"template":{"spec":{}}}}'
run 4
[ -z "$(store_last /pods/default/bare-0)" ] && [ -z "$(store_last /job-status/default/bare)" ] || fail "bare must project nothing"
echo "   bare: nothing"

echo "== E2E green: Job as params — launch once, the per-pod done fact, the fold as a source count =="
