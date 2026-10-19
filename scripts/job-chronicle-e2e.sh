#!/usr/bin/env bash
# Job as a Chronicle graph — the production chains (jb_*, gc_job_*),
# extracted from fluxor-controlplane.yaml, on a store seeded directly:
#
#   seed    a Job -> its state record /job-ctl/<ns>/<job>
#   ctl     the Job's Pods counted -> how many to start, the terminal state
#   launch  NonIndexed <job>-<i>, at most `parallelism` at once, never past
#           the completions still wanted; Indexed <job>-<i>-<attempt>, the
#           completion index as the Pod's annotation
#   status  the Job's .status: counts, startTime, completionTime, Complete
#   kill    a suspended Job's active Pod deleted
#   gc      a deleted Job's Pods collected
#
# The node is not here: a Pod's phase is written into the store between
# runs, as its node's report would set it. The API-driven behaviours
# (backoff, deadline, TTL, suspend/resume, `kubectl wait`) are
# scripts/job-e2e.sh's.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_scan store_effect store_stamped store_big; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-jobchron-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
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
# A Pod's phase set, as its node's report would (the Pod read, its status
# replaced) — the Pod keeps its identity.
pod_phase() { # pod_phase <ns/pod> <phase>
  local v; v="$(store_last "/pods/$1")"
  [ -n "$v" ] || fail "no Pod $1 to set $2 on"
  store_put "/pods/$1" "$(python3 -c 'import json,sys; o=json.loads(sys.argv[1]); o["status"]={"phase":sys.argv[2]}; print(json.dumps(o,separators=(",",":")))' "$v" "$2")"
}
jq_() { python3 -c 'import json,sys; o=json.loads(sys.argv[1] or "null"); print(eval(sys.argv[2]))' "$1" "$2"; }

python3 "$ROOT/scripts/extract-fixture.py" "$D/graph.yaml" - jb_ gc_job_ >/dev/null
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
UID1="11111111-1111-1111-1111-111111111111"
UID2="22222222-2222-2222-2222-222222222222"
UID3="33333333-3333-3333-3333-333333333333"

echo "== 1. completions=3, parallelism=2: two Pods, labelled and owned =="
store_put "/jobs.batch/default/backup" '{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"backup","namespace":"default","uid":"'$UID1'"},"spec":{"completions":3,"parallelism":2,"template":{"metadata":{"labels":{"app":"bk"}},"spec":{"restartPolicy":"Never","containers":[{"name":"backup","image":"busybox"}]}}}}'
run 8
P0="$(store_last /pods/default/backup-0)"
case "$P0" in *'"kind":"Job"'*'"uid":"'$UID1'"'*) ;; *) fail "backup-0 not owned by the Job: $P0" ;; esac
[ "$(jq_ "$P0" 'sorted(o["metadata"]["labels"].items())')" = "[('app', 'bk'), ('batch.kubernetes.io/controller-uid', '$UID1'), ('batch.kubernetes.io/job-name', 'backup'), ('controller-uid', '$UID1'), ('job-name', 'backup')]" ] || fail "backup-0 lacks its labels: $P0"
[ "$(jq_ "$P0" 'bool(o["metadata"].get("uid")) and bool(o["metadata"].get("creationTimestamp"))')" = True ] || fail "backup-0 was not given a uid and creationTimestamp: $P0"
[ -n "$(store_last /pods/default/backup-1)" ] || fail "backup-1 not launched"
[ -z "$(store_last /pods/default/backup-2)" ] || fail "backup-2 started past parallelism 2"
ST="$(store_last /jobs.batch/default/backup)"
[ "$(jq_ "$ST" 'o["status"]["active"]')" = 2 ] || fail "status.active: $ST"
[ "$(jq_ "$ST" 'o["status"]["startTime"] is not None')" = True ] || fail "no startTime: $ST"
echo "   backup-0, backup-1; status.active=2, startTime set"

echo "== 2. one succeeds: the third starts (never past completions) =="
pod_phase default/backup-0 Succeeded
run 8
[ -n "$(store_last /pods/default/backup-2)" ] || fail "backup-2 not launched after a success"
[ -z "$(store_last /pods/default/backup-3)" ] || fail "backup-3 started past the completions wanted"
ST="$(store_last /jobs.batch/default/backup)"
[ "$(jq_ "$ST" '(o["status"]["active"], o["status"]["succeeded"])')" = "(2, 1)" ] || fail "status: $ST"
echo "   backup-2 started; active=2 succeeded=1"

echo "== 3. all succeed: Complete (and SuccessCriteriaMet), completionTime =="
pod_phase default/backup-1 Succeeded
pod_phase default/backup-2 Succeeded
run 8
ST="$(store_last /jobs.batch/default/backup)"
[ "$(jq_ "$ST" '[c["type"] for c in o["status"]["conditions"] if c["status"]=="True"]')" = "['SuccessCriteriaMet', 'Complete']" ] || fail "conditions: $ST"
[ "$(jq_ "$ST" 'o["status"]["succeeded"]')" = 3 ] || fail "succeeded: $ST"
[ "$(jq_ "$ST" 'o["status"]["completionTime"] is not None')" = True ] || fail "no completionTime: $ST"
[ -z "$(store_last /pods/default/backup-3)" ] || fail "a Pod started after completion"
echo "   Complete; succeeded=3; completionTime set"

echo "== 4. Indexed, completions=3, parallelism=2: <job>-<i>-0 for the lowest indexes =="
store_put "/jobs.batch/default/idx" '{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"idx","namespace":"default","uid":"'$UID2'"},"spec":{"completions":3,"parallelism":2,"completionMode":"Indexed","template":{"spec":{"restartPolicy":"Never","containers":[{"name":"w","image":"busybox"}]}}}}'
run 10
I0="$(store_last /pods/default/idx-0-0)"
case "$I0" in *'"batch.kubernetes.io/job-completion-index":"0"'*) ;; *) fail "idx-0-0 missing or without its index: $I0" ;; esac
[ -n "$(store_last /pods/default/idx-1-0)" ] || fail "idx-1-0 not launched"
[ -z "$(store_last /pods/default/idx-2-0)" ] || fail "idx-2-0 started past parallelism"
echo "   idx-0-0, idx-1-0 (index annotations set)"

echo "== 5. index 0 succeeds, index 1 fails: the failure's backoff holds every start =="
pod_phase default/idx-0-0 Succeeded
pod_phase default/idx-1-0 Failed
run 8
# Upstream holds the Job's new Pods for the backoff (10 s after one failure).
[ -z "$(store_last /pods/default/idx-1-1)" ] || fail "idx-1-1 started inside its backoff"
[ -z "$(store_last /pods/default/idx-2-0)" ] || fail "idx-2-0 started inside the backoff"
sleep 4
run 8
[ -n "$(store_last /pods/default/idx-1-1)" ] || fail "idx-1-1 (index 1's second attempt) not started after the backoff"
[ -n "$(store_last /pods/default/idx-2-0)" ] || fail "idx-2-0 not started after the backoff"
echo "   held 10 s; then idx-1-1 (attempt 2 of index 1) and idx-2-0"

echo "== 6. both succeed: Complete, completedIndexes 0-2 =="
pod_phase default/idx-1-1 Succeeded
pod_phase default/idx-2-0 Succeeded
run 8
ST="$(store_last /jobs.batch/default/idx)"
[ "$(jq_ "$ST" 'o["status"]["completedIndexes"]')" = "0-2" ] || fail "completedIndexes: $ST"
[ "$(jq_ "$ST" '(o["status"]["succeeded"], o["status"]["failed"])')" = "(3, 1)" ] || fail "counts: $ST"
echo "   Complete; completedIndexes=0-2; succeeded=3 failed=1"

echo "== 7. suspend: the active Pod is deleted, Suspended=True, nothing starts =="
store_put "/jobs.batch/default/sus" '{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"sus","namespace":"default","uid":"'$UID3'"},"spec":{"completions":2,"template":{"spec":{"restartPolicy":"Never","containers":[{"name":"w","image":"busybox"}]}}}}'
run 8
[ -n "$(store_last /pods/default/sus-0)" ] || fail "sus-0 not launched"
store_put "/jobs.batch/default/sus" "$(python3 -c 'import json,sys; o=json.loads(sys.argv[1]); o["spec"]["suspend"]=True; print(json.dumps(o,separators=(",",":")))' "$(store_last /jobs.batch/default/sus)")"
run 8
[ -z "$(store_last /pods/default/sus-0)" ] || fail "sus-0 not deleted on suspend"
[ -n "$(store_last /~tomb/pods/default/sus-0)" ] || fail "the suspended Pod left no tomb"
ST="$(store_last /jobs.batch/default/sus)"
[ "$(jq_ "$ST" '[(c["type"],c["status"],c["reason"]) for c in o["status"]["conditions"]]')" = "[('Suspended', 'True', 'JobSuspended')]" ] || fail "suspend conditions: $ST"
echo "   sus-0 deleted (tomb left); Suspended=True"

echo "== 8. a deleted Job's Pods are collected =="
python3 - "$D/store.log" <<'PY'
import struct, os, sys
path, key = sys.argv[1], b"/jobs.batch/default/backup"
data = open(path, "rb").read(); p = last = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15]); last, p = rev, p + 15 + kl + vl
with open(path, "ab") as f:
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
PY
run 8
[ -z "$(store_last /pods/default/backup-0)" ] && [ -z "$(store_last /pods/default/backup-2)" ] || fail "the deleted Job's Pods were not collected"
[ -z "$(store_last /job-ctl/default/backup)" ] || fail "the deleted Job's state stayed"
echo "   backup-0..2 collected, /job-ctl/ dropped"

echo "== 9. a REAL template (> 4 KiB), NonIndexed and Indexed: the Pods carry it whole, the Jobs complete =="
# env, ports, resources, annotations: past a source's 512 B field, the
# decisions' 512 B scratch and a record. The chain carries names and the
# uid; the writer splices the template from the stored Job.
bigjob() { # bigjob <name> <uid> <mode>
  python3 - "$1" "$2" "$3" <<'PYB'
import json, sys
name, uid, mode = sys.argv[1:]
env = [{"name": "JOB_SETTING_%02d" % i, "value": "value-%s-%02d-abcdefghijklmnopqrstuvwxyz" % (name, i)} for i in range(48)]
tmpl = {"metadata": {"labels": {"app": name, "tier": "batch"},
                     "annotations": {"example.com/note-%d" % i: "annotation %d of %s" % (i, name) for i in range(8)}},
        "spec": {"restartPolicy": "Never",
                 "containers": [{"name": "work", "image": "busybox", "env": env,
                                 "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}}}]}}
assert len(json.dumps(tmpl, separators=(",", ":"))) >= 4096
spec = {"completions": 2, "parallelism": 2, "template": tmpl}
if mode == "Indexed":
    spec["completionMode"] = "Indexed"
print(json.dumps({"apiVersion": "batch/v1", "kind": "Job",
                  "metadata": {"name": name, "namespace": "default", "uid": uid}, "spec": spec},
                 separators=(",", ":")))
PYB
}
UID4="44444444-4444-4444-4444-444444444444"
UID5="55555555-5555-5555-5555-555555555555"
store_put "/jobs.batch/default/bigjob" "$(bigjob bigjob $UID4 NonIndexed)"
store_put "/jobs.batch/default/bigidx" "$(bigjob bigidx $UID5 Indexed)"
run 10
whole() { # whole <pod-json> <job> <uid> <index or "">
  python3 - "$1" "$2" "$3" "$4" <<'PYW'
import json, sys
p, job, uid, idx = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
m = p["metadata"]
want = {"app": job, "tier": "batch", "batch.kubernetes.io/controller-uid": uid,
        "batch.kubernetes.io/job-name": job, "controller-uid": uid, "job-name": job}
ann = {"example.com/note-%d" % i: "annotation %d of %s" % (i, job) for i in range(8)}
if idx:
    want["batch.kubernetes.io/job-completion-index"] = idx
    ann["batch.kubernetes.io/job-completion-index"] = idx
assert m["labels"] == want, m["labels"]
assert m["annotations"] == ann, m["annotations"]
assert m["ownerReferences"][0]["uid"] == uid and m["ownerReferences"][0]["kind"] == "Job"
c = p["spec"]["containers"][0]
assert len(c["env"]) == 48 and c["env"][47]["value"].startswith("value-" + job), c["env"][-1]
assert c["resources"]["requests"]["cpu"] == "100m" and p["spec"]["restartPolicy"] == "Never"
PYW
}
for i in 0 1; do
  whole "$(store_last /pods/default/bigjob-$i)" bigjob $UID4 "" || fail "bigjob-$i is not the whole template: $(store_last /pods/default/bigjob-$i | head -c 400)"
  whole "$(store_last /pods/default/bigidx-$i-0)" bigidx $UID5 "$i" || fail "bigidx-$i-0 is not the whole template: $(store_last /pods/default/bigidx-$i-0 | head -c 400)"
done
[ "$(jq_ "$(store_last /jobs.batch/default/bigjob)" 'o["status"]["active"]')" = 2 ] || fail "bigjob status.active: $(store_last /jobs.batch/default/bigjob | tail -c 300)"
echo "   bigjob-0/1, bigidx-0-0/1-0: the whole template, its labels and annotations beside upstream's; status.active=2 on the large Job"
for p in bigjob-0 bigjob-1 bigidx-0-0 bigidx-1-0; do pod_phase default/$p Succeeded; done
run 8
for j in bigjob bigidx; do
  ST="$(store_last /jobs.batch/default/$j)"
  [ "$(jq_ "$ST" '[c["type"] for c in o["status"]["conditions"] if c["status"]=="True"]')" = "['SuccessCriteriaMet', 'Complete']" ] || fail "$j not Complete: $(printf '%s' "$ST" | tail -c 400)"
  [ "$(jq_ "$ST" 'o["status"]["succeeded"]')" = 2 ] || fail "$j succeeded: $(printf '%s' "$ST" | tail -c 300)"
  [ "$(jq_ "$ST" 'len(o["spec"]["template"]["spec"]["containers"][0]["env"])')" = 48 ] || fail "$j's template was cut by its status patch"
done
[ "$(jq_ "$(store_last /jobs.batch/default/bigidx)" 'o["status"]["completedIndexes"]')" = "0-1" ] || fail "bigidx completedIndexes"
echo "   bigjob, bigidx Complete (succeeded=2; completedIndexes 0-1), their templates intact"
UID6="66666666-6666-6666-6666-666666666666"
store_put "/jobs.batch/default/bigsus" "$(bigjob bigsus $UID6 NonIndexed)"
run 8
[ -n "$(store_last /pods/default/bigsus-0)" ] || fail "bigsus-0 not launched"
store_put "/jobs.batch/default/bigsus" "$(python3 -c 'import json,sys; o=json.loads(sys.argv[1]); o["spec"]["suspend"]=True; print(json.dumps(o,separators=(",",":")))' "$(store_last /jobs.batch/default/bigsus)")"
run 8
[ -z "$(store_last /pods/default/bigsus-0)" ] || fail "bigsus-0 (past 4 KiB) not deleted on suspend"
[ "$(jq_ "$(store_last /~tomb/pods/default/bigsus-0)" 'len(o["obj"]["spec"]["containers"][0]["env"])')" = 48 ] || fail "the suspended large Pod left no whole tomb"
[ "$(jq_ "$(store_last /~tomb-at/pods/default/bigsus-0)" 'o["obj"]["metadata"]["uid"] != ""')" = True ] || fail "the large Pod's tomb has no index entry (its node would never learn)"
echo "   bigsus suspended: bigsus-0 (past 4 KiB) deleted, its whole tomb and its index entry left"

echo "== E2E green: Job as params — parallelism, completions, Indexed, backoff, suspend, status, collection =="
