#!/usr/bin/env bash
# Live E2E: Jobs as Kubernetes runs them, driven with kubectl against the
# packaged control plane (job.uproc, job_launch.uproc, job_status.uproc).
#
#   1. completions=2, parallelism=2: two Pods, labelled job-name; both
#      succeed -> `kubectl wait --for=condition=complete` returns; status
#      succeeded=2, startTime, completionTime; `kubectl get jobs` COMPLETIONS
#   2. backoffLimit=1: a failure, a 10 s backoff before the retry, a second
#      failure -> Failed/BackoffLimitExceeded (`kubectl wait
#      --for=condition=failed`), status.failed=2
#   3. activeDeadlineSeconds=4: still running at the deadline ->
#      Failed/DeadlineExceeded, its active Pod deleted
#   4. ttlSecondsAfterFinished=2: the Job (and its Pods) deleted after it
#      completes (`kubectl wait --for=delete`)
#   5. suspend: a Job created suspended starts nothing (Suspended=True);
#      resumed, it starts its Pod (Suspended=False, reason JobResumed)
#   6. Indexed, completions=2: <job>-0-0 and <job>-1-0, the completion index
#      their annotation; completedIndexes 0-1 once both succeed
#
# The node is not here: a Pod's phase is set through the status subresource,
# as its kubelet's report would set it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-job-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v MON_ || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

jpath() { k get "$@" 2>/dev/null; }
has_pods() { # has_pods <selector> <n>
  [ "$(k get pods -l "$1" -o name 2>/dev/null | wc -l)" -ge "$2" ]
}

echo "== 0. the control plane, as packaged, joined by its operator =="
kplane_up
echo "   joined"

echo "== 1. completions=2, parallelism=2 -> Complete; kubectl wait returns =="
cat >"$D/ok.yaml" <<'Y'
apiVersion: batch/v1
kind: Job
metadata: {name: ok}
spec:
  completions: 2
  parallelism: 2
  template:
    metadata: {labels: {app: ok}}
    spec:
      restartPolicy: Never
      containers: [{name: w, image: busybox, command: ["true"]}]
Y
k apply -f "$D/ok.yaml" >/dev/null || fail "apply job ok"
kplane_until 30 "two Pods for job ok" has_pods job-name=ok 2
PODS="$(k get pods -l job-name=ok -o jsonpath='{.items[*].metadata.name}')"
for p in $PODS; do
  [ "$(k get pod "$p" -o jsonpath='{.metadata.ownerReferences[0].kind}/{.metadata.labels.app}/{.metadata.labels.batch\.kubernetes\.io/job-name}')" = "Job/ok/ok" ] \
    || fail "pod $p is not the Job's: $(k get pod "$p" -o json | head -c 400)"
done
kplane_until 20 "status.active=2" sh -c "[ \"\$(kubectl --kubeconfig $KC get job ok -o jsonpath='{.status.active}')\" = 2 ]"
for p in $PODS; do kplane_pod_phase "$p" Succeeded; done
k wait --for=condition=complete job/ok --timeout=60s >/dev/null || fail "kubectl wait --for=condition=complete: $(k get job ok -o json | head -c 800)"
[ "$(k get job ok -o jsonpath='{.status.succeeded}/{.status.active}')" = "2/0" ] || fail "status counts: $(k get job ok -o jsonpath='{.status}')"
[ -n "$(k get job ok -o jsonpath='{.status.startTime}')" ] && [ -n "$(k get job ok -o jsonpath='{.status.completionTime}')" ] \
  || fail "startTime/completionTime: $(k get job ok -o jsonpath='{.status}')"
k get jobs ok --no-headers >"$D/get.out" || fail "kubectl get jobs"
grep -q "Complete" "$D/get.out" && grep -q "2/2" "$D/get.out" || fail "kubectl get jobs: $(cat "$D/get.out")"
echo "   Complete; succeeded=2; startTime, completionTime; get jobs: $(tr -s ' ' <"$D/get.out")"

echo "== 2. backoffLimit=1: fail, back off 10 s, fail again -> BackoffLimitExceeded =="
cat >"$D/bad.yaml" <<'Y'
apiVersion: batch/v1
kind: Job
metadata: {name: bad}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      containers: [{name: w, image: busybox, command: ["false"]}]
Y
k apply -f "$D/bad.yaml" >/dev/null || fail "apply job bad"
kplane_until 30 "a Pod for job bad" has_pods job-name=bad 1
P1="$(k get pods -l job-name=bad -o jsonpath='{.items[0].metadata.name}')"
kplane_pod_phase "$P1" Failed
T0=$(date +%s)
kplane_until 40 "the retry Pod for job bad" has_pods job-name=bad 2
T1=$(date +%s)
[ $((T1 - T0)) -ge 8 ] || fail "the retry came after $((T1 - T0)) s, inside the 10 s backoff"
P2="$(k get pods -l job-name=bad -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | grep -v "^$P1$")"
kplane_pod_phase "$P2" Failed
k wait --for=condition=failed job/bad --timeout=60s >/dev/null || fail "job bad never Failed: $(k get job bad -o jsonpath='{.status}')"
[ "$(k get job bad -o jsonpath='{.status.conditions[?(@.type=="Failed")].reason}/{.status.failed}')" = "BackoffLimitExceeded/2" ] \
  || fail "job bad: $(k get job bad -o jsonpath='{.status}')"
echo "   retried after $((T1 - T0)) s; Failed: BackoffLimitExceeded, failed=2"

echo "== 3. activeDeadlineSeconds=4 -> DeadlineExceeded, the active Pod deleted =="
cat >"$D/slow.yaml" <<'Y'
apiVersion: batch/v1
kind: Job
metadata: {name: slow}
spec:
  activeDeadlineSeconds: 4
  template:
    spec:
      restartPolicy: Never
      containers: [{name: w, image: busybox, command: ["sleep", "3600"]}]
Y
k apply -f "$D/slow.yaml" >/dev/null || fail "apply job slow"
kplane_until 30 "a Pod for job slow" has_pods job-name=slow 1
k wait --for=condition=failed job/slow --timeout=60s >/dev/null || fail "job slow never Failed: $(k get job slow -o jsonpath='{.status}')"
[ "$(k get job slow -o jsonpath='{.status.conditions[?(@.type=="Failed")].reason}')" = "DeadlineExceeded" ] \
  || fail "job slow: $(k get job slow -o jsonpath='{.status}')"
kplane_until 20 "the deadline's Pod deleted" sh -c "[ -z \"\$(kubectl --kubeconfig $KC get pods -l job-name=slow -o name)\" ]"
echo "   Failed: DeadlineExceeded; its Pod deleted"

echo "== 4. ttlSecondsAfterFinished=2: the finished Job is deleted =="
cat >"$D/ttl.yaml" <<'Y'
apiVersion: batch/v1
kind: Job
metadata: {name: ttl}
spec:
  ttlSecondsAfterFinished: 2
  template:
    spec:
      restartPolicy: Never
      containers: [{name: w, image: busybox, command: ["true"]}]
Y
k apply -f "$D/ttl.yaml" >/dev/null || fail "apply job ttl"
kplane_until 30 "a Pod for job ttl" has_pods job-name=ttl 1
kplane_pod_phase "$(k get pods -l job-name=ttl -o jsonpath='{.items[0].metadata.name}')" Succeeded
k wait --for=condition=complete job/ttl --timeout=60s >/dev/null || fail "job ttl never completed"
k wait --for=delete job/ttl --timeout=30s >/dev/null || fail "job ttl not deleted after its TTL"
kplane_until 20 "job ttl's Pods collected" sh -c "[ -z \"\$(kubectl --kubeconfig $KC get pods -l job-name=ttl -o name)\" ]"
echo "   completed, then deleted with its Pods"

echo "== 5. suspend: nothing starts; resumed, it runs =="
cat >"$D/sus.yaml" <<'Y'
apiVersion: batch/v1
kind: Job
metadata: {name: sus}
spec:
  suspend: true
  template:
    spec:
      restartPolicy: Never
      containers: [{name: w, image: busybox, command: ["true"]}]
Y
k apply -f "$D/sus.yaml" >/dev/null || fail "apply job sus"
k wait --for=condition=suspended job/sus --timeout=30s >/dev/null || fail "job sus not Suspended: $(k get job sus -o jsonpath='{.status}')"
sleep 3
[ -z "$(k get pods -l job-name=sus -o name)" ] || fail "a suspended Job started a Pod"
k patch job sus --type=merge -p '{"spec":{"suspend":false}}' >/dev/null || fail "resume"
kplane_until 30 "the resumed Job's Pod" has_pods job-name=sus 1
kplane_until 20 "Suspended=False JobResumed" sh -c "[ \"\$(kubectl --kubeconfig $KC get job sus -o jsonpath='{.status.conditions[?(@.type==\"Suspended\")].status}/{.status.conditions[?(@.type==\"Suspended\")].reason}')\" = False/JobResumed ]"
echo "   Suspended=True, no Pod; resumed: a Pod, Suspended=False/JobResumed"

echo "== 6. Indexed, completions=2: one Pod per index; completedIndexes =="
cat >"$D/idx.yaml" <<'Y'
apiVersion: batch/v1
kind: Job
metadata: {name: idx}
spec:
  completions: 2
  parallelism: 2
  completionMode: Indexed
  template:
    spec:
      restartPolicy: Never
      containers: [{name: w, image: busybox, command: ["true"]}]
Y
k apply -f "$D/idx.yaml" >/dev/null || fail "apply job idx"
kplane_until 40 "two indexed Pods" has_pods job-name=idx 2
for i in 0 1; do
  [ "$(k get pod "idx-$i-0" -o jsonpath='{.metadata.annotations.batch\.kubernetes\.io/job-completion-index}')" = "$i" ] \
    || fail "idx-$i-0 lacks its completion index"
  kplane_pod_phase "idx-$i-0" Succeeded
done
k wait --for=condition=complete job/idx --timeout=60s >/dev/null || fail "job idx never completed"
[ "$(k get job idx -o jsonpath='{.status.completedIndexes}')" = "0-1" ] || fail "completedIndexes: $(k get job idx -o jsonpath='{.status}')"
echo "   idx-0-0, idx-1-0 (annotated); Complete, completedIndexes=0-1"

echo "== E2E green: Jobs to Kubernetes' semantics, driven by kubectl =="
