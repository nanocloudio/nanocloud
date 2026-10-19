#!/usr/bin/env bash
# Live E2E: CronJobs as Kubernetes runs them, driven with kubectl against the
# packaged control plane (cronjob.uproc, cronjob_status.uproc; the schedule
# judged by store_source `cron` against the trusted wall clock). Real minutes
# pass: about three.
#
#   tick    "* * * * *", Allow, successfulJobsHistoryLimit 1: a Job each
#           minute, owned, named <cj>-<minutes>, annotated with its
#           scheduled time; status.active, lastScheduleTime and (once its
#           Pod succeeds) lastSuccessfulTime; the older success pruned
#   forbid  Forbid: no second Job while the first is active
#   repl    Replace: the active Job deleted for the next
#   tz      "* <H> * * *" with timeZone Etc/GMT-2, H the hour there now:
#           runs (in UTC it would not)
#   sus     suspended: nothing runs; resumed, it runs
#   bigc    a jobTemplate past 4 KiB: its Job and Pod carry it whole, the
#           Job completes, the CronJob's status records the success
# `kubectl get cronjobs` prints them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-cron-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v "MON_\|kvw\]\|port 53\|cluster_dns" || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

jobs_of() { k get jobs -o jsonpath="{range .items[?(@.metadata.ownerReferences[0].name==\"$1\")]}{.metadata.name}{\"\\n\"}{end}" 2>/dev/null | sort; }
njobs() { jobs_of "$1" | grep -c . || true; }
has_jobs() { [ "$(njobs "$1")" -ge "$2" ]; }
succeed_job() { # every Pod of the Job succeeds
  local p
  for p in $(k get pods -l "job-name=$1" -o jsonpath='{.items[*].metadata.name}'); do
    kplane_pod_phase "$p" Succeeded
  done
}
cron() { # cron <name> <schedule> <policy> [extra spec members]
  printf '%s' "{\"apiVersion\":\"batch/v1\",\"kind\":\"CronJob\",\"metadata\":{\"name\":\"$1\"},\"spec\":{\"schedule\":\"$2\",\"concurrencyPolicy\":\"$3\"${4:-},\"jobTemplate\":{\"metadata\":{\"labels\":{\"cron\":\"$1\"}},\"spec\":{\"template\":{\"spec\":{\"restartPolicy\":\"Never\",\"containers\":[{\"name\":\"c\",\"image\":\"busybox\",\"command\":[\"true\"]}]}}}}}}" \
    | k apply -f - >/dev/null || fail "apply cronjob $1"
}

echo "== 0. the control plane, joined; six CronJobs =="
kplane_up
H=$(( ($(date -u +%-H) + 2) % 24 ))
# Created well inside a minute, so the first boundary is the same for all.
while [ "$(date +%-S)" -gt 40 ]; do sleep 1; done
cron tick "* * * * *" Allow ',"successfulJobsHistoryLimit":1'
cron forbid "* * * * *" Forbid
cron repl "* * * * *" Replace
cron tz "* $H * * *" Allow ',"timeZone":"Etc/GMT-2"'
cron sus "* * * * *" Allow ',"suspend":true'
# bigc: a jobTemplate past 4 KiB (env, resources, labels, annotations) — past
# a source's 512 B field, the decisions' scratch and a record. The chain
# carries names and times; the writers splice the template.
python3 - >"$D/bigc.json" <<'PYB'
import json
env = [{"name": "CRON_SETTING_%02d" % i, "value": "value-%02d-abcdefghijklmnopqrstuvwxyz0123456789" % i} for i in range(48)]
jt = {"metadata": {"labels": {"cron": "bigc", "tier": "batch"},
                   "annotations": {"example.com/note-%d" % i: "annotation %d of bigc" % i for i in range(8)}},
      "spec": {"template": {"metadata": {"labels": {"app": "bigc"},
                                         "annotations": {"example.com/pod-note": "a pod annotation"}},
                            "spec": {"restartPolicy": "Never",
                                     "containers": [{"name": "c", "image": "busybox", "command": ["true"], "env": env,
                                                     "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}}}]}}}}
assert len(json.dumps(jt, separators=(",", ":"))) >= 4096
print(json.dumps({"apiVersion": "batch/v1", "kind": "CronJob", "metadata": {"name": "bigc"},
                  "spec": {"schedule": "* * * * *", "concurrencyPolicy": "Forbid", "successfulJobsHistoryLimit": 1,
                           "jobTemplate": jt}}, separators=(",", ":")))
PYB
k apply -f "$D/bigc.json" >/dev/null || fail "apply cronjob bigc ($(wc -c <"$D/bigc.json") bytes)"
k get cronjobs --no-headers >"$D/get.out" || fail "kubectl get cronjobs"
grep -q "^tz .*Etc/GMT-2" "$D/get.out" && grep -q "^sus .*true" "$D/get.out" || fail "kubectl get cronjobs: $(cat "$D/get.out")"
echo "   tick, forbid, repl, tz (* $H * * * in Etc/GMT-2), sus (suspended)"

echo "== 1. the first minute: a Job each (not sus), owned, annotated =="
kplane_until 90 "tick's first Job" has_jobs tick 1
for c in forbid repl tz; do kplane_until 20 "$c's first Job" has_jobs "$c" 1; done
T1="$(jobs_of tick | head -1)"
case "$T1" in tick-[0-9]*) ;; *) fail "tick's Job is named '$T1'" ;; esac
[ "$(k get job "$T1" -o jsonpath='{.metadata.ownerReferences[0].kind}/{.metadata.labels.cron}')" = "CronJob/tick" ] || fail "$T1 is not tick's"
ANN="$(k get job "$T1" -o jsonpath='{.metadata.annotations.batch\.kubernetes\.io/cronjob-scheduled-timestamp}')"
case "$ANN" in 20[0-9][0-9]-*T*:*:00Z) ;; *) fail "$T1's scheduled timestamp: '$ANN'" ;; esac
[ "$(( ${T1#tick-} * 60 ))" = "$(date -u -d "$ANN" +%s)" ] || fail "$T1's name is not its scheduled minute ($ANN)"
kplane_until 20 "tick status: active, lastScheduleTime" sh -c "[ \"\$(kubectl --kubeconfig $KC get cronjob tick -o jsonpath='{.status.active[0].name}|{.status.lastScheduleTime}')\" = \"$T1|$ANN\" ]"
[ "$(njobs sus)" = 0 ] || fail "a suspended CronJob ran"
echo "   $T1 (scheduled $ANN), forbid/repl/tz ran; sus did not; tick.status.active=[$T1]"

echo "== 2. success: lastSuccessfulTime, the active list empties =="
succeed_job "$T1"
kplane_until 30 "$T1 complete" sh -c "kubectl --kubeconfig $KC get job $T1 -o jsonpath='{.status.conditions[?(@.type==\"Complete\")].status}' | grep -qx True"
kplane_until 30 "tick lastSuccessfulTime, active empty" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get cronjob tick -o jsonpath='{.status.lastSuccessfulTime}')\" ] && [ -z \"\$(kubectl --kubeconfig $KC get cronjob tick -o jsonpath='{.status.active}')\" ]"
k patch cronjob sus --type=merge -p '{"spec":{"suspend":false}}' >/dev/null || fail "resume sus"
echo "   lastSuccessfulTime=$(k get cronjob tick -o jsonpath='{.status.lastSuccessfulTime}'); sus resumed"

echo "== 2b. bigc (a jobTemplate past 4 KiB): its Job and Pod carry the template whole; Complete; lastSuccessfulTime =="
kplane_until 20 "bigc's first Job" has_jobs bigc 1
B1="$(jobs_of bigc | head -1)"
k get job "$B1" -o json >"$D/b1.json" || fail "get job $B1"
kplane_until 20 "$B1's Pod" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get pods -l job-name=$B1 -o name)\" ]"
k get pods -l "job-name=$B1" -o json >"$D/b1pods.json" || fail "get $B1's pods"
python3 - "$D/bigc.json" "$D/b1.json" "$D/b1pods.json" <<'PYC' || fail "bigc's Job or Pod is not its template whole"
import json, sys
cj, j, pods = (json.load(open(f)) for f in sys.argv[1:])
jt = cj["spec"]["jobTemplate"]
m = j["metadata"]
assert m["labels"] == jt["metadata"]["labels"], m["labels"]
ann = dict(m["annotations"]); ts = ann.pop("batch.kubernetes.io/cronjob-scheduled-timestamp")
assert ts.endswith(":00Z") and ann == jt["metadata"]["annotations"], m["annotations"]
assert m["ownerReferences"][0]["kind"] == "CronJob" and m["ownerReferences"][0]["name"] == "bigc"
assert j["spec"]["template"] == jt["spec"]["template"], "the Job's template differs"
(p,) = pods["items"]
c = p["spec"]["containers"][0]
assert len(c["env"]) == 48 and c["env"] == jt["spec"]["template"]["spec"]["containers"][0]["env"]
assert p["metadata"]["labels"]["app"] == "bigc" and p["metadata"]["labels"]["job-name"] == j["metadata"]["name"], p["metadata"]["labels"]
assert p["metadata"]["annotations"] == {"example.com/pod-note": "a pod annotation"}, p["metadata"]["annotations"]
PYC
succeed_job "$B1"
kplane_until 30 "$B1 complete" sh -c "kubectl --kubeconfig $KC get job $B1 -o jsonpath='{.status.conditions[?(@.type==\"Complete\")].status}' | grep -qx True"
kplane_until 30 "bigc lastSuccessfulTime" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get cronjob bigc -o jsonpath='{.status.lastSuccessfulTime}')\" ]"
echo "   $B1 ($(wc -c <"$D/b1.json") bytes): template whole, Pod whole and labelled; Complete; bigc.status.lastSuccessfulTime set"

echo "== 3. the next minute: Allow runs again, Forbid waits, Replace replaces, sus runs =="
F1="$(jobs_of forbid | head -1)"; R1="$(jobs_of repl | head -1)"
kplane_until 90 "tick's second Job" has_jobs tick 2
T2="$(jobs_of tick | tail -1)"
kplane_until 20 "repl's first Job replaced" sh -c "! kubectl --kubeconfig $KC get job $R1"
kplane_until 20 "repl's second Job" has_jobs repl 1
[ "$(jobs_of repl)" != "$R1" ] || fail "repl still has only $R1"
kplane_until 20 "sus runs once resumed" has_jobs sus 1
[ "$(njobs forbid)" = 1 ] && [ "$(jobs_of forbid)" = "$F1" ] || fail "forbid ran a second Job beside an active one: $(jobs_of forbid)"
echo "   tick: $T2; repl: $R1 replaced by $(jobs_of repl); forbid: still only $F1; sus: $(jobs_of sus)"

echo "== 4. history: successfulJobsHistoryLimit 1 keeps the newest =="
succeed_job "$T2"
kplane_until 30 "$T1 pruned" sh -c "! kubectl --kubeconfig $KC get job $T1"
k get job "$T2" >/dev/null || fail "the newest success $T2 was pruned"
echo "   $T1 pruned; $T2 kept"

echo "== E2E green: CronJobs to Kubernetes' semantics, driven by kubectl =="
