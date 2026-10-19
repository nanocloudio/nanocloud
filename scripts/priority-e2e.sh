#!/usr/bin/env bash
# Live E2E: PriorityClass, scheduling order and preemption, driven with
# kubectl against the packaged control plane (priority.uproc; the scheduler's
# pick, _shared/sched.rs; scheduler.uproc).
#
#   1. the system classes exist (system-node-critical, system-cluster-critical)
#   2. a Pod's priorityClassName resolves to spec.priority (and its
#      preemptionPolicy); a Pod naming none takes the globalDefault class; one
#      naming a class that does not exist is not scheduled, and says why
#   3. the pending are bound highest priority first
#   4. a Pod no node fits preempts lower-priority Pods (Preempted events),
#      sparing — as far as it can — one a PodDisruptionBudget protects
#   5. preemptionPolicy Never: a Pod that may not preempt waits
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-prio-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v "MON_\|kvw\]\|port 53\|cluster_dns" || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

node_of() { k get pod "$1" -o jsonpath='{.spec.nodeName}' 2>/dev/null; }
bound() { [ -n "$(node_of "$1")" ]; }
gone() { ! k get pod "$1" >/dev/null 2>&1; }
pod() { # pod <name> <class|-> <cpu> [labels-json] [extra spec members]
  local cls="" labels="${4:-}"
  [ -n "$labels" ] || labels='{}'
  [ "$2" = - ] || cls=",\"priorityClassName\":\"$2\""
  printf '%s' "{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"$1\",\"labels\":$labels},\"spec\":{\"containers\":[{\"name\":\"c\",\"image\":\"busybox\",\"resources\":{\"requests\":{\"cpu\":\"$3\"}}}]$cls${5:-}}}" \
    | k apply -f - >/dev/null || fail "apply pod $1"
}
class() { # class <name> <value> [globalDefault] [preemptionPolicy]
  printf '%s' "{\"apiVersion\":\"scheduling.k8s.io/v1\",\"kind\":\"PriorityClass\",\"metadata\":{\"name\":\"$1\"},\"value\":$2,\"globalDefault\":${3:-false},\"preemptionPolicy\":\"${4:-PreemptLowerPriority}\"}" \
    | k apply -f - >/dev/null || fail "apply class $1"
}

echo "== 0. the control plane, joined; one node of cpu 1 =="
kplane_up
kplane_node node-a '{"kubernetes.io/hostname":"node-a"}' \
  '"capacity":{"cpu":"1","memory":"4Gi","pods":"110"},"allocatable":{"cpu":"1","memory":"4Gi","pods":"110"}'
echo "   node-a: cpu 1"

echo "== 1. the system classes =="
kplane_until 30 "system-node-critical" sh -c "[ \"\$(kubectl --kubeconfig $KC get priorityclass system-node-critical -o jsonpath='{.value}')\" = 2000001000 ]"
[ "$(k get priorityclass system-cluster-critical -o jsonpath='{.value}')" = 2000000000 ] || fail "system-cluster-critical"
echo "   system-node-critical 2000001000, system-cluster-critical 2000000000"

echo "== 2. resolution: named, default, missing =="
class low 10 true
class high 1000
class urgent 100000
class polite 5000 false Never
k cordon node-a >/dev/null
pod p-low - 1
pod p-high high 1
pod p-none nosuch 100m
kplane_until 30 "p-low takes the default" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod p-low -o jsonpath='{.spec.priorityClassName}/{.spec.priority}')\" = low/10 ]"
kplane_until 30 "p-high resolved" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod p-high -o jsonpath='{.spec.priority}/{.spec.preemptionPolicy}')\" = 1000/PreemptLowerPriority ]"
kplane_until 30 "p-none says its class is missing" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod p-none -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].message}')\" = 'no PriorityClass with name nosuch was found' ]"
echo "   p-low: low/10 (globalDefault); p-high: 1000; p-none: no PriorityClass with name nosuch was found"

echo "== 3. the highest priority first =="
k uncordon node-a >/dev/null
kplane_until 30 "p-high bound" bound p-high
sleep 3
[ -z "$(node_of p-low)" ] || fail "p-low (10) was bound ahead of, or beside, p-high (1000)"
[ -z "$(node_of p-none)" ] || fail "p-none was bound with its class unresolved"
echo "   p-high bound first; p-low waits (Insufficient cpu)"

echo "== 4. preemption, sparing what a budget protects =="
k delete pod p-low p-none --wait=false >/dev/null
pod crit urgent 1
kplane_until 30 "p-high preempted" gone p-high
kplane_until 30 "crit bound" bound crit
k get events --field-selector involvedObject.name=p-high -o jsonpath='{range .items[*]}{.reason}{"\n"}{end}' | grep -qx Preempted \
  || fail "no Preempted event for p-high"
k delete pod crit >/dev/null
kplane_until 30 "crit gone" gone crit
# The protected Pod sorts LAST: by name alone, it would be the victim.
pod zkeep - 500m '{"app":"keep"}'
pod aspare - 500m '{"app":"spare"}'
kplane_until 30 "zkeep bound" bound zkeep
kplane_until 30 "aspare bound" bound aspare
for p in zkeep aspare; do
  k patch pod "$p" --subresource=status --type=merge -p '{"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}' >/dev/null
done
k create poddisruptionbudget keep-pdb --selector=app=keep --min-available=1 >/dev/null
kplane_until 30 "keep-pdb allows none" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb keep-pdb -o jsonpath='{.status.disruptionsAllowed}/{.status.currentHealthy}')\" = 0/1 ]"
pod half high 500m
kplane_until 30 "half bound" bound half
gone aspare || fail "the unprotected Pod (aspare) was not the victim"
gone zkeep && fail "the Pod a budget protects (zkeep) was preempted while another would do"
echo "   crit (100000) preempted p-high (Preempted event); half (1000) took aspare, sparing zkeep (its budget)"

echo "== 5. preemptionPolicy Never waits =="
pod quiet polite 1
kplane_until 30 "quiet Unschedulable" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod quiet -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].reason}')\" = Unschedulable ]"
sleep 2
k get pod zkeep >/dev/null && k get pod half >/dev/null || fail "a Pod with preemptionPolicy Never preempted"
echo "   quiet (5000, Never): Unschedulable; nothing preempted"

echo "== E2E green: PriorityClass resolution, priority order, preemption =="
