#!/usr/bin/env bash
# Live E2E: PodDisruptionBudgets and the Eviction subresource, driven with
# kubectl against the packaged control plane (pdb.uproc, admission_k8s.uproc).
#
#   1. a budget's status as upstream's disruption controller states it:
#      expectedPods, currentHealthy (Ready Pods), desiredHealthy (from
#      minAvailable, and from a maxUnavailable percentage), disruptionsAllowed
#      and the DisruptionAllowed condition
#   2. `pods/eviction` in discovery (policy/v1 Eviction)
#   3. an eviction the budget allows deletes the Pod; one it does not is 429
#      TooManyRequests, "Cannot evict pod as it would violate the pod's
#      disruption budget."; one of a Pod that is gone is 404
#   4. `kubectl drain` evicts what the budget allows, and stops at what it
#      does not — then completes once the budget allows it
#   5. the same for a Pod past a record (≥ 4 KiB): its labels select its
#      budget, so the eviction is 429 and `kubectl drain` waits
#
# Pods are made Ready through the status subresource, as their node's report
# would; nothing runs them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-pdb-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v "MON_\|kvw\]\|port 53" || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

ready_pod() { # ready_pod <name> <app>: a Pod on node-a, Running and Ready
  printf '%s' "{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"$1\",\"labels\":{\"app\":\"$2\"}},\"spec\":{\"nodeName\":\"node-a\",\"containers\":[{\"name\":\"c\",\"image\":\"busybox\"}]}}" \
    | k apply -f - >/dev/null || fail "apply $1"
  k patch pod "$1" --subresource=status --type=merge \
    -p '{"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}' >/dev/null || fail "ready $1"
}
st() { k get pdb "$1" -o jsonpath='{.status.expectedPods}/{.status.currentHealthy}/{.status.desiredHealthy}/{.status.disruptionsAllowed}'; }
evict() { # evict <pod> -> the HTTP status of POST …/pods/<pod>/eviction
  printf '%s' "{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"$1\",\"namespace\":\"default\"}}" >"$D/ev.json"
  k create --raw "/api/v1/namespaces/default/pods/$1/eviction" -f "$D/ev.json" >"$D/ev.out" 2>&1 && echo 201 && return
  case "$(cat "$D/ev.out")" in
    *"TooManyRequests"*|*"429"*|*"disruption budget"*) echo 429 ;;
    *"NotFound"*|*"not found"*) echo 404 ;;
    *) echo "?: $(cat "$D/ev.out")" ;;
  esac
}

echo "== 0. the control plane, joined; a node; three Ready Pods app=web =="
kplane_up
kplane_node node-a '{"kubernetes.io/hostname":"node-a"}'
for p in web-1 web-2 web-3; do ready_pod "$p" web; done
ready_pod db-1 db
echo "   web-1..3, db-1 Ready on node-a"

echo "== 1. budgets' status =="
k create poddisruptionbudget web-pdb --selector=app=web --min-available=2 >/dev/null || fail "create pdb"
kplane_until 30 "web-pdb status 3/3/2/1" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb web-pdb -o jsonpath='{.status.expectedPods}/{.status.currentHealthy}/{.status.desiredHealthy}/{.status.disruptionsAllowed}')\" = 3/3/2/1 ]"
[ "$(k get pdb web-pdb -o jsonpath='{.status.conditions[?(@.type=="DisruptionAllowed")].status}/{.status.conditions[?(@.type=="DisruptionAllowed")].reason}')" = "True/SufficientPods" ] \
  || fail "web-pdb condition: $(k get pdb web-pdb -o jsonpath='{.status.conditions}')"
k create poddisruptionbudget db-pdb --selector=app=db --max-unavailable=0% >/dev/null || fail "create db-pdb"
kplane_until 30 "db-pdb status 1/1/1/0" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb db-pdb -o jsonpath='{.status.expectedPods}/{.status.currentHealthy}/{.status.desiredHealthy}/{.status.disruptionsAllowed}')\" = 1/1/1/0 ]"
[ "$(k get pdb db-pdb -o jsonpath='{.status.conditions[0].reason}')" = InsufficientPods ] || fail "db-pdb condition"
k patch pod web-3 --subresource=status --type=merge -p '{"status":{"conditions":[{"type":"Ready","status":"False"}]}}' >/dev/null
kplane_until 30 "web-pdb follows a Pod going unready (3/2/2/0)" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb web-pdb -o jsonpath='{.status.expectedPods}/{.status.currentHealthy}/{.status.desiredHealthy}/{.status.disruptionsAllowed}')\" = 3/2/2/0 ]"
k patch pod web-3 --subresource=status --type=merge -p '{"status":{"conditions":[{"type":"Ready","status":"True"}]}}' >/dev/null
kplane_until 30 "web-pdb 3/3/2/1 again" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb web-pdb -o jsonpath='{.status.disruptionsAllowed}')\" = 1 ]"
echo "   web-pdb (minAvailable 2): 3/3/2/1 SufficientPods; db-pdb (maxUnavailable 0%): 1/1/1/0 InsufficientPods; follows readiness"

echo "== 2. pods/eviction in discovery =="
k get --raw /api/v1 | python3 -c 'import json,sys; r=[x for x in json.load(sys.stdin)["resources"] if x["name"]=="pods/eviction"]; assert r and r[0]["kind"]=="Eviction" and r[0]["group"]=="policy" and r[0]["verbs"]==["create"], r' \
  || fail "pods/eviction is not in /api/v1's discovery"
echo "   pods/eviction: policy/v1 Eviction, create"

echo "== 3. evictions: allowed, refused (429), of nothing (404) =="
[ "$(evict db-1)" = 429 ] || fail "db-1's eviction must be refused: $(cat "$D/ev.out")"
grep -q "Cannot evict pod as it would violate the pod's disruption budget" "$D/ev.out" || fail "the 429's message: $(cat "$D/ev.out")"
k get pod db-1 >/dev/null || fail "a refused eviction deleted db-1"
[ "$(evict web-1)" = 201 ] || fail "web-1's eviction must be allowed: $(cat "$D/ev.out")"
kplane_until 20 "web-1 deleted" sh -c "! kubectl --kubeconfig $KC get pod web-1"
kplane_until 30 "web-pdb 2/2/2/0" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb web-pdb -o jsonpath='{.status.expectedPods}/{.status.currentHealthy}/{.status.desiredHealthy}/{.status.disruptionsAllowed}')\" = 2/2/2/0 ]"
[ "$(evict web-2)" = 429 ] || fail "web-2's eviction must now be refused: $(cat "$D/ev.out")"
[ "$(evict nosuch)" = 404 ] || fail "an absent Pod's eviction must be 404: $(cat "$D/ev.out")"
echo "   db-1 429; web-1 evicted; then web-2 429 (budget spent); nosuch 404"

echo "== 4. kubectl drain honours the budget =="
ready_pod web-4 web
kplane_until 30 "web-pdb 3/3/2/1" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb web-pdb -o jsonpath='{.status.disruptionsAllowed}')\" = 1 ]"
k delete pdb db-pdb >/dev/null
k drain node-a --pod-selector=app=web --force --timeout=15s >"$D/drain.out" 2>&1 && fail "drain completed past the budget: $(cat "$D/drain.out")"
grep -q "disruption budget" "$D/drain.out" || fail "drain did not report the budget: $(cat "$D/drain.out")"
[ "$(k get pods -l app=web --no-headers | wc -l)" = 2 ] || fail "drain should have evicted exactly one web Pod: $(k get pods -l app=web --no-headers)"
[ "$(k get node node-a -o jsonpath='{.spec.unschedulable}')" = true ] || fail "drain did not cordon node-a"
k delete pdb web-pdb >/dev/null
k drain node-a --pod-selector=app=web --force --timeout=30s >"$D/drain.out" 2>&1 || fail "drain without a budget: $(cat "$D/drain.out")"
[ -z "$(k get pods -l app=web -o name)" ] || fail "web Pods left after the drain"
echo "   drain: one web Pod evicted, the rest held by the budget (cordoned); without it, all"

echo "== 5. a Pod past a record (≥ 4 KiB) is judged by its labels all the same =="
# Its labels are read from the Pod read whole (ev_get, store_stamped): one
# past a record answers no value, and a budget judged on that selected
# nothing — the eviction was allowed past disruptionsAllowed=0.
python3 - "$D/big.json" <<'PY'
import json, sys
env = [{"name": f"E{i:03}", "value": "v" * 48} for i in range(90)]
pod = {"apiVersion": "v1", "kind": "Pod",
       "metadata": {"name": "big-1", "labels": {"app": "big", "tier": "data"}},
       "spec": {"nodeName": "node-a", "containers": [{"name": "c", "image": "busybox", "env": env}]}}
s = json.dumps(pod, separators=(",", ":"))
assert len(s) > 6000, len(s)
open(sys.argv[1], "w").write(s)
PY
k apply -f "$D/big.json" >/dev/null || fail "apply big-1"
k patch pod big-1 --subresource=status --type=merge \
  -p '{"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}' >/dev/null || fail "ready big-1"
[ "$(k get pod big-1 -o json | wc -c)" -gt 4096 ] || fail "big-1 is not past a record"
k create poddisruptionbudget big-pdb --selector=app=big --min-available=1 >/dev/null || fail "create big-pdb"
kplane_until 30 "big-pdb status 1/1/1/0" sh -c "[ \"\$(kubectl --kubeconfig $KC get pdb big-pdb -o jsonpath='{.status.expectedPods}/{.status.currentHealthy}/{.status.desiredHealthy}/{.status.disruptionsAllowed}')\" = 1/1/1/0 ]"
[ "$(evict big-1)" = 429 ] || fail "big-1's eviction must be refused (its budget allows none): $(cat "$D/ev.out")"
k get pod big-1 >/dev/null || fail "a refused eviction deleted big-1"
k drain node-a --pod-selector=app=big --force --timeout=10s >"$D/drain.out" 2>&1 && fail "drain evicted big-1 past its budget: $(cat "$D/drain.out")"
grep -q "disruption budget" "$D/drain.out" || fail "drain did not report big-1's budget: $(cat "$D/drain.out")"
k get pod big-1 >/dev/null || fail "drain deleted big-1 past its budget"
k delete pdb big-pdb >/dev/null
[ "$(evict big-1)" = 201 ] || fail "big-1's eviction without a budget must be allowed: $(cat "$D/ev.out")"
kplane_until 20 "big-1 deleted" sh -c "! kubectl --kubeconfig $KC get pod big-1"
echo "   big-1 ($(wc -c <"$D/big.json") B): 429 under a budget allowing none, drain waits; without it, evicted"

echo "== E2E green: PodDisruptionBudget status and evictions, driven by kubectl =="
