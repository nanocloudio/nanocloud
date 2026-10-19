#!/usr/bin/env bash
# Live E2E: the scheduler to Kubernetes' semantics, driven with kubectl
# against the packaged control plane (scheduler.uproc over store_source's
# `sched` pick, _shared/sched.rs).
#
#   1. Nodes report capacity and allocatable; requests are fitted within
#      them; a Pod that fits nowhere is Pending with PodScheduled=False,
#      reason Unschedulable, and upstream's FailedScheduling event
#   2. taints: NoSchedule keeps an intolerant Pod off; a toleration admits it
#   3. `kubectl cordon` / `uncordon` (spec.unschedulable; STATUS
#      Ready,SchedulingDisabled)
#   4. node affinity: required (In) and preferred (weight)
#   5. pod anti-affinity and affinity on kubernetes.io/hostname
#   6. topologySpreadConstraints (maxSkew 1 over a zone label)
#   7. NoExecute: an intolerant Pod is evicted at once; one tolerating for
#      tolerationSeconds=4 is evicted after 4 s
#   8. a bound Pod past 4 KiB counts toward its node's load: a second Pod
#      that fits only without it stays Pending
#
# Nodes are registered through the API as their leases would write them;
# nothing runs the Pods (nothing here needs them to).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-sched-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v "MON_\|kvw\]" || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

node_of() { k get pod "$1" -o jsonpath='{.spec.nodeName}' 2>/dev/null; }
bound() { [ -n "$(node_of "$1")" ]; }
pod() { # pod <name> <spec-json-members> [labels-json] [container-json-members]
  local labels="${3:-}"
  [ -n "$labels" ] || labels='{}'
  printf '%s' "{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"$1\",\"labels\":$labels},\"spec\":{\"containers\":[{\"name\":\"c\",\"image\":\"busybox\"${4:-}}]$2}}" \
    | k apply -f - >/dev/null || fail "apply pod $1"
}
req() { printf ',"resources":{"requests":{"cpu":"%s"}}' "$1"; }
clear_pods() {
  k delete pods --all --wait=false >/dev/null 2>&1 || true
  kplane_until 30 "pods cleared" sh -c "[ -z \"\$(kubectl --kubeconfig $KC get pods -o name)\" ]"
}

echo "== 0. the control plane, joined; two nodes with capacity =="
kplane_up
kplane_node node-a '{"kubernetes.io/hostname":"node-a","zone":"z1","disk":"ssd"}' \
  '"capacity":{"cpu":"2","memory":"4Gi","pods":"110"},"allocatable":{"cpu":"2","memory":"4Gi","pods":"110"}'
kplane_node node-b '{"kubernetes.io/hostname":"node-b","zone":"z2","disk":"hdd"}' \
  '"capacity":{"cpu":"1","memory":"2Gi","pods":"110"},"allocatable":{"cpu":"1","memory":"2Gi","pods":"110"}'
[ "$(k get node node-a -o jsonpath='{.status.allocatable.cpu}/{.status.capacity.memory}')" = "2/4Gi" ] \
  || fail "node-a does not report its allocatable/capacity: $(k get node node-a -o jsonpath='{.status}')"
kplane_until 30 "both nodes ready" sh -c "kubectl --kubeconfig $KC get nodes --no-headers | grep -c ' Ready' | grep -qx 2"
echo "   node-a cpu 2, node-b cpu 1; both Ready"

echo "== 1. requests within allocatable; a Pod that fits nowhere is Unschedulable =="
pod big1 '' '{}' "$(req 1500m)"
kplane_until 30 "big1 bound" bound big1
[ "$(node_of big1)" = node-a ] || fail "big1 (1500m) must land on node-a (cpu 2), got $(node_of big1)"
pod big2 '' '{}' "$(req 1500m)"
WANT="0/2 nodes are available: 2 Insufficient cpu."
kplane_until 30 "big2 PodScheduled=False" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod big2 -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].reason}')\" = Unschedulable ]"
[ "$(k get pod big2 -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}')" = "$WANT" ] \
  || fail "big2's message: $(k get pod big2 -o jsonpath='{.status.conditions}')"
k get events --field-selector involvedObject.name=big2 -o jsonpath='{range .items[*]}{.reason}|{.type}|{.message}{"\n"}{end}' >"$D/ev"
grep -qx "FailedScheduling|Warning|$WANT" "$D/ev" || fail "no FailedScheduling event for big2: $(cat "$D/ev")"
[ -z "$(node_of big2)" ] || fail "big2 bound past allocatable"
k delete pod big1 >/dev/null
kplane_until 30 "big2 bound once big1 is gone" bound big2
echo "   big1 -> node-a; big2: '$WANT' (condition + event); bound once room was made"
clear_pods

echo "== 2. taints: NoSchedule keeps the intolerant off; a toleration admits =="
k taint nodes node-a dedicated=infra:NoSchedule >/dev/null || fail "taint"
for p in t1 t2 t3; do pod "$p" ''; done
for p in t1 t2 t3; do kplane_until 30 "$p bound" bound "$p"; [ "$(node_of $p)" = node-b ] || fail "$p landed on the tainted node-a"; done
pod tol ',"tolerations":[{"key":"dedicated","operator":"Equal","value":"infra","effect":"NoSchedule"}],"nodeSelector":{"kubernetes.io/hostname":"node-a"}'
kplane_until 30 "tol bound" bound tol
[ "$(node_of tol)" = node-a ] || fail "the tolerating Pod was not placed on node-a"
pod intol ',"nodeSelector":{"kubernetes.io/hostname":"node-a"}'
WANT="0/2 nodes are available: 1 node(s) didn't match Pod's node affinity/selector, 1 node(s) had untolerated taint {dedicated: infra}."
kplane_until 30 "intol Unschedulable" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod intol -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].message}')\" = \"$WANT\" ]"
k taint nodes node-a dedicated- >/dev/null || fail "untaint"
kplane_until 30 "intol bound once the taint is gone" bound intol
echo "   intolerant Pods on node-b; the tolerating one on node-a; '$WANT'; bound once untainted"
clear_pods

echo "== 3. cordon / uncordon =="
k cordon node-a >/dev/null || fail "cordon"
[ "$(k get node node-a --no-headers | awk '{print $2}')" = "Ready,SchedulingDisabled" ] || fail "kubectl get nodes: $(k get node node-a --no-headers)"
pod c1 ',"nodeSelector":{"kubernetes.io/hostname":"node-a"}'
WANT="0/2 nodes are available: 1 node(s) didn't match Pod's node affinity/selector, 1 node(s) were unschedulable."
kplane_until 30 "c1 Unschedulable on a cordoned node" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod c1 -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].message}')\" = \"$WANT\" ]"
k uncordon node-a >/dev/null || fail "uncordon"
kplane_until 30 "c1 bound after uncordon" bound c1
echo "   cordoned: Ready,SchedulingDisabled, '$WANT'; uncordoned: bound"
clear_pods

echo "== 4. node affinity: required In; preferred weight =="
pod na1 ',"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"disk","operator":"In","values":["hdd"]}]}]}}}'
kplane_until 30 "na1 bound" bound na1
[ "$(node_of na1)" = node-b ] || fail "na1 (disk In hdd) on $(node_of na1)"
# node-a would win on load (fewer Pods); the preference says node-b.
pod na2 ',"affinity":{"nodeAffinity":{"preferredDuringSchedulingIgnoredDuringExecution":[{"weight":50,"preference":{"matchExpressions":[{"key":"zone","operator":"In","values":["z2"]}]}}]}}'
kplane_until 30 "na2 bound" bound na2
[ "$(node_of na2)" = node-b ] || fail "na2 (prefers z2) on $(node_of na2)"
pod na3 ',"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"disk","operator":"NotIn","values":["ssd","hdd"]}]}]}}}'
kplane_until 30 "na3 Unschedulable" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod na3 -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].reason}')\" = Unschedulable ]"
echo "   required In -> node-b; preferred z2 -> node-b; NotIn both -> Unschedulable"
clear_pods

echo "== 5. pod anti-affinity and affinity (kubernetes.io/hostname) =="
ANTI=',"affinity":{"podAntiAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"app":"web"}},"topologyKey":"kubernetes.io/hostname"}]}}'
pod w1 "$ANTI" '{"app":"web"}'
kplane_until 30 "w1 bound" bound w1
pod w2 "$ANTI" '{"app":"web"}'
kplane_until 30 "w2 bound" bound w2
[ "$(node_of w1)" != "$(node_of w2)" ] || fail "anti-affinity: w1 and w2 both on $(node_of w1)"
pod w3 "$ANTI" '{"app":"web"}'
kplane_until 30 "w3 Unschedulable" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod w3 -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].message}')\" = \"0/2 nodes are available: 2 node(s) didn't match pod anti-affinity rules.\" ]"
pod side ',"affinity":{"podAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":[{"labelSelector":{"matchLabels":{"app":"web"}},"topologyKey":"kubernetes.io/hostname"}]}},"nodeSelector":{"kubernetes.io/hostname":"'"$(node_of w2)"'"}'
kplane_until 30 "side bound" bound side
[ "$(node_of side)" = "$(node_of w2)" ] || fail "affinity: side on $(node_of side)"
echo "   w1 $(node_of w1), w2 $(node_of w2), w3 Unschedulable (anti-affinity); side beside w2 (affinity)"
clear_pods

echo "== 6. topologySpreadConstraints: maxSkew 1 over zone =="
SPREAD=',"topologySpreadConstraints":[{"maxSkew":1,"topologyKey":"zone","whenUnsatisfiable":"DoNotSchedule","labelSelector":{"matchLabels":{"app":"s"}}}]'
for p in s1 s2 s3 s4; do pod "$p" "$SPREAD" '{"app":"s"}'; kplane_until 30 "$p bound" bound "$p"; done
A=0; B=0; for p in s1 s2 s3 s4; do case "$(node_of $p)" in node-a) A=$((A+1));; node-b) B=$((B+1));; esac; done
[ "$A" = 2 ] && [ "$B" = 2 ] || fail "spread: node-a=$A node-b=$B"
echo "   2/2 across the zones"
clear_pods

echo "== 7. NoExecute: evicted at once; after tolerationSeconds =="
pod ne1 ',"nodeSelector":{"kubernetes.io/hostname":"node-b"}'
pod ne2 ',"nodeSelector":{"kubernetes.io/hostname":"node-b"},"tolerations":[{"key":"maint","operator":"Exists","effect":"NoExecute","tolerationSeconds":4}]'
pod ne3 ',"nodeSelector":{"kubernetes.io/hostname":"node-b"},"tolerations":[{"key":"maint","operator":"Exists","effect":"NoExecute"}]'
for p in ne1 ne2 ne3; do kplane_until 30 "$p bound" bound "$p"; done
k taint nodes node-b maint=now:NoExecute >/dev/null || fail "NoExecute taint"
T0=$(date +%s)
kplane_until 15 "ne1 evicted" sh -c "! kubectl --kubeconfig $KC get pod ne1"
k get pod ne2 >/dev/null 2>&1 || fail "ne2 evicted inside its tolerationSeconds"
kplane_until 20 "ne2 evicted after 4 s" sh -c "! kubectl --kubeconfig $KC get pod ne2"
T1=$(date +%s)
[ $((T1 - T0)) -ge 3 ] || fail "ne2 went after $((T1 - T0)) s, inside its 4 s"
sleep 2
k get pod ne3 >/dev/null 2>&1 || fail "ne3 tolerates for ever, yet was evicted"
echo "   ne1 at once; ne2 after $((T1 - T0)) s; ne3 stays"

echo "== 8. a Pod past 4 KiB holds its node's resources like any other =="
clear_pods
python3 - >"$D/fat.json" <<'PYB'
import json
env = [{"name": "SETTING_%02d" % i, "value": "value-%02d-abcdefghijklmnopqrstuvwxyz0123456789" % i} for i in range(64)]
print(json.dumps({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "fat", "labels": {"app": "fat"}},
                  "spec": {"nodeSelector": {"kubernetes.io/hostname": "node-a"},
                           "containers": [{"name": "c", "image": "busybox", "env": env,
                                           "resources": {"requests": {"cpu": "1500m"}}}]}},
                 separators=(",", ":")))
PYB
[ "$(wc -c <"$D/fat.json")" -gt 4096 ] || fail "fat.json is not past 4 KiB"
k create -f "$D/fat.json" >/dev/null || fail "create pod fat"
kplane_until 30 "fat bound" bound fat
[ "$(node_of fat)" = node-a ] || fail "fat on $(node_of fat)"
pod thin ',"nodeSelector":{"kubernetes.io/hostname":"node-a"}' '{}' "$(req 1000m)"
kplane_until 30 "thin Unschedulable beside fat" sh -c "[ \"\$(kubectl --kubeconfig $KC get pod thin -o jsonpath='{.status.conditions[?(@.type==\"PodScheduled\")].reason}')\" = Unschedulable ]"
[ -z "$(node_of thin)" ] || fail "thin (1000m) bound to node-a beside fat (1500m) of 2 cpu: the large Pod was not counted"
k get pod thin -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}' | grep -q "Insufficient cpu" \
  || fail "thin's message: $(k get pod thin -o jsonpath='{.status.conditions}')"
k delete pod fat >/dev/null
kplane_until 30 "thin bound once fat is gone" bound thin
echo "   fat ($(wc -c <"$D/fat.json") bytes, 1500m) on node-a; thin (1000m) Pending, Insufficient cpu; bound once fat went"

echo "== E2E green: the scheduler to Kubernetes' semantics, driven by kubectl =="
