#!/usr/bin/env bash
# Live E2E: ResourceQuota as Kubernetes enforces it, driven with kubectl
# against the packaged control plane (quota.uproc keeps the usage;
# admission_k8s.uproc `ad_q` refuses what would exceed it).
#
#   1. `kubectl create quota` -> status.hard, and status.used kept current
#   2. pods within pods / requests / limits are admitted; the one past them
#      is Forbidden "exceeded quota" — a body handed inline (`kubectl create`)
#      and one staged in the store (`kubectl apply`, past a record) alike
#   3. a Pod stating no cpu request, under a quota on requests.cpu, is
#      refused (upstream: it must specify one)
#   4. configmaps and count/deployments.apps: one object each
#   5. room made (a Pod deleted) is room to create again
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-quota-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v "MON_\|kvw\]\|port 53\|cluster_dns" || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

kt() { k -n team "$@"; }
pod_json() { # pod_json <name> <cpu req> <mem req> <cpu lim> <mem lim>: a small Pod
  printf '%s' "{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"$1\"},\"spec\":{\"containers\":[{\"name\":\"c\",\"image\":\"busybox\",\"resources\":{\"requests\":{\"cpu\":\"$2\",\"memory\":\"$3\"},\"limits\":{\"cpu\":\"$4\",\"memory\":\"$5\"}}}]}}"
}
used() { kt get quota q1 -o jsonpath="{.status.used.$1}"; }
used_is() { [ "$(used "$1")" = "$2" ]; }
forbidden() { # forbidden <out-file>: the answer was Forbidden "exceeded quota"
  grep -qi "forbidden" "$1" && grep -q "exceeded quota" "$1"
}

echo "== 0. the control plane, joined; namespace team =="
kplane_up
k create namespace team >/dev/null || fail "create namespace"
echo "   joined"

echo "== 1. the quota and its status =="
kt create quota q1 --hard=pods=2,requests.cpu=1,requests.memory=1Gi,limits.cpu=2,limits.memory=2Gi,configmaps=2,count/deployments.apps=1 >/dev/null \
  || fail "create quota"
kplane_until 30 "status.used.pods = 0" used_is pods 0
[ "$(kt get quota q1 -o jsonpath='{.status.hard.pods}/{.status.hard.requests\.cpu}')" = "2/1" ] || fail "status.hard: $(kt get quota q1 -o jsonpath='{.status}')"
echo "   hard: pods=2 requests.cpu=1 …; used: pods=0, requests.cpu=$(used 'requests\.cpu')"

echo "== 2. pods within the quota are admitted; the one past it is Forbidden =="
pod_json p1 500m 256Mi 1 512Mi >"$D/p1.json"
kt create -f "$D/p1.json" >/dev/null || fail "p1 (inline body) refused"
kplane_until 30 "used pods=1, requests.cpu=500m" sh -c "[ \"\$(kubectl --kubeconfig $KC -n team get quota q1 -o jsonpath='{.status.used.pods}/{.status.used.requests\.cpu}/{.status.used.requests\.memory}')\" = 1/500m/268435456 ]"
pod_json p2 500m 256Mi 1 512Mi >"$D/p2.json"
kt apply -f "$D/p2.json" >/dev/null || fail "p2 (staged body) refused"
kplane_until 30 "used pods=2, requests.cpu=1" sh -c "[ \"\$(kubectl --kubeconfig $KC -n team get quota q1 -o jsonpath='{.status.used.pods}/{.status.used.requests\.cpu}/{.status.used.limits\.cpu}')\" = 2/1/2 ]"
pod_json p3 100m 64Mi 100m 64Mi >"$D/p3.json"
kt create -f "$D/p3.json" >"$D/out" 2>&1 && fail "p3 admitted past pods=2"
forbidden "$D/out" || fail "p3's refusal: $(cat "$D/out")"
kt apply -f "$D/p3.json" >"$D/out" 2>&1 && fail "p3 (apply) admitted past pods=2"
forbidden "$D/out" || fail "p3's (apply) refusal: $(cat "$D/out")"
echo "   p1 (create) and p2 (apply) admitted; p3: $(head -1 "$D/out")"

echo "== 3. room made is room again; requests past the quota, or unstated, are refused =="
kt delete pod p1 >/dev/null
kplane_until 30 "used pods=1" used_is pods 1
pod_json big 900m 128Mi 1 128Mi >"$D/big.json"
kt create -f "$D/big.json" >"$D/out" 2>&1 && fail "big admitted past requests.cpu=1 (500m used)"
forbidden "$D/out" || fail "big's refusal: $(cat "$D/out")"
printf '%s' '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"bare"},"spec":{"containers":[{"name":"c","image":"busybox"}]}}' >"$D/bare.json"
kt create -f "$D/bare.json" >"$D/out" 2>&1 && fail "a Pod with no requests admitted under a requests quota"
forbidden "$D/out" || fail "bare's refusal: $(cat "$D/out")"
kt create -f "$D/p3.json" >/dev/null || fail "p3 refused with room made"
echo "   p3 admitted once p1 went; 900m past requests.cpu and a Pod stating no request refused"

echo "== 4. configmaps and count/deployments.apps =="
kt create configmap c1 --from-literal=a=b >/dev/null || fail "c1"
kplane_until 30 "used configmaps=1" used_is configmaps 1
kt create configmap c2 --from-literal=a=b >/dev/null || fail "c2"
kplane_until 30 "used configmaps=2" used_is configmaps 2
kt create configmap c3 --from-literal=a=b >"$D/out" 2>&1 && fail "c3 admitted past configmaps=2"
forbidden "$D/out" || fail "c3's refusal: $(cat "$D/out")"
kt create deployment d1 --image=nginx --replicas=0 >/dev/null || fail "d1"
kplane_until 30 "used count/deployments.apps=1" sh -c "[ \"\$(kubectl --kubeconfig $KC -n team get quota q1 -o jsonpath='{.status.used.count/deployments\.apps}')\" = 1 ]"
kt create deployment d2 --image=nginx --replicas=0 >"$D/out" 2>&1 && fail "d2 admitted past count/deployments.apps=1"
forbidden "$D/out" || fail "d2's refusal: $(cat "$D/out")"
echo "   c3 and d2 refused: $(head -1 "$D/out")"

echo "== E2E green: ResourceQuota enforced at admission, its usage kept current =="
