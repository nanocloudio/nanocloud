#!/usr/bin/env bash
# Drive the bare-metal control plane (packaging/rig/pi5-controlplane.scenario.toml)
# the way an operator would, with stock kubectl: wait for the API, then
#
#   1. kubectl get nodes        — this node registered itself, status.ready
#   2. kubectl apply Deployment — admitted, stored on the board's NVMe
#   3. the controllers act      — its ReplicaSet and Pods appear
#
# Run beside the scenario once the rig has power-cycled:
#   bash scripts/rig-controlplane-driver.sh [ip]
# Exit 0 only if every step held.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IP="${1:-${NANOCLOUD_RIG_IP:-192.168.1.9}}"
NODE="${NANOCLOUD_RIG_NODE:-pi5-a}"
KC="$ROOT/target/pi5/kubeconfig"
fail() { echo "FAIL: $*"; exit 1; }
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }

NANOCLOUD_RIG_IP="$IP" bash "$ROOT/scripts/rig-kubeconfig.sh" "$KC" 24 >/dev/null \
  || fail "kubeconfig"

echo "== 0. the API answers (TLS, verified against the embedded certificate)"
for i in $(seq 120); do
  k get --raw /version >/tmp/rig-version.$$ 2>/dev/null && break
  sleep 2
done
[ -s /tmp/rig-version.$$ ] || fail "no API at https://$IP:6443 after 240 s"
echo "   $(tr -d '\n' </tmp/rig-version.$$ | cut -c1-120)"
rm -f /tmp/rig-version.$$

echo "== 1. kubectl get nodes: $NODE registered and ready"
for i in $(seq 60); do
  ready="$(k get node "$NODE" -o jsonpath='{.status.ready}' 2>/dev/null)"
  [ "$ready" = "true" ] && break
  sleep 2
done
[ "${ready:-}" = "true" ] || { k get nodes -o wide; fail "$NODE not ready (status.ready=${ready:-absent})"; }
k get nodes

echo "== 2. kubectl apply a Deployment"
cat <<'YAML' | k apply -f - || fail "kubectl apply"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hello
  namespace: default
spec:
  replicas: 2
  selector:
    matchLabels: {app: hello}
  template:
    metadata:
      labels: {app: hello}
    spec:
      containers:
      - name: hello
        image: registry.nanocloud.io:5000/nanocloud/hello:latest
YAML

echo "== 3. the controllers act: a ReplicaSet, then 2 Pods"
for i in $(seq 60); do
  rs="$(k get rs -l app=hello -o name 2>/dev/null | wc -l)"
  pods="$(k get pods -l app=hello -o name 2>/dev/null | wc -l)"
  [ "$rs" -ge 1 ] && [ "$pods" -ge 2 ] && break
  sleep 2
done
k get deploy,rs,pods -o wide
[ "${rs:-0}" -ge 1 ] || fail "no ReplicaSet for hello"
[ "${pods:-0}" -ge 2 ] || fail "ReplicaSet made ${pods:-0} of 2 Pods"

# 4. The board keeps what it was told across a power cycle, and comes back
#    serving in seconds — not after replaying its whole log. Opt-in: it
#    needs the rig's power backend (`fluxor rig power`), and it reboots the
#    board under whatever scenario holds it.
if [ "${NANOCLOUD_RIG_RESTART:-0}" = "1" ]; then
  RIG="${NANOCLOUD_RIG:-pi5-a}"
  echo "== 4. power-cycle $RIG: the Deployment survives, the API is back within 60 s"
  before="$(k get deploy hello -o jsonpath='{.metadata.uid} {.spec.replicas} {.metadata.resourceVersion}')"
  [ -n "$before" ] || fail "could not read the Deployment before the restart"
  fluxor rig power cycle --rig "$RIG" >/dev/null || fail "fluxor rig power cycle"
  t0=$(date +%s)
  back=""
  for i in $(seq 90); do
    sleep 2
    k get --raw /version >/dev/null 2>&1 && { back=$(( $(date +%s) - t0 )); break; }
  done
  [ -n "$back" ] || fail "the API did not come back within 180 s of the power cycle"
  echo "   API back after ${back} s"
  [ "$back" -le 60 ] || fail "the API took ${back} s to come back: the board replayed its log instead of resuming"
  # The API answers /version before its store serves (as an apiserver does
  # while etcd catches up); the objects are read once the store is back.
  after=""
  for i in $(seq 30); do
    after="$(k get deploy hello -o jsonpath='{.metadata.uid} {.spec.replicas} {.metadata.resourceVersion}' 2>/dev/null)"
    [ -n "$after" ] && break
    sleep 2
  done
  [ -n "$after" ] || fail "the store did not serve the Deployment within 60 s of the API coming back"
  echo "   store serving after $(( $(date +%s) - t0 )) s"
  [ "${after%% *}" = "${before%% *}" ] || fail "the Deployment changed identity across the restart: '$before' -> '$after'"
  r_before="${before##* }"; r_after="${after##* }"
  [ "${r_after:-0}" -ge "${r_before:-0}" ] || fail "resourceVersion went backwards: $r_before -> $r_after"
  echo "   deployment.apps/hello: same uid, replicas=$(echo "$after" | cut -d' ' -f2), rv $r_before -> $r_after"
fi
# 5. M2: a graph-backed Pod. Opt-in: the board must carry the pod plane
#    (scripts/rig-render-controlplane.sh, NANOCLOUD_RIG_PODPLANE=1). The
#    Graph is applied here as any object (the render seeds one too). The
#    node stages it as a subgraph of its own image and its lifecycle chain's
#    phase reaches the Pod object: `kubectl get pods` says Running.
if [ "${NANOCLOUD_RIG_M2:-0}" = "1" ]; then
  echo "== 5. a Graph is applied, and a graph-backed Pod naming it is scheduled on $NODE"
  FLXA="$(python3 "$ROOT/scripts/support/flxa.py" churn_probe:1=0)"
  cat <<YAML | k apply -f - || fail "kubectl apply graph probe"
apiVersion: nanocloud.io/v1
kind: Graph
metadata:
  name: probe
spec:
  flxa: "$FLXA"
YAML
  k get graphs.nanocloud.io probe -o name >/dev/null || fail "the Graph did not read back"
  cat <<'YAML' | k apply -f - || fail "kubectl apply pod probe"
apiVersion: v1
kind: Pod
metadata:
  name: probe
  namespace: default
spec:
  runtimeClassName: fluxor
  containers:
  - name: probe
    image: probe
YAML
  for i in $(seq 60); do
    nn="$(k get pod probe -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    [ "$nn" = "$NODE" ] && break
    sleep 2
  done
  [ "${nn:-}" = "$NODE" ] || fail "pod probe was not scheduled on $NODE (nodeName='${nn:-}')"
  # The node stages the subgraph of its own image and reports: the phase
  # its lifecycle chain decides reaches the Pod object (pod_status.uproc).
  for i in $(seq 30); do
    ph="$(k get pod probe -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    [ "$ph" = "Running" ] && break
    sleep 2
  done
  k get pod probe -o wide
  [ "${ph:-}" = "Running" ] || fail "pod probe is not Running on $NODE (phase='${ph:-}')"
fi
echo "PASS: kubectl drives the bare-metal control plane"
