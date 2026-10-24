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
#
# The operator's credential is the node's own: the board announces a join
# secret and its authority's key digest on its telemetry, and
# scripts/rig-join.sh spends the secret for a kubeconfig that verifies the
# API against that authority. After a power cycle the driver listens to the
# board's telemetry itself (the run's capture has ended at its verdict) and
# joins again with the new boot's secret — a board whose vault cannot
# persist keys (no OTP device key) comes back under a new authority.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IP="${1:-${NANOCLOUD_RIG_IP:-192.168.1.9}}"
NODE="${NANOCLOUD_RIG_NODE:-pi5-a}"
KC="$ROOT/target/pi5/kubeconfig"
RIG="${NANOCLOUD_RIG:-pi5-a}"
RUNS="${FLUXOR_RIG_RUNS:-$HOME/.local/state/fluxor/labs/default/rigs/$RIG/runs}"
JOIN_RE='\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}'
LAST_JOIN=""
LISTENER=""
TLOG="$(mktemp /tmp/rig-telemetry.XXXXXX)"
trap '[ -n "$LISTENER" ] && kill "$LISTENER" 2>/dev/null; rm -f "$TLOG"' EXIT
# On failure the board's telemetry since the last power cycle is kept
# (target/pi5/driver-telemetry.log): once the run has reached its verdict
# this capture is the board's only account of what it did.
fail() {
  echo "FAIL: $*"
  [ -s "$TLOG" ] && cp "$TLOG" "$ROOT/target/pi5/driver-telemetry.log" && echo "   telemetry kept: target/pi5/driver-telemetry.log"
  exit 1
}
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }

# join <telemetry file>: the newest announcement there that is not the last
# one spent, waited for, then spent for $KC.
join() {
  local line="" secret ca
  for i in $(seq 120); do
    line="$(grep -ao "$JOIN_RE" "$1" 2>/dev/null | tail -1)"
    [ -n "$line" ] && [ "$line" != "$LAST_JOIN" ] && break
    line=""
    sleep 2
  done
  [ -n "$line" ] || fail "no new join announcement in $1 within 240 s"
  LAST_JOIN="$line"
  secret="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$line")"
  ca="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$line")"
  bash "$ROOT/scripts/rig-join.sh" "$secret" "$ca" "$IP:6443" "$KC" >/dev/null || fail "join"
  echo "   joined: authority ${ca:0:16}…, kubeconfig $KC"
}

# The board's telemetry, captured here: once a run has reached its verdict
# nothing else is listening.
listen() {
  [ -n "$LISTENER" ] && return
  # The run captures until its verdict, which can land after the steps
  # before this one: wait for it to let go of the port (its timeout_s).
  for i in $(seq 300); do
    python3 -c 'import socket; s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind(("0.0.0.0", 6666))' 2>/dev/null && break
    sleep 2
  done
  python3 -u - "$TLOG" <<'PY' &
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 6666))
with open(sys.argv[1], "ab", buffering=0) as f:
    while True:
        f.write(s.recv(65535) + b"\n")
PY
  LISTENER=$!
  sleep 0.5
  kill -0 "$LISTENER" 2>/dev/null || fail "could not listen on udp :6666 (is a run still capturing?)"
}

echo "== 0. join: the node's own secret, its authority checked against its digest"
if [ -n "${NANOCLOUD_JOIN:-}" ]; then
  printf '[join] %s\n' "$NANOCLOUD_JOIN" >"$TLOG"   # "secret=… ca-sha256=…"
  join "$TLOG"
else
  join "$(ls -td "$RUNS"/*/ | head -1)telemetry.monitor_udp.log"
fi

echo "== 0b. the API answers (TLS, verified against the node's authority)"
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
  echo "== 4. power-cycle $RIG: the Deployment survives, the API is back within 60 s"
  before="$(k get deploy hello -o jsonpath='{.metadata.uid} {.spec.replicas} {.metadata.resourceVersion}')"
  [ -n "$before" ] || fail "could not read the Deployment before the restart"
  listen
  fluxor rig power cycle --rig "$RIG" >/dev/null || fail "fluxor rig power cycle"
  t0=$(date +%s)
  back=""
  # Back = serving TLS on :6443 again (anonymous /version); whom to trust is
  # the join's question, asked next.
  for i in $(seq 90); do
    sleep 2
    curl -sk --max-time 3 "https://$IP:6443/version" >/dev/null 2>&1 && { back=$(( $(date +%s) - t0 )); break; }
  done
  [ -n "$back" ] || fail "the API did not come back within 180 s of the power cycle"
  echo "   API back after ${back} s"
  [ "$back" -le 60 ] || fail "the API took ${back} s to come back: the board replayed its log instead of resuming"
  join "$TLOG"
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

  # 6. A Deployment whose template names the Graph: its ReplicaSet's Pods
  #    run as the bare Pod did, each owned through the chain kubectl follows.
  echo "== 6. a Deployment of graph-backed Pods: both run, owned Deployment -> ReplicaSet -> Pod"
  cat <<'YAML' | k apply -f - || fail "kubectl apply deployment probes"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: probes
  namespace: default
spec:
  replicas: 2
  selector:
    matchLabels: {app: probes}
  template:
    metadata:
      labels: {app: probes}
    spec:
      runtimeClassName: fluxor
      containers:
      - name: probe
        image: probe
YAML
  probes_running() {
    k get pods -l app=probes -o json 2>/dev/null | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: print(0); raise SystemExit
print(sum(1 for p in d["items"] if p.get("status",{}).get("phase")=="Running" and p["metadata"].get("uid")))'
  }
  for i in $(seq 60); do n="$(probes_running)"; [ "${n:-0}" -ge 2 ] && break; sleep 2; done
  k get deploy,rs,pods -l app=probes -o wide 2>/dev/null; k get deploy probes -o wide
  [ "${n:-0}" -ge 2 ] || fail "deployment/probes: ${n:-0} of 2 Pods Running"
  python3 - "$(k get deploy probes -o json)" "$(k get rs probes -o json)" "$(k get pods -l app=probes -o json)" <<'PY' || fail "the controller references are not a chain"
import json, sys
d, r, pods = (json.loads(x) for x in sys.argv[1:4])
def ctl(o):
    refs = [x for x in o["metadata"].get("ownerReferences", []) if x.get("controller")]
    assert len(refs) == 1, refs
    return refs[0]
assert ctl(r)["uid"] == d["metadata"]["uid"], "replicaset's controller is not the deployment"
for p in pods["items"]:
    assert ctl(p)["uid"] == r["metadata"]["uid"], "a pod's controller is not the replicaset"
print("   controller references: deployment -> replicaset -> %d pods" % len(pods["items"]))
PY
  if [ "${NANOCLOUD_RIG_RESTART:-0}" = "1" ]; then
    echo "== 6b. power-cycle $RIG: the Deployment's Pods run again"
    listen
    fluxor rig power cycle --rig "$RIG" >/dev/null || fail "fluxor rig power cycle"
    sleep 20
    join "$TLOG"
    for i in $(seq 90); do k get --raw /version >/dev/null 2>&1 && break; sleep 2; done
    for i in $(seq 60); do n="$(probes_running)"; [ "${n:-0}" -ge 2 ] && break; sleep 2; done
    [ "${n:-0}" -ge 2 ] || fail "after the power cycle deployment/probes has ${n:-0} of 2 Pods Running"
    echo "   both Pods Running again after the power cycle"
  fi
fi
# 7. The real applications as workload images (NANOCLOUD_RIG_APPS=1; the
#    board carries the image plane: scripts/rig-render-controlplane.sh with
#    NANOCLOUD_RIG_IMAGES=1). Each is pulled from registry.nanocloud.io
#    (pushed there first: scripts/apps-publish.sh), runs as the modules it
#    brought, serves on the board's address, and comes back after the power
#    is cut.
if [ "${NANOCLOUD_RIG_APPS:-0}" = "1" ]; then
  APPS="${NANOCLOUD_RIG_APPS_LIST:-lattice-kv chronicle-http quantum-mqtt}"
  echo "== 7. kubectl apply a Deployment per app: $APPS"
  for app in $APPS; do
    cat <<YAML | k apply -f - >/dev/null || fail "kubectl apply $app"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $app
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels: {app: $app}
  template:
    metadata:
      labels: {app: $app}
    spec:
      runtimeClassName: fluxor
      containers:
      - name: $app
        image: nanocloud/$app:1
YAML
  done
  has_app() { case " $APPS " in *" $1 "*) return 0 ;; esac; return 1; }
  apps_serving() {
    { ! has_app lattice-kv || [ "$(redis-cli -h "$IP" -p 6379 PING 2>/dev/null)" = "PONG" ]; } \
      && { ! has_app chronicle-http || curl -s -m 3 "http://$IP:8080/" | grep -q chronicle; } \
      && { ! has_app quantum-mqtt || timeout 5 mosquitto_pub -h "$IP" -p 9090 -q 1 -t probe -m up >/dev/null 2>&1; }
  }
  apps_running() {
    local app
    for app in $APPS; do
      [ "$(k get pods -l "app=$app" -o jsonpath='{.items[*].status.phase}' 2>/dev/null)" = "Running" ] || return 1
    done
  }
  for i in $(seq 120); do apps_running && apps_serving && break; sleep 3; done
  k get pods -o wide
  apps_running || fail "not every app's Pod is Running"
  apps_serving || fail "not every app answers on $IP (redis :6379, http :8080, mqtt :9090)"
  has_app lattice-kv && echo "   lattice-kv SET/GET -> $(redis-cli -h "$IP" -p 6379 SET greeting hello) $(redis-cli -h "$IP" -p 6379 GET greeting)"
  has_app chronicle-http && echo "   chronicle-http -> $(curl -s -m 3 "http://$IP:8080/" | head -c 40)…"
  has_app quantum-mqtt && echo "   quantum-mqtt PUBLISH (QoS 1) acknowledged"
  if [ "${NANOCLOUD_RIG_RESTART:-0}" = "1" ]; then
    echo "== 7b. power-cycle $RIG: the apps are pulled, run and serve again"
    listen
    fluxor rig power cycle --rig "$RIG" >/dev/null || fail "fluxor rig power cycle"
    sleep 20
    join "$TLOG"
    for i in $(seq 90); do k get --raw /version >/dev/null 2>&1 && break; sleep 2; done
    for i in $(seq 120); do apps_running && apps_serving && break; sleep 3; done
    apps_running && apps_serving || { k get pods -o wide; fail "the apps did not come back after the power cycle"; }
    echo "   every app Running and serving again after the power cycle"
    # lattice keeps its data in the Pod's own storage root on the board's
    # FAT32: what was SET before the cut must still be there.
    if has_app lattice-kv; then
      got="$(redis-cli -h "$IP" -p 6379 GET greeting 2>/dev/null)"
      [ "$got" = "hello" ] || fail "lattice-kv lost its data across the power cycle: GET greeting -> '$got'"
      echo "   lattice-kv GET greeting -> $got (written before the power cycle)"
    fi
  fi
fi
echo "PASS: kubectl drives the bare-metal control plane"
