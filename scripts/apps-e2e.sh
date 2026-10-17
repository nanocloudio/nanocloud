#!/usr/bin/env bash
# Live E2E: the real applications as workload images, on a node in the
# bare-metal shape (scripts/workload-image-e2e.sh's render), driven the way
# an operator drives them — `kubectl apply` of a Deployment per app:
#
#   lattice-kv       lattice's standalone key-value node, RESP, its data in
#                    its own storage root (packaging/apps/lattice-kv.yaml)
#   chronicle-http   a Chronicle pipeline behind wave's http        (packaging/apps/chronicle-http.yaml)
#   quantum-mqtt     quantum's MQTT broker, its examples/linux/minimal.yaml as a
#                    workload (scripts/support/to-workload.py), its WAL and raft
#                    metadata in its own storage root; without its admin plane,
#                    `operations` + `http`: operations holds the node's `monitor`
#                    privilege, which the kernel refuses a tenant
#
# Each image is built with `fluxor build --emit=workload` and served by a
# registry stub as `fluxor publish workload` lays it out. Each Deployment's
# Pod is pulled, staged as the modules it brought, reaches Running and
# answers on its port. Then the power is cut — the runtime killed outright,
# no drain — and the node boots again from the same store: the Pods come
# back Running and answer again, with their data: what lattice-kv SET and the
# message quantum-mqtt retained before the cut are there after it.
#
# Then (with the stub) the registry goes away and the power is cut again: a
# node that holds an image — its blobs, the digest its reference resolved to
# — needs no registry to run it (imagePullPolicy IfNotPresent, the default
# for a tag that is not :latest), so every Pod comes back from the node's
# cache, with its data.
#
# PASS = every app Running and serving, before and after each power cut, and
# lattice-kv's and quantum-mqtt's data surviving them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
REG_PORT=5395
KV_PORT=16379
HTTP_PORT=18080
MQTT_PORT=9090   # quantum's peer_router default
# The registry the node pulls from: a stub this script serves (default), or a
# real one over TLS — NANOCLOUD_APPS_REGISTRY=<host:port> with its CA in
# NANOCLOUD_APPS_REGISTRY_CA (PEM): this pushes its own images there first
# (scripts/apps-publish.sh, tag `e2e`, on the test's ports — never the
# packaged ones another service on this host may hold) and pulls them.
REAL_REGISTRY="${NANOCLOUD_APPS_REGISTRY:-}"
TAG=1
[ -n "$REAL_REGISTRY" ] && TAG=e2e
# Every app by default; APPS narrows a local run (the suite runs them all).
APPS="${APPS:-lattice-kv chronicle-http quantum-mqtt}"
has_app() { case " $APPS " in *" $1 "*) return 0 ;; esac; return 1; }
QROOT="$(cd "$ROOT/../quantum" && pwd)"

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/workload-node.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-apps-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

if [ -n "$REAL_REGISTRY" ]; then
echo "== 1. the apps as workload images, in $REAL_REGISTRY (pulled over TLS) =="
[ -r "${NANOCLOUD_APPS_REGISTRY_CA:-}" ] || fail "NANOCLOUD_APPS_REGISTRY_CA must name the registry's CA (PEM)"
openssl x509 -in "$NANOCLOUD_APPS_REGISTRY_CA" -outform DER -out "$D/registry-ca.der"
# Pushed through the registry's default port (its uploads redirect there).
NANOCLOUD_REGISTRY_CA="$NANOCLOUD_APPS_REGISTRY_CA" APPS_TAG=e2e APPS_KV_PORT="$KV_PORT" \
  APPS_HTTP_PORT="$HTTP_PORT" bash "$ROOT/scripts/apps-publish.sh" "${REAL_REGISTRY%%:*}" \
  || fail "the images did not reach $REAL_REGISTRY"
else
echo "== 1. the apps as workload images, served as a registry would =="
sed "s/listen_port: 6379/listen_port: $KV_PORT/" "$ROOT/packaging/apps/lattice-kv.yaml" >"$D/lattice-kv.yaml"
sed "s/port: 8080/port: $HTTP_PORT/" "$ROOT/packaging/apps/chronicle-http.yaml" >"$D/chronicle-http.yaml"
has_app lattice-kv && wn_publish lattice-kv "$D/lattice-kv.yaml"
has_app chronicle-http && wn_publish chronicle-http "$D/chronicle-http.yaml"
# quantum's broker resolves its modules (and clustor's, by its
# module_search_paths) from its own tree: built there.
if has_app quantum-mqtt; then
mkdir -p "$QROOT/target/nanocloud-apps"
python3 -B "$ROOT/scripts/support/to-workload.py" "$QROOT/examples/linux/minimal.yaml" \
  "$QROOT/target/nanocloud-apps/quantum-mqtt.yaml" --drop operations --drop http \
  "$QROOT/modules" "$QROOT/../clustor/modules" "$QROOT/../wave/modules" 2>"$D/to-workload.log" \
  || { cat "$D/to-workload.log"; fail "quantum's graph did not convert"; }
wn_publish quantum-mqtt "$QROOT/target/nanocloud-apps/quantum-mqtt.yaml" "$QROOT"
fi
wn_registry "$REG_PORT"
fi

echo "== 2. the node: the plane, its kubelet's graph-pod slice and image plane, one scoped store =="
wn_seed_identity
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
if [ -n "$REAL_REGISTRY" ]; then
  wn_splice "$D/graph.yaml" node-1 "$D/blobs" "$REAL_REGISTRY" "$D/registry-ca.der" || fail "the splice did not apply"
else
  wn_splice "$D/graph.yaml" node-1 "$D/blobs" "localhost:$REG_PORT" || fail "the splice did not apply"
fi
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
boot() {
  env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" RUST_LOG=info "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  nc_store_ready "$STORE" 60 || fail "the store never answered"
  local r=""
  for _ in $(seq 90); do
    r="$(api GET /version 2>/dev/null || true)"
    case "$r" in 200\|*) return 0 ;; esac
    sleep 1
  done
  fail "no API: $r"
}
boot
cat >"$D/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters:
- name: node
  cluster:
    server: https://127.0.0.1:$PORT
    insecure-skip-tls-verify: true
users:
- name: admin
  user:
    token: $(cat "$D/token.admin")
contexts:
- name: node
  context: {cluster: node, user: admin, namespace: default}
current-context: node
KC
k() { kubectl --kubeconfig "$D/kubeconfig" --request-timeout=20s "$@"; }
k get --raw /version >/dev/null || fail "kubectl could not reach the API"
echo "   kubectl reaches the API"

echo "== 3. kubectl apply: a Deployment per app =="
for app in $APPS; do
  cat >"$D/$app.deploy.yaml" <<YAML
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
        image: nanocloud/$app:$TAG
YAML
  # The API answers /version before its authenticator holds the token key
  # (ServiceUnavailable: no verification key): a first write waits it out.
  out=""
  for _ in $(seq 60); do
    out="$(k apply -f "$D/$app.deploy.yaml" 2>&1)" && break
    case "$out" in *ServiceUnavailable*|*Forbidden*) sleep 2 ;; *) fail "kubectl apply $app: $out" ;; esac
  done
  case "$out" in *created*|*configured*|*unchanged*) echo "   $out" ;; *) fail "kubectl apply $app: $out" ;; esac
done

serving() { # serving: every app answers on its port
  { ! has_app lattice-kv || [ "$(redis-cli -p "$KV_PORT" PING 2>/dev/null)" = "PONG" ]; } \
    && { ! has_app chronicle-http || curl -s -m 2 "http://127.0.0.1:$HTTP_PORT/" | grep -q "chronicle"; } \
    && { ! has_app quantum-mqtt || timeout 5 mosquitto_pub -h 127.0.0.1 -p "$MQTT_PORT" -q 1 -t probe -m up >/dev/null 2>&1; }
}
running() { # running <app>: its Pod's phase
  k get pods -l "app=$1" -o jsonpath='{.items[*].status.phase}' 2>/dev/null || true
}
settle() { # settle <what>: every app Running and serving within 180 s
  local app
  for _ in $(seq 180); do
    local all=1
    for app in $APPS; do [ "$(running "$app")" = "Running" ] || all=0; done
    if [ "$all" = 1 ] && serving; then
      return 0
    fi
    sleep 1
  done
  for app in $APPS; do echo "   $app: phase='$(running "$app")'"; done
  k get pods -o wide 2>&1 | sed 's/^/   /'
  grep -a "\[live\]\|EXHAUSTED" "$D/run.log" | tail -3 | cut -c1-200
  fail "the apps did not all come up $1"
}

echo "== 4. the Pods pull, run and serve =="
settle "after apply"
has_app lattice-kv && echo "   lattice-kv:     PING -> PONG; SET -> $(redis-cli -p "$KV_PORT" SET greeting hello); GET -> $(redis-cli -p "$KV_PORT" GET greeting)"
has_app chronicle-http && echo "   chronicle-http: GET / -> $(curl -s -m 2 "http://127.0.0.1:$HTTP_PORT/" | head -c 60)…"
if has_app quantum-mqtt; then
  echo "   quantum-mqtt:   PUBLISH (QoS 1) on :$MQTT_PORT -> acknowledged"
  # A retained message: the broker acknowledges a QoS 1 PUBLISH once it is
  # committed to its WAL, and a new subscriber is handed the retained one.
  timeout 10 mosquitto_pub -h 127.0.0.1 -p "$MQTT_PORT" -q 1 -r -t nc/retained -m kept \
    || fail "quantum-mqtt did not acknowledge a retained PUBLISH"
  got="$(timeout 15 mosquitto_sub -h 127.0.0.1 -p "$MQTT_PORT" -q 1 -t nc/retained -C 1 -W 10 2>/dev/null || true)"
  [ "$got" = "kept" ] || fail "quantum-mqtt did not deliver its retained message: '$got'"
  echo "   quantum-mqtt:   retained PUBLISH nc/retained=kept -> a new subscriber gets '$got'"
fi
k get pods 2>&1 | sed 's/^/   /'

cut() { # cut: the power cut — the runtime killed outright, no drain
  kill -9 "$RUNTIME_PID"
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
  serving && fail "still serving with the node down: the test is not measuring the node"
  return 0
}
survived() { # survived <when>: the data written before the first cut is there
  # lattice keeps its data in its own storage root, which the restart kept
  # and the Pod (same identity) reopened: what was SET before the cut.
  if has_app lattice-kv; then
    got="$(redis-cli -p "$KV_PORT" GET greeting 2>/dev/null)"
    [ "$got" = "hello" ] || fail "lattice-kv lost its data $1: GET greeting -> '$got'"
    echo "   lattice-kv: GET greeting -> $got (written before the first cut)"
  fi
  # quantum's broker keeps its WAL and raft metadata in its own storage root
  # too: the retained message published before the cut is replayed from the
  # WAL and handed to a subscriber that arrives after it.
  if has_app quantum-mqtt; then
    got=""
    for _ in $(seq 10); do
      got="$(timeout 15 mosquitto_sub -h 127.0.0.1 -p "$MQTT_PORT" -q 1 -t nc/retained -C 1 -W 5 2>/dev/null || true)"
      [ "$got" = "kept" ] && break
      sleep 2
    done
    [ "$got" = "kept" ] || fail "quantum-mqtt lost its retained message $1: '$got'"
    echo "   quantum-mqtt: SUBSCRIBE nc/retained -> $got (retained before the first cut)"
  fi
  k get pods 2>&1 | sed 's/^/   /'
}

echo "== 5. the power is cut: the runtime killed outright, then the node boots again =="
asked=""
[ -z "$REAL_REGISTRY" ] && asked="$(wc -l <"$D/reg.log")"
cut
boot
settle "after the power cut"
echo "   every app Running and serving again"
# IfNotPresent (a tag that is not :latest): what the node holds is what it
# runs, so the restart asked the registry for nothing.
if [ -n "$asked" ]; then
  [ "$(wc -l <"$D/reg.log")" = "$asked" ] \
    || { tail -n +"$((asked + 1))" "$D/reg.log" | sed 's/^/   /'; fail "the restart asked the registry again for an image the node holds"; }
  echo "   the registry was asked for nothing: the node ran what it holds"
fi
survived "across the power cut"

# The registry is not the node's to need: what it pulled is in its blob cache
# with the digest each reference resolved to, and a tag that is not :latest is
# IfNotPresent — so the node brings its Pods back from what it holds.
if [ -z "$REAL_REGISTRY" ]; then
echo "== 6. the registry goes away, the power is cut again: the Pods come back from the node's cache =="
kill "$REG_PID" 2>/dev/null || true
wait "$REG_PID" 2>/dev/null || true
REG_PID=""
curl -s -m 2 "http://127.0.0.1:$REG_PORT/v2/" >/dev/null && fail "the registry stub still answers"
cut
boot
settle "after a power cut with the registry down"
echo "   every app Running and serving again, the registry unreachable"
survived "across a power cut with the registry down"
fi

echo "== E2E green: $APPS as workload images — kubectl Deployments pulled, run, serving, and back after a power cut, with the registry and without =="
