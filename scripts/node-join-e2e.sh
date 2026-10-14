#!/usr/bin/env bash
# Live E2E: a worker joins the cluster's own PKI, on Linux — two runtimes,
# one replicated store, and no peer key rendered anywhere.
#
#   plane    replica 0, the group's one voter: the packaged control plane
#            rendered under NANOCLOUD_PEER_TRUST=authority — the API's TLS
#            key and both peer link ends born in its vault and certified by
#            its own authority (node_pki over kagi's certificate_endpoint).
#            Replica 1, past its voters, is a worker slot: node_pki declares
#            the slot's Devices (nanocloud-peers/replica-1.server, .client)
#            and a ServiceAccount, node-bootstrap-1, bound to patching and
#            reading those two alone, and its join mints that
#            ServiceAccount's credential beside the operator's. All of it is
#            the packaged render; nothing is spliced in or seeded here.
#   worker   replica 1: the packaged worker graph, rendered under the same
#            trust with what the operator's join handed over — the authority
#            (plane-ca.der) and the slot's credential (node.token). Its link
#            ends' keys are born in its own vault and certified THROUGH THE
#            API: node_pki patches each key into the slot's Device and reads
#            the certificate back, over TLS verified by that authority.
#
# Proven:
#   1. the operator joins (scripts/rig-join.sh), and is handed the slot's
#      credential and the authority;
#   2. the slot's credential reads and patches its own Devices and nothing
#      else — another slot's Device, a Pod, a Secret are refused;
#   3. the worker's two link ends are certified as replica-1.server / .client
#      by the plane's authority, the Devices say so, and the plane — its
#      principal bound, its slot past the voters — adds it as a learner
#      with no operator command; it catches up on what was committed before;
#   4. its lease, written on the learner, is decided by the voter: kubectl
#      lists the worker;
#   5. a restarted worker asks again and is certified again (a fresh
#      certificate, never the stale one in its Device), and rejoins.
#
# Each replica is read through its own redis edge. NC_KEEP=1 keeps the run's
# directory.
#
# NC_PLANE=<ip>: the plane is a board's instead — pi5-a under
# packaging/rig/pi5-controlplane.scenario.toml, rendered with this host as
# slot 1 (`echo "0=<ip>:7450 1=<this host>:7451" > target/pi5/render/peers`)
# and joined by scripts/rig-controlplane-driver.sh, whose join left the
# kubeconfig, ca.der and node-1.token in target/pi5 (NC_PLANE_JOIN). Steps
# 0-2 are the board's; this host runs the worker, 3-7.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOARD="${NC_PLANE:-}"
if [ -n "$BOARD" ]; then
  API_PORT=6443
  PLANE_HOST="$BOARD"
  WORKER_IP="${NC_WORKER_IP:-$(ip -4 route get "$BOARD" | sed -n 's/.* src \([0-9.]*\).*/\1/p')}"
else
  API_PORT=7443
  PLANE_HOST=127.0.0.1
fi
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for c in fluxor kubectl openssl curl redis-cli; do
  command -v "$c" >/dev/null || { echo "FAIL: $c not found"; exit 1; }
done

D="$(mktemp -d /tmp/nc-node-join-XXXXXX)"
REPLICAS="plane worker"
KCDIR="$D/operator"
if [ -n "$BOARD" ]; then
  REPLICAS="worker"
  KCDIR="${NC_PLANE_JOIN:-$ROOT/target/pi5}"
fi
declare -A PID=()
declare -A RID=([plane]=0 [worker]=1)
stop() { # stop <replica>
  if [ -n "${PID[$1]:-}" ]; then
    kill "${PID[$1]}" 2>/dev/null || true
    wait "${PID[$1]}" 2>/dev/null || true
    PID[$1]=""
  fi
}
cleanup() {
  for r in $REPLICAS; do stop "$r"; done
  [ -n "${NC_KEEP:-}" ] && echo "kept $D" || rm -rf "$D"
}
trap cleanup EXIT
fail() {
  echo "FAIL: $1"
  for r in $REPLICAS; do
    echo "--- $r"
    grep -a "node_pki\|\[pr\]\|\[tls\] identity" "$D/$r/run.log" 2>/dev/null | tail -12 || true
    tail -12 "$D/$r/run.log" 2>/dev/null || true
  done
  exit 1
}

# Ports: replica i's peer listener at BASE+i, its reader at BASE+4+i.
BASE=$((21500 + ($$ % 400) * 8))
PEERS="0=127.0.0.1:$BASE 1=127.0.0.1:$((BASE + 1))"
[ -z "$BOARD" ] || PEERS="0=$BOARD:7450 1=$WORKER_IP:7451"
peer_port() { if [ -n "$BOARD" ]; then echo 7451; else echo "$((BASE + ${RID[$1]}))"; fi; }
for r in $REPLICAS; do mkdir -p "$D/$r/pki"; nc_lattice_prepare "$D/$r"; done

# render <replica> <role> <template> <out>: the packaged render, this node's
# values, the cluster's own authority behind the peer links.
render() {
  NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PKI="$D/$1/pki" NANOCLOUD_ROLE="$2" \
    NANOCLOUD_PEER_TRUST=authority NANOCLOUD_PLANE_API="$PLANE_HOST:$API_PORT" \
    NANOCLOUD_REPLICA_ID="${RID[$1]}" NANOCLOUD_NODE_NAME="$1" NANOCLOUD_VOTERS=1 \
    NANOCLOUD_PEER_PORT="$(peer_port "$1")" NANOCLOUD_PEERS="$PEERS" \
    NANOCLOUD_REPLICA_FRAGMENT="$ROOT/packaging/debian/fluxor-replica.yaml" \
    NANOCLOUD_KUBELET_FRAGMENT="$ROOT/packaging/debian/fluxor-podlifecycle.yaml" \
    sh "$ROOT/packaging/debian/nanocloud-graph.sh" "$3" "$4"
}

# splice <graph> <modules> <wiring>: append modules and edges to a graph.
splice() {
  python3 - "$@" <<'PY'
import sys
path, mods_add, wire_add = sys.argv[1:4]
mods, wiring = open(path).read().split("\nwiring:\n", 1)
open(path, "w").write(mods.rstrip("\n") + "\n" + mods_add.rstrip("\n") + "\n\nwiring:\n"
                      + wiring.rstrip("\n") + "\n" + wire_add.rstrip("\n") + "\n")
PY
}
reader() { # reader <replica-id> <with-router>: the test's GET surface
  printf '  - name: redis_edge_anchor\n    params:\n      listen_port: %s\n' "$((BASE + 4 + $1))"
  [ "$2" = 1 ] && printf '  - name: kv_request_router\n    params:\n      lin_reads: 0\n      replicated: 1\n'
  return 0
}
reader_wiring() { # reader_wiring <with-router>
  cat <<'YAML'
  - from: linux_net.net_out
    to: redis_edge_anchor.net_in
    buffer_bytes: 65536
  - from: redis_edge_anchor.net_out
    to: linux_net.net_in
  - from: redis_edge_anchor.kv_out
    to: kv_request_router.redis_in
  - from: kv_request_router.redis_out
    to: redis_edge_anchor.kv_in
YAML
  [ "$1" = 1 ] && cat <<'YAML'
  - from: kv_request_router.kv_out
    to: kv_state_worker_store.commands
  - from: kv_state_worker_store.responses
    to: kv_request_router.kv_in
  - from: kv_request_router.proposal_out
    to: gateway.client_requests
  - from: gateway.client_responses
    to: kv_request_router.gateway_reject
YAML
  return 0
}

store_put() { # store_put <key> <value>: into the plane's host log
  python3 - "$D/plane/store.log" "$1" "$2" <<'PY'
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

SEAL_plane="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
SEAL_worker="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
start() { # start <replica>: its vault sealed under its own device key
  local seal="SEAL_$1"
  mkdir -p "$D/$1/vault"
  env -C "$D/$1" FLUXOR_SEAL_KEY="${!seal}" FLUXOR_VAULT_DIR="$D/$1/vault" RUST_LOG="${NC_LOG:-warn}" \
    "$FLUXOR_RUNTIME" --config "$D/$1/config.bin" --modules "$D/$1/modules.bin" >>"$D/$1/run.log" 2>&1 &
  PID[$1]=$!
}
wait_for() { # wait_for <seconds> <what> <cmd...>
  local secs="$1" what="$2" r; shift 2
  local end=$((SECONDS + secs))
  until "$@"; do
    [ "$SECONDS" -lt "$end" ] || fail "$what (after ${secs}s)"
    for r in $REPLICAS; do
      [ -z "${PID[$r]:-}" ] || kill -0 "${PID[$r]}" 2>/dev/null || fail "the $r exited"
    done
    sleep 0.5
  done
}
get() { redis-cli -p "$((BASE + 4 + ${RID[$1]}))" --raw GET "$2" 2>/dev/null || true; }
holds() { [ "$(get "$1" "$2")" = "$3" ]; }
issued() { [ "$(grep -ac "\[node_pki\] certificate issued" "$D/$1/run.log")" -ge "$2" ]; }

if [ -z "$BOARD" ]; then
echo "== 0. the plane's one seed: a record from before the worker =="
store_put /seeded/before-join 1

echo "== 1. the plane: its API and link ends certified by its own authority; slot 1 declared =="
render plane plane "$ROOT/packaging/debian/fluxor-controlplane.yaml" "$D/plane/graph.yaml" \
  || fail "the plane did not render"
splice "$D/plane/graph.yaml" "$(reader 0 0)" "$(reader_wiring 0)"
# Small wal segments, as the board's: snapshots land, and the log below
# them is retired, long before a worker joins — so the worker can only be
# caught up by a copy of the state, never by the log.
python3 - "$D/plane/graph.yaml" <<'PY' || fail "the durability params did not take"
import sys
p = sys.argv[1]; g = open(p).read()
old = "  - name: durability\n    params:\n"
if g.count(old) != 1: sys.exit("durability block")
open(p, "w").write(g.replace(old, old + "      segment_bytes: 8192\n", 1))
PY
grep -q '^    join_nodes: "1"$' "$D/plane/graph.yaml" || fail "the plane's join declares no slot 1"
grep -q "^      voters: 1$" "$D/plane/graph.yaml" || fail "the plane admits no learner slot"
for leftover in cert_file key_file trust.pem; do
  ! grep -v '^\s*#' "$D/plane/graph.yaml" | grep -q "$leftover" || fail "the plane renders key material ($leftover)"
done
nc_build_workload "$ROOT" "$D/plane/graph.yaml" "$D/plane/config.bin" "$D/plane/modules.bin" >/dev/null \
  || fail "the plane did not build"


start plane
wait_for 60 "the plane announced no join secret" grep -aq '\[join\] secret=' "$D/plane/run.log"
wait_for 20 "the plane's identities were not all certified" issued plane 3
JOIN="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/plane/run.log" | tail -1)"
SECRET="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
CA_SHA="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
echo "   the API and both link ends certified; join announced"

echo "== 2. the operator joins, and is handed the slot's credential and the authority =="
mkdir -p "$D/operator"
KC="$KCDIR/kubeconfig"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$API_PORT" "$KC" >/dev/null \
  || fail "the operator's join did not complete"
[ -s "$KCDIR/node-1.token" ] && [ -s "$KCDIR/ca.der" ] || fail "the join handed over no slot credential"
[ "$(stat -c %a "$KCDIR/node-1.token")" = 600 ] || fail "the slot credential is readable by others"
SUB="$(python3 - "$KCDIR/node-1.token" <<'PY'
import base64, json, sys
p = open(sys.argv[1]).read().split(".")[1]
print(json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))["sub"])
PY
)"
[ "$SUB" = "system:serviceaccount:kube-system:node-bootstrap-1" ] || fail "the slot credential is '$SUB'"
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get namespaces >/dev/null || fail "the operator's kubeconfig does not work"
echo "   operator joined; node-1.token = $SUB"
else
  echo "== 0-2. the board's plane at $BOARD, joined (target/pi5); this host ($WORKER_IP) is slot 1 =="
  KC="$KCDIR/kubeconfig"
  for f in "$KC" "$KCDIR/ca.der" "$KCDIR/node-1.token"; do
    [ -s "$f" ] || fail "no $f: run scripts/rig-controlplane-driver.sh against a plane rendered with slot 1"
  done
  k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
  k get namespaces >/dev/null || fail "the board's API does not answer the joined kubeconfig"
fi

if [ -z "$BOARD" ]; then
  echo "== 2b. the plane compacts: its log below a snapshot is retired before the worker exists =="
  cm() { printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s","namespace":"default"},"data":{"k":"%s"}}' "$1" "$2" \
    | k create --validate=false -f - >/dev/null; }
  PAD="$(head -c 768 /dev/zero | tr '\0' 'x')"
  for i in $(seq 1 60); do cm "before-$i" "$PAD" || fail "the operator could not write before-$i"; done
  segs() { ls "$D/plane/wal" 2>/dev/null | grep -c 'seg' || true; }
  snaps() { ls "$D/plane/wal" 2>/dev/null | grep -c 'snap_' || true; }
  wait_for 120 "the plane never snapshotted" sh -c "ls '$D/plane/wal' | grep -q snap_"
  wait_for 60 "the plane never retired its first wal segment" sh -c "! ls '$D/plane/wal' | grep -q 'seg_00000000'"
  echo "   $(snaps) snapshot(s), $(segs) wal segment(s) left, the oldest $(ls "$D/plane/wal" | grep seg | head -1)"
fi

echo "== 3. the slot's credential: its own Devices, and nothing else =="
N=kubernetes.default.svc.cluster.local
openssl x509 -inform DER -in "$KCDIR/ca.der" -out "$D/plane-ca.pem"
as_slot() { # as_slot <method> <path> [body]: the status code
  curl -sS -o /dev/null -w '%{http_code}' --max-time 15 --cacert "$D/plane-ca.pem" \
    --resolve "$N:$API_PORT:$PLANE_HOST" -X "$1" \
    -H "Authorization: Bearer $(cat "$KCDIR/node-1.token")" \
    -H 'content-type: application/merge-patch+json' ${3:+--data "$3"} "https://$N:$API_PORT$2"
}
DEV=/apis/nanocloud.io/v1/namespaces/nanocloud-peers/devices
wait_for 30 "the slot could not read its own Device" [ "$(as_slot GET "$DEV/replica-1.server")" = 200 ]
for probe in "GET $DEV/replica-2.server" "PATCH $DEV/replica-2.server" "PATCH $DEV/replica-2.server/status" \
             "GET /api/v1/namespaces/default/pods" "GET /api/v1/namespaces/kube-system/secrets" "GET $DEV"; do
  body=""
  case "$probe" in PATCH*) body='{"spec":{"publicKey":"x"}}' ;; esac
  code="$(as_slot $probe "$body")"
  [ "$code" = 403 ] || fail "the slot credential was answered $code for $probe"
done
echo "   its Device 200; another slot's Device, Pods, Secrets, the Device list: 403"

echo "== 4. the worker: rendered with the join's credential, no key of its own on disk =="
cp "$KCDIR/ca.der" "$D/worker/pki/plane-ca.der"
cp "$KCDIR/node-1.token" "$D/worker/pki/node.token"
render worker node "$ROOT/packaging/debian/fluxor-worker.yaml" "$D/worker/graph.yaml" \
  || fail "the worker did not render"
splice "$D/worker/graph.yaml" "$(reader 1 1)" "$(reader_wiring 1)"
grep -q 'remote: "/apis/nanocloud.io/v1/namespaces/nanocloud-peers/devices/replica-1.server"' "$D/worker/graph.yaml" \
  || fail "the worker's server end is not certified through its slot's Device"
! grep -q "^      voters:" "$D/worker/graph.yaml" || fail "a worker admits learners"
nc_build_workload "$ROOT" "$D/worker/graph.yaml" "$D/worker/config.bin" "$D/worker/modules.bin" >/dev/null \
  || fail "the worker did not build"
start worker

echo "== 5. both link ends certified through the API; the plane admits the slot itself =="
wait_for 60 "the worker's link ends were not certified" issued worker 2
spiffe() { k get --raw "$DEV/replica-1.$1" | python3 -c 'import json,sys; s=json.load(sys.stdin).get("status",{}); print(s.get("spiffe",""), bool(s.get("certificate")))'; }
for role in server client; do
  [ "$(spiffe "$role")" = "spiffe://nanocloud.local/device/nanocloud-peers/replica-1.$role True" ] \
    || fail "the $role Device does not carry its certificate: $(spiffe "$role")"
done
if [ -z "$BOARD" ]; then
  wait_for 30 "the plane never admitted the slot" grep -aq "\[pr\] add learner p=1 asked" "$D/plane/run.log"
  wait_for 60 "the worker never caught up" holds worker /seeded/before-join 1
  # The log behind the plane's snapshot was retired before the worker
  # existed: only a copy of the state can have brought it this far.
  grep -aq "\[kvw\] portable snapshot installed" "$D/worker/run.log" \
    || fail "the worker caught up without the plane's state copy (was the log not compacted?)"
  has_cm() { redis-cli -p "$((BASE + 5))" --raw GET /configmaps/default/before-1 2>/dev/null | grep -q before-1; }
  has_cm || fail "the worker lacks a write the plane compacted away"
else
  # The board's own logs are its telemetry: what it committed before the
  # worker existed, held here, is the admission.
  wait_for 90 "the worker never caught up" sh -c "redis-cli -p $((BASE + 5)) --raw GET /namespaces/nanocloud-peers 2>/dev/null | grep -q nanocloud-peers"
  # The driver's Deployment was written before this worker existed.
  if k get deployment hello >/dev/null 2>&1; then
    wait_for 60 "the worker lacks the plane's earlier Deployment" \
      sh -c "redis-cli -p $((BASE + 5)) --raw GET /deployments.apps/default/hello 2>/dev/null | grep -q hello"
  fi
  if grep -aq "\[kvw\] portable snapshot installed" "$D/worker/run.log"; then
    echo "   caught up from the board's portable state copy"
  else
    echo "   caught up from the board's log (not yet compacted past the worker)"
  fi
fi
echo "   replica-1.server / .client certified; added as a learner; caught up from the plane's state copy"

echo "== 6. its lease, written on the learner, is decided by the voter =="
# A write from a manifest (JSON): kubectl's typed `create configmap` sends
# protobuf, which this API does not read.
cm() { printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s","namespace":"default"},"data":{"k":"v"}}' "$1" \
  | k create --validate=false -f - >/dev/null; }
wait_for 60 "kubectl never listed the worker" sh -c "kubectl --kubeconfig '$KC' --request-timeout=10s get nodes -o name 2>/dev/null | grep -qx node/worker"
cm after-join || fail "the operator could not write"
wait_for 30 "the worker never received a write made after it joined" \
  sh -c "redis-cli -p $((BASE + 5)) --raw GET /configmaps/default/after-join 2>/dev/null | grep -q after-join"
echo "   kubectl lists the worker; a later write reaches it"

echo "== 7. the worker restarts: certified again (fresh), and back in the group =="
OLD="$(k get --raw "$DEV/replica-1.server" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["certificate"])')"
stop worker
: >"$D/worker/run.log"
start worker
wait_for 60 "the restarted worker was not certified again" issued worker 2
NEW="$(k get --raw "$DEV/replica-1.server" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["certificate"])')"
[ "$NEW" != "$OLD" ] || fail "the restart was handed the stale certificate"
cm after-restart || fail "the operator could not write"
wait_for 60 "the restarted worker never rejoined" \
  sh -c "redis-cli -p $((BASE + 5)) --raw GET /configmaps/default/after-restart 2>/dev/null | grep -q after-restart"
echo "   a fresh certificate; the worker is replicating again"

echo "== E2E green: a worker joined by its slot's credential — certified through the API, admitted by its principal =="
