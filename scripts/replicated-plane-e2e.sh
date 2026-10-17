#!/usr/bin/env bash
# The control plane and two workers as three runtimes on one replicated
# store.
#
#   plane   replica 0, the group's one voter: lattice's store worker under
#           clustor's consensus and durability (the packaged replica
#           fragment), and the control plane's chains as
#           fluxor-controlplane.yaml ships them (node leases, readiness, the
#           scheduler, each node's /pod-specs/).
#   node-b  replica 1, and
#   node-c  replica 2: the packaged worker graph (fluxor-worker.yaml) — a
#           learner, outside the voter set; its lease; its pod plane
#           (fluxor-podlifecycle.yaml: the kubelet's chains and the sandbox
#           runtime) — its replica scoped to its node.
#
# Every peer link is mutual TLS under keys minted for the run, each replica
# bound to its pinned key; a worker joins only when an operator key adds it
# over the wire admin path. Proven:
#   1. the plane's store starts from a host store's log, imported through
#      the group's log, and its chains run;
#   2. nothing reaches a worker before it is added, and an observer key
#      cannot add one;
#   3. added, each catches up on what the plane committed before;
#   4. each worker's lease, written on the learner, is decided by the voter:
#      the plane projects both ready;
#   5. each worker runs its own Pods and no other's: the plane writes each
#      ask into its node's scope (/~node/<node>/pod-specs/), the worker's
#      kubelet reads it by its plain name and runs it, and what it records
#      (/pod-runtime/, /sandbox-status/, /sandbox-logs/, …) lands in its own
#      scope, decided by the voter; a Pod scheduled by the plane goes to
#      one worker, whose image chain asks for its image in its own scope; a
#      Pod bound to a node with no worker is asked of nobody;
#   6. a restarted voter keeps what the log holds and imports only what was
#      appended since;
#   7. every peer link carried peer records only, and no store quarantined.
#
# Each replica is read through its own redis edge (lattice's read surface:
# a GET answers from that replica's store, by the store's own names). NC_KEEP=1
# keeps the run's directory; NC_LOG sets the runtimes' RUST_LOG (default
# warn).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
command -v redis-cli >/dev/null || { echo "FAIL: redis-cli not found"; exit 1; }
for m in store_source store_effect tls peer_router consensus durability gateway operations \
         kv_state_worker_store lattice_apply_bridge ttl_scheduler redis_edge_anchor \
         kv_request_router workload_effect; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-replicated-XXXXXX)"
REPLICAS="plane node-b node-c"
declare -A PID=()
declare -A RID=([plane]=0 [node-b]=1 [node-c]=2)
cleanup() {
  for r in $REPLICAS; do [ -n "${PID[$r]:-}" ] && kill "${PID[$r]}" 2>/dev/null || true; done
  wait 2>/dev/null || true
  [ -n "${NC_KEEP:-}" ] && echo "kept $D" || rm -rf "$D"
}
trap cleanup EXIT
fail() {
  echo "FAIL: $1"
  for r in $REPLICAS; do echo "--- $r"; tail -25 "$D/$r/run.log" 2>/dev/null || true; done
  exit 1
}

# Ports: replica i's peer listener at BASE+i, its reader at BASE+4+i.
BASE=$((21000 + ($$ % 400) * 8))
PEERS="0=127.0.0.1:$BASE 1=127.0.0.1:$((BASE + 1)) 2=127.0.0.1:$((BASE + 2))"

echo "== 0. keys for the run, as postinst provisions each node's =="
# Each node's PKI: its own peer key, every replica's certificate
# (peers/<id>.pem), and the admin keys every node is given (an operator, an
# observer).
mkdir -p "$D/admin"
gen() { # gen <dir> <name>
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$1/$2.key.pem" -out "$1/$2.pem" -days 1 -subj "/CN=nc-$2" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=digitalSignature" \
    -addext "extendedKeyUsage=serverAuth,clientAuth" 2>/dev/null
}
gen "$D/admin" operator
gen "$D/admin" observer
for r in $REPLICAS; do
  P="$D/$r/pki"
  mkdir -p "$P/peers" "$P/operators" "$P/observers" "$D/$r/lattice"
  gen "$P" peer
  openssl x509 -in "$P/peer.pem" -outform DER -out "$P/peer.der"
  openssl ec -in "$P/peer.key.pem" -outform DER -out "$P/peer.key.der" 2>/dev/null
  cp "$P/peer.pem" "$D/admin/replica${RID[$r]}.pem"
done
for r in $REPLICAS; do
  for i in 0 1 2; do cp "$D/admin/replica$i.pem" "$D/$r/pki/peers/$i.pem"; done
  cp "$D/admin/operator.pem" "$D/$r/pki/operators/test.pem"
  cp "$D/admin/observer.pem" "$D/$r/pki/observers/test.pem"
done

# render <replica> <role> <template> <out>: the packaged render, with this
# node's values.
render() {
  NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PKI="$D/$1/pki" NANOCLOUD_ROLE="$2" \
    NANOCLOUD_REPLICA_ID="${RID[$1]}" NANOCLOUD_NODE_NAME="$1" NANOCLOUD_VOTERS=1 \
    NANOCLOUD_PEER_PORT="$((BASE + ${RID[$1]}))" NANOCLOUD_PEERS="$PEERS" \
    NANOCLOUD_REPLICA_FRAGMENT="$ROOT/packaging/debian/fluxor-replica.yaml" \
    NANOCLOUD_KUBELET_FRAGMENT="$ROOT/packaging/debian/fluxor-podlifecycle.yaml" \
    sh "$ROOT/packaging/debian/nanocloud-graph.sh" "$3" "$4"
}

# The shipped chains: module blocks by name, and every edge between them.
shipped() { # shipped <graph.yaml> <out> <name-regex>
  python3 - "$1" "$2" "$3" <<'PY'
import re, sys
src, out, pat = sys.argv[1], sys.argv[2], re.compile(sys.argv[3])
text = open(src).read()
mods_txt = text.split("\nwiring:\n", 1)[0].split("\nmodules:\n", 1)[1]
wire_txt = text.split("\nwiring:\n", 1)[1]
blocks, cur = [], None
for line in mods_txt.split("\n"):
    m = re.match(r"^  - name: (\S+)", line)
    if m:
        cur = [m.group(1), [line]]; blocks.append(cur)
    elif cur and line.startswith("    ") and not line.lstrip().startswith("domain:"):
        cur[1].append(line)
    elif not line.startswith("    "):
        cur = None
names = {n for n, _ in blocks if pat.fullmatch(n)}
mods = [l for n, b in blocks if n in names for l in b]
edges, cur = [], None
for line in wire_txt.split("\n"):
    m = re.match(r"^  - from: (\S+)", line)
    if m:
        cur = [line]; edges.append(cur)
    elif cur is not None and line.startswith("    "):
        cur.append(line)
    else:
        cur = None
def ends(e):
    f = re.match(r"^  - from: ([^.\s]+)", e[0]).group(1)
    t = next((re.match(r"^    to: ([^.\s]+)", l) for l in e if l.startswith("    to:")), None)
    return f, t.group(1) if t else None
wire = [l for e in edges if all(x in names for x in ends(e)) for l in e]
open(out + ".mods", "w").write("\n".join(mods) + "\n")
open(out + ".wire", "w").write("\n".join(wire) + "\n")
print(len(names))
PY
}
NP="$(shipped "$ROOT/packaging/debian/fluxor-controlplane.yaml" "$D/plane-chains" \
  '(nl_(nodes|seen|leases|stale)|ds_(nodes|mark)|sc_(unbound|patch|events)|ps_(pods|spec|tombs|gone))')"
[ "$NP" = 13 ] || fail "fluxor-controlplane.yaml: expected 13 plane chain modules, found $NP"

# A test's reader on each replica: lattice's redis edge through a router
# whose writes go to the group, as the control plane's router's do.
reader_modules() { # reader_modules <replica-id>
  cat <<YAML
  # The test's reader: a GET answered from this replica's store.
  - name: redis_edge_anchor
    params:
      listen_port: $((BASE + 4 + $1))
  - name: kv_request_router
    params:
      lin_reads: 0
      replicated: 1
YAML
}
reader_wiring() {
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
  - from: kv_request_router.kv_out
    to: kv_state_worker_store.commands
  - from: kv_state_worker_store.responses
    to: kv_request_router.kv_in
  - from: kv_request_router.proposal_out
    to: gateway.client_requests
  - from: gateway.client_responses
    to: kv_request_router.gateway_reject
  - from: kv_request_router.lin_read_out
    to: consensus.read
  - from: consensus.applied
    to: kv_request_router.read_release
YAML
}

# The plane: the store's replica (the packaged fragment, rendered as replica
# 0, the group's one voter) and the control plane's chains as it ships them.
{
  printf 'target: linux\ntick_us: 1000\nscheduler:\n  accept_cycles: true\nplatform:\n  net: {}\nmodules:\n__REPLICA_MODULES__\n'
  reader_modules 0
  cat "$D/plane-chains.mods"
  printf 'wiring:\n__REPLICA_WIRING__\n'
  reader_wiring
  cat "$D/plane-chains.wire"
} >"$D/plane-template.yaml"
render plane plane "$D/plane-template.yaml" "$D/plane/graph.yaml" || fail "the plane did not render"
grep -q 'scope: ""' "$D/plane/graph.yaml" || fail "the control plane's replica is scoped"
# The workers: the packaged worker graph, rendered as replicas 1 and 2.
for r in node-b node-c; do
  render "$r" node "$ROOT/packaging/debian/fluxor-worker.yaml" "$D/$r/worker.yaml" \
    || fail "the $r worker did not render"
  python3 - "$D/$r/worker.yaml" "$D/$r/graph.yaml" "$(reader_modules "${RID[$r]}")" "$(reader_wiring)" <<'PY'
import sys
src, out, rmods, rwire = sys.argv[1:]
mods, wiring = open(src).read().split("\nwiring:\n", 1)
open(out, "w").write(mods.rstrip("\n") + "\n" + rmods + "\nwiring:\n"
                     + wiring.rstrip("\n") + "\n" + rwire + "\n")
PY
  grep -q "label: \"$r\"" "$D/$r/graph.yaml" || fail "the $r worker's lease is not its own"
  grep -q "scope: \"$r\"" "$D/$r/graph.yaml" || fail "the $r worker's replica is not scoped to it"
done

# The plane's store starts from a host store's log, as a control plane
# upgraded from the host store does: a Pod bound to each worker (each
# echoes its name and exits), one the scheduler binds, and one bound to a
# node with no worker.
POD_B='{"metadata":{"name":"b","namespace":"default","uid":"uid-b"},"spec":{"nodeName":"node-b","restartPolicy":"Never","containers":[{"name":"b","command":["/bin/echo","ran-on-node-b"]}]}}'
POD_C='{"metadata":{"name":"c","namespace":"default","uid":"uid-c"},"spec":{"nodeName":"node-c","restartPolicy":"Never","containers":[{"name":"c","command":["/bin/echo","ran-on-node-c"]}]}}'
POD_WEB='{"metadata":{"name":"web","namespace":"default","uid":"uid-web"},"spec":{"restartPolicy":"Always","containers":[{"name":"web","image":"nginx:1.27","command":["nginx","-g","daemon off;"]}]}}'
POD_FAR='{"metadata":{"name":"far","namespace":"default","uid":"uid-far"},"spec":{"nodeName":"node-x","containers":[{"name":"far","image":"busybox:1"}]}}'

# Append one put to the plane's host store log.
store_put() { # store_put <key> <value>
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
store_put /seeded/before-join 1
store_put /pods/default/b "$POD_B"
store_put /pods/default/c "$POD_C"
store_put /pods/default/web "$POD_WEB"
store_put /pods/default/far "$POD_FAR"

for r in $REPLICAS; do
  nc_build_workload "$ROOT" "$D/$r/graph.yaml" "$D/$r/config.bin" "$D/$r/modules.bin" >/dev/null \
    || fail "the $r graph did not build"
done
start() { # start <replica>
  env -C "$D/$1" RUST_LOG="${NC_LOG:-warn}" "$FLUXOR_RUNTIME" \
    --config "$D/$1/config.bin" --modules "$D/$1/modules.bin" >>"$D/$1/run.log" 2>&1 &
  PID[$1]=$!
}
# What a replica holds at <key> (the store's own name), read from it.
get() { # get <replica> <key>
  redis-cli -p "$((BASE + 4 + ${RID[$1]}))" --raw GET "$2" 2>/dev/null || true
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
holds() { [ "$(get "$1" "$2")" = "$3" ]; }
has() { case "$(get "$1" "$2")" in *"$3"*) return 0;; *) return 1;; esac; }
absent() { [ -z "$(get "$1" "$2")" ]; }

# One wire admin command to the plane over mTLS as <who>; prints its status.
admin() { # admin <who> <op> <replica>
  python3 - "$BASE" "$D/admin/$1.pem" "$D/admin/$1.key.pem" "$2" "$3" <<'PY'
import subprocess, sys, time, select, os
port, cert, key, op, rid = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4], 0), int(sys.argv[5])
p = subprocess.Popen(["openssl", "s_client", "-quiet", "-connect", "127.0.0.1:" + port,
                      "-cert", cert, "-key", key],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
# MSG_ADMIN_COMMAND, u16 LE length, op, body
p.stdin.write(bytes([0x12, 2, 0, op, rid])); p.stdin.flush()
got, end = b"", time.time() + 10
while time.time() < end:
    r, _, _ = select.select([p.stdout], [], [], 0.2)
    if r:
        b = os.read(p.stdout.fileno(), 256)
        if not b: break
        got += b
    i = got.find(b"\x13")  # MSG_ADMIN_RESPONSE
    if i >= 0 and len(got) >= i + 4:
        print("0x%02x" % got[i + 3]); break
else:
    print("none")
p.kill(); p.wait()
PY
}
ADD_LEARNER=0x0C

echo "== 1. the plane, alone: one voter imports its host log through the group's, its chains run =="
start plane
wait_for 30 "the plane never imported its host log" holds plane /seeded/before-join 1
wait_for 10 "the import was not recorded" grep -q "store.log imported n=5" "$D/plane/run.log"
wait_for 20 "node-b's Pod was never asked of it" has plane /~node/node-b/pod-specs/uid-b node=node-b
wait_for 20 "node-c's Pod was never asked of it" has plane /~node/node-c/pod-specs/uid-c node=node-c
wait_for 20 "the Pod bound to node-x was never asked" has plane /~node/node-x/pod-specs/uid-far node=node-x
absent plane /pod-specs/uid-b || fail "an ask was written in no node's scope"
absent plane /ready-nodes/node-b || fail "node-b ready before it ever ran"
echo "   5 records imported and committed on one voter; each bound Pod asked in its node's scope; web waits for a ready node"

echo "== 2. the workers boot outside the voter set: learners, given nothing =="
start node-b
start node-c
serving() { [ "$(redis-cli -p "$((BASE + 4 + $1))" PING 2>/dev/null)" = PONG ]; }
wait_for 30 "node-b's replica never served" serving 1
wait_for 30 "node-c's replica never served" serving 2
wait_for 30 "node-b's peer link never came up" grep -q "\[pr\] conn up p=1" "$D/plane/run.log"
wait_for 30 "node-c's peer link never came up" grep -q "\[pr\] conn up p=2" "$D/plane/run.log"
sleep 3
absent node-b /seeded/before-join || fail "a learner nobody added received the log"
absent plane /nodes/node-b || fail "an unadded learner's write was decided"
S="$(admin observer "$ADD_LEARNER" 1)"
[ "$S" != 0x00 ] || fail "an observer key added a learner"
sleep 2
absent node-b /seeded/before-join || fail "the observer's refused add took effect"
echo "   learners, empty; the observer's ADD_LEARNER refused ($S)"

echo "== 3. an operator adds both: each catches up on what was committed before =="
for i in 1 2; do
  S="$(admin operator "$ADD_LEARNER" "$i")"
  [ "$S" = 0x00 ] || fail "the operator's ADD_LEARNER $i answered $S"
done
wait_for 30 "node-b never caught up" holds node-b /seeded/before-join 1
wait_for 30 "node-c never caught up" holds node-c /seeded/before-join 1
wait_for 10 "node-c lacks the earlier ask of node-b" has node-c /~node/node-b/pod-specs/uid-b node=node-b
echo "   added; each holds the plane's earlier writes (every node's, under each node's scope)"

echo "== 4. each worker's lease, written on the learner, is decided by the voter =="
for r in node-b node-c; do
  wait_for 30 "$r's registration never reached the plane" has plane "/nodes/$r" '"ready":true'
  wait_for 20 "the plane never projected $r ready" holds plane "/ready-nodes/$r" 1
done
echo "   /nodes/node-b and /nodes/node-c ready; both projected to /ready-nodes/"

echo "== 5. each worker runs its own Pods, and records them in its own scope =="
wait_for 40 "node-b never ran its Pod" has plane /~node/node-b/sandbox-logs/uid-b ran-on-node-b
wait_for 40 "node-c never ran its Pod" has plane /~node/node-c/sandbox-logs/uid-c ran-on-node-c
wait_for 30 "node-b's Pod never succeeded" has plane /~node/node-b/pod-lifecycle-status/uid-b Succeeded
wait_for 30 "node-c's Pod never succeeded" has plane /~node/node-c/pod-lifecycle-status/uid-c Succeeded
for k in pod-runtime sandboxes sandbox-status sandbox-logs; do
  absent plane "/~node/node-b/$k/uid-c" || fail "node-b acted on node-c's Pod (/$k/)"
  absent plane "/~node/node-c/$k/uid-b" || fail "node-c acted on node-b's Pod (/$k/)"
  absent plane "/$k/uid-b" || fail "a worker's /$k/ landed in no node's scope"
done
for r in node-b node-c; do
  absent plane "/~node/$r/pod-runtime/uid-far" || fail "$r acted on node-x's Pod"
done
echo "   node-b ran uid-b, node-c ran uid-c (their output in /~node/<node>/sandbox-logs/); neither touched the other's or node-x's"
wait_for 30 "web was never scheduled" has plane /pods/default/web '"nodeName":"node-'
N="$(get plane /pods/default/web | sed -n 's/.*"nodeName":"\(node-[bc]\)".*/\1/p')"
[ -n "$N" ] || fail "web was bound to no worker: $(get plane /pods/default/web)"
SPEC="$(get plane "/~node/$N/pod-specs/uid-web")"
for w in "node=$N" "image=nginx:1.27" "restart=always" "desired=running" "cmd=nginx -g daemon%20off%3B"; do
  case "$SPEC" in *"$w"*) ;; *) fail "/~node/$N/pod-specs/uid-web lacks '$w': '$SPEC'";; esac
done
wait_for 30 "$N's image chain never asked for web's image" \
  holds plane "/~node/$N/image-requests/nginx+1.27" "reg=;repo=nginx;tag=1.27;pin="
OTHER=node-b; [ "$N" = node-b ] && OTHER=node-c
absent plane "/~node/$OTHER/image-requests/nginx+1.27" || fail "$OTHER asked for an image it was not asked to run"
holds "$N" "/~node/$N/image-requests/nginx+1.27" "reg=;repo=nginx;tag=1.27;pin=" || fail "$N lacks its own image request"
echo "   web scheduled onto $N; its image asked for in $N's scope only"

echo "== 6. the voter restarts: its store is the log's, and its import resumes where it ended =="
kill "${PID[plane]}"; wait "${PID[plane]}" 2>/dev/null || true
store_put /seeded/after-restart 2
start plane
wait_for 30 "the restarted plane never imported the appended record" holds plane /seeded/after-restart 2
for r in node-b node-c; do
  wait_for 30 "$r never received the appended record" holds "$r" /seeded/after-restart 2
done
[ "$(grep -o "store.log imported n=[0-9]*" "$D/plane/run.log" | tail -1)" = "store.log imported n=1" ] \
  || fail "the restart imported other than the one appended record: $(grep -o "store.log imported n=[0-9]*" "$D/plane/run.log" | tr '\n' ' ')"
has plane /pods/default/web "\"nodeName\":\"$N\"" || fail "the restarted plane lost its binding"
holds "$N" "/~node/$N/pod-specs/uid-web" "$SPEC" || fail "$N lost its ask"
echo "   resumed past the 5 imported, imported 1; bindings and asks intact"

echo "== 7. the peer links carried peer records only =="
for r in $REPLICAS; do
  if grep -q "record desync\|client resp to a peer link" "$D/$r/run.log"; then
    fail "the $r's peer stream carried something else"
  fi
  if grep -q "QUARANTINE\|scope refused" "$D/$r/run.log"; then
    fail "the $r's store refused to serve"
  fi
done
echo "   no desynced record, no client response on a replica's link; every store served"

echo "== E2E green: two workers join by operator key over mTLS, their writes are decided by the voter, and each runs only its own Pods in its own scope =="
