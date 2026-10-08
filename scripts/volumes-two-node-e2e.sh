#!/usr/bin/env bash
# Encrypted volumes placed across the control plane and a node, end to end:
# two runtimes on one replicated store, each holding its half of the
# pipelines (scripts/support/volumes-split.py, from
# packaging/debian/fluxor-volumes.yaml), against Loam's admin gate (the
# loam-admin template from a Loam checkout) under the node's capability.
#
#   plane   replica 0, the group's one voter: provisioning, grants,
#           snapshots and the rest, Kagi's storage-key service with its
#           ledger and custodians, and the relay that carries node-only
#           effects to the node holding a claim (/~node/<node>/volume-ops/)
#   node-a  replica 1, a learner scoped to its node: attach, detach and the
#           claim's mount, the node effector (the volume's subgraph,
#           unlocked through Kagi, published over NBD), and the relay that
#           carries its Kagi policy effects to the control plane
#           (/volume-asks/)
#
# Proven: a claim provisioned on the plane is attached, mounted and written
# on the node (its Kagi effects answered by the plane through the store),
# snapshotted by the plane (its flush answered by the node through the
# store), detached, and attached again with the file intact; Loam's bodies
# hold none of it in the clear.
#
#   ./scripts/volumes-two-node-e2e.sh     (KEEP=1 keeps the work directory)
#   NODE_HOST=<user>@<host> ./scripts/volumes-two-node-e2e.sh
#
# With NODE_HOST, node-a runs on that machine (over ssh, its work directory
# at the same path there), and the plane, Loam and Kagi answer it on this
# machine's address toward it (PLANE_ADDR overrides).
#
# Needs passwordless sudo (mount, NBD), nbd-client, mkfs.ext4 and the nbd
# kernel module where node-a runs, and redis-cli and a Loam checkout
# ($LOAM_ROOT, default ../loam) here. Without them it skips;
# NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/loam-admin.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in volume_host mount_effect store_effect loam_effect kagi_storage_effect volume_relay \
         kv_state_worker_store consensus durability peer_router redis_edge_anchor remote_channel; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done
# Where node-a runs: this machine, or NODE_HOST over ssh. `on_node` runs a
# command there as root.
NODE_HOST="${NODE_HOST:-}"
if [ -n "$NODE_HOST" ]; then
  NODE_ADDR="${NODE_HOST#*@}"
  PLANE_ADDR="${PLANE_ADDR:-$(ip -4 route get "$NODE_ADDR" | sed -n 's/.* src \([0-9.]*\).*/\1/p')}"
  [ -n "$PLANE_ADDR" ] || skip "no address of this machine toward $NODE_ADDR"
  on_node() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$NODE_HOST" "sudo -n bash -c $(printf '%q' "$1")"; }
  on_node true 2>/dev/null || skip "no ssh with passwordless sudo to $NODE_HOST"
else
  NODE_ADDR=127.0.0.1
  PLANE_ADDR=127.0.0.1
  on_node() { sudo -n bash -c "$1"; }
  sudo -n true 2>/dev/null || skip "needs passwordless sudo (mount, NBD)"
fi
on_node "command -v nbd-client" >/dev/null || skip "needs nbd-client where node-a runs"
on_node "command -v mkfs.ext4" >/dev/null || skip "needs mkfs.ext4 where node-a runs"
command -v redis-cli >/dev/null || skip "needs redis-cli"
nc_loam_present || skip "no Loam checkout at $LOAM_ROOT (set LOAM_ROOT)"
on_node "modprobe nbd" 2>/dev/null || true
on_node "test -e /dev/nbd0" || skip "no NBD devices (the nbd kernel module) where node-a runs"

D="$(mktemp -d /tmp/nc-volumes2-XXXXXX)"
PLANE_PID=""
NODE_PID=""
NBD_INDEX=""
NBD_QUEUE=""
stop_node() {
  [ -n "$NODE_PID" ] || return 0
  # The runtime, by its own config path: nothing else names it.
  on_node "pkill -TERM -f '^[^ ]*fluxor-linux --config $D/node-a/config.bin'" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true
  NODE_PID=""
}
stop_plane() {
  [ -n "$PLANE_PID" ] || return 0
  kill "$PLANE_PID" 2>/dev/null || true
  wait "$PLANE_PID" 2>/dev/null || true
  PLANE_PID=""
}
cleanup() {
  stop_node
  stop_plane
  on_node "umount $D/mnt/default/data" 2>/dev/null || true
  [ -n "$NBD_INDEX" ] && on_node "nbd-client -d /dev/nbd$NBD_INDEX" >/dev/null 2>&1 || true
  if [ -n "$NBD_QUEUE" ]; then
    on_node "echo ${NBD_QUEUE% *} > /sys/block/nbd$NBD_INDEX/queue/max_sectors_kb" || true
    on_node "echo ${NBD_QUEUE#* } > /sys/block/nbd$NBD_INDEX/queue/nr_requests" || true
  fi
  if [ -n "$LOAM_PID" ]; then
    pkill -TERM -P "$LOAM_PID" 2>/dev/null || true; kill "$LOAM_PID" 2>/dev/null || true
    wait "$LOAM_PID" 2>/dev/null || true
  fi
  if [ -n "${KEEP:-}" ]; then
    echo "kept: $D${NODE_HOST:+ (and on $NODE_HOST)}"
  else
    [ -n "$NODE_HOST" ] && on_node "rm -rf $D" || true
    sudo -n rm -rf "$D"
  fi
}
trap cleanup EXIT
fail() {
  echo "FAIL: $1"
  echo "--- plane"; tail -40 "$D/plane/run.log" 2>/dev/null || true
  echo "--- node-a"; on_node "tail -40 $D/node-a/run.log" 2>/dev/null || true
  tail -10 "$D/loam.log" 2>/dev/null || true
  exit 1
}

FREE_PORT='import socket; s = socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'
free_port() { python3 -c "$FREE_PORT"; }
node_free_port() { on_node "python3 -c '$FREE_PORT'"; }
NBD_INDEX="$(on_node 'for i in $(seq 0 15); do [ -e /dev/nbd$i ] && [ ! -s /sys/block/nbd$i/pid ] && { echo $i; break; }; done')"
[ -n "$NBD_INDEX" ] || skip "no free NBD device"
Q="/sys/block/nbd$NBD_INDEX/queue"
NBD_QUEUE="$(on_node "echo \$(cat $Q/max_sectors_kb) \$(cat $Q/nr_requests)")"
on_node "echo 128 > $Q/max_sectors_kb; echo 4 > $Q/nr_requests"

# ── PKI: the volumes' (one CA; Loam and the Kagi endpoint for the plane;
# the node as node-a) and the store group's (each replica's peer key, an
# operator) ──
mkdir -p "$D/pki" "$D/mnt" "$D/admin"
cd "$D/pki"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=volumes test ca" -keyout ca.key -out ca.pem \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
leaf() { # leaf <name> <subject> <extensions>
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "$2" \
    -keyout "$1.key" -out "$1.csr" 2>/dev/null
  printf '%s\n' "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile "$1.ext" -out "$1.pem" 2>/dev/null
  openssl x509 -in "$1.pem" -outform DER -out "$1.der"
  openssl pkcs8 -topk8 -nocrypt -in "$1.key" -outform DER -out "$1.key.der"
}
SAN="IP:127.0.0.1"
[ "$PLANE_ADDR" = 127.0.0.1 ] || SAN="$SAN,IP:$PLANE_ADDR"
leaf loam "/CN=loam" "subjectAltName=$SAN
extendedKeyUsage=serverAuth"
leaf kagi "/CN=kagi" "subjectAltName=$SAN
extendedKeyUsage=serverAuth"
leaf node "/CN=node-a" "extendedKeyUsage=clientAuth"
openssl x509 -in ca.pem -outform DER -out ca.der
openssl ec -in loam.key -outform DER -out loam.sec1.der 2>/dev/null
# The node's capability, as an operator mints it from the mesh root; the
# plane presents the same file.
MESH="$(nc_loam_mesh "$D")"
nc_loam_node_cap "$D/root.seed" nanocloud node.cap || fail "minting node.cap"
gen() { # gen <dir> <name>: a self-signed peer or admin key
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$1/$2.key.pem" -out "$1/$2.pem" -days 1 -subj "/CN=nc-$2" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=digitalSignature" \
    -addext "extendedKeyUsage=serverAuth,clientAuth" 2>/dev/null
}
gen "$D/admin" operator
declare -A RID=([plane]=0 [node-a]=1)
for r in plane node-a; do
  P="$D/$r/peer-pki"
  mkdir -p "$P/peers" "$P/operators" "$D/$r/lattice" "$D/$r/vault"
  gen "$P" peer
  openssl x509 -in "$P/peer.pem" -outform DER -out "$P/peer.der"
  openssl ec -in "$P/peer.key.pem" -outform DER -out "$P/peer.key.der" 2>/dev/null
  cp "$P/peer.pem" "$D/admin/replica${RID[$r]}.pem"
done
for r in plane node-a; do
  cp "$D/admin/replica0.pem" "$D/$r/peer-pki/peers/0.pem"
  cp "$D/admin/replica1.pem" "$D/$r/peer-pki/peers/1.pem"
  cp "$D/admin/operator.pem" "$D/$r/peer-pki/operators/test.pem"
done

# ── Loam ──
LOAM_PORT="$(free_port)"
nc_loam_start "$D" "$LOAM_PORT" "$MESH" "$D/pki/ca.pem" "$D/pki/loam.der" "$D/pki/loam.sec1.der" "$D/loam.log" \
  || fail "Loam's admin gate did not come up"
LOAM_ADDR="$PLANE_ADDR:$LOAM_PORT"
cd "$D"

# ── the graphs: each side's half, rendered as its replica, pointed at this run ──
BASE="$(free_port)"
PEER1="$(node_free_port)"
KAGI_PORT="$(free_port)"
DRV_PORT="$(free_port)"
NBD_PORT="$(node_free_port)"
READ_PORT="$(free_port)"
PEERS="0=$PLANE_ADDR:$BASE 1=$NODE_ADDR:$PEER1"
render() { # render <replica> <role> <port> <template> <out>
  NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PKI="$D/$1/peer-pki" NANOCLOUD_ROLE="$2" \
    NANOCLOUD_REPLICA_ID="${RID[$1]}" NANOCLOUD_NODE_NAME="$1" NANOCLOUD_VOTERS=1 \
    NANOCLOUD_PEER_PORT="$3" NANOCLOUD_PEERS="$PEERS" \
    NANOCLOUD_REPLICA_FRAGMENT="$ROOT/packaging/debian/fluxor-replica.yaml" \
    sh "$ROOT/packaging/debian/nanocloud-graph.sh" "$4" "$5"
}
python3 -B "$ROOT/scripts/support/volumes-split.py" plane >"$D/plane-template.yaml"
python3 -B "$ROOT/scripts/support/volumes-split.py" node >"$D/node-template.yaml"
render plane plane "$BASE" "$D/plane-template.yaml" "$D/plane/rendered.yaml" || fail "the plane did not render"
render node-a node "$PEER1" "$D/node-template.yaml" "$D/node-a/rendered.yaml" || fail "the node did not render"
for r in plane node-a; do
  python3 -B - "$D/$r/rendered.yaml" "$D/$r/graph.yaml" "$D" "$LOAM_ADDR" "$KAGI_PORT" "$DRV_PORT" \
      "$NBD_PORT" "$NBD_INDEX" "$READ_PORT" "$r" "$PLANE_ADDR" <<'PY'
import sys
src, out, d, loam, kagi, drv, nbd, idx, read, who, plane = sys.argv[1:]
s = open(src).read()
s = s.replace("/etc/nanocloud/volumes", d + "/pki")
s = s.replace('"127.0.0.1:7443"', f'"{loam}"')
s = s.replace('"127.0.0.1:7446"', f'"{plane}:{kagi}"')
s = s.replace("    port: 7446\n", f"    port: {kagi}\n")
s = s.replace('    device_key_label: "nanocloud/device"\n',
              f'    device_key_label: "nanocloud/device"\n    nbd_port_base: {nbd}\n    nbd_index_base: {idx}\n'
              '    clock_unchecked: 1\n')
mods, wiring = s.split("\nwiring:\n")
if who == "plane":
    # The driver's door into the store (MERGE rows, as an operator or the
    # scheduler would write them), and the test's reader.
    mods += f"""
  - name: drv
    type: remote_channel
    transport: net
    role: listen
    port: {drv}
    require_peer: 0
    channels: "OctetStream:bytes:16384"
  - name: drv_store
    type: store_effect
    key_prefix: "/"
  - name: redis_edge_anchor
    params:
      listen_port: {read}
  - name: kv_request_router
    params:
      lin_reads: 0
      replicated: 1
"""
    wiring += """  - from: linux_net.net_out
    to: drv.transport_in
  - from: drv.transport_out
    to: linux_net.net_in
  - from: drv.ch0_rx
    to: drv_store.request_in
  - from: drv_store.response_out
    to: drv.ch0
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
"""
open(out, "w").write(mods + "\nwiring:\n" + wiring)
PY
  nc_build_workload "$ROOT" "$D/$r/graph.yaml" "$D/$r/config.bin" "$D/$r/modules.bin" >/dev/null \
    || fail "the $r graph did not build"
done

SEAL="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
run_plane() {
  env -C "$D/plane" FLUXOR_PROJECT_ROOT="$ROOT" FLUXOR_SEAL_KEY="$SEAL" \
    FLUXOR_VAULT_DIR="$D/plane/vault" RUST_LOG="${NC_LOG:-info}" \
    "$FLUXOR_RUNTIME" --config "$D/plane/config.bin" --modules "$D/plane/modules.bin" \
    >> "$D/plane/run.log" 2>&1 < /dev/null &
  PLANE_PID=$!
}
# node-a's runtime, its graph and its directories, where it runs: on
# another machine, at the same paths there.
NODE_RUNTIME="$FLUXOR_RUNTIME"
if [ -n "$NODE_HOST" ]; then
  NODE_RUNTIME="$D/fluxor-linux"
  # The capability is read at run time, where the node runs.
  ssh -o BatchMode=yes "$NODE_HOST" "mkdir -p $D/node-a/vault $D/node-a/lattice $D/mnt $D/pki" \
    && scp -q -o BatchMode=yes "$FLUXOR_RUNTIME" "$NODE_HOST:$NODE_RUNTIME" \
    && scp -q -o BatchMode=yes "$D/node-a/config.bin" "$D/node-a/modules.bin" "$NODE_HOST:$D/node-a/" \
    && scp -q -o BatchMode=yes "$D/pki/node.cap" "$NODE_HOST:$D/pki/" \
    || fail "node-a's runtime and graph did not reach $NODE_HOST"
fi
run_node() {
  # The ssh session (or the local sudo) is the runtime's parent: NODE_PID
  # waits on it.
  on_node "cd $D/node-a && exec env FLUXOR_SEAL_KEY=$SEAL \
    FLUXOR_VAULT_DIR=$D/node-a/vault FLUXOR_MOUNT_ROOT=$D/mnt \
    SECTOR_PROC_ALLOW='blkid mkfs fsck nbd-client blockdev' \
    PATH=/usr/sbin:/sbin:/usr/bin:/bin RUST_LOG=${NC_LOG:-info} \
    $NODE_RUNTIME --config $D/node-a/config.bin --modules $D/node-a/modules.bin \
    >> $D/node-a/run.log 2>&1 < /dev/null" < /dev/null &
  NODE_PID=$!
}
store_put() { # store_put <key> <value>: into the plane's host log, before it runs
  python3 -B - "$D/plane/store.log" "$1" "$2" <<'PY'
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
admin() { # admin <op> <replica>: one wire admin command to the plane, as the operator
  python3 -B - "$BASE" "$D/admin/operator.pem" "$D/admin/operator.key.pem" "$1" "$2" <<'PY'
import subprocess, sys, time, select, os
port, cert, key, op, rid = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4], 0), int(sys.argv[5])
p = subprocess.Popen(["openssl", "s_client", "-quiet", "-connect", "127.0.0.1:" + port,
                      "-cert", cert, "-key", key],
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
p.stdin.write(bytes([0x12, 2, 0, op, rid])); p.stdin.flush()
got, end = b"", time.time() + 10
while time.time() < end:
    r, _, _ = select.select([p.stdout], [], [], 0.2)
    if r:
        b = os.read(p.stdout.fileno(), 256)
        if not b: break
        got += b
    i = got.find(b"\x13")
    if i >= 0 and len(got) >= i + 4:
        print("0x%02x" % got[i + 3]); break
else:
    print("none")
p.kill(); p.wait()
PY
}

# ── 1. the node's device key: generated in its vault, enrolled with Kagi ──
run_node
for _ in $(seq 1 150); do on_node "grep -q '\[volume_host\] device key' $D/node-a/run.log" 2>/dev/null && break; sleep 0.2; done
DEVICE_KEY="$(on_node "grep -o '\[volume_host\] device key [0-9a-f]*' $D/node-a/run.log" | head -1 | awk '{print $4}' || true)"
stop_node
[ -n "$DEVICE_KEY" ] || fail "the node did not name its device key"
JKT="$(python3 -B - "$DEVICE_KEY" <<'PY'
import base64, hashlib, sys
x = base64.urlsafe_b64encode(bytes.fromhex(sys.argv[1])).rstrip(b"=").decode()
canon = '{"crv":"Ed25519","kty":"OKP","x":"%s"}' % x
print(base64.urlsafe_b64encode(hashlib.sha256(canon.encode()).digest()).rstrip(b"=").decode())
PY
)"
store_put "kagi/device/node-a" "{\"device_id\":\"node-a\",\"status\":\"active\",\"sub\":\"default\",\"cnf\":{\"jkt\":\"$JKT\"}}"
# node-b: enrolled, never run. The claim is granted to it first, so node-a's
# attach must take the grant over (KAGI_REPLACE, relayed to the plane).
JKT_B="$(python3 -c 'import base64, hashlib, os; print(base64.urlsafe_b64encode(hashlib.sha256(os.urandom(32)).digest()).rstrip(b"=").decode())')"
store_put "kagi/device/node-b" "{\"device_id\":\"node-b\",\"status\":\"active\",\"sub\":\"default\",\"cnf\":{\"jkt\":\"$JKT_B\"}}"
store_put "/storageclasses/encrypted" '{"metadata":{"name":"encrypted"},"provisioner":"encrypted.loam.nanocloud.io","parameters":{"custody":"possession","fsType":"ext4","minTier":0,"lifetimeSeconds":3600},"volumeBindingMode":"WaitForFirstConsumer"}'
store_put "/persistentvolumeclaims/default/data" '{"metadata":{"name":"data","namespace":"default"},"spec":{"storageClassName":"encrypted","accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"64Mi"}}}}'
echo "  ok  the node's device key enrolled (jkt $JKT)"

# ── 2. the plane, then the node, added as a learner by the operator ──
run_plane
for _ in $(seq 1 150); do [ "$(redis-cli -p "$READ_PORT" --raw GET /storageclasses/encrypted 2>/dev/null)" != "" ] && break; sleep 0.2; done
run_node
for _ in $(seq 1 150); do grep -q "\[pr\] conn up p=1" "$D/plane/run.log" 2>/dev/null && break; sleep 0.2; done
S="$(admin 0x0C 1)"
[ "$S" = 0x00 ] || fail "the operator's ADD_LEARNER answered $S"
echo "  ok  the node added to the group as a learner"

# ── 3. provision, attach, write, snapshot, detach, attach again ──
export -f on_node
export NODE_HOST
PYTHONPATH="$ROOT/scripts/support" python3 -B - "$D" "$DRV_PORT" "$NBD_INDEX" "$READ_PORT" <<'PY' || fail "the volume's life did not run as it should"
import os, struct, subprocess, sys, time, fxrc
d, port, idx, read = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

def node(cmd, check=True):
    """`cmd` as root where node-a runs: its output."""
    r = subprocess.run(["bash", "-c", 'on_node "$1"', "-", cmd], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"on node-a: {cmd}: {r.stderr.strip()}")
    return r.stdout

def mounted(mnt):
    return [l for l in node("cat /proc/self/mountinfo").splitlines() if f" {mnt} " in l]

def get(k):
    r = subprocess.run(["redis-cli", "-p", read, "--raw", "GET", k], capture_output=True, text=True)
    return r.stdout.rstrip("\n")

def flat(k):
    return dict(e.split("=", 1) for e in get(k).split(";") if "=" in e)

def relayed():
    """The relay's records still standing: `key out=<answered?>`."""
    r = subprocess.run(["redis-cli", "-p", read, "--raw", "KEYS", "*"], capture_output=True, text=True)
    keys = [k for k in r.stdout.split("\n") if "/volume-asks/" in k or "/volume-ops/" in k]
    return [f"{k} answered={';out=' in get(k)}" for k in keys]

def until(what, f, secs):
    end = time.time() + secs
    while time.time() < end:
        r = f()
        if r:
            return r
        time.sleep(0.5)
    raise SystemExit(f"never: {what}\nprovision {flat('/volume-provisions/default/data')}\n"
                     f"attachment {flat('/volume-attachments/default/data')}\nclaim {flat('/volumes/default/data')}\n"
                     f"snapshot {flat('/volume-snapshots/default/data/s1')}\nrelayed {relayed()}")

s = fxrc.connect(port)
cid = 0
def merge(key, value):
    global cid
    cid += 1
    def f(num, ty, b):
        return bytes([num, ty]) + struct.pack("<H", len(b)) + b
    rec = bytes([4]) + f(1, 1, struct.pack("<q", cid)) + f(2, 1, struct.pack("<q", 6)) \
        + f(3, 0, key.encode()) + f(4, 0, value.encode())
    s.sendall(rec)
    s.recv(4096)

P, A, C = "/volume-provisions/default/data", "/volume-attachments/default/data", "/volumes/default/data"
SN = "/volume-snapshots/default/data/s1"

until("provision opened", lambda: flat(P).get("phase"), 60)
merge(P[1:], "device=node-b")
until("provisioned", lambda: flat(P).get("phase") == "provisioned", 120)
a = flat(A)
assert a["vol"] == "nanocloud:/default/volumes/data" and a["grant"] == "node-b" and len(a["set"]) == 32, a
print(f"  ok  provisioned on the plane: {a['vol']}, granted to node-b")

merge(A[1:], "desired=attached;device=node-a")
until("attached", lambda: flat(A).get("phase") == "attached", 180)
until("mounted", lambda: flat(C).get("mount") == "mounted", 120)
mnt = d + "/mnt/default/data"
info = mounted(mnt)
assert info and "nodev" in info[0] and "nosuid" in info[0], info
dev = flat(A).get("dev")
assert dev == f"/dev/nbd{idx}", dev
assert flat(A)["grant"] == "node-a", flat(A)
print(f"  ok  attached on node-a, the grant taken over from node-b through the plane: {dev} mounted nodev,nosuid at {mnt}")

secret = b"encrypted-volume-two-node-" + os.urandom(16).hex().encode()
node(f"python3 -c \"open('{mnt}/hello', 'wb').write(b'{secret.decode()}' * 64)\" && sync && blockdev --flushbufs {dev}")
time.sleep(2)
leak = []
for root, _, files in os.walk(d + "/bodies"):
    for n in files:
        if secret in open(os.path.join(root, n), "rb").read():
            leak.append(n)
assert not leak, f"plaintext in Loam bodies: {leak}"
print("  ok  written through the mount; Loam's bodies hold none of it in the clear")

# A snapshot on the plane: its flush runs on the node holding the volume.
merge(SN[1:], "desired=retained")
until("snapshot retained", lambda: flat(SN).get("phase") == "retained", 180)
sn = flat(SN)
assert sn.get("root"), sn
print(f"  ok  snapshotted by the plane, flushed by node-a: root {sn['root'][:16]}…")

merge(A[1:], "desired=detached")
until("detached", lambda: flat(A).get("phase") in ("detached", "idle"), 180)
assert not mounted(mnt), "unmounted"
a = flat(A)
assert a.get("inst", "") == "" and a.get("dev", "") == "", a
print("  ok  detached: unmounted, withdrawn, unpublished, destroyed, released")

merge(A[1:], "desired=attached")
until("attached again", lambda: flat(A).get("phase") == "attached", 180)
until("mounted again", lambda: flat(C).get("mount") == "mounted", 120)
want = __import__("hashlib").sha256(secret * 64).hexdigest()
assert node(f"sha256sum {mnt}/hello").split()[0] == want, "the file reads back"
print("  ok  attached again: the file reads back")
merge(A[1:], "desired=detached")
until("detached again", lambda: flat(A).get("phase") in ("detached", "idle"), 180)
print("  ok  detached again")
PY

# ── 4. what crossed: through the store, and nothing left behind ──
grep -q "QUARANTINE\|scope refused\|REFUSED" "$D/plane/run.log" && fail "the plane refused to serve"
on_node "grep -q 'QUARANTINE\|scope refused\|REFUSED' $D/node-a/run.log" && fail "the node refused to serve"
[ -z "$(redis-cli -p "$READ_PORT" --raw KEYS '*' | grep '/volume-asks/\|/volume-ops/' || true)" ] \
  || fail "a relayed request was left behind"
stop_node
stop_plane
# Both directions carried: each relay's last counters (its heartbeat, logged
# within a second of a change).
count() { sed -n "s/.* $1=\([0-9]*\).*/\1/p"; }
PLANE_HB="$(grep -o "\[volume_relay\] hb .*" "$D/plane/run.log" | tail -1)"
NODE_HB="$(on_node "grep -o '\[volume_relay\] hb .*' $D/node-a/run.log" | tail -1)"
[ "$(echo "$NODE_HB" | count relayed)" -ge 1 ] && [ "$(echo "$PLANE_HB" | count served)" -ge 1 ] \
  || fail "node-a relayed nothing to the plane: $NODE_HB / $PLANE_HB"
[ "$(echo "$PLANE_HB" | count relayed)" -ge 1 ] && [ "$(echo "$NODE_HB" | count served)" -ge 1 ] \
  || fail "the plane relayed nothing to node-a: $PLANE_HB / $NODE_HB"
echo "  ok  every relayed request answered and withdrawn, both ways ($PLANE_HB / $NODE_HB)"
echo "PASS: an encrypted volume across the control plane and a node${NODE_HOST:+ on $NODE_HOST}: provisioned, attached, written, snapshotted, detached and attached again, its effects carried through the replicated store"
