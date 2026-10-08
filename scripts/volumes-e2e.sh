#!/usr/bin/env bash
# Encrypted volumes, end to end: the packaged graph
# (packaging/debian/fluxor-volumes.yaml) against Loam's admin gate (the
# loam-admin template from a Loam checkout), under this node's capability,
# with Kagi's storage-key service, its ledger and custodians in the graph.
#
#   provision  a PersistentVolumeClaim of class `encrypted`: the Loam volume
#              created, Kagi's key policy authorised, the volume key split to
#              three custodians in this node's vault and dropped, granted to
#              the node its consumer was scheduled to; the PersistentVolume
#              bound to the claim
#   attach     the lease, the volume's subgraph (loam_volume + crypt_block +
#              nbd_serve), Kagi's release of two custodian shares to
#              crypt_block's fresh recipient, the key rebuilt inside the
#              node's vault, the device published over NBD, the filesystem
#              made and mounted nodev,nosuid
#   data       a file written through the mount; Loam's bodies hold no byte
#              of it in the clear
#   detach     unmount, flush, withdraw, destroy, release
#   again      attached afresh — a new recipient, a new release — and the
#              file reads back
#
#   ./scripts/volumes-e2e.sh     (KEEP=1 keeps the work directory)
#
# Needs passwordless sudo (mount, NBD), nbd-client, mkfs.ext4, the nbd kernel
# module and a Loam checkout ($LOAM_ROOT, default ../loam). Without them it
# skips; NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GRAPH="$ROOT/packaging/debian/fluxor-volumes.yaml"
. "$ROOT/scripts/support/loam-admin.sh"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"

skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in volume_host mount_effect store_effect loam_effect kagi_storage_effect remote_channel; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done
sudo -n true 2>/dev/null || skip "needs passwordless sudo (mount, NBD)"
command -v nbd-client >/dev/null || skip "needs nbd-client"
command -v mkfs.ext4 >/dev/null || skip "needs mkfs.ext4"
nc_loam_present || skip "no Loam checkout at $LOAM_ROOT (set LOAM_ROOT)"
sudo -n modprobe nbd 2>/dev/null || true
[ -e /dev/nbd0 ] || skip "no NBD devices (the nbd kernel module)"

D="$(mktemp -d /tmp/nc-volumes-XXXXXX)"
NODE_PID=""
NBD_INDEX=""
NBD_QUEUE=""
stop_node() {
  [ -n "$NODE_PID" ] || return 0
  sudo -n pkill -TERM -P "$NODE_PID" 2>/dev/null || true
  sudo -n kill "$NODE_PID" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true
  NODE_PID=""
}
cleanup() {
  stop_node
  sudo -n umount "$D/mnt/default/data" 2>/dev/null || true
  [ -n "$NBD_INDEX" ] && sudo -n nbd-client -d "/dev/nbd$NBD_INDEX" >/dev/null 2>&1 || true
  if [ -n "$NBD_QUEUE" ]; then
    echo "${NBD_QUEUE% *}" | sudo -n tee "/sys/block/nbd$NBD_INDEX/queue/max_sectors_kb" >/dev/null || true
    echo "${NBD_QUEUE#* }" | sudo -n tee "/sys/block/nbd$NBD_INDEX/queue/nr_requests" >/dev/null || true
  fi
  if [ -n "$LOAM_PID" ]; then
    pkill -TERM -P "$LOAM_PID" 2>/dev/null || true; kill "$LOAM_PID" 2>/dev/null || true
    wait "$LOAM_PID" 2>/dev/null || true
  fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; sudo -n tail -60 "$D/node.log" 2>/dev/null || true; tail -10 "$D/loam.log" 2>/dev/null || true; exit 1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# A free NBD device: one with no server attached.
for i in $(seq 0 15); do
  [ -e "/dev/nbd$i" ] || continue
  if [ ! -s "/sys/block/nbd$i/pid" ]; then NBD_INDEX="$i"; break; fi
done
[ -n "$NBD_INDEX" ] || skip "no free NBD device"
# The queue bound a node's udev rule sets (packaging/debian/60-nanocloud-nbd.rules),
# on this device for the run; its own values come back afterwards.
Q="/sys/block/nbd$NBD_INDEX/queue"
NBD_QUEUE="$(cat "$Q/max_sectors_kb") $(cat "$Q/nr_requests")"
echo 128 | sudo -n tee "$Q/max_sectors_kb" >/dev/null
echo 4 | sudo -n tee "$Q/nr_requests" >/dev/null

# ── PKI: one CA; Loam and the Kagi endpoint for 127.0.0.1; the node as node-a ──
mkdir -p "$D/pki" "$D/mnt" "$D/store"
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
leaf loam "/CN=loam" "subjectAltName=IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf kagi "/CN=kagi" "subjectAltName=IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf node "/CN=node-a" "extendedKeyUsage=clientAuth"
openssl x509 -in ca.pem -outform DER -out ca.der
openssl ec -in loam.key -outform DER -out loam.sec1.der 2>/dev/null
# This node's capability, as an operator mints it from the mesh root.
MESH="$(nc_loam_mesh "$D")"
nc_loam_node_cap "$D/root.seed" nanocloud node.cap || fail "minting node.cap"

# ── Loam ──
LOAM_PORT="$(free_port)"
nc_loam_start "$D" "$LOAM_PORT" "$MESH" "$D/pki/ca.pem" "$D/pki/loam.der" "$D/pki/loam.sec1.der" "$D/loam.log" \
  || fail "Loam's admin gate did not come up"
LOAM_ADDR="127.0.0.1:$LOAM_PORT"
cd "$D"

# ── the graph: the packaged one, pointed at this run ──
KAGI_PORT="$(free_port)"
DRV_PORT="$(free_port)"
NBD_PORT="$(free_port)"
python3 -B - "$GRAPH" "$D" "$LOAM_ADDR" "$KAGI_PORT" "$DRV_PORT" "$NBD_PORT" "$NBD_INDEX" <<'PY'
import sys
src, d, loam, kagi, drv, nbd, idx = sys.argv[1:]
s = open(src).read()
s = s.replace("/etc/nanocloud/volumes", d + "/pki")
s = s.replace('"127.0.0.1:7443"', f'"{loam}"')
s = s.replace('"127.0.0.1:7446"', f'"127.0.0.1:{kagi}"')
s = s.replace("    port: 7446\n", f"    port: {kagi}\n")
s = s.replace('    device_key_label: "nanocloud/device"\n',
              f'    device_key_label: "nanocloud/device"\n    nbd_port_base: {nbd}\n    nbd_index_base: {idx}\n'
              '    clock_unchecked: 1\n')
mods, wiring = s.split("\nwiring:\n")
# The driver's door into the store: MERGE rows, as an operator or the
# scheduler would write them.
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
"""
wiring += """  - from: linux_net.net_out
    to: drv.transport_in
  - from: drv.transport_out
    to: linux_net.net_in
  - from: drv.ch0_rx
    to: drv_store.request_in
  - from: drv_store.response_out
    to: drv.ch0
"""
open(d + "/volumes.yaml", "w").write(mods + "\nwiring:\n" + wiring)
PY

SEAL="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
run_node() {
  sudo -n env FLUXOR_PROJECT_ROOT="$ROOT" FLUXOR_SEAL_KEY="$SEAL" FLUXOR_STORE_DIR="$D/store" \
    FLUXOR_MOUNT_ROOT="$D/mnt" SECTOR_PROC_ALLOW="blkid mkfs fsck nbd-client blockdev" \
    PATH="/usr/sbin:/sbin:$PATH" HOME="$HOME" \
    "$(command -v fluxor)" run "$D/volumes.yaml" >> "$D/node.log" 2>&1 < /dev/null &
  NODE_PID=$!
}

store_put() { # store_put <key> <value>: before the node runs
  sudo -n python3 -B - "$D/store/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

# ── 1. the node's device key: generated in its vault, enrolled with Kagi ──
run_node
for _ in $(seq 1 150); do sudo -n grep -q "\[volume_host\] device key" "$D/node.log" 2>/dev/null && break; sleep 0.2; done
DEVICE_KEY="$(sudo -n grep -o '\[volume_host\] device key [0-9a-f]*' "$D/node.log" | head -1 | awk '{print $4}')"
stop_node
[ -n "$DEVICE_KEY" ] || fail "the node did not name its device key"
JKT="$(python3 - "$DEVICE_KEY" <<'PY'
import base64, hashlib, sys
x = base64.urlsafe_b64encode(bytes.fromhex(sys.argv[1])).rstrip(b"=").decode()
canon = '{"crv":"Ed25519","kty":"OKP","x":"%s"}' % x
print(base64.urlsafe_b64encode(hashlib.sha256(canon.encode()).digest()).rstrip(b"=").decode())
PY
)"
store_put "kagi/device/node-a" "{\"device_id\":\"node-a\",\"status\":\"active\",\"sub\":\"default\",\"cnf\":{\"jkt\":\"$JKT\"}}"
store_put "/storageclasses/encrypted" '{"metadata":{"name":"encrypted"},"provisioner":"encrypted.loam.nanocloud.io","parameters":{"custody":"possession","fsType":"ext4","minTier":0,"lifetimeSeconds":3600},"volumeBindingMode":"WaitForFirstConsumer"}'
store_put "/persistentvolumeclaims/default/data" '{"metadata":{"name":"data","namespace":"default"},"spec":{"storageClassName":"encrypted","accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"64Mi"}}}}'
echo "  ok  the node's device key enrolled (jkt $JKT)"

# ── 2. provision, attach, write, detach, attach again ──
run_node
sudo -n env PYTHONPATH="$ROOT/scripts/support" python3 -B - "$D" "$DRV_PORT" "$NBD_INDEX" <<'PY' || fail "the volume's life did not run as it should"
import os, struct, subprocess, sys, time, fxrc
d, port, idx = sys.argv[1], int(sys.argv[2]), sys.argv[3]
LOG = d + "/store/store.log"

def store():
    data = open(LOG, "rb").read()
    kv, p = {}, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        k = data[p+15:p+15+kl].decode(errors="replace"); v = data[p+15+kl:p+15+kl+vl].decode(errors="replace")
        if op == 1: kv[k] = v
        else: kv.pop(k, None)
        p += 15 + kl + vl
    return kv

def flat(k):
    v = store().get(k, "")
    return dict(e.split("=", 1) for e in v.split(";") if "=" in e)

def until(what, f, secs):
    end = time.time() + secs
    while time.time() < end:
        r = f()
        if r:
            return r
        time.sleep(0.5)
    raise SystemExit(f"never: {what}\nprovision {flat('/volume-provisions/default/data')}\n"
                     f"attachment {flat('/volume-attachments/default/data')}\nclaim {flat('/volumes/default/data')}")

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

# Provision opens and waits for the first consumer's node.
until("provision opened", lambda: flat(P).get("phase"), 60)
merge(P[1:], "device=node-a")
until("provisioned", lambda: flat(P).get("phase") == "provisioned", 120)
p = flat(P)
kv = store()
assert "pvc-default-data" in kv.get("/persistentvolumeclaims/default/data", ""), "the claim names its volume"
assert "/persistentvolumes/pvc-default-data" in kv
a = flat(A)
assert a["vol"] == "nanocloud:/default/volumes/data" and a["grant"] == "node-a" and len(a["set"]) == 32, a
print(f"  ok  provisioned: {a['vol']}, recovery set {a['set']}, granted to node-a")

# Attach: the scheduler asks for the volume on node-a.
merge(A[1:], "desired=attached;device=node-a")
until("attached", lambda: flat(A).get("phase") == "attached", 180)
until("mounted", lambda: flat(C).get("mount") == "mounted", 120)
mnt = d + "/mnt/default/data"
info = [l for l in open("/proc/self/mountinfo") if f" {mnt} " in l]
assert info and "nodev" in info[0] and "nosuid" in info[0], info
dev = flat(A).get("dev")
assert dev == f"/dev/nbd{idx}", dev
print(f"  ok  attached: {dev} mounted nodev,nosuid at {mnt}")

secret = b"encrypted-volume-e2e-" + os.urandom(16).hex().encode()
with open(mnt + "/hello", "wb") as fh:
    fh.write(secret * 64)
os.sync()
subprocess.run(["blockdev", "--flushbufs", dev], check=True)
time.sleep(2)
leak = []
for root, _, files in os.walk(d + "/bodies"):
    for n in files:
        if secret in open(os.path.join(root, n), "rb").read():
            leak.append(n)
assert not leak, f"plaintext in Loam bodies: {leak}"
print("  ok  written through the mount; Loam's bodies hold none of it in the clear")

# Detach.
merge(A[1:], "desired=detached")
until("detached", lambda: flat(A).get("phase") in ("detached", "idle"), 180)
assert not [l for l in open("/proc/self/mountinfo") if f" {mnt} " in l], "unmounted"
assert C not in store(), "the claim withdrawn"
a = flat(A)
assert a.get("inst", "") == "" and a.get("dev", "") == "" and a.get("fence", "0") in ("", "0"), a
print("  ok  detached: unmounted, withdrawn, unpublished, destroyed, released")

# Attach again: a fresh instance, a fresh release; the data is there.
merge(A[1:], "desired=attached")
until("attached again", lambda: flat(A).get("phase") == "attached", 180)
until("mounted again", lambda: flat(C).get("mount") == "mounted", 120)
assert open(mnt + "/hello", "rb").read() == secret * 64, "the file reads back"
print("  ok  attached again: the file reads back")
merge(A[1:], "desired=detached")
until("detached again", lambda: flat(A).get("phase") in ("detached", "idle"), 180)
print("  ok  detached again")
PY
stop_node
echo "PASS: an encrypted volume provisioned, attached, written, detached and attached again, end to end"
