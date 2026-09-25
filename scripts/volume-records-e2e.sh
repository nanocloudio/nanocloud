#!/usr/bin/env bash
# The encrypted-volume pipelines' records chain against the control-plane
# store, live.
#
# The chain's nodes (`vw_*`, volume_records.uproc) and their edges are taken
# from the shipped packaging/debian/fluxor-volumes.yaml, so the graph runs
# what production runs, over a store directory seeded with a
# PersistentVolumeClaim, a retained snapshot and a key record. A Python driver
# sends each record effect, with the issuing chain's context, and checks its
# outcome; after the node stops, the store log is read back:
#   the identity persisted once; a claim put, asked to unmount, withdrawn; the
#   PersistentVolume made and the claim's spec.volumeName naming it, and made
#   for a claim already gone; a detach reason kept against a second; the
#   grant moved; a rotation requested once; key epochs merged; merges into
#   records that do not stand leave them absent; pins counted per epoch, and
#   a count that cannot be decided failed rather than short; a clone
#   persisted from its snapshot's source; an audit kept under its wall-clock
#   key; the volume forgotten, twice; an effect not the chain's dropped; a
#   burst of effects answered in order.
#
#   ./scripts/volume-records-e2e.sh     (KEEP=1 keeps the work directory)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_effect decision remote_channel; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-volume-records-XXXXXX)"
NODE_PID=""
cleanup() {
  [ -n "$NODE_PID" ] && { pkill -TERM -P "$NODE_PID" 2>/dev/null || true; kill "$NODE_PID" 2>/dev/null || true; }
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/node.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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

VOL="default:/volumes/data"
store_put "/persistentvolumeclaims/default/data" '{"metadata":{"name":"data","namespace":"default"},"spec":{"storageClassName":"encrypted","accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}}}}'
store_put "/volume-snapshots/default/data/s1" "kvol=$VOL;elo=1;ehi=2;phase=retained;root=r1"
store_put "/volume-keys/default/other" "kvol=$VOL;epoch=3"

P="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
cat > "$D/node.yaml" <<EOF
target: linux
tick_us: 1000
module_search_paths:
  - $ROOT/modules/app
platform:
  net: {}
modules:
  - name: requests
    type: remote_channel
    transport: net
    role: listen
    port: $P
    require_peer: 0
    channels: "OctetStream:bytes:16384"
__CHAIN_MODULES__
wiring:
  - from: linux_net.net_out
    to: requests.transport_in
  - from: requests.transport_out
    to: linux_net.net_in
  - from: requests.ch0_rx
    to: vw_s1.request_in
  - from: vw_answer.result_out
    to: requests.ch0
__CHAIN_WIRING__
EOF
# The shipped chain: its nodes, and the edges between them.
python3 -B - "$ROOT/packaging/debian/fluxor-volumes.yaml" "$D/node.yaml" <<'EXTRACT'
import re, sys
src, out = sys.argv[1:]
mods_text, wire_text = open(src).read().split("\nmodules:\n", 1)[1].split("\nwiring:\n", 1)
mods = re.findall(r"^  - name: (vw_\S+)\n((?:    .*\n)+)", mods_text, re.M)
edges = re.findall(r"^  - from: (\S+)\n    to: (\S+)\n", wire_text, re.M)
chain = "".join(f"  - name: {n}\n{body}" for n, body in mods)
wires = "".join(f"  - from: {a}\n    to: {b}\n" for a, b in edges
                if a.startswith("vw_") and b.startswith("vw_"))
assert len(mods) == 4 and wires.count("from:") == 3, (len(mods), wires)
g = open(out).read().replace("__CHAIN_MODULES__\n", chain).replace("__CHAIN_WIRING__\n", wires)
open(out, "w").write(g)
EXTRACT
FLUXOR_PROJECT_ROOT="$ROOT" FLUXOR_STORE_DIR="$D" fluxor run "$D/node.yaml" > "$D/node.log" 2>&1 < /dev/null &
NODE_PID=$!

PYTHONPATH="$ROOT/scripts/support" python3 -B - "$P" "$VOL" <<'PY' || fail "an outcome was wrong"
import struct, sys, fxrc
port, VOL = int(sys.argv[1]), sys.argv[2]
s = fxrc.connect(port)
def rec(fields):
    out = bytes([len(fields)])
    for num, val in fields:
        if isinstance(val, int):
            ty, b = 1, struct.pack("<q", val)
        elif isinstance(val, list):
            ty, b = 3, rec(val)  # a nested message: the issuing chain's context
        else:
            ty, b = 0, val.encode() if isinstance(val, str) else val
        out += bytes([num, ty]) + struct.pack("<H", len(b)) + b
    return out
buf = b""
def read_rec():
    global buf
    while True:
        if buf:
            n, p, ok = buf[0], 1, True
            for _ in range(n):
                if p + 4 > len(buf): ok = False; break
                p += 4 + struct.unpack("<H", buf[p+2:p+4])[0]
            if ok and p <= len(buf):
                frame, buf = buf[:p], buf[p:]
                out, q = {}, 1
                for _ in range(frame[0]):
                    num, ty, ln = frame[q], frame[q+1], struct.unpack("<H", frame[q+2:q+4])[0]
                    v = frame[q+4:q+4+ln]
                    out[num] = struct.unpack("<q", v)[0] if ty == 1 else v.decode()
                    q += 4 + ln
                return out
        c = s.recv(65536); assert c, "closed"; buf += c
cid = 0
def send(code, claim, a=(), ctx=()):
    global cid
    cid += 1
    f = [(1, cid), (2, code), (3, claim)] + [(4 + i, v) for i, v in enumerate(a)]
    f.append((254, [(31, "ph")] + list(ctx)))
    s.sendall(rec(f))
    return cid
def answered(c, code, want="ok"):
    o = read_rec()
    assert o[1] == c and o[2] == code and 254 in o, o
    for k in range(1, 17):
        assert k in o, f"field {k}"
    assert o[4] == want, o
    return o
def op(code, claim, a=(), ctx=(), want="ok"):
    return answered(send(code, claim, a, ctx), code, want)
CL = "default/data"
op(43, CL, [VOL, "ext4", "node-a", "ReadWriteOnce", "set1"]); print("  ok  identity persisted")
op(43, CL, [VOL, "xfs", "node-z", "ReadWriteOnce", "set2"]); print("  ok  persisted again: unchanged")
op(40, CL, ["/dev/nbd0", "ext4", "1"]); print("  ok  claim put")
op(42, CL); print("  ok  unmount asked")
op(41, CL); op(41, CL); print("  ok  claim withdrawn, twice")
op(42, CL); print("  ok  unmount of a withdrawn claim")
op(44, CL, ["default", "data", VOL, "1Gi", "ReadWriteOnce"]); print("  ok  pv bound")
op(44, "default/gone", ["default", "gone", VOL, "1Gi", "ReadWriteOnce"]); print("  ok  pv made for a claim already gone")
op(62, CL, ["deleting", "1"]); op(62, CL, ["other", ""]); print("  ok  detach reason kept")
op(63, CL, ["node-b"]); print("  ok  grant moved")
op(64, CL, ["evict", "ops"]); op(64, CL, ["again", "ops2"]); print("  ok  rotation requested once")
op(66, CL, ["2", "3"]); print("  ok  key epochs")
op(63, "default/ghost", ["node-b"]); op(66, "default/ghost", ["2", "3"]); op(62, "default/ghost", ["x", "1"])
print("  ok  merges into records that do not stand")
o = op(70, CL, [VOL, "1", CL]); assert o[13] == 1, o; print("  ok  pins at epoch 1: the snapshot")
o = op(70, CL, [VOL, "3", CL]); assert o[13] == 1, o; print("  ok  pins at epoch 3: the other claim")
o = op(70, CL, [VOL, "0", CL]); assert o[13] == 2, o; print("  ok  pins at any epoch: both")
o = op(70, CL, [VOL, "9", CL]); assert o[13] == 0, o; print("  ok  pins at epoch 9: none")
op(70, CL, [VOL, "x", CL], want="failed"); print("  ok  an undecidable count fails")
SRC = [(35, "ext4"), (44, "ReadWriteOnce"), (37, "node-b"), (43, "set1")]
op(75, "default/clone", ["default:/volumes/clone", VOL, "1", "2", "default/data/s1"], ctx=SRC); print("  ok  clone persisted")
op(77, CL, ["recovery", "alice", "bob", "node-r"]); print("  ok  audit")
op(83, "default/clone"); op(83, "default/clone"); print("  ok  clone forgotten, twice")
dropped = send(10, CL, ["inst"])
c = send(40, "default/burst", ["/dev/nbd1", "ext4", "0"])
answered(c, 40); print("  ok  an effect not the chain's dropped")
burst = [send(40, f"default/b{i}", [f"/dev/nbd{i}", "ext4", "0"]) for i in range(6)]
for c in burst:
    answered(c, 40)
print("  ok  a burst answered in order")
PY
kill "$NODE_PID" 2>/dev/null || true; wait "$NODE_PID" 2>/dev/null || true; NODE_PID=""

python3 - "$D/store.log" "$VOL" <<'PY' || fail "the store does not hold what was written"
import struct, sys, json
data = open(sys.argv[1], "rb").read(); VOL = sys.argv[2]
kv, hist, p = {}, [], 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl].decode(); v = data[p+15+kl:p+15+kl+vl].decode()
    hist.append((op, k, v))
    if op == 1: kv[k] = v
    else: kv.pop(k, None)
    p += 15 + kl + vl
flat = lambda k: dict(e.split("=", 1) for e in kv[k].split(";") if "=" in e)
a = flat("/volume-attachments/default/data")
assert a["vol"] == VOL and a["fs"] == "ext4" and a["set"] == "set1" and a["fresh"] == "1", a
assert a["reason"] == "deleting" and a["erase"] == "1" and a["grant"] == "node-b", a
k = flat("/volume-keys/default/data")
assert k == {"kvol": VOL, "epoch": "2", "next": "3"}, k
assert "/volumes/default/data" not in kv, "the claim stays withdrawn"
pv = json.loads(kv["/persistentvolumes/pvc-default-data"])
assert pv["spec"]["csi"]["volumeHandle"] == VOL and pv["spec"]["claimRef"]["name"] == "data", pv
pvc = json.loads(kv["/persistentvolumeclaims/default/data"])
assert pvc["spec"]["volumeName"] == "pvc-default-data", pvc
assert pvc["spec"]["storageClassName"] == "encrypted", "the rest of the claim stands"
r = flat("/volume-rotations/default/data")
assert r == {"why": "evict", "actor": "ops"}, r
clone = [v for op, k, v in hist if op == 1 and k == "/volume-attachments/default/clone"]
assert clone == ["vol=default:/volumes/clone;fs=ext4;access=ReadWriteOnce;grant=node-b;set=set1"], clone
ckeys = [v for op, k, v in hist if op == 1 and k == "/volume-keys/default/clone"]
assert ckeys == [f"kvol={VOL};epoch=1;next=2"], ckeys
assert "/volume-attachments/default/clone" not in kv and "/volume-keys/default/clone" not in kv
import re
audits = [k for k in kv if k.startswith("/volume-audit/default/data/")]
assert len(audits) == 1 and "approver=bob" in kv[audits[0]], audits
m = re.fullmatch(r"/volume-audit/default/data/(\d{13})-(\d+)", audits[0])
assert m and flat(audits[0])["at"] == m.group(1), audits[0]
for k in ("/volume-attachments/default/ghost", "/volume-keys/default/ghost"):
    assert k not in kv, f"a merge recreated {k}"
assert "/persistentvolumes/pvc-default-gone" in kv and "/persistentvolumeclaims/default/gone" not in kv
assert all(f"/volumes/default/b{i}" in kv for i in range(6)) and "/volumes/default/burst" in kv
print("  ok  the store holds every write")
PY
echo "PASS: the records chain makes the pipelines' record effects in the control-plane store"
