#!/usr/bin/env bash
# kagi_storage_effect against Kagi's storage-key service, live.
#
# Kagi's own modules run in a real graph: `storage_key`, its
# `security_state` ledger on the control-plane store, and three
# `storage_custodian`s, each with a recovery key in this node's vault. First a
# probe graph asks each custodian for its recovery public key (a vault key,
# persisted under the run's seal key). The control-plane graph then runs the
# connector as packaged — no keys pinned, so it asks the custodians in its
# graph — and a Python driver sends each pipeline effect and checks the
# outcome:
#   create and split and record the set, the vault's key destroyed; grant,
#   rotate the grant, replace the device; rotate the data key, retire the old
#   epoch; authorise a recovery; revoke; regrant the recovered node over the
#   revoked grant; erase (a scan from the wrong epoch refused, then erased,
#   then asked again); after a restart, with the probed keys pinned, the
#   ledger still holds the erasure and the resource is not created again.
#
#   ./scripts/kagi-storage-effect-e2e.sh     (KEEP=1 keeps the work directory)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in kagi_storage_effect storage_key storage_custodian security_state remote_channel; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-kagi-storage-XXXXXX)"
NODE_PID=""
stop_node() {
  [ -n "$NODE_PID" ] || return 0
  pkill -TERM -P "$NODE_PID" 2>/dev/null || true; kill "$NODE_PID" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true; NODE_PID=""
}
cleanup() {
  stop_node
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -40 "$D/node.log" 2>/dev/null || true; exit 1; }

# The node's seal key and store: the vault keeps the custodians' recovery keys
# and the storage-grant key under it, and the ledger lives in the store.
SEAL="$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')"
mkdir -p "$D/store"
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

run_node() { # run_node <graph>: its driver dials it until it is up
  FLUXOR_PROJECT_ROOT="$ROOT" FLUXOR_SEAL_KEY="$SEAL" FLUXOR_STORE_DIR="$D/store" \
    fluxor run "$1" >> "$D/node.log" 2>&1 < /dev/null &
  NODE_PID=$!
}

custodians() { # the three custodian modules, one indent level in
  for i in 0 1 2; do
    cat <<EOF
  - name: c$i
    type: storage_custodian
    params:
      index: $i
      label: "custodian-$i"
EOF
  done
}

# ── 1. the custodians' recovery keys ──
P="$(free_port)"
{
  cat <<EOF
target: linux
tick_us: 1000
module_search_paths:
  - $ROOT/modules/app
platform:
  net: {}
modules:
  - name: probe
    type: remote_channel
    transport: net
    role: listen
    port: $P
    require_peer: 0
    channels: "OctetStream:bytes:16384"
EOF
  custodians
  cat <<EOF
wiring:
  - from: linux_net.net_out
    to: probe.transport_in
  - from: probe.transport_out
    to: linux_net.net_in
EOF
  for i in 0 1 2; do
    printf '  - from: probe.ch0_rx\n    to: c%s.orders\n  - from: c%s.results\n    to: probe.ch0\n' "$i" "$i"
  done
} > "$D/probe.yaml"
run_node "$D/probe.yaml"
KEYS="$(PYTHONPATH="$ROOT/scripts/support" python3 -B - "$P" <<'PY'
import socket, struct, sys, time, fxrc
s = fxrc.connect(int(sys.argv[1]), timeout=20)
keys = {}
buf = b""
for attempt in range(40):
    for i in range(3):
        if i not in keys:
            s.sendall(bytes([0x84]) + struct.pack("<H", 5) + struct.pack("<I", 100 + i) + bytes([i]))
    deadline = time.time() + 0.5
    s.settimeout(0.5)
    while len(keys) < 3 and time.time() < deadline:
        try:
            c = s.recv(65536)
        except socket.timeout:
            break
        assert c, "closed"
        buf += c
        while len(buf) >= 3 and len(buf) >= 3 + struct.unpack("<H", buf[1:3])[0]:
            t, n = buf[0], struct.unpack("<H", buf[1:3])[0]
            p, buf = buf[3:3 + n], buf[3 + n:]
            if t != 0x85:
                continue
            corr, idx, status, ln = struct.unpack("<IBBB", p[:7])
            if status == 0 and ln == 65:
                keys[idx] = p[7:7 + 65].hex()
    if len(keys) == 3:
        break
assert len(keys) == 3, f"custodian keys: {sorted(keys)}"
assert len(set(keys.values())) == 3, "three distinct recovery keys"
print(" ".join(keys[i] for i in range(3)))
PY
)" || fail "the custodians did not name their recovery keys"
stop_node
read -r K0 K1 K2 <<< "$KEYS"
echo "  ok  three custodian recovery keys"

# ── 2. the control plane ──
graph() { # graph <port> <pinned: 1|0>
  cat <<EOF
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
    port: $1
    require_peer: 0
    channels: "OctetStream:bytes:16384"
  - name: kagi_storage_effect
    params:
      aead: 1
EOF
  if [ "$2" = 1 ]; then
    printf '      custodian_1: "%s"\n      custodian_2: "%s"\n      custodian_3: "%s"\n' "$K0" "$K1" "$K2"
  fi
  cat <<EOF
  - name: storage_key
    params:
      iss: "https://kagi.nanocloud.test"
  - name: security_state
    params:
      authority: "single-node"
EOF
  custodians
  cat <<EOF
wiring:
  - from: linux_net.net_out
    to: requests.transport_in
  - from: requests.transport_out
    to: linux_net.net_in
  - from: requests.ch0_rx
    to: kagi_storage_effect.request_in
  - from: kagi_storage_effect.response_out
    to: requests.ch0
  - from: kagi_storage_effect.request_out
    to: storage_key.request_in
  - from: storage_key.response_out
    to: kagi_storage_effect.response_in
  - from: kagi_storage_effect.key_out
    to: storage_key.signing_key
  - from: storage_key.ledger_out
    to: security_state.requests
  - from: security_state.replies
    to: storage_key.ledger_in
EOF
  for i in 0 1 2; do
    printf '  - from: storage_key.key_announce\n    to: c%s.verify_key\n' "$i"
    printf '  - from: storage_key.custodian_out\n    to: c%s.orders\n' "$i"
    printf '  - from: c%s.results\n    to: storage_key.custodian_in\n' "$i"
    printf '  - from: kagi_storage_effect.custodian_req\n    to: c%s.orders\n' "$i"
    printf '  - from: c%s.results\n    to: kagi_storage_effect.custodian_in\n' "$i"
  done
}

driver() { # driver <port> <script>
  PYTHONPATH="$ROOT/scripts/support" python3 -B - "$1" "$2" <<'PY'
import socket, struct, sys, time, fxrc
port, script = int(sys.argv[1]), sys.argv[2]
s = fxrc.connect(port, timeout=60)

def rec(fields):
    out = bytes([len(fields)])
    for num, val in fields:
        if isinstance(val, int):
            ty, b = 1, struct.pack("<q", val)
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
                if p + 4 > len(buf):
                    ok = False; break
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
        chunk = s.recv(65536)
        assert chunk, "the node closed the request stream"
        buf += chunk

cid = 0
def op(code, a=(), n1=0, n2=0, claim="default/data"):
    global cid
    cid += 1
    f = [(1, cid), (2, code), (3, claim)]
    for i, v in enumerate(a):
        f.append((4 + i, v))
    f += [(9, n1), (10, n2), (254, rec([(31, "ph-" + str(cid))]))]
    s.sendall(rec(f))
    o = read_rec()
    assert o[1] == cid and o[2] == code, o
    assert 254 in o, "carry echoed"
    for k in range(1, 17):
        assert k in o, f"field {k} present"
    return o

def check(o, want, what):
    assert o[4] == want, f"{what}: {o[4]} (wanted {want}) {o}"
    print(f"  ok  {what}: {o[4]}")

def hexid(v):
    return len(v) == 32 and all(c in "0123456789abcdef" for c in v)

VOL = "default:/volumes/data"
if script == "restart":
    o = op(24, [VOL, "default", "possession"], 0, 3600)
    check(o, "refused", "after a restart: the erased resource is not created again")
    sys.exit(0)

o = op(24, [VOL, "default", "possession"], 0, 3600); check(o, "ok", "create (epoch 1)")
o = op(24, [VOL, "default", "possession"], 0, 3600); check(o, "ok", "create asked again")
o = op(31, [VOL]); check(o, "ok", "split in the provisioning vault")
o = op(25, [VOL]); check(o, "ok", "record the set")
assert hexid(o[11]), o[11]; set1 = o[11]
o = op(25, [VOL]); check(o, "ok", "record asked again"); assert o[11] == set1
o = op(32, [VOL]); check(o, "ok", "the vault's key destroyed")
o = op(20, [VOL, "node-a", "possession"], 0, 3600); check(o, "ok", "grant node-a")
assert o[12] == "node-a", o[12]
o = op(20, [VOL, "node-a", "possession"], 0, 3600); check(o, "ok", "grant asked again")
o = op(20, [VOL, "node-b", "possession"], 0, 3600); check(o, "refused", "a grant over node-a's")
o = op(60, [VOL, "node-a", "possession", "ops"], 0, 600); check(o, "ok", "rotate the grant")
o = op(21, ["node-a", "node-b", VOL]); check(o, "ok", "replace node-a with node-b")
assert o[12] == "node-b", o[12]
o = op(65, [VOL, "1", "ops"]); check(o, "ok", "rotate the data key"); assert o[14] == "2", o[14]
o = op(65, [VOL, "1", "ops"]); check(o, "ok", "rotation asked again"); assert o[14] == "2", o[14]
o = op(31, [VOL, "2"]); check(o, "ok", "split epoch 2")
o = op(25, [VOL, "2"]); check(o, "ok", "record epoch 2's set")
assert hexid(o[11]) and o[11] != set1, o[11]
o = op(32, [VOL, "2"]); check(o, "ok", "epoch 2's key destroyed")
o = op(71, [VOL, "1", "ops"]); check(o, "ok", "retire epoch 1")
o = op(71, [VOL, "1", "ops"]); check(o, "ok", "retire asked again")
o = op(71, [VOL, "2", "ops"]); check(o, "refused", "retire the current epoch")
o = op(78, [VOL, "2", "node-r", "ops"], 600); check(o, "ok", "authorise a recovery to node-r")
assert hexid(o[14]), o[14]
o = op(61, [VOL, "ops"]); check(o, "ok", "revoke")
o = op(61, [VOL, "ops"]); check(o, "ok", "revoke asked again")
o = op(80, [VOL, "node-r", "ops", "node-b"]); check(o, "ok", "regrant node-r over the revoked grant")
assert o[12] == "node-r", o[12]
o = op(81, [VOL, "ops", "1"]); check(o, "refused", "erase under a scan of a retired epoch")
o = op(81, [VOL, "ops", "2"]); check(o, "ok", "erase")
o = op(81, [VOL, "ops", "2"]); check(o, "ok", "erase asked again")
o = op(20, [VOL, "node-z", "possession"], 0, 60); check(o, "refused", "a grant over the erasure")
PY
}

P="$(free_port)"; graph "$P" 0 > "$D/cp.yaml"
run_node "$D/cp.yaml"
driver "$P" all || fail "an outcome was wrong"
grep -q "custodian recovery keys named by the custodians" "$D/node.log" || fail "the keys were not asked of the custodians"
stop_node
P="$(free_port)"; graph "$P" 1 > "$D/cp.yaml"
run_node "$D/cp.yaml"
driver "$P" restart || fail "the ledger did not survive a restart"
stop_node
echo "PASS: kagi_storage_effect drives Kagi's storage-key service, its ledger and custodians, live"
