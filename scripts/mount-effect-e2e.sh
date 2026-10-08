#!/usr/bin/env bash
# mount_effect, the claim chain's mount actuator, on a real block device.
#
# A loop device over an image stands in for a volume's NBD export. The
# actuator runs in a real graph as root (the mount privilege), its tools
# through the process executor; a Python driver sends Mnt records and checks
# the raw facts:
#   a blank device with mkfs asked: made, mounted nodev,nosuid at
#   <root>/<claim>; unmounted; mounted again: checked (fsck), not remade;
#   unmounted twice (nothing the second time); a source that is no block device refused; and, unprivileged,
#   nothing runs (priv 0).
#
#   ./scripts/mount-effect-e2e.sh     (KEEP=1 keeps the work directory)
#
# Needs passwordless sudo, losetup and mkfs.ext4; without them it skips, and
# NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"

skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
[ -e "$MODULES_DIR/mount_effect.fmod" ] || { echo "FAIL: missing mount_effect.fmod"; exit 1; }
sudo -n true 2>/dev/null || skip "needs passwordless sudo (mount)"
command -v losetup >/dev/null && command -v mkfs.ext4 >/dev/null || skip "needs losetup and mkfs.ext4"

D="$(mktemp -d /tmp/nc-mount-effect-XXXXXX)"
LOOP=""
NODE_PID=""
cleanup() {
  [ -n "$NODE_PID" ] && { sudo -n pkill -TERM -P "$NODE_PID" 2>/dev/null || true; sudo -n kill "$NODE_PID" 2>/dev/null || true; }
  sudo -n umount "$D/mnt/default/data" 2>/dev/null || true
  [ -n "$LOOP" ] && sudo -n losetup -d "$LOOP" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -40 "$D/node.log" 2>/dev/null || true; exit 1; }

truncate -s 32M "$D/vol.img"
LOOP="$(sudo -n losetup --find --show "$D/vol.img")"
mkdir -p "$D/mnt"

graph() { # graph <port> <file>
  cat > "$2" <<EOF
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
  - name: mount_effect
wiring:
  - from: linux_net.net_out
    to: requests.transport_in
  - from: requests.transport_out
    to: linux_net.net_in
  - from: requests.ch0_rx
    to: mount_effect.request_in
  - from: mount_effect.response_out
    to: requests.ch0
EOF
}
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

run_node() { # run_node <as: root|user> <port>
  graph "$2" "$D/node-$1.yaml"
  local env=(FLUXOR_PROJECT_ROOT="$ROOT" FLUXOR_MOUNT_ROOT="$D/mnt"
             SECTOR_PROC_ALLOW="blkid mkfs fsck" PATH="/usr/sbin:/sbin:$PATH" HOME="$HOME")
  if [ "$1" = root ]; then
    sudo -n env "${env[@]}" "$(command -v fluxor)" run "$D/node-$1.yaml" > "$D/node.log" 2>&1 < /dev/null &
  else
    env "${env[@]}" fluxor run "$D/node-$1.yaml" > "$D/node.log" 2>&1 < /dev/null &
  fi
  NODE_PID=$!
}
stop_node() {
  sudo -n pkill -TERM -P "$NODE_PID" 2>/dev/null || true; sudo -n kill "$NODE_PID" 2>/dev/null || true
  wait "$NODE_PID" 2>/dev/null || true; NODE_PID=""
}

driver() { # driver <port> <script>
  PYTHONPATH="$ROOT/scripts/support" python3 -B - "$1" "$LOOP" "$D/mnt" "$2" <<'PY'
import struct, sys, time, os, fxrc
port, loop, mnt, script = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]
s = fxrc.connect(port, timeout=120)
def rec(fields):
    out = bytes([len(fields)])
    for num, val in fields:
        if isinstance(val, int):
            ty, b = 1, struct.pack("<q", val)
        else:
            ty, b = 0, val if isinstance(val, bytes) else val.encode()
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
                f, buf = buf[:p], buf[p:]
                o, q = {}, 1
                for _ in range(f[0]):
                    num, ty, ln = f[q], f[q+1], struct.unpack("<H", f[q+2:q+4])[0]
                    v = f[q+4:q+4+ln]
                    o[num] = struct.unpack("<q", v)[0] if ty == 1 else v.decode()
                    q += 4 + ln
                return o
        c = s.recv(65536); assert c, "closed"; buf += c
cid = 0
def mnt_op(op, claim, dev="", fs="", mkfs=""):
    global cid
    cid += 1
    s.sendall(rec([(1, cid), (2, op), (3, claim), (4, dev), (5, fs), (6, ""), (7, mkfs),
                   (254, rec([(31, "m")]))]))
    o = read_rec()
    assert o[1] == cid and o[2] == op and 254 in o, o
    return o
def show(what, o):
    print(f"  ok  {what}: priv={o[4]} mkfs={o[9]} fsck={o[5]} code={o[6]} was={o[8]} dir={o[7]}")
CL = "default/data"
if script == "unprivileged":
    o = mnt_op(50, CL, loop, "ext4", "1")
    assert o[4] == 0 and o[9] == -1 and o[5] == -1, o; show("unprivileged mount", o)
    sys.exit(0)
o = mnt_op(50, CL, loop, "ext4", "1")
assert (o[4], o[9], o[5], o[6]) == (1, 0, -1, 0), o
assert o[7] == f"{mnt}/{CL}", o[7]; show("blank device made and mounted", o)
opts = [l for l in open("/proc/self/mountinfo") if f" {mnt}/{CL} " in l]
assert opts and "nodev" in opts[0] and "nosuid" in opts[0], opts
o = mnt_op(51, CL); assert (o[4], o[8], o[6]) == (1, 1, 0), o; show("unmount", o)
o = mnt_op(50, CL, loop, "ext4", "1")
assert (o[4], o[9], o[6]) == (1, -1, 0) and o[5] in (0, 1), o; show("mounted again: checked, not remade", o)
o = mnt_op(51, CL); assert o[8] == 1, o; show("unmount", o)
o = mnt_op(51, CL); assert (o[8], o[6]) == (0, 0), o; show("nothing mounted", o)
o = mnt_op(50, "default/odd", "/dev/null", "ext4", "")
assert o[4] == 1 and o[6] != 0 and o[7] == "", o; show("a character device", o)
PY
}

# ── as root ──
P="$(free_port)"; run_node root "$P"
driver "$P" privileged || fail "an answer was wrong"
stop_node
# ── unprivileged: nothing runs ──
P="$(free_port)"; run_node user "$P"
driver "$P" unprivileged || fail "an unprivileged answer was wrong"
stop_node
echo "PASS: mount_effect makes, checks, mounts and unmounts a real block device"
