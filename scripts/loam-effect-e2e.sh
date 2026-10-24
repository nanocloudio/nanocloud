#!/usr/bin/env bash
# loam_effect against Loam's admin gate over mutual TLS, under capabilities.
#
# Loam runs as its operators run it: loam's published `loam-admin` bundle,
# run by name under `fluxor run` with a CA, the node's server identity and a
# mesh root as its params. The connector runs in a real graph,
# once per capability file, each instance behind a client-mode tls under the
# node's certificate, its effects arriving on a listening remote_channel. A
# Python driver sends each pipeline op and checks the outcome.
#
# Under the node's capability (`nanocloud/` and the body plane, read and
# command):
#   create, and its re-run naming the same volume; a lease acquired, its
#   holder re-acquiring under the same fence, another claim held off,
#   renewed; a recovery's lease held apart from the claim's own; a snapshot,
#   its re-run, its restore, a restore from no snapshot; a delete refused
#   under the lease, then released, a renew after release lost, deleted,
#   deleted again, the path created afresh; the snapshot dropped; sizes.
# Each refused, never answered otherwise and never left hanging:
#   no capability file (unavailable: the operator has yet to mint one); a
#   chain from another mesh root; an expired chain; a read-only chain (no
#   lease); a chain without the body plane (no volume page); a root the
#   capability does not reach.
#
#   ./scripts/loam-effect-e2e.sh     (KEEP=1 keeps the work directory)
#
# Needs loam's `loam-admin` bundle pinned (`fluxor update`); without it it
# skips, and NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/support/loam-admin.sh"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"

skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not on PATH"; exit 1; }
nc_loam_present || skip "loam-admin is not pinned (fluxor update)"
for m in loam_effect remote_channel tls; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-loam-effect-XXXXXX)"
NODE_PID=""
stop() { # stop <pid>: the graph and what it started
  [ -n "$1" ] || return 0
  pkill -TERM -P "$1" 2>/dev/null || true
  kill "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}
cleanup() {
  stop "$NODE_PID"
  stop "$LOAM_PID"
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -40 "$D/node.log" 2>/dev/null || true; tail -20 "$D/loam.log" 2>/dev/null || true; exit 1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# ── PKI: one CA; Loam's node for localhost; this node as CN node-a ──
cd "$D"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=loam test ca" -keyout ca.key -out ca.pem \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
leaf() { # leaf <name> <subject> <extensions>
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "$2" \
    -keyout "$1.key" -out "$1.csr" 2>/dev/null
  printf '%s\n' "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile "$1.ext" -out "$1.pem" 2>/dev/null
  openssl x509 -in "$1.pem" -outform DER -out "$1.der"
}
leaf server "/CN=loam" "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf node "/CN=node-a" "extendedKeyUsage=clientAuth"
openssl ec -in server.key -outform DER -out server.key.der 2>/dev/null
openssl x509 -in ca.pem -outform DER -out ca.der
openssl pkcs8 -topk8 -nocrypt -in node.key -outform DER -out node.key.der

# ── capabilities: the mesh root Loam trusts, and one it does not ──
MESH="$(nc_loam_mesh "$D")"
mkdir foreign && nc_loam_mesh "$D/foreign" > /dev/null
BODY_PLANE="$(nc_loam_body_plane)"
NOW="$(date +%s)"
nc_loam_node_cap root.seed nanocloud node.cap || fail "minting node.cap"
nc_loam_node_cap foreign/root.seed nanocloud foreign.cap || fail "minting foreign.cap"
{ nc_loam_mint root.seed --scope nanocloud/ read_state,send_command $((NOW - 7200)) $((NOW - 3600))
  nc_loam_mint root.seed --object "$BODY_PLANE" read_state,send_command; } > expired.cap
{ nc_loam_mint root.seed --scope nanocloud/ read_state
  nc_loam_mint root.seed --object "$BODY_PLANE" read_state; } > readonly.cap
nc_loam_mint root.seed --scope nanocloud/ read_state,send_command > nobody.cap
for c in expired readonly nobody; do
  grep -q '^fxcap1\.' "$c.cap" || fail "minting $c.cap"
done

# ── Loam ──
LOAM_PORT="$(free_port)"
nc_loam_start "$D" "$LOAM_PORT" "$MESH" "$D/ca.pem" "$D/server.der" "$D/server.key.der" "$D/loam.log" \
  || fail "Loam's admin gate did not come up"

# ── the node: one loam_effect per case, each its own channel of one
# listening remote_channel ──
# name capability root
CASES="main:node.cap:nanocloud
missing:none.cap:nanocloud
foreign:foreign.cap:nanocloud
expired:expired.cap:nanocloud
readonly:readonly.cap:nanocloud
nobody:nobody.cap:nanocloud
elsewhere:node.cap:other"
NCASES="$(printf '%s\n' "$CASES" | wc -l)"
REQ_PORT="$(free_port)"
TABLE="$(for _ in $(seq "$NCASES"); do printf 'OctetStream:bytes:16384;'; done)"
{
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
    port: $REQ_PORT
    require_peer: 0
    channels: "${TABLE%;}"
EOF
  while IFS=: read -r name cap root; do
    cat <<EOF
  - name: tls_$name
    type: tls
    mode: 0
    peer_auth: ca_dns
    verify_hostname: localhost
    trust: "\${file:$D/ca.der}"
    cert_file: $D/node.der
    key_file: $D/node.key.der
  - name: eff_$name
    type: loam_effect
    admin: "localhost:$LOAM_PORT"
    capability: $D/$cap
    root: $root
EOF
  done <<< "$CASES"
  cat <<EOF
wiring:
  - from: linux_net.net_out
    to: requests.transport_in
  - from: requests.transport_out
    to: linux_net.net_in
EOF
  i=0
  while IFS=: read -r name _ _; do
    echo "$name $i" >> channels
    cat <<EOF
  - from: linux_net.net_out
    to: tls_$name.cipher_in
  - from: tls_$name.cipher_out
    to: linux_net.net_in
  - from: tls_$name.clear_out
    to: eff_$name.admin_in
  - from: eff_$name.admin_out
    to: tls_$name.clear_in
  - from: requests.ch${i}_rx
    to: eff_$name.request_in
  - from: eff_$name.response_out
    to: requests.ch$i
EOF
    i=$((i + 1))
  done <<< "$CASES"
} > node.yaml
FLUXOR_PROJECT_ROOT="$ROOT" fluxor run node.yaml > node.log 2>&1 < /dev/null &
NODE_PID=$!

# ── the driver ──
PYTHONPATH="$ROOT/scripts/support" python3 -B - "$REQ_PORT" "$D/channels" <<'PY' || fail "an outcome was wrong"
import struct, sys, fxrc

index = {name: int(i) for name, i in (line.split() for line in open(sys.argv[2]))}
session = fxrc.connect(int(sys.argv[1]), timeout=60, channels=len(index))
chans = {name: session[i] for name, i in index.items()}

def rec(fields):
    out = bytes([len(fields)])
    for num, val in fields:
        if isinstance(val, int):
            ty, b = 1, struct.pack("<q", val)
        else:
            ty, b = 0, val.encode() if isinstance(val, str) else val
        out += bytes([num, ty]) + struct.pack("<H", len(b)) + b
    return out

bufs = {name: b"" for name in chans}
def read_rec(name):
    s = chans[name]
    while True:
        buf = bufs[name]
        if buf:
            n, p, ok = buf[0], 1, True
            for _ in range(n):
                if p + 4 > len(buf):
                    ok = False; break
                p += 4 + struct.unpack("<H", buf[p+2:p+4])[0]
            if ok and p <= len(buf):
                frame, bufs[name] = buf[:p], buf[p:]
                out, q = {}, 1
                for _ in range(frame[0]):
                    num, ty, ln = frame[q], frame[q+1], struct.unpack("<H", frame[q+2:q+4])[0]
                    v = frame[q+4:q+4+ln]
                    out[num] = struct.unpack("<q", v)[0] if ty == 1 else v.decode()
                    q += 4 + ln
                return out
        bufs[name] += s.recv(65536)

cid = 0
def op(code, claim, a=(), n1=0, on="main"):
    global cid
    cid += 1
    f = [(1, cid), (2, code), (3, claim)]
    for i, v in enumerate(a):
        f.append((4 + i, v))
    f.append((9, n1))
    f.append((254, rec([(31, "ph-" + str(cid))])))
    chans[on].sendall(rec(f))
    o = read_rec(on)
    assert o[1] == cid and o[2] == code, o
    assert 254 in o, "carry echoed"
    for k in range(1, 17):
        assert k in o, f"field {k} present"
    return o

def check(o, want, what):
    assert o[4] == want, f"{what}: {o[4]} (wanted {want})"
    print(f"  ok  {what}: {o[4]}")

CL = "default/data"
o = op(30, CL, ["default", "data", "1048576"]); check(o, "ok", "create")
vol = o[10]; assert vol == "nanocloud:/default/volumes/data", vol
o = op(30, CL, ["default", "data", "1048576"]); check(o, "ok", "create again")
assert o[10] == vol

o = op(1, CL, [vol]); check(o, "ok", "lease acquire")
fence = o[5]; assert fence > 0 and o[6] > 0, o
o = op(1, CL, [vol]); check(o, "ok", "the holder acquires again")
assert o[5] == fence, "and keeps its fence"
o = op(1, "default/other", [vol]); check(o, "held", "another claim's acquire")
o = op(2, CL, [vol], fence); check(o, "ok", "lease renew")
assert o[6] > 0

o = op(73, "default/data/s1", [vol, "default/data/s1"]); check(o, "ok", "snapshot")
snap = o[14]; assert snap == "nanocloud:/default/snapshots/data/s1", snap
o = op(73, "default/data/s1", [vol, "default/data/s1"]); check(o, "ok", "snapshot again")
assert o[14] == snap
o = op(74, "default/data/s1", [snap, "default/clone"]); check(o, "ok", "restore")
assert o[14] == "nanocloud:/default/volumes/clone", o[14]
o = op(74, "default/none", ["nanocloud:/default/snapshots/none", "default/other"])
check(o, "failed", "a restore from no snapshot")

o = op(82, CL, [vol]); check(o, "held", "delete under the lease")
o = op(3, CL, [vol], fence); check(o, "ok", "lease release")
o = op(2, CL, [vol], fence); check(o, "lost", "a renew after the release")
R = [vol, "", "", "", "recovery"]
o = op(1, CL, R); check(o, "ok", "a recovery's lease, as rc:<claim>")
rfence = o[5]
o = op(1, CL, [vol]); check(o, "held", "the claim's own acquire under the recovery")
o = op(3, CL, R, rfence); check(o, "ok", "the recovery's release")
o = op(82, CL, [vol]); check(o, "ok", "delete")
o = op(82, CL, [vol]); check(o, "ok", "delete again")
o = op(30, CL, ["default", "data", "1048576"]); check(o, "ok", "the path created afresh")
o = op(76, "default/data/s1", ["default/data/s1"]); check(o, "ok", "snapshot drop")

o = op(30, "default/sized", ["default", "sized", "64Mi"]); check(o, "ok", "create of a 64Mi claim")
o = op(30, "default/odd", ["default", "odd", "1.5Gi"]); check(o, "failed", "a fractional quantity")

A = ["default", "x", "1048576"]
check(op(30, "default/x", A, on="missing"), "unavailable", "no capability file")
check(op(30, "default/x", A, on="foreign"), "refused", "a chain from another mesh root")
check(op(30, "default/x", A, on="expired"), "refused", "an expired chain")
check(op(1, CL, [vol], on="readonly"), "refused", "a lease under a read-only chain")
check(op(30, "default/x", A, on="nobody"), "refused", "a create without the body plane")
check(op(30, "default/x", A, on="elsewhere"), "refused", "a root the capability does not reach")
o = op(1, "default/sized", ["nanocloud:/default/volumes/sized"]); check(o, "ok", "the node still served")
print("PASS")
PY
grep -q "\[loam_effect\] session with the gate ready" node.log || fail "no session was said ready"
grep -q "\[loam_effect\] the gate refused this node's capability" node.log || fail "the refusal was not said"
grep -q "the capability file cannot be opened" node.log || fail "the missing file was not said"
echo "PASS: loam_effect drives Loam's admin gate over mutual TLS, under capabilities"
