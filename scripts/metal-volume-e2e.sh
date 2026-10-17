#!/usr/bin/env bash
# Live E2E: a STATEFUL workload on a node that runs module subgraphs — the
# bare-metal node's shape (scripts/metal-plane-e2e.sh: the control plane and
# the kubelet's graph-pod slice on one node-scoped store), a Loam volume as
# the Pod's storage, and a Pod whose subgraph is
#
#   tls ──(net lane)── Loam's admin gate      the volume's admin plane
#   loam_volume.blocks → stateful_probe        the Pod's block device
#
# stateful_probe keeps an epoch in the volume's first block: each start reads
# it back, adds one, writes and flushes. The Pod is created, reaches Running
# and logs epoch=1; it is deleted and created again and logs epoch=2 — the
# state lived in the volume between two lives of the Pod.
#
# Loam runs as its operators run it (the loam-admin template from a Loam
# checkout, scripts/support/loam-admin.sh); the volume is created with the
# operator's applet (`fluxor exec loam -- volume create`). Needs a Loam
# checkout ($LOAM_ROOT, default ../loam); without one it skips, and
# NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
#
# The Graph object carries this node's TLS material and capability chains
# inline (the FLXA's extended entries), as volume_host stages a volume's
# subgraph with: an E2E's shortcut. A node substituting its own identity into
# a Graph that asks for it is not built yet (the next step of E3).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
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
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
nc_loam_present || skip "no Loam checkout at $LOAM_ROOT (set LOAM_ROOT)"
D="$(mktemp -d /tmp/nc-metal-volume-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
stop() { [ -n "$1" ] || return 0; pkill -TERM -P "$1" 2>/dev/null || true; kill "$1" 2>/dev/null || true; wait "$1" 2>/dev/null || true; }
cleanup() {
  stop "$RUNTIME_PID"
  stop "$LOAM_PID"
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; tail -10 "$D/loam.log" 2>/dev/null || true; exit 1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

store_put() { # store_put <key> <hex-value>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
import struct, sys, os, binascii
path, key, val = sys.argv[1], sys.argv[2].encode(), binascii.unhexlify(sys.argv[3])
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
put_text() { store_put "$1" "$(printf '%s' "$2" | python3 -c 'import sys,binascii;print(binascii.hexlify(sys.stdin.buffer.read()).decode())')"; }

echo "== 1. Loam: a CA, the gate's and this node's identities, a mesh root, the node's capability =="
cd "$D"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
  -subj "/CN=metal volume test ca" -keyout ca.key -out ca.pem \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
leaf() { # leaf <name> <subject> <extensions>
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -subj "$2" -keyout "$1.key" -out "$1.csr" 2>/dev/null
  printf '%s\n' "$3" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 -extfile "$1.ext" -out "$1.pem" 2>/dev/null
  openssl x509 -in "$1.pem" -outform DER -out "$1.der"
}
leaf server "/CN=loam" "subjectAltName=DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth"
leaf node "/CN=node-1" "extendedKeyUsage=clientAuth"
openssl ec -in server.key -outform DER -out server.key.der 2>/dev/null
openssl x509 -in ca.pem -outform DER -out ca.der
openssl pkcs8 -topk8 -nocrypt -in node.key -outform DER -out node.key.der
MESH="$(nc_loam_mesh "$D")"
nc_loam_node_cap root.seed nanocloud node.cap || fail "minting node.cap"
LOAM_PORT="$(free_port)"
mkdir -p "$D/loam"
nc_loam_start "$D/loam" "$LOAM_PORT" "$MESH" "$D/ca.pem" "$D/server.pem" "$D/server.key.der" "$D/loam.log" \
  || fail "loam's admin gate did not come up"
echo "   loam-admin on :$LOAM_PORT"

echo "== 2. the operator creates the Pod's volume: nanocloud:/default/volumes/probe =="
# The applet runs from the real store, under a private HOME (its client identity);
# the plane's identity below is minted with the real HOME's Python packages.
REAL_HOME="$HOME"
export FLUXOR_STORE="${FLUXOR_STORE:-${XDG_DATA_HOME:-$HOME/.local/share}/fluxor/store}"
export HOME="$D/home" XDG_DATA_HOME="$D/home/data" XDG_STATE_HOME="$D/home/state"
export FLUXOR_APPLETS="$D/home/applets.toml" FLUXOR_CA_BUNDLE="$D/ca.pem"
export FLUXOR_INSTALL_ROOT="${FLUXOR_INSTALL_ROOT:-$ROOT/../fluxor}"
mkdir -p "$D/home/.config/loam"
cp node.der "$D/home/.config/loam/client.der"
cp node.key.der "$D/home/.config/loam/client.key.der"
(cd "$LOAM_ROOT" && timeout 600 fluxor install packaging/cli/workload.toml) > "$D/install.log" 2>&1 || fail "installing loam's applet: $(tail -3 "$D/install.log")"
timeout 120 fluxor exec loam -- --admin "localhost:$LOAM_PORT" --capability "$D/node.cap" \
  volume create nanocloud /default/volumes/probe 1048576 32768 > "$D/vcreate.log" 2>&1 \
  || fail "volume create: $(tail -3 "$D/vcreate.log")"
echo "   created (1 MiB, 32 KiB extents)"

echo "== 3. the plane's identity and the Pod's Graph: tls → loam_volume → stateful_probe =="
cd "$ROOT"
env HOME="$REAL_HOME" python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
ISSUER, KID = b"https://kubernetes.default.svc", b"sa-1"
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1)
        + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex","w").write(binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"
now = int(time.time())
claims = {"iss": ISSUER.decode(), "sub": "system:serviceaccount:default:admin", "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/token.admin","w").write((signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode())
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
put_text "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
put_text "/nodes/node-1" '{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1"},"status":{"ready":true,"conditions":[{"type":"Ready","status":"True"}]}}'
printf 'localhost' > "$D/hostname"
# tls: client, peer_auth ca_dns, the clock unchecked (a test CA's 2-day
# certificates); its DER material as extended entries. loam_volume: root,
# path, lease, block size, the gate, this node's capability chains (an
# extended entry: the subgraph has no files of its own to read them from).
FLXA="$(python3 "$ROOT/scripts/support/flxa.py" \
  "tls:1=0,11=2,12=1,10=f:$D/node.der,11=f:$D/node.key.der,12=f:$D/ca.der,13=f:$D/hostname" \
  "loam_volume:1=s:nanocloud,2=s:/default/volumes/probe,3=u32:5000,4=u32:4096,8=s:127.0.0.1:$LOAM_PORT,20=f:$D/node.cap" \
  "stateful_probe" \
  --edge 0:0:0 3:0:0 0 --edge 3:0:0 0:0:0 0 \
  --edge 0:0:1 0:1:0 131072 --edge 0:1:1 0:0:1 131072 \
  --edge 0:1:0 0:2:0 0)"
put_text "/graphs.nanocloud.io/probe-vol" "{\"apiVersion\":\"nanocloud.io/v1\",\"kind\":\"Graph\",\"metadata\":{\"name\":\"probe-vol\"},\"spec\":{\"flxa\":\"$FLXA\"}}"

echo "== 4. render the node: the plane + the kubelet's graph-pod slice on one scoped store; pack the Pod's module types =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
# A deleted object's tomb lives 3 s here, not 300: step 7 sees it end, and
# with it every record the node kept for the Pod.
sed -i '/^    # A delete leaves the object.s last state/{n;s|^    tomb: "/~tomb"$|&\n    tomb_ms: 3000|}' "$D/graph.yaml"
[ "$(grep -c 'tomb_ms: 3000' "$D/graph.yaml")" = 1 ] || fail "the tomb TTL splice did not apply"
python3 - "$D/graph.yaml" "$ROOT" node-1 <<'PY2' || fail "the splice did not apply"
import re, sys
out, root, node = sys.argv[1:4]
g = open(out).read()
def sub(old, new):
    global g
    if g.count(old) != 1: sys.exit(f"expected 1 x {old[:50]!r}, found {g.count(old)}")
    g = g.replace(old, new)
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/")
sub('      scope: ""\n      scoped: ""\n', f'      scope: "{node}"\n      scoped: "{SCOPED}"\n')
kube = open(f"{root}/packaging/debian/fluxor-podlifecycle.yaml").read()
kube_mods = kube[kube.index("modules:\n") + len("modules:\n"):kube.index("\nwiring:")]
kube_wire = kube[kube.index("wiring:\n") + len("wiring:\n"):]
KEEP_PREFIX = ("pj_", "pl_", "pr_")
KEEP = {"wl", "sb_mspec", "sb_mspecd", "sb_sig", "sb_sigd", "sb_pause", "sb_paused", "sb_ev", "sb_evout"}
def kept(name): return name in KEEP or name.startswith(KEEP_PREFIX)
blocks = re.split(r"(?m)^(?=  - name: )", kube_mods)
kept_mods = "".join(b for b in blocks if b.startswith("  - name: ") and kept(b.split()[2]))
pairs, frm = [], None
for line in kube_wire.splitlines():
    m = re.match(r"  - from: (\S+)$", line)
    if m: frm = m.group(1); continue
    m = re.match(r"    to: (\S+)$", line)
    if m and frm: pairs.append((frm, m.group(1))); frm = None
edges = "".join(f"  - from: {a}\n    to: {b}\n" for a, b in pairs if kept(a.split(".")[0]) and kept(b.split(".")[0]))
mods, wiring = g.split("\nwiring:\n", 1)
g = (mods.rstrip("\n") + "\n  # ── The pod plane: fluxor-podlifecycle.yaml, its graph-pod slice ──\n"
     + kept_mods.rstrip("\n") + "\n\nwiring:\n" + wiring.rstrip("\n") + "\n" + edges
     + "\nworkload_modules:\n  - tls\n  - loam_volume\n  - stateful_probe\n")
open(out, "w").write(g)
PY2
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 5. run; wait for the API over TLS =="
env -C "$STORE" RUST_LOG=info "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"
api() { # api <method> <path> [json-body] -> "<status>|<body>"
  python3 - "$PORT" "$D/token.admin" "$1" "$2" "${3:-}" <<'PY'
import socket, ssl, sys
port, tokf, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
tok = open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
raw = socket.create_connection(("127.0.0.1", port), timeout=10)
s = ctx.wrap_socket(raw, server_hostname="localhost")
b = body.encode()
req = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n"
       f"Content-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b
s.sendall(req); buf = b""; s.settimeout(10)
try:
    while True:
        chunk = s.recv(4096)
        if not chunk: break
        buf += chunk
except (socket.timeout, ssl.SSLError):
    pass
s.close()
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac

POD='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe-vol","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"probe-vol"}]}}'
dump_chain() { # dump_chain <uid>: the node's records of one Pod, in order
  local k
  for k in "/~node/node-1/pod-specs/$1" "/~node/node-1/metal-sandboxes/$1" "/~node/node-1/sandbox-status/$1" \
           "/~node/node-1/pod-runtime/$1" "/~node/node-1/pod-lifecycle-status/$1" "/podstatus/$1"; do
    printf '   %-52s %s\n' "$k" "$(nc_store_get "$STORE" "$k" 2>/dev/null | cut -c1-100)"
  done
}
UIDS=""
life() { # life <expected epoch>: create the Pod, see it Running and its epoch logged
  for _ in $(seq 90); do
    r="$(api POST /api/v1/namespaces/default/pods "$POD")"
    case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
  done
  case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST pod: ${r:0:200}" ;; esac
  local uid
  uid="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
  UIDS="$UIDS $uid"
  local phase="" epoch=""
  for _ in $(seq 120); do
    r="$(api GET /api/v1/namespaces/default/pods/probe-vol)"
    phase="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("status",{}).get("phase",""))
except Exception: print("")' 2>/dev/null)"
    epoch="$( (grep -ao "\[stateful_probe\] epoch=[0-9]*" "$D/run.log" || true) | tail -1 | sed 's/.*epoch=//')"
    [ "$phase" = "Running" ] && [ "${epoch:-0}" = "$1" ] && break
    sleep 1
  done
  echo "   phase=${phase:-} epoch=${epoch:-}"
  [ "$phase" = "Running" ] || fail "the Pod never reached Running (phase='${phase:-}')"
  if [ "${epoch:-0}" != "$1" ]; then
    local u
    for u in $UIDS; do echo "   records of $u:"; dump_chain "$u"; done
    grep -a "\[live\]\|loam_volume\]\|\[wl\]" "$D/run.log" | tail -6 | cut -c1-140
    fail "expected epoch $1, the probe logged '${epoch:-}'"
  fi
}
echo "== 6. first life: the Pod runs over its volume and writes epoch 1 =="
life 1

echo "== 7. the Pod is deleted; its subgraph is torn down =="
r="$(api DELETE /api/v1/namespaces/default/pods/probe-vol)"
case "$r" in 20[0-9]\|*) echo "   DELETE -> ${r%%|*}" ;; *) fail "DELETE pod: ${r:0:200}" ;; esac
for _ in $(seq 60); do
  r="$(api GET /api/v1/namespaces/default/pods/probe-vol)"
  case "$r" in 404\|*) break ;; esac
  sleep 1
done
case "$r" in 404\|*) echo "   gone" ;; *) fail "the Pod did not go away: ${r:0:120}" ;; esac
# The first life's writer lease (5 s here) lapses: a subgraph torn down by
# DESTROY does not drain its lease first — the orderly drain on DESTROY is
# the next step of E3; until then a Pod recreated at once waits it out.
sleep 8
for u in $UIDS; do echo "   records of $u after the delete:"; dump_chain "$u"; done
# Its phase record goes with it: the record names the Pod by NAME, and the
# Pod made next (step 8) is the same name, another incarnation.
for u in $UIDS; do
  for _ in $(seq 30); do
    [ -z "$(nc_store_get "$STORE" "/podstatus/$u" 2>/dev/null)" ] && break
    sleep 1
  done
  [ -z "$(nc_store_get "$STORE" "/podstatus/$u" 2>/dev/null)" ] \
    || fail "the deleted Pod's /podstatus/$u was left behind: $(nc_store_get "$STORE" "/podstatus/$u")"
done
echo "   the deleted Pod's phase record is gone"
# Its tomb ends (3 s), the plane drops the node's spec, and the node drops
# every record it kept for that uid: nothing else removes them, and a node's
# records would otherwise grow by every Pod it ever ran.
NODE_RECS="pod-specs metal-sandboxes sandbox-status sandbox-boot-of pod-runtime pod-lifecycle-status probe-status pod-resolved"
left=""
for u in $UIDS; do
  for _ in $(seq 40); do
    left=""
    for c in $NODE_RECS; do
      [ -n "$(nc_store_get "$STORE" "/~node/node-1/$c/$u" 2>/dev/null)" ] && left="$left $c"
    done
    [ -z "$left" ] && [ -z "$(nc_store_get "$STORE" "/~tomb/pods/default/probe-vol" 2>/dev/null)" ] && break
    sleep 1
  done
  if [ -n "$left" ]; then dump_chain "$u"; fail "the deleted Pod's node records outlived its tomb:$left"; fi
done
echo "   its tomb ended, and the node's records of it went with it"

echo "== 8. second life: the same volume, epoch 2 =="
life 2

echo "== E2E green: a Pod's state lived in its Loam volume between two lives on a node that runs module subgraphs =="
