#!/usr/bin/env bash
# Live E2E for mTLS peer identity through the apiserver graph. Two gates in
# series, and this script proves both:
#
#   1. CHAIN  — `tls` runs peer_auth=ca_dns against the cluster CA: a client
#      certificate must chain to it (CA:FALSE, KU digitalSignature, EKU
#      clientAuth on the leaf) or the handshake is refused. For an mTLS SERVER
#      the profile carries no name rule — a client serves no hostname — so
#      "ca_dns" here means exactly "issued by our CA".
#   2. AUTHZ  — the accepted leaf's pubkey is hashed into a 32-byte SVID and
#      emitted on `peer_identity` → wave's `http` → the envelope TRAILER →
#      `kube_decode` field 12 → `admit`, and the RBAC walk decides. A CA-issued
#      cert bound to admin gets 200/201; a CA-issued cert with no binding gets
#      403.
#
# So: eve (self-signed, no CA) never reaches the apiserver at all; mallory
# (properly issued, unbound) reaches it and is denied. Defence in depth —
# a stolen-but-unissued key cannot even open the connection.
#
# The SVID the graph derives is sha256(uncompressed-EC-point) of the client
# pubkey — reproduced here so we can seed the RBAC binding for it. No host code.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=7445

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
for m in kube_decode kagi_verify rbac_gate api_admission store_effect store_source token_verify decision pipeline http tls; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apiplane-mtls-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

# A self-signed P-256 leaf — used for the CA itself and for the rogue client
# that must NOT be able to reach the apiserver.
gen_p256() { # gen_p256 <name> <cn> [extra openssl args...]
  local n="$1" cn="$2"; shift 2
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$D/$n.key.pem" -out "$D/$n.pem" -days 1 -subj "/CN=$cn" "$@" 2>/dev/null
}

# A P-256 leaf ISSUED BY the cluster CA, with the shape the validator demands:
# not a CA, digitalSignature, and the EKU for the role it plays.
gen_issued() { # gen_issued <name> <cn> <eku>
  local n="$1" cn="$2" eku="$3"
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$D/$n.key.pem" -out "$D/$n.csr" -subj "/CN=$cn" 2>/dev/null
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=%s\nsubjectAltName=DNS:%s\n' \
    "$eku" "$cn" > "$D/$n.ext"
  openssl x509 -req -in "$D/$n.csr" -CA "$D/ca.pem" -CAkey "$D/ca.key.pem" \
    -CAcreateserial -out "$D/$n.pem" -days 1 -extfile "$D/$n.ext" 2>/dev/null
}
svid_of() { # svid_of <name> -> sha256 hex of the 65-byte uncompressed EC point
  openssl x509 -in "$D/$1.pem" -noout -pubkey \
    | openssl pkey -pubin -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1
}

wal_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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

echo "== 1. cluster CA, an issued server cert, and three clients (P-256) =="
gen_p256 ca nanocloud-cluster-ca \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl x509 -in "$D/ca.pem" -outform DER -out "$D/ca.der" 2>/dev/null

gen_issued server apiserver serverAuth
openssl x509 -in "$D/server.pem" -outform DER -out "$D/server.der" 2>/dev/null
openssl ec   -in "$D/server.key.pem" -outform DER -out "$D/server.key.der" 2>/dev/null

gen_issued alice   alice   clientAuth   # issued + bound to admin  -> allowed
gen_issued mallory mallory clientAuth   # issued, no binding       -> 403
gen_p256   eve     eve                  # self-signed, no CA       -> no handshake
ALICE_SVID="$(svid_of alice)"
echo "   cluster CA minted; alice SVID = $ALICE_SVID"

echo "== 2. seed RBAC — bind alice's SVID (not mallory's) to admin =="
wal_put "/roles/admin" "rules=*:*"
# The identity the chain produces is `peer:<hex>`, not the bare hex: two
# authentication methods that could yield the same subject string would let
# a binding written for one silently grant the other.
wal_put "/rolebindings/alice" "subjects=peer:$ALICE_SVID;role=admin"

echo "== 3. write the mTLS Chronicle api-plane graph =="
python3 - "$ROOT" "$D" <<'PYGEN'
import sys
root, d = sys.argv[1], sys.argv[2]
# The SHIPPED graph with TLS spliced in front — same nodes, same params. A
# hand-written copy of a 20-node chain would drift from the thing that ships,
# and then this would be testing the copy.
g = open(f"{root}/packaging/debian/fluxor-apiplane.yaml").read()
g = g.replace("modules:\n  - name: http", f"""modules:
  - name: tls
    mode: 1
    verify_peer: 1
    # Mandatory whenever this instance authenticates a peer: ca_dns + the
    # cluster CA = "the client cert must be one we issued".
    peer_auth: 2
    trust: "${{file:{d}/ca.der}}"
    cert_file: "{d}/server.der"
    key_file: "{d}/server.key.der"
  - name: http""")
g = g.replace("    port: 7444", "    port: 7445")
g = g.replace("""  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in""", """  - from: linux_net.net_out
    to: tls.cipher_in
  - from: tls.cipher_out
    to: linux_net.net_in
  - from: tls.clear_out
    to: http.net_in
  - from: http.net_out
    to: tls.clear_in
  # The whole point of the wave change: the verified peer reaches the
  # application instead of stopping at the TLS boundary.
  - from: tls.peer_identity
    to: http.peer_identity""")
open(f"{d}/graph.yaml", "w").write(g)
PYGEN


echo "== 4. build config + module table =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 5. start the graph =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

# mtls <cert-name> <method> <path> [body] -> "<status>|<body>"
mtls() {
  python3 - "$PORT" "$D/$1.pem" "$D/$1.key.pem" "$2" "$3" "${4:-}" <<'PY'
import socket, ssl, sys
port, cert, key, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
ctx.load_cert_chain(certfile=cert, keyfile=key)
req = f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
if body: req += f"Content-Length: {len(body)}\r\n"
req += "\r\n" + body
raw = socket.create_connection(("127.0.0.1", port), timeout=8)
s = ctx.wrap_socket(raw, server_hostname="localhost")
s.sendall(req.encode())
buf = b""; s.settimeout(8)
try:
    while True:
        c = s.recv(4096)
        if not c: break
        buf += c
except (socket.timeout, ssl.SSLError):
    pass
s.close()
head, _, rbody = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n",1)[0].split(b" ",2)[1].decode() if head else "?"
print(f"{status}|{rbody.decode(errors='replace')}")
PY
}

echo "== 6. alice (bound SVID) is authorized; mallory (unbound) is denied =="
CM=/api/v1/namespaces/default/configmaps
out="$(mtls alice POST "$CM" '{"metadata":{"name":"cm1"},"key":"v1"}')"
st="${out%%|*}"; [ "$st" = "201" ] || fail "alice POST: got '$st' (${out#*|})"
echo "   alice  POST configmaps -> $st (authorized as her mTLS SVID)"

out="$(mtls alice GET "$CM/cm1" '')"
st="${out%%|*}"; [ "$st" = "200" ] || fail "alice GET: got '$st' (${out#*|})"
echo "   alice  GET  configmaps/cm1 -> $st"

out="$(mtls mallory GET "$CM/cm1" '')"
st="${out%%|*}"; [ "$st" = "403" ] || fail "mallory GET: got '$st' want 403 (${out#*|})"
echo "   mallory GET configmaps/cm1 -> $st (issued, but unbound SVID denied)"

echo "== 7. eve (self-signed, not issued by the cluster CA) is refused at the handshake =="
# The chain gate fires before any HTTP is spoken. Under TLS 1.3 the client
# finishes its own handshake optimistically, so the refusal shows up as the
# server's alert and NO response at all — `?`, never a status code. Any 2xx/4xx
# here would mean eve reached the apiserver and the gate did NOT run.
out="$(mtls eve GET "$CM/cm1" '' 2>&1 || true)"
st="${out%%|*}"
[ "$st" = "?" ] || fail "eve reached the apiserver and got '$st' — the CA chain gate did not fire"
echo "   eve    GET configmaps/cm1 -> no response (refused at the handshake: leaf-is-ca, no path to the cluster CA)"

echo "== E2E green: mTLS end to end — tls -> http -> trailer -> params, per-SVID authz =="
