#!/usr/bin/env bash
# Live E2E: real Kubernetes clients against the Chronicle API plane — the S1.4
# checkpoint. kubectl is client-go: `get -w` is a list-then-watch from the
# list's resourceVersion, and `wait` / `delete --wait` run an INFORMER (a
# reflector: list, watch with a field selector, resume on its own), so these
# pass only if list revisions, watch events and selectors are upstream-shaped.
#
#   two `kubectl get pods -w` sessions side by side see the same ADDED and
#   DELETED; `kubectl wait --for=jsonpath` sees a label arrive; `kubectl
#   delete` (which waits for the deletion by informer) and `kubectl wait
#   --for=delete` both complete.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7446

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kagi_verify store_effect store_source token_verify decision pipeline http http_chain tls; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-kubectl-XXXXXX)"
# The store is lattice's (the graph's `kv_state_worker_store`): the runtime
# runs in $D, where the store lives (lattice/) and the seeded host log
# (store.log) is imported from.
nc_lattice_prepare "$D"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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

echo "== 1. seed identity, an RBAC role that grants watch, and one pod =="
python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
ISSUER, KID = b"https://kubernetes.default.svc", b"sa-1"
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1)
        + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex","w").write(
    binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"
def mint(sa):
    now = int(time.time())
    claims = {"iss": ISSUER.decode(), "sub": f"system:serviceaccount:default:{sa}",
              "aud": "nanocloud", "iat": now, "exp": now + 3600}
    signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    return (signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode()
open(f"{D}/token.admin","w").write(mint("admin"))
open(f"{D}/token.lister","w").write(mint("lister"))
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$D/store.log" admin "*" "*"
rbac_crb "$D/store.log" admins admin ServiceAccount admin default
rbac_crb "$D/store.log" listers listonly ServiceAccount lister default
# `list` but NOT `watch` — the two are distinct Kubernetes verbs.
rbac_clusterrole "$D/store.log" listonly "list,get" "pods"
put_text "/pods/default/web-0" '{"metadata":{"name":"web-0","namespace":"default","labels":{"app":"web"}}}'
echo "   seeded"

echo "== 2. build the SHIPPED graph =="
[ -f "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }
# client-go sends a bearer token only over TLS, so the SHIPPED graph gets a
# server-only TLS front (a throwaway self-signed key; kubectl skips its
# verification) — same nodes, same params behind it.
openssl ecparam -name prime256v1 -genkey -noout -out "$D/server.key.pem" 2>/dev/null
openssl req -x509 -new -key "$D/server.key.pem" -subj "/CN=nanocloud" -days 1 \
  -out "$D/server.pem" 2>/dev/null
openssl x509 -in "$D/server.pem" -outform DER -out "$D/server.der" 2>/dev/null
openssl ec -in "$D/server.key.pem" -outform DER -out "$D/server.key.der" 2>/dev/null
python3 - "$GRAPH" "$D" <<'TLS'
import sys
g, d = open(sys.argv[1]).read(), sys.argv[2]
g = g.replace("modules:\n  - name: http", f"""modules:
  - name: tls
    mode: 1
    verify_peer: 0
    cert_file: "{d}/server.der"
    key_file: "{d}/server.key.der"
  - name: http""")
g = g.replace("    port: 7444", "    port: 7446")
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
    to: tls.clear_in""")
open(f"{d}/graph.yaml", "w").write(g)
TLS
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
echo "   settling the RBAC grant index"
rbac_settle "$FLUXOR_RUNTIME" "$D/config.bin" "$D/modules.bin" "$D" present \
  '/rbac-g/system:serviceaccount:default:admin/*|*|*|*|*' \
  '/rbac-g/system:serviceaccount:default:lister/*|list|*|pods|*'

echo "== 3. start ONE runtime and hold it for the whole test =="
env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

echo "== 4. kubeconfig for the admin service account =="
KC="$D/kubeconfig"
cat >"$KC" <<KCFG
apiVersion: v1
kind: Config
clusters:
- name: nc
  cluster:
    server: https://127.0.0.1:$PORT
    insecure-skip-tls-verify: true
users:
- name: admin
  user:
    token: $(cat "$D/token.admin")
contexts:
- name: nc
  context: {cluster: nc, user: admin, namespace: default}
current-context: nc
KCFG
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get pods -o name >"$D/list.out" 2>&1 || { cat "$D/list.out"; fail "kubectl get pods"; }
grep -qx "pod/web-0" "$D/list.out" || { cat "$D/list.out"; fail "kubectl get pods did not list web-0"; }
echo "   kubectl get pods: pod/web-0"

echo "== 5. two kubectl -w sessions, then create, label, delete =="
k get pods -w --output-watch-events -o json >"$D/w1.out" 2>"$D/w1.err" &
W1=$!
k get pods -w --output-watch-events -o json >"$D/w2.out" 2>"$D/w2.err" &
W2=$!
# Both sessions must have LISTED (their initial ADDED web-0) before anything
# changes: each does discovery, a TLS handshake and a list first, and under
# load that takes longer than any fixed sleep.
for _ in $(seq 300); do
  grep -q '"web-0"' "$D/w1.out" 2>/dev/null && grep -q '"web-0"' "$D/w2.out" 2>/dev/null && break
  sleep 0.1
done
grep -q '"web-0"' "$D/w2.out" 2>/dev/null || { cat "$D/w2.err"; fail "a kubectl -w session never listed"; }
cat >"$D/pod.json" <<'POD'
{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web-5","namespace":"default","labels":{"app":"web"}},"spec":{"containers":[{"name":"c","image":"nginx"}]}}
POD
# --validate=false: client-side validation needs an OpenAPI schema this API
# plane does not serve yet (S1.6); the watch path is what is under test.
k create --validate=false -f "$D/pod.json" >"$D/create.out" 2>&1 || { cat "$D/create.out"; fail "kubectl create"; }
k wait --for=jsonpath='{.metadata.labels.app}'=web pod/web-5 --timeout=20s >"$D/wait1.out" 2>&1 \
  || { cat "$D/wait1.out"; fail "kubectl wait --for=jsonpath (an informer) did not see web-5"; }
echo "   kubectl wait --for=jsonpath: $(cat "$D/wait1.out")"
k delete pod web-5 --timeout=20s >"$D/delete.out" 2>&1 || { cat "$D/delete.out"; fail "kubectl delete (waits by informer)"; }
echo "   kubectl delete: $(cat "$D/delete.out")"
k wait --for=delete pod/web-5 --timeout=10s >"$D/wait2.out" 2>&1 || { cat "$D/wait2.out"; fail "kubectl wait --for=delete"; }
sleep 2
kill "$W1" "$W2" 2>/dev/null || true
wait "$W1" "$W2" 2>/dev/null || true
for w in w1 w2; do
  python3 - "$D/$w.out" <<'EV' || { echo "--- $w"; cat "$D/$w.out" "$D/$w.err" | tail -40; fail "$w: events"; }
import json, sys
text, dec, i, evs = open(sys.argv[1]).read(), json.JSONDecoder(), 0, []
while True:
    while i < len(text) and text[i].isspace():
        i += 1
    if i >= len(text):
        break
    obj, i = dec.raw_decode(text, i)
    evs.append((obj["type"], obj["object"]["metadata"]["name"]))
want = [("ADDED", "web-0"), ("ADDED", "web-5"), ("DELETED", "web-5")]
if [e for e in evs if e in want] != want:
    print("events: %r" % evs); sys.exit(1)
EV
done
echo "   both sessions: ADDED pod/web-0, ADDED pod/web-5, DELETED pod/web-5"

echo "== E2E green: kubectl lists, watches (twice), waits and deletes by informer against the Chronicle API plane =="
