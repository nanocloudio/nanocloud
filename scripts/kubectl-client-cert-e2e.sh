#!/usr/bin/env bash
# Live E2E: kubectl with a client certificate from an authority outside the
# cell — the shape a managed cell takes when its customers' certificates are
# issued by a management plane (Acorn) and kubectl goes straight to the cell.
#
#   - the API's `tls` trusts a BUNDLE: the cluster's own authority and the
#     outside one, side by side (`trust`, up to MAX_ANCHORS);
#   - the identity a verified certificate yields is `peer:<hex>`, the SHA-256
#     of the leaf's raw subjectPublicKey (for P-256 the 65-byte point) — not
#     its subject or SANs, which the outside authority chooses;
#   - a customer's access is one RoleBinding to that subject, written through
#     the API by the operator: before it, 403; with it, 200; after it is
#     deleted, 403 again on the next requests — which is how the outside
#     authority revokes while no console stands in front of the cell;
#   - a certificate from neither authority never reaches the API.
#
# Every client here is real kubectl (client-go) reading the certificate and key
# from the files its kubeconfig names, the operator included.
#
# The listener ASKS for a certificate and does not require one
# (`verify_peer: 2`), as the shipped control plane renders it: a client that
# offers none reaches the API and is judged by its bearer token, here none. scripts/node-pki-e2e.sh runs the shipped renderer with a token-bearing
# operator and a certificate-bearing customer on its one listener.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7447

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
for m in kagi_verify store_effect store_source token_verify decision pipeline http http_chain tls; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-kubectl-cc-XXXXXX)"
nc_lattice_prepare "$D"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

# ca <name> <cn>: a self-signed P-256 authority.
ca() {
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$D/$1.key.pem" -out "$D/$1.pem" -days 1 -subj "/CN=$2" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
  openssl x509 -in "$D/$1.pem" -outform DER -out "$D/$1.der" 2>/dev/null
}
# issue <name> <cn> <eku> <ca> [san]: a P-256 leaf, its key born here (as a
# customer's is, with openssl), certified by <ca>.
issue() {
  local n="$1" cn="$2" eku="$3" by="$4" san="${5:-}"
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$D/$n.key.pem" -out "$D/$n.csr" -subj "/CN=$cn" 2>/dev/null
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=%s\n' "$eku" >"$D/$n.ext"
  [ -n "$san" ] && printf 'subjectAltName=%s\n' "$san" >>"$D/$n.ext"
  openssl x509 -req -in "$D/$n.csr" -CA "$D/$by.pem" -CAkey "$D/$by.key.pem" \
    -CAcreateserial -out "$D/$n.pem" -days 1 -extfile "$D/$n.ext" 2>/dev/null
}
# peer_of <name>: the subject the API derives — sha256 of the raw
# subjectPublicKey (the last 65 bytes of a P-256 SPKI).
peer_of() {
  echo "peer:$(openssl x509 -in "$D/$1.pem" -noout -pubkey \
    | openssl pkey -pubin -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1)"
}

echo "== 1. two authorities, the API's certificate, three clients =="
ca cluster nanocloud-cluster-ca
ca acorn acorn-customer-ca          # stands in for the management plane's
ca stranger someone-else
issue server kubernetes serverAuth cluster "DNS:kubernetes.default.svc.cluster.local"
openssl x509 -in "$D/server.pem" -outform DER -out "$D/server.der" 2>/dev/null
openssl ec -in "$D/server.key.pem" -outform DER -out "$D/server.key.der" 2>/dev/null
cat "$D/cluster.der" "$D/acorn.der" >"$D/client-ca.der"
issue operator operator clientAuth cluster
issue customer "account:ab12cd" clientAuth acorn   # the subject is the issuer's; nothing reads it
issue mallory mallory clientAuth stranger
OP="$(peer_of operator)"
CU="$(peer_of customer)"
echo "   operator $OP"
echo "   customer $CU"

echo "== 2. seed: the operator is admin; the customer is bound to nothing =="
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$D/store.log" admin "*" "*"
rbac_crb "$D/store.log" operator admin User "$OP"

echo "== 3. the SHIPPED API plane behind a tls that asks for a client certificate =="
python3 - "$GRAPH" "$D" "$PORT" <<'TLS'
import sys
g, d, port = open(sys.argv[1]).read(), sys.argv[2], sys.argv[3]
g = g.replace("modules:\n  - name: http", f"""modules:
  - name: tls
    mode: 1
    verify_peer: 2
    # ca_dns: the chain must reach an anchor in `trust`; a server checks no
    # name of a client.
    peer_auth: 2
    trust: "{d}/client-ca.der"
    cert_file: "{d}/server.der"
    key_file: "{d}/server.key.der"
  - name: http""")
g = g.replace("    port: 7444", f"    port: {port}")
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
  - from: tls.peer_identity
    to: http.peer_identity""")
open(f"{d}/graph.yaml", "w").write(g)
TLS
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
rbac_settle "$FLUXOR_RUNTIME" "$D/config.bin" "$D/modules.bin" "$D" present "/rbac-g/$OP/*|*|*|*|*"

env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 80); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited before binding :$PORT"
  sleep 0.1
done

# kubeconfig <who>: what the customer's snippet writes with `kubectl config`:
# the cell's authority, the API's name, and the certificate and key by path.
kubeconfig() {
  local kc="$D/$1.kubeconfig"
  KUBECONFIG="$kc" kubectl config set-cluster cell --server "https://127.0.0.1:$PORT" \
    --certificate-authority "$D/cluster.pem" --tls-server-name kubernetes.default.svc.cluster.local >/dev/null
  KUBECONFIG="$kc" kubectl config set-credentials "$1" \
    --client-certificate "$D/$1.pem" --client-key "$D/$1.key.pem" >/dev/null
  KUBECONFIG="$kc" kubectl config set-context cell --cluster cell --user "$1" --namespace default >/dev/null
  KUBECONFIG="$kc" kubectl config use-context cell >/dev/null
}
for who in operator customer mallory; do kubeconfig "$who"; done
k() { local who="$1"; shift; kubectl --kubeconfig "$D/$who.kubeconfig" --request-timeout=15s "$@"; }

echo "== 4. the operator, by certificate alone =="
k operator create configmap seed --from-literal=k=v >"$D/op.out" 2>&1 || { cat "$D/op.out"; fail "operator create configmap"; }
k operator get configmap seed -o name >"$D/op.out" 2>&1 || { cat "$D/op.out"; fail "operator get configmap"; }
echo "   $(cat "$D/op.out")"

echo "== 5. the customer's certificate is genuine, and grants nothing =="
if k customer get configmaps >"$D/cu.out" 2>&1; then cat "$D/cu.out"; fail "an unbound customer listed configmaps"; fi
grep -qi "forbidden" "$D/cu.out" || { cat "$D/cu.out"; fail "an unbound customer was not answered Forbidden"; }
echo "   unbound: $(head -1 "$D/cu.out")"

echo "== 6. a certificate from neither authority never reaches the API =="
if k mallory get configmaps >"$D/ma.out" 2>&1; then cat "$D/ma.out"; fail "a stranger's certificate was served"; fi
grep -qi "forbidden" "$D/ma.out" && fail "a stranger's certificate reached the API (answered Forbidden, not refused at the handshake)"
echo "   refused: $(head -1 "$D/ma.out")"

echo "== 6b. a client with no certificate is not refused at the handshake =="
CODE="$(curl -s -o "$D/anon.out" -w '%{http_code}' --max-time 10 --cacert "$D/cluster.pem" \
  --resolve "kubernetes.default.svc.cluster.local:$PORT:127.0.0.1" \
  "https://kubernetes.default.svc.cluster.local:$PORT/api/v1/namespaces/default/configmaps")" \
  || fail "a client with no certificate did not complete the handshake"
# Answered by the API's own authentication (this graph seeds no token key,
# so a bearer path answers 503 "no verification key"; a seeded one, 401).
grep -q '"kind":"Status"' "$D/anon.out" && case "$CODE" in 401|503) true ;; *) false ;; esac \
  || { cat "$D/anon.out"; fail "a client with no certificate and no token: got $CODE, want the API's 401 or 503"; }
echo "   no certificate, no token: $CODE from the API, $(grep -o '"message":"[^"]*"' "$D/anon.out")"

# until_ok <secs> <who> <args…>: the grant index follows a binding as the
# rbac chains run; how long that takes is reported, never assumed.
until_ok() {
  local secs="$1"; shift
  local t0; t0="$(date +%s%N)"
  for _ in $(seq $((secs * 4))); do
    if k "$@" >"$D/u.out" 2>&1; then
      echo $(( ($(date +%s%N) - t0) / 1000000 ))
      return 0
    fi
    sleep 0.25
  done
  return 1
}
until_forbidden() {
  local secs="$1"; shift
  local t0; t0="$(date +%s%N)"
  for _ in $(seq $((secs * 4))); do
    if ! k "$@" >"$D/u.out" 2>&1 && grep -qi forbidden "$D/u.out"; then
      echo $(( ($(date +%s%N) - t0) / 1000000 ))
      return 0
    fi
    sleep 0.25
  done
  return 1
}

echo "== 7. the operator binds the customer's key; the customer works =="
k operator create role cm-edit --verb=get,list,create,delete --resource=configmaps >"$D/op.out" 2>&1 \
  || { cat "$D/op.out"; fail "create role"; }
k operator create rolebinding customer --role=cm-edit --user="$CU" >"$D/op.out" 2>&1 \
  || { cat "$D/op.out"; fail "create rolebinding for $CU"; }
MS="$(until_ok 30 customer get configmaps -o name)" || { cat "$D/u.out"; fail "the bound customer was not served within 30 s"; }
echo "   bound: served after ${MS} ms"
k customer create configmap mine --from-literal=a=b >"$D/cu.out" 2>&1 || { cat "$D/cu.out"; fail "customer create configmap"; }
k customer get configmap mine -o jsonpath='{.data.a}' >"$D/cu.out" 2>&1 || { cat "$D/cu.out"; fail "customer get configmap"; }
[ "$(cat "$D/cu.out")" = b ] || fail "customer read back '$(cat "$D/cu.out")'"
if k customer get secrets >"$D/cu.out" 2>&1; then fail "the binding granted secrets"; fi
echo "   the customer creates and reads configmaps, and nothing else"

echo "== 8. deleting the binding revokes the key =="
k operator delete rolebinding customer >"$D/op.out" 2>&1 || { cat "$D/op.out"; fail "delete rolebinding"; }
MS="$(until_forbidden 30 customer get configmaps)" || { cat "$D/u.out"; fail "the customer was still served 30 s after the binding went"; }
echo "   revoked: Forbidden after ${MS} ms"
for _ in 1 2 3; do
  if k customer get configmaps >"$D/cu.out" 2>&1; then fail "a revoked key was served again"; fi
done
echo "   and on every request after"

echo "== E2E green: kubectl with a client certificate from a second authority, bound and revoked by its peer: subject =="
