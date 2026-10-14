#!/usr/bin/env bash
# Join a node as its operator: trade the node's single-use join secret for a
# kubeconfig, trusting the API only once its authority matches the digest the
# node announced beside the secret.
#
#   rig-join.sh <secret> <ca-sha256> [host[:port]] [out]
#       (default host 192.168.1.9:6443, out target/pi5/kubeconfig)
#
# The node logs both when it comes up (node_pki):
#   [join] secret=<32 hex> ca-sha256=<64 hex>
#
#   1. read the chain the API presents and take its authority (the last
#      certificate); its SubjectPublicKeyInfo must hash to <ca-sha256> —
#      kubeadm's discovery-token-ca-cert-hash, so a machine in the middle
#      cannot hand us its own authority;
#   2. verify the API's leaf against that authority, for the API's name
#      (kubernetes.default.svc.cluster.local);
#   3. POST /apis/nanocloud.io/v1/join with the secret as the bearer
#      credential, over a connection verified by that authority: the node
#      answers its admin token and the authority, once;
#   4. write a kubeconfig that trusts only that authority, names the API by
#      that name (tls-server-name) and carries the token;
#   5. beside it, what joining a worker takes: the authority (ca.der) and,
#      for each worker slot the node declared (node_pki `join_nodes`), that
#      slot's bootstrap credential (node-<id>.token) — copied into a
#      worker's PKI directory as plane-ca.der and node.token
#      (packaging/debian/nanocloud-graph.sh, NANOCLOUD_PEER_TRUST=authority).
#
# Nothing here needs a key from the machine that built the image.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRET="${1:?usage: rig-join.sh <secret> <ca-sha256> [host[:port]] [out]}"
CA_SHA="${2:?usage: rig-join.sh <secret> <ca-sha256> [host[:port]] [out]}"
HOSTPORT="${3:-${NANOCLOUD_RIG_IP:-192.168.1.9}:6443}"
OUT="${4:-$ROOT/target/pi5/kubeconfig}"
case "$HOSTPORT" in *:*) ;; *) HOSTPORT="$HOSTPORT:6443" ;; esac
HOST="${HOSTPORT%:*}"
PORT="${HOSTPORT##*:}"
NAME="kubernetes.default.svc.cluster.local"
W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT
fail() { echo "rig-join: $*" >&2; exit 1; }

# ── 1. the presented authority, checked against the announced digest
openssl s_client -connect "$HOST:$PORT" -servername "$NAME" -showcerts </dev/null \
  >"$W/hello" 2>/dev/null || true
awk '/-----BEGIN CERTIFICATE-----/{n++; in_c=1} in_c{print > (dir "/c" n ".pem")} /-----END CERTIFICATE-----/{in_c=0}' \
  dir="$W" "$W/hello"
N="$(ls "$W"/c*.pem 2>/dev/null | wc -l)"
[ "$N" -ge 2 ] || fail "the API at $HOST:$PORT presented no chain (got $N certificate(s))"
cp "$W/c$N.pem" "$W/ca.pem"
cp "$W/c1.pem" "$W/leaf.pem"
GOT="$(openssl x509 -in "$W/ca.pem" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum | cut -d' ' -f1)"
[ "$GOT" = "$CA_SHA" ] || fail "the API's authority does not match the node's digest (presented $GOT, announced $CA_SHA) — not joining"

# ── 2. the leaf, for the API's name, under that authority
openssl verify -CAfile "$W/ca.pem" -verify_hostname "$NAME" "$W/leaf.pem" >/dev/null 2>&1 \
  || fail "the API's certificate does not verify for $NAME under its authority"

# ── 3. the secret, spent
ANS="$(curl -sS --max-time 20 --cacert "$W/ca.pem" --resolve "$NAME:$PORT:$HOST" \
  -X POST --data '' -H "Authorization: Bearer $SECRET" \
  -w '\n%{http_code}' "https://$NAME:$PORT/apis/nanocloud.io/v1/join")" || fail "the join request failed"
CODE="${ANS##*$'\n'}"
BODY="${ANS%$'\n'*}"
[ "$CODE" = "200" ] || fail "the node refused the secret ($CODE): $BODY"

# ── 4. the kubeconfig
mkdir -p "$(dirname "$OUT")"
python3 - "$BODY" "$W/ca.pem" "$HOST" "$PORT" "$NAME" "$OUT" <<'PY'
import base64, json, subprocess, sys
body, ca_pem, host, port, name, out = sys.argv[1:7]
ans = json.loads(body)
der = base64.urlsafe_b64decode(ans["ca"] + "=" * (-len(ans["ca"]) % 4))
presented = subprocess.run(["openssl", "x509", "-in", ca_pem, "-outform", "DER"],
                           capture_output=True, check=True).stdout
if der != presented:
    sys.exit("rig-join: the authority the node answered is not the one its API presented")
pem = open(ca_pem, "rb").read()
open(out, "w").write(f"""apiVersion: v1
kind: Config
clusters:
- name: nanocloud
  cluster:
    server: https://{host}:{port}
    certificate-authority-data: {base64.b64encode(pem).decode()}
    tls-server-name: {name}
users:
- name: admin
  user:
    token: {ans["token"]}
contexts:
- name: nanocloud
  context: {{cluster: nanocloud, user: admin, namespace: default}}
current-context: nanocloud
""")
# What a worker joins with: the authority, and each slot's credential —
# secrets, so readable by the operator alone.
import os
d = os.path.dirname(os.path.abspath(out))
old = os.umask(0o077)
open(os.path.join(d, "ca.der"), "wb").write(der)
for slot, tok in sorted((ans.get("nodes") or {}).items()):
    if not slot.isdigit():
        sys.exit(f"rig-join: the node named a worker slot '{slot}'")
    open(os.path.join(d, f"node-{slot}.token"), "w").write(tok)
os.umask(old)
print(out)
PY
