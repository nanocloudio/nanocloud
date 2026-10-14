#!/usr/bin/env bash
# Live E2E: a node is its own PKI, as the bare-metal render makes it
# (scripts/rig-render-controlplane.sh), on Linux.
#
# The control plane runs with NO key material from its build: the API's TLS
# key is born in the node's vault (tls `identity_label`), node_pki has kagi's
# authority certify it on the node, and the operator's first credential — a
# kube-system/admin token from the node's own ServiceAccount mint — is
# released once by the join secret node_pki announces with its authority's
# key digest. Nothing is seeded: the namespaces and who the joined admin is
# (cluster-admin, bound to kube-system/admin) are node_pki's to declare.
# It is all the packaged render (nanocloud-graph.sh) under
# NANOCLOUD_PEER_TRUST=authority — nothing is spliced in here.
#
#   1. the node announces `[join] secret=… ca-sha256=…`;
#   2. scripts/rig-join.sh checks the presented authority against the digest,
#      spends the secret, writes a kubeconfig;
#   3. stock kubectl, verifying TLS against that authority, reads the API;
#   4. the secret answers once;
#   5. a restart keeps the authority (the vault persists its keys here), so
#      the kubeconfig still works, and a boot's unspent secret dies with it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-node-pki-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
stop() { if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; fi; }
cleanup() { stop; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; grep -a "node_pki\|\[join\]\|\[tls\] identity\|certificate_endpoint" "$D/run.log" 2>/dev/null | tail -20; tail -15 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <text>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
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

echo "== 1. no seeds but a stale verification key =="

# A key some earlier image left at /authn-keys/sa (a board's disk outlives
# its images; a seeded dev key, or a volatile mint key of the last boot). The
# verifier must follow the mint's key when it replaces it, not keep this one.
STALE="$(python3 - <<'PYK'
import struct, binascii
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
pub = ec.generate_private_key(ec.SECP256R1()).public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(b"https://kubernetes.default.svc") + struct.pack("<H", 1) + f8(b"sa-1")
        + struct.pack("<H", 1) + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
print(binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
PYK
)"
python3 - "$STORE/store.log" "/authn-keys/sa" "$STALE" <<'PY'
import struct, sys, os, binascii
path, key, val = sys.argv[1], sys.argv[2].encode(), binascii.unhexlify(sys.argv[3])
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.seek(p)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
PY

echo "== 2. render the control plane as the board's: vault identities, node_pki, no keys at all =="
NANOCLOUD_PEER_TRUST=authority nc_controlplane_graph "$STORE" "$D/graph.yaml" \
  || fail "the control plane did not render"
python3 - "$D/graph.yaml" <<'PY' || fail "the render carries key material"
import sys
g = "\n".join(l for l in open(sys.argv[1]).read().splitlines() if not l.lstrip().startswith("#"))
if 'identity_label: "nanocloud.api"' not in g or "join: 1" not in g:
    sys.exit("the API's identity is not the vault's, or nothing mints the join")
for leftover in ("cert_file", "key_file", "trust.pem", "_key:"):
    if leftover in g:
        sys.exit(f"the graph still renders key material ({leftover})")
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"

# This host's device key, as the deb's unit supplies it: the vault seals the
# authority's and the API's keys under it and keeps them across restarts.
# (A host without one — like a board with no OTP key — mints a new authority
# every boot, and its operator joins again.)
SEAL="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
mkdir -p "$D/vault"
boot() { # boot [volatile]: without the device key, as a board with no OTP key
  : >"$D/run.log"
  local keys=(FLUXOR_SEAL_KEY="$SEAL" FLUXOR_VAULT_DIR="$D/vault")
  [ "${1:-}" = volatile ] && keys=()
  env -C "$STORE" "${keys[@]}" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  JOIN=""
  for _ in $(seq 240); do
    JOIN="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/run.log" | tail -1 || true)"
    [ -n "$JOIN" ] && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the runtime exited"
    sleep 0.25
  done
  [ -n "$JOIN" ] || fail "no join secret announced in 60 s"
  SECRET="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
  CA_SHA="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
}

echo "== 3. boot: the node certifies its API and announces a join secret =="
boot
for _ in $(seq 40); do
  [ "$(grep -ac "\[node_pki\] certificate issued" "$D/run.log")" -ge 3 ] && break
  sleep 0.25
done
[ "$(grep -ac "\[node_pki\] certificate issued" "$D/run.log")" -ge 3 ] \
  || fail "not every identity was certified (the API's and both peer link ends)"
echo "   secret=${SECRET:0:6}… ca-sha256=${CA_SHA:0:16}…"
FIRST_CA="$CA_SHA"

echo "== 4. join: authority checked against the digest, the secret spent, a kubeconfig =="
KC="$D/kubeconfig"
bash "$ROOT/scripts/rig-join.sh" 0000000000000000000000000000000000000000000000000000000000000000 \
  "$CA_SHA" "127.0.0.1:$PORT" "$KC" 2>/dev/null && fail "joined with a wrong secret"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$(printf '%064d' 0)" "127.0.0.1:$PORT" "$KC" 2>"$D/join.err" \
  && fail "joined although the authority did not match the digest"
grep -q "does not match the node's digest" "$D/join.err" || fail "wrong-digest refusal: $(cat "$D/join.err")"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$KC" >/dev/null || fail "the join did not complete"
echo "   joined"

echo "== 5. stock kubectl, verifying the API against the node's authority =="
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get --raw /version >/dev/null || fail "kubectl could not reach the API"
NS="$(k get namespaces -o jsonpath='{.items[*].metadata.name}')" || fail "kubectl get namespaces"
case " $NS " in *" kube-system "*) ;; *) fail "namespaces: $NS";; esac
echo "   kubectl get namespaces: $NS"
SUB="$(python3 - "$KC" <<'PY'
import base64, json, re, sys
tok = re.search(r"token: (\S+)", open(sys.argv[1]).read()).group(1)
p = tok.split(".")[1]
c = json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
print(c["sub"], c["exp"] - c["iat"])
PY
)"
[ "$SUB" = "system:serviceaccount:kube-system:admin 2592000" ] || fail "the joined credential is '$SUB'"
echo "   the credential: $SUB s"

echo "== 6. the secret answers once =="
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$D/kc2" 2>"$D/join.err" \
  && fail "the secret joined twice"
grep -q "refused the secret (401)" "$D/join.err" || fail "replay: $(cat "$D/join.err")"
echo "   replay -> 401"

echo "== 7. restart: the same authority, the old kubeconfig still works, a fresh secret =="
stop
boot
[ "$CA_SHA" = "$FIRST_CA" ] || fail "the authority changed across a restart ($FIRST_CA -> $CA_SHA)"
UNSPENT="$SECRET"
for _ in $(seq 40); do k get --raw /version >/dev/null 2>&1 && break; sleep 0.5; done
k get namespaces >/dev/null || fail "the kubeconfig stopped working across a restart"
echo "   same authority; the kubeconfig works"

echo "== 8. a secret left unspent dies with its boot =="
stop
boot
[ "$SECRET" != "$UNSPENT" ] || fail "the restart announced the same secret"
bash "$ROOT/scripts/rig-join.sh" "$UNSPENT" "$CA_SHA" "127.0.0.1:$PORT" "$D/kc3" 2>/dev/null \
  && fail "the previous boot's secret still joined"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$D/kc3" >/dev/null \
  || fail "this boot's secret did not join"
echo "   the last boot's secret is refused; this boot's joins"

echo "== 9. a boot that cannot keep keys: a new authority, and the operator joins again =="
stop
boot volatile
[ "$CA_SHA" != "$FIRST_CA" ] || fail "a host without a device key kept its authority"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$KC" >/dev/null || fail "the re-join did not complete"
# The store still holds the last boot's verification key; the verifier must
# follow the mint's new one.
ok=""
for _ in $(seq 60); do k get namespaces >/dev/null 2>&1 && { ok=1; break; }; sleep 1; done
[ -n "$ok" ] || fail "the re-joined credential is rejected: the verifier kept the last boot's key"
echo "   new authority ${CA_SHA:0:16}…; re-joined; kubectl works"

echo "== 10. the CLI applet, as this node's replica under the same authority =="
# Rendered from the packaged template as the node's own plane: no
# authority of its own in the template, so it carries one — the same vault
# key, in the same vault, certifying the same principals.
stop
NANOCLOUD_PEER_TRUST=authority nc_cli_graph "$STORE" "$D/cli.yaml" || fail "the CLI did not render under authority"
grep -q "^  - name: certificate_endpoint$" "$D/cli.yaml" || fail "the CLI carries no authority"
! grep -q "^  - name: node_pki$" "$D/cli.yaml" || fail "the CLI mints a join"
nc_build_workload "$ROOT" "$D/cli.yaml" "$D/cli-config.bin" "$D/cli-modules.bin" >/dev/null || fail "the CLI did not build"
rc=0
env -C "$STORE" FLUXOR_SEAL_KEY="$SEAL" FLUXOR_VAULT_DIR="$D/vault" RUST_LOG=warn timeout 60 "$FLUXOR_RUNTIME" \
  --config "$D/cli-config.bin" --modules "$D/cli-modules.bin" -- get namespaces >"$D/cli.out" 2>"$D/cli.err" || rc=$?
[ "$rc" = 0 ] || { tail -5 "$D/cli.err"; fail "nanocloud get namespaces exited $rc"; }
grep -qx "kube-system" "$D/cli.out" || fail "the CLI read no namespaces: $(cat "$D/cli.out")"
echo "   nanocloud get namespaces: $(tr '\n' ' ' <"$D/cli.out")"

echo "== E2E green: the node minted its own PKI, and its operator joined with one secret =="
