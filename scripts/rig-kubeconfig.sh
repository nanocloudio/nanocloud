#!/usr/bin/env bash
# A kubeconfig for the bare-metal Pi 5 control plane (packaging/rig/
# pi5-controlplane.yaml): its API at https://<ip>:6443, the API certificate
# the render embedded as the trust anchor, and a bearer token for
# system:serviceaccount:kube-system:admin signed with the dev ServiceAccount
# key whose public half the render seeded at /authn-keys/sa.
#
#   rig-kubeconfig.sh [out] [ttl-hours]     (default: target/pi5/kubeconfig, 24)
#
# M1's stand-in for the join-token bootstrap (M3): it needs the render's keys
# (target/pi5/pki), so it works only on the machine that built the image.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/target/pi5/kubeconfig}"
TTL_H="${2:-24}"
IP="${NANOCLOUD_RIG_IP:-192.168.1.9}"
PKI="$ROOT/target/pi5/pki"
[ -f "$PKI/sa.key.pem" ] && [ -f "$PKI/api.pem" ] \
  || { echo "rig-kubeconfig: no keys in $PKI — render the image first" >&2; exit 2; }

python3 - "$PKI" "$OUT" "$TTL_H" "$IP" <<'PY'
import base64, json, sys, time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, utils
pki, out, ttl_h, ip = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
key = serialization.load_pem_private_key(open(f"{pki}/sa.key.pem", "rb").read(), None)
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
hdr = b64u(json.dumps({"alg": "ES256", "typ": "JWT", "kid": "sa-1"},
                      separators=(",", ":")).encode())
now = int(time.time())
claims = {"iss": "https://kubernetes.default.svc",
          "sub": "system:serviceaccount:kube-system:admin",
          "aud": "nanocloud", "iat": now, "exp": now + ttl_h * 3600}
signing = hdr + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
token = (signing + b"." + b64u(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode()
ca = base64.b64encode(open(f"{pki}/api.pem", "rb").read()).decode()
open(out, "w").write(f"""apiVersion: v1
kind: Config
clusters:
- name: nanocloud-rig
  cluster:
    server: https://{ip}:6443
    certificate-authority-data: {ca}
users:
- name: admin
  user:
    token: {token}
contexts:
- name: nanocloud-rig
  context: {{cluster: nanocloud-rig, user: admin, namespace: default}}
current-context: nanocloud-rig
""")
print(out)
PY
