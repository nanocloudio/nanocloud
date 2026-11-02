#!/usr/bin/env bash
# Live E2E: LoadBalancer Services through the API of the SHIPPED control
# plane, its pool set (NANOCLOUD_LB_POOL) — the cell as its own load
# balancer, and the external load-balancer hook (`spec.loadBalancerClass`).
#
#   - a LoadBalancer Service of NO class is given an address of the pool
#     (/lb-addr/<ns>/<svc>) and its status.loadBalancer names it (svc_alloc);
#   - a LoadBalancer Service of class `example.com/external-lb` is given node
#     ports and nothing else: no address of the pool, no status from the cell;
#   - its controller — this test, an ordinary API client — writes its
#     status through the Service's `/status` subresource; the cell leaves it
#     as written, the classless Service's own status unchanged beside it;
#   - a status write through `/status` changes status only (a spec in it is
#     ignored), and a write of the Service itself keeps the status it carries.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-lb-class-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. seed: the operator's key, an admin, the default namespace =="
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
open(f"{D}/keyframe.hex", "w").write(
    binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"
now = int(time.time())
claims = {"iss": ISSUER.decode(), "sub": "system:serviceaccount:default:admin",
          "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/token.admin", "w").write(
    (signing + b"." + b64u(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode())
PY
python3 - "$STORE/store.log" "$(cat "$D/keyframe.hex")" <<'PY'
import binascii, struct, sys
path, val = sys.argv[1], binascii.unhexlify(sys.argv[2])
ns = b'{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
with open(path, "ab") as f:
    f.write(struct.pack("<QBHI", 1, 1, len(b"/authn-keys/sa"), len(val)) + b"/authn-keys/sa" + val)
    f.write(struct.pack("<QBHI", 2, 1, len(b"/namespaces/default"), len(ns)) + b"/namespaces/default" + ns)
PY
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default

echo "== 2. render the SHIPPED control plane with a load-balancer pool, build it =="
NANOCLOUD_LB_POOL=10.250.0.0/24 nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
grep -q '^    lb_pool: "10.250.0.0/24"$' "$D/graph.yaml" || fail "the render carries no lb_pool"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 3. run it; wait for the API over TLS =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"

echo "== 4. two LoadBalancer Services: one of no class, one of an external controller's class =="
python3 - "$PORT" "$D" "$(cat "$STORE/read.port")" <<'PY' || fail "LoadBalancer classes"
import json, socket, ssl, subprocess, sys, time
port, D, rport = int(sys.argv[1]), sys.argv[2], sys.argv[3]
TOK = open(f"{D}/token.admin").read().strip()
CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
CTX.minimum_version = ssl.TLSVersion.TLSv1_3

def fail(msg):
    print("FAIL: " + msg)
    sys.exit(1)

def ok(msg):
    print("   " + msg)

def req(method, path, body=None, ctype="application/json"):
    b = json.dumps(body, separators=(",", ":")).encode() if body is not None else b""
    for _ in range(3):
        try:
            s = CTX.wrap_socket(socket.create_connection(("127.0.0.1", port), timeout=10), server_hostname="localhost")
            s.sendall((f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOK}\r\n"
                       f"Content-Type: {ctype}\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b)
            data = b""
            s.settimeout(10)
            while True:
                try:
                    c = s.recv(65536)
                except (socket.timeout, ssl.SSLError):
                    break
                if not c:
                    break
                data += c
            s.close()
            head, _, rest = data.partition(b"\r\n\r\n")
            code = int(head.split(b"\r\n")[0].split(b" ")[1])
            try:
                return code, json.loads(rest) if rest.strip() else None
            except ValueError:
                return code, rest.decode(errors="replace")
        except (OSError, IndexError, ValueError):
            time.sleep(1)
    fail(f"{method} {path}: no answer")

def until(what, f, within=60):
    end = time.time() + within
    last = None
    while time.time() < end:
        last = f()
        if last is True:
            return
        time.sleep(1)
    fail(f"{what} (last: {str(last)[:400]})")

def store_get(key):
    return subprocess.run(["redis-cli", "-p", rport, "--raw", "GET", key], capture_output=True, text=True).stdout.strip()

SVCS = "/api/v1/namespaces/default/services"
def svc(name, cls=None):
    spec = {"type": "LoadBalancer", "ports": [{"port": 80, "targetPort": 8080}]}
    if cls:
        spec["loadBalancerClass"] = cls
    return {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "namespace": "default"}, "spec": spec}
def get(name):
    c, b = req("GET", f"{SVCS}/{name}")
    return b if c == 200 else None
def ingress(o):
    return ((o or {}).get("status") or {}).get("loadBalancer")
def nodeport(o):
    return (((o or {}).get("spec") or {}).get("ports") or [{}])[0].get("nodePort")

until("the API answers the admin", lambda: req("GET", SVCS)[0] == 200 or req("GET", SVCS))
for name, cls in (("own", None), ("ext", "example.com/external-lb")):
    c, b = req("POST", SVCS, svc(name, cls))
    c in (200, 201) or fail(f"create {name}: {c} {str(b)[:300]}")
ok("POST services own (no class) and ext (loadBalancerClass example.com/external-lb)")

# The cell is own's load balancer: an address of the pool, said in its status.
def own_ready():
    o = get("own")
    ip = ((ingress(o) or {}).get("ingress") or [{}])[0].get("ip")
    return True if (ip and nodeport(o) and store_get("/lb-addr/default/own") == f"ip={ip}") else o
until("own given a pool address and its status", own_ready)
own = get("own")
own_ip = ingress(own)["ingress"][0]["ip"]
own_ip.startswith("10.250.0.") or fail(f"own's address is not of the pool: {own_ip}")
ok(f"own: nodePort {nodeport(own)}, /lb-addr/default/own = ip={own_ip}, status.loadBalancer.ingress[0].ip {own_ip}")

# ext: node ports, and nothing else from the cell.
until("ext given a node port", lambda: bool(nodeport(get("ext"))) or get("ext"))
time.sleep(4)
ext = get("ext")
ext_np = nodeport(ext)
30000 <= ext_np <= 32767 or fail(f"ext's nodePort: {ext_np}")
store_get("/lb-addr/default/ext") == "" or fail(f"ext was given a pool address: {store_get('/lb-addr/default/ext')}")
ingress(ext) in (None, {}) or fail(f"the cell wrote ext's status: {ingress(ext)}")
ok(f"ext: nodePort {ext_np}; no /lb-addr/default/ext, no status.loadBalancer — its class's controller serves it")

# Its controller writes the status through /status (a merge patch, as a
# controller's UpdateStatus/patch does); a spec in a status write is ignored.
st = {"loadBalancer": {"ingress": [{"ip": "203.0.113.7"}]}}
c, b = req("PATCH", f"{SVCS}/ext/status", {"status": st, "spec": {"loadBalancerClass": "example.com/other"}},
           ctype="application/merge-patch+json")
(c == 200 and ingress(b) == st["loadBalancer"]) or fail(f"PATCH ext/status: {c} {str(b)[:300]}")
c, b = req("GET", f"{SVCS}/ext/status")
(c == 200 and ingress(b) == st["loadBalancer"]) or fail(f"GET ext/status: {c} {str(b)[:300]}")
ok("PATCH services/ext/status -> 200: status.loadBalancer.ingress 203.0.113.7")
time.sleep(6)
ext = get("ext")
ingress(ext) == st["loadBalancer"] or fail(f"the cell touched ext's status: {ingress(ext)}")
ext["spec"].get("loadBalancerClass") == "example.com/external-lb" or fail(f"a status write changed ext's spec: {ext['spec']}")
nodeport(ext) == ext_np or fail(f"ext's nodePort moved: {nodeport(ext)}")
store_get("/lb-addr/default/ext") == "" or fail("ext was given a pool address after its status")
ingress(get("own")) == {"ingress": [{"ip": own_ip}]} or fail(f"own's status moved: {ingress(get('own'))}")
ok("ext's status kept as its controller wrote it (spec untouched by the status write, nodePort unchanged, no pool address); own's unchanged")

# A replace of the Service itself states its status: kept as stated.
ext["metadata"].setdefault("labels", {})["tier"] = "edge"
c, b = req("PUT", f"{SVCS}/ext", ext)
c == 200 or fail(f"PUT ext: {c} {str(b)[:300]}")
time.sleep(4)
ext = get("ext")
(ext["metadata"].get("labels", {}).get("tier") == "edge" and ingress(ext) == st["loadBalancer"]) \
    or fail(f"after a replace: {ext.get('metadata')} {ingress(ext)}")
ok("PUT services/ext (a label added, its status carried): the controller's status kept")

for name in ("own", "ext"):
    c, b = req("DELETE", f"{SVCS}/{name}")
    c == 200 or fail(f"DELETE {name}: {c}")
until("own's address released", lambda: store_get("/lb-addr/default/own") == "" or store_get("/lb-addr/default/own"))
ok("own and ext deleted: own's pool address released")
PY

echo "== E2E green: the cell is the load balancer of a Service of no class; a Service of any class is its controller's, which writes its status through the API =="
