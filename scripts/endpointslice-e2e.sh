#!/usr/bin/env bash
# Live E2E: EndpointSlices (discovery.k8s.io/v1) and Endpoints (v1) as the
# SHIPPED control plane serves them — rendered by the endpoints controller
# (endpoints.plan rows; endpoints_doc.plan and store_effect's EP_DOCS,
# _shared/epdoc.rs) from Services and Pods, and kept current:
#   discovery   /apis lists discovery.k8s.io; its v1 lists endpointslices
#   LIST        every namespace and one, a labelSelector on
#               kubernetes.io/service-name, kubectl get endpointslices
#   the slice   addresses, conditions (ready/serving/terminating), nodeName,
#               zone (the node's label), targetRef, the Service's port with
#               its NAMED targetPort resolved against the Pods' container
#               ports, the service-name and managed-by labels, an
#               ownerReference to the Service, addressType IPv4
#   Endpoints   JSON, read with jq: addresses and notReadyAddresses, ports
#   watch       a readiness flip is a MODIFIED slice (and Endpoints); a
#               Service deleted is a DELETED slice, and 404s after
#   a user's    Endpoints of a selectorless Service is never touched, not
#               even when the Service goes
#   slices      as upstream's controller cuts them: Pods whose named port
#               resolves differently in a slice of their own; a dual-stack
#               Service's IPv6 slice from the Pods' podIPs; a Service of 101
#               Pods in slices of at most 100 (named <svc>, <svc>-<digest>-<n>),
#               a shard no longer needed deleted; Endpoints of all 101
#   Table       kubectl's columns (NAME ADDRESSTYPE PORTS ENDPOINTS AGE);
#               /openapi/v3 has discovery.k8s.io/v1
#   namespace   a user's EndpointSlice goes with its namespace
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
command -v jq >/dev/null || { echo "FAIL: jq not found"; exit 1; }

D="$(mktemp -d /tmp/nc-endpointslice-XXXXXX)"
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

echo "== 1. seed: the operator's key, an admin, a Node in zone-a, Services and their Pods =="
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
for who in ("admin",):
    claims = {"iss": ISSUER.decode(), "sub": f"system:serviceaccount:default:{who}",
              "aud": "nanocloud", "iat": now, "exp": now + 3600}
    signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
    r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
    open(f"{D}/token.{who}", "w").write(
        (signing + b"." + b64u(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode())
PY
python3 - "$STORE/store.log" "$(cat "$D/keyframe.hex")" <<'PY'
import binascii, struct, sys, os
path, val = sys.argv[1], binascii.unhexlify(sys.argv[2])
with open(path, "ab") as f:
    f.write(struct.pack("<QBHI", 1, 1, len(b"/authn-keys/sa"), len(val)) + b"/authn-keys/sa" + val)
PY
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
python3 - "$STORE/store.log" <<'PY'
import json, struct, sys, os
path = sys.argv[1]
def pod(name, app, ip, ready, uid, port=8080, ip6=None):
    st = {"phase": "Running", "podIP": ip,
          "conditions": [{"type": "Ready", "status": "True" if ready else "False"}]}
    if ip6:
        st["podIPs"] = [{"ip": ip}, {"ip": ip6}]
    return f"/pods/default/{name}", {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": "default", "uid": uid, "labels": {"app": app}},
        "spec": {"nodeName": "n1", "containers": [{"name": "c", "image": "busybox",
                 "ports": [{"name": "http", "containerPort": port}, {"name": "metrics", "containerPort": 9100}]}]},
        "status": st}
def svc(name, app, ip, ports, **spec):
    return f"/services/default/{name}", {"apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": "default", "uid": f"u-svc-{name}"},
        "spec": dict({"selector": {"app": app}, "clusterIP": ip, "ports": ports}, **spec)}
seed = [
    ("/namespaces/default", {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}),
    ("/nodes/n1", {"apiVersion": "v1", "kind": "Node",
                   "metadata": {"name": "n1", "labels": {"kubernetes.io/hostname": "n1", "topology.kubernetes.io/zone": "zone-a"}},
                   "status": {"conditions": [{"type": "Ready", "status": "True"}], "allocatable": {"cpu": "4", "memory": "8Gi"}}}),
    ("/services/default/web", {"apiVersion": "v1", "kind": "Service",
                               "metadata": {"name": "web", "namespace": "default", "uid": "u-svc-web", "labels": {"tier": "front"}},
                               "spec": {"selector": {"app": "web"}, "clusterIP": "10.96.0.40",
                                        "ports": [{"name": "http", "port": 80, "targetPort": "http", "protocol": "TCP"}]}}),
    ("/services/default/api", {"apiVersion": "v1", "kind": "Service",
                               "metadata": {"name": "api", "namespace": "default", "uid": "u-svc-api"},
                               "spec": {"selector": {"app": "api"}, "clusterIP": "10.96.0.41",
                                        "ports": [{"port": 8000, "targetPort": 9000}]}}),
    ("/services/default/manual", {"apiVersion": "v1", "kind": "Service",
                                  "metadata": {"name": "manual", "namespace": "default", "uid": "u-svc-manual"},
                                  "spec": {"clusterIP": "10.96.0.42", "ports": [{"port": 5432}]}}),
    pod("web-1", "web", "10.244.0.11", True, "u-web-1"),
    pod("web-2", "web", "10.244.0.12", False, "u-web-2"),
    pod("api-1", "api", "10.244.0.21", True, "u-api-1"),
    # Two Pods whose `http` is a different number: two port sets.
    svc("multi", "multi", "10.96.0.43", [{"name": "http", "port": 80, "targetPort": "http"}]),
    pod("multi-1", "multi", "10.244.0.41", True, "u-multi-1", port=8080),
    pod("multi-2", "multi", "10.244.0.42", True, "u-multi-2", port=8081),
    # A dual-stack Service and a Pod with both families.
    svc("dual", "dual", "10.96.0.44", [{"name": "http", "port": 80, "targetPort": 8080}],
        ipFamilies=["IPv4", "IPv6"], ipFamilyPolicy="RequireDualStack", clusterIPs=["10.96.0.44", "fd00:96::44"]),
    pod("dual-1", "dual", "10.244.0.51", True, "u-dual-1", ip6="fd00:10:244::51"),
    # 101 Pods: two slices.
    svc("big", "big", "10.96.0.45", [{"name": "http", "port": 80, "targetPort": 8080}]),
] + [pod(f"big-{i:03d}", "big", f"10.244.{10 + i // 200}.{i % 200 + 1}", True, f"u-big-{i}") for i in range(101)]
for key, obj in seed:
    key, val = key.encode(), json.dumps(obj, separators=(",", ":")).encode()
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    with os.fdopen(fd, "r+b") as f:
        data = f.read(); p = last = 0
        while p + 15 <= len(data):
            rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
            if p + 15 + kl + vl > len(data): break
            last, p = rev, p + 15 + kl + vl
        f.truncate(p); f.seek(0, 2)
        f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
PY
echo "   Node n1 (zone-a); Services web (a named targetPort), api, manual (no selector), multi, dual, big; Pods web-1 (Ready), web-2 (not Ready), api-1, multi-1/2, dual-1, big-000..100"

echo "== 2. render the SHIPPED control plane for this node, build it =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
grep -q 'name: ep_api$' "$D/graph.yaml" || fail "the rendered control plane has no EndpointSlice chain"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 3. run it; wait for the API over TLS =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"

echo "== 4. the API, end to end (discovery, LIST, the slice, Endpoints, watch) =="
python3 -u - "$PORT" "$D" "$(cat "$STORE/read.port")" <<'PY' || fail "EndpointSlice API"
import json, socket, ssl, subprocess, sys, time
port, D, rport = int(sys.argv[1]), sys.argv[2], sys.argv[3]
TOK = {w: open(f"{D}/token.{w}").read().strip() for w in ("admin",)}
CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
CTX.minimum_version = ssl.TLSVersion.TLSv1_3

def fail(msg):
    print("FAIL: " + msg)
    sys.exit(1)

def ok(msg):
    print("   " + msg)

def conn():
    return CTX.wrap_socket(socket.create_connection(("127.0.0.1", port), timeout=10), server_hostname="localhost")

def req(method, path, body=None, who="admin", ctype="application/json", accept=None):
    b = (body if isinstance(body, str) else json.dumps(body, separators=(",", ":"))).encode() if body is not None else b""
    acc = f"Accept: {accept}\r\n" if accept else ""
    for _ in range(3):
        try:
            s = conn()
            s.sendall((f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOK[who]}\r\n{acc}"
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

def until(what, f, within=90):
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

class Watch:
    def __init__(self, path, who="admin"):
        self.s = conn()
        self.s.sendall((f"GET {path}?watch=true HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOK[who]}\r\n\r\n").encode())
        head = b""
        while b"\r\n\r\n" not in head:
            c = self.s.recv(4096)
            if not c:
                fail(f"watch {path} closed before its headers")
            head += c
        h, _, self.buf = head.partition(b"\r\n\r\n")
        self.status = int(h.split(b"\r\n")[0].split(b" ")[1])

    def events(self, want, within):
        got, end = [], time.time() + within
        while True:
            while b"\n" in self.buf:
                line, _, self.buf = self.buf.partition(b"\n")
                if line.strip():
                    got.append(json.loads(line))
            if want(got):
                return got
            left = end - time.time()
            if left <= 0:
                return got
            self.s.settimeout(left)
            try:
                c = self.s.recv(65536)
            except (socket.timeout, ssl.SSLError):
                return got
            if not c:
                return got
            self.buf += c

ES = "/apis/discovery.k8s.io/v1"
def items(path):
    c, b = req("GET", path)
    return (c, b.get("items", []) if isinstance(b, dict) else [], b)
def slice_named(name):
    c, b = req("GET", f"{ES}/namespaces/default/endpointslices/{name}")
    return b if c == 200 else None
def eps(sl):
    return [(e["addresses"][0], e["conditions"]["ready"], e["conditions"]["serving"], e["conditions"]["terminating"])
            for e in (sl or {}).get("endpoints") or []]
def jq(doc, f):
    return subprocess.run(["jq", "-c", f], input=json.dumps(doc), capture_output=True, text=True).stdout.strip()

until("the API answers the admin", lambda: req("GET", f"{ES}/endpointslices")[0] == 200 or req("GET", f"{ES}/endpointslices"))

# ---- discovery ----
c, b = req("GET", "/apis")
groups = {g["name"]: g for g in (b or {}).get("groups", [])} if c == 200 else {}
("discovery.k8s.io" in groups and groups["discovery.k8s.io"]["versions"][0]["groupVersion"] == "discovery.k8s.io/v1") \
    or fail(f"/apis groups: {list(groups)}")
c, b = req("GET", ES)
r = [x for x in (b or {}).get("resources", []) if x["name"] == "endpointslices"]
(c == 200 and b.get("groupVersion") == "discovery.k8s.io/v1" and r and r[0]["namespaced"] is True
 and r[0]["kind"] == "EndpointSlice" and {"get", "list", "watch"} <= set(r[0]["verbs"])) or fail(f"{ES}: {c} {b}")
ok("discovery: /apis lists discovery.k8s.io; its v1 lists endpointslices (namespaced) get/list/watch")

# ---- LIST ----
def web_two():
    sl = slice_named("web")
    e = eps(sl)
    return True if e == [("10.244.0.11", True, True, False), ("10.244.0.12", False, False, False)] else (sl or "no web slice")
until("web's EndpointSlice (web-1 ready, web-2 not)", web_two)
c, its, b = items(f"{ES}/endpointslices")
names = sorted(i["metadata"]["namespace"] + "/" + i["metadata"]["name"] for i in its)
(c == 200 and b.get("kind") == "EndpointSliceList" and {"default/web", "default/api"} <= set(names)) \
    or fail(f"LIST endpointslices: {c} {names} {str(b)[:300]}")
c, its2, _ = items(f"{ES}/namespaces/default/endpointslices")
(c == 200 and {"web", "api"} <= {i["metadata"]["name"] for i in its2}) or fail(f"LIST default/endpointslices: {c}")
ok(f"LIST endpointslices -> EndpointSliceList {names}; the namespaced LIST too")
c, its, _ = items(f"{ES}/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dweb")
(c == 200 and [i["metadata"]["name"] for i in its] == ["web"]) or fail(f"labelSelector service-name=web: {c} {[i['metadata']['name'] for i in its]}")
c, its, _ = items(f"{ES}/namespaces/default/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dapi")
(c == 200 and [i["metadata"]["name"] for i in its] == ["api"]) or fail(f"labelSelector service-name=api: {c}")
ok("labelSelector=kubernetes.io/service-name=web -> [web]; =api -> [api]")

# ---- the slice ----
s = slice_named("web")
m = s["metadata"]
(s["apiVersion"] == "discovery.k8s.io/v1" and s["kind"] == "EndpointSlice" and s["addressType"] == "IPv4") or fail(f"slice: {s}")
(m["name"] == "web" and m["namespace"] == "default" and m.get("uid") and m.get("creationTimestamp") and m.get("resourceVersion")) \
    or fail(f"slice metadata: {m}")
(m["labels"].get("kubernetes.io/service-name") == "web"
 and m["labels"].get("endpointslice.kubernetes.io/managed-by") == "endpointslice-controller.k8s.io"
 and m["labels"].get("tier") == "front") or fail(f"slice labels: {m['labels']}")
(m.get("ownerReferences") == [{"apiVersion": "v1", "kind": "Service", "name": "web", "uid": "u-svc-web",
                               "controller": True, "blockOwnerDeletion": True}]) or fail(f"slice ownerReferences: {m.get('ownerReferences')}")
e1 = s["endpoints"][0]
(e1.get("nodeName") == "n1" and e1.get("zone") == "zone-a"
 and e1.get("targetRef") == {"kind": "Pod", "namespace": "default", "name": "web-1", "uid": "u-web-1"}) or fail(f"web-1 endpoint: {e1}")
(s["ports"] == [{"name": "http", "protocol": "TCP", "port": 8080}]) or fail(f"slice ports (targetPort `http` resolved): {s['ports']}")
a = slice_named("api")
(eps(a) == [("10.244.0.21", True, True, False)] and a["ports"] == [{"name": "", "protocol": "TCP", "port": 9000}]) or fail(f"api slice: {a}")
ok("web: 10.244.0.11 ready (nodeName n1, zone zone-a, targetRef Pod web-1), 10.244.0.12 not ready; port http -> 8080 (named, from the Pods);"
   " labels service-name + managed-by (+ the Service's own); ownerReference to Service web; uid, creationTimestamp, resourceVersion")

# ---- slices as upstream cuts them ----
def of_svc(name):
    c, its, _ = items(f"{ES}/namespaces/default/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3D{name}")
    return its if c == 200 else []
import re
HEX = re.compile(r"^[0-9a-f]{8}$")
def multi_two():
    its = of_svc("multi")
    got = sorted((e["addresses"][0], i["ports"][0]["port"]) for i in its for e in i.get("endpoints") or [])
    names = sorted(i["metadata"]["name"] for i in its)
    if (len(its) == 2 and got == [("10.244.0.41", 8080), ("10.244.0.42", 8081)] and names[0] == "multi"
            and names[1].startswith("multi-") and HEX.match(names[1][6:])):
        return True
    return [(i["metadata"]["name"], i.get("ports"), eps(i)) for i in its]
until("multi's two port sets in two slices", multi_two)
for i in of_svc("multi"):
    (i["metadata"]["ownerReferences"][0]["name"] == "multi" and i["metadata"]["labels"]["kubernetes.io/service-name"] == "multi"
     and len(i["endpoints"]) == 1) or fail(f"multi slice {i}")
ok("multi: http -> 8080 on multi-1 and 8081 on multi-2 -> two slices, multi and multi-<digest>, each one endpoint, one port set")
def dual_two():
    its = {i["addressType"]: i for i in of_svc("dual")}
    if (set(its) == {"IPv4", "IPv6"} and its["IPv4"]["metadata"]["name"] == "dual"
            and eps(its["IPv4"]) == [("10.244.0.51", True, True, False)]
            and eps(its["IPv6"]) == [("fd00:10:244::51", True, True, False)]
            and its["IPv6"]["metadata"]["name"].startswith("dual-")
            and its["IPv6"]["ports"] == [{"name": "http", "protocol": "TCP", "port": 8080}]
            and its["IPv6"]["endpoints"][0]["targetRef"]["name"] == "dual-1"):
        return True
    return [(i["metadata"]["name"], i["addressType"], eps(i)) for i in its.values()]
until("dual's IPv4 and IPv6 slices", dual_two)
ok("dual (ipFamilies IPv4,IPv6): slice dual addressType IPv4 [10.244.0.51]; an IPv6 slice [fd00:10:244::51] from the Pod's podIPs")
def big_shards(n):
    its = of_svc("big")
    sizes = sorted(len(i.get("endpoints") or []) for i in its)
    want = sorted([100] * (n // 100) + ([n % 100] if n % 100 else []))
    addrs = {e["addresses"][0] for i in its for e in i.get("endpoints") or []}
    if sizes == want and len(addrs) == n:
        return its
    return None
until("big's 101 endpoints in slices of 100 and 1", lambda: big_shards(101) is not None or sorted(len(i.get("endpoints") or []) for i in of_svc("big")), within=600)
its = big_shards(101)
names = sorted(i["metadata"]["name"] for i in its)
h = names[1][len("big-"):len("big-") + 8]
(names == ["big", f"big-{h}-1"] and HEX.match(h)) or fail(f"big's slice names: {names}")
c, ep = req("GET", "/api/v1/namespaces/default/endpoints/big")
(c == 200 and len(ep["subsets"][0]["addresses"]) == 101) or fail(f"endpoints/big: {c} {str(ep)[:300]}")
ok(f"big: 101 endpoints -> {names} (100, 1); endpoints/big lists all 101")
for i in (100,):
    c, b = req("DELETE", f"/api/v1/namespaces/default/pods/big-{i:03d}")
    c == 200 or fail(f"DELETE pod big-{i:03d}: {c} {str(b)[:200]}")
until(f"big-{h}-1 deleted once its Pod went", lambda: req("GET", f"{ES}/namespaces/default/endpointslices/big-{h}-1")[0] == 404 or "still there")
until("big in one full slice", lambda: big_shards(100) is not None or "not yet")
ok(f"big-100 deleted -> big-{h}-1 deleted, big holds 100")

# ---- Table and OpenAPI ----
c, t = req("GET", f"{ES}/namespaces/default/endpointslices/dual", accept="application/json;as=Table;v=v1;g=meta.k8s.io")
c == 200 or fail(f"Table GET: {c} {t}")
cols = [d["name"] for d in t.get("columnDefinitions", [])]
cols == ["Name", "AddressType", "Ports", "Endpoints", "Age"] or fail(f"Table columns: {cols}")
cells = t["rows"][0]["cells"]
cells[:4] == ["dual", "IPv4", "8080", "10.244.0.51"] or fail(f"Table cells: {cells}")
c, t = req("GET", f"{ES}/namespaces/default/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dbig", accept="application/json;as=Table;v=v1;g=meta.k8s.io")
more = [r["cells"][3] for r in t.get("rows", [])]
(c == 200 and more and more[0].endswith(" + 97 more...") and len(more) == 1) or fail(f"Table ENDPOINTS for big: {c} {more}")
ok(f"Table: columns {cols}; dual {cells[:4]}; big's ENDPOINTS '{more[0]}'")
c, b = req("GET", "/openapi/v3")
(c == 200 and b["paths"].get("apis/discovery.k8s.io/v1", {}).get("serverRelativeURL") == "/openapi/v3/apis/discovery.k8s.io/v1") or fail(f"/openapi/v3: {c} {str(b)[:300]}")
c, b = req("GET", "/openapi/v3/apis/discovery.k8s.io/v1")
gvk = [p["patch"]["x-kubernetes-group-version-kind"] for p in (b or {}).get("paths", {}).values()] if c == 200 else []
gvk == [{"group": "discovery.k8s.io", "version": "v1", "kind": "EndpointSlice"}] or fail(f"/openapi/v3/apis/discovery.k8s.io/v1: {c} {str(b)[:300]}")
ok("/openapi/v3 lists apis/discovery.k8s.io/v1; its document names EndpointSlice")

# ---- a namespace's EndpointSlices go with it ----
c, b = req("POST", "/api/v1/namespaces", {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "scratch"}})
c in (200, 201) or fail(f"POST namespace scratch: {c} {str(b)[:200]}")
USL = {"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "metadata": {"name": "mine", "namespace": "scratch"},
       "addressType": "IPv4", "endpoints": [{"addresses": ["192.0.2.7"]}], "ports": [{"port": 80}]}
c, b = req("POST", f"{ES}/namespaces/scratch/endpointslices", USL)
c in (200, 201) or fail(f"POST a user's EndpointSlice: {c} {str(b)[:200]}")
until("the user's EndpointSlice readable", lambda: req("GET", f"{ES}/namespaces/scratch/endpointslices/mine")[0] == 200 or "not yet")
# The API's DELETE of a namespace marks it Terminating (store_doc), as
# upstream's does; the teardown chain (namespace.plan, ns_*) then sweeps what
# is in it and finalizes it.
c, b = req("DELETE", "/api/v1/namespaces/scratch")
c == 200 or fail(f"DELETE namespace scratch: {c} {str(b)[:200]}")
# (Answered as a delete is, a Status; what proves the mark is the sweep
# below: a namespace record deleted outright left its objects behind.)
def gone(path):
    c, b = req("GET", path)
    return True if c == 404 else f"GET {c} {str(b)[:200]}"
until("the user's EndpointSlice gone with its namespace", lambda: gone(f"{ES}/namespaces/scratch/endpointslices/mine"))
until("namespace scratch gone", lambda: gone("/api/v1/namespaces/scratch"))
ok("DELETE namespace scratch -> Terminating -> its EndpointSlice mine swept, then the namespace finalized")
c, b = req("DELETE", "/api/v1/namespaces/default")
c == 403 or fail(f"DELETE namespace default: {c} {str(b)[:200]} (upstream refuses it)")
c, b = req("GET", "/api/v1/namespaces/default")
(c == 200 and "Terminating" not in str(b)) or fail(f"namespace default after a refused delete: {c} {str(b)[:200]}")
ok("DELETE namespace default -> 403, and default stays Active")

# ---- Endpoints, through jq ----
c, ep = req("GET", "/api/v1/namespaces/default/endpoints/web")
c == 200 or fail(f"GET endpoints/web: {c} {ep}")
(jq(ep, "[.apiVersion,.kind,.metadata.name]") == '["v1","Endpoints","web"]') or fail(f"endpoints/web: {ep}")
(jq(ep, "[.subsets[0].addresses[].ip]") == '["10.244.0.11"]' and jq(ep, "[.subsets[0].notReadyAddresses[].ip]") == '["10.244.0.12"]'
 and jq(ep, ".subsets[0].ports") == '[{"name":"http","port":8080,"protocol":"TCP"}]'
 and jq(ep, ".subsets[0].addresses[0].targetRef.name") == '"web-1"') or fail(f"endpoints/web subsets: {ep}")
(jq(ep, '.metadata.labels["endpoints.kubernetes.io/managed-by"]') == '"endpoint-controller"') or fail(f"endpoints/web labels: {ep}")
ok("GET endpoints/web (jq): addresses [10.244.0.11], notReadyAddresses [10.244.0.12], ports [http 8080/TCP], managed-by endpoint-controller")

# ---- watch: a readiness flip ----
w = Watch(f"{ES}/namespaces/default/endpointslices")
w.status == 200 or fail(f"watch endpointslices: {w.status}")
we = Watch("/api/v1/namespaces/default/endpoints")
we.status == 200 or fail(f"watch endpoints: {we.status}")
c, b = req("PATCH", "/api/v1/namespaces/default/pods/web-2/status",
           {"status": {"conditions": [{"type": "Ready", "status": "True"}]}}, ctype="application/merge-patch+json")
c == 200 or fail(f"PATCH web-2 Ready: {c} {str(b)[:200]}")
flipped = lambda x: (x["type"] == "MODIFIED" and x["object"]["metadata"]["name"] == "web"
                     and eps(x["object"]) == [("10.244.0.11", True, True, False), ("10.244.0.12", True, True, False)])
evs = w.events(lambda e: any(flipped(x) for x in e), 60)
any(flipped(x) for x in evs) or fail(f"no MODIFIED web slice with web-2 ready on the watch: {[(x['type'], x['object']['metadata'].get('name')) for x in evs]}")
ok(f"web-2 Ready -> a MODIFIED EndpointSlice web on the watch (both ready); events seen: {[x['type'] for x in evs]}")
epflip = lambda x: (x["type"] == "MODIFIED" and x["object"]["metadata"]["name"] == "web"
                    and len((x["object"].get("subsets") or [{}])[0].get("addresses", [])) == 2)
evs = we.events(lambda e: any(epflip(x) for x in e), 30)
any(epflip(x) for x in evs) or fail(f"no MODIFIED Endpoints web with two addresses: {evs}")
ok("... and a MODIFIED Endpoints web with both addresses")

# ---- a user's Endpoints, a Service deleted ----
USER = {"apiVersion": "v1", "kind": "Endpoints", "metadata": {"name": "manual", "namespace": "default"},
        "subsets": [{"addresses": [{"ip": "192.0.2.10"}], "ports": [{"port": 5432, "protocol": "TCP"}]}]}
c, b = req("POST", "/api/v1/namespaces/default/endpoints", USER)
c in (200, 201) or fail(f"POST endpoints/manual: {c} {str(b)[:200]}")
for n in ("api", "manual"):
    c, b = req("DELETE", f"/api/v1/namespaces/default/services/{n}")
    c == 200 or fail(f"DELETE service {n}: {c} {str(b)[:200]}")
gone = lambda x: x["type"] == "DELETED" and x["object"]["metadata"]["name"] == "api"
evs = w.events(lambda e: any(gone(x) for x in e), 60)
any(gone(x) for x in evs) or fail(f"no DELETED api slice on the watch: {[(x['type'], x['object']['metadata'].get('name')) for x in evs]}")
until("GET endpointslices/api 404", lambda: req("GET", f"{ES}/namespaces/default/endpointslices/api")[0] == 404 or "still there")
until("GET endpoints/api 404", lambda: req("GET", "/api/v1/namespaces/default/endpoints/api")[0] == 404 or "still there")
time.sleep(3)
c, b = req("GET", "/api/v1/namespaces/default/endpoints/manual")
(c == 200 and jq(b, "[.subsets[0].addresses[].ip]") == '["192.0.2.10"]'
 and jq(b, '.metadata.labels["endpoints.kubernetes.io/managed-by"]') == "null") \
    or fail(f"the user's Endpoints manual after its Service went: {c} {b}")
ok("Service api deleted -> a DELETED EndpointSlice api on the watch, endpointslices/api and endpoints/api 404;"
   " the user's own Endpoints manual (a selectorless Service) unchanged, and kept after its Service went")
PY

echo "== 5. kubectl get endpointslices =="
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
if command -v kubectl >/dev/null; then
  k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
  k get endpointslices -o name >"$D/k.out" 2>&1 || { cat "$D/k.out"; fail "kubectl get endpointslices"; }
  grep -qx "endpointslice.discovery.k8s.io/web" "$D/k.out" || { cat "$D/k.out"; fail "kubectl get endpointslices did not list web"; }
  k get endpointslices -l kubernetes.io/service-name=web -o json >"$D/k.json" 2>"$D/k.err" || { cat "$D/k.err"; fail "kubectl get -l"; }
  [ "$(jq -c '[.items[] | .metadata.name]' "$D/k.json")" = '["web"]' ] || fail "kubectl -l service-name=web: $(jq -c '[.items[].metadata.name]' "$D/k.json")"
  [ "$(jq -c '[.items[0].endpoints[].addresses[0]]' "$D/k.json")" = '["10.244.0.11","10.244.0.12"]' ] || fail "kubectl -l web endpoints: $(head -c 400 "$D/k.json")"
  k get endpoints web -o json >"$D/ke.json" 2>"$D/ke.err" || { cat "$D/ke.err"; fail "kubectl get endpoints web"; }
  [ "$(jq -c '[.subsets[0].addresses[].ip]' "$D/ke.json")" = '["10.244.0.11","10.244.0.12"]' ] || fail "kubectl get endpoints web: $(head -c 400 "$D/ke.json")"
  k get endpointslices dual >"$D/kt.out" 2>&1 || { cat "$D/kt.out"; fail "kubectl get endpointslices dual"; }
  head -1 "$D/kt.out" | grep -Eq '^NAME +ADDRESSTYPE +PORTS +ENDPOINTS +AGE$' || { cat "$D/kt.out"; fail "kubectl's EndpointSlice columns"; }
  sed -n 2p "$D/kt.out" | grep -Eq '^dual +IPv4 +8080 +10\.244\.0\.51 ' || { cat "$D/kt.out"; fail "kubectl's dual row"; }
  echo "   kubectl get endpointslices: $(tr '\n' ' ' <"$D/k.out"); -l kubernetes.io/service-name=web -> [web]; kubectl get endpoints web: both addresses"
  echo "   kubectl get endpointslices dual: $(head -2 "$D/kt.out" | tr -s ' ' | tr '\n' '|')"
else
  echo "   (kubectl not installed: its step skipped)"
fi

echo "== E2E green: EndpointSlices and Endpoints derived from Services and Pods, listed, selected, watched, a user's left alone =="
