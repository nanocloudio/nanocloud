#!/usr/bin/env bash
# Live E2E: ZoneSummary (nanocloud.io/v1, cluster-scoped, read-only),
# served by the SHIPPED control plane: each zone's Nodes, capacity and store
# voters, derived by zone_node.plan + zone_summary.plan from /nodes/ and
# /pods/ — two zones, a Node with no zone (not summarised, but a voter
# outside both), a NotReady Node, a cordoned one, Pods' requests (a terminal
# Pod's not counted), voters inside and outside — and kept current: a
# readiness flip, an uncordon, a Pod gone, a zone's last Node gone (its
# summary with it). Discovered and watched; every client write is 405.
# RBAC: a caller with no grant is 403; a caller granted zonesummaries reads
# them and nothing else (docs/architecture/zone-summary.md).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-zone-summary-XXXXXX)"
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

echo "== 1. seed: the operator's key, three callers (admin, nobody, a viewer), two zones of Nodes, their Pods =="
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
for who in ("admin", "nobody", "viewer"):
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
# A viewer may read ZoneSummaries — no other resource.
rbac_clusterrole_g "$STORE/store.log" viewer "nanocloud.io" "get,list,watch" "zonesummaries"
rbac_crb "$STORE/store.log" viewers viewer ServiceAccount viewer default
store_put "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
python3 - "$D/seed.json" <<'PY'
import json, sys
def node(name, zone=None, ready=True, cordoned=False, voter=False, cpu="4", mem="8Gi"):
    labels = {"kubernetes.io/hostname": name}
    if zone: labels["topology.kubernetes.io/zone"] = zone
    if voter: labels["nanocloud.io/store-voter"] = "true"
    o = {"apiVersion": "v1", "kind": "Node", "metadata": {"name": name, "labels": labels},
         "spec": {"unschedulable": True} if cordoned else {},
         "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                    "allocatable": {"cpu": cpu, "memory": mem}}}
    return f"/nodes/{name}", o
def pod(name, node, phase, cpu, mem=None):
    req = {"cpu": cpu}
    if mem: req["memory"] = mem
    o = {"apiVersion": "v1", "kind": "Pod",
         "metadata": {"name": name, "namespace": "default", "uid": f"uid-{name}"},
         "spec": {"containers": [{"name": "c", "image": "busybox", "resources": {"requests": req}}]},
         "status": {"phase": phase}}
    if node: o["spec"]["nodeName"] = node
    return f"/pods/default/{name}", o
seed = [
    node("za-1", "zone-a", voter=True, cpu="4", mem="8Gi"),
    node("za-2", "zone-a", cordoned=True, cpu="2", mem="4Gi"),
    node("za-3", "zone-a", ready=False, voter=True, cpu="8", mem="16Gi"),
    node("zb-1", "zone-b", voter=True, cpu="4", mem="8Gi"),
    node("zb-2", "zone-b", ready=False, voter=True, cpu="2", mem="2Gi"),
    node("nz-1", None, voter=True, cpu="16", mem="32Gi"),
    pod("p1", "za-1", "Running", "500m", "256Mi"),
    pod("p2", "za-2", "Running", "1", "1Gi"),
    pod("p3", "za-1", "Succeeded", "2", "2Gi"),
    pod("p4", "zb-1", "Running", "250m", "128Mi"),
    pod("p5", "nz-1", "Running", "100m", "64Mi"),
    pod("p6", None, "Pending", "3", "3Gi"),
]
json.dump([[k, json.dumps(o, separators=(",", ":"))] for k, o in seed], open(sys.argv[1], "w"))
PY
python3 - "$STORE/store.log" "$D/seed.json" <<'PY'
import json, struct, sys, os
path = sys.argv[1]
for key, val in json.load(open(sys.argv[2])):
    key, val = key.encode(), val.encode()
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
echo "   6 Nodes (zone-a: ready voter, cordoned, NotReady voter; zone-b: ready voter, NotReady voter; one with no zone, a voter), 6 Pods"

echo "== 2. render the SHIPPED control plane for this node, build it =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
grep -q 'name: zs_put' "$D/graph.yaml" || fail "the rendered control plane has no ZoneSummary chain"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 3. run it; wait for the API over TLS =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"

echo "== 4. the API, end to end (one client: discovery, ZoneSummary, RBAC) =="
python3 - "$PORT" "$D" "$(cat "$STORE/read.port")" <<'PY' || fail "ZoneSummary API"
import json, socket, ssl, subprocess, sys, time
port, D, rport = int(sys.argv[1]), sys.argv[2], sys.argv[3]
TOK = {w: open(f"{D}/token.{w}").read().strip() for w in ("admin", "nobody", "viewer")}
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

def req(method, path, body=None, who="admin", ctype="application/json"):
    b = (body if isinstance(body, str) else json.dumps(body, separators=(",", ":"))).encode() if body is not None else b""
    for _ in range(3):
        try:
            s = conn()
            s.sendall((f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOK[who]}\r\n"
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

ZS = "/apis/nanocloud.io/v1/zonesummaries"

# The API answers, and the admin's grant is in the index.
until("the API answers the admin", lambda: req("GET", ZS)[0] == 200 or req("GET", ZS))
ok("GET zonesummaries -> 200 (the admin's grant is live)")

# ---- discovery ----
c, b = req("GET", "/apis")
groups = [g["name"] for g in (b or {}).get("groups", [])] if c == 200 else []
"nanocloud.io" in groups or fail(f"/apis groups: {groups}")
c, b = req("GET", "/apis/nanocloud.io/v1")
z = [r for r in (b or {}).get("resources", []) if r["name"] == "zonesummaries"]
(z and z[0]["namespaced"] is False and z[0]["kind"] == "ZoneSummary" and sorted(z[0]["verbs"]) == ["get", "list", "watch"]) \
    or fail(f"nanocloud.io/v1 zonesummaries: {z}")
ok("discovery: /apis lists nanocloud.io; its v1 lists zonesummaries (cluster-scoped) get/list/watch")

# ---- ZoneSummary ----
GI = 1 << 30
WANT_A = {"nodes": {"total": 3, "ready": 2, "schedulable": 1},
          "cpuMilli": {"allocatableSchedulable": 4000, "allocatableReady": 6000, "requested": 1500},
          "memoryBytes": {"allocatableSchedulable": 8 * GI, "allocatableReady": 12 * GI, "requested": 256 * (1 << 20) + GI},
          "voters": {"inside": 2, "outside": 3, "outsideReady": 2}}
WANT_B = {"nodes": {"total": 2, "ready": 1, "schedulable": 1},
          "cpuMilli": {"allocatableSchedulable": 4000, "allocatableReady": 4000, "requested": 250},
          "memoryBytes": {"allocatableSchedulable": 8 * GI, "allocatableReady": 8 * GI, "requested": 128 * (1 << 20)},
          "voters": {"inside": 2, "outside": 3, "outsideReady": 2}}
def zs(zone):
    c, b = req("GET", f"{ZS}/{zone}")
    return b if c == 200 else None
def matches(zone, want):
    def f():
        b = zs(zone)
        if not b:
            return f"no {zone}"
        st = b.get("status", {})
        for k, v in want.items():
            if st.get(k) != v:
                return st
        return True
    return f
until("zone-a summarised", matches("zone-a", WANT_A))
until("zone-b summarised", matches("zone-b", WANT_B))
a = zs("zone-a")
(a["apiVersion"] == "nanocloud.io/v1" and a["kind"] == "ZoneSummary" and a["metadata"]["name"] == "zone-a") or fail(f"zone-a object: {a}")
rg_a = a["status"]["readinessGeneration"]
(isinstance(rg_a, int) and rg_a > 0 and a["status"]["observedAtMs"] > 1700000000000) or fail(f"zone-a generation/clock: {a['status']}")
ok(f"zone-a: {a['status']['nodes']} cpu {a['status']['cpuMilli']} voters {a['status']['voters']} (rg {rg_a})")
ok(f"zone-b: {zs('zone-b')['status']['nodes']} — a cordoned Node is ready not schedulable; a NotReady Node is in total only;"
   " a Succeeded Pod and a Pod on a Node with no zone are not counted; a voter with no zone is outside both")
stored = store_get("/zonesummaries.nanocloud.io/zone-a")
'"kind":"ZoneSummary"' in stored or fail(f"not stored at /zonesummaries.nanocloud.io/zone-a: {stored[:200]}")
c, b = req("GET", ZS)
names = sorted(i["metadata"]["name"] for i in (b or {}).get("items", []))
(c == 200 and names == ["zone-a", "zone-b"] and b.get("kind") == "ZoneSummaryList") or fail(f"LIST zonesummaries: {c} {names} {str(b)[:200]}")
ok("LIST zonesummaries -> ZoneSummaryList [zone-a, zone-b] (no summary for a Node without a zone); stored at /zonesummaries.nanocloud.io/<zone>")

# Every client write refused: the base's own derivation.
for m, p, body in (("POST", ZS, {"apiVersion": "nanocloud.io/v1", "kind": "ZoneSummary", "metadata": {"name": "zone-z"}}),
                   ("PUT", f"{ZS}/zone-a", a), ("PATCH", f"{ZS}/zone-a", {"status": {"nodes": {"total": 99}}}),
                   ("DELETE", f"{ZS}/zone-a", None), ("PUT", f"{ZS}/zone-a/status", a)):
    c, b = req(m, p, body, ctype="application/merge-patch+json" if m == "PATCH" else "application/json")
    (c == 405 and isinstance(b, dict) and b.get("reason") == "MethodNotAllowed") or fail(f"{m} {p}: {c} {str(b)[:200]}")
matches("zone-a", WANT_A)() is True or fail("a refused write changed zone-a")
ok("POST/PUT/PATCH/DELETE zonesummaries (and /status) -> 405 MethodNotAllowed; zone-a unchanged")

# Kept current: watched.
w = Watch(ZS)
w.status == 200 or fail(f"watch zonesummaries: {w.status}")
c, b = req("PATCH", "/api/v1/nodes/za-3/status", {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
           ctype="application/merge-patch+json")
c == 200 or fail(f"PATCH za-3 Ready: {c} {str(b)[:200]}")
want = {**WANT_A, "nodes": {"total": 3, "ready": 3, "schedulable": 2},
        "cpuMilli": {"allocatableSchedulable": 12000, "allocatableReady": 14000, "requested": 1500},
        "memoryBytes": {"allocatableSchedulable": 24 * GI, "allocatableReady": 28 * GI, "requested": 256 * (1 << 20) + GI}}
until("zone-a after za-3 turned Ready", matches("zone-a", want))
# The flip's revision follows the figures by one pass (the row, then the
# zone's mark): the generation moves past what the first read saw.
until("zone-a's readinessGeneration past " + str(rg_a),
      lambda: (zs("zone-a") or {}).get("status", {}).get("readinessGeneration", 0) > rg_a or zs("zone-a"))
a2 = zs("zone-a")
evs = w.events(lambda e: any(x["type"] == "MODIFIED" and x["object"]["metadata"]["name"] == "zone-a"
                             and x["object"]["status"]["nodes"]["ready"] == 3 for x in e), 30)
any(x["type"] == "MODIFIED" and x["object"]["metadata"]["name"] == "zone-a" for x in evs) or fail(f"no MODIFIED zone-a on the watch: {evs}")
ok(f"za-3 Ready -> zone-a ready 3/schedulable 2, readinessGeneration {rg_a} -> {a2['status']['readinessGeneration']}, a MODIFIED event")
c, b = req("PATCH", "/api/v1/nodes/za-2", {"spec": {"unschedulable": False}}, ctype="application/merge-patch+json")
c == 200 or fail(f"PATCH za-2 uncordon: {c}")
c, b = req("DELETE", "/api/v1/namespaces/default/pods/p2")
c == 200 or fail(f"DELETE p2: {c} {str(b)[:200]}")
want = {**want, "nodes": {"total": 3, "ready": 3, "schedulable": 3},
        "cpuMilli": {"allocatableSchedulable": 14000, "allocatableReady": 14000, "requested": 500},
        "memoryBytes": {"allocatableSchedulable": 28 * GI, "allocatableReady": 28 * GI, "requested": 256 * (1 << 20)}}
until("zone-a after the uncordon and p2 gone", matches("zone-a", want))
ok("za-2 uncordoned, p2 deleted -> zone-a schedulable 3, cpu requested 500")
for n in ("zb-1", "zb-2"):
    c, b = req("DELETE", f"/api/v1/nodes/{n}")
    c == 200 or fail(f"DELETE {n}: {c}")
until("zone-b gone with its last Node", lambda: req("GET", f"{ZS}/zone-b")[0] == 404 or req("GET", f"{ZS}/zone-b"))
want = {**want, "voters": {"inside": 2, "outside": 1, "outsideReady": 1}}
until("zone-a's outside voters after zone-b went", matches("zone-a", want))
ok("zone-b's last Node deleted -> GET zonesummaries/zone-b 404; zone-a's outside voters 3 -> 1")

# ---- RBAC ----
for m, p in (("GET", ZS), ("GET", f"{ZS}/zone-a")):
    c, b = req(m, p, who="nobody")
    c == 403 or fail(f"nobody {m} {p}: {c}")
until("the viewer's grant", lambda: req("GET", ZS, who="viewer")[0] == 200 or req("GET", ZS, who="viewer"))
c, b = req("GET", f"{ZS}/zone-a", who="viewer")
(c == 200 and b["metadata"]["name"] == "zone-a") or fail(f"viewer GET zone-a: {c} {str(b)[:200]}")
for m, p in (("GET", "/api/v1/nodes"), ("GET", "/api/v1/namespaces/default/pods"), ("GET", "/apis/nanocloud.io/v1/graphs")):
    c, b = req(m, p, who="viewer")
    c == 403 or fail(f"viewer {m} {p}: {c}")
ok("no grant -> 403 on zonesummaries; a zonesummaries grant reads them and nothing else (nodes, pods, graphs 403)")
PY

echo "== E2E green: ZoneSummary derived from Nodes and Pods, kept current, served read-only and RBAC-guarded =="
