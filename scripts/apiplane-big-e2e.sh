#!/usr/bin/env bash
# Live E2E: objects past one record through the Chronicle API plane (S1.5).
#
# A 1 MiB ConfigMap is 250 Chronicle records' worth. The watch connector
# (store_watch: store_source with a heap) reads it whole, stamps its
# resourceVersion, and streams the wire bytes across records; a change-feed
# batch holding it outgrows the connector's buffer and is fetched again on the
# heap. Proven: GET returns the object whole, LIST streams it among its
# siblings (and pages past it), and a watch from no revision delivers it as
# ADDED. A 1 MiB create and replace go BY REFERENCE (the HTTP server stages
# the body) and answer with the object stored; a watch sees both whole; a body
# past 3 MiB is 413, a ConfigMap past 1 MiB of data 422; an expired staged
# body is collected.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7448

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kagi_verify store_effect store_source token_verify decision pipeline http http_chain; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apibig-XXXXXX)"
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
# A ConfigMap of just over 1 MiB, and a small one beside it.
python3 - "$D/store.log" <<'BIG'
import json, struct, sys
path = sys.argv[1]
data = open(path, "rb").read(); p = last = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    last, p = rev, p + 15 + kl + vl
big = {"apiVersion": "v1", "kind": "ConfigMap",
       "metadata": {"name": "big", "namespace": "default"},
       "data": {"k%05d" % i: "v" * 40 for i in range(22000)}}
small = {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "small", "namespace": "default"}, "data": {"a": "b"}}
with open(path, "ab") as f:
    for i, (k, v) in enumerate([("/configmaps/default/big", big), ("/configmaps/default/small", small)]):
        kb, vb = k.encode(), json.dumps(v, separators=(",", ":")).encode()
        f.write(struct.pack("<QBHI", last + 1 + i, 1, len(kb), len(vb)) + kb + vb)
print("   big ConfigMap: %d bytes" % len(json.dumps(big, separators=(",", ":"))))
# A staged body long past its deadline: the collector must take both keys.
with open(path, "ab") as f:
    for i, (k, v) in enumerate([("/blobs/sha256/" + "ab" * 32, b"stale"),
                                ("/blobs/at/" + "ab" * 32, b'{"deadline":1}')]):
        kb = k.encode()
        f.write(struct.pack("<QBHI", last + 3 + i, 1, len(kb), len(v)) + kb + v)
BIG
echo "   seeded"

echo "== 2. build the SHIPPED graph =="
[ -f "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }
python3 - "$GRAPH" "$D/graph.yaml" <<'PORTS'
import sys
g = open(sys.argv[1]).read()
open(sys.argv[2], "w").write(g.replace("    port: 7444", "    port: 7448"))
PORTS
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

echo "== 4. GET, LIST and WATCH a 1 MiB object =="
python3 - "$PORT" "$(cat "$D/token.admin")" <<'PY' || fail "large objects"
import json, socket, sys, time
port, tok = int(sys.argv[1]), sys.argv[2]
NS = "/api/v1/namespaces/default/configmaps"

def fail(msg):
    print("FAIL: " + msg); sys.exit(1)

def get(path, timeout=30):
    c = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    c.sendall(("GET %s HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n"
               "Connection: close\r\n\r\n" % (path, tok)).encode())
    data = b""
    while True:
        try:
            chunk = c.recv(1 << 20)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    c.close()
    head, _, rest = data.partition(b"\r\n\r\n")
    return int(head.split(b"\r\n")[0].split(b" ")[1]), rest

t0 = time.time()
code, body = get(NS + "/big")
if code != 200:
    fail("GET big: %d %r" % (code, body[:200]))
obj = json.loads(body)
if len(obj["data"]) != 22000 or obj["data"]["k21999"] != "v" * 40:
    fail("GET big: data does not round-trip (%d keys)" % len(obj["data"]))
if not obj["metadata"].get("resourceVersion"):
    fail("GET big: no resourceVersion")
print("   GET big: %d bytes, 22000 keys, resourceVersion %s (%.1fs)"
      % (len(body), obj["metadata"]["resourceVersion"], time.time() - t0))

code, body = get(NS)
lst = json.loads(body)
names = [i["metadata"]["name"] for i in lst["items"]]
if code != 200 or lst["kind"] != "ConfigMapList" or names != ["big", "small"]:
    fail("LIST: %d %r %r" % (code, lst.get("kind"), names))
if len(lst["items"][0]["data"]) != 22000:
    fail("LIST: the big item is not whole")
print("   LIST: ConfigMapList [big (whole), small]")
code, body = get(NS + "?limit=1")
p1 = json.loads(body)
code, body = get(NS + "?limit=1&continue=" + p1["metadata"]["continue"])
p2 = json.loads(body)
if [i["metadata"]["name"] for i in p1["items"]] != ["big"] or \
   [i["metadata"]["name"] for i in p2["items"]] != ["small"]:
    fail("LIST paging past a big item: %r %r" % (p1["items"][:0], p2["items"][:0]))
print("   LIST limit=1: [big] + continue -> [small]")

w = socket.create_connection(("127.0.0.1", port), timeout=30)
w.sendall(("GET %s?watch=true HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n\r\n"
           % (NS, tok)).encode())
buf, evs, end = b"", [], time.time() + 30
while len(evs) < 2 and time.time() < end:
    chunk = w.recv(1 << 20)
    if not chunk:
        break
    buf += chunk
    if b"\r\n\r\n" in buf and not evs and buf.startswith(b"HTTP"):
        buf = buf.partition(b"\r\n\r\n")[2]
    while b"\n" in buf:
        line, _, buf = buf.partition(b"\n")
        if line.strip():
            evs.append(json.loads(line))
got = sorted((e["type"], e["object"]["metadata"]["name"], len(e["object"].get("data", {}))) for e in evs)
if got != [("ADDED", "big", 22000), ("ADDED", "small", 1)]:
    fail("WATCH: %r" % got)
print("   WATCH (no revision): ADDED big (whole, 22000 keys), ADDED small")

# --- WRITE: a 1 MiB create goes by reference and answers with the object
def send(method, path, body):
    c = socket.create_connection(("127.0.0.1", port), timeout=60)
    c.sendall(("%s %s HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n"
               "Content-Type: application/json\r\nContent-Length: %d\r\n"
               "Connection: close\r\n\r\n" % (method, path, tok, len(body))).encode() + body)
    data = b""
    while True:
        try:
            chunk = c.recv(1 << 20)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    c.close()
    head, _, rest = data.partition(b"\r\n\r\n")
    return int(head.split(b"\r\n")[0].split(b" ")[1]), rest

w = socket.create_connection(("127.0.0.1", port), timeout=60)
rv0 = int(json.loads(get(NS)[1])["metadata"]["resourceVersion"])
w.sendall(("GET %s?watch=true&resourceVersion=%d HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n\r\n"
           % (NS, rv0, tok)).encode())
big2 = {"apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "big2", "namespace": "default"},
        "data": {"k%05d" % i: "w" * 40 for i in range(22000)}}
code, body = send("POST", NS, json.dumps(big2).encode())
if code != 201:
    fail("POST big2: %d %r" % (code, body[:300]))
made = json.loads(body)
if made["metadata"]["name"] != "big2" or len(made["data"]) != 22000:
    fail("POST big2 answered %r…" % body[:200])
print("   POST 1 MiB: 201, answered with the object it stored (%d bytes)" % len(body))
code, body = get(NS + "/big2")
if code != 200 or json.loads(body)["data"]["k00007"] != "w" * 40:
    fail("GET big2 after create: %d" % code)
big2["data"]["k00007"] = "changed"
code, body = send("PUT", NS + "/big2", json.dumps(big2).encode())
if code != 200 or json.loads(body)["data"]["k00007"] != "changed":
    fail("PUT big2: %d %r" % (code, body[:200]))
print("   PUT 1 MiB: 200, the object as replaced")
buf, evs, end = b"", [], time.time() + 30
while len(evs) < 2 and time.time() < end:
    chunk = w.recv(1 << 20)
    if not chunk:
        break
    buf += chunk
    if buf.startswith(b"HTTP") and b"\r\n\r\n" in buf:
        buf = buf.partition(b"\r\n\r\n")[2]
    while b"\n" in buf:
        line, _, buf = buf.partition(b"\n")
        if line.strip():
            evs.append(json.loads(line))
got = [(e["type"], e["object"]["metadata"]["name"], e["object"]["data"].get("k00007")) for e in evs]
if got != [("ADDED", "big2", "w" * 40), ("MODIFIED", "big2", "changed")]:
    fail("WATCH of large writes: %r" % got)
print("   WATCH: ADDED big2, MODIFIED big2 — each whole")

# Refused on its declared length, before a byte of it is read: the head alone
# is sent, and the answer is 413.
c = socket.create_connection(("127.0.0.1", port), timeout=30)
c.sendall(("POST %s HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n"
           "Content-Type: application/json\r\nContent-Length: %d\r\n\r\n"
           % (NS, tok, 3 * 1024 * 1024 + 16)).encode())
head = c.recv(4096)
c.close()
if not head.startswith(b"HTTP/1.1 413"):
    fail("a 3 MiB+ body: %r want 413" % head[:60])
print("   POST past 3 MiB: 413")

# Upstream's bound on a ConfigMap is its DATA: keys and values past 1 MiB are
# 422, however the JSON spells them (big2 above, 1.01 MB of data, passed).
over = {"apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "over", "namespace": "default"},
        "data": {"k%05d" % i: "w" * 40 for i in range(23000)}}
code, body = send("POST", NS, json.dumps(over).encode())
if code != 422 or json.loads(body).get("reason") != "Invalid":
    fail("a ConfigMap past 1 MiB of data: %d %r want 422" % (code, body[:200]))
code, _ = get(NS + "/over")
if code != 404:
    fail("the refused ConfigMap was stored: GET %d" % code)
print("   POST 1.06 MB of data: 422 Invalid, nothing stored")
PY

echo "== 5. staged bodies are collected at their deadline =="
STALE="$(nc_store_keys "$D" /blobs/ | grep -xF -e "/blobs/sha256/$(printf 'ab%.0s' $(seq 32))" -e "/blobs/at/$(printf 'ab%.0s' $(seq 32))" || true)"
[ -z "$STALE" ] || fail "the expired staged body was not collected: still live: $STALE"
echo "   /blobs/sha256/abab… and /blobs/at/abab… deleted"

echo "== E2E green: a 1 MiB object is created, replaced, served and watched whole; staged bodies are collected =="
