#!/usr/bin/env bash
# Live E2E for Kubernetes WATCH and typed LIST served by the Chronicle graph.
#
# A watch is the one API surface that is not one-request-one-response. The
# request chain registers the connection (/watch-reg/<cid>) and holds the
# response open (the exchange's HOLD); the watch chain reads the store's CHANGE FEED per
# registration and streams events into the same reply stage:
#
#   request chain: … -> act  (registers the watcher; the response stays open)
#   watch chain:   store_source(events) -> ws_map (label selector)
#                  -> ws_emit (field selector, event, unregister) -> reply -> http
#
# Proven here, against ONE runtime holding many connections: a typed List with
# its resourceVersion; ADDED/MODIFIED/DELETED (with the object's last state)
# strictly after the list's revision, on two watchers at once; resume from a
# revision; the current objects as ADDED for a watch with none; 410 Expired
# past the store's history; timeoutSeconds; BOOKMARK; label and field
# selectors; sendInitialEvents refused; a quiet watch alive past 30 s; a LIST
# filtered by label and field selectors and paged by limit/continue; and
# `watch` as its own RBAC verb.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7444

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kagi_verify store_effect store_source token_verify decision pipeline http http_chain; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-apiwatch-XXXXXX)"
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
# Push the store's change history (65536 on Linux) past revision 1, so a watch
# from resourceVersion=1 is too old: 410 Expired.
python3 - "$D/store.log" <<'FLOOD'
import struct, sys
path = sys.argv[1]
data = open(path, "rb").read(); p = last = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    last, p = rev, p + 15 + kl + vl
key, val = b"/flood/x", b"1"
with open(path, "ab") as f:
    for i in range(1, 66001):
        f.write(struct.pack("<QBHI", last + i, 1, len(key), len(val)) + key + val)
FLOOD
echo "   seeded (and 66000 revisions of history pushed past)"

echo "== 2. build the SHIPPED graph =="
[ -f "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }
# One change from the shipped graph: bookmarks every second, not every minute.
python3 - "$GRAPH" "$D/graph.yaml" <<'BM'
import sys
g = open(sys.argv[1]).read()
anchor = "    events: 1\n"
assert g.count(anchor) == 1
open(sys.argv[2], "w").write(g.replace(anchor, anchor + "    bookmark_ms: 1000\n"))
BM
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

echo "== 4. RBAC: 'watch' is its own verb — a list-only role cannot watch =="
CODE="$(python3 - "$PORT" "$(cat "$D/token.lister")" <<'PY'
import socket, sys
port, tok = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port), timeout=6)
s.sendall(("GET /api/v1/namespaces/default/pods?watch=true HTTP/1.1\r\n"
           "Host: x\r\nAuthorization: Bearer %s\r\n\r\n" % tok).encode())
data = s.recv(4096); s.close()
print(data.split(b"\r\n")[0].split(b" ")[1].decode())
PY
)"
[ "$CODE" = "403" ] || fail "list-only role watching: got '$CODE' want 403"
echo "   403 — a role granting list does NOT grant watch"

echo "== 5. LIST and WATCH semantics =="
python3 - "$PORT" "$(cat "$D/token.admin")" <<'PY' || fail "watch semantics"
import json, socket, sys, time
port, tok = int(sys.argv[1]), sys.argv[2]
NS = "/api/v1/namespaces/default/pods"

def fail(msg):
    print("FAIL: " + msg); sys.exit(1)

def req(method, path, body=None):
    c = socket.create_connection(("127.0.0.1", port), timeout=10)
    b = json.dumps(body).encode() if body is not None else b""
    c.sendall(("%s %s HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n"
               "Content-Type: application/json\r\nContent-Length: %d\r\n"
               "Connection: close\r\n\r\n" % (method, path, tok, len(b))).encode() + b)
    data = b""
    while True:
        try:
            chunk = c.recv(65536)
        except socket.timeout:
            break
        if not chunk:
            break
        data += chunk
    c.close()
    head, _, rest = data.partition(b"\r\n\r\n")
    return int(head.split(b"\r\n")[0].split(b" ")[1]), rest

class Watch:
    def __init__(self, query):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.s.sendall(("GET %s?watch=true%s HTTP/1.1\r\nHost: x\r\n"
                        "Authorization: Bearer %s\r\n\r\n" % (NS, query, tok)).encode())
        head = b""
        while b"\r\n\r\n" not in head:
            b = self.s.recv(4096)
            if not b:
                fail("watch %r closed before its headers" % query)
            head += b
        h, _, self.buf = head.partition(b"\r\n\r\n")
        self.status = int(h.split(b"\r\n")[0].split(b" ")[1])
        self.closed = False

    def events(self, want, within):
        """Read events until `want(events)` holds or `within` seconds pass."""
        got, end = [], time.time() + within
        while True:
            while b"\n" in self.buf:
                line, _, self.buf = self.buf.partition(b"\n")
                if line.strip():
                    got.append(json.loads(line))
            if want(got) or self.closed:
                return got
            left = end - time.time()
            if left <= 0:
                return got
            self.s.settimeout(left)
            try:
                b = self.s.recv(65536)
            except socket.timeout:
                return got
            if not b:
                self.closed = True
            self.buf += b

def names(evs):
    return [(e["type"], e["object"].get("metadata", {}).get("name")) for e in evs]

def rv(e):
    return int(e["object"]["metadata"]["resourceVersion"])

# --- a typed List, with the revision it was read at
code, body = req("GET", NS)
if code != 200:
    fail("list: %d" % code)
lst = json.loads(body)
if lst.get("kind") != "PodList" or lst.get("apiVersion") != "v1":
    fail("list envelope: %r" % {k: lst.get(k) for k in ("kind", "apiVersion")})
L = int(lst["metadata"]["resourceVersion"])
if [i["metadata"]["name"] for i in lst["items"]] != ["web-0"]:
    fail("list items: %r" % lst["items"])
print("   LIST: PodList, resourceVersion %d, items [web-0]" % L)

# --- watchers opened before the changes
quiet = Watch("&resourceVersion=%d" % L)              # checked last: alive past 30 s
w1 = Watch("&resourceVersion=%d" % L)
w2 = Watch("&resourceVersion=%d" % L)
sel = Watch("&resourceVersion=%d&labelSelector=app%%3Dweb" % L)
sin = Watch("&resourceVersion=%d&labelSelector=app+in+%%28api%%2Cweb%%29" % L)
fld = Watch("&resourceVersion=%d&fieldSelector=metadata.name%%3Dweb-9" % L)
for w in (quiet, w1, w2, sel, sin, fld):
    if w.status != 200:
        fail("a watch opened with %d" % w.status)
t_quiet = time.time()

def pod(n, app, img):
    return {"metadata": {"name": n, "namespace": "default", "labels": {"app": app}},
            "spec": {"containers": [{"name": "c", "image": img}]}}

for m, p, b, want in [("POST", NS, pod("web-9", "web", "nginx"), 201),
                      ("POST", NS, pod("db-1", "db", "pg"), 201),
                      ("PUT", NS + "/web-9", pod("web-9", "web", "nginx:2"), 200),
                      ("DELETE", NS + "/web-9", None, 200)]:
    c, r = req(m, p, b)
    if c != want:
        fail("%s %s: %d want %d (%r)" % (m, p, c, want, r[:200]))

full = [("ADDED", "web-9"), ("ADDED", "db-1"), ("MODIFIED", "web-9"), ("DELETED", "web-9")]
for label, w in (("watcher 1", w1), ("watcher 2", w2)):
    evs = w.events(lambda e: len(e) >= 4, 10)
    if names(evs) != full:
        fail("%s: %r want %r" % (label, names(evs), full))
    revs = [rv(e) for e in evs]
    if not (L < revs[0] < revs[1] < revs[2] < revs[3]):
        fail("%s: revisions %r not strictly after %d and increasing" % (label, revs, L))
    last = evs[3]["object"]
    if last.get("spec", {}).get("containers", [{}])[0].get("image") != "nginx:2":
        fail("%s: DELETED does not carry the last state: %r" % (label, last))
R_added = revs[0]
print("   two watchers: ADDED web-9, ADDED db-1, MODIFIED web-9, DELETED web-9 (last state)")

# --- label and field selectors
for label, w in (("app=web", sel), ("app in (api,web)", sin), ("metadata.name=web-9", fld)):
    evs = w.events(lambda e: len(e) >= 3, 6)
    if names(evs) != [("ADDED", "web-9"), ("MODIFIED", "web-9"), ("DELETED", "web-9")]:
        fail("selector %s: %r" % (label, names(evs)))
print("   labelSelector app=web, app in (api,web), fieldSelector metadata.name=web-9: web-9 only")

# --- resume strictly after a revision: the create is behind it
w = Watch("&resourceVersion=%d" % R_added)
evs = w.events(lambda e: len(e) >= 3, 6)
if names(evs) != full[1:]:
    fail("resume after %d: %r" % (R_added, names(evs)))
print("   resume from web-9's ADDED revision: %r" % names(evs))

# --- no revision: the current objects as ADDED
w = Watch("")
evs = w.events(lambda e: len(e) >= 2, 6)
if sorted(names(evs)[:2]) != [("ADDED", "db-1"), ("ADDED", "web-0")]:
    fail("watch with no revision: %r" % names(evs))
print("   no resourceVersion: ADDED db-1, ADDED web-0 (the current state)")

# --- too old: 410 Expired, and the stream ends
w = Watch("&resourceVersion=1")
evs = w.events(lambda e: len(e) >= 1, 6)
if not evs or evs[0]["type"] != "ERROR" or evs[0]["object"].get("code") != 410:
    fail("resourceVersion=1: %r" % evs[:1])
w.events(lambda e: False, 3)
if not w.closed:
    fail("the stream did not end after 410")
print("   resourceVersion=1: ERROR 410 Expired, stream ended")

# --- timeoutSeconds ends the watch
t0 = time.time()
w = Watch("&resourceVersion=%d&timeoutSeconds=2" % L)
w.events(lambda e: False, 8)
if not w.closed or time.time() - t0 > 7:
    fail("timeoutSeconds=2: closed=%s after %.1fs" % (w.closed, time.time() - t0))
print("   timeoutSeconds=2: ended after %.1fs" % (time.time() - t0))

# --- bookmarks, when asked for
w = Watch("&resourceVersion=%d&allowWatchBookmarks=true" % L)
evs = w.events(lambda e: any(x["type"] == "BOOKMARK" for x in e), 8)
bm = [e for e in evs if e["type"] == "BOOKMARK"]
if not bm or bm[0]["object"].get("kind") != "Pod" or not bm[0]["object"]["metadata"].get("resourceVersion"):
    fail("bookmark: %r" % evs)
print("   allowWatchBookmarks: BOOKMARK (kind Pod, resourceVersion %s)"
      % bm[0]["object"]["metadata"]["resourceVersion"])

# --- sendInitialEvents is refused, not half-served
w = Watch("&sendInitialEvents=true&resourceVersionMatch=NotOlderThan")
if w.status != 400:
    fail("sendInitialEvents: %d want 400" % w.status)
print("   sendInitialEvents=true: 400")

# --- a watch with nothing to say is not cut off at 30 s
wait = 33 - (time.time() - t_quiet)
if wait > 0:
    time.sleep(wait)
c, _ = req("POST", NS, pod("late-1", "web", "nginx"))
if c != 201:
    fail("late create: %d" % c)
evs = quiet.events(lambda e: ("ADDED", "late-1") in names(e), 10)
if ("ADDED", "late-1") not in names(evs):
    fail("a watch held %.0fs lost its stream: %r" % (time.time() - t_quiet, names(evs)[-3:]))
print("   a watch held %.0fs still streams (ADDED late-1)" % (time.time() - t_quiet))

# --- concurrent requests: every one answered (a decode-mode pipeline fed by a
# byte stream would take two requests' frames as one and lose the second)
import threading
codes = []
def one():
    c, _ = req("GET", NS + "/web-0")
    codes.append(c)
ts = [threading.Thread(target=one) for _ in range(16)]
for t in ts:
    t.start()
for t in ts:
    t.join(20)
if codes.count(200) != 16:
    fail("16 concurrent GETs: %r" % sorted(codes))
print("   16 concurrent GETs: all 200")

# --- LIST: selectors, an empty result, and paging (limit / continue)
def listing(q):
    c, b = req("GET", NS + q)
    if c != 200:
        fail("list %s: %d" % (q, c))
    d = json.loads(b)
    if d.get("kind") != "PodList" or "resourceVersion" not in d.get("metadata", {}):
        fail("list %s envelope: %r" % (q, b[:200]))
    return [i["metadata"]["name"] for i in d["items"]], d["metadata"]
got, _ = listing("?labelSelector=app%3Dweb")
if got != ["late-1", "web-0"]:
    fail("list labelSelector=app=web: %r" % got)
got, _ = listing("?fieldSelector=metadata.name%3Ddb-1")
if got != ["db-1"]:
    fail("list fieldSelector=metadata.name=db-1: %r" % got)
got, _ = listing("?labelSelector=app%3Dnone")
if got != []:
    fail("list labelSelector=app=none: %r" % got)
print("   LIST labelSelector app=web: [late-1, web-0]; fieldSelector name=db-1: [db-1]; no match: []")
page1, meta1 = listing("?limit=2")
if page1 != ["db-1", "late-1"] or not meta1.get("continue"):
    fail("list limit=2: %r %r" % (page1, meta1))
page2, meta2 = listing("?limit=2&continue=%s" % meta1["continue"])
if page2 != ["web-0"] or meta2.get("continue"):
    fail("list continue: %r %r" % (page2, meta2))
print("   LIST limit=2: %r + continue -> %r" % (page1, page2))
PY

echo "== E2E green: LIST and WATCH are Chronicle chains over the store change feed =="
