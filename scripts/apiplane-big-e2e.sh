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
#
# A store too busy to decide a 1 MiB write in time (a loaded host: 10-30 s)
# answers it 503 ServiceUnavailable with Retry-After — never 500, never the
# HTTP server's 504 — and the client asks again, as a Kubernetes client
# does; the write lands. NC_BIG_STALL=1 makes that happen on any host (the
# writer gives the store 1 s), NC_FAULT_EVERY=N runs it on a store whose
# reads wait.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7448
# NC_FAULT_EVERY=N (N >= 2; 0 = off, the default): the store's TEST-ONLY
# busy knob (see scripts/store-read-wait-e2e.sh) — the replace of a 1 MiB
# object under a store that answers "not yet" as a loaded host's does.
EVERY="${NC_FAULT_EVERY:-0}"
case "$EVERY" in ''|*[!0-9]*) echo "FAIL: NC_FAULT_EVERY must be a number"; exit 1 ;; esac
[ "$EVERY" = 0 ] || [ "$EVERY" -ge 2 ] || { echo "FAIL: NC_FAULT_EVERY must be 0 or >= 2"; exit 1; }
# NC_BIG_STALL=1: the writer (se_probe, store_doc) gives the store 1 s to
# decide a write (`answer_ms`) — a 1 MiB create or replace takes the store
# several, on any host — so each answers "not decided in time". That is a
# retryable 503 ServiceUnavailable (once a 500 InternalError: the reply stage
# read store_doc's 503 as an internal error), never a 500 or 504, and the
# write still lands: the test waits for it. The store as a loaded host makes
# it, deterministically.
STALL="${NC_BIG_STALL:-0}"

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
python3 - "$GRAPH" "$D/graph.yaml" "$EVERY" "$STALL" <<'PORTS'
import os, sys
src, dst, every, stall = sys.argv[1:]
g = open(src).read().replace("    port: 7444", "    port: 7448")
# A tomb that ends inside the test (the default is 5 min): the deleted
# 1 MiB object's tomb must go at its deadline, though no source can read it
# into a record (its end is indexed apart, /~tomb-at/).
old = '    # A delete leaves the object\'s last state for a watch\'s DELETED event.\n    tomb: "/~tomb"\n'
if g.count(old) != 1:
    sys.exit(f"expected one tomb writer, found {g.count(old)}")
g = g.replace(old, old + "    tomb_ms: 40000\n")
if stall != "0":
    old = '    blob: "/blobs/sha256/"\n'
    if g.count(old) != 1:
        sys.exit(f"expected one writer, found {g.count(old)}")
    g = g.replace(old, old + "    answer_ms: 1000\n")
if every != "0":
    # The store busy as a loaded host (or the board's device) makes it:
    # one step in N answers every read EAGAIN, busy steps reach the store's
    # own lookups, and each write's log answer is held a few steps (TEST
    # ONLY; this rendered copy).
    old = "      history_revisions: 65536\n"
    if g.count(old) != 1:
        sys.exit(f"expected one store, found {g.count(old)}")
    g = g.replace(old, old + f"      fault_read_eagain_every: {every}\n"
                  "      fault_busy_reaches_store: 1\n"
                  f"      fault_log_hold_steps: {os.environ.get('NC_FAULT_LOG_HOLD', '4')}\n")
open(dst, "w").write(g)
PORTS
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
if [ "$EVERY" != 0 ]; then
  grep -aq "FAULT INJECTION ON" "$D/modules.bin" || fail "the built store does not know the fault knob"
  echo "   store fault knob on: one step in $EVERY busy"
fi
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
python3 - "$PORT" "$(cat "$D/token.admin")" "$STALL" <<'PY' || fail "large objects"
import json, socket, sys, time
port, tok, stall = int(sys.argv[1]), sys.argv[2], sys.argv[3] != "0"
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
    send.head = head
    return int(head.split(b"\r\n")[0].split(b" ")[1]), rest

def retry_after(head):
    for line in head.split(b"\r\n")[1:]:
        k, _, v = line.partition(b":")
        if k.strip().lower() == b"retry-after":
            return int(v.strip())
    return None

def undecided(what, code, body):
    """A write the store could not decide in time: 503 ServiceUnavailable,
    retryable. Anything else 5xx is the bug this test exists for."""
    if code >= 500 and code != 503:
        fail("%s: %d %r — a write not decided in time is a retryable 503, never %d"
             % (what, code, body[:200], code))
    if code == 503:
        st = json.loads(body) if body else {}
        if st.get("reason") != "ServiceUnavailable" or st.get("code") != 503:
            fail("%s: 503 without its Status: %r" % (what, body[:200]))
        return True
    return False

def send_retrying(method, path, body):
    """A replace as a Kubernetes client sends it: a 503 that says when to
    come back (Retry-After) is asked again then; the write is idempotent."""
    for attempt in range(4):
        code, rbody = send(method, path, body)
        if not undecided("%s %s" % (method, path), code, rbody):
            return code, rbody, attempt
        ra = retry_after(send.head)
        if ra is None:
            return code, rbody, attempt
        print("   %s: 503 ServiceUnavailable, Retry-After %ds — asked again" % (method, ra))
        time.sleep(ra)
    return code, rbody, attempt

def wait_for(path, ok, what):
    end = time.time() + 120
    while time.time() < end:
        code, body = get(path)
        if code == 200 and ok(json.loads(body)):
            return
        time.sleep(1)
    fail("%s never landed" % what)

w = socket.create_connection(("127.0.0.1", port), timeout=60)
rv0 = int(json.loads(get(NS)[1])["metadata"]["resourceVersion"])
w.sendall(("GET %s?watch=true&resourceVersion=%d HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer %s\r\n\r\n"
           % (NS, rv0, tok)).encode())
# Read as a watch client reads: all along, not after the writes. A client
# that stops reading while megabytes of events queue is cut as too slow.
import threading
wq, wdone = [], threading.Event()
def drain():
    w.settimeout(1)
    while not wdone.is_set():
        try:
            chunk = w.recv(1 << 20)
        except socket.timeout:
            continue
        except OSError:
            break
        if not chunk:
            break
        wq.append(chunk)
wt = threading.Thread(target=drain, daemon=True)
wt.start()
big2 = {"apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": "big2", "namespace": "default"},
        "data": {"k%05d" % i: "w" * 40 for i in range(22000)}}
code, body = send("POST", NS, json.dumps(big2).encode())
if stall:
    if not undecided("POST big2", code, body):
        fail("POST big2 with the store given 1 s: %d %r, want 503" % (code, body[:200]))
    if retry_after(send.head) is None:
        fail("POST big2: a 503 with no Retry-After")
    wait_for(NS + "/big2", lambda o: o["data"]["k00007"] == "w" * 40, "the create")
    print("   POST 1 MiB, the store given 1 s: 503 ServiceUnavailable (Retry-After), and the create landed")
else:
    tries = 0
    while undecided("POST big2", code, body):
        # As a Kubernetes client does: asked again after Retry-After. The
        # first may have landed: then the create is AlreadyExists, and the
        # object is ours.
        ra = retry_after(send.head)
        if ra is None or tries == 3:
            fail("POST big2: 503 %s" % ("with no Retry-After" if ra is None else "four times"))
        tries += 1
        print("   POST: 503 ServiceUnavailable, Retry-After %ds — asked again" % ra)
        time.sleep(ra)
        code, body = send("POST", NS, json.dumps(big2).encode())
    if tries and code == 409:
        c2, b2 = get(NS + "/big2")
        if c2 != 200 or json.loads(b2)["data"]["k00007"] != "w" * 40:
            fail("POST big2: 409 after a 503, and the object is not ours: %d" % c2)
        print("   POST 1 MiB: the 503'd create had landed (409 AlreadyExists on the retry, the object ours)")
    else:
        if code != 201:
            fail("POST big2: %d %r" % (code, body[:300]))
        made = json.loads(body)
        if made["metadata"]["name"] != "big2" or len(made["data"]) != 22000:
            fail("POST big2 answered %r…" % body[:200])
        print("   POST 1 MiB: 201, answered with the object it stored (%d bytes)%s"
              % (len(body), " (after %d retryable 503)" % tries if tries else ""))
code, body = get(NS + "/big2")
if code != 200 or json.loads(body)["data"]["k00007"] != "w" * 40:
    fail("GET big2 after create: %d" % code)
big2["data"]["k00007"] = "changed"
if stall:
    code, body = send("PUT", NS + "/big2", json.dumps(big2).encode())
    if not undecided("PUT big2", code, body):
        fail("PUT big2 with the store given 1 s: %d %r, want 503" % (code, body[:200]))
    wait_for(NS + "/big2", lambda o: o["data"]["k00007"] == "changed", "the replace")
    print("   PUT 1 MiB, the store given 1 s: 503 ServiceUnavailable, and the replace landed")
else:
    code, body, tries = send_retrying("PUT", NS + "/big2", json.dumps(big2).encode())
    if code != 200 or json.loads(body)["data"]["k00007"] != "changed":
        fail("PUT big2: %d %r" % (code, body[:200]))
    print("   PUT 1 MiB: 200, the object as replaced%s"
          % (" (after %d retryable 503)" % tries if tries else ""))
buf, evs, end = b"", [], time.time() + 30
while len(evs) < 2 and time.time() < end:
    if not wq:
        if not wt.is_alive():
            break
        time.sleep(0.1)
        continue
    buf += wq.pop(0)
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

# --- DELETE: a 1 MiB object leaves its tomb (its last state, whole) for the
# watch's DELETED event, and is gone. The tomb is written inside a journaled
# serve; a 1 MiB read or write there once overflowed the journal, so the
# tomb was never written and the DELETED event carried no object.
code, body, tries = send_retrying("DELETE", NS + "/big2", b"")
if code == 404 and tries:
    print("   DELETE 1 MiB: the 503'd delete had landed (404 on the retry)")
elif code not in (200, 202):
    fail("DELETE big2: %d %r" % (code, body[:200]))
else:
    print("   DELETE 1 MiB: %d%s" % (code, " (after %d retryable 503)" % tries if tries else ""))
end = time.time() + 60
dele = None
while dele is None and time.time() < end:
    if not wq:
        if not wt.is_alive():
            break
        time.sleep(0.1)
        continue
    buf += wq.pop(0)
    while b"\n" in buf:
        line, _, buf = buf.partition(b"\n")
        if line.strip():
            e = json.loads(line)
            if e["type"] == "DELETED" and e["object"]["metadata"]["name"] == "big2":
                dele = e
            elif e["type"] != "BOOKMARK":
                fail("WATCH before the DELETED: %r" % ((e["type"], e["object"]["metadata"].get("name")),))
wdone.set()
if dele is None:
    fail("WATCH: no DELETED event for the 1 MiB object")
# As upstream: the DELETED event carries the object's last state, whole —
# read from its 1 MiB tomb onto the watch's heap, streamed across records.
data = dele["object"].get("data") or {}
if len(data) != 22000 or data.get("k00007") != "changed":
    fail("WATCH: the DELETED event does not carry the object whole: %d keys, k00007=%r"
         % (len(data), data.get("k00007")))
if dele["object"]["metadata"].get("namespace") != "default":
    fail("WATCH: the DELETED event's object: %r" % dele["object"]["metadata"])
code, body = get(NS + "/big2")
if code != 404:
    fail("GET big2 after its delete: %d, want 404" % code)
print("   WATCH: DELETED big2, whole (22000 keys, its last state); GET -> 404")

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

echo "== 5. the deleted 1 MiB object left its tomb, whole =="
# store_doc's journaled serve writes the tomb before the delete goes to the
# dialect: its last state, with a deadline, for a watch's DELETED event. A
# 1 MiB tomb (and the 1 MiB read it is built from) once overflowed the
# serve's 16 KiB journal: the tomb was refused ERR_DIVERGED, never written.
TOMB_OK=""
for _ in $(seq 60); do
  if nc_store_get "$D" "/~tomb/configmaps/default/big2" | python3 -c '
import json, sys
t = json.loads(sys.stdin.buffer.read() or b"null")
assert isinstance(t, dict) and str(t.get("deadline", "")), "no tomb"  # a moment (_shared/tomb.rs)
o = t["obj"]
assert o["metadata"]["name"] == "big2" and len(o["data"]) == 22000, "not whole"
assert o["data"]["k00007"] == "changed", "not the last state"
' 2>/dev/null; then TOMB_OK=1; break; fi
  sleep 0.5
done
[ -n "$TOMB_OK" ] || fail "the 1 MiB delete left no tomb (or not the object whole) at /~tomb/configmaps/default/big2"
echo "   /~tomb/configmaps/default/big2: the object's last state, whole (22000 keys), with its deadline"
AT="$(nc_store_get "$D" "/~tomb-at/configmaps/default/big2")"
[ -n "$AT" ] || fail "the 1 MiB tomb has no expiry entry at /~tomb-at/configmaps/default/big2"
echo "   /~tomb-at/configmaps/default/big2: its end, indexed apart: $AT"
# A restart with the tomb pending: its deadline is a moment (wall time when
# the clock is trusted, else this boot's monotonic time, re-based to its
# ttl past the next boot) — either way it ends within tomb_ms (40 s here)
# of the restart, never waits out the previous run's uptime.
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
RUNTIME_PID=$!
RESTARTED=$SECONDS
echo "   runtime restarted with the tomb pending"
# The tomb ends at its deadline: the source scans the small index entry,
# never the 1 MiB tomb, and drops both together.
GONE=""
for _ in $(seq 120); do
  if [ -z "$(nc_store_get "$D" "/~tomb/configmaps/default/big2")" ] \
     && [ -z "$(nc_store_get "$D" "/~tomb-at/configmaps/default/big2")" ]; then GONE=1; break; fi
  sleep 1
done
[ -n "$GONE" ] || fail "the 1 MiB tomb (or its index entry) outlived its deadline"
TOOK=$((SECONDS - RESTARTED))
[ "$TOOK" -le 55 ] || fail "the tomb ended ${TOOK}s after the restart, past its 40 s ttl"
echo "   ${TOOK}s after the restart (ttl 40 s): the 1 MiB tomb and its index entry are gone"

echo "== 6. staged bodies are collected at their deadline =="
STALE="$(nc_store_keys "$D" /blobs/ | grep -xF -e "/blobs/sha256/$(printf 'ab%.0s' $(seq 32))" -e "/blobs/at/$(printf 'ab%.0s' $(seq 32))" || true)"
[ -z "$STALE" ] || fail "the expired staged body was not collected: still live: $STALE"
echo "   /blobs/sha256/abab… and /blobs/at/abab… deleted"

echo "== E2E green: a 1 MiB object is created, replaced, deleted (its tomb whole), served and watched whole; staged bodies are collected =="
