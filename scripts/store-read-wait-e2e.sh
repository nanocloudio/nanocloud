#!/usr/bin/env bash
# Live E2E: the board's store, on Linux. The board-shape plane of
# scripts/metal-plane-e2e.sh (the control plane AND the kubelet's graph-pod
# slice in ONE graph over ONE node-scoped lattice store) runs with the store
# provider's TEST-ONLY fault knob on: `fault_read_eagain_every: N`
# (kv_state_worker_store, lattice docs/architecture/limit_register.md
# "Test-only fault injection") makes one store step in N busy: every read in
# it — GET, HEAD, RANGE_GET, LIST, CHANGES, an initial-listing SUBSCRIBE, by
# any module — answers EAGAIN with nothing moved, as the Pi 5's store answers
# while its device is taken by a write.
# storage.object says a read's EAGAIN is "ask again", never absent: a
# consumer that reads it as absence answers NotFound for a key it has just
# written, and one that re-asks inside a step spins.
#
# PASS = with reads waiting all the time, the injected-wait counter on
# `[kvw] store hb` climbs, AND
#   - Graphs created through the API are read back at once, every one,
#     never 404 (create-then-get), and listed;
#   - a Pod naming a Graph reaches Running, and a Deployment's two Pods too;
#   - `kubectl get` lists (pods, graphs) and a `kubectl get -w` session lists
#     and then hears a Pod created after it started.
#
# The knob ships in no graph (lattice's tools/ci/no_fault_injection.sh): it is
# set here, in the rendered copy under $D only.
#
# Inputs (env):
#   NC_FAULT_EVERY      one store step in N is busy (default 3; >= 2).
#   NC_LATTICE_MODULES  a directory of lattice .fmod built from a checkout
#                       whose kv_state_worker_store knows the knob; that one
#                       module is overlaid on the project's module root (until the synced lattice
#                       carries it, e.g. `fluxor modules build --target
#                       bcm2712 --strict --out X` in lattice: X/aarch64/modules).
#   NC_MODULE_ROOT      the nanocloud module root to run (default: this
#                       project's target/fluxor/aarch64/modules).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Not 7443: this plane is slow on purpose, and shares no port with the
# others.
PORT=7643
EVERY="${NC_FAULT_EVERY:-3}"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-read-wait-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
cleanup() {
  # Waited for, not just signalled: a runtime still draining holds :7443, and
  # the next E2E to bind it finds no API.
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <hex-value>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
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

echo "== 0. the module root: the project's, and a store that knows the fault knob =="
case "$EVERY" in ''|*[!0-9]*) fail "NC_FAULT_EVERY must be a number >= 2" ;; esac
[ "$EVERY" -ge 2 ] || fail "NC_FAULT_EVERY must be >= 2 (1 makes every step busy)"
MODROOT="${NC_MODULE_ROOT:-$ROOT/target/fluxor/aarch64/modules}"
[ -f "$MODROOT/kv_state_worker_store.fmod" ] || fail "no module root at $MODROOT (build the project's modules)"
mkdir -p "$D/modroot"
cp -a "$MODROOT/." "$D/modroot/"
BUILD_ROOT="$ROOT"
if [ -n "${NC_LATTICE_MODULES:-}" ]; then
  # fluxor.lock pins win over any module root, so the overlaid modules are
  # built from a scratch project whose lock no longer pins them.
  BUILD_ROOT="$D/proj"
  mkdir -p "$BUILD_ROOT/target/fluxor"
  cp "$ROOT/fluxor.toml" "$BUILD_ROOT/"
  names=()
  # Only the store: every other module is the pinned one, as shipped.
  for f in "$NC_LATTICE_MODULES"/kv_state_worker_store.fmod; do
    [ -f "$f" ] || fail "no kv_state_worker_store.fmod in $NC_LATTICE_MODULES"
    cp "$f" "$D/modroot/"; names+=("$(basename "$f" .fmod)")
  done
  python3 - "$ROOT/fluxor.lock" "$BUILD_ROOT/fluxor.lock" "${names[@]}" <<'PY' || fail "the scratch lock"
import re, sys
src, dst, *names = sys.argv[1:]
parts = re.split(r"(?m)^(?=\[\[artifact\]\])", open(src).read())
keep = [p for p in parts if not any(re.search(rf'(?m)^name = "{re.escape(n)}"$', p) for n in names)]
open(dst, "w").write("".join(keep))
PY
  echo "   kv_state_worker_store from $NC_LATTICE_MODULES, unpinned"
fi
grep -aq fault_log_hold_steps "$D/modroot/kv_state_worker_store.fmod" \
  || fail "this kv_state_worker_store has no fault_log_hold_steps (set NC_LATTICE_MODULES to a lattice build that has it)"
# Every graph this test builds is built against that root.
export FLUXOR_MODULE_ROOT="$D/modroot"

echo "== 1. seed the operator's identity: SA key, cluster-admin binding, a ready node =="
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
now = int(time.time())
claims = {"iss": ISSUER.decode(), "sub": "system:serviceaccount:default:admin",
          "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/token.admin","w").write((signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode())
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$STORE/store.log" admin "*" "*"
rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
put_text "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
put_text "/nodes/node-1" '{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1"},"status":{"ready":true,"conditions":[{"type":"Ready","status":"True"}]}}'
FLXA="$(python3 "$ROOT/scripts/support/flxa.py" churn_probe:1=0)"
put_text "/graphs.nanocloud.io/probe" "{\"apiVersion\":\"nanocloud.io/v1\",\"kind\":\"Graph\",\"metadata\":{\"name\":\"probe\"},\"spec\":{\"flxa\":\"$FLXA\"}}"

echo "== 2. render the control plane for this node, splice the kubelet's graph-pod slice onto its store =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
python3 - "$D/graph.yaml" "$ROOT" node-1 "$EVERY" "$PORT" <<'PY2' || fail "the splice did not apply"
import os, re, sys
out, root, node, every, port = sys.argv[1:6]
g = open(out).read()
def sub(old, new):
    global g
    if g.count(old) != 1: sys.exit(f"expected 1 x {old[:50]!r}, found {g.count(old)}")
    g = g.replace(old, new)
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/")
sub('      scope: ""\n      scoped: ""\n', f'      scope: "{node}"\n      scoped: "{SCOPED}"\n')
# The board's store, on Linux: reads wait (TEST ONLY; this rendered copy).
sub('      history_revisions: 65536\n',
    f'      history_revisions: 65536\n      fault_read_eagain_every: {every}\n'
    '      fault_busy_reaches_store: 1\n'
    f'      fault_log_hold_steps: {os.environ.get("NC_FAULT_LOG_HOLD", "4")}\n')
sub('    port: 7443\n', f'    port: {port}\n')
kube = open(f"{root}/packaging/debian/fluxor-podlifecycle.yaml").read()
kube_mods = kube[kube.index("modules:\n") + len("modules:\n"):kube.index("\nwiring:")]
kube_wire = kube[kube.index("wiring:\n") + len("wiring:\n"):]
KEEP_PREFIX = ("pj_", "pl_", "pr_")
KEEP = {"wl", "sb_mspec", "sb_mspecd", "sb_sig", "sb_sigd", "sb_pause", "sb_paused", "sb_ev", "sb_evout"}
def kept(name): return name in KEEP or name.startswith(KEEP_PREFIX)
blocks = re.split(r"(?m)^(?=  - name: )", kube_mods)
kept_mods = "".join(b for b in blocks if b.startswith("  - name: ") and kept(b.split()[2]))
pairs, frm = [], None
for line in kube_wire.splitlines():
    m = re.match(r"  - from: (\S+)$", line)
    if m: frm = m.group(1); continue
    m = re.match(r"    to: (\S+)$", line)
    if m and frm: pairs.append((frm, m.group(1))); frm = None
edges = "".join(f"  - from: {a}\n    to: {b}\n" for a, b in pairs if kept(a.split(".")[0]) and kept(b.split(".")[0]))
mods, wiring = g.split("\nwiring:\n", 1)
g = (mods.rstrip("\n") + "\n  # ── The pod plane: fluxor-podlifecycle.yaml, its graph-pod slice ──\n"
     + kept_mods.rstrip("\n") + "\n\nwiring:\n" + wiring.rstrip("\n") + "\n" + edges
     + "\nworkload_modules:\n  - churn_probe\n")
open(out, "w").write(g)
print(f"   spliced: {g.count(chr(10) + '  - name:')} modules")
PY2
nc_build_workload "$BUILD_ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
grep -aq "FAULT INJECTION ON" "$D/modules.bin" || fail "the built graph does not carry the store that knows the fault knob"

echo "== 3. run it; wait for the API over TLS =="
env -C "$STORE" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"
api() { # api <method> <path> [json-body] -> "<status>|<body>"
  python3 - "$PORT" "$D/token.admin" "$1" "$2" "${3:-}" <<'PY'
import socket, ssl, sys
port, tokf, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
tok = open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
raw = socket.create_connection(("127.0.0.1", port), timeout=10)
s = ctx.wrap_socket(raw, server_hostname="localhost")
b = body.encode()
req = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n"
       f"Content-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b
s.sendall(req)
buf = b""
s.settimeout(10)
try:
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
except (socket.timeout, ssl.SSLError):
    pass
s.close()
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac


grep -aq "FAULT INJECTION ON (test only): one store step in N busy, every read EAGAIN, N=$EVERY" "$D/run.log" \
  || fail "the store did not turn the fault knob on"
echo "   one store step in $EVERY is busy: every read in it answers EAGAIN"

echo "== 3b. Graphs created through the API are read back at once, never 404 =="
# Each create is answered from the store, and the GET straight after it
# reads the key just written: a read that waits and is read as absent is
# the 404 this catches.
for i in $(seq 0 11); do
  G="{\"apiVersion\":\"nanocloud.io/v1\",\"kind\":\"Graph\",\"metadata\":{\"name\":\"rw-$i\"},\"spec\":{\"flxa\":\"$FLXA\"}}"
  for _ in $(seq 90); do
    r="$(api POST /apis/nanocloud.io/v1/graphs "$G")"
    case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
  done
  case "$r" in 20[01]\|*) ;; *) fail "POST graph rw-$i: ${r:0:200}" ;; esac
  r="$(api GET /apis/nanocloud.io/v1/graphs/rw-$i)"
  case "$r" in 200\|*"\"name\":\"rw-$i\""*"$FLXA"*) ;;
    *) fail "GET graphs/rw-$i straight after its create: ${r:0:200}" ;; esac
done
echo "   12 x POST graph then GET: 200, flxa intact, never 404"
r="$(api GET /apis/nanocloud.io/v1/graphs)"
n="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(sum(1 for g in json.load(sys.stdin).get("items",[]) if g["metadata"]["name"].startswith("rw-")))
except Exception: print(-1)')"
[ "${r%%|*}" = 200 ] && [ "$n" = 12 ] || fail "LIST graphs: ${n} of 12 rw-* (${r:0:200})"
echo "   LIST graphs: all 12"

echo "== 4. a Pod naming the Graph is created through the API =="
POD='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"probe"}]}}'
for _ in $(seq 90); do
  r="$(api POST /api/v1/namespaces/default/pods "$POD")"
  case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
done
case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST pod: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
echo "   uid $UID_"

echo "== 5. the node stages it; the Pod's phase reaches the object =="
phase=""; node=""
for _ in $(seq 90); do
  r="$(api GET /api/v1/namespaces/default/pods/probe)"
  phase="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin); print(d.get("status",{}).get("phase",""), d.get("spec",{}).get("nodeName",""))
except Exception: print("", "")' 2>/dev/null)"
  node="${phase#* }"; phase="${phase%% *}"
  [ "$phase" = "Running" ] && break
  sleep 1
done
echo "   nodeName=${node:-} phase=${phase:-}"
if [ "$phase" != "Running" ]; then
  echo "   the chain, as the store holds it:"
  for k in "/~node/node-1/pod-specs/$UID_" "/~node/node-1/metal-sandboxes/$UID_" "/~node/node-1/sandbox-status/$UID_" \
           "/~node/node-1/pod-runtime/$UID_" "/~node/node-1/pod-lifecycle-status/$UID_" "/podstatus/$UID_"; do
    printf '   %-48s %s\n' "$k" "$(nc_store_get "$STORE" "$k" 2>/dev/null | cut -c1-110)"
  done
  grep -a "\[live\]\|owners=\|workload" "$D/run.log" | tail -3 | cut -c1-120
  fail "the Pod never reached Running (phase='${phase:-}')"
fi
echo "== 6. a Deployment whose template names the Graph: its ReplicaSet's Pods run too =="
DEP='{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"probes","namespace":"default"},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"probes"}},"template":{"metadata":{"labels":{"app":"probes"}},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"probe"}]}}}}'
r="$(api POST /apis/apps/v1/namespaces/default/deployments "$DEP")"
case "$r" in 20[01]\|*) echo "   POST deployments -> ${r%%|*}" ;; *) fail "POST deployment: ${r:0:200}" ;; esac
running=0
for _ in $(seq 120); do
  r="$(api GET '/api/v1/namespaces/default/pods?labelSelector=app%3Dprobes')"
  running="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin); print(sum(1 for p in d.get("items",[]) if p.get("status",{}).get("phase")=="Running" and p.get("metadata",{}).get("uid")))
except Exception: print(0)' 2>/dev/null)"
  [ "${running:-0}" -ge 2 ] && break
  sleep 1
done
echo "   Running Pods of deployment/probes, each with a uid: ${running:-0} of 2"
[ "${running:-0}" -ge 2 ] || { printf '%s\n' "${r#*|}" | head -c 600; echo; fail "the Deployment's Pods did not all reach Running"; }
# The chain of controller references kubectl follows (describe, rollout):
# Deployment uid -> its ReplicaSet's controller ref; ReplicaSet uid -> each
# Pod's. Every object parses as JSON.
DJ="$(api GET /apis/apps/v1/namespaces/default/deployments/probes)"
RJ="$(api GET /apis/apps/v1/namespaces/default/replicasets/probes)"
PJ="$(api GET '/api/v1/namespaces/default/pods?labelSelector=app%3Dprobes')"
python3 - "${DJ#*|}" "${RJ#*|}" "${PJ#*|}" <<'PY' || fail "the Deployment's controller references are not a chain"
import json, sys
d, r, pods = (json.loads(x) for x in sys.argv[1:4])
def ctl(o):
    refs = [x for x in o["metadata"].get("ownerReferences", []) if x.get("controller")]
    assert len(refs) == 1, f"{o['metadata']['name']}: controller refs {refs}"
    return refs[0]
x = ctl(r)
assert (x["kind"], x["name"], x["uid"], x["apiVersion"]) == ("Deployment", "probes", d["metadata"]["uid"], "apps/v1"), x
for p in pods["items"]:
    y = ctl(p)
    assert (y["kind"], y["uid"], y["apiVersion"]) == ("ReplicaSet", r["metadata"]["uid"], "apps/v1"), y
    assert p.get("kind") == "Pod" and p["metadata"].get("uid"), p["metadata"]
print("   controller references: deployment", d["metadata"]["uid"][:8], "-> replicaset", r["metadata"]["uid"][:8], "->", len(pods["items"]), "pods")
PY
echo "== 7. kubectl lists, and a watch lists then hears what follows =="
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
k() { kubectl --kubeconfig "$KC" --request-timeout=30s "$@"; }
k get pods -o name >"$D/list.out" 2>&1 || { cat "$D/list.out"; fail "kubectl get pods"; }
grep -qx "pod/probe" "$D/list.out" || { cat "$D/list.out"; fail "kubectl get pods did not list pod/probe"; }
[ "$(grep -c '^pod/probes-' "$D/list.out")" -ge 2 ] || { cat "$D/list.out"; fail "kubectl get pods did not list the Deployment's Pods"; }
echo "   kubectl get pods: $(wc -l <"$D/list.out") pods"
k get graphs.nanocloud.io -o name >"$D/graphs.out" 2>&1 || { cat "$D/graphs.out"; fail "kubectl get graphs"; }
[ "$(grep -c 'rw-' "$D/graphs.out")" = 12 ] || { cat "$D/graphs.out"; fail "kubectl get graphs did not list the 12"; }
echo "   kubectl get graphs: $(wc -l <"$D/graphs.out") graphs"
kubectl --kubeconfig "$KC" get pods -w --output-watch-events -o name >"$D/w.out" 2>"$D/w.err" &
W=$!
for _ in $(seq 60); do grep -q "pod/probe$" "$D/w.out" 2>/dev/null && break; sleep 0.5; done
grep -q "pod/probe$" "$D/w.out" || { kill "$W" 2>/dev/null; cat "$D/w.err"; fail "kubectl get -w never listed"; }
POD2='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"late","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"probe"}]}}'
r="$(api POST /api/v1/namespaces/default/pods "$POD2")"
case "$r" in 20[01]\|*) ;; *) kill "$W" 2>/dev/null; fail "POST pod late: ${r:0:200}" ;; esac
for _ in $(seq 60); do grep -q "pod/late$" "$D/w.out" 2>/dev/null && break; sleep 0.5; done
kill "$W" 2>/dev/null || true; wait "$W" 2>/dev/null || true
grep -q "pod/late$" "$D/w.out" || { cat "$D/w.out" "$D/w.err"; fail "kubectl get -w never heard pod/late"; }
echo "   kubectl get -w: listed pod/probe, then heard pod/late"

echo "== 8. the store really made reads wait =="
inj=""
for _ in $(seq 20); do
  inj="$(grep -ao '\[kvw\] store hb .* fault=[0-9]*' "$D/run.log" | tail -1 | sed 's/.*fault=//')"
  [ -n "$inj" ] && [ "$inj" -gt 0 ] && break
  sleep 1
done
[ -n "$inj" ] && [ "$inj" -gt 0 ] || fail "no injected read waits counted on [kvw] store hb"
echo "   reads answered EAGAIN by injection: $inj"
echo "== E2E green: on the board's store shape with one store step in $EVERY busy, Graphs read back at once, Pods ran, and kubectl listed and watched =="
