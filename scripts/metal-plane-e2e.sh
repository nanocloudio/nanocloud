#!/usr/bin/env bash
# Live E2E: the bare-metal node's shape, on Linux — the control plane AND the
# kubelet's graph-pod slice in ONE graph over ONE store scoped to the node,
# laid out as scripts/rig-render-controlplane.sh lays it out for the board
# (the store's `scope`/`scoped`, the slice of fluxor-podlifecycle.yaml a
# graph-backed Pod needs, `workload_modules`). A Graph and a Pod naming it
# are applied through the API; the node stages the subgraph of its own
# image and the Pod's phase reaches the object: `kubectl get pods` Running.
#
# PASS = the Pod's status.phase is Running within 90 s. On failure the
# records of the chain are printed from the store, in order, so the stage
# that stopped is named.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-dep-api-XXXXXX)"
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
python3 - "$D/graph.yaml" "$ROOT" node-1 <<'PY2' || fail "the splice did not apply"
import re, sys
out, root, node = sys.argv[1:4]
g = open(out).read()
def sub(old, new):
    global g
    if g.count(old) != 1: sys.exit(f"expected 1 x {old[:50]!r}, found {g.count(old)}")
    g = g.replace(old, new)
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/")
sub('      scope: ""\n      scoped: ""\n', f'      scope: "{node}"\n      scoped: "{SCOPED}"\n')
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
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

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
# Labelled as the template says, beside the ReplicaSet's own non-empty
# pod-template-hash — what `kubectl get pods -l` and a rollout select on.
h = r["metadata"]["labels"].get("pod-template-hash", "")
assert len(h) == 8, r["metadata"]["labels"]
for p in pods["items"]:
    assert p["metadata"]["labels"] == {"app": "probes", "pod-template-hash": h}, p["metadata"]["labels"]
print("   controller references: deployment", d["metadata"]["uid"][:8], "-> replicaset", r["metadata"]["uid"][:8], "->", len(pods["items"]), "pods, labelled app=probes, pod-template-hash", h)
PY

echo "== 7. a Deployment with a REAL template (> 4 KiB): ReplicaSet, Pods Running, rolled to another =="
# env, ports, resources, annotations: a template past the decisions' 512 B
# scratch and past a record. The chain carries its digest; the writers
# splice the bytes from the stored objects; the readers past a record
# (store_scan, store_stamped) read them whole.
bigdep() { # bigdep <variant>
  python3 - "$1" <<'PYB'
import json, sys
v = sys.argv[1]
env = [{"name": "APP_SETTING_%02d" % i, "value": "value-%s-%02d-abcdefghijklmnopqrstuvwxyz" % (v, i)} for i in range(48)]
tmpl = {"metadata": {"labels": {"app": "bigweb", "tier": "backend"},
                     "annotations": {"example.com/note-%d" % i: "annotation %d of template %s" % (i, v) for i in range(8)}},
        "spec": {"runtimeClassName": "fluxor",
                 "containers": [{"name": "probe", "image": "probe", "env": env,
                                 "ports": [{"name": "http", "containerPort": 8080, "protocol": "TCP"}],
                                 "resources": {"requests": {"cpu": "100m", "memory": "64Mi"},
                                               "limits": {"cpu": "500m", "memory": "256Mi"}}}]}}
assert len(json.dumps(tmpl, separators=(",", ":"))) >= 4096
print(json.dumps({"apiVersion": "apps/v1", "kind": "Deployment",
                  "metadata": {"name": "bigweb", "namespace": "default"},
                  "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "bigweb"}}, "template": tmpl}},
                 separators=(",", ":")))
PYB
}
bigwait() { # bigwait <variant>: 2 Running Pods on that template, labelled, hashed as their ReplicaSet
  local ok=""
  for _ in $(seq 120); do
    RJ="$(api GET /apis/apps/v1/namespaces/default/replicasets/bigweb)"
    PJ="$(api GET '/api/v1/namespaces/default/pods?labelSelector=app%3Dbigweb')"
    if python3 - "$1" "${RJ#*|}" "${PJ#*|}" <<'PYW' 2>/dev/null; then ok=1; break; fi
import json, sys
v, r, pods = sys.argv[1], json.loads(sys.argv[2]), json.loads(sys.argv[3])
h = r["metadata"]["labels"]["pod-template-hash"]
assert len(h) == 8 and r["spec"]["template"]["spec"]["containers"][0]["env"][47]["value"].startswith("value-" + v)
run = [p for p in pods["items"] if p.get("status", {}).get("phase") == "Running"
       and p["metadata"]["labels"] == {"app": "bigweb", "tier": "backend", "pod-template-hash": h}
       and len(p["metadata"].get("annotations", {})) == 8
       and p["spec"]["containers"][0]["env"][47]["value"].startswith("value-" + v)]
assert len(run) == 2 and len(pods["items"]) == 2
PYW
    sleep 1
  done
  [ -n "$ok" ]
}
r="$(api POST /apis/apps/v1/namespaces/default/deployments "$(bigdep v1)")"
case "$r" in 20[01]\|*) echo "   POST deployments/bigweb ($(bigdep v1 | wc -c) bytes) -> ${r%%|*}" ;; *) fail "POST bigweb: ${r:0:200}" ;; esac
bigwait v1 || { printf '%s\n' "${PJ#*|}" | head -c 600; echo; fail "the large Deployment's Pods did not all reach Running, labelled and hashed"; }
H1="$(printf '%s' "${RJ#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["labels"]["pod-template-hash"])')"
echo "   replicaset bigweb (hash $H1): 2 Pods Running, the whole template, labelled app=bigweb,tier=backend + hash"
r="$(api PUT /apis/apps/v1/namespaces/default/deployments/bigweb "$(bigdep v2)")"
case "$r" in 200\|*) echo "   PUT deployments/bigweb (template v2) -> 200" ;; *) fail "PUT bigweb: ${r:0:200}" ;; esac
bigwait v2 || { printf '%s\n' "${PJ#*|}" | head -c 600; echo; fail "the large Deployment did not roll to its new template"; }
H2="$(printf '%s' "${RJ#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["labels"]["pod-template-hash"])')"
[ "$H1" != "$H2" ] || fail "the template changed and the hash did not ($H1)"
echo "   rolled: hash $H1 -> $H2, 2 Pods Running on the v2 template"
echo "== E2E green: on one node-scoped store, the plane's Pod became the node's subgraph and its phase came back, and a Deployment's Pods ran the same way — a real-sized template too, labelled, rolled =="
