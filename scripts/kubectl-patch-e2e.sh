#!/usr/bin/env bash
# Live E2E: kubectl PATCH against the Chronicle API plane (T1.6.1/T1.6.2).
#
# Every patch shape kubectl sends, applied WHERE THE OBJECT IS (store_effect
# reads, patches and conditionally writes it) and answered with the object as
# stored: a strategic merge (kubectl's default — containers merge by name), a
# JSON merge patch (null deletes), a JSON Patch (RFC 6902, `test` failures are
# 422), label/annotate, a patch of an absent object (404), and server-side
# apply (creates when absent, merges when present; managedFields, conflicts,
# force, removal of what a manager stopped applying). Nothing else of the
# object is lost: the pod keeps every field no patch named.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7449

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
for m in kagi_verify store_effect store_source token_verify decision pipeline http http_chain tls; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-kpatch-XXXXXX)"
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
# A pod that RAN: its sandbox's output tail, recorded under its uid.
put_text "/pods/default/logger" '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"logger","namespace":"default","uid":"0f5e7a2c-1111-4222-8333-444455556666"},"spec":{"containers":[{"name":"c","image":"busybox:1"}]}}'
put_text "/sandbox-logs/0f5e7a2c-1111-4222-8333-444455556666" "$(printf 'one\ntwo\nthree\n')"
put_text "/pods/default/web-0" '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web-0","namespace":"default","labels":{"app":"web"},"finalizers":["a/one"]},"spec":{"containers":[{"name":"web","image":"nginx:1","ports":[{"containerPort":80}],"env":[{"name":"A","value":"1"}]},{"name":"side","image":"busybox:1"}]}}'
echo "   seeded"

echo "== 2. build the SHIPPED graph =="
[ -f "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }
# client-go sends a bearer token only over TLS, so the SHIPPED graph gets a
# server-only TLS front (a throwaway self-signed key; kubectl skips its
# verification) — same nodes, same params behind it.
openssl ecparam -name prime256v1 -genkey -noout -out "$D/server.key.pem" 2>/dev/null
openssl req -x509 -new -key "$D/server.key.pem" -subj "/CN=nanocloud" -days 1 \
  -out "$D/server.pem" 2>/dev/null
openssl x509 -in "$D/server.pem" -outform DER -out "$D/server.der" 2>/dev/null
openssl ec -in "$D/server.key.pem" -outform DER -out "$D/server.key.der" 2>/dev/null
python3 - "$GRAPH" "$D" <<'TLS'
import sys
g, d = open(sys.argv[1]).read(), sys.argv[2]
g = g.replace("modules:\n  - name: http", f"""modules:
  - name: tls
    mode: 1
    verify_peer: 0
    cert_file: "{d}/server.der"
    key_file: "{d}/server.key.der"
  - name: http""")
g = g.replace("    port: 7444", "    port: 7449")
g = g.replace("""  - from: linux_net.net_out
    to: http.net_in
  - from: http.net_out
    to: linux_net.net_in""", """  - from: linux_net.net_out
    to: tls.cipher_in
  - from: tls.cipher_out
    to: linux_net.net_in
  - from: tls.clear_out
    to: http.net_in
  - from: http.net_out
    to: tls.clear_in""")
open(f"{d}/graph.yaml", "w").write(g)
TLS
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

echo "== 4. kubeconfig for the admin service account =="
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
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get pods -o name >"$D/list.out" 2>&1 || { cat "$D/list.out"; fail "kubectl get pods"; }
grep -qx "pod/web-0" "$D/list.out" || { cat "$D/list.out"; fail "kubectl get pods did not list web-0"; }
echo "   kubectl get pods: pod/web-0"

echo "== 5. strategic merge (kubectl's default): containers merge by name =="
podjson() { k get pod web-0 -o json >"$D/pod.json" 2>"$D/pod.err" || { cat "$D/pod.err"; fail "get pod"; }; }
check() { # check <python expression over `p`> <what>
  podjson
  python3 - "$D/pod.json" "$1" <<'PY' || fail "$2"
import json, sys
p = json.load(open(sys.argv[1]))
ok = eval(sys.argv[2])
if not ok:
    print(json.dumps(p, indent=1)[:1500]); sys.exit(1)
PY
  echo "   $2"
}
k patch pod web-0 -p '{"spec":{"containers":[{"name":"web","image":"nginx:2","env":[{"name":"B","value":"2"}]}]},"metadata":{"labels":{"tier":"a"}}}' \
  >"$D/p1.out" 2>&1 || { cat "$D/p1.out"; fail "kubectl patch (strategic)"; }
check '[c["name"] for c in p["spec"]["containers"]] == ["web","side"] and p["spec"]["containers"][0]["image"] == "nginx:2" and [e["name"] for e in p["spec"]["containers"][0]["env"]] == ["A","B"] and p["spec"]["containers"][0]["ports"] == [{"containerPort":80}] and p["metadata"]["labels"] == {"app":"web","tier":"a"}' \
  "web updated in place, side kept, env merged by name, ports kept, labels merged"
k patch pod web-0 -p '{"spec":{"containers":[{"name":"side","$patch":"delete"}]}}' >/dev/null 2>&1 || fail "patch \$patch: delete"
check '[c["name"] for c in p["spec"]["containers"]] == ["web"]' "\$patch: delete removes the side container, only it"

echo "== 6. JSON merge patch: null deletes, the rest merges =="
k patch pod web-0 --type=merge -p '{"metadata":{"labels":{"app":null,"env":"prod"}}}' >/dev/null 2>&1 || fail "kubectl patch --type=merge"
check 'p["metadata"]["labels"] == {"tier":"a","env":"prod"} and p["spec"]["containers"][0]["image"] == "nginx:2"' "labels: app removed, env added; spec untouched"

echo "== 7. JSON Patch (RFC 6902) =="
k patch pod web-0 --type=json -p '[{"op":"test","path":"/spec/containers/0/name","value":"web"},{"op":"replace","path":"/spec/containers/0/image","value":"nginx:3"},{"op":"add","path":"/metadata/annotations","value":{"x":"1"}}]' \
  >/dev/null 2>&1 || fail "kubectl patch --type=json"
check 'p["spec"]["containers"][0]["image"] == "nginx:3" and p["metadata"]["annotations"] == {"x":"1"}' "test held; image replaced; annotations added"
if k patch pod web-0 --type=json -p '[{"op":"test","path":"/spec/containers/0/name","value":"nope"}]' >"$D/p7.out" 2>&1; then
  fail "a JSON Patch whose test fails was applied"
fi
grep -qi "invalid\|422" "$D/p7.out" || { cat "$D/p7.out"; fail "a failed test did not answer 422 Invalid"; }
echo "   a failed test: 422 Invalid, nothing written"

echo "== 8. label, annotate; a patch of an absent object =="
k label pod web-0 zone=z1 >/dev/null 2>&1 || fail "kubectl label"
k annotate pod web-0 note=hi >/dev/null 2>&1 || fail "kubectl annotate"
check 'p["metadata"]["labels"].get("zone") == "z1" and p["metadata"]["annotations"] == {"x":"1","note":"hi"} and p["metadata"]["finalizers"] == ["a/one"]' "label and annotation added; finalizers kept"
if k patch pod nope -p '{"metadata":{"labels":{"a":"b"}}}' >"$D/p8.out" 2>&1; then fail "patched an absent pod"; fi
grep -q "NotFound" "$D/p8.out" || { cat "$D/p8.out"; fail "an absent pod did not answer NotFound"; }
echo "   an absent pod: NotFound"

echo "== 9. server-side apply: creates, then merges =="
cat >"$D/cm.yaml" <<'CM'
apiVersion: v1
kind: ConfigMap
metadata:
  name: applied
  namespace: default
data:
  a: "1"
CM
k apply --server-side --validate=false -f "$D/cm.yaml" >"$D/a1.out" 2>&1 || { cat "$D/a1.out"; fail "apply --server-side (create)"; }
sed -i 's/a: "1"/a: "2"\n  b: "3"/' "$D/cm.yaml"
k apply --server-side --validate=false -f "$D/cm.yaml" >"$D/a2.out" 2>&1 || { cat "$D/a2.out"; fail "apply --server-side (update)"; }
k get configmap applied -o json >"$D/cm.json" 2>&1 || fail "get applied configmap"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1]))["data"]; sys.exit(0 if d=={"a":"2","b":"3"} else 1)' "$D/cm.json" \
  || { cat "$D/cm.json"; fail "server-side apply did not converge"; }
echo "   created, then data a=2 b=3"

echo "== 10. what the server stamps: uid, creationTimestamp, generateName, the namespace label =="
cat >"$D/gen.yaml" <<'GEN'
apiVersion: v1
kind: ConfigMap
metadata:
  generateName: gen-
  namespace: default
data:
  a: "1"
GEN
k create --validate=false -f "$D/gen.yaml" >"$D/g.out" 2>&1 || { cat "$D/g.out"; fail "create with generateName"; }
GEN_NAME="$(sed -n 's#^configmap/\(gen-[a-z0-9]\{5\}\) created$#\1#p' "$D/g.out")"
[ -n "$GEN_NAME" ] || { cat "$D/g.out"; fail "no generated name answered"; }
k get configmap "$GEN_NAME" -o json >"$D/g.json" 2>&1 || fail "get $GEN_NAME"
python3 - "$D/g.json" <<'PY' || fail "create stamps"
import json, re, sys
m = json.load(open(sys.argv[1]))["metadata"]
assert re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", m.get("uid", "")), m
assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", m.get("creationTimestamp", "")), m
assert m.get("resourceVersion"), m
PY
UID0="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["metadata"]["uid"])' "$D/g.json")"
python3 - "$D/g.json" >"$D/g2.json" <<'PY'
import json, sys
o = json.load(open(sys.argv[1]))
o["data"]["a"] = "2"
for k in ("uid", "creationTimestamp", "resourceVersion"):
    o["metadata"].pop(k, None)
print(json.dumps(o))
PY
k replace --validate=false -f "$D/g2.json" >/dev/null 2>&1 || fail "replace $GEN_NAME"
k get configmap "$GEN_NAME" -o json >"$D/g3.json" 2>&1
python3 -c 'import json,sys; o=json.load(open(sys.argv[1])); sys.exit(0 if o["metadata"]["uid"]==sys.argv[2] and o["data"]["a"]=="2" else 1)' "$D/g3.json" "$UID0" \
  || { cat "$D/g3.json"; fail "a replace that omitted uid lost it"; }
# From a manifest: `kubectl create namespace` sends protobuf, which this API
# does not read (refused, not stored).
printf '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"stamped"}}' >"$D/ns-in.json"
k create --validate=false -f "$D/ns-in.json" >"$D/ns.out" 2>&1 || { cat "$D/ns.out"; fail "create namespace"; }
k get namespace stamped -o json >"$D/ns.json" 2>&1
python3 -c 'import json,sys; l=json.load(open(sys.argv[1]))["metadata"].get("labels",{}); sys.exit(0 if l.get("kubernetes.io/metadata.name")=="stamped" else 1)' "$D/ns.json" \
  || { cat "$D/ns.json"; fail "namespace lacks kubernetes.io/metadata.name"; }
echo "   $GEN_NAME: uid + creationTimestamp stamped, kept across a replace; namespace labelled with its name"

echo "== 11. finalizers hold a delete: deletionTimestamp, then gone when the last is removed =="
printf '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"held","namespace":"default","finalizers":["example.com/hold"]},"data":{"a":"1"}}' >"$D/held.json"
k create --validate=false -f "$D/held.json" >/dev/null 2>&1 || fail "create held"
k delete configmap held --wait=false >"$D/d.out" 2>&1 || { cat "$D/d.out"; fail "delete held"; }
k get configmap held -o json >"$D/held-now.json" 2>&1 || { cat "$D/held-now.json"; fail "a finalizer did not hold the delete"; }
python3 -c 'import json,sys; m=json.load(open(sys.argv[1]))["metadata"]; sys.exit(0 if m.get("deletionTimestamp") and m.get("finalizers")==["example.com/hold"] else 1)' "$D/held-now.json" \
  || { cat "$D/held-now.json"; fail "held object lacks deletionTimestamp"; }
k patch configmap held --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null 2>&1 || fail "remove the finalizer"
if k get configmap held >"$D/gone.out" 2>&1; then cat "$D/gone.out"; fail "the object outlived its last finalizer"; fi
grep -q NotFound "$D/gone.out" || { cat "$D/gone.out"; fail "want NotFound after the last finalizer"; }
echo "   delete held by a finalizer (deletionTimestamp set); gone when it is removed"

echo "== 12. subresources: kubectl scale, a Scale view, a status write =="
printf '{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"default"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"web"}},"template":{"metadata":{"labels":{"app":"web"}},"spec":{"containers":[{"name":"web","image":"nginx:1"}]}}}}' >"$D/dep.json"
k create --validate=false -f "$D/dep.json" >/dev/null 2>&1 || fail "create deployment"
k scale deployment web --replicas=3 >"$D/sc.out" 2>&1 || { cat "$D/sc.out"; fail "kubectl scale"; }
k get deployment web -o json >"$D/dep-now.json" 2>&1 || fail "get deployment"
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if d["spec"]["replicas"]==3 and d["spec"]["template"]["spec"]["containers"][0]["image"]=="nginx:1" else 1)' "$D/dep-now.json" \
  || { cat "$D/dep-now.json"; fail "scale did not set spec.replicas (or lost the rest)"; }
k patch deployment web --subresource=status --type=merge -p '{"status":{"replicas":2}}' >"$D/st.out" 2>&1 || { cat "$D/st.out"; fail "status patch"; }
k get deployment web --subresource=scale -o json >"$D/scale.json" 2>&1 || { cat "$D/scale.json"; fail "get the scale subresource"; }
python3 - "$D/scale.json" <<'PY' || fail "the Scale view"
import json, sys
s = json.load(open(sys.argv[1]))
assert s["kind"] == "Scale" and s["apiVersion"] == "autoscaling/v1", s
assert s["spec"]["replicas"] == 3 and s["status"]["replicas"] == 2, s
assert s["status"]["selector"] == "app=web" and s["metadata"]["resourceVersion"], s
PY
echo "   kubectl scale 1 -> 3; status.replicas 2 through /status; the Scale view reads both"

echo "== 13. a VALIDATING apply (OpenAPI v3, fieldValidation) and short names =="
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: validated\n  namespace: default\ndata:\n  a: "1"\n' >"$D/v.yaml"
k apply -f "$D/v.yaml" >"$D/v.out" 2>&1 || { cat "$D/v.out"; fail "kubectl apply (validating) create"; }
sed -i 's/a: "1"/a: "2"/' "$D/v.yaml"
k apply -f "$D/v.yaml" >"$D/v.out" 2>&1 || { cat "$D/v.out"; fail "kubectl apply (validating) update"; }
[ "$(k get cm validated -o jsonpath='{.data.a}')" = "2" ] || fail "short name cm / the applied update"
k get deploy web -o name >"$D/sn.out" 2>&1 && grep -qx "deployment.apps/web" "$D/sn.out" || { cat "$D/sn.out"; fail "short name deploy"; }
echo "   kubectl apply validates against /openapi/v3; cm and deploy resolve"

echo "== 14. server dry runs store nothing; kubectl diff =="
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: dry1\n  namespace: default\ndata:\n  a: "1"\n' >"$D/dry.yaml"
k apply --dry-run=server -f "$D/dry.yaml" >"$D/dr.out" 2>&1 || { cat "$D/dr.out"; fail "apply --dry-run=server"; }
grep -q "server dry run" "$D/dr.out" || { cat "$D/dr.out"; fail "kubectl did not report a server dry run"; }
if k get cm dry1 >/dev/null 2>&1; then fail "a dry-run create was stored"; fi
k patch cm validated --dry-run=server -p '{"data":{"a":"9"}}' -o jsonpath='{.data.a}' >"$D/dp.out" 2>&1 || { cat "$D/dp.out"; fail "patch --dry-run=server"; }
[ "$(cat "$D/dp.out")" = "9" ] || { cat "$D/dp.out"; fail "a dry-run patch did not answer the patched object"; }
[ "$(k get cm validated -o jsonpath='{.data.a}')" = "2" ] || fail "a dry-run patch was stored"
k delete cm validated --dry-run=server >/dev/null 2>&1 || fail "delete --dry-run=server"
k get cm validated >/dev/null 2>&1 || fail "a dry-run delete deleted"
sed -i 's/a: "2"/a: "3"/' "$D/v.yaml"
set +e; k diff -f "$D/v.yaml" >"$D/diff.out" 2>&1; rc=$?; set -e
[ "$rc" = 1 ] && grep -q '^+  a: "3"' "$D/diff.out" || { cat "$D/diff.out"; fail "kubectl diff (rc $rc)"; }
echo "   dry-run create/patch/delete answered and not stored; kubectl diff shows a: 2 -> 3"

echo "== 15. server-side Tables: kubectl get prints upstream's columns =="
k get deploy web >"$D/t1.out" 2>&1 || { cat "$D/t1.out"; fail "get deploy"; }
head -1 "$D/t1.out" | grep -Eq '^NAME +READY +UP-TO-DATE +AVAILABLE +AGE$' || { cat "$D/t1.out"; fail "deployment columns"; }
grep -Eq '^web +[0-9]+/3 +0 +0 +[0-9]+[smhd]' "$D/t1.out" || { cat "$D/t1.out"; fail "deployment row"; }
k get pods >"$D/t2.out" 2>&1 || { cat "$D/t2.out"; fail "get pods"; }
head -1 "$D/t2.out" | grep -Eq '^NAME +READY +STATUS +RESTARTS +AGE$' || { cat "$D/t2.out"; fail "pod columns"; }
k get cm validated >"$D/t3.out" 2>&1 || fail "get cm"
head -1 "$D/t3.out" | grep -Eq '^NAME +AGE$' || { cat "$D/t3.out"; fail "a kind with no columns: Name and Age"; }
echo "   deployments: READY UP-TO-DATE AVAILABLE; pods: READY STATUS RESTARTS; others: NAME AGE"

echo "== 16. field ownership: managedFields, apply conflicts, force, removal =="
mf() { k get cm owned -o json --show-managed-fields >"$D/owned.json" 2>&1 || { cat "$D/owned.json"; fail "get owned"; }; }
# Who owns data.<k>, as "<manager>/<operation>" lines.
owners() { python3 -c '
import json, sys
o = json.load(open(sys.argv[1]))
for e in o["metadata"].get("managedFields", []):
    d = e.get("fieldsV1", {}).get("f:data", {})
    if "f:" + sys.argv[2] in d:
        print(e["manager"] + "/" + e["operation"])' "$D/owned.json" "$1"; }
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: owned\ndata:\n  x: "1"\n  w: "1"\n' >"$D/own-a.yaml"
k apply --server-side --field-manager=team-a --validate=false -f "$D/own-a.yaml" >"$D/o1.out" 2>&1 || { cat "$D/o1.out"; fail "apply as team-a"; }
mf; [ "$(owners x)" = "team-a/Apply" ] && [ "$(owners w)" = "team-a/Apply" ] || { cat "$D/owned.json"; fail "team-a does not own data.x and data.w"; }
# Another manager applying a DIFFERENT value is a conflict, named as upstream names it.
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: owned\ndata:\n  x: "2"\n' >"$D/own-b.yaml"
if k apply --server-side --field-manager=team-b --validate=false -f "$D/own-b.yaml" >"$D/o2.out" 2>&1; then
  cat "$D/o2.out"; fail "a conflicting apply was accepted"
fi
grep -q 'conflict with "team-a"' "$D/o2.out" && grep -q '\.data\.x' "$D/o2.out" || { cat "$D/o2.out"; fail "the conflict does not name team-a and .data.x"; }
mf; python3 -c 'import json,sys; sys.exit(0 if json.load(open(sys.argv[1]))["data"]["x"]=="1" else 1)' "$D/owned.json" || fail "a refused apply changed the object"
# The same value is shared, not a conflict; forcing takes the field.
k apply --server-side --field-manager=team-b --force-conflicts --validate=false -f "$D/own-b.yaml" >"$D/o3.out" 2>&1 || { cat "$D/o3.out"; fail "a forced apply"; }
mf; [ "$(owners x)" = "team-b/Apply" ] || { cat "$D/owned.json"; fail "forcing did not move data.x to team-b"; }
# team-a stops applying y: it is removed (no one else owns it); x is team-b's and stays.
printf 'apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: owned\ndata:\n  z: "1"\n' >"$D/own-a2.yaml"
k apply --server-side --field-manager=team-a --validate=false -f "$D/own-a2.yaml" >"$D/o4.out" 2>&1 || { cat "$D/o4.out"; fail "team-a re-applies"; }
mf; python3 -c 'import json,sys; d=json.load(open(sys.argv[1]))["data"]; sys.exit(0 if d=={"x":"2","z":"1"} else 1)' "$D/owned.json" \
  || { cat "$D/owned.json"; fail "a field team-a stopped applying was not removed (or another was)"; }
# An update (a patch) owns what it changed, taken from the applier.
k patch cm owned --field-manager=hand --type=merge -p '{"data":{"z":"9"}}' >"$D/o5.out" 2>&1 || { cat "$D/o5.out"; fail "patch as hand"; }
mf; [ "$(owners z)" = "hand/Update" ] || { cat "$D/owned.json"; fail "the patch does not own data.z"; }
echo "   team-a applies x,w; team-b conflicts on x (409), forces it; team-a drops w (removed); a patch takes z"

echo "== 17. kubectl logs: the pod's recorded output, --tail and --limit-bytes =="
k logs logger >"$D/l1.out" 2>&1 || { cat "$D/l1.out"; fail "kubectl logs"; }
[ "$(cat "$D/l1.out")" = "$(printf 'one\ntwo\nthree')" ] || { cat -A "$D/l1.out"; fail "kubectl logs: not the recorded output"; }
k logs logger --tail=2 >"$D/l2.out" 2>&1 || { cat "$D/l2.out"; fail "kubectl logs --tail"; }
[ "$(cat "$D/l2.out")" = "$(printf 'two\nthree')" ] || { cat -A "$D/l2.out"; fail "kubectl logs --tail=2"; }
k logs logger --limit-bytes=5 >"$D/l3.out" 2>&1 || { cat "$D/l3.out"; fail "kubectl logs --limit-bytes"; }
[ "$(cat "$D/l3.out")" = "$(printf 'one\nt')" ] || { cat -A "$D/l3.out"; fail "kubectl logs --limit-bytes=5"; }
k logs web-0 >"$D/l4.out" 2>&1 || { cat "$D/l4.out"; fail "kubectl logs of a pod that never ran"; }
[ ! -s "$D/l4.out" ] || { cat -A "$D/l4.out"; fail "a pod that never ran has output"; }
echo "   logs: whole tail; --tail=2; --limit-bytes=5; a pod that never ran: empty"

echo "== E2E green: kubectl patch (strategic, merge, json), label, annotate, server-side apply with field ownership, the create stamps, finalizers, subresources, validating apply, server dry runs, Tables and logs =="
