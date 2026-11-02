#!/usr/bin/env bash
# Live E2E: CustomResourceDefinitions — cluster-scoped and namespaced custom
# kinds served at their scope, and discovered.
#
# The SHIPPED API graph (fluxor-apiplane.yaml), one boot. A CRD becomes rows
# (crd.plan): its scope (/crd-scope/<plural>.<group>), its kinds per served
# version (/discovery-res/…) and its group-versions (/discovery-groups/…).
# The request path keys a custom kind `/<plural>.<group>/[<ns>/]<name>`
# (kube.plan), lattice's dialect takes its id from the key's depth (`?=`),
# and the scope stage (apiplane_scope.plan) answers 404 for a kind addressed
# at the scope it is not, or one no CRD defines.
#
# Proves: a cluster-scoped custom kind is created, read and listed (it was
# 422 before); a namespaced one too; each is 404 at the other scope; an
# undefined kind is 404; a custom group's plural is not a built-in's key
# (a `secrets` in another group is not the core Secret); built-in kinds are
# scoped the same way; `/apis` lists the CRD's group beside every built-in
# one and `/apis/<group>/<version>` its kinds (the shapes client-go's
# discovery reads); deleting a CRD withdraws its kind and its discovery.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GRAPH="$ROOT/packaging/debian/fluxor-apiplane.yaml"
PORT=7444

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-crd-XXXXXX)"
nc_lattice_prepare "$D"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <hex-value>
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

echo "== 1. seed an operator identity and one core Secret; build the shipped graph =="
python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
ISSUER, KID = b"https://kubernetes.default.svc", b"sa-1"
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1) + bytes([1]) + bytes([0x01])
        + struct.pack("<I", 1) + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex", "w").write(binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
hdr = b64u(json.dumps({"alg": "ES256", "typ": "JWT", "kid": KID.decode()}, separators=(",", ":")).encode())
now = int(time.time())
claims = {"iss": ISSUER.decode(), "sub": "system:serviceaccount:default:admin", "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = hdr + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/token", "w").write((signing + b"." + b64u(r.to_bytes(32, "big") + s.to_bytes(32, "big"))).decode())
PY
store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
. "$ROOT/scripts/support/rbac.sh"
rbac_clusterrole "$D/store.log" admin "*" "*"
rbac_crb "$D/store.log" admins admin ServiceAccount admin default
put_text "/secrets/default/db" '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"db","namespace":"default"},"data":{"p":"c2VjcmV0"}}'
grep -q '?=metadata.namespace,metadata.name"' "$GRAPH" || fail "the dialect does not take an unnamed kind's id by key depth"
cp "$GRAPH" "$D/graph.yaml"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"
rbac_settle "$FLUXOR_RUNTIME" "$D/config.bin" "$D/modules.bin" "$D" present '/rbac-g/system:serviceaccount:default:admin/*|*|*|*|*'

env -C "$D" RUST_LOG=warn "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
for _ in $(seq 100); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the runtime exited"
  sleep 0.1
done
B="http://127.0.0.1:$PORT"
api() { # api <method> <path> [body] -> "<code>|<body>"
  local out
  out="$(curl -s --max-time 8 -X "$1" -H "Authorization: Bearer $(cat "$D/token")" \
    -H 'Content-Type: application/json' ${3:+--data-binary "$3"} -w '\n%{http_code}' "$B$2" || true)"
  printf '%s|%s' "${out##*$'\n'}" "${out%$'\n'*}"
}
want() { # want <code> <label> <api args…>
  local code="$1" label="$2" r; shift 2
  r="$(api "$@")"
  [ "${r%%|*}" = "$code" ] || fail "$label: want $code, got ${r:0:400}"
  echo "   $label -> $code"
}
until_has() { # until_has <path> <text> [absent]
  local r=""
  for _ in $(seq 60); do
    r="$(api GET "$1")"
    if [ "${3:-}" = absent ]; then case "$r" in *"$2"*) ;; *) return 0 ;; esac
    else case "$r" in 200\|*"$2"*) return 0 ;; esac; fi
    sleep 0.5
  done
  echo "   last: ${r:0:400}"; return 1
}

echo "== 2. before any CRD: the kind is not served =="
want 404 "POST /apis/example.com/v1/widgets (no CRD)" POST /apis/example.com/v1/widgets \
  '{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w0"}}'
want 404 "GET  /apis/example.com/v1 (no CRD)" GET /apis/example.com/v1

echo "== 3. two CRDs: Widget (Cluster), Gadget (Namespaced) =="
crd() { # crd <plural> <kind> <scope> [shortName]
  local short=""; [ -n "${4:-}" ] && short=",\"shortNames\":[\"$4\"]"
  want 201 "POST CRD $1.example.com ($3)" POST /apis/apiextensions.k8s.io/v1/customresourcedefinitions \
    "{\"apiVersion\":\"apiextensions.k8s.io/v1\",\"kind\":\"CustomResourceDefinition\",\"metadata\":{\"name\":\"$1.example.com\"},\"spec\":{\"group\":\"example.com\",\"scope\":\"$3\",\"names\":{\"plural\":\"$1\",\"singular\":\"${2,,}\",\"kind\":\"$2\"$short},\"versions\":[{\"name\":\"v1\",\"served\":true,\"storage\":true,\"schema\":{\"openAPIV3Schema\":{\"type\":\"object\",\"x-kubernetes-preserve-unknown-fields\":true}}}]}}"
}
crd widgets Widget Cluster wd
crd gadgets Gadget Namespaced

echo "== 4. discovery: /apis/example.com/v1 lists both kinds, /apis the group beside the built-ins =="
until_has /apis/example.com/v1 '"name":"gadgets"' || fail "gadgets not discovered"
until_has /apis/example.com/v1 '"name":"widgets"' || fail "widgets not discovered"
R="$(api GET /apis/example.com/v1)"
python3 - "${R#*|}" <<'PY' || fail "the group-version document: ${R:0:600}"
import json, sys
d = json.loads(sys.argv[1])
# groupVersion is the path's, stated by the LIST's envelope.
assert d["kind"] == "APIResourceList" and d["groupVersion"] == "example.com/v1", d
r = {x["name"]: x for x in d["resources"]}
assert r["widgets"]["namespaced"] is False and r["widgets"]["kind"] == "Widget" and r["widgets"]["shortNames"] == ["wd"], r
assert r["gadgets"]["namespaced"] is True and r["gadgets"]["singularName"] == "gadget", r
assert "list" in r["widgets"]["verbs"] and "create" in r["gadgets"]["verbs"], r
PY
echo "   /apis/example.com/v1: widgets (namespaced=false, shortNames [wd]), gadgets (namespaced=true)"
until_has /apis '"name":"example.com"' || fail "/apis does not list example.com"
R="$(api GET /apis)"
python3 - "${R#*|}" <<'PY' || fail "/apis: ${R:0:800}"
import json, sys
d = json.loads(sys.argv[1])
names = [g["name"] for g in d["groups"]]
for g in ("apps", "batch", "rbac.authorization.k8s.io", "networking.k8s.io", "policy", "nanocloud.io", "example.com"):
    assert g in names, (g, names)
assert names.count("example.com") == 1, names
e = [g for g in d["groups"] if g["name"] == "example.com"][0]
assert e["preferredVersion"] == {"groupVersion": "example.com/v1", "version": "v1"}, e
PY
echo "   /apis: every built-in group, and example.com once (preferred v1)"

echo "== 4b. a group of several versions and CRDs: ONE APIGroup, every version, upstream's preferred; subresources listed =="
mcrd() { # mcrd <plural> <kind> <versions-json>
  want 201 "POST CRD $1.multi.example.org" POST /apis/apiextensions.k8s.io/v1/customresourcedefinitions \
    "{\"apiVersion\":\"apiextensions.k8s.io/v1\",\"kind\":\"CustomResourceDefinition\",\"metadata\":{\"name\":\"$1.multi.example.org\"},\"spec\":{\"group\":\"multi.example.org\",\"scope\":\"Namespaced\",\"names\":{\"plural\":\"$1\",\"singular\":\"${2,,}\",\"kind\":\"$2\"},\"versions\":$3}}"
}
SCH='"schema":{"openAPIV3Schema":{"type":"object","x-kubernetes-preserve-unknown-fields":true}}'
mcrd things Thing "[{\"name\":\"v1beta1\",\"served\":true,\"storage\":false,$SCH,\"subresources\":{\"status\":{}}},{\"name\":\"v1\",\"served\":true,\"storage\":true,$SCH,\"subresources\":{\"status\":{},\"scale\":{\"specReplicasPath\":\".spec.replicas\",\"statusReplicasPath\":\".status.replicas\"}}},{\"name\":\"v2alpha1\",\"served\":true,\"storage\":false,$SCH},{\"name\":\"v0\",\"served\":false,\"storage\":false,$SCH}]"
mcrd others Other "[{\"name\":\"v1\",\"served\":true,\"storage\":true,$SCH},{\"name\":\"v10\",\"served\":true,\"storage\":false,$SCH}]"
groups_ok() { # groups_ok <want versions, comma-joined, in order> <want preferred>
  python3 - "$1" "$2" "$(api GET /apis)" <<'PY2'
import json, sys
want, pref, r = sys.argv[1].split(","), sys.argv[2], sys.argv[3]
d = json.loads(r.split("|", 1)[1])
gs = [g for g in d["groups"] if g["name"] == "multi.example.org"]
assert len(gs) == 1, ("the group must be listed once", [g["name"] for g in d["groups"]])
vs = [v["version"] for v in gs[0]["versions"]]
assert vs == want, (vs, want)
assert all(v["groupVersion"] == "multi.example.org/" + v["version"] for v in gs[0]["versions"]), gs
assert gs[0]["preferredVersion"] == {"groupVersion": "multi.example.org/" + pref, "version": pref}, gs
PY2
}
ok=0
for _ in $(seq 60); do groups_ok "v10,v1,v1beta1,v2alpha1" v10 2>/dev/null && { ok=1; break; }; sleep 0.5; done
[ "$ok" = 1 ] || { groups_ok "v10,v1,v1beta1,v2alpha1" v10 || true; fail "/apis: multi.example.org not one group of v10,v1,v1beta1,v2alpha1 (preferred v10)"; }
echo "   /apis: multi.example.org ONCE, versions v10, v1, v1beta1, v2alpha1 (GA newest first, then beta, alpha; v0 unserved), preferred v10"
until_has /apis/multi.example.org/v1 '"name":"things/scale"' || fail "things/scale not discovered"
R="$(api GET /apis/multi.example.org/v1)"
python3 - "${R#*|}" <<'PY2' || fail "/apis/multi.example.org/v1: ${R:0:900}"
import json, sys
d = json.loads(sys.argv[1])
assert d["groupVersion"] == "multi.example.org/v1", d
r = {x["name"]: x for x in d["resources"]}
assert set(r) == {"things", "things/status", "things/scale", "others"}, sorted(r)
assert r["things/status"]["kind"] == "Thing" and r["things/status"]["verbs"] == ["get", "patch", "update"] and r["things/status"]["namespaced"] is True, r
assert r["things/scale"]["kind"] == "Scale" and r["things/scale"]["group"] == "autoscaling" and r["things/scale"]["version"] == "v1", r
PY2
R="$(api GET /apis/multi.example.org/v1beta1)"
python3 - "${R#*|}" <<'PY2' || fail "/apis/multi.example.org/v1beta1: ${R:0:900}"
import json, sys
d = json.loads(sys.argv[1])
assert d["groupVersion"] == "multi.example.org/v1beta1", d
assert sorted(x["name"] for x in d["resources"]) == ["things", "things/status"], d
PY2
want 404 "GET  /apis/multi.example.org/v0 (not served)" GET /apis/multi.example.org/v0
echo "   /apis/multi.example.org/v1: things, things/status, things/scale (autoscaling/v1 Scale), others; v1beta1: things, things/status"
want 200 "DELETE CRD others.multi.example.org" DELETE /apis/apiextensions.k8s.io/v1/customresourcedefinitions/others.multi.example.org
ok=0
for _ in $(seq 60); do groups_ok "v1,v1beta1,v2alpha1" v1 2>/dev/null && { ok=1; break; }; sleep 0.5; done
[ "$ok" = 1 ] || { groups_ok "v1,v1beta1,v2alpha1" v1 || true; fail "/apis: v10 still listed after the only CRD serving it went"; }
want 200 "DELETE CRD things.multi.example.org" DELETE /apis/apiextensions.k8s.io/v1/customresourcedefinitions/things.multi.example.org
until_has /apis '"name":"multi.example.org"' absent || fail "/apis still lists multi.example.org"
echo "   a version no CRD serves leaves the group (preferred back to v1); the last CRD takes the group"

echo "== 4c. a CRD past one record (a real openAPI schema, several KiB) is served; a cluster-scoped kind's /status takes a long name =="
# crd_src/crd_vers read a CRD whole (store_scan): a document past one record
# was refused by a plain store_source ("past the buffer and no heap holds
# it"), so its kind was never scoped and every request for it was 404.
BIG="$(python3 - <<'PY2'
import json
props = {f"field{i:03d}": {"type": "string", "description": "a property of a real schema, described at length " + "x" * 40} for i in range(90)}
sch = {"openAPIV3Schema": {"type": "object", "properties": {"spec": {"type": "object", "properties": props},
       "status": {"type": "object", "x-kubernetes-preserve-unknown-fields": True}}}}
doc = {"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
       "metadata": {"name": "failuredomainoperations.zones.ops.example.net", "annotations": {"example.net/fence": "1/1/0123456789abcdef0123456789abcdef"}},
       "spec": {"group": "zones.ops.example.net", "scope": "Cluster",
                "names": {"plural": "failuredomainoperations", "singular": "failuredomainoperation", "kind": "FailureDomainOperation"},
                "versions": [{"name": "v1", "served": True, "storage": True, "schema": sch, "subresources": {"status": {}}}]}}
print(json.dumps(doc, separators=(",", ":")))
PY2
)"
[ "${#BIG}" -gt 9000 ] || fail "the big CRD is only ${#BIG} bytes"
want 201 "POST CRD failuredomainoperations.zones.ops.example.net (${#BIG} B)" POST /apis/apiextensions.k8s.io/v1/customresourcedefinitions "$BIG"
until_has /apis/zones.ops.example.net/v1 '"name":"failuredomainoperations/status"' || fail "the big CRD's status subresource is not discovered"
until_has /apis/zones.ops.example.net/v1 '"name":"failuredomainoperations",' || fail "the big CRD's kind is not discovered"
ok=""
for _ in $(seq 60); do
  r="$(api POST /apis/zones.ops.example.net/v1/failuredomainoperations '{"apiVersion":"zones.ops.example.net/v1","kind":"FailureDomainOperation","metadata":{"name":"drain-zone-a-rack-0042"},"spec":{"field001":"a"}}')"
  [ "${r%%|*}" = 201 ] && { ok=1; break; }
  sleep 0.5
done
[ -n "$ok" ] || fail "the big CRD's kind is not served: ${r:0:300}"
echo "   POST /apis/zones.ops.example.net/v1/failuredomainoperations (22-byte name) -> 201"
# A cluster-scoped custom kind's /status with a name of 10 bytes and more:
# kube.plan route built its key and membership test by concatenation, past the
# decision's string arena, and failed closed (no answer).
for n in drain-zone-a-rack-0042 abcdefghij; do
  [ "$n" = abcdefghij ] && want 201 "POST failuredomainoperations/abcdefghij" POST /apis/zones.ops.example.net/v1/failuredomainoperations \
    '{"apiVersion":"zones.ops.example.net/v1","kind":"FailureDomainOperation","metadata":{"name":"abcdefghij"}}'
  out="$(curl -s --max-time 8 -X PATCH -H "Authorization: Bearer $(cat "$D/token")" -H 'Content-Type: application/merge-patch+json' \
    -A 'probe-client-with-a-long-agent/1.0' --data-binary '{"status":{"phase":"Draining"}}' -w '\n%{http_code}' \
    "$B/apis/zones.ops.example.net/v1/failuredomainoperations/$n/status?fieldManager=a-field-manager-of-some-length" || true)"
  [ "${out##*$'\n'}" = 200 ] || fail "PATCH failuredomainoperations/$n/status: want 200, got ${out:0:300}"
  case "$out" in *'"phase":"Draining"'*) ;; *) fail "PATCH $n/status did not answer the status: ${out:0:300}" ;; esac
  echo "   PATCH /apis/zones.ops.example.net/v1/failuredomainoperations/$n/status (${#n}-byte name) -> 200"
done
R="$(api GET /apis/zones.ops.example.net/v1/failuredomainoperations/drain-zone-a-rack-0042)"
case "$R" in 200\|*'"phase":"Draining"'*'"field001":"a"'*|200\|*'"field001":"a"'*'"phase":"Draining"'*) ;; *) fail "GET after the status PATCH: ${R:0:300}" ;; esac
grep -a "past the buffer and no heap holds it" "$D/run.log" && fail "a CRD was refused as past the buffer"
grep -a "scratch arena overflow\|fused decision failed closed" "$D/run.log" && fail "a decision failed closed (its string arena)"
want 200 "DELETE CRD failuredomainoperations.zones.ops.example.net" DELETE /apis/apiextensions.k8s.io/v1/customresourcedefinitions/failuredomainoperations.zones.ops.example.net
until_has /apis '"name":"zones.ops.example.net"' absent || fail "/apis still lists zones.ops.example.net"

echo "== 5. the cluster-scoped kind: created, read, listed; 404 on a namespaced path =="
W='{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w1"},"spec":{"size":3}}'
want 201 "POST /apis/example.com/v1/widgets (w1)" POST /apis/example.com/v1/widgets "$W"
R="$(api GET /apis/example.com/v1/widgets/w1)"
case "$R" in 200\|*'"name":"w1"'*'"size":3'*) echo "   GET  /apis/example.com/v1/widgets/w1 -> 200" ;; *) fail "GET w1: ${R:0:300}" ;; esac
R="$(api GET /apis/example.com/v1/widgets)"
case "$R" in 200\|*'"name":"w1"'*) echo "   LIST /apis/example.com/v1/widgets -> 200 with w1" ;; *) fail "LIST widgets: ${R:0:300}" ;; esac
want 404 "POST /apis/example.com/v1/namespaces/default/widgets" POST /apis/example.com/v1/namespaces/default/widgets \
  '{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w2","namespace":"default"}}'
want 404 "GET  /apis/example.com/v1/namespaces/default/widgets/w1" GET /apis/example.com/v1/namespaces/default/widgets/w1
want 200 "PUT  /apis/example.com/v1/widgets/w1" PUT /apis/example.com/v1/widgets/w1 \
  '{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w1"},"spec":{"size":4}}'

echo "== 6. the namespaced kind: created, read; 404 named or written on the cluster path =="
G='{"apiVersion":"example.com/v1","kind":"Gadget","metadata":{"name":"g1","namespace":"default"},"spec":{"a":1}}'
want 201 "POST /apis/example.com/v1/namespaces/default/gadgets (g1)" POST /apis/example.com/v1/namespaces/default/gadgets "$G"
want 200 "GET  /apis/example.com/v1/namespaces/default/gadgets/g1" GET /apis/example.com/v1/namespaces/default/gadgets/g1
want 404 "POST /apis/example.com/v1/gadgets" POST /apis/example.com/v1/gadgets "$G"
want 404 "GET  /apis/example.com/v1/gadgets/g1" GET /apis/example.com/v1/gadgets/g1
R="$(api GET /apis/example.com/v1/gadgets)"
case "$R" in 200\|*'"name":"g1"'*) echo "   LIST /apis/example.com/v1/gadgets (every namespace) -> 200 with g1" ;; *) fail "LIST gadgets: ${R:0:300}" ;; esac

echo "== 7. an undefined kind, and a custom group's plural that names a core kind =="
want 404 "POST /apis/example.com/v1/things" POST /apis/example.com/v1/things '{"metadata":{"name":"t"}}'
want 200 "GET  /api/v1/namespaces/default/secrets/db (core)" GET /api/v1/namespaces/default/secrets/db
want 404 "GET  /apis/evil.example/v1/namespaces/default/secrets/db" GET /apis/evil.example/v1/namespaces/default/secrets/db

echo "== 8. built-in kinds are scoped too =="
want 404 "GET  /api/v1/namespaces/default/nodes/n1" GET /api/v1/namespaces/default/nodes/n1
want 404 "POST /api/v1/pods (cluster path)" POST /api/v1/pods '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"default"}}'
want 200 "GET  /api/v1/pods (every namespace)" GET /api/v1/pods

echo "== 9. deleting a CRD withdraws its kind and its discovery =="
want 200 "DELETE CRD gadgets.example.com" DELETE /apis/apiextensions.k8s.io/v1/customresourcedefinitions/gadgets.example.com
until_has /apis/example.com/v1 '"name":"gadgets"' absent || fail "gadgets still discovered"
want 404 "GET  /apis/example.com/v1/namespaces/default/gadgets/g1 (CRD gone)" GET /apis/example.com/v1/namespaces/default/gadgets/g1
want 200 "GET  /apis/example.com/v1/widgets/w1 (its CRD stays)" GET /apis/example.com/v1/widgets/w1
want 200 "DELETE CRD widgets.example.com" DELETE /apis/apiextensions.k8s.io/v1/customresourcedefinitions/widgets.example.com
until_has /apis '"name":"example.com"' absent || fail "/apis still lists example.com"
want 404 "GET  /apis/example.com/v1 (no CRD left)" GET /apis/example.com/v1
want 404 "GET  /apis/example.com/v1/widgets/w1 (CRD gone)" GET /apis/example.com/v1/widgets/w1

echo "== E2E green: custom kinds are served at their scope, and discovered =="
