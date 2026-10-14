#!/usr/bin/env bash
# Live E2E: kubectl's typed generators against the control plane.
#
# `kubectl create configmap|secret|namespace|serviceaccount|deployment|job|
# cronjob|role|clusterrole|rolebinding|clusterrolebinding|service|quota|
# priorityclass|poddisruptionbudget|ingress|token` send their object as
# `application/vnd.kubernetes.protobuf`, and kubectl does not fall back to
# JSON. The API's ingress (http_chain) transcodes such a body to the JSON
# object it stands for (_shared/k8s_proto.rs; its schema generated from
# Kubernetes' generated.proto and Go types by
# scripts/support/k8s_proto_schema.py), and the chain creates it as it
# creates any JSON object. Each generator runs as an operator runs it, and
# each object reads back with what the generator was given (which needs the
# groups' discovery: batch, rbac, scheduling, policy, networking).
#
# `kubectl create token` is a TokenRequest on serviceaccounts/<name>/token:
# the ServiceAccount is read, the plane's one issuer signs (tk_api ->
# tk_mint, apiplane_token.uproc), and the answer carries the token and its
# expiry. The token authenticates against this same API; a missing
# ServiceAccount and a lifetime under 10 minutes are refused as upstream
# refuses them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-kcreate-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
stop() { if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; fi; }
cleanup() { stop; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. the control plane, as packaged, joined by its operator =="
NANOCLOUD_PEER_TRUST=authority nc_controlplane_graph "$STORE" "$D/graph.yaml" \
  || fail "the control plane did not render"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"
SEAL="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
mkdir -p "$D/vault"
env -C "$STORE" FLUXOR_SEAL_KEY="$SEAL" FLUXOR_VAULT_DIR="$D/vault" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
JOIN=""
for _ in $(seq 240); do
  JOIN="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/run.log" | tail -1 || true)"
  [ -n "$JOIN" ] && break
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the runtime exited"
  sleep 0.25
done
[ -n "$JOIN" ] || fail "no join secret announced in 60 s"
SECRET="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
CA_SHA="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
KC="$D/kubeconfig"
bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$KC" >/dev/null || fail "the join did not complete"
k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }
k get --raw /version >/dev/null || fail "kubectl could not reach the API"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout "$D/t.key" -out "$D/t.pem" \
  -days 1 -subj /CN=t 2>/dev/null
echo "   joined"

echo "== 2. every typed generator, as protobuf =="
gen() { # gen <description> <kubectl args…>
  local what="$1"; shift
  k "$@" >"$D/gen.out" 2>&1 || fail "kubectl $*: $(cat "$D/gen.out")"
  echo "   $what"
}
gen "configmap"           create configmap cm1 --from-literal=a=b --from-literal=c=d
gen "secret generic"      create secret generic s1 --from-literal=user=admin
gen "secret tls"          create secret tls tls1 --cert="$D/t.pem" --key="$D/t.key"
gen "secret docker"       create secret docker-registry reg1 --docker-server=r.example --docker-username=u --docker-password=p
gen "namespace"           create namespace ns1
gen "serviceaccount"      create serviceaccount sa1
gen "deployment"          create deployment web --image=nginx:1.27 --replicas=0 --port=80
gen "job"                 create job job1 --image=busybox -- echo hi
gen "cronjob"             create cronjob cj1 --image=busybox --schedule="*/5 * * * *" -- echo hi
gen "role"                create role r1 --verb=get,list --resource=pods,configmaps --resource-name=a
gen "clusterrole"         create clusterrole cr1 --verb=get --resource=nodes
gen "rolebinding"         create rolebinding rb1 --role=r1 --serviceaccount=default:sa1 --user=alice
gen "clusterrolebinding"  create clusterrolebinding crb1 --clusterrole=cr1 --group=g1
gen "service clusterip"   create service clusterip svc1 --tcp=80:8080
gen "service nodeport"    create service nodeport svc2 --tcp=443:8443 --node-port=30443
gen "quota"               create quota q1 --hard=cpu=1,memory=1G,pods=2
gen "priorityclass"       create priorityclass pc1 --value=1000 --preemption-policy=Never
gen "poddisruptionbudget" create poddisruptionbudget pdb1 --selector=app=web --max-unavailable=20%
gen "ingress"             create ingress ing1 --rule="foo.com/bar=svc1:80,tls=my-cert"

echo "== 3. each object reads back with what its generator was given =="
check() { # check <kubectl get args…> -- <python expression over o>
  local args=()
  while [ "$1" != "--" ]; do args+=("$1"); shift; done
  shift
  k get "${args[@]}" -o json >"$D/obj.json" 2>&1 || fail "kubectl get ${args[*]}: $(cat "$D/obj.json")"
  python3 -c "import json,sys; o=json.load(open(sys.argv[1])); sys.exit(0 if ($1) else 1)" "$D/obj.json" \
    || fail "${args[*]}: ($1) does not hold: $(head -c 600 "$D/obj.json")"
}
check configmap cm1 -- 'o["data"] == {"a": "b", "c": "d"}'
check secret s1 -- 'o["data"]["user"] == "YWRtaW4=" and "type" not in o'
check secret tls1 -- 'o["type"] == "kubernetes.io/tls" and o["data"]["tls.crt"]'
check secret reg1 -- 'o["type"] == "kubernetes.io/dockerconfigjson"'
check namespace ns1 -- 'o["metadata"]["name"] == "ns1"'
check serviceaccount sa1 -- 'o["metadata"]["name"] == "sa1"'
check deployment web -- 'o["spec"]["replicas"] == 0 and o["spec"]["template"]["spec"]["containers"][0]["image"] == "nginx:1.27"'
check job job1 -- 'o["spec"]["template"]["spec"]["containers"][0]["command"] == ["echo", "hi"]'
check cronjob cj1 -- 'o["spec"]["schedule"] == "*/5 * * * *"'
check role r1 -- 'o["rules"][0]["verbs"] == ["get", "list"] and o["rules"][0]["resourceNames"] == ["a"]'
check clusterrole cr1 -- 'o["rules"][0]["resources"] == ["nodes"]'
check rolebinding rb1 -- 'o["roleRef"]["name"] == "r1" and len(o["subjects"]) == 2'
check clusterrolebinding crb1 -- 'o["subjects"][0]["kind"] == "Group"'
check service svc1 -- 'o["spec"]["ports"][0]["targetPort"] == 8080'
check service svc2 -- 'o["spec"]["type"] == "NodePort" and o["spec"]["ports"][0]["nodePort"] == 30443'
check resourcequota q1 -- 'o["spec"]["hard"]["memory"] == "1G"'
check priorityclass pc1 -- 'o["value"] == 1000 and o["preemptionPolicy"] == "Never"'
check poddisruptionbudget pdb1 -- 'o["spec"]["maxUnavailable"] == "20%"'
check ingress ing1 -- 'o["spec"]["rules"][0]["host"] == "foo.com" and o["spec"]["tls"][0]["secretName"] == "my-cert"'
echo "   all read back"

echo "== 4. kubectl create token: a TokenRequest as protobuf, answered with a token =="
TOK="$(k create token sa1 --duration=10m 2>"$D/tok.err")" || fail "create token: $(cat "$D/tok.err")"
python3 - "$TOK" <<'PY' || fail "the token is not sa1's: $TOK"
import base64, json, sys
p = sys.argv[1].split(".")[1]
c = json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))
assert c["sub"] == "system:serviceaccount:default:sa1", c
assert 500 <= c["exp"] - c["iat"] <= 700, c
PY
echo "   a token for system:serviceaccount:default:sa1, ~10 min"
# Signed by the plane's one issuer, so the plane verifies it: sa1 is
# authenticated (no grants, so Forbidden — not Unauthorized).
CODE="$(curl -sk -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $TOK" \
  "https://127.0.0.1:$PORT/api/v1/namespaces/default/configmaps")"
[ "$CODE" = 403 ] || fail "the minted token was not accepted as sa1 (want 403, got $CODE)"
echo "   the token authenticates as sa1 (403 without grants, not 401)"
k create token nobody >"$D/tok.err" 2>&1 && fail "a token for a ServiceAccount that does not exist"
grep -q "not found" "$D/tok.err" || fail "missing ServiceAccount: $(cat "$D/tok.err")"
k create token sa1 --duration=5m >"$D/tok.err" 2>&1 && fail "a token under the 10-minute floor"
grep -q "less than 10 minutes" "$D/tok.err" || fail "short duration: $(cat "$D/tok.err")"
echo "   refused: a missing ServiceAccount (404), a duration under 10 minutes (422)"

echo "== E2E green: kubectl's typed generators create through the API, their protobuf transcoded to JSON =="
