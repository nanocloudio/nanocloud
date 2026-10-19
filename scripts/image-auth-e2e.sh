#!/usr/bin/env bash
# Live E2E: imagePullSecrets (image_auth.uproc, http_fetch's `pull_auth`).
#
# A private repository on a registry that wants Basic credentials, and the
# same image on ANOTHER registry (a reference naming it is asked of it, by
# its name) that wants a Bearer token from its realm. Credentials are
# `kubernetes.io/dockerconfigjson` Secrets; a Pod offers its own
# imagePullSecrets, or its ServiceAccount's when it states none:
#
#   1. a Pod with the Secret pulls; one without gets ErrImagePull, 401
#      ("the registry refused the credentials (status 401)") — though the
#      image was just pulled for the first: its pull is its own
#      (imagePullPolicy Always)
#   2. a Pod stating no Secret pulls through its ServiceAccount's
#   3. a Pod naming a wrong Secret first, then the right one: each offered in
#      turn, as a kubelet's keyring does
#   4. another registry, named by the reference: asked by its own name, its
#      Bearer realm answered with the Secret's entry for it
#   5. the shared pull (IfNotPresent): a Pod without credentials backs off on
#      401; a Pod WITH them asking for the same image is not made to wait out
#      that delay — its record clears the backoff — and once the image is on
#      the node the first Pod runs it too: upstream's default (no
#      `ensureImagePullCredentials`), an image present is not authorized again
#
# PASS = all five.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
PORT=5397        # Basic
PORT2=5398       # Bearer, through
REALM=5399       # its realm

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in http_fetch store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgauth-XXXXXX)"
fail() { echo "FAIL: $1"; grep -a 'ERROR' "$D/run.log" 2>/dev/null | tail -5 || true; exit 1; }
. "$ROOT/scripts/support/image-plane.sh"
cleanup() { ip_cleanup; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

echo "== 1. a private image on two registries (Basic on :$PORT, Bearer on :$PORT2) =="
mkdir -p "$D/reg" "$D/blobs"
ip_layer "$D/l.tgz" "etc/secret=private"
M="$(ip_image private/app 1 arm64 "$D/l.tgz")"
ip_image private/app 2 arm64 "$D/l.tgz" >/dev/null
ip_registry "$PORT" "$D/reg.log" --private private/ --auth basic --user puller:s3cret
ip_registry "$PORT2" "$D/reg2.log" --private private/ --auth bearer --user puller:s3cret --realm-port "$REALM"
echo "   private/app:1 -> sha256:${M:0:12}…"

ip_graph "$D/graph.yaml" "localhost:$PORT" "$D/blobs" '"kubernetes.io/arch":"arm64","kubernetes.io/os":"linux"'
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null

# The Secrets and a ServiceAccount, as the API stores them.
python3 - "$PORT" "$PORT2" >"$D/seed.txt" <<'PY'
import base64, json, sys
p1, p2 = sys.argv[1:3]
b = lambda s: base64.b64encode(s.encode()).decode()
def secret(name, auths):
    return json.dumps({"apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/dockerconfigjson",
                       "metadata": {"name": name, "namespace": "default"},
                       "data": {".dockerconfigjson": b(json.dumps({"auths": auths}))}})
print("/secrets/default/regcred\t" + secret("regcred", {
    f"localhost:{p1}": {"auth": b("puller:s3cret")},
    f"https://localhost:{p2}/v1/": {"username": "puller", "password": "s3cret"}}))
print("/secrets/default/wrong\t" + secret("wrong", {f"localhost:{p1}": {"auth": b("puller:nope")}}))
print("/serviceaccounts/default/puller\t" + json.dumps(
    {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "puller", "namespace": "default"},
     "imagePullSecrets": [{"name": "regcred"}]}))
PY
while IFS=$'\t' read -r k v; do ip_put "$k" "$v"; done <"$D/seed.txt"
# pod <name> <uid> <image> <pull> [secrets csv] [serviceAccountName]: the Pod
# object and the node's spec for it (into the host log: the node stopped).
pod() {
  local js
  js="$(python3 -c '
import json, sys
name, uid, image, pull, secrets, sa = sys.argv[1:7]
spec = {"containers": [{"name": "c", "image": image, "imagePullPolicy": pull}]}
if secrets: spec["imagePullSecrets"] = [{"name": s} for s in secrets.split(",")]
if sa: spec["serviceAccountName"] = sa
print(json.dumps({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": "default", "uid": uid}, "spec": spec}))
' "$1" "$2" "$3" "$4" "${5:-}" "${6:-}")"
  ip_put "/pods/default/$1" "$js"
  ip_put "/pod-specs/$2" "image=$3;pull=$4;pod=default/$1;desired=running"
}
event() { ip_get "/events/default/$1.$2"; }
msg() { python3 -c 'import json,sys; e=json.loads(sys.argv[1]); print("%s|%s" % (e["reason"], e["message"]))' "$1"; }
digest() { ip_get "/image-digests/$1" | sed -n 's/^digest=//p'; }

echo "== 2. with the Secret: pulled; without: ErrImagePull, 401; through a ServiceAccount; a wrong Secret first =="
pod with u-with "private/app:1" Always regcred
pod without u-without "private/app:1" Always
pod viasa u-sa "private/app:1" Always "" puller
pod both u-both "private/app:1" Always wrong,regcred
ip_boot
ip_wait 40 eval '[ "$(digest u-with)" = "$M" ] && [ "$(digest u-sa)" = "$M" ] && [ "$(digest u-both)" = "$M" ]' \
  || fail "pulls: with='$(ip_get /image-digests/u-with)' sa='$(ip_get /image-digests/u-sa)' both='$(ip_get /image-digests/u-both)'"
[ "$(ip_get /image-pull-auth/u-with)" = "rk=u-with;s=default/regcred" ] || fail "u-with's record: '$(ip_get /image-pull-auth/u-with)'"
[ "$(ip_get /image-pull-auth/u-sa)" = "rk=u-sa;s=default/regcred" ] || fail "the ServiceAccount's Secrets: '$(ip_get /image-pull-auth/u-sa)'"
echo "   with the Secret, through the ServiceAccount, and past a wrong Secret: sha256:${M:0:12}…"
ip_wait 30 eval '[ -n "$(event u-without ErrImagePull)" ]' || fail "no ErrImagePull for the Pod without credentials: '$(ip_get /image-pull-failed/u-without)'"
E="$(msg "$(event u-without ErrImagePull)")"
[ "$E" = 'ErrImagePull|Failed to pull image "private/app:1": the registry refused the credentials (status 401)' ] \
  || fail "ErrImagePull: '$E'"
[ -z "$(digest u-without)" ] || fail "the Pod without credentials resolved the private image"
[ -z "$(ip_get /image-pull-auth/u-without)" ] || fail "a record for a Pod offering nothing"
echo "   without: $E"
grep -q "401 GET /v2/private/app/manifests/1" "$D/reg.log" || fail "the registry never refused"

echo "== 3. another registry, named by the reference: its own name dialled, its Bearer realm answered =="
# The store imports its host log at boot: the node is stopped to be given more.
ip_stop
pod far u-far "localhost:$PORT2/private/app:1" Always regcred
ip_boot
ip_wait 40 eval '[ "$(digest u-far)" = "$M" ]' || fail "the other registry's image: '$(ip_get /image-digests/u-far)' '$(ip_get /image-pull-failed/u-far)'"
grep -q "200 GET /v2/private/app/manifests/1" "$D/reg2.log" || fail "the other registry was not asked"
grep -q "TOKEN basic ok" "$D/reg2.log" || fail "the realm was not asked with the Secret's credentials"
echo "   localhost:$PORT2/private/app:1 -> sha256:${M:0:12}…, its token won with the Secret's https://localhost:$PORT2/v1/ entry"

echo "== 4. a shared pull: a Pod without credentials backs off; one with them is not made to wait =="
ip_stop
pod lone u-lone "private/app:2" IfNotPresent
ip_boot
ip_wait 60 eval 'case "$(ip_get "/image-fetch-q/m-private~app+2")" in *fails=2*) true ;; *) false ;; esac' \
  || fail "the shared pull did not back off: '$(ip_get "/image-fetch-q/m-private~app+2")'"
[ -z "$(digest "private~app+2")" ] || fail "resolved without credentials"
case "$(ip_get /pod-wait/u-lone)" in *"(status 401)"*) ;; *) fail "u-lone's wait: '$(ip_get /pod-wait/u-lone)'" ;; esac
# Its delay stands at 20 s, and a restarted node's clock leaves it longer.
ip_stop
pod helper u-helper "private/app:2" IfNotPresent regcred
ip_boot
t0=$SECONDS
ip_wait 15 eval '[ "$(digest "private~app+2")" = "$M" ]' || fail "the Pod with credentials waited out the backoff: '$(ip_get "/image-fetch-q/m-private~app+2")'"
echo "   resolved $((SECONDS - t0)) s after a Pod with credentials asked (the backoff stood at 20 s)"
# u-lone resolves through the same request: no longer waiting on a refused
# pull (it waits on nothing the registry decides).
ip_wait 20 eval '[ -z "$(ip_get /pod-wait/u-lone)" ]' || fail "u-lone still waits: '$(ip_get /pod-wait/u-lone)'"
echo "   the Pod without credentials shares the pulled image (IfNotPresent: an image present is not authorized again)"
ip_stop

echo "== E2E green: imagePullSecrets (the Pod's, its ServiceAccount's, each in turn), Basic and Bearer, another registry by name; Always isolates, IfNotPresent shares as upstream does =="
