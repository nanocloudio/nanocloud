#!/usr/bin/env bash
# Live E2E for RBAC privilege-escalation prevention on the packaged control
# plane, through the API (apiplane_esc.plan, _shared/rbac_cover.rs).
#
# A ServiceAccount `mgr` may write Roles, RoleBindings, ClusterRoles,
# ClusterRoleBindings and ManagementBindings — and holds only get/list on
# Pods in its namespace. Upstream's rule, enforced:
#   1. a Role granting what mgr holds is created (inline body, and a staged
#      one past a record); one granting more is 403 with upstream's message,
#      inline and staged;
#   2. a patch is checked on its result: a merge, strategic or JSON patch
#      adding a permission is 403, one leaving the rules alone is applied;
#   3. a RoleBinding of a role mgr holds is created; of cluster-admin 403; of
#      a role that does not exist 404;
#   4. a ClusterRole, a ClusterRoleBinding and a ManagementBinding granting
#      more than mgr holds cluster-wide are 403, and so is an aggregationRule;
#   5. the exceptions: `escalate` on that Role's name lets mgr create it,
#      `bind` on cluster-admin lets mgr bind it — and nothing else;
#   6. a delete is not checked; the operator (cluster-admin) writes anything.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-rbac-esc-XXXXXX)"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; [ -s "$D/body" ] && { echo "--- last answer:"; head -c 1500 "$D/body"; echo; }; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"

RBAC=/apis/rbac.authorization.k8s.io/v1
MB=/apis/nanocloud.io/v1/managementbindings
CODE=0
as() { # as <token> <method> <path> [body-file] [content-type] -> CODE, $D/body
  local tok="$1" m="$2" p="$3" f="${4:-}" ct="${5:-application/json}"
  local args=(-sk -o "$D/body" -w "%{http_code}" --max-time 20 -X "$m" -H "Authorization: Bearer $tok")
  if [ -n "$f" ]; then args+=(-H "Content-Type: $ct" --data-binary "@$f"); fi
  CODE="$(curl "${args[@]}" "$SERVER$p" || true)"
}
body() { printf '%s' "$1" >"$D/req.json"; echo "$D/req.json"; }
want() { # want <code> <label>
  [ "$CODE" = "$1" ] || fail "$2: answered $CODE (want $1)"
  echo "   $2: $CODE"
}
refused() { # refused <label> [message fragment]: 403, upstream's message
  want 403 "$1"
  grep -q '"reason":"Forbidden"' "$D/body" || fail "$1: no Forbidden Status"
  grep -qF "${2:-attempting to grant RBAC permissions not currently held}" "$D/body" \
    || fail "$1: the Status does not say why"
}
until_code() { # until_code <code> <label> <cmd…>: retry (the grant index follows by watch)
  local want="$1" what="$2" i
  shift 2
  for i in $(seq 150); do
    "$@"
    [ "$CODE" = "$want" ] && { echo "   $what: $CODE"; return 0; }
    sleep 0.2
  done
  fail "$what: still $CODE after 30 s (want $want)"
}

role() { # role <name> <rules-json>
  printf '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"%s","namespace":"esc"},"rules":%s}' "$1" "$2"
}
rb() { # rb <name> <roleRef-kind> <roleRef-name>
  printf '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"%s","namespace":"esc"},"subjects":[{"kind":"ServiceAccount","name":"x","namespace":"esc"}],"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"%s","name":"%s"}}' "$1" "$2" "$3"
}
PODS='[{"apiGroups":[""],"resources":["pods"],"verbs":["get","list"]}]'
SECRETS='[{"apiGroups":[""],"resources":["secrets"],"verbs":["get"]}]'

echo "== 0. the control plane, as packaged, joined by its operator =="
kplane_up
echo "   joined"
SERVER="$(k config view --raw -o jsonpath='{.clusters[0].cluster.server}')"
k create namespace esc >/dev/null || fail "namespace"
k -n esc create serviceaccount mgr >/dev/null || fail "serviceaccount mgr"
k -n esc create serviceaccount x >/dev/null || fail "serviceaccount x"
cat >"$D/mgr.json" <<'JSON'
{"apiVersion":"v1","kind":"List","items":[
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"rbac-writer","namespace":"esc"},
 "rules":[{"apiGroups":["rbac.authorization.k8s.io"],"resources":["roles","rolebindings"],"verbs":["get","list","create","update","patch","delete"]},
          {"apiGroups":[""],"resources":["pods"],"verbs":["get","list"]}]},
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"rbac-writer","namespace":"esc"},
 "subjects":[{"kind":"ServiceAccount","name":"mgr","namespace":"esc"}],
 "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"rbac-writer"}},
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"esc-cluster-writer"},
 "rules":[{"apiGroups":["rbac.authorization.k8s.io"],"resources":["clusterroles","clusterrolebindings"],"verbs":["create"]},
          {"apiGroups":["nanocloud.io"],"resources":["managementbindings"],"verbs":["create"]}]},
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":"esc-cluster-writer"},
 "subjects":[{"kind":"ServiceAccount","name":"mgr","namespace":"esc"}],
 "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"esc-cluster-writer"}}]}
JSON
k apply --validate=false -f "$D/mgr.json" >/dev/null || fail "mgr's own roles"
TOK="$(k -n esc create token mgr)" || fail "a token for mgr"
until_code 200 "mgr may list pods in esc" as "$TOK" GET /api/v1/namespaces/esc/pods
until_code 200 "mgr may list roles in esc" as "$TOK" GET "$RBAC/namespaces/esc/roles"

echo "== 1. a Role grants only what its writer holds =="
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role pods-read "$PODS")")"
want 201 "a Role of get/list pods (held)"
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role secrets-read "$SECRETS")")"
refused "a Role of get secrets (not held)"
grep -qF 'Resources:[\"secrets\"]' "$D/body" || fail "the Status does not name the permission"
grep -qF 'system:serviceaccount:esc:mgr' "$D/body" || fail "the Status does not name the writer"
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role pods-star '[{"apiGroups":[""],"resources":["pods"],"verbs":["*"]}]')")"
refused "a Role of every verb on pods (mgr holds two)"
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role pods-named '[{"apiGroups":[""],"resources":["pods"],"resourceNames":["web-0"],"verbs":["get"]}]')")"
want 201 "a Role of get pods/web-0 (held: every name is)"
# Staged bodies (past the 512-byte inline record): the check reads the blob.
BIG="$(python3 -c 'import json;print(json.dumps([{"apiGroups":[""],"resources":["pods"],"resourceNames":["p%03d"%i],"verbs":["get"]} for i in range(40)]))')"
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role big-held "$BIG")")"
want 201 "a staged Role of 40 held rules"
BIGX="$(python3 -c 'import json;r=[{"apiGroups":[""],"resources":["pods"],"resourceNames":["p%03d"%i],"verbs":["get"]} for i in range(40)];r.append({"apiGroups":[""],"resources":["configmaps"],"verbs":["get"]});print(json.dumps(r))')"
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role big-not "$BIGX")")"
refused "a staged Role whose last rule is not held"
k -n esc get role secrets-read >/dev/null 2>&1 && fail "the refused Role was stored"
k -n esc get role big-not >/dev/null 2>&1 && fail "the refused staged Role was stored"

echo "== 2. a patch is checked on its result =="
as "$TOK" PATCH "$RBAC/namespaces/esc/roles/pods-read" "$(body '{"rules":[{"apiGroups":[""],"resources":["pods","secrets"],"verbs":["get"]}]}')" application/merge-patch+json
refused "a merge patch adding secrets"
as "$TOK" PATCH "$RBAC/namespaces/esc/roles/pods-read" "$(body '{"rules":[{"apiGroups":[""],"resources":["secrets"],"verbs":["get"]}]}')" application/strategic-merge-patch+json
refused "a strategic patch adding secrets"
as "$TOK" PATCH "$RBAC/namespaces/esc/roles/pods-read" "$(body '[{"op":"add","path":"/rules/-","value":{"apiGroups":[""],"resources":["secrets"],"verbs":["get"]}}]')" application/json-patch+json
refused "a JSON patch adding a secrets rule"
as "$TOK" PATCH "$RBAC/namespaces/esc/roles/pods-read" "$(body '{"metadata":{"labels":{"tier":"read"}}}')" application/merge-patch+json
want 200 "a merge patch of a label (the rules kept are held)"
as "$TOK" PUT "$RBAC/namespaces/esc/roles/pods-read" "$(body "$(role pods-read "$SECRETS")")"
refused "a replace with secrets"
[ "$(k -n esc get role pods-read -o jsonpath='{.rules[0].resources[0]}')" = pods ] || fail "a refused patch changed the Role"

echo "== 3. a binding binds only a role its writer holds =="
as "$TOK" POST "$RBAC/namespaces/esc/rolebindings" "$(body "$(rb x-pods Role pods-read)")"
want 201 "a RoleBinding of pods-read (held)"
as "$TOK" POST "$RBAC/namespaces/esc/rolebindings" "$(body "$(rb x-admin ClusterRole cluster-admin)")"
refused "a RoleBinding of cluster-admin"
as "$TOK" POST "$RBAC/namespaces/esc/rolebindings" "$(body "$(rb x-ghost Role ghost)")"
want 404 "a RoleBinding of a Role that does not exist"
grep -qF 'roles.rbac.authorization.k8s.io \"ghost\" not found' "$D/body" || fail "the 404 does not name the role"
as "$TOK" PATCH "$RBAC/namespaces/esc/rolebindings/x-pods" "$(body '{"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"cluster-admin"}}')" application/merge-patch+json
refused "a patch pointing the binding at cluster-admin"

echo "== 4. cluster-wide: held cluster-wide, or refused =="
CR='{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"esc-pods"},"rules":[{"apiGroups":[""],"resources":["pods"],"verbs":["get"]}]}'
as "$TOK" POST "$RBAC/clusterroles" "$(body "$CR")"
refused "a ClusterRole of get pods (held only in esc)"
as "$TOK" POST "$RBAC/clusterroles" "$(body '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"esc-agg"},"aggregationRule":{"clusterRoleSelectors":[{"matchLabels":{"esc":"agg"}}]},"rules":[]}')"
refused "a ClusterRole with an aggregationRule" "must have cluster-admin privileges to use the aggregationRule"
as "$TOK" POST "$RBAC/clusterrolebindings" "$(body '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":"esc-admin"},"subjects":[{"kind":"ServiceAccount","name":"mgr","namespace":"esc"}],"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"cluster-admin"}}')"
refused "a ClusterRoleBinding of cluster-admin to itself"
MBJ='{"apiVersion":"nanocloud.io/v1","kind":"ManagementBinding","metadata":{"name":"esc-mb"},"spec":{"cellUID":"c-1","envelopeDigest":"sha256:00","grant":"g-1","authorityEpoch":1,"rules":[{"apiGroups":["*"],"resources":["*"],"verbs":["*"]}]}}'
as "$TOK" POST "$MB" "$(body "$MBJ")"
refused "a ManagementBinding granting everything"
k get managementbinding esc-mb >/dev/null 2>&1 && fail "the refused ManagementBinding was stored"

echo "== 5. the exceptions: escalate and bind, by name =="
cat >"$D/grant.json" <<'JSON'
{"apiVersion":"v1","kind":"List","items":[
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role","metadata":{"name":"esc-exceptions","namespace":"esc"},
 "rules":[{"apiGroups":["rbac.authorization.k8s.io"],"resources":["roles"],"resourceNames":["secrets-read"],"verbs":["escalate"]},
          {"apiGroups":["rbac.authorization.k8s.io"],"resources":["clusterroles"],"resourceNames":["cluster-admin"],"verbs":["bind"]}]},
{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding","metadata":{"name":"esc-exceptions","namespace":"esc"},
 "subjects":[{"kind":"ServiceAccount","name":"mgr","namespace":"esc"}],
 "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"esc-exceptions"}}]}
JSON
k apply --validate=false -f "$D/grant.json" >/dev/null || fail "the exceptions"
until_code 201 "escalate on secrets-read: the Role is created" \
  as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role secrets-read "$SECRETS")")"
as "$TOK" POST "$RBAC/namespaces/esc/roles" "$(body "$(role secrets-other "$SECRETS")")"
refused "escalate names secrets-read only: another name is still refused"
as "$TOK" POST "$RBAC/namespaces/esc/rolebindings" "$(body "$(rb x-admin ClusterRole cluster-admin)")"
want 201 "bind on cluster-admin: the RoleBinding of it is created"
as "$TOK" POST "$RBAC/namespaces/esc/rolebindings" "$(body "$(rb x-secrets Role secrets-read)")"
refused "bind names cluster-admin only: a binding of secrets-read is still refused"
as "$TOK" POST "$RBAC/clusterrolebindings" "$(body '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":"esc-admin"},"subjects":[{"kind":"ServiceAccount","name":"mgr","namespace":"esc"}],"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"cluster-admin"}}')"
refused "bind in esc is not bind cluster-wide: the ClusterRoleBinding is still refused"

echo "== 6. a delete is not checked; the operator writes anything =="
as "$TOK" DELETE "$RBAC/namespaces/esc/rolebindings/x-admin"
want 200 "mgr deletes the RoleBinding of cluster-admin"
printf '%s' "$CR" | k apply -f - >/dev/null || fail "the operator's ClusterRole"
printf '%s' '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"esc-urls"},"rules":[{"nonResourceURLs":["/metrics"],"verbs":["get"]}]}' \
  | k apply -f - >/dev/null || fail "the operator's nonResourceURLs ClusterRole"
printf '%s' "$MBJ" | k apply -f - >/dev/null || fail "the operator's ManagementBinding"
echo "   the operator (cluster-admin) wrote a ClusterRole, a URL ClusterRole and a ManagementBinding"

echo "== E2E green: no write granted more than its writer held; escalate and bind admitted exactly what they name =="
