#!/usr/bin/env bash
# Live E2E for Kubernetes RBAC as Chronicle params (rbac_rules.uproc,
# rbac_grants.uproc, rbac_prune.uproc; graph EXTRACTED from
# fluxor-controlplane.yaml into packaging/debian/fluxor-rbac.yaml).
#
# Real rbac.authorization.k8s.io/v1 objects go in; the grant index the API
# plane probes comes out:
#   /rbac-g/<identity>/<scope>|<verb>|<group>|<resource>|<name>
#   (scope `*` = cluster-wide; name `*` = every name; `~` a non-resource URL)
#
# Proves: ClusterRole + ClusterRoleBinding grant everywhere, wildcards kept as
# `*`; a ClusterRole bound by a RoleBinding grants only in that namespace; a
# Role's ServiceAccount subject defaults to the binding's namespace; apiGroups
# are distinguished (core is ``); resourceNames restrict a grant to those
# names; nonResourceURLs grant only through a ClusterRoleBinding, as `~`
# literals; deleting a binding REVOKES its grants; editing a role narrows them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GRAPH="$ROOT/packaging/debian/fluxor-rbac.yaml"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/rbac.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
[ -e "$GRAPH" ] || { echo "FAIL: missing $GRAPH"; exit 1; }

D="$(mktemp -d /tmp/nc-rbac-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
L="$D/store.log"
settle() { rbac_settle "$FLUXOR_RUNTIME" "$D/config.bin" "$D/modules.bin" "$D" "$@"; }
has() { rbac_keys "$L" | grep -qxF "$1"; }

echo "== 1. build the extracted RBAC graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. seed: two ClusterRoles, a namespaced Role, three bindings =="
rbac_clusterrole "$L" admin "*" "*"
rbac_clusterrole "$L" viewer "get,list" "pods"
rbac_clusterrole "$L" one-secret "get" "secrets" "db-password"
rbac_role "$L" team cm-editor "create,update" "configmaps"
rbac_clusterrole_g "$L" apps-reader "apps" "get" "deployments"
rbac_clusterrole_urls "$L" metrics-reader "get" "/metrics"
rbac_crb "$L" admins admin ServiceAccount admin default
rbac_crb "$L" apps-readers apps-reader User dave
rbac_crb "$L" scrapers metrics-reader User mon
rbac_rb "$L" team team-scrapers ClusterRole metrics-reader User erin
rbac_crb "$L" secret-reader one-secret User carol
rbac_rb "$L" team alice-views ClusterRole viewer User alice
rbac_rb "$L" team bot-edits Role cm-editor ServiceAccount bot

A='/rbac-g/system:serviceaccount:default:admin'
settle present "$A/*|*|*|*|*" "/rbac-g/alice/team|get|*|pods|*" "/rbac-g/alice/team|list|*|pods|*" \
  "/rbac-g/system:serviceaccount:team:bot/team|create||configmaps|*" \
  "/rbac-g/system:serviceaccount:team:bot/team|update||configmaps|*" \
  "/rbac-g/carol/*|get|*|secrets|db-password" \
  "/rbac-g/dave/*|get|apps|deployments|*" \
  "/rbac-g/mon/~|get|~|/metrics|~"
echo "   admin: *|*|*|*|* (ClusterRoleBinding, wildcards kept literally)"
echo "   alice: team|get|*|pods|*, team|list|*|pods|* (a ClusterRole bound in ONE namespace)"
echo "   bot:   team|create||configmaps|* (SA defaults to the binding's namespace; core group is \`\`)"
echo "   carol: *|get|*|secrets|db-password (resourceNames: that name only)"
echo "   dave:  *|get|apps|deployments|* (apiGroups distinguished)"
echo "   mon:   ~|get|~|/metrics|~ (nonResourceURLs, through a ClusterRoleBinding)"

echo "== 3. what must NOT be granted =="
if has "/rbac-g/alice/*|get|*|pods|*"; then echo "FAIL: a RoleBinding granted cluster-wide"; exit 1; fi
if has "/rbac-g/carol/*|get|*|secrets|*"; then
  echo "FAIL: a resourceNames rule granted its verb on EVERY name"; exit 1
fi
if rbac_keys "$L" | grep -q '^/rbac-g/dave/.*|\*|deployments|\|^/rbac-g/dave/.*||deployments|'; then
  echo "FAIL: an apps-only role granted deployments in another group"; exit 1
fi
if rbac_keys "$L" | grep -q '^/rbac-g/erin/'; then
  echo "FAIL: nonResourceURLs granted through a RoleBinding (they are cluster-scoped)"; exit 1
fi
if has "/rbac-g/alice/team|delete|*|pods|*"; then echo "FAIL: a verb the role lacks was granted"; exit 1; fi
echo "   no cluster-wide grant from a RoleBinding; resourceNames not widened; groups not"
echo "   widened; no URL grant through a RoleBinding; no extra verbs"

echo "== 4. revocation: delete alice's binding -> her grants go =="
rbac_delete "$L" /rolebindings/team/alice-views
settle absent "/rbac-g/alice/team|get|*|pods|*" "/rbac-g/alice/team|list|*|pods|*"
has "$A/*|*|*|*|*" || { echo "FAIL: revoking one binding removed another's grant"; exit 1; }
echo "   alice's grants removed; admin's untouched"

echo "== 5. narrowing: cm-editor loses 'create' -> bot keeps only update =="
rbac_role "$L" team cm-editor "update" "configmaps"
settle absent "/rbac-g/system:serviceaccount:team:bot/team|create||configmaps|*"
has "/rbac-g/system:serviceaccount:team:bot/team|update||configmaps|*" \
  || { echo "FAIL: narrowing a role removed the verb it kept"; exit 1; }
echo "   create revoked, update kept"

echo "== 6. aggregationRule: an aggregating role's subjects gain the matched roles' grants =="
rbac_clusterrole_l "$L" svc-view "'agg.example/view':'true'" "get" "services"
rbac_clusterrole_l "$L" ep-view "'tier':'gold'" "get" "endpoints"
rbac_clusterrole_l "$L" node-view "'tier':'lead'" "get" "nodes"
rbac_clusterrole_agg "$L" view-agg \
  "{'matchLabels':{'agg.example/view':'true'}},{'matchExpressions':[{'key':'tier','operator':'In','values':['gold','silver']}]}" \
  "'agg.example/edit':'true'"
rbac_clusterrole_agg "$L" edit-agg "{'matchLabels':{'agg.example/edit':'true'}}"
rbac_crb "$L" viewers view-agg User frank
rbac_crb "$L" editors edit-agg User gina
settle present "/rbac-g/frank/*|get|*|services|*" "/rbac-g/frank/*|get|*|endpoints|*" \
  "/rbac-g/gina/*|get|*|services|*" "/rbac-g/gina/*|get|*|endpoints|*"
if rbac_keys "$L" | grep -q '^/rbac-g/\(frank\|gina\)/.*|nodes|'; then
  echo "FAIL: a role no selector matches was aggregated"; exit 1
fi
echo "   frank: services (matchLabels) + endpoints (matchExpressions In); nodes not matched"
echo "   gina:  the same, through an aggregation of an aggregation"

echo "== 7. relabel: svc-view leaves the selector -> its grants leave both =="
rbac_clusterrole_l "$L" svc-view "'agg.example/view':'false'" "get" "services"
settle absent "/rbac-g/frank/*|get|*|services|*" "/rbac-g/gina/*|get|*|services|*"
has "/rbac-g/frank/*|get|*|endpoints|*" || { echo "FAIL: relabelling one role removed another's aggregation"; exit 1; }
echo "   services revoked from frank and gina; endpoints kept"

echo "== 8. unbinding the aggregating role removes its mirrored grants =="
rbac_delete "$L" /clusterrolebindings/viewers
settle absent "/rbac-g/frank/*|get|*|endpoints|*"
has "/rbac-g/gina/*|get|*|endpoints|*" || { echo "FAIL: unbinding frank removed gina's aggregation"; exit 1; }
echo "   frank's mirrored grants removed; gina's kept"

echo "== E2E green: Kubernetes RBAC as params — grants, scopes, and revocation =="
