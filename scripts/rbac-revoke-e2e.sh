#!/usr/bin/env bash
# Live E2E for how soon an RBAC revocation takes effect on the packaged
# control plane, through the API, with a ServiceAccount's own token.
#
# Authorization reads no cache: every request probes the grant INDEX
# (/rbac-g/, rbac_rules.plan / rbac_grants.plan / rbac_prune.plan) in the
# store, and the index follows the RBAC objects by watch — a role or binding
# written is re-derived at once, and the rows it no longer produces are
# pruned. So a revocation is not instant (the derivation is a few chained
# store writes after the write that revoked), but it is BOUNDED, and once a
# request is denied every later one is (the index never re-grants what was
# removed). docs/architecture/modules.md states the bound this proves:
# NC_RBAC_REVOKE_MS (default 2000 ms) from the revoking write's answer to the
# first 403, for each way a grant is revoked —
#   1. the ClusterRole's rules emptied (PUT `rules: []`)
#   2. the binding deleted
#   3. the subject removed from the binding's subjects
# — each followed by a run of further requests that must all be denied.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not on PATH"; exit 1; }
BOUND_MS="${NC_RBAC_REVOKE_MS:-2000}"

D="$(mktemp -d /tmp/nc-rbac-revoke-XXXXXX)"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"

ms() { date +%s%3N; }

echo "== 0. the control plane, as packaged, joined by its operator =="
kplane_up
echo "   joined"
SERVER="$(k config view --raw -o jsonpath='{.clusters[0].cluster.server}')"

echo "== 1. a ServiceAccount granted get services by a ClusterRole and its binding =="
k create namespace rv >/dev/null || fail "namespace"
k -n rv create serviceaccount acc >/dev/null || fail "serviceaccount"
k -n rv create service clusterip web --tcp=80:80 >/dev/null || fail "service"
role() { # role <rules-json>
  printf '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"svc-read"},"rules":%s}' "$1" \
    | k apply -f - >/dev/null
}
binding() { # binding <subjects-json>
  printf '{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRoleBinding","metadata":{"name":"svc-read"},"subjects":%s,"roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"ClusterRole","name":"svc-read"}}' "$1" \
    | k apply -f - >/dev/null
}
RULES='[{"apiGroups":[""],"resources":["services"],"verbs":["get","list"]}]'
ACC='[{"kind":"ServiceAccount","name":"acc","namespace":"rv"}]'
BOTH='[{"kind":"ServiceAccount","name":"acc","namespace":"rv"},{"kind":"ServiceAccount","name":"other","namespace":"rv"}]'
role "$RULES" || fail "clusterrole"
binding "$ACC" || fail "clusterrolebinding"
TOK="$(k -n rv create token acc)" || fail "a token for acc"
as_acc() { # the status of acc's GET of the Service
  curl -sk -o /dev/null -w "%{http_code}" --max-time 5 \
    -H "Authorization: Bearer $TOK" "$SERVER/api/v1/namespaces/rv/services/web" || true
}
TOOK=0
BEFORE=0
until_code() { # until_code <code> <label> <limit-ms> -> TOOK (ms), BEFORE (answers that were not <code>)
  local want="$1" t0 c
  t0="$(ms)"
  BEFORE=0
  while :; do
    c="$(as_acc)"
    [ "$c" = "$want" ] && { TOOK=$(( $(ms) - t0 )); return 0; }
    BEFORE=$((BEFORE + 1))
    [ $(( $(ms) - t0 )) -gt "$3" ] && fail "$2: acc's GET still answers $c after $3 ms (want $want)"
    sleep 0.02
  done
}
stays() { # stays <code> <label>: 25 more requests, every one <code>
  local c i
  for i in $(seq 25); do
    c="$(as_acc)"
    [ "$c" = "$1" ] || fail "$2: request $i after the first $1 answered $c — a revocation must not flap"
  done
}
granted() {
  until_code 200 "$1 (granted)" 30000
  echo "   granted ($1) — acc's GET 200 after $TOOK ms"
}
revoked() { # revoked <label>
  until_code 403 "$1" "$BOUND_MS"
  local t="$TOOK" b="$BEFORE"
  stays 403 "$1"
  echo "   revoked ($1): first 403 $t ms after the write was answered, $b request(s) granted before it (bound $BOUND_MS ms); 25 more all 403"
}
granted "bound"

echo "== 2. the ClusterRole's rules emptied =="
role '[]' || fail "emptying the rules"
revoked "rules: []"
role "$RULES" || fail "restoring the rules"
granted "rules restored"

echo "== 3. the binding deleted =="
k delete clusterrolebinding svc-read >/dev/null || fail "deleting the binding"
revoked "binding deleted"
binding "$BOTH" || fail "recreating the binding"
granted "binding recreated"

echo "== 4. the subject removed from the binding =="
binding '[{"kind":"ServiceAccount","name":"other","namespace":"rv"}]' || fail "narrowing the subjects"
revoked "subject removed"

echo "== E2E green: every revocation denied acc within $BOUND_MS ms of its write, and stayed denied =="
