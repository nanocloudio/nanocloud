#!/usr/bin/env bash
# Live E2E: `kubectl delete pod` of a Pod a CONTROLLER wrote, driven with
# kubectl against the packaged control plane.
#
# A ReplicaSet's or StatefulSet's Pods are written straight to the store by
# its controller (store_stamped), never through the API's document executor,
# so on a fresh plane the executor has no descriptor for `pods`. A delete there
# was answered "no collection" by the executor, and the API still answered
# 200 Success (a delete's Success is decided before the write): the object
# stayed at the same resourceVersion while its tomb told the node to kill it.
# store_doc now creates the collection (a dry-run patch) and asks the delete
# again; a delete the store did not do answers its failure, never Success.
#
#   1. a ReplicaSet's Pod, deleted FIRST of anything written to `pods`
#      through the API: the object is gone (a GET is 404), and the ReplicaSet
#      replaces it with a Pod of a new uid
#   2. a StatefulSet's db-0, deleted: gone, then db-0 again with a new uid
#   3. a delete of a Pod that is not there: 404 NotFound, not Success
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-cpdel-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v MON_ || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
cleanup() { kplane_down; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

uid_of() { k get pod "$1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true; }
wait_uid() { # wait_uid <pod> [not-this-uid] -> echo the uid (a replacement: a new one)
  local u=""
  for _ in $(seq 120); do
    u="$(uid_of "$1")"
    if [ -n "$u" ] && [ "$u" != "${2:-}" ]; then echo "$u"; return 0; fi
    sleep 0.5
  done
  return 1
}
# The DELETE as the API answers it: "<http status> <reason>".
raw_delete() {
  { k delete --raw "/api/v1/namespaces/default/pods/$1" -v=6 2>&1 || true; } \
    | grep -o 'status="[0-9]* [A-Za-z ]*"' | tail -1 | tr -d '"' | sed 's/^status=//'
}

echo "== 0. the control plane, as packaged, joined by its operator =="
kplane_up
echo "   joined"

echo "== 1. a ReplicaSet's Pod, deleted first of anything: gone, then replaced =="
cat >"$D/rs.yaml" <<'Y'
apiVersion: apps/v1
kind: ReplicaSet
metadata: {name: rsd}
spec:
  replicas: 1
  selector: {matchLabels: {app: rsd}}
  template:
    metadata: {labels: {app: rsd}}
    spec:
      containers: [{name: w, image: busybox, command: ["sleep", "1000"]}]
Y
k apply -f "$D/rs.yaml" >/dev/null || fail "the ReplicaSet was not created"
u1="$(wait_uid rsd-0)" || fail "the ReplicaSet wrote no Pod rsd-0"
rv1="$(k get pod rsd-0 -o jsonpath='{.metadata.resourceVersion}')"
got="$(raw_delete rsd-0)"
[ "$got" = "200 OK" ] || fail "DELETE rsd-0 answered '$got', not 200 OK"
u2="$(wait_uid rsd-0 "$u1")" \
  || fail "rsd-0 still has uid $u1 (resourceVersion $(k get pod rsd-0 -o jsonpath='{.metadata.resourceVersion}' 2>/dev/null), was $rv1): the DELETE answered 200 but the object stayed"
echo "   rsd-0 $u1 deleted; replaced by $u2"

echo "== 2. a StatefulSet's db-0, deleted: gone, then db-0 with a new uid =="
cat >"$D/sts.yaml" <<'Y'
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: db}
spec:
  serviceName: db
  replicas: 1
  selector: {matchLabels: {app: db}}
  template:
    metadata: {labels: {app: db}}
    spec:
      containers: [{name: w, image: busybox, command: ["sleep", "1000"]}]
Y
k apply -f "$D/sts.yaml" >/dev/null || fail "the StatefulSet was not created"
s1="$(wait_uid db-0)" || fail "the StatefulSet wrote no Pod db-0"
k delete pod db-0 --wait=false >/dev/null || fail "kubectl delete pod db-0 failed"
s2="$(wait_uid db-0 "$s1")" \
  || fail "db-0 still has uid $s1: the delete answered but the object stayed"
echo "   db-0 $s1 deleted; recreated as $s2"

echo "== 3. a delete of nothing: 404 NotFound =="
got="$(raw_delete nope-0)"
[ "$got" = "404 Not Found" ] || fail "DELETE of an absent Pod answered '$got', not 404"
echo "   404"

echo "PASS: controller-written Pods delete as the API's do"
