#!/usr/bin/env bash
# Live E2E: a delete a CONTROLLER makes leaves the tomb an API delete leaves,
# driven with kubectl against the packaged control plane.
#
# A watch's DELETED event carries the tomb (`/~tomb/<collection>/<ns>/<name>`,
# the object's last state), and a Pod's node learns of the delete from the
# tomb's index (`/~tomb-at/pods/…`, pod_specs.uproc `gone` ->
# desired=deleted). A controller's delete that left none was seen by a
# watcher (kubectl get -w, an informer) as identity only — or, for a Pod,
# not at all by its node, which ran it on. Every Pod and Job here is past a
# record (≥ 4 KiB): its delete reads it whole (a heap build) for its tomb.
#
#   1. a ReplicaSet scaled down (rs_apply): the surplus Pod
#   2. a DaemonSet's node removed (ds_collect): that node's Pod
#   3. a Pod whose ReplicaSet was deleted (gc_pod_collect)
#   4. a Job past its ttlSecondsAfterFinished (jb_w1), and its Pod
#      (gc_job_collect)
#   5. a CronJob's finished Job past successfulJobsHistoryLimit (cj_hdel)
#
# Each delete: the tomb is there with its index entry (naming the uid), a
# watch saw DELETED with the whole object (built from the tomb: without one
# it is identity only), and a Pod's node ask says desired=deleted.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }
command -v redis-cli >/dev/null || { echo "FAIL: redis-cli not found"; exit 1; }

D="$(mktemp -d /tmp/nc-ctomb-XXXXXX)"
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null | grep -v "MON_\|kvw\]\|port 53" || true; exit 1; }
. "$ROOT/scripts/support/kplane.sh"
WATCHES=()
cleanup() {
  for w in "${WATCHES[@]}"; do kill "$w" 2>/dev/null || true; done
  kplane_down
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT

# A 5000-byte annotation: every template here, and every object made from
# it, is past a record (the node's ask, which carries no annotations, is not).
PAD="$(head -c 5000 /dev/zero | tr '\0' p)"
TPL() { # TPL <labels-json> [restartPolicy] -> a Pod template past a record
  printf '{"metadata":{"labels":%s,"annotations":{"pad":"%s"}},"spec":{%s"containers":[{"name":"c","image":"busybox","command":["sleep","1000"]}]}}' \
    "$1" "$PAD" "${2:+\"restartPolicy\":\"$2\",}"
}
rget() { timeout 10 redis-cli -p "$(cat "$STORE/read.port")" --raw GET "$1" 2>/dev/null || true; }
watch_kind() { # watch_kind <resource> -> $D/w-<resource>.json, every event
  kubectl --kubeconfig "$KC" --request-timeout=0 get "$1" -w --output-watch-events -o json \
    >"$D/w-$1.json" 2>"$D/w-$1.err" &
  WATCHES+=($!)
}
uid_of() { k get "$1" "$2" -o jsonpath='{.metadata.uid}'; }
running_on() { # running_on <node> <uid>: its node's ask is to run it
  kplane_until 60 "the node ask for $2 on $1" sh -c "timeout 10 redis-cli -p $(cat "$STORE/read.port") --raw GET '/~node/$1/pod-specs/$2' | grep -q 'desired=running'"
}
# gone <collection> <resource> <name> <uid> [node]: the delete left its tomb
# (the whole object), the tomb's index, a DELETED event with the whole
# object, and — a Pod's — its node's ask desired=deleted.
gone() {
  local coll="$1" res="$2" name="$3" uid="$4" node="${5:-}"
  kplane_until 60 "$res/$name's DELETED event" python3 "$D/ev.py" has "$D/w-$res.json" "$uid"
  python3 "$D/ev.py" whole "$D/w-$res.json" "$uid" || fail "$res/$name's DELETED event is not the whole object"
  # The tomb is past a record (the reader answers only its key): its
  # content is what the DELETED event above was built from.
  nc_store_keys "$STORE" "/~tomb/$coll/default/" | grep -qx "/~tomb/$coll/default/$name" \
    || fail "$res/$name left no tomb"
  local at
  at="$(rget "/~tomb-at/$coll/default/$name")"
  printf '%s' "$at" | grep -q "\"uid\":\"$uid\"" || fail "$res/$name's tomb has no index entry naming $uid: '$at'"
  if [ -n "$node" ]; then
    kplane_until 30 "$name's node ask desired=deleted" sh -c "timeout 10 redis-cli -p $(cat "$STORE/read.port") --raw GET '/~node/$node/pod-specs/$uid' | grep -q 'desired=deleted'"
  fi
  echo "   $res/$name ($uid): tomb + index, DELETED whole${node:+, $node told desired=deleted}"
}

# ev.py has|whole <watch output> <uid>: a DELETED event of that uid was
# seen; it carries the whole object (its 5000-byte annotation, its spec).
cat >"$D/ev.py" <<'PY'
import json, sys
mode, path, uid = sys.argv[1:4]
text, dec, at, hit = open(path).read(), json.JSONDecoder(), 0, None
while True:
    while at < len(text) and text[at].isspace():
        at += 1
    if at >= len(text):
        break
    try:
        ev, at = dec.raw_decode(text, at)
    except ValueError:
        break
    if ev.get("type") == "DELETED" and ev["object"]["metadata"].get("uid") == uid:
        hit = ev["object"]
if hit is None:
    sys.exit(1)
if mode == "whole":
    tpl = hit["spec"].get("template", hit)
    assert hit["metadata"].get("name") and hit["metadata"].get("creationTimestamp"), hit["metadata"]
    assert len(tpl["metadata"].get("annotations", {}).get("pad", "")) == 5000, "identity only, or a cut object"
    assert tpl["spec"]["containers"][0]["image"] == "busybox", "no spec"
if mode == "complete":
    assert any(c.get("type") == "Complete" and c.get("status") == "True"
               for c in hit.get("status", {}).get("conditions", [])), hit.get("status")
PY

echo "== 0. the control plane, joined; two nodes; watches on Pods and Jobs =="
kplane_up
nc_store_ready "$STORE" || fail "the store's reader never answered"
kplane_node node-a '{"kubernetes.io/hostname":"node-a"}'
kplane_node node-b '{"kubernetes.io/hostname":"node-b"}'
watch_kind pods
watch_kind jobs
echo "   node-a, node-b ready"

echo "== 1. a ReplicaSet scaled down: the surplus Pod's tomb =="
printf '{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"rs"},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"rs"}},"template":%s}}' \
  "$(TPL '{"app":"rs"}')" >"$D/rs.json"
k apply -f "$D/rs.json" >/dev/null || fail "apply rs"
kplane_until 60 "rs-0 and rs-1 scheduled" sh -c "[ \"\$(kubectl --kubeconfig $KC get pods -l app=rs -o jsonpath='{range .items[*]}{.spec.nodeName}{\"\\n\"}{end}' | grep -c node)\" = 2 ]"
U="$(uid_of pod rs-1)"; N="$(k get pod rs-1 -o jsonpath='{.spec.nodeName}')"
running_on "$N" "$U"
[ "$(k get pod rs-1 -o json | wc -c)" -gt 4096 ] || fail "rs-1 is not past a record"
k scale rs rs --replicas=1 >/dev/null || fail "scale rs"
kplane_until 30 "rs-1 deleted" sh -c "! kubectl --kubeconfig $KC get pod rs-1"
gone pods pods rs-1 "$U" "$N"
k get pod rs-0 >/dev/null || fail "the scale-down deleted rs-0"

echo "== 2. a DaemonSet's node removed: that node's Pod's tomb =="
printf '{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"ds"},"spec":{"selector":{"matchLabels":{"app":"ds"}},"template":%s}}' \
  "$(TPL '{"app":"ds"}')" >"$D/ds.json"
k apply -f "$D/ds.json" >/dev/null || fail "apply ds"
dspod() { k get pods -l app=ds -o jsonpath="{range .items[?(@.spec.nodeName==\"$1\")]}{.metadata.name}{end}"; }
kplane_until 60 "a ds Pod on node-b" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get pods -l app=ds -o jsonpath='{range .items[?(@.spec.nodeName==\"node-b\")]}{.metadata.name}{end}')\" ]"
P="$(dspod node-b)"; U="$(uid_of pod "$P")"
running_on node-b "$U"
rm -f "$D/node-node-b.json"
k delete node node-b >/dev/null || fail "delete node-b"
kplane_until 90 "$P (node-b's) deleted" sh -c "! kubectl --kubeconfig $KC get pod $P"
gone pods pods "$P" "$U" node-b
[ -n "$(dspod node-a)" ] || fail "node-a's ds Pod went too"

echo "== 3. a Pod whose ReplicaSet was deleted: collected, with its tomb =="
printf '{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"orph"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"orph"}},"template":%s}}' \
  "$(TPL '{"app":"orph"}')" >"$D/orph.json"
k apply -f "$D/orph.json" >/dev/null || fail "apply orph"
kplane_until 60 "orph-0 scheduled" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get pod orph-0 -o jsonpath='{.spec.nodeName}')\" ]"
U="$(uid_of pod orph-0)"; N="$(k get pod orph-0 -o jsonpath='{.spec.nodeName}')"
running_on "$N" "$U"
k delete rs orph --wait=false >/dev/null || fail "delete rs orph"
kplane_until 60 "orph-0 collected" sh -c "! kubectl --kubeconfig $KC get pod orph-0"
gone pods pods orph-0 "$U" "$N"

echo "== 4. a Job past its ttlSecondsAfterFinished: its tomb, and its Pod's =="
printf '{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"ttl"},"spec":{"ttlSecondsAfterFinished":2,"template":%s}}' \
  "$(TPL '{"j":"ttl"}' Never)" >"$D/ttl.json"
k apply -f "$D/ttl.json" >/dev/null || fail "apply job ttl"
JU="$(uid_of job ttl)"
kplane_until 60 "job ttl's Pod scheduled" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get pods -l job-name=ttl -o jsonpath='{.items[0].spec.nodeName}')\" ]"
P="$(k get pods -l job-name=ttl -o jsonpath='{.items[0].metadata.name}')"
U="$(uid_of pod "$P")"; N="$(k get pod "$P" -o jsonpath='{.spec.nodeName}')"
running_on "$N" "$U"
[ "$(k get job ttl -o json | wc -c)" -gt 4096 ] || fail "job ttl is not past a record"
kplane_pod_phase "$P" Succeeded
k wait --for=condition=complete job/ttl --timeout=60s >/dev/null || fail "job ttl never completed"
k wait --for=delete job/ttl --timeout=30s >/dev/null || fail "job ttl not deleted after its TTL"
gone jobs.batch jobs ttl "$JU"
kplane_until 30 "job ttl's Pod collected" sh -c "! kubectl --kubeconfig $KC get pod $P"
gone pods pods "$P" "$U" "$N"

echo "== 5. a CronJob's finished Job past its history limit: its tomb =="
printf '{"apiVersion":"batch/v1","kind":"CronJob","metadata":{"name":"hist"},"spec":{"schedule":"* * * * *","successfulJobsHistoryLimit":0,"jobTemplate":{"spec":{"template":%s}}}}' \
  "$(TPL '{"cron":"hist"}' Never)" >"$D/hist.json"
k apply -f "$D/hist.json" >/dev/null || fail "apply cronjob hist"
kplane_until 90 "a Job of cronjob hist" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get jobs -o jsonpath='{range .items[?(@.metadata.ownerReferences[0].name==\"hist\")]}{.metadata.name}{end}')\" ]"
k patch cronjob hist --type=merge -p '{"spec":{"suspend":true}}' >/dev/null || fail "suspend hist"
J="$(k get jobs -o jsonpath='{range .items[?(@.metadata.ownerReferences[0].name=="hist")]}{.metadata.name}{" "}{end}' | awk '{print $1}')"
JU="$(uid_of job "$J")"
kplane_until 60 "$J's Pod" sh -c "[ -n \"\$(kubectl --kubeconfig $KC get pods -l job-name=$J -o name)\" ]"
kplane_pod_phase "$(k get pods -l job-name="$J" -o jsonpath='{.items[0].metadata.name}')" Succeeded
# Pruned the moment it completes (a history of none): its Complete is what
# the DELETED event carries.
k wait --for=delete "job/$J" --timeout=60s >/dev/null || fail "$J not pruned past successfulJobsHistoryLimit 0"
python3 "$D/ev.py" complete "$D/w-jobs.json" "$JU" || fail "$J's DELETED event does not say Complete"
gone jobs.batch jobs "$J" "$JU"

echo "== E2E green: controller and GC deletes leave the API's tomb: whole DELETED events, nodes told =="
