#!/usr/bin/env bash
# Live E2E: PersistentVolumeClaims through the API, to Kubernetes' semantics,
# on a node in the shape a deb node runs (scripts/support/pod-node.sh).
#
#   bind        a PersistentVolume pre-bound to its claim (claimRef), and a
#               claim naming its volume (volumeName): each claim Bound, its
#               volume's directory the claim's record (volume_static.uproc)
#   Deployment  a Pod mounting the claim writes to it; the Deployment is
#               rolled (a new ReplicaSet, a Pod of a new UID) and the new Pod
#               reads what the first wrote
#   StatefulSet volumeClaimTemplates: each ordinal gets its own claim,
#               <template>-<pod>, created once; a Pod deleted (scaled away) and made again
#               (a new UID) mounts the same claim and reads what its
#               predecessor wrote, the other ordinal its own
#   fsGroup     a claim whose directory lies under the node's mount root,
#               mounted by a Pod with an fsGroup: the directory and what was
#               in it group-owned by the fsGroup (setgid, group-writable), so
#               a non-root container that is neither its owner nor in its
#               group writes it, and what it writes is the fsGroup's;
#               OnRootMismatch leaves a matching root's contents alone
#
# A Pod's own storage — the owner root its runtime keys by the Pod's UID — is
# ephemeral, as an emptyDir is upstream: what must outlive a Pod belongs in a
# claim, which is what this proves.
#
# Needs passwordless sudo: the node runs as root, as the deb's service does.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7473
REG_PORT=5413

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v cc >/dev/null || { echo "FAIL: cc not found (the probe is a static binary)"; exit 1; }
skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
sudo -n true 2>/dev/null || skip "passwordless sudo unavailable (the node runs as root)"

D="$(mktemp -d /tmp/nc-podvol-XXXXXX)"
chmod 755 "$D"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs" "$D/rootfs" "$D/owners" "$D/pods" "$D/pv/data" "$D/pv/other" "$D/pv/db-0" "$D/pv/db-1" "$D/mnt/pv/fsg"
nc_lattice_prepare "$STORE"
. "$ROOT/scripts/support/workload-node.sh"
. "$ROOT/scripts/support/pod-node.sh"
CG="/sys/fs/cgroup/nc-podvol-$$"
RUNTIME_PID=""
REG_PID=""
trap pn_cleanup EXIT
fail() { echo "FAIL: $1"; grep -v "WARN\|DEBUG" "$D/run.log" 2>/dev/null | tail -30 || true; exit 1; }
expect() { [ "$2" = "$3" ] || fail "$1:
   got  '$2'
   want '$3'"; }

echo "== 1. the probe image; render and run the node =="
pn_probe_image probe app
# The workloads' own images, their steps in the image's Cmd: a Deployment's
# or a StatefulSet's spec is kept small (their controllers carry it whole).
pn_probe_image writer "" '["cat","/data/log","appendenv","/data/log","V","sleep","600"]'
pn_probe_image db "" '["cat","/data/id","appendenv","/data/id","P","sleep","600"]'
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"
wn_registry "$REG_PORT"
wn_seed_identity
put_text "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"
pn_render
pn_run
echo "   GET /version -> 200"

jget() { # jget <api path> <python expr over d>
  local r
  r="$(api GET "$1")"
  printf '%s' "${r#*|}" | python3 -c "import json,sys
try:
    d=json.load(sys.stdin); print($2)
except Exception: print('')"
}
wait_for() { # wait_for <what> <seconds> <command…>: until the command succeeds
  local what="$1" n="$2"; shift 2
  for _ in $(seq "$n"); do "$@" && return 0; sleep 1; done
  fail "$what"
}
pods_of() { # pods_of <label selector>: "<name> <uid> <phase>" per Pod
  local r
  r="$(api GET "/api/v1/namespaces/default/pods?labelSelector=$1")"
  printf '%s' "${r#*|}" | python3 -c 'import json,sys
try:
    for p in json.load(sys.stdin).get("items", []):
        m = p["metadata"]
        print(m["name"], m["uid"], p.get("status", {}).get("phase", ""))
except Exception:
    pass'
}

echo "== 2. PersistentVolumes: one pre-bound to its claim, one a claim names; both Bound =="
pn_create /api/v1/persistentvolumes '{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pv-data"},"spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"storageClassName":"","claimRef":{"namespace":"default","name":"data"},"hostPath":{"path":"'"$D/pv/data"'"}}}'
pn_create /api/v1/namespaces/default/persistentvolumeclaims '{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"data"},"spec":{"accessModes":["ReadWriteOnce"],"storageClassName":"","resources":{"requests":{"storage":"1Gi"}}}}'
pn_create /api/v1/persistentvolumes '{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pv-other"},"spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"storageClassName":"","local":{"path":"'"$D/pv/other"'"}}}'
pn_create /api/v1/namespaces/default/persistentvolumeclaims '{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"other"},"spec":{"accessModes":["ReadWriteOnce"],"storageClassName":"","volumeName":"pv-other","resources":{"requests":{"storage":"1Gi"}}}}'
bound() { [ "$(jget "/api/v1/namespaces/default/persistentvolumeclaims/$1" 'd.get("status",{}).get("phase","")')" = Bound ]; }
wait_for "claim data never Bound: $(nc_store_get "$STORE" /volumes/default/data)" 60 bound data
wait_for "claim other never Bound: $(nc_store_get "$STORE" /volumes/default/other)" 60 bound other
expect "data's record" "$(nc_store_get "$STORE" /volumes/default/data)" "path=$D/pv/data;pv=pv-data"
expect "other's record" "$(nc_store_get "$STORE" /volumes/default/other)" "path=$D/pv/other;pv="
echo "   data <- pv-data (claimRef), other -> pv-other (volumeName): Bound"

echo "== 3. a Deployment's Pod writes to its claim; rolled, the new Pod reads it =="
dep() { # dep <version>
  printf '{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"writer","namespace":"default"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"writer"}},"template":{"metadata":{"labels":{"app":"writer"}},"spec":{"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"data"}}],"containers":[{"name":"w","image":"nanocloud/writer:1","env":[{"name":"V","value":"%s"}],"volumeMounts":[{"name":"d","mountPath":"/data"}]}]}}}}' "$1"
}
pn_create /apis/apps/v1/namespaces/default/deployments "$(dep v1)"
FIRST=""
for _ in $(seq 90); do
  FIRST="$(pods_of 'app%3Dwriter' | awk '$3=="Running"{print $2; exit}')"
  [ -n "$FIRST" ] && break
  sleep 1
done
[ -n "$FIRST" ] || fail "the writer never ran: $(pods_of 'app%3Dwriter') rs: $(nc_store_keys "$STORE" /replicasets.apps/) pods: $(nc_store_keys "$STORE" /pods/) spec bytes: $(nc_store_get "$STORE" /deployments.apps/default/writer | python3 -c 'import json,sys; print(len(json.dumps(json.load(sys.stdin)["spec"],separators=(",",":"))))')"
pn_wait_log "$FIRST" "append /data/log=ok"
expect "the first Pod found the claim empty" "$(pn_logs "$FIRST" | sed -n 's|^cat /data/log=||p')" "ERR"
expect "what it wrote, on the volume" "$(sudo -n cat "$D/pv/data/log")" "v1"
echo "   writer v1 ($FIRST): wrote v1 to the claim"
r="$(api PUT /apis/apps/v1/namespaces/default/deployments/writer "$(dep v2)")"
case "$r" in 200\|*) ;; *) fail "rolling the Deployment: ${r:0:300}" ;; esac
SECOND=""
for _ in $(seq 120); do
  SECOND="$(pods_of 'app%3Dwriter' | awk -v f="$FIRST" '$2!=f && $3=="Running"{print $2; exit}')"
  [ -n "$SECOND" ] && break
  sleep 1
done
[ -n "$SECOND" ] || fail "the Deployment was not rolled: $(pods_of 'app%3Dwriter')"
pn_wait_log "$SECOND" "append /data/log=ok"
expect "the rolled Pod read what the first wrote" "$(pn_logs "$SECOND" | sed -n 's|^cat /data/log=||p')" "v1"
expect "and added its own" "$(sudo -n cat "$D/pv/data/log")" "v1v2"
echo "   writer v2 ($SECOND, a new UID): read v1, wrote v2"

echo "== 4. a StatefulSet's claim templates: a claim per ordinal, kept across the Pod's recreation =="
pn_create /api/v1/persistentvolumes '{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pv-db-0"},"spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"storageClassName":"","claimRef":{"namespace":"default","name":"data-db-0"},"hostPath":{"path":"'"$D/pv/db-0"'"}}}'
pn_create /api/v1/persistentvolumes '{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pv-db-1"},"spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"storageClassName":"","claimRef":{"namespace":"default","name":"data-db-1"},"hostPath":{"path":"'"$D/pv/db-1"'"}}}'
pn_create /apis/apps/v1/namespaces/default/statefulsets '{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"db","namespace":"default"},"spec":{"serviceName":"db","replicas":2,"selector":{"matchLabels":{"app":"db"}},"template":{"metadata":{"labels":{"app":"db"}},"spec":{"containers":[{"name":"db","image":"nanocloud/db:1","env":[{"name":"P","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}}],"volumeMounts":[{"name":"data","mountPath":"/data"}]}]}},"volumeClaimTemplates":[{"metadata":{"name":"data"},"spec":{"accessModes":["ReadWriteOnce"],"storageClassName":"","resources":{"requests":{"storage":"1Gi"}}}}]}}'
uid_of() { jget "/api/v1/namespaces/default/pods/$1" 'd["metadata"]["uid"] if d.get("status",{}).get("phase")=="Running" else ""'; }
DB0=""; DB1=""
for _ in $(seq 150); do
  DB0="$(uid_of db-0)"; DB1="$(uid_of db-1)"
  [ -n "$DB0" ] && [ -n "$DB1" ] && break
  sleep 1
done
[ -n "$DB0" ] && [ -n "$DB1" ] || fail "the StatefulSet's Pods never ran: $(pods_of 'app%3Ddb') claims: $(nc_store_keys "$STORE" /persistentvolumeclaims/)"
pn_wait_log "$DB0" "append /data/id=ok"
pn_wait_log "$DB1" "append /data/id=ok"
for c in data-db-0 data-db-1; do
  expect "$c (a claim per ordinal, from the template)" "$(jget "/api/v1/namespaces/default/persistentvolumeclaims/$c" 'd["metadata"]["name"]+" "+d.get("status",{}).get("phase","")')" "$c Bound"
done
expect "db-0's volume" "$(sudo -n cat "$D/pv/db-0/id")" "db-0"
expect "db-1's volume" "$(sudo -n cat "$D/pv/db-1/id")" "db-1"
echo "   db-0 ($DB0) -> data-db-0, db-1 ($DB1) -> data-db-1"
# The Pod made again: scaled to 1 the controller deletes db-1 (highest
# first); scaled back to 2 it makes db-1 anew — a new UID, the same claim.
sts_scale() {
  local r
  r="$(api PATCH /apis/apps/v1/namespaces/default/statefulsets/db "{\"spec\":{\"replicas\":$1}}")"
  case "$r" in 200\|*) ;; *) fail "scaling db to $1: ${r:0:200}" ;; esac
}
sts_scale 1
for _ in $(seq 120); do
  [ "$(api GET /api/v1/namespaces/default/pods/db-1 | cut -d'|' -f1)" = 404 ] && break
  sleep 1
done
[ "$(api GET /api/v1/namespaces/default/pods/db-1 | cut -d'|' -f1)" = 404 ] || fail "db-1 survived the scale-down: $(pods_of 'app%3Ddb')"
expect "db-0 kept through the scale-down" "$(uid_of db-0)" "$DB0"
sts_scale 2
NEW1=""
for _ in $(seq 150); do
  NEW1="$(uid_of db-1)"
  [ -n "$NEW1" ] && [ "$NEW1" != "$DB1" ] && break
  NEW1=""
  sleep 1
done
[ -n "$NEW1" ] || fail "db-1 was not made again: $(pods_of 'app%3Ddb')"
pn_wait_log "$NEW1" "append /data/id=ok"
expect "the new db-1 read its predecessor's claim" "$(pn_logs "$NEW1" | sed -n 's|^cat /data/id=||p')" "db-1"
expect "and its claim holds both incarnations' writes" "$(sudo -n cat "$D/pv/db-1/id")" "db-1db-1"
expect "still one claim per ordinal" "$(jget /api/v1/namespaces/default/persistentvolumeclaims '" ".join(sorted(i["metadata"]["name"] for i in d["items"] if i["metadata"]["name"].startswith("data-db-")))')" "data-db-0 data-db-1"
echo "   db-1 again ($NEW1, a new UID): read db-1 from data-db-1"

echo "== 5. fsGroup: a claim under the mount root owned by the Pod's fsGroup =="
# The volume's directory, as an operator made it: owned by this user (not
# the container's), a file already in it, nothing writable by others.
echo "before" >"$D/mnt/pv/fsg/old"
chmod 755 "$D/mnt/pv/fsg"
chmod 644 "$D/mnt/pv/fsg/old"
pn_create /api/v1/persistentvolumes '{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"pv-fsg"},"spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"storageClassName":"","claimRef":{"namespace":"default","name":"fsg"},"local":{"path":"'"$D/mnt/pv/fsg"'"}}}'
pn_create /api/v1/namespaces/default/persistentvolumeclaims '{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"fsg"},"spec":{"accessModes":["ReadWriteOnce"],"storageClassName":"","resources":{"requests":{"storage":"1Gi"}}}}'
wait_for "claim fsg never Bound: $(nc_store_get "$STORE" /volumes/default/fsg)" 60 bound fsg
pn_create /api/v1/namespaces/default/pods '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"fsg"},"spec":{"restartPolicy":"Never","securityContext":{"runAsUser":1500,"fsGroup":4321},"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"fsg"}}],"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id","write","/data/new","x","stat","/data","stat","/data/new","stat","/data/old"],"volumeMounts":[{"name":"d","mountPath":"/data"}]}]}}'
FSG="$CREATED_UID"
pn_wait_phase "$FSG" Succeeded 120
expect "the container holds the fsGroup" "$(pn_logs "$FSG" | grep '^uid=')" "uid=1500 gid=0 groups=0,4321"
expect "uid 1500 writes the claim through the fsGroup" "$(pn_logs "$FSG" | sed -n 's|^write /data/new=||p')" "ok"
expect "the claim's directory: the fsGroup's, setgid, group-writable" "$(stat -c '%a %g' "$D/mnt/pv/fsg")" "2775 4321"
expect "what was in it: the fsGroup's, group read and write added" "$(stat -c '%a %g' "$D/mnt/pv/fsg/old")" "664 4321"
expect "what the container wrote: the fsGroup's (setgid)" "$(sudo -n stat -c '%g' "$D/mnt/pv/fsg/new")" "4321"
echo "   fsg: $D/mnt/pv/fsg 2775:4321, its old file 664:4321, uid 1500 wrote new (4321)"
# OnRootMismatch: the root already matches, so what is inside is left as is.
sudo -n touch "$D/mnt/pv/fsg/later"
sudo -n chgrp 0 "$D/mnt/pv/fsg/later"
pn_create /api/v1/namespaces/default/pods '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"fsg2"},"spec":{"restartPolicy":"Never","securityContext":{"runAsUser":1500,"fsGroup":4321,"fsGroupChangePolicy":"OnRootMismatch"},"volumes":[{"name":"d","persistentVolumeClaim":{"claimName":"fsg"}}],"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","stat","/data/later"],"volumeMounts":[{"name":"d","mountPath":"/data"}]}]}}'
FSG2="$CREATED_UID"
pn_wait_phase "$FSG2" Succeeded 120
expect "OnRootMismatch on a matching root leaves its contents" "$(pn_logs "$FSG2" | sed -n 's|^stat /data/later=||p')" "0644 0"
echo "   fsg2 (OnRootMismatch): the matching root's contents left alone"

echo "== E2E green: claims bound to static volumes, a Deployment's data across a rollout, a StatefulSet's per-ordinal claims across recreation, a claim owned by the Pod's fsGroup =="
