#!/usr/bin/env bash
# Live E2E: a Pod's spec through the API, to Kubernetes' semantics, on a node
# in the shape a deb node runs (scripts/support/pod-node.sh: the control
# plane, its kubelet's chains, its image plane, one store scoped to node-1),
# from an OCI image of a probe that prints what its process sees.
#
# PASS =
#   env        the namespace's Service links (<SVC>_SERVICE_HOST/_PORT, …),
#              envFrom (a ConfigMap, a Secret with a prefix), env in order —
#              value with $(VAR) expanded ($$ escapes it, an unknown one left
#              as written), valueFrom configMapKeyRef / secretKeyRef /
#              fieldRef (metadata.name/namespace/uid, spec.nodeName,
#              status.podIP) / resourceFieldRef — and command/args expanded
#              against the container's environment
#   config     a container whose ConfigMap is missing is not created
#              (CreateContainerConfigError, in the Pod's status) and starts
#              once the ConfigMap exists
#   security   runAsUser/runAsGroup (container over Pod), runAsGroup alone
#              keeping the image's uid; runAsNonRoot refusing an image that
#              runs as root and one whose user is a name; fsGroup and
#              supplementalGroups in the process's groups, with the image's
#              /etc/group memberships (Merge) or without them (Strict);
#              readOnlyRootFilesystem: a write to / refused, a volume still
#              written
#   fsGroup    an emptyDir group-owned by it (setgid, group-writable); a
#              ConfigMap's and a Secret's files at their modes, group-owned
#              by it with 0440 added (a 0400 Secret readable through it); a
#              Secret's files on a memory filesystem, a ConfigMap's not
#   projected  a downwardAPI volume (fieldRef, the labels map,
#              resourceFieldRef) and a projected one (ConfigMap, Secret and
#              downward API sources, at their modes, on a memory filesystem)
#   atomic     a ConfigMap updated under a running reader: each update seen,
#              whole (two keys read through one ..data never differ)
#   gone       a deleted Pod's volume directory removed, its Secret's
#              memory filesystem unmounted
#   volumes    an emptyDir shared by an init container and two app
#              containers, writable by a non-root user; a ConfigMap volume
#              (its keys, an item at its path); a Secret volume (defaultMode
#              0400, decoded); a hostPath
#   init       init containers run to completion, in order, before the app
#              containers; two app containers both run; an init container
#              that fails under restartPolicy Never fails the Pod and nothing
#              after it runs
#   pause      nanocloud.io/paused: "true" freezes the Pod's sandbox (the
#              cgroup freezer), removing it thaws it
#   dns        /etc/resolv.conf by dnsPolicy: ClusterFirst (the cluster DNS,
#              the namespace's search path, ndots:5), Default (the node's
#              own file), None with dnsConfig (that alone)
#   delete     `kubectl delete pod` as upstream: a two-container Pod stops
#              both containers and every node record of both goes when its
#              tomb ends; a Pod trapping SIGTERM (5 s, grace 10) exits
#              gracefully while kubectl waits, Terminating meanwhile; one
#              ignoring SIGTERM is killed once its grace is spent; and
#              `--force --grace-period=0` removes a Terminating one and
#              kills it at once
#
# Needs passwordless sudo: the node runs as root, as the deb's service does.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7471
REG_PORT=5411

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

D="$(mktemp -d /tmp/nc-podspec-XXXXXX)"
chmod 755 "$D"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs" "$D/rootfs" "$D/owners" "$D/pods" "$D/host"
nc_lattice_prepare "$STORE"
. "$ROOT/scripts/support/workload-node.sh"
. "$ROOT/scripts/support/pod-node.sh"
CG="/sys/fs/cgroup/nc-podspec-$$"
RUNTIME_PID=""
REG_PID=""
trap pn_cleanup EXIT
fail() { echo "FAIL: $1"; grep -v "WARN\|DEBUG" "$D/run.log" 2>/dev/null | tail -30 || true; exit 1; }
CTX=""
expect() { [ "$2" = "$3" ] || fail "$1:
   got  '$2'
   want '$3'
   ${CTX:+spec='$(pn_nodekey "pod-specs/$CTX" | cut -c1-1500)'
   logs='$(pn_logs "$CTX" | head -c 1500)'}"; }

echo "== 1. images: the probe as app (1234:2345), and as root (no User) =="
pn_probe_image probe app
pn_probe_image rootprobe ""
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"
wn_registry "$REG_PORT"
wn_seed_identity
put_text "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"
# node-1 with an address: its Pods' hostIP and podIP (they share its network).
put_text "/nodes/node-1" '{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1"},"status":{"ready":true,"conditions":[{"type":"Ready","status":"True"}],"addresses":[{"type":"InternalIP","address":"127.0.0.1"}],"allocatable":{"cpu":"4","memory":"4Gi"}}}'
# The cluster DNS's address, as cluster_dns keeps it once it serves.
put_text "/cluster-dns" "10.96.0.10"
put_text "/namespaces/strict" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"strict","labels":{"pod-security.kubernetes.io/enforce":"baseline"}}}'

echo "== 2. render and run the node =="
# A deleted Pod's tomb, and with it its node's records, stays 20 s (step 12).
TOMB_S=20
PN_TOMB_MS=$((TOMB_S * 1000)) pn_render
pn_run
echo "   GET /version -> 200"

echo "== 3. the objects a Pod's spec names: a ConfigMap, a Secret, a Service =="
pn_create /api/v1/namespaces/default/configmaps '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cfg"},"data":{"MODE":"fast","greeting":"hello world","app.conf":"a=1\nb=2"}}'
# "s3cr3t", "admin"
pn_create /api/v1/namespaces/default/secrets '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"sec"},"type":"Opaque","data":{"token":"czNjcjN0","user":"YWRtaW4="}}'
pn_create /api/v1/namespaces/default/services '{"apiVersion":"v1","kind":"Service","metadata":{"name":"redis-main"},"spec":{"selector":{"app":"redis"},"ports":[{"name":"db","port":6380,"targetPort":6380}]}}'
SVC_IP=""
for _ in $(seq 30); do
  r="$(api GET /api/v1/namespaces/default/services/redis-main)"
  SVC_IP="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("spec",{}).get("clusterIP",""))' 2>/dev/null || true)"
  [ -n "$SVC_IP" ] && break
  sleep 1
done
[ -n "$SVC_IP" ] || fail "the Service was given no clusterIP: ${r:0:300}"
echo "   cfg, sec; redis-main at $SVC_IP"

pod() { # pod <name> <spec-members> [ns]: a Pod of the probe image
  printf '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"%s","namespace":"%s","labels":{"tier":"a b"}},"spec":{%s}}' "$1" "${3:-default}" "$2"
}
declare -A U
make_pod() { # make_pod <name> <spec-members> [ns]
  pn_create "/api/v1/namespaces/${3:-default}/pods" "$(pod "$1" "$2" "${3:-default}")"
  U[$1]="$CREATED_UID"
}
field() { pn_logs "${U[$1]}" | sed -n "s/^$2=//p"; }

echo "== 4. env: Service links, envFrom, env with \$(VAR) and valueFrom; command/args expanded =="
make_pod envpod '"restartPolicy":"Never","containers":[{"name":"c","image":"nanocloud/probe:1",
  "command":["/bin/probe","env","argv"],"args":["$(GREETING)","$$(GREETING)","$(NOPE)","$(DB_user)"],
  "resources":{"limits":{"cpu":"250m","memory":"64Mi"}},
  "envFrom":[{"configMapRef":{"name":"cfg"}},{"prefix":"DB_","secretRef":{"name":"sec"}}],
  "env":[{"name":"MODE","value":"slow"},
         {"name":"GREETING","valueFrom":{"configMapKeyRef":{"name":"cfg","key":"greeting"}}},
         {"name":"TOKEN","valueFrom":{"secretKeyRef":{"name":"sec","key":"token"}}},
         {"name":"MSG","value":"$(GREETING) from $(POD_NAME)"},
         {"name":"POD_NAME","valueFrom":{"fieldRef":{"fieldPath":"metadata.name"}}},
         {"name":"POD_NS","valueFrom":{"fieldRef":{"fieldPath":"metadata.namespace"}}},
         {"name":"POD_UID","valueFrom":{"fieldRef":{"fieldPath":"metadata.uid"}}},
         {"name":"NODE","valueFrom":{"fieldRef":{"fieldPath":"spec.nodeName"}}},
         {"name":"TIER","valueFrom":{"fieldRef":{"fieldPath":"metadata.labels['"'"'tier'"'"']"}}},
         {"name":"POD_IP","valueFrom":{"fieldRef":{"fieldPath":"status.podIP"}}},
         {"name":"CPU_M","valueFrom":{"resourceFieldRef":{"resource":"limits.cpu","divisor":"1m"}}},
         {"name":"MEM_MI","valueFrom":{"resourceFieldRef":{"resource":"limits.memory","divisor":"1Mi"}}},
         {"name":"DB_URL","value":"redis://$(REDIS_MAIN_SERVICE_HOST):$(REDIS_MAIN_SERVICE_PORT)"},
         {"name":"LIT","value":"$$(MODE) $(MODE)"}]}]'
pn_wait_phase "${U[envpod]}" Succeeded
CTX="${U[envpod]}"
envv() { field envpod env | sed -n "s/^$1=//p"; }
expect "envFrom ConfigMap, overridden by env" "$(envv MODE)" "slow"
expect "MODE defined once" "$(field envpod env | grep -c '^MODE=')" "1"
expect "configMapKeyRef" "$(envv GREETING)" "hello world"
expect "secretKeyRef (decoded)" "$(envv TOKEN)" "s3cr3t"
expect "envFrom Secret with a prefix" "$(envv DB_user)/$(envv DB_token)" "admin/s3cr3t"
expect "\$(VAR) of an earlier entry; a later one left as written" "$(envv MSG)" 'hello world from $(POD_NAME)'
expect "fieldRef metadata.name" "$(envv POD_NAME)" "envpod"
expect "fieldRef metadata.namespace" "$(envv POD_NS)" "default"
expect "fieldRef metadata.uid" "$(envv POD_UID)" "${U[envpod]}"
expect "fieldRef spec.nodeName" "$(envv NODE)" "node-1"
expect "fieldRef metadata.labels" "$(envv TIER)" "a b"
expect "fieldRef status.podIP (the node's: a Pod shares its network)" "$(envv POD_IP)" "127.0.0.1"
expect "resourceFieldRef limits.cpu in 1m" "$(envv CPU_M)" "250"
expect "resourceFieldRef limits.memory in 1Mi" "$(envv MEM_MI)" "64"
expect "Service link host" "$(envv REDIS_MAIN_SERVICE_HOST)" "$SVC_IP"
expect "Service link port" "$(envv REDIS_MAIN_SERVICE_PORT)" "6380"
expect "Service link named port" "$(envv REDIS_MAIN_SERVICE_PORT_DB)" "6380"
expect "Service link docker form" "$(envv REDIS_MAIN_PORT)" "tcp://$SVC_IP:6380"
expect "Service links expand in env" "$(envv DB_URL)" "redis://$SVC_IP:6380"
expect "\$\$ escapes" "$(envv LIT)" '$(MODE) slow'
expect "the image's Env still under the container's" "$(envv IMG)" "yes"
expect "argv expanded against the environment" "$(field envpod argv | paste -sd'|' -)" '/bin/probe|env|argv|hello world|$(GREETING)|$(NOPE)|admin'
echo "   env: links, envFrom (+prefix), valueFrom (ConfigMap, Secret, fieldRef, resourceFieldRef), \$(VAR)/\$\$; argv expanded"

CTX=""
echo "== 5. a missing ConfigMap: CreateContainerConfigError, then it starts once the ConfigMap exists =="
make_pod late '"restartPolicy":"Never","containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","env"],"env":[{"name":"V","valueFrom":{"configMapKeyRef":{"name":"later","key":"v"}}}]}]'
why=""
for _ in $(seq 60); do
  why="$(pn_pod_json default late | python3 -c 'import json,sys
d=json.load(sys.stdin); cs=d.get("status",{}).get("containerStatuses") or [{}]
w=cs[0].get("state",{}).get("waiting",{}); print(w.get("reason",""), "|", w.get("message",""))' 2>/dev/null || true)"
  case "$why" in CreateContainerConfigError*) break ;; esac
  sleep 1
done
case "$why" in 'CreateContainerConfigError | configmap "later" not found') ;; *) fail "late's status: '$why' sbst='$(pn_nodekey "sandbox-status/${U[late]}")' report='$(nc_store_get "$STORE" "/podstatus/${U[late]}")' lc='$(pn_nodekey "pod-lifecycle-status/${U[late]}")' pod='$(pn_pod_json default late | head -c 1500)'" ;; esac
[ -z "$(pn_logs "${U[late]}")" ] || fail "late ran without its ConfigMap"
echo "   late: $why"
pn_create /api/v1/namespaces/default/configmaps '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"later"},"data":{"v":"arrived"}}'
pn_wait_phase "${U[late]}" Succeeded
expect "late's V" "$(field late env | sed -n 's/^V=//p')" "arrived"
echo "   late: started once the ConfigMap existed (V=arrived)"

echo "== 6. securityContext =="
make_pod user1 '"restartPolicy":"Never","securityContext":{"runAsUser":1000,"runAsGroup":3000},"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id"],"securityContext":{"runAsUser":1500}}]'
make_pod group1 '"restartPolicy":"Never","containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id"],"securityContext":{"runAsGroup":4000}}]'
make_pod nonroot-ok '"restartPolicy":"Never","securityContext":{"runAsNonRoot":true,"runAsUser":1234},"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id"]}]'
make_pod nonroot-name '"restartPolicy":"Never","securityContext":{"runAsNonRoot":true},"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id"]}]'
make_pod nonroot-root '"restartPolicy":"Never","securityContext":{"runAsNonRoot":true},"containers":[{"name":"c","image":"nanocloud/rootprobe:1","command":["/bin/probe","id"]}]'
make_pod rofs '"restartPolicy":"Never","volumes":[{"name":"w","emptyDir":{}}],"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","write","/x","y","write","/w/x","y","cat","/w/x","cat","/etc/resolv.conf"],"securityContext":{"readOnlyRootFilesystem":true},"volumeMounts":[{"name":"w","mountPath":"/w"}]}]'
make_pod merge '"restartPolicy":"Never","securityContext":{"fsGroup":6000,"supplementalGroups":[5000]},"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id"]}]'
make_pod strict '"restartPolicy":"Never","securityContext":{"fsGroup":6000,"supplementalGroups":[5000],"supplementalGroupsPolicy":"Strict"},"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id"]}]'
for p in user1 group1 nonroot-ok rofs merge strict; do pn_wait_phase "${U[$p]}" Succeeded; done
expect "user1 (container's runAsUser over the Pod's, the Pod's runAsGroup)" "$(pn_logs "${U[user1]}" | grep '^uid=' | cut -d' ' -f1,2)" "uid=1500 gid=3000"
expect "group1 (runAsGroup alone keeps the image's uid)" "$(pn_logs "${U[group1]}" | grep '^uid=' | cut -d' ' -f1,2)" "uid=1234 gid=4000"
expect "nonroot-ok" "$(pn_logs "${U[nonroot-ok]}" | grep '^uid=' | cut -d' ' -f1)" "uid=1234"
# The image's app (1234:2345) is a member of extra (7777) in its /etc/group.
expect "merge (fsGroup, supplementalGroups and the image's memberships)" "$(pn_logs "${U[merge]}" | grep '^uid=')" "uid=1234 gid=2345 groups=2345,5000,6000,7777"
expect "strict (the primary gid, fsGroup and supplementalGroups only)" "$(pn_logs "${U[strict]}" | grep '^uid=')" "uid=1234 gid=2345 groups=2345,5000,6000"
expect "rofs: a write to / refused" "$(pn_logs "${U[rofs]}" | sed -n 's|^write /x=||p')" "ERR"
expect "rofs: its volume still written" "$(pn_logs "${U[rofs]}" | sed -n 's|^write /w/x=||p')/$(pn_logs "${U[rofs]}" | sed -n 's|^cat /w/x=||p')" "ok/y"
case "$(pn_logs "${U[rofs]}" | sed -n 's|^cat /etc/resolv.conf=||p')" in nameserver*) ;; *) fail "rofs: its resolv.conf: $(pn_logs "${U[rofs]}")" ;; esac
echo "   merge/strict groups as declared; rofs: / read-only, its emptyDir and resolv.conf in place"
refused() { # refused <pod> <message substring>
  local w=""
  for _ in $(seq 60); do
    w="$(pn_pod_json default "$1" | python3 -c 'import json,sys
d=json.load(sys.stdin); cs=d.get("status",{}).get("containerStatuses") or [{}]
w=cs[0].get("state",{}).get("waiting",{}); print(w.get("reason",""), "|", w.get("message",""))' 2>/dev/null || true)"
    case "$w" in "CreateContainerConfigError | "*"$2"*) break ;; esac
    sleep 1
  done
  case "$w" in "CreateContainerConfigError | "*"$2"*) ;; *) fail "$1 not refused for '$2': '$w' sbst='$(pn_nodekey "sandbox-status/${U[$1]}")'" ;; esac
  [ -z "$(pn_logs "${U[$1]}")" ] || fail "$1 ran: $(pn_logs "${U[$1]}")"
  echo "   $1: $w"
}
refused nonroot-name "image has non-numeric user (app)"
refused nonroot-root "image will run as root"
make_pod badpol '"restartPolicy":"Never","securityContext":{"supplementalGroupsPolicy":"Loose"},"containers":[{"name":"c","image":"nanocloud/probe:1"}]'
refused badpol "supplementalGroupsPolicy"

echo "== 7. volumes and containers: init containers in order, two app containers, an emptyDir, a ConfigMap, a Secret, a hostPath =="
echo "from-the-host" >"$D/host/note"
make_pod multi '"restartPolicy":"Never","securityContext":{"runAsUser":1500},
  "volumes":[{"name":"work","emptyDir":{}},
             {"name":"cfg","configMap":{"name":"cfg","items":[{"key":"app.conf","path":"conf/app.conf"}]}},
             {"name":"allcfg","configMap":{"name":"cfg"}},
             {"name":"sec","secret":{"secretName":"sec","defaultMode":256}},
             {"name":"host","hostPath":{"path":"'"$D/host"'"}}],
  "initContainers":[
    {"name":"i0","image":"nanocloud/probe:1","command":["/bin/probe","write","/work/a","first","id"],"volumeMounts":[{"name":"work","mountPath":"/work"}]},
    {"name":"i1","image":"nanocloud/probe:1","command":["/bin/probe","cat","/work/a","write","/work/b","second"],"volumeMounts":[{"name":"work","mountPath":"/work"}]}],
  "containers":[
    {"name":"app","image":"nanocloud/probe:1","command":["/bin/probe","cat","/work/a","cat","/work/b","cat","/etc/cfg/conf/app.conf","ls","/etc/all","cat","/etc/all/greeting","mode","/etc/all/greeting","cat","/etc/sec/token","mode","/etc/sec/token","cat","/host/note","write","/work/app","x","wait","/work/side","20"],
     "volumeMounts":[{"name":"work","mountPath":"/work"},{"name":"cfg","mountPath":"/etc/cfg"},{"name":"allcfg","mountPath":"/etc/all"},{"name":"sec","mountPath":"/etc/sec"},{"name":"host","mountPath":"/host","readOnly":true}]},
    {"name":"side","image":"nanocloud/probe:1","command":["/bin/probe","wait","/work/app","20","write","/work/side","y","id"],"volumeMounts":[{"name":"work","mountPath":"/work"}]}]'
M="${U[multi]}"
pn_wait_phase "$M" Succeeded 120
for c in "~i0" "~i1" "~c1"; do pn_wait_phase "$M$c" Succeeded 60; done
expect "i0 ran as the Pod's runAsUser" "$(pn_logs "$M~i0" | grep '^uid=' | cut -d' ' -f1)" "uid=1500"
expect "i1 read what i0 wrote (in order)" "$(pn_logs "$M~i1" | sed -n 's|^cat /work/a=||p')" "first"
expect "app read the init containers' files" "$(pn_logs "$M" | sed -n 's|^cat /work/a=||p')/$(pn_logs "$M" | sed -n 's|^cat /work/b=||p')" "first/second"
expect "a ConfigMap item at its path" "$(pn_logs "$M" | sed -n 's|^cat /etc/cfg/conf/app.conf=||p')" "a=1|b=2"
expect "a ConfigMap's every key" "$(pn_logs "$M" | sed -n 's|^ls /etc/all=||p' | tr ',' '\n' | LC_ALL=C sort | paste -sd, -)" ",MODE,app.conf,greeting"
expect "a ConfigMap key's file" "$(pn_logs "$M" | sed -n 's|^cat /etc/all/greeting=||p')" "hello world"
expect "the default mode" "$(pn_logs "$M" | sed -n 's|^mode /etc/all/greeting=||p')" "644"
expect "a Secret, decoded, at defaultMode 0400 (unreadable to uid 1500)" "$(pn_logs "$M" | sed -n 's|^mode /etc/sec/token=||p') $(pn_logs "$M" | sed -n 's|^cat /etc/sec/token=||p')" "400 ERR"
expect "a hostPath" "$(pn_logs "$M" | sed -n 's|^cat /host/note=||p')" "from-the-host|"
expect "the second app container ran beside the first, sharing the emptyDir" "$(pn_logs "$M~c1" | sed -n 's|^wait /work/app=||p')/$(pn_logs "$M" | sed -n 's|^wait /work/side=||p')" "ok/ok"
expect "the emptyDir is the Pod's, under the node's pods directory (in the mount root)" "$(sudo -n cat "$D/mnt/pods/$M/volumes/work/side")" "y"
echo "   i0 -> i1 -> app + side; work (emptyDir, uid 1500 writes), cfg (item), allcfg (keys, 0644), sec (0400), host"

echo "== 7b. fsGroup: an emptyDir, a ConfigMap and a Secret owned by it; the Secret in memory =="
make_pod fsg '"restartPolicy":"Never","securityContext":{"runAsUser":1500,"fsGroup":6000},
  "volumes":[{"name":"work","emptyDir":{}},
             {"name":"cfg","configMap":{"name":"cfg","items":[{"key":"greeting","path":"g"},{"key":"MODE","path":"sub/m","mode":256}]}},
             {"name":"sec","secret":{"secretName":"sec","defaultMode":256}}],
  "containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","id","stat","/work","write","/work/f","x","stat","/work/f","stat","/etc/cfg/g","stat","/etc/cfg/sub/m","cat","/etc/cfg/sub/m","stat","/etc/sec/token","cat","/etc/sec/token","fstype","/etc/sec","sleep","600"],
    "volumeMounts":[{"name":"work","mountPath":"/work"},{"name":"cfg","mountPath":"/etc/cfg"},{"name":"sec","mountPath":"/etc/sec"}]}]'
FG="${U[fsg]}"
pn_wait_log "$FG" "fstype /etc/sec=" 90
CTX="$FG"
st() { pn_logs "$FG" | sed -n "s|^stat $1=||p"; }
expect "fsg's groups (fsGroup)" "$(pn_logs "$FG" | grep '^uid=')" "uid=1500 gid=0 groups=0,6000"
expect "the emptyDir: the fsGroup's, setgid, group-writable" "$(st /work)" "2775 6000"
expect "a file the container made there inherits the group" "$(pn_logs "$FG" | sed -n 's|^write /work/f=||p') $(st /work/f)" "ok 0644 6000"
expect "a ConfigMap file (0644): the fsGroup's, 0440 added" "$(st /etc/cfg/g)" "0644 6000"
expect "an item's mode (0400): 0440, the fsGroup's" "$(st /etc/cfg/sub/m)" "0440 6000"
expect "the item readable through the group" "$(pn_logs "$FG" | sed -n 's|^cat /etc/cfg/sub/m=||p')" "fast"
expect "a Secret at defaultMode 0400: 0440, the fsGroup's" "$(st /etc/sec/token)" "0440 6000"
expect "the Secret readable to uid 1500 through the fsGroup" "$(pn_logs "$FG" | sed -n 's|^cat /etc/sec/token=||p')" "s3cr3t"
expect "the Secret on a memory filesystem (in the container)" "$(pn_logs "$FG" | sed -n 's|^fstype /etc/sec=||p')" "1021994"
VD="$D/mnt/pods/$FG/volumes"
expect "the Secret's directory a memory mount of its own (on the node)" "$(findmnt -n -o SOURCE,FSTYPE -M "$VD/sec" | tr -s ' ')" "fluxor-projection tmpfs"
expect "the ConfigMap's directory no mount of its own" "$(findmnt -n -o SOURCE -M "$VD/cfg" || true)" ""
expect "the ConfigMap projected as upstream's AtomicWriter lays it out" "$(sudo -n readlink "$VD/cfg/g") $(sudo -n readlink "$VD/cfg/..data" | cut -c1-2)" "..data/g .."
CTX=""
echo "   fsg: emptyDir 2775:6000, ConfigMap/Secret files 0440/0644:6000, the Secret on tmpfs"

echo "== 7c. a downwardAPI and a projected volume (ConfigMap, Secret and downward API sources) =="
make_pod proj '"restartPolicy":"Never",
  "volumes":[{"name":"dw","downwardAPI":{"items":[{"path":"name","fieldRef":{"fieldPath":"metadata.name"}},{"path":"meta/labels","fieldRef":{"fieldPath":"metadata.labels"}},{"path":"mem","resourceFieldRef":{"containerName":"c","resource":"limits.memory","divisor":"1Mi"}}]}},
             {"name":"pj","projected":{"defaultMode":288,"sources":[{"configMap":{"name":"cfg","items":[{"key":"greeting","path":"g"}]}},{"secret":{"name":"sec","items":[{"key":"token","path":"s/t","mode":256}]}},{"downwardAPI":{"items":[{"path":"ns","fieldRef":{"fieldPath":"metadata.namespace"}}]}}]}}],
  "containers":[{"name":"c","image":"nanocloud/rootprobe:1","resources":{"limits":{"memory":"32Mi"}},"command":["/bin/probe","cat","/dw/name","cat","/dw/meta/labels","cat","/dw/mem","mode","/dw/name","cat","/pj/g","mode","/pj/g","cat","/pj/s/t","mode","/pj/s/t","cat","/pj/ns","sleep","600"],
    "volumeMounts":[{"name":"dw","mountPath":"/dw"},{"name":"pj","mountPath":"/pj"}]}]'
PJ="${U[proj]}"
pn_wait_log "$PJ" "cat /pj/ns=" 90
CTX="$PJ"
pv() { pn_logs "$PJ" | sed -n "s|^$1=||p"; }
expect "downwardAPI metadata.name" "$(pv 'cat /dw/name')" "proj"
expect "downwardAPI metadata.labels (key=\"value\" lines)" "$(pv 'cat /dw/meta/labels')" 'tier="a b"|'
expect "downwardAPI limits.memory in 1Mi" "$(pv 'cat /dw/mem')" "32"
expect "downwardAPI default mode" "$(pv 'mode /dw/name')" "644"
expect "projected ConfigMap item at the volume's defaultMode (0440)" "$(pv 'cat /pj/g') $(pv 'mode /pj/g')" "hello world 440"
expect "projected Secret item at its mode" "$(pv 'cat /pj/s/t') $(pv 'mode /pj/s/t')" "s3cr3t 400"
expect "projected downward API item" "$(pv 'cat /pj/ns')" "default"
VP="$D/mnt/pods/$PJ/volumes"
expect "the projected volume on a memory filesystem" "$(findmnt -n -o SOURCE,FSTYPE -M "$VP/pj" | tr -s ' ')" "fluxor-projection tmpfs"
expect "the downwardAPI volume not" "$(findmnt -n -o SOURCE -M "$VP/dw" || true)" ""
CTX=""
echo "   proj: downwardAPI (name, labels, limits.memory), projected (ConfigMap, Secret, downward API) on tmpfs"

echo "== 7d. a ConfigMap updated under a reader: each update seen, whole =="
pn_create /api/v1/namespaces/default/configmaps '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"pairs"},"data":{"a":"1","b":"1"}}'
make_pod reader '"restartPolicy":"Never","volumes":[{"name":"p","configMap":{"name":"pairs"}}],
  "containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","pair","/etc/p","120"],"volumeMounts":[{"name":"p","mountPath":"/etc/p"}]}]'
RD="${U[reader]}"
pn_wait_log "$RD" "pair=1" 90
for v in 2 3 4 5; do
  r="$(api PUT /api/v1/namespaces/default/configmaps/pairs '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"pairs"},"data":{"a":"'"$v"'","b":"'"$v"'"}}')"
  case "$r" in 200\|*) ;; *) fail "PUT pairs: ${r:0:200}" ;; esac
  pn_wait_log "$RD" "pair=$v" 60
done
case "$(pn_logs "$RD")" in *MIX*) fail "the reader saw a mix of two projections: $(pn_logs "$RD" | grep MIX | head -3)" ;; esac
expect "every update seen in order" "$(pn_logs "$RD" | sed -n 's/^pair=\([0-9]\)$/\1/p' | paste -sd, -)" "1,2,3,4,5"
echo "   reader: 1 -> 2 -> 3 -> 4 -> 5, never a mix"

echo "== 8. an init container that fails under restartPolicy Never fails the Pod =="
make_pod initfail '"restartPolicy":"Never","initContainers":[{"name":"i0","image":"nanocloud/probe:1","command":["/bin/probe","exit","3"]}],"containers":[{"name":"app","image":"nanocloud/probe:1","command":["/bin/probe","argv"]}]'
F="${U[initfail]}"
pn_wait_phase "$F~i0" Failed 60
pn_wait_phase "$F" Failed 60
[ -z "$(pn_logs "$F")" ] || fail "the app container ran after its init failed: $(pn_logs "$F")"
ph=""
for _ in $(seq 30); do
  ph="$(pn_pod_json default initfail | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",{}).get("phase",""))' 2>/dev/null || true)"
  [ "$ph" = Failed ] && break
  sleep 1
done
expect "initfail's phase" "$ph" "Failed"
echo "   initfail: i0 exit 3 -> Pod Failed, app never ran"

echo "== 9. hostPath under a namespace whose Pod Security level is baseline: refused =="
make_pod hp '"restartPolicy":"Never","volumes":[{"name":"h","hostPath":{"path":"/etc"}}],"containers":[{"name":"c","image":"nanocloud/probe:1","volumeMounts":[{"name":"h","mountPath":"/h"}]}]' strict
w=""
for _ in $(seq 60); do
  w="$(pn_pod_json strict hp | python3 -c 'import json,sys
d=json.load(sys.stdin); cs=d.get("status",{}).get("containerStatuses") or [{}]
print(cs[0].get("state",{}).get("waiting",{}).get("message",""))' 2>/dev/null || true)"
  case "$w" in *"hostPath volumes are forbidden"*) break ;; esac
  sleep 1
done
case "$w" in *"hostPath volumes are forbidden"*) echo "   strict/hp: $w" ;; *) fail "strict/hp not refused: '$w'" ;; esac

echo "== 10. pause: nanocloud.io/paused freezes the Pod, removing it thaws it =="
make_pod sleeper '"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","sleep","600"]}]'
S="${U[sleeper]}"
pn_wait_phase "$S" Running 60
r="$(api PATCH /api/v1/namespaces/default/pods/sleeper '{"metadata":{"annotations":{"nanocloud.io/paused":"true"}}}')"
case "$r" in 200\|*) ;; *) fail "PATCH paused: ${r:0:200}" ;; esac
st=""
for _ in $(seq 60); do st="$(pn_nodekey "sandbox-status/$S")"; case "$st" in *paused*) break ;; esac; sleep 1; done
# A pause is the same run: its status keeps the run's start (`started`).
[[ "$st" =~ ^state=paused\;code=0\;started=([0-9]+|m[0-9]+@([0-9a-f]{32}|-))$ ]] || fail "the paused sandbox: '$st' pause='$(pn_nodekey "sandbox-pause/$S")' spec='$(pn_nodekey "pod-specs/$S" | tr ';' '\n' | grep paused)' log='$(grep -i "pause\|freez" "$D/run.log" | tail -5)'"
RUN="${st#*;started=}"
pn_wait_phase "$S" Paused 30
cgf="$(grep -l . "$CG"/*/cgroup.freeze 2>/dev/null | xargs -r grep -l '^1$' | head -1)"
[ -n "$cgf" ] || fail "no frozen cgroup under $CG"
echo "   sleeper: paused (frozen: $cgf)"
r="$(api PATCH /api/v1/namespaces/default/pods/sleeper '{"metadata":{"annotations":{"nanocloud.io/paused":null}}}')"
case "$r" in 200\|*) ;; *) fail "PATCH unpause: ${r:0:200}" ;; esac
for _ in $(seq 60); do st="$(pn_nodekey "sandbox-status/$S")"; case "$st" in *running*) break ;; esac; sleep 1; done
expect "the resumed sandbox (the run it was paused in)" "$st" "state=running;code=0;started=$RUN"
pn_wait_phase "$S" Running 30
expect "restarts while paused" "$(pn_nodekey "pod-runtime/$S" | tr ';' '\n' | sed -n 's/^restarts=//p')" ""
echo "   sleeper: resumed, never restarted"

echo "== 11. /etc/resolv.conf by dnsPolicy: ClusterFirst, Default, None + dnsConfig =="
make_pod dns-cf '"restartPolicy":"Never","containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","cat","/etc/resolv.conf"]}]'
make_pod dns-def '"restartPolicy":"Never","dnsPolicy":"Default","containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","cat","/etc/resolv.conf"]}]'
make_pod dns-none '"restartPolicy":"Never","dnsPolicy":"None","dnsConfig":{"nameservers":["1.1.1.1"],"searches":["example.com"],"options":[{"name":"ndots","value":"2"}]},"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","cat","/etc/resolv.conf"]}]'
for p in dns-cf dns-def dns-none; do pn_wait_phase "${U[$p]}" Succeeded 60; done
resolv() { pn_logs "${U[$1]}" | sed -n 's|^cat /etc/resolv.conf=||p'; }
expect "ClusterFirst" "$(resolv dns-cf)" "nameserver 10.96.0.10|search default.svc.cluster.local svc.cluster.local cluster.local|options ndots:5|"
expect "Default (the node's own)" "$(resolv dns-def)" "$(tr '\n' '|' </etc/resolv.conf)"
expect "None + dnsConfig" "$(resolv dns-none)" "nameserver 1.1.1.1|search example.com|options ndots:2|"
echo "   ClusterFirst, Default and None + dnsConfig as the kubelet writes them"

echo "== 12. delete: every container stopped, with the grace Kubernetes gives it =="
cat >"$D/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters: [{name: e2e, cluster: {server: "https://127.0.0.1:$PORT", insecure-skip-tls-verify: true}}]
users: [{name: admin, user: {token: "$(cat "$D/token.admin")"}}]
contexts: [{name: e2e, context: {cluster: e2e, user: admin, namespace: default}}]
current-context: e2e
KC
k() { kubectl --kubeconfig "$D/kubeconfig" --request-timeout=60s "$@"; }
now() { date +%s.%N; }
since() { python3 -c "import sys; print(round(float(sys.argv[2]) - float(sys.argv[1]), 1))" "$1" "$(now)"; }
state() { pn_nodekey "sandbox-status/$1" | sed -n 's/^state=\([a-z]*\).*/\1/p'; }
stopped() { # stopped <sandbox id> [seconds]: its sandbox no longer runs
  local st=""
  for _ in $(seq "${2:-30}"); do
    st="$(state "$1")"
    case "$st" in running|paused|created) ;; *) return 0 ;; esac
    sleep 1
  done
  fail "$1 still $st: spec='$(pn_nodekey "pod-specs/$1" | cut -c1-300)'"
}
# end <sandbox id>: how its last run ended, as its node recorded it
# (pod_report's memory): "<why> <code>" — Completed 0, Error 137 (SIGKILL).
end() {
  local c="" w=""
  for _ in $(seq 20); do
    c="$(pn_nodekey "pod-cond/$1" | tr ';' '\n' | sed -n 's/^code=//p')"
    w="$(pn_nodekey "pod-cond/$1" | tr ';' '\n' | sed -n 's/^why=//p')"
    [ -n "$c" ] && break
    sleep 0.5
  done
  echo "$w $c"
}
gone() { ! api GET "/api/v1/namespaces/default/pods/$1" | grep -q '^200|'; }
pod_meta() { pn_pod_json default "$1" | python3 -c 'import json,sys; m=json.load(sys.stdin).get("metadata",{}); print(m.get("deletionGracePeriodSeconds",""), "|" if m.get("deletionTimestamp") else "-")' 2>/dev/null || true; }

echo "-- 12a. a two-container Pod: both stopped, every record of both gone with its tomb"
make_pod pair '"terminationGracePeriodSeconds":3,"containers":[
  {"name":"a","image":"nanocloud/probe:1","command":["/bin/probe","sleep","600"]},
  {"name":"b","image":"nanocloud/probe:1","command":["/bin/probe","sleep","600"]}]'
P="${U[pair]}"
pn_wait_phase "$P" Running 60
pn_wait_phase "$P~c1" Running 60
k delete pod pair --timeout=60s >"$D/del-pair.out" 2>&1 || fail "kubectl delete pod pair: $(cat "$D/del-pair.out")"
gone pair || fail "pair is still in the API after kubectl delete returned"
stopped "$P"
stopped "$P~c1"
for id in "$P" "$P~c1"; do
  case "$(pn_nodekey "pod-specs/$id")" in *desired=deleted*) ;; *) fail "$id's spec says '$(pn_nodekey "pod-specs/$id" | cut -c1-200)', not deleted" ;; esac
done
echo "   pair: both containers stopped ($(state "$P"), $(state "$P~c1")), both specs desired=deleted"
left=""
for _ in $(seq $((TOMB_S + 60))); do
  left=""
  for id in "$P" "$P~c1"; do
    for c in pod-specs pod-runtime sandboxes sandbox-status pod-lifecycle-status; do
      [ -n "$(pn_nodekey "$c/$id")" ] && left="$left $c/$id"
    done
  done
  [ -z "$left" ] && break
  sleep 1
done
[ -z "$left" ] || fail "records of the deleted Pod left after its tomb ended:$left"
echo "   pair: every node record of both containers gone once its tomb ended (${TOMB_S} s)"

echo "-- 12b. SIGTERM trapped (5 s), grace 10: it exits gracefully, kubectl waits, Terminating meanwhile"
make_pod graceful '"terminationGracePeriodSeconds":10,"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","onterm","5","sleep","600"]}]'
G="${U[graceful]}"
pn_wait_phase "$G" Running 60
pn_wait_log "$G" "onterm=armed" 30
t0="$(now)"
k delete pod graceful --timeout=60s >"$D/del-graceful.out" 2>&1 &
KD=$!
sleep 2
expect "graceful while terminating (deletionGracePeriodSeconds, deletionTimestamp)" "$(pod_meta graceful)" "10 |"
wait "$KD" || fail "kubectl delete pod graceful: $(cat "$D/del-graceful.out")"
took="$(since "$t0")"
gone graceful || fail "graceful is still in the API after kubectl delete returned"
python3 -c "import sys; sys.exit(0 if 4 <= float(sys.argv[1]) < 25 else 1)" "$took" \
  || fail "kubectl delete took ${took} s, not the ~5 s the Pod took to exit"
stopped "$G"
expect "graceful's exit" "$(end "$G")" "Completed 0"
case "$(pn_logs "$G")" in *onterm=done*) ;; *) fail "graceful never finished its SIGTERM handler: $(pn_logs "$G" | head -c 300)" ;; esac
echo "   graceful: SIGTERM, 5 s, exit 0; kubectl waited ${took} s"

echo "-- 12c. SIGTERM ignored, grace 4: killed once its grace is spent"
make_pod stubborn '"terminationGracePeriodSeconds":4,"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","ignoreterm","sleep","600"]}]'
S2="${U[stubborn]}"
pn_wait_phase "$S2" Running 60
pn_wait_log "$S2" "ignoreterm=armed" 30
t0="$(now)"
k delete pod stubborn --timeout=60s >"$D/del-stubborn.out" 2>&1 || fail "kubectl delete pod stubborn: $(cat "$D/del-stubborn.out")"
took="$(since "$t0")"
python3 -c "import sys; sys.exit(0 if 3 <= float(sys.argv[1]) < 25 else 1)" "$took" \
  || fail "stubborn was gone after ${took} s, not once its 4 s grace was spent"
stopped "$S2"
expect "stubborn's end (SIGKILL)" "$(end "$S2")" "Error 137"
echo "   stubborn: SIGTERM ignored, SIGKILL after its grace; gone after ${took} s"

echo "-- 12d. --force --grace-period=0 on a Terminating Pod (grace 60): removed and killed at once"
make_pod forced '"terminationGracePeriodSeconds":60,"containers":[{"name":"c","image":"nanocloud/probe:1","command":["/bin/probe","ignoreterm","sleep","600"]}]'
F2="${U[forced]}"
pn_wait_phase "$F2" Running 60
pn_wait_log "$F2" "ignoreterm=armed" 30
k delete pod forced --wait=false >/dev/null 2>&1 || fail "kubectl delete pod forced --wait=false"
sleep 3
expect "forced while terminating" "$(pod_meta forced)" "60 |"
expect "forced's sandbox in its grace" "$(state "$F2")" "running"
t0="$(now)"
k delete pod forced --force --grace-period=0 >"$D/del-forced.out" 2>&1 || fail "kubectl delete --force --grace-period=0: $(cat "$D/del-forced.out")"
gone forced || fail "forced is still in the API after --force --grace-period=0"
stopped "$F2" 10
took="$(since "$t0")"
python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) < 10 else 1)" "$took" || fail "forced took ${took} s to die"
expect "forced's end (SIGKILL)" "$(end "$F2")" "Error 137"
case "$(pn_nodekey "pod-specs/$F2")" in *grace=0\;*) ;; *) fail "forced's spec: '$(pn_nodekey "pod-specs/$F2" | cut -c1-200)'" ;; esac
echo "   forced: Terminating (grace 60), then --force --grace-period=0 -> gone and killed in ${took} s"

echo "-- 12e. a deleted Pod's volumes: its directory removed, its Secret's memory filesystem unmounted"
[ -d "$D/mnt/pods/$FG/volumes/work" ] || fail "fsg's volumes are not there to remove: $(sudo -n ls -la "$D/mnt/pods/$FG/volumes" 2>&1)"
k delete pod fsg --timeout=60s >"$D/del-fsg.out" 2>&1 || fail "kubectl delete pod fsg: $(cat "$D/del-fsg.out")"
for _ in $(seq $((TOMB_S + 60))); do
  sudo -n test -e "$D/mnt/pods/$FG" || break
  sleep 1
done
sudo -n test -e "$D/mnt/pods/$FG" && fail "fsg's volume directory outlived it: $(sudo -n ls -la "$D/mnt/pods/$FG/volumes" 2>&1) $(grep volume_files "$D/run.log" | tail -5)"
expect "fsg's Secret unmounted" "$(findmnt -n -o TARGET | grep -c "$D/mnt/pods/$FG" || true)" "0"
echo "   fsg: gone, and with it $D/mnt/pods/$FG (its Secret's tmpfs unmounted)"

echo "== E2E green: a Pod's spec — env, config, securityContext (groups, read-only root), volumes (fsGroup, modes, memory-backed Secrets, atomic updates), init and app containers, pause, DNS, deletion — as Kubernetes runs it =="
