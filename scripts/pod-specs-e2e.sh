#!/usr/bin/env bash
# A scheduled Pod as its node's ask: the control plane projects
# /pods/<ns>/<name> bound to a node into the flat /pod-specs/<uid> record the
# node's pod-lifecycle chain runs — one per container (<uid>, <uid>~c<n>,
# <uid>~i<n>) — and a Pod deleted outright (its tomb under
# index entry under /~tomb-at/pods/) into desired=deleted. Each lands in its node's scope,
# /~node/<node>/pod-specs/<uid> (a worker's store names it /pod-specs/<uid>).
# A container Pod's records are pod_resolve's (_shared/podres.rs), a
# graph-backed Pod's and every tomb's modules/app/_chronicle/pod_specs.uproc.
#
#   spec   /pods/ (scheduled) -> /pod-specs/<uid>[~<container>]
#          node image pull restart grace desired cmd args env workdir cpu mem
#          eph tasks pod rp_cmd ruid rgid nonroot volumes gate ctr paused
#          cfgerr — restartPolicy and the grace period defaulted as the API
#          server defaults them, command and args apart, env/workingDir and
#          the resource limits (Kubernetes quantities) in the record's own
#          grammar, a missing Secret a config error
#   gone   /~tomb-at/pods/     -> /pod-specs/<uid> desired=deleted, and
#          every <uid>~… record of its other containers with it
#          (store_effect PUT_FAMILY), with the delete's grace (the tomb's
#          deletionGracePeriodSeconds: 0 for a forced delete), else the
#          Pod's terminationGracePeriodSeconds, else 30 (the tomb's index
#          entry: small whatever the Pod's size, its identity at the tomb's
#          paths — _shared/tomb.rs)
#
# Every record is asserted WHOLE: a field that moved, vanished or gained a
# byte fails, as it would for the node that reads it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect pod_resolve; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-podspecs-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
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
store_last() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}
# how many records were written for <key> (PUT-if-changed must not spend revisions)
store_writes() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}

DEC_SPEC="$(nc_decision "$ROOT/modules/app/_chronicle/pod_specs.uproc" spec_p)"
DEC_GONE="$(nc_decision "$ROOT/modules/app/_chronicle/pod_specs.uproc" gone_p)"
for v in "$DEC_SPEC" "$DEC_GONE"; do
  [ -n "$v" ] || { echo "FAIL: a pod_specs.uproc entry did not compile"; exit 1; }
done
# The projection the control plane ships, not a copy of it.
PS_PATHS="$(python3 - "$ROOT/packaging/debian/fluxor-controlplane.yaml" <<'PY'
import re, sys
g = open(sys.argv[1]).read()
blk = g[g.index("  - name: ps_pods\n"):]
blk = blk[:blk.index("\n  - name: ", 5)]
print(re.search(r'paths: "([^"]+)"', blk).group(1))
PY
)"
[ -n "$PS_PATHS" ] || { echo "FAIL: no ps_pods paths in the control plane"; exit 1; }
PS_TOMB_PATHS="$(python3 - "$ROOT/packaging/debian/fluxor-controlplane.yaml" <<'PY'
import re, sys
g = open(sys.argv[1]).read()
blk = g[g.index("  - name: ps_tombs\n"):]
blk = blk[:blk.index("\n  - name: ", 5)]
print(re.search(r'paths: "([^"]+)"', blk).group(1))
PY
)"
[ -n "$PS_TOMB_PATHS" ] || { echo "FAIL: no ps_tombs paths in the control plane"; exit 1; }
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: ps_pods
    type: store_source
    prefix: "/pods/"
    paths: "${PS_PATHS}"
  - name: ps_spec
    type: store_effect
    decision: {hex: "${DEC_SPEC}"}
    key_prefix: "/~node/"
  - name: ps_resolve
    type: pod_resolve
  - name: ps_rput
    type: store_effect
    key_prefix: "/"
  - name: ps_tombs
    type: store_source
    prefix: "/~tomb-at/pods/"
    paths: "${PS_TOMB_PATHS}"
  - name: ps_gone
    type: store_effect
    decision: {hex: "${DEC_GONE}"}
    key_prefix: "/~node/"
wiring:
  - from: ps_pods.record_out
    to: ps_resolve.request_in
  - from: ps_resolve.forward_out
    to: ps_spec.request_in
  - from: ps_resolve.request_out
    to: ps_rput.request_in
  - from: ps_tombs.record_out
    to: ps_gone.request_in
YAML

run() { # run <seconds>
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  sleep "$1"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}
# is <key> <want>: the record at <key> is exactly <want>.
is() {
  local got
  got="$(store_last "$1")"
  [ "$got" = "$2" ] || fail "$1:
   got  '$got'
   want '$2'"
}

echo "== 1. Pods: scheduled, unscheduled, being deleted, deleted outright; defaulted; owned =="
store_put "/pods/default/web" '{"metadata":{"name":"web","namespace":"default","uid":"uid-web"},"spec":{"nodeName":"node-a","restartPolicy":"OnFailure","terminationGracePeriodSeconds":5,"containers":[{"name":"web","image":"busybox:1.36","imagePullPolicy":"IfNotPresent","command":["sh","-c"],"args":["echo hi; sleep 9"],"workingDir":"/srv/w;x","env":[{"name":"A","value":"1"},{"name":"B","value":"x, y; 100%"},{"name":"S","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}},{"name":"E"}],"resources":{"limits":{"cpu":"500m","memory":"128Mi","ephemeral-storage":"1Gi"},"requests":{"cpu":"9"}}}]}}'
store_put "/pods/default/wait" '{"metadata":{"name":"wait","namespace":"default","uid":"uid-wait"},"spec":{"containers":[{"name":"wait","image":"busybox:1.36"}]}}'
store_put "/pods/default/bye" '{"metadata":{"name":"bye","namespace":"default","uid":"uid-bye","deletionTimestamp":"2026-10-03T00:00:00Z"},"spec":{"nodeName":"node-b","containers":[{"name":"bye","image":"busybox:1.36","command":["sleep","60"]}]}}'
store_put "/~tomb/pods/default/gone" '{"deadline":"1","obj":{"metadata":{"name":"gone","namespace":"default","uid":"uid-gone"},"spec":{"nodeName":"node-a"}}}'
store_put "/~tomb-at/pods/default/gone" '{"deadline":"1","ttl":300000,"obj":{"metadata":{"name":"gone","namespace":"default","uid":"uid-gone"},"spec":{"nodeName":"node-a","terminationGracePeriodSeconds":12}}}'
# Its other containers' records, as pod_resolve left them running — and
# another uid that merely begins with its own.
store_put "/~node/node-a/pod-specs/uid-gone~c1" "node=node-a;image=busybox:1.36;restart=always;grace=12;desired=running;cmd=sleep 60;ctr=side"
store_put "/~node/node-a/pod-specs/uid-gone~i0" "node=node-a;image=busybox:1.36;restart=onfailure;grace=12;desired=running;cmd=true;ctr=setup"
store_put "/~node/node-a/pod-specs/uid-gone2" "node=node-a;grace=30;desired=running;cmd=sleep 60"
# Deleted with --force --grace-period=0: the tomb states the delete's grace.
store_put "/~tomb-at/pods/default/forced" '{"deadline":"1","ttl":300000,"obj":{"metadata":{"name":"forced","namespace":"default","uid":"uid-forced","deletionGracePeriodSeconds":0},"spec":{"nodeName":"node-a","terminationGracePeriodSeconds":12}}}'
# A Pod past a record (its spec 6 KiB): its tomb no source can read whole,
# its index entry every source can — the writer prunes it to the identity.
HUGE_ENV="$(python3 -c 'import json;print(json.dumps([{"name":"E%03d"%i,"value":"v"*40} for i in range(120)]))')"
HUGE='{"metadata":{"name":"huge","namespace":"default","uid":"uid-huge"},"spec":{"nodeName":"node-b","containers":[{"name":"c","image":"busybox:1.36","env":'"$HUGE_ENV"'}]}}'
[ "${#HUGE}" -gt 4096 ] || fail "the huge Pod is not past a record (${#HUGE} bytes)"
store_put "/~tomb/pods/default/huge" '{"deadline":"1","obj":'"$HUGE"'}'
store_put "/~tomb-at/pods/default/huge" '{"deadline":"1","ttl":300000,"obj":{"metadata":{"name":"huge","namespace":"default","uid":"uid-huge"},"spec":{"nodeName":"node-b","containers":[{"image":"busybox:1.36"}]}}}'
# No restartPolicy, no grace, args only, quantities in every spelling.
store_put "/pods/default/dflt" '{"metadata":{"name":"dflt","namespace":"default","uid":"uid-dflt","ownerReferences":[{"kind":"ReplicaSet","name":"web-1"}]},"spec":{"nodeName":"node-a","containers":[{"name":"c","image":"nanocloud/app:1","args":["--port","80"],"resources":{"limits":{"cpu":"2","memory":"1e9","ephemeral-storage":"1.5Gi"}}}]}}'
# A Job's Pod keeps the policy its template must state.
store_put "/pods/default/job-0" '{"metadata":{"name":"job-0","namespace":"default","uid":"uid-job","ownerReferences":[{"kind":"Job","name":"job"}]},"spec":{"nodeName":"node-a","restartPolicy":"Never","containers":[{"name":"c","image":"busybox:1.36","command":["true"]}]}}'
# A graph-backed Pod defaults the same way and carries its limits.
store_put "/pods/default/g" '{"metadata":{"name":"g","namespace":"default","uid":"uid-g"},"spec":{"nodeName":"node-a","runtimeClassName":"fluxor","containers":[{"name":"g","image":"probe","resources":{"limits":{"cpu":"100m","memory":"64Mi"}}}]}}'
store_put "/pods/default/hnet" '{"metadata":{"name":"hnet","namespace":"default","uid":"uid-hnet"},"spec":{"nodeName":"node-a","hostNetwork":true,"runtimeClassName":"fluxor","containers":[{"name":"k","image":"nanocloud/kv:1"}]}}'
store_put "/pods/default/wimg" '{"metadata":{"name":"wimg","namespace":"default","uid":"uid-wimg"},"spec":{"nodeName":"node-a","restartPolicy":"Never","runtimeClassName":"fluxor","containers":[{"name":"w","image":"nanocloud/probe:1","resources":{"limits":{"ephemeral-storage":"10Mi"}}}]}}'
# An exec readinessProbe rides along for the node's probe chain; a
# graph-backed Pod has no exec seam and carries none.
store_put "/pods/default/rdy" '{"metadata":{"name":"rdy","namespace":"default","uid":"uid-rdy"},"spec":{"nodeName":"node-a","containers":[{"name":"c","image":"busybox:1.36","command":["sleep","60"],"readinessProbe":{"exec":{"command":["cat","/tmp/ready now"]},"periodSeconds":2,"timeoutSeconds":3,"failureThreshold":4,"successThreshold":1}}]}}'
store_put "/pods/default/live" '{"metadata":{"name":"live","namespace":"default","uid":"uid-live"},"spec":{"nodeName":"node-a","containers":[{"name":"c","image":"busybox:1.36","livenessProbe":{"exec":{"command":["sh","-c","true"]},"initialDelaySeconds":5},"readinessProbe":{"httpGet":{"path":"/","port":80}}}]}}'
# Init containers and a second app container: a record each, ordered.
store_put "/pods/default/two" '{"metadata":{"name":"two","namespace":"default","uid":"uid-two","annotations":{"nanocloud.io/paused":"true"}},"spec":{"nodeName":"node-a","restartPolicy":"Never","initContainers":[{"name":"setup","image":"busybox:1.36","command":["true"]}],"containers":[{"name":"a","image":"busybox:1.36","command":["sleep","1"]},{"name":"b","image":"busybox:1.36","command":["sleep","2"]}]}}'
run 6
is /~node/node-a/pod-specs/uid-two "node=node-a;image=busybox:1.36;pull=;restart=never;grace=30;desired=running;cmd=sleep 1;args=;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/two;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=1;ord=;ctr=a;paused=true;cfgerr="
is /~node/node-a/pod-specs/uid-two~c1 "node=node-a;image=busybox:1.36;pull=;restart=never;grace=30;desired=running;cmd=sleep 2;args=;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/two;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=1;ord=;ctr=b;paused=true;cfgerr="
is /~node/node-a/pod-specs/uid-two~i0 "node=node-a;image=busybox:1.36;pull=;restart=never;grace=30;desired=running;cmd=true;args=;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/two;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=1;ctr=setup;paused=true;cfgerr="
[ -z "$(store_last /~node/node-a/pod-specs/uid-two~c2)" ] || fail "a container the Pod does not have"
echo "   two:  a record per container — <uid> (a), <uid>~c1 (b), <uid>~i0 (setup, gate 0) — the app's waiting on 1 init container; paused carried"
is /~node/node-a/pod-specs/uid-rdy "node=node-a;image=busybox:1.36;pull=;restart=always;grace=30;desired=running;cmd=sleep 60;args=;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/rdy;rp_cmd=cat /tmp/ready%20now;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=c;paused=;cfgerr="
is /~node/node-a/pod-probes/uid-rdy "rp_cmd=cat /tmp/ready%20now;rp_period=2;rp_timeout=3;rp_fail=4;rp_succ=1;rp_delay=;lp_cmd=;lp_period=;lp_timeout=;lp_fail=;lp_succ=;lp_delay=;sp_cmd=;sp_period=;sp_timeout=;sp_fail=;sp_succ=;sp_delay="
is /~node/node-a/pod-probes/uid-live "rp_cmd=@http 127.0.0.1 80 /;rp_period=;rp_timeout=;rp_fail=;rp_succ=;rp_delay=;lp_cmd=sh -c true;lp_period=;lp_timeout=;lp_fail=;lp_succ=;lp_delay=5;sp_cmd=;sp_period=;sp_timeout=;sp_fail=;sp_succ=;sp_delay="
[ -z "$(store_last /~node/node-a/pod-probes/uid-web)" ] || fail "a Pod with no probe has a probes record"
[ -z "$(store_last /~node/node-a/pod-probes/uid-g)" ] || fail "a graph-backed Pod has a probes record"
echo "   rdy, live: exec and httpGet probes as /pod-probes/ (unstated fields empty: the probe chain's defaults); none for a Pod without, or a graph-backed one"
echo "   rdy:  its exec readinessProbe's command as rp_cmd (argv): the readiness gate"
is /~node/node-a/pod-specs/uid-web 'node=node-a;image=busybox:1.36;pull=IfNotPresent;restart=onfailure;grace=5;desired=running;cmd=sh -c;args=echo%20hi%3B%20sleep%209;env=A=1,B=x%2C y%3B 100%25,E=;workdir=/srv/w%3Bx;cpu=500;mem=134217728;eph=1073741824;tasks=4194304;pod=default/web;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=web;paused=;cfgerr=secret \"s\" not found'
echo "   web:  command and args apart, each keeping its bounds; env, workingDir, limits as millicores/bytes; its missing Secret a config error"
[ -z "$(store_last /~node//pod-specs/uid-wait)" ] || fail "an unscheduled Pod is no node's ask"
echo "   wait: unscheduled, no record"
is /~node/node-b/pod-specs/uid-bye "node=node-b;image=busybox:1.36;pull=;grace=30;desired=deleted;cmd=sleep 60;args=;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/bye;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=bye;paused=;cfgerr="
echo "   bye:  being deleted -> desired=deleted, the default 30 s grace"
is /~node/node-a/pod-specs/uid-gone "node=node-a;grace=12;desired=deleted;cmd="
is /~node/node-a/pod-specs/uid-gone~c1 "node=node-a;grace=12;desired=deleted;cmd="
is /~node/node-a/pod-specs/uid-gone~i0 "node=node-a;grace=12;desired=deleted;cmd="
is /~node/node-a/pod-specs/uid-gone2 "node=node-a;grace=30;desired=running;cmd=sleep 60"
echo "   gone: deleted outright (its tomb) -> desired=deleted for every container (<uid>, ~c1, ~i0), its own 12 s grace; another uid untouched"
is /~node/node-a/pod-specs/uid-forced "node=node-a;grace=0;desired=deleted;cmd="
echo "   forced: --force --grace-period=0 (the tomb's deletionGracePeriodSeconds 0) -> grace 0"
is /~node/node-b/pod-specs/uid-huge "node=node-b;grace=30;desired=deleted;cmd="
echo "   huge: a Pod past a record (6 KiB), deleted outright -> desired=deleted, the default 30 s grace, from its tomb's index"
is /~node/node-a/pod-specs/uid-dflt "node=node-a;image=nanocloud/app:1;pull=;restart=always;grace=30;desired=running;cmd=;args=--port 80;env=;workdir=;cpu=2000;mem=1000000000;eph=1610612736;tasks=4194304;pod=default/dflt;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=c;paused=;cfgerr="
echo "   dflt: no restartPolicy -> always (a ReplicaSet's Pod), no grace -> 30; args with no command"
is /~node/node-a/pod-specs/uid-job "node=node-a;image=busybox:1.36;pull=;restart=never;grace=30;desired=running;cmd=true;args=;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/job-0;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=c;paused=;cfgerr="
echo "   job-0: a Job's Never kept"
is /~node/node-a/pod-specs/uid-g "node=node-a;graph=probe;restart=always;grace=0;desired=running;cmd=;cpu=100;mem=67108864;eph=;pod=default/g;paused=;hostnet="
is /~node/node-a/pod-specs/uid-wimg "node=node-a;graph=nanocloud/probe:1;image=nanocloud/probe:1;restart=never;grace=0;desired=running;cmd=;cpu=;mem=;eph=10485760;pod=default/wimg;paused=;hostnet="
# hostNetwork: the node's network, no address of its own (node_net and the
# metal sandbox read hostnet=true).
is /~node/node-a/pod-specs/uid-hnet "node=node-a;graph=nanocloud/kv:1;image=nanocloud/kv:1;restart=always;grace=0;desired=running;cmd=;cpu=;mem=;eph=;pod=default/hnet;paused=;hostnet=true"
echo "   g, wimg: graph-backed Pods defaulted alike, their limits carried"

echo "== 2. a pass with nothing changed spends no revision =="
W0="$(store_writes /~node/node-a/pod-specs/uid-web)"
run 3
[ "$(store_writes /~node/node-a/pod-specs/uid-web)" = "$W0" ] || fail "an unchanged Pod rewrote its spec"
echo "   uid-web: $W0 write(s), unchanged"

echo "== 3. the Pod's image changes; then it is marked for deletion =="
store_put "/pods/default/web" '{"metadata":{"name":"web","namespace":"default","uid":"uid-web"},"spec":{"nodeName":"node-a","restartPolicy":"OnFailure","terminationGracePeriodSeconds":5,"containers":[{"name":"web","image":"busybox:1.37","command":["sh","-c"],"args":["echo hi; sleep 9"]}]}}'
run 4
is /~node/node-a/pod-specs/uid-web "node=node-a;image=busybox:1.37;pull=;restart=onfailure;grace=5;desired=running;cmd=sh -c;args=echo%20hi%3B%20sleep%209;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/web;rp_cmd=;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=web;paused=;cfgerr="
echo "   image=busybox:1.37"
store_put "/pods/default/web" '{"metadata":{"name":"web","namespace":"default","uid":"uid-web","deletionTimestamp":"2026-10-03T00:00:00Z"},"spec":{"nodeName":"node-a","restartPolicy":"OnFailure","terminationGracePeriodSeconds":5,"containers":[{"name":"web","image":"busybox:1.37","command":["sh","-c"],"args":["echo hi; sleep 9"]}]}}'
store_put "/pods/default/g" '{"metadata":{"name":"g","namespace":"default","uid":"uid-g","deletionTimestamp":"2026-10-03T00:00:00Z"},"spec":{"nodeName":"node-a","runtimeClassName":"fluxor","containers":[{"name":"g","image":"probe"}]}}'
run 4
is /~node/node-a/pod-specs/uid-web "node=node-a;image=busybox:1.37;pull=;grace=5;desired=deleted;cmd=sh -c;args=echo%20hi%3B%20sleep%209;env=;workdir=;cpu=;mem=;eph=;tasks=4194304;pod=default/web;ruid=;rgid=;nonroot=;volumes=/etc/resolv.conf:/etc/resolv.conf:ro;gate=0;ord=;ctr=web;paused=;cfgerr="
echo "   desired=deleted, its own 5 s grace"
is /~node/node-a/pod-specs/uid-g "node=node-a;graph=probe;grace=0;desired=deleted;cmd=;cpu=;mem=;eph=;pod=default/g"
echo "   a graph-backed Pod being deleted still names its graph (its metal sandbox is torn down)"

echo "== E2E green: scheduled Pods projected to their node's ask, exactly: defaults, command/args, env, workingDir, limits; deletion carried =="
