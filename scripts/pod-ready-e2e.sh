#!/usr/bin/env bash
# Live E2E: what a Pod's status says, as clients read it, for Pods that
# really run — on a node in the shape a deb node runs: the control plane, its
# kubelet's chains (fluxor-podlifecycle.yaml, whole: host sandboxes, probes,
# image, lifecycle, status) and its image plane (fluxor-imagepull.yaml) on
# one store scoped to node-1, with the Endpoints chains the control plane carries
# — from an OCI image a registry stub serves:
#
#   /bin/ncsrv   a static binary: `serve <port> <delay-s>` listens after a
#              delay and answers every connection; `ready <port>` exits 0
#              once something listens there (an exec readiness probe);
#              `crash` exits 3; `done` exits 0
#
# PASS =
#   ready     a Deployment's Pod with an exec readinessProbe is Running and
#             NOT Ready (READY 0/1) until its server listens, then Ready —
#             `kubectl wait --for=condition=Ready` and `kubectl rollout
#             status` complete, the Deployment is Available
#   fields    conditions (PodScheduled, Initialized, ContainersReady, Ready,
#             each with a lastTransitionTime), containerStatuses[0] (name,
#             image, imageID = the resolved digest, ready, started,
#             restartCount, state.running.startedAt), startTime, hostIP =
#             podIP = the node's address
#   service   the Service selecting it has the Pod's address as its endpoint,
#             and the app answers there: the Pod listens on the node's
#             address
#   crash     a crashing Pod reads CrashLoopBackOff, RESTARTS growing, its
#             lastState terminated exit 3 (Error)
#   pull      an image the registry does not have: ErrImagePull, then
#             ImagePullBackOff
#   create    a command too long for its record: the runtime refuses the
#             CREATE — CreateContainerError
#   done      a Pod that exits 0 (restartPolicy Never): Completed, 0/1
#   quiet     a settled Pod's status is not written again
#
# Needs passwordless sudo: the node runs as root, as the deb's service does.
# Without it the suite skips, and NANOCLOUD_REQUIRE_E2E=1 makes that a
# failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7467
REG_PORT=5399
APP_PORT=18093

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }
command -v cc >/dev/null || { echo "FAIL: cc not found (the server is a static binary)"; exit 1; }
skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
sudo -n true 2>/dev/null || skip "passwordless sudo unavailable (the node runs as root)"
curl -s -m 2 "http://127.0.0.1:$APP_PORT/" >/dev/null 2>&1 && { echo "FAIL: something already listens on :$APP_PORT"; exit 1; }

D="$(mktemp -d /tmp/nc-podready-XXXXXX)"
chmod 755 "$D"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs" "$D/rootfs" "$D/owners"
nc_lattice_prepare "$STORE"
. "$ROOT/scripts/support/workload-node.sh"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then
    sudo -n pkill -f "$D/[c]onfig.bin" 2>/dev/null || true
    for _ in $(seq 20); do pgrep -f "$D/[c]onfig.bin" >/dev/null || break; sleep 0.5; done
    sudo -n pkill -KILL -f "$D/[c]onfig.bin" 2>/dev/null || true
  fi
  sudo -n pkill -f "$D/[r]ootfs" 2>/dev/null || true
  # The server runs pivoted into its image's root, where no path names $D.
  sudo -n pkill -KILL -x ncsrv 2>/dev/null || true
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; grep -v "WARN\|DEBUG" "$D/run.log" 2>/dev/null | tail -20 || true; exit 1; }

echo "== 1. the image: a static server and its readiness check, an OCI config =="
mkdir -p "$D/l/bin" "$D/l/tmp"
cat >"$D/srv.c" <<'EOF'
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
static int connect_to(int port) {
  int s = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in a = {0};
  a.sin_family = AF_INET; a.sin_port = htons(port); a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  int ok = connect(s, (struct sockaddr *)&a, sizeof a) == 0;
  close(s);
  return ok;
}
int main(int argc, char **argv) {
  if (argc >= 2 && strcmp(argv[1], "crash") == 0) return 3;
  if (argc >= 2 && strcmp(argv[1], "done") == 0) return 0;
  if (argc >= 3 && strcmp(argv[1], "ready") == 0) return connect_to(atoi(argv[2])) ? 0 : 1;
  if (argc >= 4 && strcmp(argv[1], "serve") == 0) {
    sleep(atoi(argv[3]));
    int s = socket(AF_INET, SOCK_STREAM, 0), one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_port = htons(atoi(argv[2])); a.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(s, (struct sockaddr *)&a, sizeof a) != 0 || listen(s, 16) != 0) return 2;
    const char *r = "HTTP/1.0 200 OK\r\nContent-Length: 6\r\n\r\nhello\n";
    for (;;) {
      int c = accept(s, 0, 0);
      if (c < 0) continue;
      char buf[512];
      (void)!read(c, buf, sizeof buf);
      (void)!write(c, r, strlen(r));
      close(c);
    }
  }
  return 64;
}
EOF
cc -static -O2 -o "$D/l/bin/ncsrv" "$D/srv.c"
printf 'root:x:0:0:root:/root:/bin/sh\n' >"$D/l/etc-passwd"
mkdir -p "$D/l/etc" && mv "$D/l/etc-passwd" "$D/l/etc/passwd" && printf 'root:x:0:\n' >"$D/l/etc/group"
mkdir -p "$D/reg/srv"
tar -czf "$D/reg/srv/layer.tgz" -C "$D/l" bin etc tmp
python3 - "$D/reg/srv" <<'PY'
import hashlib, json, os, sys
d = sys.argv[1]
def blob(b):
    h = hashlib.sha256(b).hexdigest()
    open(os.path.join(d, h), "wb").write(b)
    return h, len(b)
lh, ln = blob(open(os.path.join(d, "layer.tgz"), "rb").read())
os.remove(os.path.join(d, "layer.tgz"))
config = {"architecture": "arm64", "os": "linux", "config": {
    "Env": ["PATH=/bin"], "Entrypoint": ["/bin/ncsrv"], "Cmd": ["done"]}}
ch, cn = blob(json.dumps(config).encode())
manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + ch, "size": cn},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "sha256:" + lh, "size": ln}]}
m = json.dumps(manifest).encode()
open(os.path.join(d, "manifest.json"), "wb").write(m)
print("   nanocloud/srv:1 manifest sha256:" + hashlib.sha256(m).hexdigest()[:12] + "…")
PY
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"
wn_registry "$REG_PORT"

echo "== 2. the operator's identity, a ready node-1 that advertises 127.0.0.1, the node's image config =="
WN_NO_NODE=1 wn_seed_identity
put_text "/nodes/node-1" '{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1"},"status":{"ready":true,"addresses":[{"type":"InternalIP","address":"127.0.0.1"},{"type":"Hostname","address":"node-1"}],"conditions":[{"type":"Ready","status":"True"}]}}'
put_text "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"

echo "== 3. render the node: the plane + the kubelet + its image plane + Endpoints, on one store scoped to node-1 =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
python3 - "$D/graph.yaml" "$ROOT" node-1 "$D/blobs" "localhost:$REG_PORT" "$PORT" <<'PY' || fail "the splice did not apply"
import re, sys
out, root, node, blobs, registry, port = sys.argv[1:7]
g = open(out).read()
def sub(old, new, text):
    if text.count(old) != 1: sys.exit(f"expected 1 x {old[:50]!r}, found {text.count(old)}")
    return text.replace(old, new)
g = sub("    port: 7443\n", f"    port: {port}\n", g)
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/,/blobs/,/fetch-q/")
g = sub('      scope: ""\n      scoped: ""\n', f'      scope: "{node}"\n      scoped: "{SCOPED}"\n', g)
# The controllers no Pod here needs, left out (name prefixes), so the graph
# stays inside the runtime's config arena (as scripts/support/pod-node.sh).
DROP = ("jb_", "cj_", "pc_", "hp_", "sn_", "dv_", "dss_", "ip_", "ns_", "q_", "pdb_", "te_", "gc_")
head, rest = g.split("modules:\n", 1)
cmods, cwire = rest.split("\nwiring:\n", 1)
cblocks = re.split(r"(?m)^(?=  - name: )", cmods)
dropped = {b.split()[2] for b in cblocks if b.startswith("  - name: ") and b.split()[2].startswith(DROP)}
cmods = "".join(b for b in cblocks if not (b.startswith("  - name: ") and b.split()[2] in dropped))
wblocks = re.split(r"(?m)^(?=  - from: )", cwire)
def gone(b):
    f = re.match(r"  - from: (\S+)\n    to: (\S+)", b)
    return bool(f) and (f.group(1).split(".")[0] in dropped or f.group(2).split(".")[0] in dropped)
cwire = "".join(b for b in wblocks if not gone(b))
g = head + "modules:\n" + cmods.rstrip("\n") + "\n\nwiring:\n" + cwire
def parts(path):
    y = open(path).read()
    mods = y[y.index("modules:\n") + len("modules:\n"):y.index("\nwiring:")]
    wire = y[y.index("wiring:\n") + len("wiring:\n"):]
    blocks = [b for b in re.split(r"(?m)^(?=  - name: )", mods) if b.startswith("  - name: ")]
    pairs, frm = [], None
    for line in wire.splitlines():
        m = re.match(r"  - from: (\S+)$", line)
        if m: frm = m.group(1); continue
        m = re.match(r"    to: (\S+)$", line)
        if m and frm: pairs.append((frm, m.group(1))); frm = None
    return blocks, pairs
blocks, pairs = parts(f"{root}/packaging/debian/fluxor-podlifecycle.yaml")
kube = sub('    blobs: "/var/lib/nanocloud.io/image/blobs"\n', f'    blobs: "{blobs}"\n', "".join(blocks))
iblocks, ipairs = parts(f"{root}/packaging/debian/fluxor-imagepull.yaml")
img = "".join(iblocks)
img = sub('authority: "registry.nanocloud.io:5000"', f'authority: "{registry}"', img)
img = sub('dir: "/var/lib/nanocloud.io/image/blobs"', f'dir: "{blobs}"', img)
# Endpoints come with the control plane (its Service plane).
mods, wiring = g.split("\nwiring:\n", 1)
g = (mods.rstrip("\n") + "\n  # ── The kubelet, its image plane ──\n" + kube.rstrip("\n") + "\n"
     + img.rstrip("\n") + "\n\nwiring:\n" + wiring.rstrip("\n") + "\n"
     + "".join(f"  - from: {a}\n    to: {b}\n" for a, b in pairs + ipairs))
open(out, "w").write(g)
print(f"   spliced: {g.count(chr(10) + '  - name:')} modules")
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null

echo "== 4. run it as root =="
sudo -n env -C "$STORE" FLUXOR_SEAL_KEY=0123456789abcdef FLUXOR_OWNER_ROOT="$D/owners" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 90 || fail "the store never answered"
r=""
for _ in $(seq 90); do
  r="$(api GET /api/v1/namespaces/default/pods 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   the API answers" ;; *) fail "no API: ${r:0:200}" ;; esac
cat >"$D/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters:
- name: node
  cluster:
    server: https://127.0.0.1:$PORT
    insecure-skip-tls-verify: true
users:
- name: admin
  user:
    token: $(cat "$D/token.admin")
contexts:
- name: node
  context: {cluster: node, user: admin, namespace: default}
current-context: node
KC
k() { kubectl --kubeconfig "$D/kubeconfig" --request-timeout=20s "$@"; }

echo "== 5. kubectl apply: a Deployment with an exec readinessProbe, its Service; a crashing Pod, an unpullable one, an uncreatable one, one that completes =="
LONG="$(head -c 600 /dev/zero | tr '\0' x)"
cat >"$D/apply.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata: {name: web, namespace: default}
spec:
  replicas: 1
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec:
      nodeName: node-1
      containers:
      - name: web
        image: nanocloud/srv:1
        args: ["serve", "$APP_PORT", "6"]
        readinessProbe:
          exec: {command: ["/bin/ncsrv", "ready", "$APP_PORT"]}
          periodSeconds: 1
---
apiVersion: v1
kind: Service
metadata: {name: web, namespace: default}
spec:
  selector: {app: web}
  ports: [{port: 80, targetPort: $APP_PORT}]
---
apiVersion: v1
kind: Pod
metadata: {name: crashy, namespace: default}
spec:
  nodeName: node-1
  containers: [{name: c, image: "nanocloud/srv:1", args: ["crash"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: nopull, namespace: default}
spec:
  nodeName: node-1
  containers: [{name: c, image: "nanocloud/missing:1"}]
---
apiVersion: v1
kind: Pod
metadata: {name: uncreatable, namespace: default}
spec:
  nodeName: node-1
  containers: [{name: c, image: "nanocloud/srv:1", command: ["/bin/ncsrv", "$LONG"]}]
---
apiVersion: v1
kind: Pod
metadata: {name: once, namespace: default}
spec:
  nodeName: node-1
  restartPolicy: Never
  containers: [{name: c, image: "nanocloud/srv:1", args: ["done"]}]
YAML
out=""
for _ in $(seq 60); do
  out="$(k apply -f "$D/apply.yaml" 2>&1)" && break
  case "$out" in *ServiceUnavailable*|*Forbidden*) sleep 2 ;; *) fail "kubectl apply: $out" ;; esac
done
printf '%s\n' "$out" | sed 's/^/   /'

pods() { k get pods --no-headers 2>/dev/null || true; }
row() { pods | awk -v n="$1" '$1 ~ "^"n { print $2, $3, $4; exit }'; }
wait_row() { # wait_row <name-prefix> <regex over "READY STATUS RESTARTS"> <secs>
  local got=""
  for _ in $(seq "$3"); do
    got="$(row "$1")"
    [[ "$got" =~ $2 ]] && { echo "   $1: READY/STATUS/RESTARTS = $got"; return 0; }
    sleep 1
  done
  k get pods 2>&1 | sed 's/^/   /'
  fail "$1: '$got' never matched /$2/"
}
pod_json() { k get pod "$1" -o json 2>/dev/null; }
WEB=""
for _ in $(seq 60); do
  WEB="$(k get pods -l app=web -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [ -n "$WEB" ] && break
  sleep 1
done
[ -n "$WEB" ] || fail "the Deployment made no Pod"

echo "== 6. Running before Ready: the readiness probe holds it at 0/1 until its server listens =="
wait_row "$WEB" '^0/1 Running 0$' 120
avail="$(k get deploy web -o jsonpath='{.status.availableReplicas}')"
[ "${avail:-0}" = 0 ] || fail "the Deployment counts a Pod available before it is Ready: $avail"
ready_before="$(pod_json "$WEB" | python3 -c 'import json,sys; c={x["type"]:x for x in json.load(sys.stdin)["status"]["conditions"]}; print(c["Ready"]["status"], c["Ready"].get("reason",""))')"
[ "$ready_before" = "False ContainersNotReady" ] || fail "Ready condition before the probe passes: '$ready_before'"
echo "   Ready False (ContainersNotReady); deploy/web availableReplicas 0"
k wait --for=condition=Ready "pod/$WEB" --timeout=90s >"$D/wait.out" 2>&1 || fail "kubectl wait --for=condition=Ready: $(cat "$D/wait.out")"
echo "   $(cat "$D/wait.out")"
k rollout status deployment/web --timeout=60s >"$D/rollout.out" 2>&1 || fail "kubectl rollout status: $(cat "$D/rollout.out")"
echo "   $(tail -1 "$D/rollout.out")"
k wait --for=condition=Available deployment/web --timeout=30s >/dev/null 2>&1 || fail "deploy/web never Available"
wait_row "$WEB" '^1/1 Running 0$' 30

echo "== 7. the Pod's status, field by field =="
DIGEST="$(nc_store_get "$STORE" "/~node/node-1/pod-resolved/$(pod_json "$WEB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')" | tr ';' '\n' | sed -n 's/^digest=//p')"
[ -n "$DIGEST" ] || fail "no resolved digest for $WEB"
pod_json "$WEB" | python3 -c '
import json, sys, re
p = json.load(sys.stdin); st = p["status"]; dig = sys.argv[1]
RFC = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
cond = {c["type"]: c for c in st["conditions"]}
for t in ("PodScheduled", "Initialized", "ContainersReady", "Ready"):
    assert cond[t]["status"] == "True" and RFC.match(cond[t]["lastTransitionTime"]), cond[t]
assert cond["Ready"]["lastTransitionTime"] >= cond["PodScheduled"]["lastTransitionTime"], cond
assert st["phase"] == "Running" and RFC.match(st["startTime"]), st
assert st["hostIP"] == "127.0.0.1" and st["podIP"] == "127.0.0.1" and st["podIPs"] == [{"ip": "127.0.0.1"}], st
cs = st["containerStatuses"][0]
assert cs["name"] == "web" and cs["image"] == "nanocloud/srv:1", cs
assert cs["imageID"] == "sha256:" + dig, (cs["imageID"], dig)
assert cs["ready"] is True and cs["started"] is True and cs["restartCount"] == 0, cs
assert RFC.match(cs["state"]["running"]["startedAt"]) and cs["lastState"] == {}, cs
print("   conditions all True with times; phase Running since", st["startTime"], "; hostIP=podIP 127.0.0.1")
print("   containerStatuses[0]: web nanocloud/srv:1 imageID sha256:" + dig[:12] + "… ready started restartCount 0 running since", cs["state"]["running"]["startedAt"])
' "$DIGEST" || fail "$WEB: $(pod_json "$WEB" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["status"]))')"

echo "== 8. the Service reaches it: its endpoint is the Pod's address, where it listens =="
ep=""
for _ in $(seq 60); do
  ep="$(nc_store_get "$STORE" /endpoints/default/web)"
  [ "$ep" = "$WEB=127.0.0.1" ] && break
  sleep 1
done
[ "$ep" = "$WEB=127.0.0.1" ] || fail "/endpoints/default/web = '$ep', want '$WEB=127.0.0.1'"
body="$(curl -s -m 3 "http://${ep#*=}:$APP_PORT/")"
[ "$body" = "hello" ] || fail "the Pod does not answer at its endpoint ${ep#*=}:$APP_PORT: '$body'"
echo "   /endpoints/default/web = $ep; GET http://${ep#*=}:$APP_PORT/ -> $body"

echo "== 9. a crashing Pod: CrashLoopBackOff, RESTARTS growing, lastState exit 3 =="
wait_row crashy '^0/1 CrashLoopBackOff [2-9]' 120
pod_json crashy | python3 -c '
import json, sys
st = json.load(sys.stdin)["status"]; cs = st["containerStatuses"][0]
assert st["phase"] == "Running", st["phase"]
assert cs["state"]["waiting"]["reason"] == "CrashLoopBackOff", cs
t = cs["lastState"]["terminated"]
assert t["exitCode"] == 3 and t["reason"] == "Error" and t["startedAt"] and t["finishedAt"], t
assert t["startedAt"] <= t["finishedAt"], t
assert cs["ready"] is False and cs["restartCount"] >= 2, cs
print("   waiting CrashLoopBackOff; lastState terminated exit", t["exitCode"], t["reason"], t["startedAt"], "->", t["finishedAt"], "; restartCount", cs["restartCount"])
' || fail "crashy: $(pod_json crashy | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["status"]))')"

echo "== 10. an unpullable image: ErrImagePull, then ImagePullBackOff =="
seen=""
for _ in $(seq 120); do
  s="$(row nopull | awk '{print $2}')"
  case "$s" in ErrImagePull|ImagePullBackOff) case " $seen " in *" $s "*) ;; *) seen="$seen $s" ;; esac ;; esac
  case "$seen" in *ImagePullBackOff*) break ;; esac
  sleep 1
done
case "$seen" in *ImagePullBackOff*) ;; *) k get pods 2>&1 | sed 's/^/   /'; fail "nopull never read ImagePullBackOff (saw:$seen)" ;; esac
echo "   nopull STATUS:$seen"
pod_json nopull | python3 -c '
import json, sys
st = json.load(sys.stdin)["status"]; cs = st["containerStatuses"][0]
assert st["phase"] == "Pending", st
w = cs["state"]["waiting"]
assert w["reason"] in ("ErrImagePull", "ImagePullBackOff") and "nanocloud/missing:1" in w["message"], w
assert cs["imageID"] == "" and cs["ready"] is False, cs
print("   waiting", w["reason"] + ":", w["message"])
' || fail "nopull: $(pod_json nopull | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["status"]))')"

echo "== 11. an uncreatable container: CreateContainerError =="
wait_row uncreatable '^0/1 CreateContainerError ' 120
pod_json uncreatable | python3 -c '
import json, sys
st = json.load(sys.stdin)["status"]; w = st["containerStatuses"][0]["state"]["waiting"]
assert w["reason"] == "CreateContainerError" and "-22" in w["message"], w
print("   waiting CreateContainerError:", w["message"])
' || fail "uncreatable: $(pod_json uncreatable | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["status"]))')"

echo "== 12. a Pod that completes: Completed, 0/1 =="
wait_row once '^0/1 Completed 0$' 120
pod_json once | python3 -c '
import json, sys
st = json.load(sys.stdin)["status"]; t = st["containerStatuses"][0]["state"]["terminated"]
cond = {c["type"]: c for c in st["conditions"]}
assert st["phase"] == "Succeeded" and t["exitCode"] == 0 and t["reason"] == "Completed", st
assert cond["Ready"]["status"] == "False" and cond["Ready"]["reason"] == "PodCompleted", cond["Ready"]
print("   Succeeded; terminated exit 0 Completed; Ready False (PodCompleted)")
' || fail "once: $(pod_json once | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["status"]))')"
k get pods -o wide 2>&1 | sed 's/^/   /'

echo "== 13. a settled Pod's status is not written again =="
rv() { pod_json "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["resourceVersion"])'; }
R1="$(rv "$WEB")"; R2="$(rv once)"
sleep 8
[ "$(rv "$WEB")" = "$R1" ] || fail "$WEB was written again while settled: rv $R1 -> $(rv "$WEB")"
[ "$(rv once)" = "$R2" ] || fail "once was written again while settled: rv $R2 -> $(rv once)"
echo "   $WEB rv $R1 and once rv $R2 unchanged over 8 s (crashy and nopull still moving)"

echo "== E2E green: Running before Ready, Ready by its probe, wait/rollout status, Service endpoint at the node's address, CrashLoopBackOff, ErrImagePull/ImagePullBackOff, CreateContainerError, Completed — and a settled status writes nothing =="
