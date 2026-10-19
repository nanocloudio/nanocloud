#!/usr/bin/env bash
# Live E2E: what a Kubernetes Pod spec becomes as a process. Pods are created
# through the API of a node in the shape a deb node runs — the control plane,
# its kubelet's chains (fluxor-podlifecycle.yaml, whole) and its image plane
# (fluxor-imagepull.yaml) on one store scoped to the node — from an OCI image
# a registry stub serves:
#
#   /bin/probe   a static binary that prints its argv, cwd, uid/gid/groups,
#                every environment entry and its cgroup
#   config       Env [PATH=/bin, IMG_ONLY=…, SHARED=image], Entrypoint
#                [/bin/probe, ep], Cmd [cmd1, cmd2], WorkingDir /srv/app (not
#                in the image: the runtime creates it), User app (a name the
#                image's own /etc/passwd resolves: 1234:2345, + group extra)
#
# PASS =
#   env      the process's environment is EXACTLY HOSTNAME=<pod>, the image's
#            Env overridden by the container's env in order, HOME from the
#            image's passwd — none of the runtime's (a sentinel and every
#            FLUXOR_* the runtime was started with are absent)
#   cwd      the image's WorkingDir (created), or the container's workingDir
#   user     the image's User, resolved in the image's own /etc/passwd and
#            /etc/group, with only that user's groups
#   argv     command/args against Entrypoint/Cmd by the Kubernetes rules:
#            neither, command only, args only, both
#   root     the image's rootfs is the process's root (the probe exists
#            only there)
#   restart  a Deployment's Pod (no restartPolicy: Always) that crashes is
#            restarted
#   limits   cpu/memory limits land in the Pod's cgroup (cpu.max,
#            memory.max) and ephemeral-storage becomes its owner's storage
#            cap (the runtime's admission line)
#
# Needs passwordless sudo: the node runs as root, as the deb's service does
# (pivot_root, setuid, cgroups). Without it the suite skips, and
# NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7463
REG_PORT=5397

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

D="$(mktemp -d /tmp/nc-podproc-XXXXXX)"
chmod 755 "$D"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs" "$D/rootfs" "$D/owners"
nc_lattice_prepare "$STORE"
. "$ROOT/scripts/support/workload-node.sh"
CG="/sys/fs/cgroup/nc-podproc-$$"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  # The runtime itself (sudo does not always pass a TERM on), then whatever
  # of its Pods outlived it. `[c]`/`[r]`: a pattern that does not match the
  # sudo running pkill.
  if [ -n "$RUNTIME_PID" ]; then
    sudo -n pkill -f "$D/[c]onfig.bin" 2>/dev/null || true
    for _ in $(seq 20); do pgrep -f "$D/[c]onfig.bin" >/dev/null || break; sleep 0.5; done
    sudo -n pkill -KILL -f "$D/[c]onfig.bin" 2>/dev/null || true
  fi
  sudo -n pkill -f "$D/[r]ootfs" 2>/dev/null || true
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -d "$CG" ]; then
    for c in "$CG"/*/; do [ -d "$c" ] && sudo -n rmdir "$c" 2>/dev/null || true; done
    sudo -n rmdir "$CG" 2>/dev/null || true
  fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; grep -v "WARN\|DEBUG" "$D/run.log" 2>/dev/null | tail -30 || true; exit 1; }

echo "== 1. the image: a static probe, its user databases, an OCI config =="
mkdir -p "$D/l/bin" "$D/l/etc"
cat >"$D/probe.c" <<'EOF'
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <grp.h>
extern char **environ;
int main(int argc, char **argv) {
  char buf[512];
  gid_t g[32];
  int n = getgroups(32, g), crash = 0;
  for (int i = 0; i < argc; i++) {
    printf("argv=%s\n", argv[i]);
    if (strcmp(argv[i], "crash") == 0) crash = 1;
  }
  printf("cwd=%s\n", getcwd(buf, sizeof buf) ? buf : "?");
  printf("uid=%d gid=%d groups=", (int)getuid(), (int)getgid());
  for (int i = 0; i < n; i++) printf(i ? ",%d" : "%d", (int)g[i]);
  printf("\n");
  for (char **e = environ; *e; e++) printf("env=%s\n", *e);
  FILE *f = fopen("/proc/self/cgroup", "r");
  if (f) { while (fgets(buf, sizeof buf, f)) printf("cgroup=%s", buf); fclose(f); }
  fflush(stdout);
  return crash ? 3 : 0;
}
EOF
cc -static -O2 -o "$D/l/bin/probe" "$D/probe.c"
printf 'root:x:0:0:root:/root:/bin/sh\napp:x:1234:2345:app:/home/app:/bin/sh\n' >"$D/l/etc/passwd"
printf 'root:x:0:\napp:x:2345:\nextra:x:3456:app\nother:x:4567:root\n' >"$D/l/etc/group"
mkdir -p "$D/reg/probe"
tar -czf "$D/reg/probe/layer.tgz" -C "$D/l" bin etc
python3 - "$D/reg/probe" <<'PY'
import hashlib, json, os, sys
d = sys.argv[1]
def blob(b):
    h = hashlib.sha256(b).hexdigest()
    open(os.path.join(d, h), "wb").write(b)
    return h, len(b)
lh, ln = blob(open(os.path.join(d, "layer.tgz"), "rb").read())
os.remove(os.path.join(d, "layer.tgz"))
config = {"architecture": "arm64", "os": "linux", "config": {
    "Env": ["PATH=/bin", "IMG_ONLY=from-image", "SHARED=image"],
    "Entrypoint": ["/bin/probe", "ep"], "Cmd": ["cmd1", "cmd2"],
    "WorkingDir": "/srv/app", "User": "app"}}
ch, cn = blob(json.dumps(config).encode())
manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + ch, "size": cn},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "sha256:" + lh, "size": ln}]}
open(os.path.join(d, "manifest.json"), "w").write(json.dumps(manifest))
PY
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"
wn_registry "$REG_PORT"

echo "== 2. the operator's identity, a ready node-1, the node's image config =="
wn_seed_identity
put_text "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"

echo "== 3. render the node: the plane + the kubelet's chains + its image plane, on one store scoped to node-1 =="
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
# The kubelet, WHOLE: host sandboxes, volumes, probes, project, image, lifecycle, prune.
blocks, pairs = parts(f"{root}/packaging/debian/fluxor-podlifecycle.yaml")
kube = sub('    blobs: "/var/lib/nanocloud.io/image/blobs"\n', f'    blobs: "{blobs}"\n', "".join(blocks))
iblocks, ipairs = parts(f"{root}/packaging/debian/fluxor-imagepull.yaml")
img = "".join(iblocks)
img = sub('authority: "registry.nanocloud.io:5000"', f'authority: "{registry}"', img)
img = sub('dir: "/var/lib/nanocloud.io/image/blobs"', f'dir: "{blobs}"', img)
mods, wiring = g.split("\nwiring:\n", 1)
g = (mods.rstrip("\n") + "\n  # ── The kubelet and its image plane ──\n" + kube.rstrip("\n") + "\n"
     + img.rstrip("\n") + "\n\nwiring:\n" + wiring.rstrip("\n") + "\n"
     + "".join(f"  - from: {a}\n    to: {b}\n" for a, b in pairs + ipairs))
open(out, "w").write(g)
print(f"   spliced: {g.count(chr(10) + '  - name:')} modules")
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null

echo "== 4. run it as root, with a secret in its environment; a delegated cgroup base =="
sudo -n mkdir -p "$CG"
sudo -n env -C "$STORE" NC_E2E_SENTINEL=runtime-secret FLUXOR_SEAL_KEY=0123456789abcdef \
  FLUXOR_OWNER_ROOT="$D/owners" FLUXOR_HOST_CGROUP_ROOT="$CG" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 90 || fail "the store never answered"
r=""
for _ in $(seq 90); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac

echo "== 5. Pods through the API: command/args combinations, env, workingDir, limits; a crashing Deployment =="
pod() { # pod <name> <container-json-extra>: a Never Pod of the probe image
  printf '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"%s","namespace":"default"},"spec":{"restartPolicy":"Never","containers":[{"name":"c","image":"nanocloud/probe:1"%s}]}}' "$1" "$2"
}
declare -A UIDS
create() { # create <name> <json>
  local r
  for _ in $(seq 60); do
    r="$(api POST /api/v1/namespaces/default/pods "$2")"
    case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
  done
  case "$r" in 20[01]\|*) ;; *) fail "POST $1: ${r:0:200}" ;; esac
  UIDS[$1]="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
}
create envpod "$(pod envpod ',"env":[{"name":"SHARED","value":"container"},{"name":"CTR","value":"x y;z"}],"resources":{"limits":{"cpu":"250m","memory":"64Mi","ephemeral-storage":"1Gi"}}')"
create cmdpod "$(pod cmdpod ',"command":["/bin/probe","C"],"workingDir":"/tmp"')"
create argspod "$(pod argspod ',"args":["a1","a 2"]')"
create bothpod "$(pod bothpod ',"command":["/bin/probe","C"],"args":["a1"]')"
# A command too long for the record it crosses: refused, never cut or emptied
# (an empty command would run the image's Entrypoint instead).
LONG="$(head -c 600 /dev/zero | tr '\0' x)"
create longpod "$(pod longpod ",\"command\":[\"/bin/probe\",\"$LONG\"]")"
DEP='{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"crash","namespace":"default"},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"crash"}},"template":{"metadata":{"labels":{"app":"crash"}},"spec":{"containers":[{"name":"c","image":"nanocloud/probe:1","args":["crash"]}]}}}}'
r="$(api POST /apis/apps/v1/namespaces/default/deployments "$DEP")"
case "$r" in 20[01]\|*) echo "   POST deployment crash -> ${r%%|*}" ;; *) fail "POST deployment: ${r:0:200}" ;; esac

nodekey() { nc_store_get "$STORE" "/~node/node-1/$1"; }
expect() { [ "$2" = "$3" ] || fail "$1:
   got  '$2'
   want '$3'"; }
for p in envpod cmdpod argspod bothpod; do
  u="${UIDS[$p]}"
  st=""
  for _ in $(seq 120); do
    st="$(nodekey "pod-lifecycle-status/$u" 2>/dev/null || true)"
    case "$st" in *Succeeded*|*Failed*) break ;; esac
    sleep 1
  done
  case "$st" in
    *Succeeded*) echo "   $p ($u): Succeeded" ;;
    *) fail "$p never succeeded: status='$st' spec='$(nodekey "pod-specs/$u")' resolved='$(nodekey "pod-resolved/$u")' sandbox='$(nodekey "sandboxes/$u")' sbst='$(nodekey "sandbox-status/$u")' logs='$(nodekey "sandbox-logs/$u")'" ;;
  esac
done

st=""
for _ in $(seq 60); do
  st="$(nodekey "pod-lifecycle-status/${UIDS[longpod]}" 2>/dev/null || true)"
  case "$st" in *Failed*|*Succeeded*) break ;; esac
  sleep 1
done
case "$st" in *Failed*) ;; *) fail "longpod did not fail: status='$st' spec='$(nodekey "pod-specs/${UIDS[longpod]}" | cut -c1-200)'" ;; esac
expect "longpod's sandbox" "$(nodekey "sandbox-status/${UIDS[longpod]}")" "state=failed;code=-22"
expect "longpod's spec carries the overflow, not a cut command" \
  "$(nodekey "pod-specs/${UIDS[longpod]}" | tr ';' '\n' | sed -n 's/^cmd=//p')" "%00"
ev="$(nc_store_get "$STORE" "/events/default/${UIDS[longpod]}.Failed.0")"
case "$ev" in *'"reason":"Failed"'*'"type":"Warning"'*) ;; *) fail "no Failed event for longpod: '$ev'" ;; esac
[ -z "$(nodekey "sandbox-logs/${UIDS[longpod]}")" ] || fail "longpod ran: $(nodekey "sandbox-logs/${UIDS[longpod]}")"
echo "   longpod: a 600-byte argument is OVERFLOW — refused (failed -22), Failed with its Warning event, nothing ran"

echo "== 6. what each process was =="
logs() { nodekey "sandbox-logs/${UIDS[$1]}"; }
field() { logs "$1" | sed -n "s/^$2=//p"; }
argv() { field "$1" argv | paste -sd'|' -; }
expect "envpod argv (neither: Entrypoint ++ Cmd)" "$(argv envpod)" "/bin/probe|ep|cmd1|cmd2"
expect "cmdpod argv (command only: Cmd ignored)" "$(argv cmdpod)" "/bin/probe|C"
expect "argspod argv (args only: Entrypoint ++ args)" "$(argv argspod)" "/bin/probe|ep|a1|a 2"
expect "bothpod argv (command ++ args)" "$(argv bothpod)" "/bin/probe|C|a1"
echo "   argv: neither / command / args / both — by the Kubernetes rules"
expect "envpod environment" "$(field envpod env | sort | paste -sd'|' -)" \
  "$(printf '%s\n' CTR='x y;z' HOME=/home/app HOSTNAME=envpod IMG_ONLY=from-image PATH=/bin SHARED=container | sort | paste -sd'|' -)"
for p in envpod cmdpod argspod bothpod; do
  case "$(logs "$p")" in
    *NC_E2E_SENTINEL*|*FLUXOR_*|*runtime-secret*) fail "$p saw the runtime's environment: $(logs "$p")" ;;
  esac
done
echo "   env: exactly HOSTNAME, the image's Env overridden by the container's, HOME — nothing of the runtime's (no sentinel, no FLUXOR_*)"
expect "envpod cwd (the image's WorkingDir, created)" "$(field envpod cwd)" "/srv/app"
expect "cmdpod cwd (the container's workingDir)" "$(field cmdpod cwd)" "/tmp"
echo "   cwd: the image's WorkingDir, or the container's"
expect "envpod identity (the image's User, from its own passwd/group)" "$(logs envpod | grep '^uid=')" "uid=1234 gid=2345 groups=2345,3456"
echo "   user: app -> 1234:2345, groups 2345,3456 (the image's /etc/passwd and /etc/group)"
[ -e /bin/probe ] && fail "/bin/probe exists on the host: the test cannot tell the roots apart"
echo "   root: /bin/probe ran, and exists only in the image's rootfs"

echo "== 7. limits: cpu/memory in the Pod's cgroup, ephemeral-storage as its owner's storage cap =="
cgl="$(field envpod cgroup | sed -n 's/^0:://p')"
[ -n "$cgl" ] || fail "envpod reported no cgroup: $(logs envpod)"
case "$cgl" in "${CG#/sys/fs/cgroup}/"*) ;; *) fail "envpod ran outside the node's cgroup base: '$cgl'" ;; esac
cpu="$(sudo -n cat "/sys/fs/cgroup$cgl/cpu.max" 2>/dev/null || true)"
mem="$(sudo -n cat "/sys/fs/cgroup$cgl/memory.max" 2>/dev/null || true)"
expect "cpu.max (250m)" "$cpu" "25000 100000"
expect "memory.max (64Mi)" "$mem" "67108864"
echo "   $cgl: cpu.max '$cpu', memory.max $mem"
grep -q "compute_milli=250 memory_bytes=67108864 max_tasks=4194304 storage_bytes=1073741824" "$D/run.log" \
  || fail "the runtime never admitted envpod's envelope with its storage cap: $(grep '\[workload\] host workload' "$D/run.log" | head -5)"
echo "   admitted: compute_milli=250 memory_bytes=67108864 storage_bytes=1073741824 (ephemeral-storage 1Gi)"

echo "== 8. a Deployment's Pod with no restartPolicy (Always) that crashes is restarted =="
cu=""
for _ in $(seq 60); do
  r="$(api GET '/api/v1/namespaces/default/pods?labelSelector=app%3Dcrash')"
  cu="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["items"][0]["metadata"]["uid"])
except Exception: pass' 2>/dev/null || true)"
  [ -n "$cu" ] && break
  sleep 1
done
[ -n "$cu" ] || fail "the Deployment made no Pod: ${r:0:200}"
expect "the crash Pod's restart policy" "$(nodekey "pod-specs/$cu" | tr ';' '\n' | sed -n 's/^restart=//p')" "always"
n=0
for _ in $(seq 60); do
  n="$(nodekey "pod-runtime/$cu" | tr ';' '\n' | sed -n 's/^restarts=//p')"
  [ "${n:-0}" -ge 2 ] && break
  sleep 1
done
[ "${n:-0}" -ge 2 ] || fail "the crashing Pod was not restarted: runtime='$(nodekey "pod-runtime/$cu")' status='$(nodekey "pod-lifecycle-status/$cu")' logs='$(nodekey "sandbox-logs/$cu")'"
echo "   crash ($cu): restartPolicy unset -> always; restarted $n times (exit 3 each)"

echo "== E2E green: a Pod spec through the API became exactly its process — env, cwd, user, argv, root, restart, limits =="
