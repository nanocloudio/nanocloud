#!/usr/bin/env bash
# Live E2E: a node whose wall clock is not set — a board before SNTP — never
# stamps a Pod's status with 1970. The node runs under a clock shim
# (LD_PRELOAD): until $D/clock says 1, CLOCK_REALTIME reads the time since
# the host booted (a clock no one set) and adjtimex(2) says unsynchronised,
# so the runtime's TIMER::TRUSTED_UNIX is not TRUSTED. Then the clock is set.
#
# PASS =
#   untrusted  the Pod runs and is Ready, its status says so — and every
#              moment in it (startTime, each condition's lastTransitionTime,
#              the container's startedAt) is null: absent, never wrong; no
#              1970 anywhere. The node's memory holds the moments as
#              m<monotonic ms>@<boot> (_shared/moment.rs)
#   trusted    once the clock is, the same moments resolve to wall-clock time
#              — when the Pod really started, by the monotonic clock, not
#              when the clock was set — and the memory is rewritten as it
#
# The node shape of pod-ready-e2e.sh (the plane, the whole kubelet, its
# image plane); the Pod is seeded into the store before the boot.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7468
REG_PORT=5400
APP_PORT=18094

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

D="$(mktemp -d /tmp/nc-podclock-XXXXXX)"
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

echo "== 4. the clock shim: CLOCK_REALTIME from the host's boot, adjtimex unsynchronised, until \$D/clock says 1 =="
cat >"$D/clk.c" <<'CEOF'
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/timex.h>
#include <time.h>
static int (*real_gettime)(clockid_t, struct timespec *);
static int set = 0;
static long long checked_ns = -1;
static int clock_is_set(void) {
  struct timespec m;
  if (!real_gettime) real_gettime = dlsym(RTLD_NEXT, "clock_gettime");
  real_gettime(CLOCK_MONOTONIC, &m);
  long long now = (long long)m.tv_sec * 1000000000LL + m.tv_nsec;
  if (!set && (checked_ns < 0 || now - checked_ns > 200000000LL)) {
    checked_ns = now;
    const char *f = getenv("NC_CLOCK_FILE");
    FILE *fp = f ? fopen(f, "r") : NULL;
    if (fp) { set = fgetc(fp) == '1'; fclose(fp); }
  }
  return set;
}
int clock_gettime(clockid_t id, struct timespec *ts) {
  if (!real_gettime) real_gettime = dlsym(RTLD_NEXT, "clock_gettime");
  if ((id == CLOCK_REALTIME || id == CLOCK_REALTIME_COARSE) && !clock_is_set())
    return real_gettime(CLOCK_BOOTTIME, ts);
  return real_gettime(id, ts);
}
int adjtimex(struct timex *t) {
  static int (*real)(struct timex *);
  if (!real) real = dlsym(RTLD_NEXT, "adjtimex");
  int r = real(t);
  if (!clock_is_set()) { t->status |= STA_UNSYNC; return TIME_ERROR; }
  return r;
}
CEOF
cc -shared -fPIC -O2 -o "$D/clk.so" "$D/clk.c" -ldl
echo 0 >"$D/clock"

echo "== 5. a Pod for node-1, in the store before the boot =="
UIDP=7c1f0c3e-0000-4000-8000-00000000c10c
put_text "/pods/default/clk" '{"apiVersion":"v1","kind":"Pod","metadata":{"name":"clk","namespace":"default","uid":"'"$UIDP"'","creationTimestamp":"2026-01-01T00:00:00Z"},"spec":{"nodeName":"node-1","containers":[{"name":"c","image":"nanocloud/srv:1","args":["serve","'"$APP_PORT"'","0"]}]},"status":{"phase":"Pending"}}'

echo "== 6. boot it, its clock unset =="
T_BOOT="$(date +%s)"
sudo -n env -C "$STORE" LD_PRELOAD="$D/clk.so" NC_CLOCK_FILE="$D/clock" FLUXOR_SEAL_KEY=0123456789abcdef \
  FLUXOR_OWNER_ROOT="$D/owners" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 90 || fail "the store never answered"
pod() { nc_store_get "$STORE" /pods/default/clk; }
for _ in $(seq 120); do
  case "$(pod)" in *'"containerStatuses"'*'"running"'*'"ready":true'*) break ;; esac
  sleep 1
done
T_RUN="$(date +%s)"
pod | python3 -c '
import json, sys
p = json.load(sys.stdin); st = p["status"]
assert st["phase"] == "Running", st
cond = {c["type"]: c for c in st["conditions"]}
assert cond["Ready"]["status"] == "True", cond
for t, c in cond.items():
    assert c["lastTransitionTime"] is None, (t, c)
assert st.get("startTime") is None, st.get("startTime")
cs = st["containerStatuses"][0]
assert cs["ready"] is True and cs["state"]["running"]["startedAt"] is None, cs
txt = json.dumps(st)
assert "1970" not in txt, txt
print("   Running, Ready True; startTime, every lastTransitionTime and startedAt null; no 1970 anywhere")
' || fail "the Pod's status while the clock is untrusted: $(pod | head -c 1500)"
MEM="$(nc_store_get "$STORE" "/~node/node-1/pod-cond/$UIDP")"
case "$MEM" in *"t0=m"*"@"*) echo "   the node's memory holds the moments as monotonic time: ${MEM:0:90}…" ;;
  *) fail "the node's memory is not monotonic while the clock is untrusted: '$MEM'" ;; esac

echo "== 7. the clock is set: the moments resolve to when they happened =="
sleep 3
echo 1 >"$D/clock"
T_SET="$(date +%s)"
for _ in $(seq 60); do
  case "$(pod)" in *'"startTime":"'*) break ;; esac
  sleep 1
done
pod | python3 -c '
import json, sys, datetime
p = json.load(sys.stdin); st = p["status"]
boot, run, set_ = (int(x) for x in sys.argv[1:4])
def t(s): return int(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp())
cond = {c["type"]: c for c in st["conditions"]}
times = {"startTime": st["startTime"], "startedAt": st["containerStatuses"][0]["state"]["running"]["startedAt"]}
times.update({k: c["lastTransitionTime"] for k, c in cond.items()})
for k, v in times.items():
    assert v is not None, (k, times)
    # When it happened: after the boot, by the time the Pod was seen running,
    # and before the clock was set — not the moment it was trusted.
    assert boot - 2 <= t(v) <= run + 1, (k, v, boot, run)
    assert t(v) < set_, (k, v, set_)
print("   " + ", ".join(f"{k} {v}" for k, v in sorted(times.items())))
' "$T_BOOT" "$T_RUN" "$T_SET" || fail "the moments once the clock is trusted: $(pod | head -c 1500)"
# Every moment of the memory (t0 tr tf trdy ltr ltf). `rs` is no moment: it
# names the run by the runtime's own `started` stamp, the value the report
# compares the sandbox's status with, and stays exactly as the runtime said.
monotonic() { printf '%s' "$1" | tr ';' '\n' | grep -Eq '^(t0|tr|tf|trdy|ltr|ltf)=m'; }
for _ in $(seq 30); do
  MEM="$(nc_store_get "$STORE" "/~node/node-1/pod-cond/$UIDP")"
  monotonic "$MEM" && sleep 1 || break
done
monotonic "$MEM" && fail "the memory was not rewritten as wall-clock time: '$MEM'"
echo "   the node's memory, now wall-clock time: ${MEM:0:90}…"

echo "== E2E green: a node with no trusted clock reports its moments as absent, never 1970, and resolves them to when they happened once it has one =="
