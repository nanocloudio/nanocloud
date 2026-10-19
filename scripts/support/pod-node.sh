# A node in the shape a deb node runs — the control plane, its kubelet's
# chains (fluxor-podlifecycle.yaml, whole) and its image plane
# (fluxor-imagepull.yaml) on one store scoped to node-1 — for the E2Es that
# run Pods of OCI images through the API (scripts/pod-spec-e2e.sh,
# scripts/pod-volumes-e2e.sh). The controllers a Pod does not need (Job,
# CronJob, HPA, DaemonSet status, quota, disruption budgets, taints,
# snapshots, devices, Pod IPAM, namespace teardown, the ownerRef cascade:
# PN_DROP, name prefixes) are left out of the render, so the graph stays
# inside the runtime's config arena whatever else the control plane grows. Sourced after
# scripts/fluxor-env.sh and scripts/support/workload-node.sh, with D, ROOT,
# STORE, PORT, REG_PORT and fail set.

# pn_probe_image <repo> <User> [Cmd JSON]: an OCI image of /bin/probe — a static binary
# that runs its arguments as a script of steps, printing what each finds:
#   argv | env | id | cwd           what the process is
#   cat <f> | mode <f> | ls <d>     what a file holds, its mode, a listing
#   write <f> <text> | append <f> <text> | appendenv <f> <VAR>
#   sleep <s> | wait <f> <s>        wait (for a file to appear)
#   exit <n>                        end with that status
#   onterm <s>                      on SIGTERM: print onterm=got, take <s>
#                                   seconds, print onterm=done, exit 0
#   ignoreterm                      ignore SIGTERM (only SIGKILL ends it)
# and an /etc/passwd (root, app 1234:2345) and /etc/group.
pn_probe_image() {
  local repo="$1" user="$2" cmd="${3:-[\"argv\"]}"
  if [ ! -x "$D/probe-bin" ]; then
    cat >"$D/probe.c" <<'EOF'
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grp.h>
#include <signal.h>
extern char **environ;
static int term_secs;
static void on_term(int sig) {
  (void)sig;
  write(1, "onterm=got\n", 11);
  sleep(term_secs);
  write(1, "onterm=done\n", 12);
  _exit(0);
}
static void cat(const char *f) {
  FILE *h = fopen(f, "r");
  if (!h) { printf("cat %s=ERR\n", f); return; }
  char b[4096]; size_t n = fread(b, 1, sizeof b - 1, h); b[n] = 0; fclose(h);
  for (size_t i = 0; i < n; i++) if (b[i] == '\n') b[i] = '|';
  printf("cat %s=%s\n", f, b);
}
int main(int argc, char **argv) {
  char buf[512];
  for (int i = 1; i < argc; i++) {
    const char *s = argv[i];
    if (!strcmp(s, "argv")) { for (int k = 0; k < argc; k++) printf("argv=%s\n", argv[k]); }
    else if (!strcmp(s, "env")) { for (char **e = environ; *e; e++) printf("env=%s\n", *e); }
    else if (!strcmp(s, "id")) {
      gid_t g[32]; int n = getgroups(32, g);
      printf("uid=%d gid=%d groups=", (int)getuid(), (int)getgid());
      for (int k = 0; k < n; k++) printf(k ? ",%d" : "%d", (int)g[k]);
      printf("\n");
    }
    else if (!strcmp(s, "cwd")) printf("cwd=%s\n", getcwd(buf, sizeof buf) ? buf : "?");
    else if (!strcmp(s, "cat") && i + 1 < argc) cat(argv[++i]);
    else if (!strcmp(s, "mode") && i + 1 < argc) {
      struct stat st; const char *f = argv[++i];
      if (stat(f, &st)) printf("mode %s=ERR\n", f);
      else printf("mode %s=%o\n", f, st.st_mode & 0777);
    }
    else if (!strcmp(s, "ls") && i + 1 < argc) {
      DIR *d = opendir(argv[++i]); struct dirent *e;
      printf("ls %s=", argv[i]);
      if (d) { while ((e = readdir(d))) if (e->d_name[0] != '.') printf("%s,", e->d_name); closedir(d); }
      else printf("ERR");
      printf("\n");
    }
    else if ((!strcmp(s, "write") || !strcmp(s, "append")) && i + 2 < argc) {
      FILE *h = fopen(argv[i + 1], s[0] == 'w' ? "w" : "a");
      printf("%s %s=%s\n", s, argv[i + 1], h && fputs(argv[i + 2], h) >= 0 ? "ok" : "ERR");
      if (h) fclose(h);
      i += 2;
    }
    else if (!strcmp(s, "appendenv") && i + 2 < argc) {
      FILE *h = fopen(argv[i + 1], "a"); const char *v = getenv(argv[i + 2]);
      printf("append %s=%s\n", argv[i + 1], h && v && fputs(v, h) >= 0 ? "ok" : "ERR");
      if (h) fclose(h);
      i += 2;
    }
    else if (!strcmp(s, "sleep") && i + 1 < argc) { fflush(stdout); sleep(atoi(argv[++i])); }
    else if (!strcmp(s, "wait") && i + 2 < argc) {
      const char *f = argv[i + 1]; int t = atoi(argv[i + 2]) * 10; i += 2;
      struct stat st;
      while (t-- > 0 && stat(f, &st)) usleep(100000);
      printf("wait %s=%s\n", f, stat(f, &st) ? "TIMEOUT" : "ok");
    }
    else if (!strcmp(s, "exit") && i + 1 < argc) { fflush(stdout); return atoi(argv[++i]); }
    else if (!strcmp(s, "onterm") && i + 1 < argc) {
      term_secs = atoi(argv[++i]);
      signal(SIGTERM, on_term);
      printf("onterm=armed\n");
    }
    else if (!strcmp(s, "ignoreterm")) { signal(SIGTERM, SIG_IGN); printf("ignoreterm=armed\n"); }
    else printf("step %s=?\n", s);
    fflush(stdout);
  }
  return 0;
}
EOF
    cc -static -O2 -o "$D/probe-bin" "$D/probe.c" || fail "the probe did not build"
  fi
  local l="$D/layer-$repo"
  mkdir -p "$l/bin" "$l/etc" "$l/tmp" "$D/reg/$repo"
  chmod 1777 "$l/tmp"
  cp "$D/probe-bin" "$l/bin/probe"
  printf 'root:x:0:0:root:/root:/bin/sh\napp:x:1234:2345:app:/home/app:/bin/sh\n' >"$l/etc/passwd"
  printf 'root:x:0:\napp:x:2345:\n' >"$l/etc/group"
  tar -czf "$D/reg/$repo/layer.tgz" -C "$l" bin etc tmp
  python3 - "$D/reg/$repo" "$user" "$cmd" <<'PY'
import hashlib, json, os, sys
d, user, cmd = sys.argv[1:4]
def blob(b):
    h = hashlib.sha256(b).hexdigest()
    open(os.path.join(d, h), "wb").write(b)
    return h, len(b)
lh, ln = blob(open(os.path.join(d, "layer.tgz"), "rb").read())
os.remove(os.path.join(d, "layer.tgz"))
cfg = {"Env": ["PATH=/bin", "IMG=yes"], "Entrypoint": ["/bin/probe"], "Cmd": json.loads(cmd)}
if user:
    cfg["User"] = user
config = {"architecture": "arm64", "os": "linux", "config": cfg}
ch, cn = blob(json.dumps(config).encode())
manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + ch, "size": cn},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "sha256:" + lh, "size": ln}]}
open(os.path.join(d, "manifest.json"), "w").write(json.dumps(manifest))
PY
}

# pn_render: the node's graph at $D/graph.yaml (config $D/config.bin), its
# API on $PORT, its Pods' own volumes under $D/pods. PN_TOMB_MS: how long a
# deleted Pod's tomb (and so its node's records) stays — the API's delete
# (se_probe) and the kubelet's final one (ps_rput) — when not the default.
pn_render() {
  nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
  sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
  PN_DROP="${PN_DROP:-jb_ cj_ pc_ hp_ sn_ dv_ dss_ ip_ ns_ q_ pdb_ te_ gc_}" \
  python3 - "$D/graph.yaml" "$ROOT" node-1 "$D/blobs" "localhost:$REG_PORT" "$PORT" "$D/pods" <<'PY' || fail "the splice did not apply"
import os, re, sys
out, root, node, blobs, registry, port, pods = sys.argv[1:8]
g = open(out).read()
def sub(old, new, text):
    if text.count(old) != 1: sys.exit(f"expected 1 x {old[:50]!r}, found {text.count(old)}")
    return text.replace(old, new)
g = sub("    port: 7443\n", f"    port: {port}\n", g)
tomb_ms = os.environ.get("PN_TOMB_MS", "")
if tomb_ms:
    for name in ("se_probe", "ps_rput"):
        at = g.index(f"  - name: {name}\n")
        end = g.index("\n  - name: ", at + 5)
        blk = g[at:end]
        if blk.count('    tomb: "/~tomb"\n') != 1:
            sys.exit(f"{name}: expected one tomb param")
        g = g[:at] + blk.replace('    tomb: "/~tomb"\n', f'    tomb: "/~tomb"\n    tomb_ms: {tomb_ms}\n') + g[end:]
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/,/blobs/,/fetch-q/")
g = sub('      scope: ""\n      scoped: ""\n', f'      scope: "{node}"\n      scoped: "{SCOPED}"\n', g)
def parts(text):
    mods = text[text.index("modules:\n") + len("modules:\n"):text.index("\nwiring:")]
    wire = text[text.index("wiring:\n") + len("wiring:\n"):]
    blocks = [b for b in re.split(r"(?m)^(?=  - name: )", mods) if b.startswith("  - name: ")]
    pairs, frm, extra = [], None, {}
    for line in wire.splitlines():
        m = re.match(r"  - from: (\S+)$", line)
        if m: frm = m.group(1); continue
        m = re.match(r"    to: (\S+)$", line)
        if m and frm: pairs.append((frm, m.group(1))); frm = None; continue
        m = re.match(r"    (\S+): (.+)$", line)
        if m and pairs: extra.setdefault(len(pairs) - 1, []).append(line)
    return mods, blocks, pairs, extra, wire
# The controllers no Pod here needs, left out (PN_DROP: name prefixes).
DROP = tuple(os.environ.get("PN_DROP", "").split())
head, rest = g.split("modules:\n", 1)
mods, blocks, pairs, extra, wire = parts(g)
dropped = {b.split()[2] for b in blocks if b.split()[2].startswith(DROP)}
keep_blocks = [b for b in re.split(r"(?m)^(?=  - name: )", mods) if not (b.startswith("  - name: ") and b.split()[2] in dropped)]
kw = []
for i, (a, b) in enumerate(pairs):
    if a.split(".")[0] in dropped or b.split(".")[0] in dropped:
        continue
    kw.append(f"  - from: {a}\n    to: {b}\n" + "".join(l + "\n" for l in extra.get(i, [])))
g = head + "modules:\n" + "".join(keep_blocks).rstrip("\n") + "\n\nwiring:\n" + "".join(kw)
# The kubelet, WHOLE, and its image plane.
kmods, kblocks, kpairs, kextra, _ = parts(open(f"{root}/packaging/debian/fluxor-podlifecycle.yaml").read())
kube = sub('    blobs: "/var/lib/nanocloud.io/image/blobs"\n', f'    blobs: "{blobs}"\n', "".join(kblocks))
kube = sub("    type: volume_files\n", f'    type: volume_files\n    root: "{pods}"\n    gc_ms: 2000\n', kube)
imods, iblocks, ipairs, iextra, _ = parts(open(f"{root}/packaging/debian/fluxor-imagepull.yaml").read())
img = "".join(iblocks)
img = sub('authority: "registry.nanocloud.io:5000"', f'authority: "{registry}"', img)
img = sub('dir: "/var/lib/nanocloud.io/image/blobs"', f'dir: "{blobs}"', img)
m2, w2 = g.split("\nwiring:\n", 1)
def edges(pairs, extra):
    return "".join(f"  - from: {a}\n    to: {b}\n" + "".join(l + "\n" for l in extra.get(i, [])) for i, (a, b) in enumerate(pairs))
g = (m2.rstrip("\n") + "\n  # ── The kubelet and its image plane ──\n" + kube.rstrip("\n") + "\n"
     + img.rstrip("\n") + "\n\nwiring:\n" + w2.rstrip("\n") + "\n" + edges(kpairs, kextra) + edges(ipairs, iextra))
open(out, "w").write(g)
print(f"   spliced: {g.count(chr(10) + '  - name:')} modules ({len(dropped)} controllers left out)")
PY
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null \
    || fail "the node did not build"
  echo "   config: $(stat -c %s "$D/config.bin") bytes"
}

# pn_run: the node, as root (pivot_root, setuid, cgroups, chmod), with a
# delegated cgroup base at $CG; its pid in RUNTIME_PID.
pn_run() {
  sudo -n mkdir -p "$CG"
  sudo -n env -C "$STORE" FLUXOR_SEAL_KEY=0123456789abcdef SECTOR_PROC_ALLOW="chmod rm" \
    FLUXOR_OWNER_ROOT="$D/owners" FLUXOR_HOST_CGROUP_ROOT="$CG" RUST_LOG=info "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  nc_store_ready "$STORE" 90 || fail "the store never answered"
  local r=""
  for _ in $(seq 90); do
    r="$(api GET /version 2>/dev/null || true)"
    case "$r" in 200\|*) break ;; esac
    sleep 1
  done
  case "$r" in 200\|*) ;; *) fail "no API: $r" ;; esac
}

# pn_cleanup: the runtime, what of its Pods outlived it, the registry, the
# cgroups, the work directory (KEEP=1 keeps it).
pn_cleanup() {
  if [ -n "${RUNTIME_PID:-}" ]; then
    sudo -n pkill -f "$D/[c]onfig.bin" 2>/dev/null || true
    for _ in $(seq 20); do pgrep -f "$D/[c]onfig.bin" >/dev/null || break; sleep 0.5; done
    sudo -n pkill -KILL -f "$D/[c]onfig.bin" 2>/dev/null || true
  fi
  sudo -n pkill -f "$D/[r]ootfs" 2>/dev/null || true
  [ -n "${REG_PID:-}" ] && kill "$REG_PID" 2>/dev/null || true
  # Every container outlives its runtime (a pivoted process's command line
  # names no path of ours): each is in a cgroup under $CG, killed there —
  # and they hold the runtime's sockets (its API port) while they live.
  if [ -d "$CG" ]; then
    for c in "$CG"/*/; do
      [ -d "$c" ] || continue
      echo 1 | sudo -n tee "$c/cgroup.kill" >/dev/null 2>&1 || true
      echo 0 | sudo -n tee "$c/cgroup.freeze" >/dev/null 2>&1 || true
    done
    sleep 0.5
    for c in "$CG"/*/; do
      [ -d "$c" ] && sudo -n rmdir "$c" 2>/dev/null || true
    done
    sudo -n rmdir "$CG" 2>/dev/null || true
  fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}

# pn_create <collection path> <json>: POST, retried while the API warms up;
# the created object's uid in CREATED_UID.
pn_create() {
  local r=""
  for _ in $(seq 60); do
    r="$(api POST "$1" "$2")"
    case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
  done
  case "$r" in 20[01]\|*) ;; *) fail "POST $1: ${r:0:300}" ;; esac
  CREATED_UID="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("metadata",{}).get("uid",""))')"
}

# pn_nodekey <key>: the node's own record (its scope).
pn_nodekey() { nc_store_get "$STORE" "/~node/node-1/$1"; }

# pn_logs <sandbox id>: what it printed.
pn_logs() { pn_nodekey "sandbox-logs/$1"; }

# pn_wait_phase <uid> <regex> [seconds]: until the node's lifecycle phase
# for <uid> matches.
pn_wait_phase() {
  local st=""
  for _ in $(seq "${3:-90}"); do
    st="$(pn_nodekey "pod-lifecycle-status/$1" 2>/dev/null || true)"
    if printf '%s' "$st" | grep -Eq "$2"; then return 0; fi
    sleep 1
  done
  fail "$1 never reached $2: status='$st' spec='$(pn_nodekey "pod-specs/$1" | cut -c1-600)' sandbox='$(pn_nodekey "sandboxes/$1" | cut -c1-300)' sbst='$(pn_nodekey "sandbox-status/$1")' logs='$(pn_logs "$1" | head -c 400)'"
}

# pn_wait_log <sandbox id> <text> [seconds]
pn_wait_log() {
  for _ in $(seq "${3:-60}"); do
    case "$(pn_logs "$1")" in *"$2"*) return 0 ;; esac
    sleep 1
  done
  fail "$1 never printed '$2': logs='$(pn_logs "$1" | head -c 600)' status='$(pn_nodekey "sandbox-status/$1")' spec='$(pn_nodekey "pod-specs/$1" | cut -c1-500)'"
}

# pn_pod_json <ns> <name>: the Pod as the API answers it.
pn_pod_json() { local r; r="$(api GET "/api/v1/namespaces/$1/pods/$2")"; printf '%s' "${r#*|}"; }
