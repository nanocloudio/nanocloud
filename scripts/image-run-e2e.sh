#!/usr/bin/env bash
# Live E2E for the COMPLETE pod-image path: a pod
# spec carrying `image=` drives pull → verify → assemble → run, all fmods:
#
#   the pod chain   image= → /image-requests/ (pull trigger) + Pending gate
#   image.uproc      index → manifest → config + layers over http_fetch →
#                    verified blob cache
#   image_asm.uproc  every layer cached → assembly job → /image-rootfs/ ready
#   sandbox chain    runs the assembly job, then the pod itself (pivot_root)
#
# The script plays the registry (docker-v2 stub) and authors a REAL runnable
# image, in the shape a real registry serves it:
#   - a multi-arch INDEX at the tag (amd64 decoy first, then arm64) — the
#     fetcher must select by platform and re-GET by digest;
#   - a platform manifest naming a real CONFIG blob + 2 layers;
#   - layer1 = a static aarch64 binary (/bin/hello-static) + /marker;
#     layer2 = /etc/tag + a whiteout deleting /marker.
# The pod spec carries NO `cmd=` — argv comes from the image config's
# Entrypoint ++ Cmd, which is the whole point: `image=<name>` alone must run.
# A second image, v2 (another Entrypoint, one more layer), proves the keying:
# every tag resolves to a manifest DIGEST and everything is cached by it, so
# testapp:v1 and testapp:v2 run different programs; `registry.nanocloud.io:
# 5000/testapp:v2` (the node's registry, named) and `testapp@sha256:…` run
# v2; a reference to another registry is refused (ErrImagePull, nothing
# pulled); and once `latest` is moved to v2, a new Pod (Always, the default
# for :latest) runs v2 while an IfNotPresent one keeps the cached v1 and the
# first Pod keeps the digest it resolved.
# PASS = pod reaches Succeeded and /sandbox-logs/ carries the binary's output,
# with the whiteout applied in the assembled rootfs.
#
# Needs passwordless sudo (pivot_root/mknod): the RUNTIME runs under sudo,
# exactly as the production node service does (root). Without it the suite
# skips, and NANOCLOUD_REQUIRE_E2E=1 makes that a failure.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
PORT=5392

. "$ROOT/scripts/fluxor-env.sh"
# The container runtime, as the packaged sandbox graph states it.
SANDBOX_MODS="$(python3 "$ROOT/scripts/support/sandbox-block.py" mods)"
SANDBOX_WIRE="$(python3 "$ROOT/scripts/support/sandbox-block.py" wire)"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
skip() {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = "1" ] && { echo "FAIL: $1"; exit 1; }
  echo "SKIP: $1"; exit 0
}
sudo -n true 2>/dev/null || skip "passwordless sudo unavailable (pivot_root needs root)"
for m in http_fetch store_source store_effect decision workload_effect; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgrun-XXXXXX)"
chmod 755 "$D"
REG_PID=""
RUNTIME_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && sudo -n kill "$RUNTIME_PID" 2>/dev/null || true
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value>
  sudo -n python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush()
    os.fsync(f.fileno())
PY
}

wal_last() { # wal_last <key>
  sudo -n python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want:
        out = v
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. author two runnable images (static binaries + a whiteout) =="
mkdir -p "$D/reg" "$D/blobcache" "$D/rootfs" "$D/l1/bin" "$D/l2/etc" "$D/l3/bin"
cat > "$D/hello.c" <<'EOF'
#include <unistd.h>
int main(void) { write(1, "hello-from-image\n", 17); return 0; }
EOF
cat > "$D/hello2.c" <<'EOF'
#include <unistd.h>
int main(void) { write(1, "hello-from-v2\n", 14); return 0; }
EOF
cc -static -o "$D/l1/bin/hello-static" "$D/hello.c"
cc -static -o "$D/l3/bin/hello-v2" "$D/hello2.c"
echo "to-be-whited-out" > "$D/l1/marker"
echo "layer2" > "$D/l2/etc/tag"
touch "$D/l2/.wh.marker"                       # OCI whiteout: delete /marker
tar -czf "$D/reg/l1.tgz" -C "$D/l1" bin marker
tar -czf "$D/reg/l2.tgz" -C "$D/l2" etc .wh.marker
tar -czf "$D/reg/l3.tgz" -C "$D/l3" bin
python3 - "$D/reg" <<'PY'
import hashlib, json, os, sys
d = sys.argv[1]

def land(raw):
    """Store `raw` content-addressed under its digest; return (hex, len)."""
    h = hashlib.sha256(raw).hexdigest()
    open(os.path.join(d, h), "wb").write(raw)
    return h, len(raw)

digs = []
for f in ("l1.tgz", "l2.tgz", "l3.tgz"):
    path = os.path.join(d, f)
    b = open(path, "rb").read()
    os.remove(path)
    digs.append(land(b))

def image(entrypoint, layers):
    # The image config: what the runtime contract comes from. Entrypoint ++
    # Cmd is the argv the pod must end up spawning without the spec naming it.
    config = {"architecture": "arm64", "os": "linux",
              "config": {"Env": ["PATH=/bin"], "Entrypoint": [entrypoint], "Cmd": ["--ignored"],
                         "WorkingDir": "/", "User": "root"}}
    cfg_hex, cfg_len = land(json.dumps(config).encode())
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "size": cfg_len, "digest": "sha256:" + cfg_hex},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "size": n, "digest": "sha256:" + h} for h, n in layers],
    }
    return land(json.dumps(manifest).encode()) + (cfg_hex, cfg_len)

v1_hex, v1_len, cfg_hex, cfg_len = image("/bin/hello-static", digs[:2])
# v2: another Entrypoint, one more layer; the first two shared with v1.
v2_hex, v2_len, _, _ = image("/bin/hello-v2", digs)

# `latest` resolves to an INDEX, not the manifest — the multi-arch shape. The
# amd64 entry comes FIRST so "take the first entry" would pick the wrong one.
def index(h, n):
    return json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"mediaType": "application/vnd.oci.image.manifest.v1+json",
             "size": 3, "digest": "sha256:" + "e" * 64,
             "platform": {"architecture": "amd64", "os": "linux"}},
            {"mediaType": "application/vnd.oci.image.manifest.v1+json",
             "size": n, "digest": "sha256:" + h,
             "platform": {"architecture": "arm64", "os": "linux"}},
        ]})
open(os.path.join(d, "index-v2.json"), "w").write(index(v2_hex, v2_len))
# The registry's tags, as files: what each names right now.
os.makedirs(os.path.join(d, "tags"), exist_ok=True)
for tag, body in (("latest", index(v1_hex, v1_len)), ("v1", open(os.path.join(d, v1_hex)).read()),
                  ("v2", open(os.path.join(d, v2_hex)).read())):
    open(os.path.join(d, "tags", tag), "w").write(body)
open(os.path.join(d, "digests.txt"), "w").write(
    "\n".join([h for h, _ in digs[:2]] + [cfg_hex, str(cfg_len), v1_hex, v2_hex]))
PY
D1="$(sed -n 1p "$D/reg/digests.txt")"
D2="$(sed -n 2p "$D/reg/digests.txt")"
DCFG="$(sed -n 3p "$D/reg/digests.txt")"
NCFG="$(sed -n 4p "$D/reg/digests.txt")"
DMAN="$(sed -n 5p "$D/reg/digests.txt")"
DMAN2="$(sed -n 6p "$D/reg/digests.txt")"
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"

echo "== 2. start the registry stub (:$PORT) =="
python3 - "$D/reg" "$PORT" >"$D/reg.log" 2>&1 <<'PY' &
import http.server, os, socketserver, sys
d, port = sys.argv[1], int(sys.argv[2])
def blob(ref):
    p = os.path.join(d, ref.split(":")[-1])
    return open(p, "rb").read() if os.path.exists(p) else None
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        body = None
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "manifests":
            # A digest names that exact manifest; a tag what tags/<tag> holds now.
            if parts[-1].startswith("sha256:"):
                body = blob(parts[-1])
            elif os.path.exists(os.path.join(d, "tags", parts[-1])):
                body = open(os.path.join(d, "tags", parts[-1]), "rb").read()
        elif len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "blobs":
            body = blob(parts[-1])
        sys.stderr.write(f"GET {self.path} {200 if body is not None else 404}\n"); sys.stderr.flush()
        if body is None:
            self.send_response(404); self.send_header("Content-Length", "0")
            self.send_header("Connection", "close"); self.end_headers(); return
        status = 200
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            lo_s, _, hi_s = rng[6:].partition("-")
            lo = int(lo_s) if lo_s else 0
            hi = int(hi_s) if hi_s else len(body) - 1
            body = body[lo:hi + 1]; status = 206
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
class S(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
with S(("127.0.0.1", port), H) as srv:
    srv.serve_forever()
PY
REG_PID=$!
for i in $(seq 1 40); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && break
  sleep 0.1
done
# Guard against a STALE stub squatting the port (a previous run's trap not
# firing — e.g. a machine reset): the listener must serve OUR manifest.
kill -0 "$REG_PID" 2>/dev/null || fail "registry stub died (stale listener on :$PORT? $(ss -ltnp 2>/dev/null | grep ":$PORT" || true))"
curl -s "http://127.0.0.1:$PORT/v2/testapp/manifests/latest" | cmp -s - "$D/reg/tags/latest" \
  || fail "listener on :$PORT is not our stub (stale process from a previous run?)"

echo "== 3. write the pod-image graph =="
# The pod-lifecycle CHAIN, from the packaged graph rather than restated here:
# this E2E proves the REAL image path (registry -> pull -> assemble -> run)
# and it must prove it on what ships, not on a copy that can drift.
chain_part() {
  python3 - "$ROOT/packaging/debian/fluxor-podlifecycle.yaml" "$1" <<'PY2'
import re, sys
src = open(sys.argv[1]).read()
if sys.argv[2] == "mods":
    # Not pi_bad: sb_evout feeds it (a pack's cache faults), and this
    # graph's sandbox runtime is not the block that carries sb_evout.
    mods = src[src.index('  # \u2500\u2500 project'):src.index('\nwiring:')]
    mods = mods[:mods.index("  # \u2500\u2500 a pack's cache faults")]
    print(mods.rstrip())
else:
    raw = src[src.index('wiring:') + len('wiring:\n'):].split('\n')
    blocks, cur = [], None
    for l in raw:
        if re.match(r'\s*- from: ', l):
            if cur: blocks.append(cur)
            cur = [l]
        elif cur is not None and re.match(r'\s+\S', l):
            cur.append(l)
        else:
            if cur: blocks.append(cur); cur = None
    if cur: blocks.append(cur)
    print('\n'.join('\n'.join(b) for b in blocks
                     if re.match(r'\s*- from: (pj|pi|pl|pr)_', b[0])))
PY2
}
CHAIN_MODS="$(chain_part mods)"
CHAIN_WIRE="$(chain_part wire)"
# The packaged pull chain too, pointed at the stub and the temp cache.
# Only where it dials moves: the node's registry stays registry.nanocloud.io:5000,
# which a reference naming it explicitly is served from too.
PULL="$(sed -e "s#authority: \"registry.nanocloud.io:5000\"#authority: \"127.0.0.1:$PORT\"#" \
  -e "s#/var/lib/nanocloud.io/image/blobs#$D/blobcache#" "$ROOT/packaging/debian/fluxor-imagepull.yaml")"
PULL_MODS="$(printf '%s\n' "$PULL" | sed -n '/^modules:/,/^wiring:/p' | sed '1d;$d')"
PULL_WIRE="$(printf '%s\n' "$PULL" | sed -n '/^wiring:/,$p' | sed '1d')"
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
platform:
  net: {}
scheduler:
  accept_cycles: true
modules:
${PULL_MODS}
${SANDBOX_MODS}
${CHAIN_MODS}
wiring:
${PULL_WIRE}
${SANDBOX_WIRE}
${CHAIN_WIRE}
YAML

echo "== 4. build config + module table; seed the image-backed pods =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
# No `cmd=`: argv must come from the image config the pull brings.
# Where assembly runs, for this test (the packaged paths otherwise).
wal_put "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobcache"
# A volume: a host directory, bound read-only into the container at /data.
mkdir -p "$D/voldata" && echo "from-the-host" >"$D/voldata/note"
wal_put "/volumes/data" "path=$D/voldata"
# No tag: latest, imagePullPolicy Always by default.
wal_put "/pod-specs/pod-img-1" "image=testapp;iso=1;desired=running;restart=never;volumes=data:/data:ro"
# Tags v1 and v2: different Entrypoints.
wal_put "/pod-specs/pod-v1" "image=testapp:v1;iso=1;desired=running;restart=never"
wal_put "/pod-specs/pod-v2" "image=testapp:v2;iso=1;desired=running;restart=never"
# The node's own registry, named; and v2 pinned by its digest.
wal_put "/pod-specs/pod-host" "image=registry.nanocloud.io:5000/testapp:v2;iso=1;desired=running;restart=never"
wal_put "/pod-specs/pod-pin" "image=testapp@sha256:$DMAN2;iso=1;desired=running;restart=never"
# :latest, IfNotPresent: the reference's cached resolution, shared.
wal_put "/pod-specs/pod-inp1" "image=testapp:latest;pull=IfNotPresent;iso=1;desired=running;restart=never"
# Another registry: refused, the Pod's event, nothing pulled from this one.
wal_put "/pod-specs/pod-foreign" "image=other.example:5000/testapp:v1;pod=default/foreign;iso=1;desired=running;restart=never"

terminal() { # terminal <uid>: Succeeded / Failed / ""
  case "$(wal_last "/pod-lifecycle-status/$1")" in
    *Succeeded*) echo Succeeded ;; *Failed*) echo Failed ;; *) echo "" ;;
  esac
}
run_until_done() { # run_until_done <uid>... — the graph (as root: pivot_root) until each terminates
  sudo -n env FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  local i u left
  for i in $(seq 1 240); do
    left=""
    for u in "$@"; do [ -n "$(terminal "$u")" ] || left="$left $u"; done
    [ -z "$left" ] && break
    sudo -n kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited early"
    sleep 0.25
  done
  sleep 1
  sudo -n kill "$RUNTIME_PID" 2>/dev/null || true
  for i in $(seq 1 20); do sudo -n kill -0 "$RUNTIME_PID" 2>/dev/null || break; sleep 0.25; done
  RUNTIME_PID=""
  [ -z "$left" ] || fail "never terminated:$left (pod-img-1 status='$(wal_last /pod-lifecycle-status/pod-img-1)' digest='$(wal_last /image-digests/pod-img-1)')"
}
dig_of() { wal_last "/image-digests/$1" | sed -n 's/^digest=//p'; }
ran() { # ran <uid> <digest> <log line>: Succeeded, resolved to that digest, said that
  [ "$(terminal "$1")" = Succeeded ] || fail "$1 did not succeed: '$(wal_last "/pod-lifecycle-status/$1")'"
  case ";$(wal_last "/pod-resolved/$1");" in
    ";rootfs=$D/rootfs/$2;digest=$2;"*";cmd="*) ;;
    *) fail "$1 resolved to '$(wal_last "/pod-resolved/$1")', not $2" ;;
  esac
  case "$(wal_last "/sandbox-logs/$1")" in
    *"$3"*) ;; *) fail "$1 logs: '$(wal_last "/sandbox-logs/$1")', want '$3'" ;;
  esac
}

echo "== 5. run the graph (as root — pivot_root) until every pod terminates =="
run_until_done pod-img-1 pod-v1 pod-v2 pod-host pod-pin pod-inp1

echo "== 6. verify every stage of the chain =="
req="$(wal_last "/image-requests/pod-img-1")"
[ "$req" = "reg=;repo=testapp;tag=latest;pin=" ] || fail "pull trigger wrong: '$req'"
echo "   pull trigger: /image-requests/pod-img-1 = $req (no tag: latest, Always — the Pod's own)"
[ "$(dig_of pod-img-1)" = "$DMAN" ] || fail "latest resolved to '$(wal_last /image-digests/pod-img-1)', not the index's arm64 manifest $DMAN"
man="$(wal_last "/image-manifests/$DMAN")"
case "$man" in
  *"config=$DCFG;configsize=$NCFG"*) echo "   index: arm64 manifest selected (amd64 decoy skipped) -> ${DMAN:0:12}…, config descriptor recorded" ;;
  *) fail "manifest record missing the config descriptor: '$man'" ;;
esac
cfg="$(wal_last "/image-config/$DMAN")"
want="entrypoint=/bin/hello-static;cmd=--ignored;env=PATH=/bin;workdir=/;user=root"
[ "$cfg" = "$want" ] || fail "image config wrong: got '$cfg' want '$want'"
echo "   image config: $cfg"
sbx="$(wal_last "/sandboxes/pod-img-1")"
case "$sbx" in
  "cmd=/bin/hello-static --ignored;"*) echo "   argv came from the image (Entrypoint ++ Cmd), not the pod spec" ;;
  *) fail "sandbox argv not config-derived: '$sbx'" ;;
esac
[ -n "$(wal_last "/image-layer-ok/$DMAN-0")" ] && [ -n "$(wal_last "/image-layer-ok/$DMAN-1")" ] \
  || fail "layers not all marked cached"
# Blob files + rootfs are root-owned (the runtime runs as root) — check via sudo.
for dg in "$D1" "$D2"; do
  got="$(sudo -n sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "cached blob mismatch: $dg"
done
echo "   blob cache: both layers digest-verified"
rfs="$(wal_last "/image-rootfs/$DMAN")"
[ "$rfs" = "path=$D/rootfs/$DMAN;state=ready" ] || fail "rootfs record wrong: '$rfs'"
sudo -n test -x "$D/rootfs/$DMAN/bin/hello-static" || fail "assembled rootfs missing the binary"
sudo -n test -f "$D/rootfs/$DMAN/etc/tag" || fail "assembled rootfs missing layer2 file"
sudo -n test -e "$D/rootfs/$DMAN/marker" && fail "whiteout NOT applied (marker survived)"
echo "   rootfs: assembled at $D/rootfs/${DMAN:0:12}… (keyed by the digest), whiteout applied"
ran pod-img-1 "$DMAN" hello-from-image
echo "   container ran: logs carry 'hello-from-image'"
[ "$(wal_last /volume-binds/pod-img-1/0)" = "$D/voldata:/data:ro" ] || fail "volume bind row: '$(wal_last /volume-binds/pod-img-1/0)'"
echo "   volume: $D/voldata bound read-only at /data (the pod ran with it)"
asm="$(wal_last "/sandbox-status/img-asm-$DMAN")"
case "$asm" in
  *destroyed*|*exited*) echo "   assembly job torn down ($asm)" ;;
  *) fail "assembly job not cleaned up: '$asm'" ;;
esac

echo "== 7. a tag change runs the other image: v1 and v2, different Entrypoints =="
ran pod-v1 "$DMAN" hello-from-image
ran pod-v2 "$DMAN2" hello-from-v2
[ "$(dig_of 'testapp+v1')" = "$DMAN" ] && [ "$(dig_of 'testapp+v2')" = "$DMAN2" ] || fail "tags keyed wrong"
echo "   testapp:v1 -> ${DMAN:0:12}… said hello-from-image; testapp:v2 -> ${DMAN2:0:12}… said hello-from-v2"

echo "== 8. registry.nanocloud.io:5000/testapp:v2 (the node's registry, named) and testapp@sha256:… =="
ran pod-host "$DMAN2" hello-from-v2
ran pod-pin "$DMAN2" hello-from-v2
grep -q "GET /v2/testapp/manifests/sha256:$DMAN2 200" "$D/reg.log" || fail "the pin was not fetched by its digest"
echo "   both ran v2; the pin was fetched by digest"

echo "== 9. another registry: refused, the Pod's ErrImagePull event, nothing pulled =="
case "$(wal_last '/image-digests/other.example+5000~testapp+v1')" in
  "refused=registry other.example:5000 is not this node's registry registry.nanocloud.io:5000"*) ;;
  *) fail "foreign reference: '$(wal_last '/image-digests/other.example+5000~testapp+v1')'" ;;
esac
E="$(wal_last /events/default/pod-foreign.ErrImagePull)"
python3 -c 'import json,sys; e=json.loads(sys.argv[1]); assert e["reason"]=="ErrImagePull" and e["type"]=="Warning" and e["involvedObject"]["name"]=="foreign", e' "$E" \
  || fail "no ErrImagePull event: '$E'"
[ -z "$(wal_last /pod-resolved/pod-foreign)" ] && [ -z "$(terminal pod-foreign)" ] || fail "a refused Pod ran"
[ "$(grep -c 'manifests/v1 ' "$D/reg.log")" = 1 ] || fail "the foreign reference was asked of this registry"
echo "   $(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["message"])' "$E")"

echo "== 10. latest moves to v2: Always re-resolves for a new Pod, IfNotPresent keeps what is cached, a Pod keeps its digest =="
cp "$D/reg/index-v2.json" "$D/reg/tags/latest"
wal_put "/pod-specs/pod-latest2" "image=testapp:latest;iso=1;desired=running;restart=never"
wal_put "/pod-specs/pod-inp2" "image=testapp:latest;pull=IfNotPresent;iso=1;desired=running;restart=never"
run_until_done pod-latest2 pod-inp2
ran pod-latest2 "$DMAN2" hello-from-v2
ran pod-inp2 "$DMAN" hello-from-image
[ "$(dig_of pod-img-1)" = "$DMAN" ] || fail "pod-img-1's resolution moved with the tag"
ran pod-img-1 "$DMAN" hello-from-image
echo "   pod-latest2 (Always) -> v2; pod-inp2 (IfNotPresent) -> the cached v1; pod-img-1 still names v1"

echo "== E2E green: pods with image= ONLY — tags resolved to digests (index→platform manifest→config+layers), assembled per digest (whiteouts + /dev), argv from the image config, RAN; tag change, :latest Always, host:port and digest references, a foreign registry refused =="
