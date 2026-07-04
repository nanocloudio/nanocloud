#!/usr/bin/env bash
# Live E2E for the COMPLETE pod-image path: a pod
# spec carrying `image=` drives pull → verify → assemble → run, all fmods:
#
#   the pod chain   image= → /image-requests/ (pull trigger) + Pending gate
#   image_fetcher    manifest + blobs from the registry → verified blob cache
#   image_puller     the missing-layer plan (drains as blobs land)
#   image_assembler  drained plan → assembly job → /image-rootfs/ ready
#   sandbox_runner   runs the assembly job, then the pod itself (pivot_root)
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
# PASS = pod reaches Succeeded and /sandbox-logs/ carries the binary's output,
# with the whiteout applied in the assembled rootfs.
#
# Needs passwordless sudo (pivot_root/mknod): the RUNTIME runs under sudo,
# exactly as the production node service does (root).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=5392

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
sudo -n true 2>/dev/null || { echo "SKIP: passwordless sudo unavailable (pivot_root needs root)"; exit 0; }
for m in image_fetcher image_puller image_assembler store_source store_effect decision sandbox_runner; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgrun-XXXXXX)"
chmod 755 "$D"
REG_PID=""
RUNTIME_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && sudo -n kill "$RUNTIME_PID" 2>/dev/null || true
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"
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

echo "== 1. author a runnable 2-layer image (static binary + whiteout) =="
mkdir -p "$D/reg" "$D/blobcache" "$D/rootfs" "$D/l1/bin" "$D/l2/etc"
cat > "$D/hello.c" <<'EOF'
#include <unistd.h>
int main(void) { write(1, "hello-from-image\n", 17); return 0; }
EOF
cc -static -o "$D/l1/bin/hello-static" "$D/hello.c"
echo "to-be-whited-out" > "$D/l1/marker"
echo "layer2" > "$D/l2/etc/tag"
touch "$D/l2/.wh.marker"                       # OCI whiteout: delete /marker
tar -czf "$D/reg/l1.tgz" -C "$D/l1" bin marker
tar -czf "$D/reg/l2.tgz" -C "$D/l2" etc .wh.marker
python3 - "$D/reg" <<'PY'
import hashlib, json, os, sys
d = sys.argv[1]

def land(raw):
    """Store `raw` content-addressed under its digest; return (hex, len)."""
    h = hashlib.sha256(raw).hexdigest()
    open(os.path.join(d, h), "wb").write(raw)
    return h, len(raw)

digs = []
for f in ("l1.tgz", "l2.tgz"):
    path = os.path.join(d, f)
    b = open(path, "rb").read()
    os.remove(path)
    digs.append(land(b))

# The image config: what the runtime contract comes from. Entrypoint ++ Cmd is
# the argv the pod must end up spawning without the spec ever naming it.
config = {
    "architecture": "arm64",
    "os": "linux",
    "config": {
        "Env": ["PATH=/bin"],
        "Entrypoint": ["/bin/hello-static"],
        "Cmd": ["--ignored"],
        "WorkingDir": "/",
        "User": "root",
    },
}
cfg_hex, cfg_len = land(json.dumps(config).encode())

manifest = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
               "size": cfg_len, "digest": "sha256:" + cfg_hex},
    "layers": [
        {"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
         "size": n, "digest": "sha256:" + h} for h, n in digs
    ],
}
man_hex, man_len = land(json.dumps(manifest).encode())

# The tag resolves to an INDEX, not the manifest — the multi-arch shape. The
# amd64 entry comes FIRST so "take the first entry" would pick the wrong one.
index = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.index.v1+json",
    "manifests": [
        {"mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": 3, "digest": "sha256:" + "e" * 64,
         "platform": {"architecture": "amd64", "os": "linux"}},
        {"mediaType": "application/vnd.oci.image.manifest.v1+json",
         "size": man_len, "digest": "sha256:" + man_hex,
         "platform": {"architecture": "arm64", "os": "linux"}},
    ],
}
open(os.path.join(d, "index.json"), "w").write(json.dumps(index))
open(os.path.join(d, "digests.txt"), "w").write(
    "\n".join([h for h, _ in digs] + [cfg_hex, str(cfg_len), man_hex]))
PY
D1="$(sed -n 1p "$D/reg/digests.txt")"
D2="$(sed -n 2p "$D/reg/digests.txt")"
DCFG="$(sed -n 3p "$D/reg/digests.txt")"
NCFG="$(sed -n 4p "$D/reg/digests.txt")"
DMAN="$(sed -n 5p "$D/reg/digests.txt")"
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"

echo "== 2. start the registry stub (:$PORT) =="
python3 - "$D/reg" "$PORT" >"$D/reg.log" 2>&1 <<'PY' &
import http.server, os, socketserver, sys
d, port = sys.argv[1], int(sys.argv[2])
index = open(os.path.join(d, "index.json"), "rb").read()
def blob(ref):
    p = os.path.join(d, ref.split(":")[-1])
    return open(p, "rb").read() if os.path.exists(p) else None
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        body = None
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "manifests":
            # A tag resolves to the index; a digest to that exact manifest.
            body = blob(parts[-1]) if parts[-1].startswith("sha256:") else index
        elif len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "blobs":
            body = blob(parts[-1])
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
curl -s "http://127.0.0.1:$PORT/v2/testapp/manifests/latest" | cmp -s - "$D/reg/index.json" \
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
    print(src[src.index('  # \u2500\u2500 project'):src.index('\nwiring:')].rstrip())
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
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
platform:
  net: {}
scheduler:
  accept_cycles: true
modules:
  - name: image_puller
  - name: image_fetcher
    registry_ip: 2130706433
    registry_port: $PORT
    host: "127.0.0.1"
    blob_dir: "$D/blobcache"
    chunk_bytes: 0
    boot_delay_ms: 200
  - name: image_assembler
    script: "$D/assemble-image.sh"
    rootfs_base: "$D/rootfs"
    blob_dir: "$D/blobcache"
  - name: sandbox_runner
${CHAIN_MODS}
wiring:
  - from: linux_net.net_out
    to: image_fetcher.net_in
  - from: image_fetcher.net_out
    to: linux_net.net_in
  - from: image_puller.status
    to: image_puller.changes
  - from: image_fetcher.status
    to: image_fetcher.changes
  - from: image_assembler.status
    to: image_assembler.changes
  - from: sandbox_runner.status
    to: sandbox_runner.changes
${CHAIN_WIRE}
YAML

echo "== 4. build config + module table; seed the image-backed pod =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
# No `cmd=`: argv must come from the image config the fetcher pulls.
wal_put "/pod-specs/pod-img-1" "image=testapp;iso=1;desired=running;restart=never"

echo "== 5. run the graph (as root — pivot_root) until the pod terminates =="
sudo -n env FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
phase=""
for i in $(seq 1 120); do
  st="$(wal_last "/pod-lifecycle-status/pod-img-1")"
  case "$st" in
    *Succeeded*) phase="Succeeded"; break ;;
    *Failed*)    phase="Failed"; break ;;
  esac
  sudo -n kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited early"
  sleep 0.25
done
sudo -n kill "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""
[ "$phase" = "Succeeded" ] || fail "pod never Succeeded: status='$(wal_last "/pod-lifecycle-status/pod-img-1")' cfg='$(wal_last "/image-config/testapp")' man='$(wal_last "/image-manifests/testapp")' rootfs='$(wal_last "/image-rootfs/testapp")' plan='$(wal_last "/image-pull-plan/testapp")' sbx='$(wal_last "/sandbox-status/pod-img-1")' asmjob='$(wal_last "/sandboxes/img-asm-testapp")' asmst='$(wal_last "/sandbox-status/img-asm-testapp")' asmlog='$(wal_last "/sandbox-logs/img-asm-testapp")'"

echo "== 6. verify every stage of the chain =="
req="$(wal_last "/image-requests/testapp")"
[ "$req" = "repo=testapp;tag=latest" ] || fail "pull trigger wrong: '$req'"
echo "   pull trigger: $req (written by pod_lifecycle)"
man="$(wal_last "/image-manifests/testapp")"
case "$man" in
  *"config=$DCFG;configsize=$NCFG"*) echo "   index: arm64 manifest selected (amd64 decoy skipped), config descriptor recorded" ;;
  *) fail "manifest record missing the config descriptor: '$man'" ;;
esac
cfg="$(wal_last "/image-config/testapp")"
want="entrypoint=/bin/hello-static;cmd=--ignored;env=PATH=/bin;workdir=/;user=root"
[ "$cfg" = "$want" ] || fail "image config wrong: got '$cfg' want '$want'"
echo "   image config: $cfg"
sbx="$(wal_last "/sandboxes/pod-img-1")"
case "$sbx" in
  "cmd=/bin/hello-static --ignored;"*) echo "   argv came from the image (Entrypoint ++ Cmd), not the pod spec" ;;
  *) fail "sandbox argv not config-derived: '$sbx'" ;;
esac
plan="$(wal_last "/image-pull-plan/testapp")"
[ "$plan" = "pull=" ] || fail "plan not drained: '$plan'"
# Blob files + rootfs are root-owned (the runtime runs as root) — check via sudo.
for dg in "$D1" "$D2"; do
  got="$(sudo -n sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "cached blob mismatch: $dg"
done
echo "   blob cache: both layers digest-verified"
rfs="$(wal_last "/image-rootfs/testapp")"
[ "$rfs" = "path=$D/rootfs/testapp;state=ready" ] || fail "rootfs record wrong: '$rfs'"
sudo -n test -x "$D/rootfs/testapp/bin/hello-static" || fail "assembled rootfs missing the binary"
sudo -n test -f "$D/rootfs/testapp/etc/tag" || fail "assembled rootfs missing layer2 file"
sudo -n test -e "$D/rootfs/testapp/marker" && fail "whiteout NOT applied (marker survived)"
echo "   rootfs: assembled at $D/rootfs/testapp, whiteout applied"
logs="$(wal_last "/sandbox-logs/pod-img-1")"
case "$logs" in
  *hello-from-image*) echo "   container ran: logs carry 'hello-from-image'" ;;
  *) fail "sandbox logs wrong: got '$logs', want 'hello-from-image'" ;;
esac
asm="$(wal_last "/sandbox-status/img-asm-testapp")"
case "$asm" in
  *destroyed*|*exited*) echo "   assembly job torn down ($asm)" ;;
  *) fail "assembly job not cleaned up: '$asm'" ;;
esac

echo "== E2E green: pod with image= ONLY — index→platform manifest→config+layers pulled, assembled (whiteouts + /dev), argv from the image config, and RAN — the full container chain =="
