#!/usr/bin/env bash
# Live E2E for the pod-image pull chain: image_puller computes the missing-layer plan (decision),
# image_fetcher performs the OCI distribution-API fetch (effect) — manifest →
# missing digests → fetch → digest-verify → content-addressed blob cache.
#
# The fetcher is given a NAME authority (localhost:<port>), so its dial travels
# as an AF_NAME CMD_CONNECT_TO record the network provider resolves, and the
# same bytes go out as the HTTP Host: header. image-run-e2e.sh dials a literal,
# which travels as AF_INET.
#
# The script plays the registry: a local HTTP stub serving a docker-v2 image
# manifest (2 layers) + its blobs. It seeds /image-requests/<name>, runs the
# graph (linux_net + image_fetcher + image_puller), and asserts the settled
# state: /image-manifests/ recorded, both blob files on disk byte-identical,
# /blobs/ markers present, and the pull plan drained to "pull=".
#
# Data model:
#   /image-requests/<name>  = "repo=<repo>;tag=<tag>"
#   /image-manifests/<name> = "layers=<hex64>,..;sizes=<n>,..;config=<hex64>;configsize=<n>"
#   /image-config/<name>    = "entrypoint=..;cmd=..;env=..;workdir=..;user=.."
#   /image-pull-plan/<name> = "pull=<hex64>,..."  →  "pull="
#   /blobs/<hex64>          = "size=<n>"
#   <blob_dir>/<hex64>      = the verified bytes
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
PORT=5391

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/image_fetcher.fmod" "$MODULES_DIR/image_puller.fmod"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgfetch-XXXXXX)"
REG_PID=""
RUNTIME_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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
  python3 - "$D/store.log" "$1" <<'PY'
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

echo "== 1. author two layer blobs + the image manifest =="
mkdir -p "$D/reg" "$D/blobcache"
# Layer 1: 100 KB of deterministic bytes (multi-frame stream); layer 2: small.
python3 - "$D/reg" <<'PY'
import hashlib, json, os, sys
d = sys.argv[1]
l1 = bytes((i * 31 + 7) % 256 for i in range(100 * 1024))
l2 = b"config-layer-tiny\n"
digs = []
for l in (l1, l2):
    h = hashlib.sha256(l).hexdigest()
    open(os.path.join(d, h), "wb").write(l)
    digs.append((h, len(l)))
# The image config blob - served like any other blob, but parsed in memory and
# projected as /image-config/, never cached as a layer.
cfg = json.dumps({"architecture": "arm64", "os": "linux",
                  "config": {"Env": ["PATH=/usr/bin"],
                             "Entrypoint": ["/bin/sh", "-c"],
                             "Cmd": ["echo hi"]}}).encode()
cfg_hex = hashlib.sha256(cfg).hexdigest()
open(os.path.join(d, cfg_hex), "wb").write(cfg)
manifest = {
    "schemaVersion": 2,
    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
    "config": {"mediaType": "application/vnd.docker.container.image.v1+json",
               "size": len(cfg), "digest": "sha256:" + cfg_hex},
    "layers": [
        {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
         "size": n, "digest": "sha256:" + h} for h, n in digs
    ],
}
open(os.path.join(d, "manifest.json"), "w").write(json.dumps(manifest))
open(os.path.join(d, "digests.txt"), "w").write(
    "\n".join([h for h, _ in digs] + [cfg_hex, str(len(cfg))]))
PY
D1="$(sed -n 1p "$D/reg/digests.txt")"
D2="$(sed -n 2p "$D/reg/digests.txt")"
DCFG="$(sed -n 3p "$D/reg/digests.txt")"
NCFG="$(sed -n 4p "$D/reg/digests.txt")"
echo "   layers: $D1 (100K), $D2 (tiny); config: $DCFG ($NCFG B)"

echo "== 2. start the registry stub (:$PORT) =="
# Registry stub: GET /v2/<repo>/manifests/<tag> and /v2/<repo>/blobs/sha256:<hex>,
# Content-Length framing, Connection: close (what image_fetcher speaks).
python3 - "$D/reg" "$PORT" >"$D/reg.log" 2>&1 <<'PY' &
import http.server, os, socketserver, sys
d, port = sys.argv[1], int(sys.argv[2])
manifest = open(os.path.join(d, "manifest.json"), "rb").read()
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        body = None
        ctype = "application/octet-stream"
        # /v2/<repo>/manifests/<tag>  |  /v2/<repo>/blobs/sha256:<hex>
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "manifests":
            body = manifest
            ctype = "application/vnd.docker.distribution.manifest.v2+json"
        elif len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "blobs":
            hexd = parts[-1].split(":")[-1]
            p = os.path.join(d, hexd)
            if os.path.exists(p):
                body = open(p, "rb").read()
        if body is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return
        status = 200
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            spec = rng[6:]
            lo_s, _, hi_s = spec.partition("-")
            lo = int(lo_s) if lo_s else 0
            hi = int(hi_s) if hi_s else len(body) - 1
            body = body[lo:hi + 1]
            status = 206
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a):
        sys.stderr.write("%s\n" % (a[1] if len(a) > 1 else a))
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

echo "== 3. write the image-fetch graph (temp blob cache) =="
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
    authority: "localhost:$PORT"
    blob_dir: "$D/blobcache"
    chunk_bytes: 0
    boot_delay_ms: 200
wiring:
  - from: linux_net.net_out
    to: image_fetcher.net_in
  - from: image_fetcher.net_out
    to: linux_net.net_in
  - from: image_puller.status
    to: image_puller.changes
  - from: image_fetcher.status
    to: image_fetcher.changes
YAML

echo "== 4. build config + module table; seed the image request =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
wal_put "/image-requests/testapp" "repo=testapp;tag=latest"

echo "== 5. run the graph until the plan drains =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
settled=0
for i in $(seq 1 60); do
  plan="$(wal_last "/image-pull-plan/testapp")"
  if [ "$plan" = "pull=" ]; then settled=1; break; fi
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited early"
  sleep 0.25
done
kill "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""
[ "$settled" = 1 ] || fail "pull plan never drained: got '$(wal_last "/image-pull-plan/testapp")'"

echo "== 6. verify the settled state =="
man="$(wal_last "/image-manifests/testapp")"
case "$man" in
  layers=$D1,$D2\;sizes=102400,18\;config=$DCFG\;configsize=$NCFG) echo "   manifest: $man" ;;
  *) fail "manifest record wrong: got '$man'" ;;
esac
# The config blob is fetched + digest-verified like a layer, but lands as the
# runtime contract in the store - NOT in the blob cache and NOT in the plan.
cfg="$(wal_last "/image-config/testapp")"
want="entrypoint=/bin/sh,-c;cmd=echo hi;env=PATH=/usr/bin"
[ "$cfg" = "$want" ] || fail "image config wrong: got '$cfg' want '$want'"
[ -f "$D/blobcache/$DCFG" ] && fail "config blob wrongly cached as a layer"
[ -z "$(wal_last "/blobs/$DCFG")" ] || fail "config blob wrongly marked in /blobs/"
echo "   image config: $cfg"
for dg in "$D1" "$D2"; do
  [ -f "$D/blobcache/$dg" ] || fail "blob file missing: $dg"
  got="$(sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "blob content mismatch for $dg (sha=$got)"
  cmp -s "$D/blobcache/$dg" "$D/reg/$dg" || fail "blob bytes differ from registry copy: $dg"
  mk="$(wal_last "/blobs/$dg")"
  case "$mk" in size=*) : ;; *) fail "blob marker missing for $dg: got '$mk'" ;; esac
  echo "   blob $dg: file + marker ($mk) ok"
done

echo "== 7. chunked-Range pass (chunk_bytes=4096 — the constrained-bearer posture) =="
rm -f "$D/store.log" "$D/blobcache/"*
sed 's/chunk_bytes: 0/chunk_bytes: 4096/' "$D/graph.yaml" >"$D/graph2.yaml"
nc_build_workload "$ROOT" "$D/graph2.yaml" "$D/config2.bin" "$D/modules2.bin"
wal_put "/image-requests/testapp" "repo=testapp;tag=latest"
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config2.bin" --modules "$D/modules2.bin" >"$D/run2.log" 2>&1 &
RUNTIME_PID=$!
settled=0
for i in $(seq 1 80); do
  plan="$(wal_last "/image-pull-plan/testapp")"
  if [ "$plan" = "pull=" ]; then settled=1; break; fi
  kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited early (chunked pass)"
  sleep 0.25
done
kill "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""
[ "$settled" = 1 ] || fail "chunked pull plan never drained: got '$(wal_last "/image-pull-plan/testapp")'"
for dg in "$D1" "$D2"; do
  got="$(sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "chunked blob mismatch for $dg (sha=$got)"
done
echo "   both blobs re-fetched in 4 KiB Range chunks, digests verified"

echo "== E2E green: pod-image pull chain (request -> manifest -> plan -> fetch -> verified content-addressed cache; whole-blob + chunked Range) =="
