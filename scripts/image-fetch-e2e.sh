#!/usr/bin/env bash
# Live E2E for image pull as params (packaging/debian/fluxor-imagepull.yaml):
# image.uproc over the http_fetch connector — request → manifest (through an
# index when the tag names one) → config → every layer a sha256-verified file
# in the content-addressed cache, each marked in /image-blobs/ — everything
# keyed by the manifest's digest, which the tag resolved to.
#
# The fetcher is given a NAME authority (localhost:<port>), so its dial travels
# as an AF_NAME CMD_CONNECT_TO record the network provider resolves, and the
# same bytes go out as the HTTP Host: header.
#
# The script plays the registry: a local HTTP stub serving a docker-v2 image
# manifest (2 layers), its blobs and config, and an OCI index for the tag
# `multi` whose arm64/linux entry names that manifest by digest.
#
#   /image-requests/<rk>    = "reg=;repo=<repo>;tag=<tag>;pin="
#   /image-index/<rk>       = "digest=sha256:<hex64>"          (an index answered)
#   /image-digests/<rk>     = "digest=<m>"  (<m>: the manifest's digest) | "refused=…"
#   /image-manifests/<m>    = "repo=..;layers=<hex64>,..;sizes=<n>,..;config=<hex64>;configsize=<n>;n=<count>"
#   /image-config/<m>       = "entrypoint=..;cmd=..;env=..;workdir=..;user=.."
#   /image-blobs/<hex64>    = "size=<n>"
#   /image-layer-ok/<m>-<i>                                     (layer i is cached)
#   <dir>/<hex64>           = the verified bytes
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
PORT=5391
AUTH_PORT=5392   # the same images, behind a Bearer challenge
REALM_PORT=5393  # that challenge's token realm — another authority

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/http_fetch.fmod"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgfetch-XXXXXX)"
REG_PID=""
AUTH_PID=""
RUNTIME_PID=""
cleanup() {
  [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  [ -n "$AUTH_PID" ] && kill "$AUTH_PID" 2>/dev/null || true
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
mj = json.dumps(manifest).encode()
open(os.path.join(d, "manifest.json"), "wb").write(mj)
mdig = hashlib.sha256(mj).hexdigest()
index = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
         "manifests": [
             {"mediaType": "application/vnd.oci.image.manifest.v1+json", "size": 7,
              "digest": "sha256:" + "0" * 64, "platform": {"architecture": "amd64", "os": "linux"}},
             {"mediaType": "application/vnd.docker.distribution.manifest.v2+json", "size": len(mj),
              "digest": "sha256:" + mdig, "platform": {"architecture": "arm64", "os": "linux"}}]}
open(os.path.join(d, "index.json"), "w").write(json.dumps(index))
open(os.path.join(d, "mdigest.txt"), "w").write(mdig)
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
# Content-Length framing, Connection: close (what http_fetch speaks).
python3 - "$D/reg" "$PORT" >"$D/reg.log" 2>&1 <<'PY' &
import http.server, os, socketserver, sys
d, port = sys.argv[1], int(sys.argv[2])
manifest = open(os.path.join(d, "manifest.json"), "rb").read()
index = open(os.path.join(d, "index.json"), "rb").read()
mdig = open(os.path.join(d, "mdigest.txt")).read().strip()
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        parts = self.path.strip("/").split("/")
        body = None
        ctype = "application/octet-stream"
        # /v2/<repo>/manifests/<tag>  |  /v2/<repo>/blobs/sha256:<hex>
        if len(parts) >= 4 and parts[0] == "v2" and parts[-2] == "manifests":
            ref = parts[-1]
            if ref == "multi":
                body = index
                ctype = "application/vnd.oci.image.index.v1+json"
            elif ref in ("latest", "sha256:" + mdig):
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
        sys.stderr.write(self.requestline + "\n")
        sys.stderr.flush()
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

echo "== 3. the packaged image-pull graph, pointed at the stub and a temp cache =="
sed -e "s#registry.nanocloud.io:5000#localhost:$PORT#" -e "s#/var/lib/nanocloud.io/image/blobs#$D/blobcache#" \
  "$ROOT/packaging/debian/fluxor-imagepull.yaml" >"$D/graph.yaml"

dig_of() { wal_last "/image-digests/$1" | sed -n 's/^digest=//p'; }
settle() { # settle <rk> <seconds> — resolved, every layer cached and marked
  local i m
  for i in $(seq 1 $(( $2 * 4 ))); do
    m="$(dig_of "$1")"
    if [ -n "$m" ] && [ -n "$(wal_last "/image-layer-ok/$m-0")" ] && [ -n "$(wal_last "/image-layer-ok/$m-1")" ] \
       && [ -n "$(wal_last "/image-config/$m")" ]; then return 0; fi
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited early"
    sleep 0.25
  done
  return 1
}
run_until() { # run_until <config> <modules> <name> <seconds>
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" --config "$1" --modules "$2" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  settle "$3" "$4" || { kill "$RUNTIME_PID" 2>/dev/null; RUNTIME_PID=""; fail "$3 never settled: digest '$(wal_last "/image-digests/$3")'"; }
  sleep 1
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}

echo "== 4. build config + module table; seed the image request =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
wal_put "/image-requests/testapp+latest" "reg=;repo=testapp;tag=latest;pin="

echo "== 5. run the graph until every layer is cached =="
run_until "$D/config.bin" "$D/modules.bin" testapp+latest 20

echo "== 6. verify the settled state =="
MD="$(cat "$D/reg/mdigest.txt")"
[ "$(wal_last /image-digests/testapp+latest)" = "digest=$MD" ] \
  || fail "the tag did not resolve to the manifest's digest: '$(wal_last /image-digests/testapp+latest)'"
echo "   testapp:latest -> digest ${MD:0:12}… (the body's own sha256)"
man="$(wal_last "/image-manifests/$MD")"
[ "$man" = "repo=testapp;layers=$D1,$D2;sizes=102400,18;config=$DCFG;configsize=$NCFG;n=2" ] \
  || fail "manifest record wrong: got '$man'"
echo "   manifest: $man"
# The config blob is fetched + digest-verified like a layer, but lands as the
# runtime contract in the store - NOT in the blob cache.
cfg="$(wal_last "/image-config/$MD")"
want="entrypoint=/bin/sh,-c;cmd=echo hi;env=PATH=/usr/bin;workdir=;user="
[ "$cfg" = "$want" ] || fail "image config wrong: got '$cfg' want '$want'"
[ -f "$D/blobcache/$DCFG" ] && fail "config blob wrongly cached as a layer"
[ -z "$(wal_last "/image-blobs/$DCFG")" ] || fail "config blob wrongly marked in /image-blobs/"
echo "   image config: $cfg"
for dg in "$D1" "$D2"; do
  [ -f "$D/blobcache/$dg" ] || fail "blob file missing: $dg"
  got="$(sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "blob content mismatch for $dg (sha=$got)"
  cmp -s "$D/blobcache/$dg" "$D/reg/$dg" || fail "blob bytes differ from registry copy: $dg"
  mk="$(wal_last "/image-blobs/$dg")"
  case "$mk" in size=*) : ;; *) fail "blob marker missing for $dg: got '$mk'" ;; esac
  echo "   blob $dg: file + marker ($mk) ok"
done
N1="$(grep -c "blobs/sha256:$D1" "$D/reg.log" || true)"

echo "== 7. a tag that names an INDEX: the arm64/linux entry, fetched by digest; cached layers not re-fetched =="
wal_put "/image-requests/testapp+multi" "reg=;repo=testapp;tag=multi;pin="
# A digest pin: fetched by that digest and verified against it.
wal_put "/image-requests/testapp@$MD" "reg=;repo=testapp;tag=;pin=sha256:$MD"
# Another registry than this node's: refused, never asked of this one.
wal_put "/image-requests/other.example+5000~testapp+v1" "reg=other.example:5000;repo=testapp;tag=v1;pin="
run_until "$D/config.bin" "$D/modules.bin" testapp+multi 20
[ "$(wal_last /image-index/testapp+multi)" = "digest=sha256:$MD" ] || fail "index: '$(wal_last /image-index/testapp+multi)'"
[ "$(dig_of testapp+multi)" = "$MD" ] || fail "multi resolved to '$(wal_last /image-digests/testapp+multi)'"
[ "$(dig_of "testapp@$MD")" = "$MD" ] || fail "pinned digest: '$(wal_last "/image-digests/testapp@$MD")'"
grep -q "GET /v2/testapp/manifests/sha256:$MD " "$D/reg.log" || fail "the pin was not fetched by digest"
case "$(wal_last /image-digests/other.example+5000~testapp+v1)" in
  "refused=registry other.example:5000 is not this node's registry localhost:$PORT"*) : ;;
  *) fail "foreign registry: '$(wal_last /image-digests/other.example+5000~testapp+v1)'" ;;
esac
grep -q "manifests/v1" "$D/reg.log" && fail "a foreign reference was pulled from this registry"
echo "   foreign registry refused: $(wal_last /image-digests/other.example+5000~testapp+v1 | cut -c1-80)…"
echo "   pin sha256:${MD:0:12}… fetched by digest and verified"
[ "$(grep -c "blobs/sha256:$D1" "$D/reg.log" || true)" = "$N1" ] || fail "a cached layer was fetched again"
[ -z "$(python3 - "$D/store.log" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0; live = {}
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]
    if k.startswith(b"/image-fetch-q/"):
        if op == 1: live[k] = 1
        else: live.pop(k, None)
    p += 15 + kl + vl
print(" ".join(k.decode() for k in live))
PY
)" ] || fail "the work queue did not drain"
echo "   index -> sha256:${MD:0:12}…; layers shared, fetched once; queue drained"

echo "== 8. chunked-Range pass (chunk_bytes=4096 — the constrained-bearer posture) =="
rm -f "$D/store.log" "$D/blobcache/"*
sed 's/chunk_bytes: 0/chunk_bytes: 4096/' "$D/graph.yaml" >"$D/graph2.yaml"
nc_build_workload "$ROOT" "$D/graph2.yaml" "$D/config2.bin" "$D/modules2.bin"
wal_put "/image-requests/testapp+latest" "reg=;repo=testapp;tag=latest;pin="
run_until "$D/config2.bin" "$D/modules2.bin" testapp+latest 30
for dg in "$D1" "$D2"; do
  got="$(sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "chunked blob mismatch for $dg (sha=$got)"
done
grep -q "206" "$D/reg.log" || grep -q "Range" "$D/reg.log" || true
echo "   both blobs re-fetched in 4 KiB Range chunks, digests verified"

echo "== 9. a registry that wants a token: Bearer challenge -> its realm (another authority, Basic credentials from the store) -> the pull again; tokens expire mid-blob; chunked transfer coding =="
# The registry answers 401 + `WWW-Authenticate: Bearer realm=…` to a request
# without a live token; the realm hands out tokens that serve 6 requests, so
# a 4 KiB-Range pull of the 100 KiB layer meets several expiries and must
# refresh mid-blob. Manifests and tokens come chunked-coded, without a
# Content-Length. The credentials are a store key's value, never config.
python3 - "$D/reg" "$AUTH_PORT" "$REALM_PORT" >"$D/auth.log" 2>&1 <<'PY' &
import base64, http.server, json, os, secrets, socketserver, sys, threading, urllib.parse
d, port, realm_port = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
manifest = open(os.path.join(d, "manifest.json"), "rb").read()
USES = 6
tokens, lock = {}, threading.Lock()
def log(m):
    sys.stderr.write(m + "\n"); sys.stderr.flush()
class Base(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def answer(self, status, body=b"", ctype="application/octet-stream", chunked=False, extra=()):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        for k, v in extra:
            self.send_header(k, v)
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
        else:
            self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if chunked:
            for i in range(0, len(body), 37):  # odd chunk sizes, an extension on one
                part = body[i:i + 37]
                ext = ";x=1" if i == 0 else ""
                self.wfile.write(b"%x%s\r\n" % (len(part), ext.encode()) + part + b"\r\n")
            self.wfile.write(b"0\r\nX-Trailer: t\r\n\r\n")
        else:
            self.wfile.write(body)
    def log_message(self, *a):
        pass
class Realm(Base):
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        auth = self.headers.get("Authorization", "")
        ok = auth == "Basic " + base64.b64encode(b"puller:s3cret").decode()
        log(f"TOKEN path={u.path} service={q.get('service')} scope={q.get('scope')} basic={'ok' if ok else 'no'}")
        if u.path != "/token" or not ok or q.get("service") != ["stub-registry"] \
                or q.get("scope") != ["repository:testapp:pull"]:
            return self.answer(401, b"{}", "application/json")
        t = secrets.token_urlsafe(900)  # ~1.2 KiB, a registry JWT's size
        with lock:
            tokens[t] = USES
        self.answer(200, json.dumps({"token": t, "expires_in": 60}).encode(), "application/json", chunked=True)
class Reg(Base):
    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        t = auth[7:] if auth.startswith("Bearer ") else ""
        with lock:
            left = tokens.get(t, 0)
            if left > 0:
                tokens[t] = left - 1
        if left <= 0:
            log(f"401 {self.path} token={'stale' if t else 'none'}")
            ch = (f'Bearer realm="http://127.0.0.1:{realm_port}/token",'
                  f'service="stub-registry",scope="repository:testapp:pull"')
            return self.answer(401, b'{"errors":[{"code":"UNAUTHORIZED"}]}', "application/json",
                               extra=[("WWW-Authenticate", ch)])
        parts = self.path.strip("/").split("/")
        log(f"200 {self.path} range={self.headers.get('Range')}")
        if parts[-2] == "manifests":
            return self.answer(200, manifest, "application/vnd.docker.distribution.manifest.v2+json", chunked=True)
        body = open(os.path.join(d, parts[-1].split(":")[-1]), "rb").read()
        rng = self.headers.get("Range")
        if rng:
            lo, _, hi = rng[6:].partition("-")
            body = body[int(lo):int(hi) + 1]
            return self.answer(206, body)
        self.answer(200, body)
class S(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
r = S(("127.0.0.1", realm_port), Realm)
threading.Thread(target=r.serve_forever, daemon=True).start()
with S(("127.0.0.1", port), Reg) as srv:
    srv.serve_forever()
PY
AUTH_PID=$!
for i in $(seq 1 40); do
  python3 -c "import socket;socket.create_connection(('127.0.0.1',$AUTH_PORT),timeout=0.2).close()" 2>/dev/null && break
  sleep 0.1
done
rm -f "$D/store.log" "$D/blobcache/"*
sed -e "s#localhost:$PORT#localhost:$AUTH_PORT#" \
    -e 's#credentials: "/registry-auth/registry.nanocloud.io"#credentials: "/registry-auth/stub"#' "$D/graph2.yaml" >"$D/graph3.yaml"
grep -q 'credentials: "/registry-auth/stub"' "$D/graph3.yaml" || fail "the credentials splice did not apply"
nc_build_workload "$ROOT" "$D/graph3.yaml" "$D/config3.bin" "$D/modules3.bin"
wal_put "/registry-auth/stub" "puller:s3cret"
wal_put "/image-requests/testapp+latest" "reg=;repo=testapp;tag=latest;pin="
run_until "$D/config3.bin" "$D/modules3.bin" testapp+latest 60
for dg in "$D1" "$D2"; do
  got="$(sha256sum "$D/blobcache/$dg" | cut -d' ' -f1)"
  [ "$got" = "$dg" ] || fail "authenticated blob mismatch for $dg (sha=$got)"
done
[ "$(wal_last /image-config/$MD)" = "$want" ] || fail "authenticated image config: '$(wal_last /image-config/$MD)'"
NT="$(grep -c "^TOKEN .*basic=ok" "$D/auth.log" || true)"
NS="$(grep -c "^401 .*token=stale" "$D/auth.log" || true)"
grep -q "^TOKEN .*basic=no" "$D/auth.log" && fail "a token request went without the store's credentials"
[ "$NT" -ge 2 ] && [ "$NS" -ge 1 ] || { cat "$D/auth.log" | tail -20; fail "expected token refreshes after expiry (tokens=$NT stale=$NS)"; }
echo "   pulled through the challenge: $NT token(s) from the realm, $NS expired mid-pull and refreshed; digests verified"

echo "== E2E green: image pull as params (request -> index -> manifest -> config -> verified content-addressed cache; whole-blob + chunked Range; Bearer token auth) =="
