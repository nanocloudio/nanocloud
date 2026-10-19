# Shared by the E2Es that run Pods of workload images on a node in the
# bare-metal shape (scripts/workload-image-e2e.sh, scripts/apps-e2e.sh):
# building and serving workload images, and the node render that pulls and
# runs them. Sourced after scripts/fluxor-env.sh, with D, ROOT and fail set.

# wn_publish <repo> <graph.yaml> [project-dir]: build the workload image (from
# the project the graph belongs to, so its modules resolve) and lay it out
# under $D/reg/<repo> as `fluxor publish workload` would push it.
wn_publish() {
  local proj="${3:-$ROOT}"
  (cd "$proj" && fluxor build "$2" --emit=workload -o "$D/wimg-$1" >"$D/wbuild-$1.log" 2>&1) \
    || { cat "$D/wbuild-$1.log"; fail "fluxor build --emit=workload ($1)"; }
  python3 - "$D/wimg-$1" "$D/reg/$1" <<'PY'
import hashlib, json, os, sys
src, reg = sys.argv[1:3]
os.makedirs(reg, exist_ok=True)
def blob(b):
    h = hashlib.sha256(b).hexdigest()
    open(os.path.join(reg, h), "wb").write(b)
    return h, len(b)
meta = json.load(open(os.path.join(src, "workload-image.json")))
cfg_h, cfg_n = blob(b"{}")
flxa_h, flxa_n = blob(open(os.path.join(src, "graph.flxa"), "rb").read())
layers = [{"mediaType": "application/vnd.nanocloud.fluxor.flxa.v1", "digest": "sha256:" + flxa_h,
           "size": flxa_n, "annotations": {"org.opencontainers.image.title": "graph.flxa"}}]
for m in meta["modules"]:
    b = open(os.path.join(src, "modules", m["name"] + ".fmod"), "rb").read()
    h, n = blob(b)
    assert h == m["sha256"], (m["name"], h, m["sha256"])
    layers.append({"mediaType": "application/vnd.nanocloud.fluxor.module.v1", "digest": "sha256:" + h,
                   "size": n, "annotations": {"io.fluxor.module.name": m["name"],
                                              "io.fluxor.module.name-hash": str(m["name_hash"])}})
manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": "application/vnd.nanocloud.fluxor.workload-image.v1",
            "config": {"mediaType": "application/vnd.oci.empty.v1+json", "digest": "sha256:" + cfg_h, "size": cfg_n},
            "layers": layers, "annotations": {"io.fluxor.kind": "workload-image"}}
open(os.path.join(reg, "manifest.json"), "w").write(json.dumps(manifest))
print("   " + os.path.basename(reg) + ":", " ".join(l["digest"][7:19] for l in layers))
PY
}

# wn_registry <port>: serve $D/reg/<repo> as registry repos nanocloud/<repo>,
# tag `1` (manifest and blobs); its pid in REG_PID, its requests in $D/reg.log.
# While $D/reg.fail holds an HTTP status, every request is answered that
# status instead (a registry that is up and failing: 503, 500, …).
wn_registry() {
  python3 - "$D/reg" "$1" "$D/reg.fail" >>"$D/reg.log" 2>&1 <<'PY' &
import http.server, os, socketserver, sys
d, port, failf = sys.argv[1], int(sys.argv[2]), sys.argv[3]
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        try:
            fail = int(open(failf).read().strip())
        except (OSError, ValueError):
            fail = 0
        if fail:
            sys.stderr.write(f"{self.path} {fail}\n"); sys.stderr.flush()
            self.send_response(fail)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            return
        parts = self.path.strip("/").split("/")
        body, ctype = None, "application/octet-stream"
        repo = os.path.join(d, parts[2]) if len(parts) == 5 and parts[:2] == ["v2", "nanocloud"] else ""
        if repo and parts[3] == "manifests" and parts[4] == "1" and os.path.isdir(repo):
            body = open(os.path.join(repo, "manifest.json"), "rb").read()
            ctype = "application/vnd.oci.image.manifest.v1+json"
        elif repo and parts[3] == "blobs":
            p = os.path.join(repo, parts[4].split(":")[-1])
            if os.path.exists(p):
                body = open(p, "rb").read()
        sys.stderr.write(f"{self.path} {'200' if body is not None else '404'}\n"); sys.stderr.flush()
        self.send_response(200 if body is not None else 404)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body or b"")))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body or b"")
    def log_message(self, *a): pass
class S(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
with S(("127.0.0.1", port), H) as srv:
    srv.serve_forever()
PY
  REG_PID=$!
}

# wn_splice <graph.yaml> <node> <blobs-dir> <registry host:port> [ca.der]: onto
# the rendered control plane, the kubelet's graph-pod slice and its image
# plane, on one store scoped to <node> (scripts/metal-plane-e2e.sh's shape).
# With a CA, the pull goes through a `tls` client that authenticates the
# registry as registry.nanocloud.io by that CA (fluxor-image-fetch.yaml's
# posture); without, http_fetch dials it in the clear.
wn_splice() {
  python3 - "$1" "$ROOT" "$2" "$3" "$4" "${5:-}" <<'PY2'
import re, sys
out, root, node, blobs, registry, ca = sys.argv[1:7]
g = open(out).read()
def sub(old, new, text=None):
    global g
    t = g if text is None else text
    if t.count(old) != 1: sys.exit(f"expected 1 x {old[:50]!r}, found {t.count(old)}")
    r = t.replace(old, new)
    if text is None: g = r
    return r
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/,/blobs/,/fetch-q/")
sub('      scope: ""\n      scoped: ""\n', f'      scope: "{node}"\n      scoped: "{SCOPED}"\n')
# The controllers no Pod here needs, left out (WN_DROP: name prefixes), so
# the graph stays inside the runtime's config arena.
import os
DROP = tuple(os.environ.get("WN_DROP", "jb_ cj_ pc_ hp_ sn_ dv_ dss_ ip_ ns_ q_ pdb_ te_ gc_").split())
if DROP:
    head, rest = g.split("modules:\n", 1)
    cmods, cwire = rest.split("\nwiring:\n", 1)
    cblocks = re.split(r"(?m)^(?=  - name: )", cmods)
    dropped = {b.split()[2] for b in cblocks if b.startswith("  - name: ") and b.split()[2].startswith(DROP)}
    cmods = "".join(b for b in cblocks if not (b.startswith("  - name: ") and b.split()[2] in dropped))
    def gone(b):
        f = re.match(r"  - from: (\S+)\n    to: (\S+)", b)
        return bool(f) and (f.group(1).split(".")[0] in dropped or f.group(2).split(".")[0] in dropped)
    cwire = "".join(b for b in re.split(r"(?m)^(?=  - from: )", cwire) if not gone(b))
    g = head + "modules:\n" + cmods.rstrip("\n") + "\n\nwiring:\n" + cwire
def parts(path):
    y = open(path).read()
    mods = y[y.index("modules:\n") + len("modules:\n"):y.index("\nwiring:")]
    wire = y[y.index("wiring:\n") + len("wiring:\n"):]
    blocks = re.split(r"(?m)^(?=  - name: )", mods)
    pairs, frm = [], None
    for line in wire.splitlines():
        m = re.match(r"  - from: (\S+)$", line)
        if m: frm = m.group(1); continue
        m = re.match(r"    to: (\S+)$", line)
        if m and frm: pairs.append((frm, m.group(1))); frm = None
    return [b for b in blocks if b.startswith("  - name: ")], pairs
# The kubelet's graph-pod slice, and its image resolution (pi_*).
KEEP_PREFIX = ("pj_", "pl_", "pr_", "pi_")
KEEP = {"wl", "sb_mspec", "sb_mspecd", "sb_sig", "sb_sigd", "sb_pause", "sb_paused", "sb_ev", "sb_evout"}
def kept(name): return name in KEEP or name.startswith(KEEP_PREFIX)
blocks, pairs = parts(f"{root}/packaging/debian/fluxor-podlifecycle.yaml")
kube_mods = "".join(b for b in blocks if kept(b.split()[2]))
kube_mods = sub('    blobs: "/var/lib/nanocloud.io/image/blobs"\n', f'    blobs: "{blobs}"\n', kube_mods)
edges = [(a, b) for a, b in pairs if kept(a.split(".")[0]) and kept(b.split(".")[0])]
# The pull: http_fetch straight to linux_net, and image.uproc/image_asm.uproc.
iblocks, ipairs = parts(f"{root}/packaging/debian/fluxor-imagepull.yaml")
img_mods = "".join(iblocks)
img_mods = sub('authority: "registry.nanocloud.io:5000"', f'authority: "{registry}"', img_mods)
img_mods = sub('dir: "/var/lib/nanocloud.io/image/blobs"', f'dir: "{blobs}"', img_mods)
if ca:
    # fetch <-> fetch_tls <-> linux_net, in place of fetch <-> linux_net.
    ipairs = [(a, b) for a, b in ipairs if not ({a.split(".")[0], b.split(".")[0]} == {"fetch", "linux_net"})]
    ipairs += [("linux_net.net_out", "fetch_tls.cipher_in"), ("fetch_tls.cipher_out", "linux_net.net_in"),
               ("fetch_tls.clear_out", "fetch.net_in"), ("fetch.net_out", "fetch_tls.clear_in")]
    img_mods += ("  - name: fetch_tls\n    type: tls\n    mode: 0\n    alpn_h1_only: 1\n"
                 "    peer_auth: 2\n    trust: \"${file:" + ca + "}\"\n"
                 "    verify_hostname: \"registry.nanocloud.io\"\n")
edges += ipairs
mods, wiring = g.split("\nwiring:\n", 1)
g = (mods.rstrip("\n") + "\n  # ── The pod plane and its image plane ──\n" + kube_mods.rstrip("\n")
     + "\n" + img_mods.rstrip("\n") + "\n\nwiring:\n" + wiring.rstrip("\n") + "\n"
     + "".join(f"  - from: {a}\n    to: {b}\n" for a, b in edges))
open(out, "w").write(g)
print(f"   spliced: {g.count(chr(10) + '  - name:')} modules")
PY2
}

# store_put <key> <hex-value> / put_text <key> <text>: into the host log the
# store in $STORE imports at boot.
store_put() { # store_put <key> <hex-value>  — into the host log the store imports
  python3 - "$STORE/store.log" "$1" "$2" <<'PY'
import struct, sys, os, binascii
path, key, val = sys.argv[1], sys.argv[2].encode(), binascii.unhexlify(sys.argv[3])
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
put_text() { store_put "$1" "$(printf '%s' "$2" | python3 -c 'import sys,binascii;print(binascii.hexlify(sys.stdin.buffer.read()).decode())')"; }

# wn_seed_identity: the operator's identity (an SA key, its token at
# $D/token.admin, a cluster-admin binding), the default namespace and a ready
# node-1, seeded into $STORE — no node-1 with WN_NO_NODE=1 (a node that
# registers itself).
wn_seed_identity() {
  python3 - "$D" <<'PY'
import base64, json, struct, sys, time, binascii
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.primitives import hashes, serialization
D = sys.argv[1]
key = ec.generate_private_key(ec.SECP256R1())
pub = key.public_key().public_bytes(
    serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
ISSUER, KID = b"https://kubernetes.default.svc", b"sa-1"
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(ISSUER) + struct.pack("<H", 1) + f8(KID) + struct.pack("<H", 1)
        + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
open(f"{D}/keyframe.hex","w").write(
    binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode())
b64u = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=")
HDR = b"eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InNhLTEifQ"
now = int(time.time())
claims = {"iss": ISSUER.decode(), "sub": "system:serviceaccount:default:admin",
          "aud": "nanocloud", "iat": now, "exp": now + 3600}
signing = HDR + b"." + b64u(json.dumps(claims, separators=(",", ":")).encode())
r, s = utils.decode_dss_signature(key.sign(signing, ec.ECDSA(hashes.SHA256())))
open(f"{D}/token.admin","w").write((signing + b"." + b64u(r.to_bytes(32,"big") + s.to_bytes(32,"big"))).decode())
PY
  store_put "/authn-keys/sa" "$(cat "$D/keyframe.hex")"
  . "$ROOT/scripts/support/rbac.sh"
  rbac_clusterrole "$STORE/store.log" admin "*" "*"
  rbac_crb "$STORE/store.log" admins admin ServiceAccount admin default
  put_text "/namespaces/default" '{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"default"}}'
  [ "${WN_NO_NODE:-0}" = 1 ] || put_text "/nodes/node-1" '{"apiVersion":"v1","kind":"Node","metadata":{"name":"node-1"},"status":{"ready":true,"conditions":[{"type":"Ready","status":"True"}]}}'
}

# api <method> <path> [json-body] -> "<status>|<body>", as the operator, to the
# API on $PORT.
api() { # api <method> <path> [json-body] -> "<status>|<body>"
  python3 - "$PORT" "$D/token.admin" "$1" "$2" "${3:-}" <<'PY'
import socket, ssl, sys
port, tokf, method, path, body = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
tok = open(tokf).read().strip()
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
raw = socket.create_connection(("127.0.0.1", port), timeout=10)
s = ctx.wrap_socket(raw, server_hostname="localhost")
b = body.encode()
req = (f"{method} {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {tok}\r\n"
       f"Content-Type: application/json\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n").encode() + b
s.sendall(req)
buf = b""
s.settimeout(10)
try:
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
except (socket.timeout, ssl.SSLError):
    pass
s.close()
head, _, body = buf.partition(b"\r\n\r\n")
status = head.split(b"\r\n", 1)[0].split(b" ", 2)[1].decode() if head else "?"
print(f"{status}|{body.decode(errors='replace')}")
PY
}
