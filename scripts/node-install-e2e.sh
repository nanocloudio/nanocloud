#!/usr/bin/env bash
# Live E2E: the INSTALLED shape of a node — a fresh install's single node —
# rendered exactly as the deb renders it, registering itself and running a
# Pod of an image pulled from a registry.
#
#   1. the deb's /etc/nanocloud.io/fluxor, staged from packaging/debian/
#      build.sh's own install lines (templates + the replica, kubelet and
#      image fragments by their installed names), and the cluster.env
#      postinst writes on a fresh install (extracted from postinst) — which
#      says NANOCLOUD_PODS=1; the service's environment (nanocloud.service's
#      Environment= and /etc/default/nanocloud) must not set
#      FLUXOR_STORE_DIR, which would bring up a host store beside the
#      graph's lattice one and replace it silently
#   2. a worker's render of the same tree (NANOCLOUD_ROLE=node): the image
#      pull is in it, and every record it writes — its requests, digests,
#      manifests, blob markers, fetch queue — is under a prefix the node's
#      store scopes to the node; the registry's CA is named by path, no
#      certificate or key is in the graph
#   3. the plane's render (nanocloud-graph.sh on the installed directory, as
#      the unit's ExecStartPre runs it): the kubelet, the pull behind a `tls`
#      client that authenticates the registry as registry.nanocloud.io by
#      the CA from the node's config, and the node's lease
#   4. a registry stub over TLS (a cert for registry.nanocloud.io under a
#      test CA) serving a workload image (fluxor build --emit=workload)
#   5. the node runs: it registers itself in /nodes/ and is Ready; a Pod
#      (no nodeName — the scheduler places it) of `nanocloud/probe:1` is
#      pulled through TLS, resolved to its digest, and runs the module it
#      brought; a Pod of another registry's image gets ErrImagePull
#
# What the test moves, and only that: the paths a test cannot write
# (/etc, /var/lib), the listening ports, and where the registry is dialled
# (127.0.0.1:<port>, its name still registry.nanocloud.io).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REG_PORT=5388
PORT=$((24100 + ($$ % 400)))

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/workload-node.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-install-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs" "$D/etc/fluxor" "$D/lib"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null | grep -av MON_ || true; exit 1; }
DEB="$ROOT/packaging/debian"

echo "== 1. the deb's tree, its fresh cluster.env and its service environment =="
# Every graph build.sh installs under /etc/nanocloud.io/fluxor/, by the name
# it installs it as.
python3 - "$DEB/build.sh" "$DEB" "$D/etc/fluxor" <<'PY'
import re, shutil, sys, os
build, deb, out = sys.argv[1:]
n = 0
for src, dst in re.findall(r'install -Dm644 "\$\{SCRIPT_DIR\}/([^"]+)"\s+"\$\{STAGING_ROOT\}/etc/nanocloud\.io/fluxor/([^"]+)"',
                           open(build).read()):
    shutil.copy(os.path.join(deb, src), os.path.join(out, dst)); n += 1
for need in ("controlplane.yaml", "worker.yaml", "replica.yaml", "kubelet.yaml", "image.yaml"):
    if not os.path.exists(os.path.join(out, need)):
        sys.exit(f"build.sh does not install {need}")
print(f"   {n} graphs staged as installed: " + " ".join(sorted(os.listdir(out))))
PY
cp "$DEB/nanocloud-graph.sh" "$D/lib/nanocloud-graph.sh"
# The cluster.env postinst writes on a fresh install, for this host.
HOST=node-a
sed -n '/cat >"\$ENV" <<EOF/,/^EOF$/p' "$DEB/postinst" | sed '1d;$d' | sed "s/\$HOST/$HOST/g" >"$D/cluster.env"
grep -qx 'NANOCLOUD_PODS=1' "$D/cluster.env" || fail "a fresh install's cluster.env does not run Pods: $(cat "$D/cluster.env")"
grep -qx 'NANOCLOUD_ROLE=plane' "$D/cluster.env" || fail "a fresh install is not a control plane"
echo "   cluster.env: $(grep -v '^#' "$D/cluster.env" | tr '\n' ' ')"
# The unit's environment: its Environment= lines and /etc/default/nanocloud.
SVC_ENV="$( { sed -n 's/^Environment=//p' "$DEB/nanocloud.service"; grep -E '^[A-Z_]+=' "$DEB/nanocloud.default"; } )"
if printf '%s\n' "$SVC_ENV" | grep -q '^FLUXOR_STORE_DIR='; then
  fail "the service's environment sets FLUXOR_STORE_DIR: a host store would replace the graph's"
fi
echo "   service environment: $(printf '%s\n' "$SVC_ENV" | cut -d= -f1 | tr '\n' ' ')(no FLUXOR_STORE_DIR)"

# The node's own PKI (as postinst provisions it) and the registry's CA.
nc_node_pki "$STORE"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 \
  -keyout "$D/regca.key" -out "$D/regca.pem" -subj "/CN=nc-test-registry-ca" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=keyCertSign,cRLSign" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$D/reg.key" -out "$D/reg.csr" -subj "/CN=registry.nanocloud.io" 2>/dev/null
printf 'subjectAltName=DNS:registry.nanocloud.io\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n' >"$D/reg.ext"
openssl x509 -req -in "$D/reg.csr" -CA "$D/regca.pem" -CAkey "$D/regca.key" -CAcreateserial \
  -days 1 -extfile "$D/reg.ext" -out "$D/reg.pem" 2>/dev/null
openssl x509 -in "$D/regca.pem" -outform DER -out "$D/ca.der"

render() { # render <role> <out>: nanocloud-graph.sh on the installed tree, as the unit runs it
  local base=$((23000 + ($$ % 500) * 2))
  env -i PATH="$PATH" HOME="$HOME" $(printf '%s\n' "$SVC_ENV" | grep -v '^FLUXOR_' | tr '\n' ' ') \
    NANOCLOUD_CLUSTER_ENV="$D/cluster.env" NANOCLOUD_ROLE="$1" \
    NANOCLOUD_PKI="$STORE/pki" NANOCLOUD_FLUXOR_ABI="$(sh "$ROOT/scripts/fluxor-abi.sh" "$ROOT")" \
    NANOCLOUD_PEER_PORT="$base" NANOCLOUD_PEERS="0=127.0.0.1:$base" \
    NANOCLOUD_REGISTRY_CA="$D/ca.der" NANOCLOUD_REGISTRY_ADDR="127.0.0.1:$REG_PORT" \
    NANOCLOUD_IMAGE_BLOBS="$D/blobs" \
    sh "$D/lib/nanocloud-graph.sh" "$D/etc/fluxor" "$2"
}

echo "== 2. a worker's render: the image pull, every record of it scoped to the node, no secret in the graph =="
render node "$D/worker.yaml" 2>"$D/render-node.err" || { cat "$D/render-node.err"; fail "the worker did not render"; }
python3 - "$D/worker.yaml" "$ROOT" <<'PY'
import re, sys, glob
g = open(sys.argv[1]).read(); root = sys.argv[2]
scoped = re.search(r'^      scoped: "([^"]*)"$', g, re.M).group(1).split(",")
assert re.search(r'^      scope: "node-a"$', g, re.M), "the worker's store is not scoped to it"
for m in ("fetch", "fetch_tls", "im_req", "im_q", "im_ans", "im_tag", "im_as1", "pi_digest"):
    assert re.search(rf"^  - name: {m}$", g, re.M), f"{m} is not in the worker's graph"
# Every key the image chain writes or reads, from its sources.
keys = set()
for f in glob.glob(f"{root}/modules/app/_chronicle/image*.uproc") + [f"{root}/modules/app/_chronicle/pod_image.uproc"]:
    keys |= set(re.findall(r'key: "(/[a-z-]+/)', open(f).read()))
keys |= {"/image-blobs/", "/image-fetch-q/", "/image-digests/", "/image-requests/", "/image-manifests/",
         "/image-config/", "/image-index/", "/image-layer-ok/", "/image-rootfs/", "/sandboxes/"}
# A Pod's events are the plane's to serve: written unscoped on purpose.
keys.discard("/events/")
# A Pod and its ServiceAccount are the cluster's: READ for its pull
# credentials (image_auth.uproc), never written.
keys -= {"/pods/", "/serviceaccounts/"}
unscoped = [k for k in sorted(keys) if not any(k.startswith(p) for p in scoped)]
assert not unscoped, f"image records the worker's store does not scope: {unscoped}"
assert "BEGIN" not in g and "PRIVATE" not in g, "a certificate or key is IN the graph"
ca = re.search(r'^  - name: fetch_tls\n(?:    .*\n)*?    trust: "\$\{file:([^}]*)\}"', g, re.M)
assert ca, "fetch_tls does not name the registry CA by path"
assert re.search(r'^    verify_hostname: "registry.nanocloud.io"$', g, re.M)
print("   scoped: every image record (" + ", ".join(sorted(keys)) + ") under " + ",".join(scoped))
print(f"   fetch_tls trusts ${{file:{ca.group(1)}}} (by path) as registry.nanocloud.io")
PY
nc_build_workload "$ROOT" "$D/worker.yaml" "$D/worker-config.bin" "$D/worker-modules.bin" \
  || fail "the worker's graph, with its pull, did not build"
echo "   the worker's graph builds ($(grep -c '^  - name: ' "$D/worker.yaml") modules)"

echo "== 3. the plane's render: a cluster of one — the kubelet, the pull behind tls, its own lease, its store scoped =="
render plane "$D/plane.rendered" 2>"$D/render-plane.err" || { cat "$D/render-plane.err"; fail "the plane did not render"; }
for m in nl_self nl_renew pi_source fetch fetch_tls im_req ps_spec sc_patch; do
  grep -qx "  - name: $m" "$D/plane.rendered" || fail "$m is not in the single node's graph"
done
grep -qx '      scope: "node-a"' "$D/plane.rendered" || fail "the single node's store is not scoped to it"
# Only what a test cannot have: the API's certificate paths and its port;
# lattice's redis edge as the test's reader.
RPORT=$((23000 + ($$ % 500) * 2 + 1))
python3 - "$D/plane.rendered" "$D/graph.yaml" "$STORE" "$PORT" "$RPORT" <<'PY'
import sys
src, out, store, port, rport = sys.argv[1:]
g = open(src).read()
for a, b in (("/etc/nanocloud.io/certs/server.der", f"{store}/server.der"),
             ("/etc/nanocloud.io/certs/server.key.der", f"{store}/server.key.der"),
             ("    port: 7443\n", f"    port: {port}\n")):
    assert g.count(a) == 1, a
    g = g.replace(a, b)
mods, wiring = g.split("\nwiring:\n", 1)
mods += ("\n  - name: redis_edge_anchor\n    params:\n      listen_port: " + rport + "\n")
wiring = wiring.rstrip("\n") + (
    "\n  - from: linux_net.net_out\n    to: redis_edge_anchor.net_in\n    buffer_bytes: 65536\n"
    "  - from: redis_edge_anchor.net_out\n    to: linux_net.net_in\n"
    "  - from: redis_edge_anchor.kv_out\n    to: kv_request_router.redis_in\n"
    "  - from: kv_request_router.redis_out\n    to: redis_edge_anchor.kv_in\n")
open(out, "w").write(mods + "\nwiring:\n" + wiring)
PY
echo "$RPORT" >"$STORE/read.port"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
echo "   $(grep -c '^  - name: ' "$D/graph.yaml") modules"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" || fail "the single node's graph did not build"

echo "== 4. a registry over TLS (registry.nanocloud.io under a test CA) serving a workload image =="
cat >"$D/probe.yaml" <<'YAML'
target: linux
tick_us: 1000
modules: []
wiring: []
workloads:
  - modules:
      - name: prog
        type: program_probe
YAML
wn_publish probe "$D/probe.yaml"
python3 - "$D/reg" "$REG_PORT" "$D/reg.pem" "$D/reg.key" >"$D/reg.log" 2>&1 <<'PY' &
import http.server, os, socketserver, ssl, sys
d, port, cert, key = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
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
    daemon_threads = True
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)
ctx.minimum_version = ssl.TLSVersion.TLSv1_3
ctx.set_alpn_protocols(["http/1.1"])
with S(("127.0.0.1", port), H) as srv:
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    srv.serve_forever()
PY
REG_PID=$!
for _ in $(seq 40); do
  curl -s --cacert "$D/regca.pem" --resolve "registry.nanocloud.io:$REG_PORT:127.0.0.1" \
    "https://registry.nanocloud.io:$REG_PORT/v2/nanocloud/probe/manifests/1" -o /dev/null && break
  sleep 0.1
done
kill -0 "$REG_PID" 2>/dev/null || fail "the registry stub died (:$REG_PORT taken?)"
curl -sf --cacert "$D/regca.pem" --resolve "registry.nanocloud.io:$REG_PORT:127.0.0.1" \
  "https://registry.nanocloud.io:$REG_PORT/v2/nanocloud/probe/manifests/1" | cmp -s - "$D/reg/probe/manifest.json" \
  || fail "the listener on :$REG_PORT is not this test's registry"
MDIG="$(sha256sum "$D/reg/probe/manifest.json" | cut -d' ' -f1)"
echo "   https://registry.nanocloud.io:$REG_PORT (dialled at 127.0.0.1) serves nanocloud/probe:1 = sha256:${MDIG:0:12}…"

echo "== 5. the node runs: it registers itself, is Ready, and runs the Pod scheduled onto it =="
WN_NO_NODE=1 wn_seed_identity
env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" FLUXOR_VAULT_DIR="$STORE/vault" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 90 || fail "the store never answered"
r=""
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac
ready=""
for _ in $(seq 60); do
  r="$(api GET /api/v1/nodes/node-a)"
  ready="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(str(json.load(sys.stdin).get("status",{}).get("ready","")).lower())
except Exception: print("")' 2>/dev/null)"
  [ "$ready" = "true" ] && break
  sleep 1
done
[ "$ready" = "true" ] || fail "node-a never registered itself Ready: ${r:0:300}"
echo "   /api/v1/nodes/node-a: Ready (its own lease)"

POD='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"nanocloud/probe:1"}]}}'
for _ in $(seq 90); do
  r="$(api POST /api/v1/namespaces/default/pods "$POD")"
  case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
done
case "$r" in 20[01]\|*) echo "   POST pods/probe -> ${r%%|*}" ;; *) fail "POST pod: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
dump() {
  local k
  for k in "/~node/node-a/pod-specs/$UID_" "/~node/node-a/image-requests/nanocloud~probe+1" \
           "/~node/node-a/image-digests/nanocloud~probe+1" "/~node/node-a/image-rootfs/$MDIG" \
           "/~node/node-a/pod-resolved/$UID_" "/~node/node-a/metal-sandboxes/$UID_" \
           "/~node/node-a/sandbox-status/$UID_" "/podstatus/$UID_"; do
    printf '   %-60s %s\n' "$k" "$(nc_store_get "$STORE" "$k" 2>/dev/null | cut -c1-110)"
  done
  tail -5 "$D/reg.log" | sed 's/^/   registry: /'
  grep -a "http_fetch\]\|\[tls\]" "$D/run.log" | tail -5 | cut -c1-160
}
phase=""
node=""
for _ in $(seq 120); do
  r="$(api GET /api/v1/namespaces/default/pods/probe)"
  read -r phase node < <(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try:
    o = json.load(sys.stdin); print(o.get("status",{}).get("phase","-"), o.get("spec",{}).get("nodeName","-"))
except Exception: print("- -")' 2>/dev/null)
  [ "$phase" = "Running" ] && grep -aq "\[prog\]" "$D/run.log" && break
  sleep 1
done
echo "   probe: nodeName=$node phase=$phase"
[ "$node" = "node-a" ] || { dump; fail "the scheduler did not place the Pod on node-a"; }
[ "$phase" = "Running" ] || { dump; fail "the Pod never reached Running"; }
grep -aq "\[prog\]" "$D/run.log" || { dump; fail "Running, but program_probe never logged"; }
[ "$(nc_store_get "$STORE" "/~node/node-a/image-digests/nanocloud~probe+1")" = "digest=$MDIG" ] \
  || { dump; fail "the tag did not resolve to the manifest's digest"; }
case "$(nc_store_get "$STORE" "/~node/node-a/pod-resolved/$UID_")" in
  *"digest=$MDIG"*) echo "   resolved to sha256:${MDIG:0:12}…, pulled over TLS, its module ran: $(grep -ao '\[prog\][^"]*' "$D/run.log" | head -1 | cut -c1-60)" ;;
  *) dump; fail "the Pod does not run the digest it resolved" ;;
esac
[ -n "$(nc_store_keys "$STORE" "/~node/node-a/image-blobs/" | head -1)" ] || { dump; fail "no blob marker under the node's scope"; }
[ -z "$(nc_store_keys "$STORE" "/image-" | head -1)" ] || fail "image records outside the node's scope: $(nc_store_keys "$STORE" /image- | head -3)"
echo "   every image record under /~node/node-a/ (blob markers: $(nc_store_keys "$STORE" "/~node/node-a/image-blobs/" | wc -l))"

echo "== 6. another registry's image: refused, visibly =="
POD2='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"elsewhere","namespace":"default"},"spec":{"containers":[{"name":"c","image":"docker.example.com/library/nginx:1.27"}]}}'
r="$(api POST /api/v1/namespaces/default/pods "$POD2")"
case "$r" in 20[01]\|*) ;; *) fail "POST pod elsewhere: ${r:0:200}" ;; esac
UID2="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
ev=""
for _ in $(seq 60); do
  ev="$(api GET /api/v1/namespaces/default/events)"
  case "$ev" in *ErrImagePull*) break ;; esac
  sleep 1
done
printf '%s' "${ev#*|}" | python3 -c '
import json, sys
uid = sys.argv[1]
items = [e for e in json.load(sys.stdin).get("items", []) if e.get("reason") == "ErrImagePull"]
assert any(e["involvedObject"].get("name") == "elsewhere" and "docker.example.com" in e["message"] for e in items), items
print("   kubectl get events: Warning ErrImagePull pod/elsewhere: " + items[0]["message"])
' "$UID2" || fail "no ErrImagePull event for the foreign image: ${ev:0:300}"
grep -q "nginx" "$D/reg.log" && fail "the foreign image was asked of this registry"
r="$(api GET /api/v1/namespaces/default/pods/elsewhere)"
case "$r" in *'"phase":"Running"'*) fail "a refused Pod is Running" ;; esac

echo "== E2E green: the installed single node — rendered as the deb does, its own node, Ready, pulling over TLS from its configured registry, running a Pod by digest, refusing another registry =="
