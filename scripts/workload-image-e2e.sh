#!/usr/bin/env bash
# Live E2E: a Pod of a WORKLOAD IMAGE — `runtimeClassName: fluxor` naming an
# OCI reference — on a node in the bare-metal shape (scripts/metal-plane-e2e.sh):
# the node pulls the image, holds the Pod Pending until every layer is in its
# blob cache, then creates the workload from the modules it brought
# (SOURCE_FMOD_PACK, each pinned by its layer digest). The module is NOT in
# the node's image: only the pack can have run it.
#
#   fluxor build --emit=workload  -> graph.flxa + program_probe.fmod
#   a registry stub serves them as `fluxor publish workload` lays them out
#   (artifactType application/vnd.nanocloud.fluxor.workload-image.v1)
#   POST Pod image=nanocloud/probe:1 -> pull -> /image-digests/nanocloud~probe+1
#     (the manifest's digest <m>) -> /image-manifests/<m> kind=workload
#     -> /image-rootfs/<m> path=pack:<m> -> /pod-resolved/ -> /metal-sandboxes/
#     pack=… -> CREATE (pack) -> Running, and the module's own log line
#   DELETE the Pod -> its sandbox destroyed
#   a second image that SERVES: redis_edge_anchor wired to the node's network
#   through a lane of its own (its `linux_net` wires), answering PING on its port
#   an image at the layer bound: its graph and 64 modules (Fluxor's
#   MAX_ADD_MODULES) — 65 layers, every hop the list rides sized for it
#   (http_fetch's answer, the store's values, the assembly record,
#   workload_effect's pack) — pulled, staged and run
#   one layer past it: refused with its count, said on the Pod
#   (ErrImagePull "… has 66 layers, at most 65 …"), no layer fetched; and
#   80, whose digest list is past what http_fetch answers (413): the same
#
# PASS = Running, `[prog]` in the node's log, the module absent from the
# node's image, the delete settled, the second Pod answering on its port, the
# 65-layer Pod Running from its pack, and the 66-layer one Pending with the
# reason on its status.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
REG_PORT=5394
SERVE_PORT=16380

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/workload-node.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-wimg-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }


echo "== 1. the workload images: fluxor build --emit=workload, served as a registry would =="
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
# A Pod that serves: the anchor answers PING itself, over a lane of its own.
cat >"$D/front.yaml" <<YAML
target: linux
tick_us: 1000
modules: []
wiring: []
workloads:
  - modules:
      - name: anchor
        type: redis_edge_anchor
        listen_port: $SERVE_PORT
    wiring:
      - from: linux_net.net_out
        to: anchor.net_in
      - from: anchor.net_out
        to: linux_net.net_in
YAML
wn_publish front "$D/front.yaml"
# At the layer bound: a graph of one program_probe, and 63 more modules in
# its pack (the pack may carry more than its graph runs; each pinned by its
# layer digest) — 65 layers, the most a workload image has. One more: 66.
cat >"$D/wide.yaml" <<'YAML'
target: linux
tick_us: 1000
modules: []
wiring: []
workloads:
  - modules:
      - name: wide
        type: program_probe
YAML
wn_publish wide "$D/wide.yaml"
wn_widen() { # wn_widen <from-repo> <to-repo> <layers>: <from>'s image grown to <layers> layers
  python3 - "$D/reg/$1" "$D/reg/$2" "$3" "$ROOT/target/fluxor/aarch64/modules" <<'PY'
import functools, glob, hashlib, json, os, shutil, struct, sys
src, dst, want, mods = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
if os.path.realpath(src) != os.path.realpath(dst):
    shutil.copytree(src, dst)
m = json.load(open(os.path.join(src, "manifest.json")))
fnv = lambda s: functools.reduce(lambda h, c: ((h ^ c) * 0x01000193) & 0xffffffff, s, 0x811c9dc5)
have = {l["annotations"].get("io.fluxor.module.name") for l in m["layers"]}
def manifest_of(b):
    # The fmod's manifest: past its header, code, data, exports and schema.
    code, data = struct.unpack_from("<II", b, 8)
    exports = struct.unpack_from("<H", b, 24)[0]
    schema, size = struct.unpack_from("<HH", b, 62)
    at = 80 + code + data + exports * 8 + schema
    return b[at:at + size]
# What this runtime admits: a module built for it (the linux runtime's bit
# in its manifest) against its ABI surface (the attestation, the manifest's
# last 32 bytes) — the graph's own module's.
own = manifest_of(open(os.path.join(src, m["layers"][1]["digest"][7:]), "rb").read())
LINUX = 0x10
# The smallest such modules of distinct names, none the graph's own.
for p in sorted(glob.glob(mods + "/*.fmod"), key=os.path.getsize):
    if len(m["layers"]) >= want:
        break
    b = open(p, "rb").read()
    name = b[28:60].split(b"\0")[0].decode(errors="replace")
    mf = manifest_of(b)
    if not name or name in have or len(mf) < 49 or mf[-32:] != own[-32:] \
            or not struct.unpack_from("<H", mf, 10)[0] & LINUX:
        continue
    have.add(name)
    h = hashlib.sha256(b).hexdigest()
    open(os.path.join(dst, h), "wb").write(b)
    m["layers"].append({"mediaType": "application/vnd.nanocloud.fluxor.module.v1", "digest": "sha256:" + h,
                        "size": len(b), "annotations": {"io.fluxor.module.name": name,
                                                        "io.fluxor.module.name-hash": str(fnv(name.encode()))}})
assert len(m["layers"]) == want, len(m["layers"])
open(os.path.join(dst, "manifest.json"), "w").write(json.dumps(m))
print("   %s: %d layers, the manifest %d bytes" % (os.path.basename(dst), want, len(json.dumps(m))))
PY
}
wn_widen wide wide 65 || fail "the 65-layer image was not laid out"
wn_widen wide over 66 || fail "the 66-layer image was not laid out"
wn_widen wide far 80 || fail "the 80-layer image was not laid out"
wn_registry "$REG_PORT"
MDIG="$(sha256sum "$D/reg/probe/manifest.json" | cut -d' ' -f1)"

echo "== 2. seed the operator's identity: SA key, cluster-admin binding, a ready node =="
wn_seed_identity

echo "== 3. render the node: the plane + the kubelet's graph-pod slice + its image plane, on one scoped store =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
wn_splice "$D/graph.yaml" node-1 "$D/blobs" "localhost:$REG_PORT" || fail "the splice did not apply"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
python3 - "$D/modules.bin" <<'PY' || fail "the node's image holds program_probe: the test would prove nothing"
import struct, sys
b = open(sys.argv[1], "rb").read()
n = b[5]
fnv = lambda s: __import__("functools").reduce(lambda h, c: ((h ^ c) * 0x01000193) & 0xffffffff, s, 0x811c9dc5)
hashes = {struct.unpack_from("<I", b, 16 + 16 * i)[0] for i in range(n)}
sys.exit(1 if fnv(b"program_probe") in hashes else 0)
PY
echo "   the node's image does not hold program_probe"

echo "== 4. run it; wait for the API over TLS =="
env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac

echo "== 5. a Pod of the workload image is created through the API =="
POD='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"probe","image":"nanocloud/probe:1","resources":{"limits":{"ephemeral-storage":"256Mi"}}}]}}'
for _ in $(seq 90); do
  r="$(api POST /api/v1/namespaces/default/pods "$POD")"
  case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
done
case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST pod: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
echo "   uid $UID_"
dump() {
  local k
  for k in "/~node/node-1/pod-specs/$UID_" "/~node/node-1/image-requests/nanocloud~probe+1" \
           "/~node/node-1/image-digests/nanocloud~probe+1" "/~node/node-1/image-manifests/$MDIG" "/~node/node-1/image-rootfs/$MDIG" \
           "/~node/node-1/pod-resolved/$UID_" "/~node/node-1/metal-sandboxes/$UID_" \
           "/~node/node-1/sandbox-status/$UID_" "/~node/node-1/pod-lifecycle-status/$UID_" "/podstatus/$UID_"; do
    printf '   %-56s %s\n' "$k" "$(nc_store_get "$STORE" "$k" 2>/dev/null | cut -c1-110)"
  done
  tail -5 "$D/reg.log" | sed 's/^/   registry: /'
  nc_store_keys "$STORE" 2>/dev/null | grep -aE "/image-" | head -12 | sed 's/^/   key: /'
  grep -a "http_fetch\]" "$D/run.log" | tail -3 | cut -c1-140
}

echo "== 6. the node pulls it and runs the modules it brought; the phase reaches the object =="
phase=""
for _ in $(seq 120); do
  r="$(api GET /api/v1/namespaces/default/pods/probe)"
  phase="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("status",{}).get("phase",""))
except Exception: print("")' 2>/dev/null)"
  [ "$phase" = "Running" ] && grep -aq "\[prog\]" "$D/run.log" && break
  sleep 1
done
echo "   phase=${phase:-}"
if [ "$phase" != "Running" ]; then dump; fail "the Pod never reached Running (phase='${phase:-}')"; fi
grep -aq "\[prog\]" "$D/run.log" || { dump; fail "Running, but program_probe never logged: the pack did not run"; }
echo "   $(grep -ao '\[prog\][^"]*' "$D/run.log" | head -1 | cut -c1-80)"
[ "$(nc_store_get "$STORE" "/~node/node-1/image-digests/nanocloud~probe+1")" = "digest=$MDIG" ] \
  || { dump; fail "nanocloud/probe:1 did not resolve to its manifest's digest"; }
man="$(nc_store_get "$STORE" "/~node/node-1/image-manifests/$MDIG")"
case "$man" in *";kind=workload;"*) echo "   manifest: kind=workload" ;; *) fail "manifest not marked a workload image: $man" ;; esac
case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_")" in
  *"pack=$MDIG"*";eph=268435456"*) echo "   metal sandbox names its pack (the digest) and its ephemeral-storage limit" ;;
  *) dump; fail "the metal sandbox does not name the pack and its limit" ;;
esac
grep -aq "\[workload\] graph workload [0-9.]*: storage_bytes=268435456" "$D/run.log" \
  || fail "the pack was not created with its ephemeral-storage limit as its owner's storage cap"
echo "   created with storage_bytes=268435456: the Pod's own storage root capped at its ephemeral-storage"

echo "== 7. the Pod is deleted; its workload goes =="
r="$(api DELETE /api/v1/namespaces/default/pods/probe)"
case "$r" in 20[0-9]\|*) echo "   DELETE -> ${r%%|*}" ;; *) fail "DELETE pod: ${r:0:200}" ;; esac
st=""
for _ in $(seq 60); do
  st="$(nc_store_get "$STORE" "/~node/node-1/sandbox-status/$UID_" 2>/dev/null || true)"
  case "$st" in ""|*destroyed*) break ;; esac
  sleep 1
done
case "$st" in ""|*destroyed*) echo "   sandbox: ${st:-gone}" ;; *) dump; fail "the workload was not destroyed: $st" ;; esac

echo "== 8. a Pod that serves: its module listens on the node's network through a lane of its own =="
FRONT='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"front","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"front","image":"nanocloud/front:1"}]}}'
r="$(api POST /api/v1/namespaces/default/pods "$FRONT")"
case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST front: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
pong=""
for _ in $(seq 120); do
  pong="$(redis-cli -p "$SERVE_PORT" PING 2>/dev/null || true)"
  [ "$pong" = "PONG" ] && break
  sleep 1
done
[ "$pong" = "PONG" ] || { dump; fail "the serving Pod never answered on :$SERVE_PORT"; }
echo "   PING :$SERVE_PORT -> PONG (a pulled module, listening through its lane)"

echo "== 9. an image at the layer bound: its graph and 64 modules, 65 layers, pulled and run =="
WDIG="$(sha256sum "$D/reg/wide/manifest.json" | cut -d' ' -f1)"
WIDE='{"apiVersion":"v1","kind":"Pod","metadata":{"name":"wide","namespace":"default"},"spec":{"runtimeClassName":"fluxor","containers":[{"name":"wide","image":"nanocloud/wide:1"}]}}'
# Its module is a program_probe too: its own `[prog]` line is the next one.
np="$(grep -ac "\[prog\]" "$D/run.log" || true)"
r="$(api POST /api/v1/namespaces/default/pods "$WIDE")"
case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST wide: ${r:0:200}" ;; esac
UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
MDIG="$WDIG"
phase=""
for _ in $(seq 120); do
  r="$(api GET /api/v1/namespaces/default/pods/wide)"
  phase="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("status",{}).get("phase",""))
except Exception: print("")' 2>/dev/null)"
  [ "$phase" = "Running" ] && [ "$(grep -ac "\[prog\]" "$D/run.log" || true)" -gt "$np" ] && break
  sleep 1
done
echo "   phase=${phase:-}"
if [ "$phase" != "Running" ]; then dump; fail "the 65-layer Pod never reached Running (phase='${phase:-}')"; fi
[ "$(grep -ac "\[prog\]" "$D/run.log" || true)" -gt "$np" ] || { dump; fail "Running, but the 65-layer pack's module never logged"; }
# Every layer marked cached for the manifest — 0..64 — which only a
# manifest recorded whole (all 65 digests) expands into. (The test's store
# reader, lattice's redis edge, does not answer a value this long whole.)
for i in 0 32 64; do
  [ -n "$(nc_store_get "$STORE" "/~node/node-1/image-layer-ok/$WDIG-$i")" ] \
    || { dump; fail "layer $i of the 65 is not marked cached"; }
done
[ -z "$(nc_store_get "$STORE" "/~node/node-1/image-layer-ok/$WDIG-65")" ] || fail "a 66th layer was marked"
nb="$(grep -c "^/v2/nanocloud/wide/blobs/" "$D/reg.log" || true)"
[ "$nb" -ge 65 ] || fail "only $nb blob requests for 65 layers and a config"
echo "   /image-layer-ok/${WDIG:0:12}…-0..64 marked ($nb blob GETs); the Pod runs from its 65-layer pack"

# refused <repo> <layers>: a Pod of an image past the bound is Pending, its
# status and its ErrImagePull event naming the count; nothing recorded, no
# layer asked of the registry.
refused() {
  local repo="$1" n="$2" odig pod want got r
  odig="$(sha256sum "$D/reg/$repo/manifest.json" | cut -d' ' -f1)"
  pod="{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"name\":\"$repo\",\"namespace\":\"default\"},\"spec\":{\"runtimeClassName\":\"fluxor\",\"containers\":[{\"name\":\"$repo\",\"image\":\"nanocloud/$repo:1\"}]}}"
  r="$(api POST /api/v1/namespaces/default/pods "$pod")"
  case "$r" in 20[01]\|*) echo "   POST pods -> ${r%%|*}" ;; *) fail "POST $repo: ${r:0:200}" ;; esac
  UID_="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])')"
  want="has $n layers, at most 65 (its graph and 64 modules)"
  got=""
  for _ in $(seq 90); do
    r="$(api GET "/api/v1/namespaces/default/pods/$repo")"
    got="$(printf '%s' "${r#*|}" | python3 -c 'import json,sys
try:
    st = json.load(sys.stdin).get("status", {})
    w = ((st.get("containerStatuses") or [{}])[0].get("state") or {}).get("waiting") or {}
    print("%s|%s|%s" % (st.get("phase", ""), w.get("reason", ""), w.get("message", "")))
except Exception: print("")' 2>/dev/null)"
    case "$got" in *"$want"*) break ;; esac
    sleep 1
  done
  case "$got" in
    "Pending|ErrImagePull|"*"$want"*|"Pending|ImagePullBackOff|"*"$want"*) echo "   ${got}" ;;
    *) dump; nc_store_get "$STORE" "/~node/node-1/image-pull-failed/nanocloud~$repo+1" | sed 's/^/   pull-failed: /'
       fail "the $n-layer Pod does not say why it waits: '${got}'" ;;
  esac
  r="$(api GET /api/v1/namespaces/default/events)"
  printf '%s' "${r#*|}" | python3 -c 'import json,sys
uid, want = sys.argv[1], sys.argv[2]
ev = [e for e in json.load(sys.stdin).get("items", []) if e["metadata"]["name"] == uid + ".ErrImagePull"]
assert ev and want in ev[0]["message"] and ev[0]["type"] == "Warning", ev
print("   event ErrImagePull: " + ev[0]["message"])' "$UID_" "$want" || fail "no ErrImagePull event naming the count"
  [ -z "$(nc_store_get "$STORE" "/~node/node-1/image-manifests/$odig" 2>/dev/null)" ] \
    || fail "the $n-layer manifest was recorded"
  ! grep -q "^/v2/nanocloud/$repo/blobs/" "$D/reg.log" || fail "a layer of the refused image was fetched"
  echo "   nothing recorded, no blob asked of the registry"
}
echo "== 10. one layer past the bound: refused with its count, on the Pod — no layer fetched =="
refused over 66
echo "== 11. far past it — the digest list past what http_fetch answers (its 413): refused the same =="
refused far 80
grep -aq "\[http_fetch\] an answer past what a reply carries" "$D/run.log" \
  || fail "the 80-layer manifest was not answered 413 by http_fetch"
echo "   http_fetch answered it 413 with its count, not an empty list"

echo "== E2E green: Pods of workload images pulled, pinned, staged as the modules they brought, run, serving, and deleted; a 65-layer image runs, a 66- and an 80-layer one are refused on the Pod with their count =="
