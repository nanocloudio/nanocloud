#!/usr/bin/env bash
# Live E2E: a multi-arch image pulls the node's own platform
# (packaging/debian/fluxor-imagepull.yaml, image.uproc `fq`).
#
# A tag names an image INDEX with a real amd64 image and a real arm64/v8
# image (an arm/v7 decoy first, so "the first arm-ish entry" would be wrong).
# The node's architecture comes from its labels — the `facts` its render
# stamps on the queue's source (`"kubernetes.io/arch":"…"`, as
# nanocloud-graph.sh states them for its Node):
#
#   1. a node labelled amd64 resolves the tag to the amd64 manifest and
#      pulls ITS layer, never the arm64 one
#   2. a node labelled arm64 resolves it to the arm64/v8 manifest
#   3. an index with no entry for the node's platform (s390x, ppc64le only)
#      is a clear ErrImagePull on the Pod — "no matching manifest for
#      linux/arm64/v8 in the manifest list entries" — and the pull backs off
#      (ImagePullBackOff), never a hang and never another platform's image
#
# PASS = all three.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
PORT=5395

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in http_fetch store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgplat-XXXXXX)"
fail() { echo "FAIL: $1"; grep -a 'ERROR' "$D/run.log" 2>/dev/null | tail -5 || true; exit 1; }
. "$ROOT/scripts/support/image-plane.sh"
cleanup() { ip_cleanup; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT

echo "== 1. a multi-arch image: an index of amd64 and arm64/v8 images (an arm/v7 decoy first) =="
mkdir -p "$D/reg"
ip_layer "$D/amd.tgz" "etc/arch=amd64"
ip_layer "$D/arm.tgz" "etc/arch=arm64"
AMD="$(ip_image app amd64-only amd64 "$D/amd.tgz")"
ARM="$(ip_image app arm64-only arm64 "$D/arm.tgz")"
ip_index app multi "arm/v7=-" "amd64=$AMD" "arm64/v8=$ARM" >/dev/null
ip_index app other "s390x=-" "ppc64le=-" >/dev/null
L_AMD="$(sha256sum "$D/amd.tgz" | cut -d' ' -f1)"
L_ARM="$(sha256sum "$D/arm.tgz" | cut -d' ' -f1)"
echo "   amd64 -> ${AMD:0:12}… (layer ${L_AMD:0:12}…), arm64/v8 -> ${ARM:0:12}… (layer ${L_ARM:0:12}…)"
ip_registry "$PORT" "$D/reg.log"

node() { # node <arch>: a fresh store and blob cache, the graph for that arch
  ip_stop
  rm -rf "$D/store.log" "$D/blobs" "$D/run.log"
  mkdir -p "$D/blobs"
  ip_graph "$D/graph.yaml" "localhost:$PORT" "$D/blobs" \
    "\"kubernetes.io/arch\":\"$1\",\"kubernetes.io/os\":\"linux\",\"kubernetes.io/hostname\":\"node-1\""
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
}
resolved() { [ -n "$(ip_get "/image-layer-ok/$1-0")" ] && [ -n "$(ip_get "/image-config/$1")" ]; }

echo "== 2. a node labelled amd64 pulls the amd64 image =="
node amd64
ip_put "/image-requests/app+multi" "reg=;repo=app;tag=multi;pin="
ip_boot
ip_wait 30 resolved "$AMD" || fail "the amd64 node did not pull the amd64 image: '$(ip_get /image-digests/app+multi)'"
ip_stop
[ "$(ip_get /image-digests/app+multi)" = "digest=$AMD" ] || fail "amd64 resolved to '$(ip_get /image-digests/app+multi)'"
[ "$(ip_get /image-index/app+multi)" = "digest=sha256:$AMD" ] || fail "index: '$(ip_get /image-index/app+multi)'"
[ -f "$D/blobs/$L_AMD" ] || fail "the amd64 layer is not cached"
[ -e "$D/blobs/$L_ARM" ] && fail "the amd64 node fetched the arm64 layer"
grep -q "GET /v2/app/blobs/sha256:$L_ARM" "$D/reg.log" && fail "the arm64 layer was asked for"
echo "   app:multi -> sha256:${AMD:0:12}… (linux/amd64); its layer cached, the arm64 one never asked"

echo "== 3. a node labelled arm64 pulls the arm64/v8 image =="
: >"$D/reg.log"
node arm64
ip_put "/image-requests/app+multi" "reg=;repo=app;tag=multi;pin="
ip_boot
ip_wait 30 resolved "$ARM" || fail "the arm64 node did not pull the arm64 image: '$(ip_get /image-digests/app+multi)'"
ip_stop
[ "$(ip_get /image-digests/app+multi)" = "digest=$ARM" ] || fail "arm64 resolved to '$(ip_get /image-digests/app+multi)'"
[ -f "$D/blobs/$L_ARM" ] || fail "the arm64 layer is not cached"
grep -q "GET /v2/app/blobs/sha256:$L_AMD" "$D/reg.log" && fail "the arm64 node asked for the amd64 layer"
grep -q "manifests/sha256:ffff" "$D/reg.log" && fail "the arm/v7 decoy was asked for"
echo "   app:multi -> sha256:${ARM:0:12}… (linux/arm64/v8), past the arm/v7 decoy"

echo "== 4. an index with nothing for this node: ErrImagePull saying so, then backing off =="
node arm64
ip_put "/pod-specs/u-plat" "image=app:other;pull=IfNotPresent;pod=default/plat;desired=running"
ip_boot
event() { ip_get "/events/default/u-plat.$1"; }
ip_wait 30 eval '[ -n "$(event ErrImagePull)" ]' || fail "no ErrImagePull event: '$(ip_get /image-pull-failed/app+other)'"
E="$(event ErrImagePull)"
python3 -c 'import json,sys; e=json.loads(sys.argv[1]); assert e["reason"]=="ErrImagePull" and e["type"]=="Warning" and e["message"]=="Failed to pull image \"app:other\": no matching manifest for linux/arm64/v8 in the manifest list entries", e["message"]' "$E" \
  || fail "ErrImagePull event: '$E'"
echo "   $(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["message"])' "$E")"
case "$(ip_get /image-fetch-q/m-app+other)" in
  *"fails="*"due="*) ;; *) fail "the pull is not backing off: '$(ip_get /image-fetch-q/m-app+other)'" ;;
esac
ip_wait 40 eval '[ -n "$(event ImagePullBackOff)" ]' || fail "no ImagePullBackOff after the retry"
ip_stop
[ -z "$(ip_get /image-digests/app+other)" ] || fail "an index with no entry for the node resolved: '$(ip_get /image-digests/app+other)'"
[ -z "$(ip_get /pod-resolved/u-plat)" ] || fail "the Pod resolved"
grep -q "manifests/sha256:ffff" "$D/reg.log" && fail "another platform's manifest was asked for"
echo "   ImagePullBackOff: $(python3 -c 'import json,sys; e=json.loads(sys.argv[1]); print(e["message"], "count", e["count"])' "$(event ImagePullBackOff)")"

echo "== E2E green: an index is read for the node's own platform (amd64, arm64/v8 past a decoy); none for it is a clear ErrImagePull that backs off =="
