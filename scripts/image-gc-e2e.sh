#!/usr/bin/env bash
# Live E2E: image garbage collection (image_gc.uproc, image_gcd.uproc,
# image_gcl.uproc; fs_effect for the rootfs; the blob dropped through
# http_fetch).
#
#   1. two images pulled and run (app:v1 and app:v2, sharing a layer), and a
#      third whose pull cannot finish (a layer the registry does not have)
#   2. GC's limits allow every image: nothing is removed
#   3. the Pod using app:v1 is deleted, and the limits are lowered (one image
#      held, gc_high=1, gc_low=1): GC runs. app:v1 goes — its manifest,
#      config, rootfs record AND directory, layer marks, the requests that
#      resolved to it, the blob only it held (file and marker) — while
#      app:v2, which a Pod uses, stays whole, the shared layer with it; and
#      the image being pulled is never touched
#   4. a Pod asking for app:v1 again pulls it again and runs
#
# Needs passwordless sudo: the runtime runs as root, as the node does.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
PORT=5400

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
sudo -n true 2>/dev/null || {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = 1 ] && { echo "FAIL: passwordless sudo unavailable"; exit 1; }
  echo "SKIP: passwordless sudo unavailable (the runtime runs as root)"; exit 0; }
for m in http_fetch store_source store_effect decision workload_effect fs_effect; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imggc-XXXXXX)"
chmod 755 "$D"
IP_SUDO=1
export IP_ROOTFS="$D/rootfs"
fail() { echo "FAIL: $1"; sudo -n grep -a 'ERROR' "$D/run.log" 2>/dev/null | tail -5 || true; exit 1; }
. "$ROOT/scripts/support/image-plane.sh"
cleanup() {
  ip_cleanup
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT

echo "== 1. two images sharing a layer, and one whose pull cannot finish =="
mkdir -p "$D/reg" "$D/blobs" "$D/rootfs" "$D/l1/bin" "$D/l2/etc" "$D/l3/etc" "$D/l4/etc"
cat >"$D/hello.c" <<'EOF'
#include <unistd.h>
int main(void) { write(1, "hello-from-gc\n", 14); return 0; }
EOF
cc -static -o "$D/l1/bin/hello" "$D/hello.c"
echo v1 >"$D/l2/etc/tag"; echo v2 >"$D/l3/etc/tag"; echo v3 >"$D/l4/etc/tag"
# A tree deeper than a path the fs contract takes (its entries' paths pass
# 255 bytes), and a link to the host's root, which must not be followed.
DEEP="$D/l2/etc/deep"
for i in $(seq 1 14); do DEEP="$DEEP/branch-$i-xxxxxxxxxxxxxx"; done
mkdir -p "$DEEP"
echo deep >"$DEEP/leaf"
ln -s / "$D/l2/etc/root-link"
for i in 1 2 3 4; do tar -czf "$D/l$i.tgz" -C "$D/l$i" .; done
V1="$(ip_image app v1 arm64 "$D/l1.tgz" "$D/l2.tgz")"
V2="$(ip_image app v2 arm64 "$D/l1.tgz" "$D/l3.tgz")"
V3="$(ip_image app v3 arm64 "$D/l4.tgz")"
L1="$(sha256sum "$D/l1.tgz" | cut -d' ' -f1)"
L2="$(sha256sum "$D/l2.tgz" | cut -d' ' -f1)"
L4="$(sha256sum "$D/l4.tgz" | cut -d' ' -f1)"
rm -f "$D/reg/$L4"   # the registry does not have app:v3's layer
echo "   app:v1 ${V1:0:12}… (layers ${L1:0:12}…, ${L2:0:12}…), app:v2 ${V2:0:12}… (shares ${L1:0:12}…), app:v3 ${V3:0:12}… (its layer missing)"
ip_registry "$PORT" "$D/reg.log"
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"
ip_graph "$D/graph.yaml" "localhost:$PORT" "$D/blobs" '"kubernetes.io/arch":"arm64","kubernetes.io/os":"linux"' sandbox
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null

done_ok() { case "$(ip_get "/pod-lifecycle-status/$1")" in *Succeeded*) return 0 ;; esac; return 1; }
pod() { ip_put "/pod-specs/$1" "image=$2;pull=IfNotPresent;pod=default/$1;iso=1;desired=running;restart=never"; }
# have <m>: every record of the image, and its rootfs directory.
have() {
  [ -n "$(ip_get "/image-manifests/$1")" ] && [ -n "$(ip_get "/image-config/$1")" ] \
    && [ -n "$(ip_get "/image-rootfs/$1")" ] && sudo -n test -d "$D/rootfs/$1"
}
gone() {
  [ -z "$(ip_get "/image-manifests/$1")" ] && [ -z "$(ip_get "/image-config/$1")" ] \
    && [ -z "$(ip_get "/image-rootfs/$1")" ] && ! sudo -n test -e "$D/rootfs/$1" \
    && [ -z "$(ip_keys "/image-layer-ok/$1-")" ] && [ -z "$(ip_keys /image-gc/)" ]
}

echo "== 2. both run; GC's limits allow every image: nothing is removed =="
ip_put "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"
pod u-a app:v1
pod u-b app:v2
pod u-c app:v3
ip_boot
ip_wait 90 eval 'done_ok u-a && done_ok u-b' || fail "the Pods did not run: '$(ip_get /pod-lifecycle-status/u-a)' '$(ip_get /pod-lifecycle-status/u-b)'"
sleep 3
ip_stop
have "$V1" && have "$V2" || fail "an image is missing before GC"
[ -n "$(ip_get "/image-manifests/$V3")" ] || fail "app:v3's pull did not start"
[ -z "$(ip_get "/image-rootfs/$V3")" ] || fail "app:v3 is ready: '$(ip_get "/image-rootfs/$V3")'"
[ -z "$(ip_keys /image-gc/)" ] && [ -z "$(ip_get /image-gc-run)" ] || fail "GC ran under its limits"
echo "   app:v1 and app:v2 held, used by u-a and u-b; app:v3 still pulling"

echo "== 3. u-a deleted, the limits lowered to one image: app:v1 goes, app:v2 and app:v3 stay =="
ip_del /pod-specs/u-a
ip_put "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs;gc_high=1;gc_low=1;gc_min=1"
asked="$(grep -c "GET /v2/app/blobs/sha256:$L2" "$D/reg.log" || true)"
ip_boot
ip_wait 90 gone "$V1" || fail "app:v1 was not removed: manifest='$(ip_get "/image-manifests/$V1")' rootfs='$(ip_get "/image-rootfs/$V1")' gc='$(ip_keys /image-gc/ | tr '\n' ' ')' $(ip_get "/image-gc/$V1")"
ip_wait 30 eval '[ -z "$(ip_get "/image-blobs/'"$L2"'")" ] && ! sudo -n test -e "$D/blobs/'"$L2"'"' \
  || fail "app:v1's own layer was not dropped: marker='$(ip_get "/image-blobs/$L2")'"
ip_wait 15 eval '[ -z "$(ip_get /image-gc-run)" ]' || fail "GC did not stop at gc_low: '$(ip_get /image-gc-run)'"
sleep 2
ip_stop
[ -z "$(ip_get /image-digests/app+v1)" ] && [ -z "$(ip_get /image-requests/app+v1)" ] \
  || fail "the request for app:v1 outlived it: '$(ip_get /image-digests/app+v1)'"
[ -z "$(ip_keys /image-fetch-q/d-)" ] || fail "a drop row was left: $(ip_keys /image-fetch-q/d-)"
have "$V2" || fail "app:v2, in use, was touched"
[ "$(ip_get "/image-rootfs/$V2")" = "path=$D/rootfs/$V2;state=ready" ] || fail "app:v2's rootfs: '$(ip_get "/image-rootfs/$V2")'"
[ -n "$(ip_get "/image-blobs/$L1")" ] && sudo -n test -f "$D/blobs/$L1" || fail "the layer app:v2 shares was dropped"
[ -n "$(ip_get "/image-manifests/$V3")" ] || fail "app:v3, being pulled, was removed"
sudo -n test -e /etc/hostname || fail "the host is touched (the rootfs walk followed a link)"
echo "   app:v1: manifest, config, rootfs record and directory, layer marks, request, its own blob — gone"
echo "   app:v2 (u-b's) and the layer it shares — kept; app:v3 (being pulled) — kept; GC stopped at one image"

echo "== 4. a Pod asking for app:v1 again pulls it again and runs =="
pod u-d app:v1
ip_put "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"
ip_boot
ip_wait 90 done_ok u-d || fail "app:v1 did not run again: '$(ip_get /pod-lifecycle-status/u-d)' '$(ip_get "/image-rootfs/$V1")'"
ip_stop
[ "$(grep -c "GET /v2/app/blobs/sha256:$L2" "$D/reg.log" || true)" -gt "$asked" ] || fail "app:v1's layer was not fetched again"
case "$(ip_get /sandbox-logs/u-d)" in *hello-from-gc*) ;; *) fail "u-d's logs: '$(ip_get /sandbox-logs/u-d)'" ;; esac
echo "   pulled again, assembled, Succeeded"

echo "== E2E green: GC removes the least recently used unused image — records, rootfs, its own blobs — past its limits, never one in use or being pulled =="
