#!/usr/bin/env bash
# Live E2E: an image assembly that fails is not the last word
# (image_asm.uproc over the sandbox seam, packaging/debian/assemble-image.sh).
#
#   1. the node cannot write its rootfs directory (a FILE stands where the
#      directory goes): the assembly fails, the Pod is told — ErrImagePull
#      "the image could not be assembled from its layers (status 1)", then
#      ImagePullBackOff with a growing count — and it is asked again with
#      the kubelet's backoff (10 s, 20 s, …), never left `state=failed`
#   2. the cause is fixed (the file removed): with nothing deleted by hand,
#      the next attempt assembles the image and the Pod runs to Succeeded
#   3. a cached layer is not its digest (bit rot under the cache): the
#      assembly finds it before extracting anything (exit 65), the image's
#      layers are dropped and fetched again (image_heal.uproc), and the Pod
#      runs from the re-fetched, verified layer
#
# Needs passwordless sudo: the runtime runs as root, as the node does.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
PORT=5396

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
sudo -n true 2>/dev/null || {
  [ "${NANOCLOUD_REQUIRE_E2E:-}" = 1 ] && { echo "FAIL: passwordless sudo unavailable"; exit 1; }
  echo "SKIP: passwordless sudo unavailable (the runtime runs as root)"; exit 0; }
for m in http_fetch store_source store_effect decision workload_effect; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgasm-XXXXXX)"
chmod 755 "$D"
IP_SUDO=1
fail() { echo "FAIL: $1"; sudo -n grep -a 'ERROR' "$D/run.log" 2>/dev/null | tail -5 || true; exit 1; }
. "$ROOT/scripts/support/image-plane.sh"
cleanup() {
  ip_cleanup
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else sudo -n rm -rf "$D" 2>/dev/null || rm -rf "$D"; fi
}
trap cleanup EXIT

echo "== 1. two images sharing a layer: a static binary, and its tag =="
mkdir -p "$D/reg" "$D/blobs" "$D/l1/bin" "$D/l2/etc" "$D/l3/etc"
cat >"$D/hello.c" <<'EOF'
#include <unistd.h>
int main(void) { write(1, "hello-from-assembly\n", 20); return 0; }
EOF
cc -static -o "$D/l1/bin/hello" "$D/hello.c"
echo v1 >"$D/l2/etc/tag"
echo v2 >"$D/l3/etc/tag"
tar -czf "$D/l1.tgz" -C "$D/l1" bin
tar -czf "$D/l2.tgz" -C "$D/l2" etc
tar -czf "$D/l3.tgz" -C "$D/l3" etc
M1="$(ip_image app v1 arm64 "$D/l1.tgz" "$D/l2.tgz")"
M2="$(ip_image app v2 arm64 "$D/l1.tgz" "$D/l3.tgz")"
L1="$(sha256sum "$D/l1.tgz" | cut -d' ' -f1)"
echo "   app:v1 -> ${M1:0:12}…, app:v2 -> ${M2:0:12}… (layer ${L1:0:12}… shared)"
ip_registry "$PORT" "$D/reg.log"
cp "$ROOT/packaging/debian/assemble-image.sh" "$D/assemble-image.sh"
chmod 755 "$D/assemble-image.sh"

ip_graph "$D/graph.yaml" "localhost:$PORT" "$D/blobs" '"kubernetes.io/arch":"arm64","kubernetes.io/os":"linux"' sandbox
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null

rootfs() { ip_get "/image-rootfs/$1"; }
field() { tr ';' '\n' <<<"$1" | sed -n "s/^$2=//p"; }
event() { ip_get "/events/default/$1.$2"; }
msg() { python3 -c 'import json,sys; e=json.loads(sys.argv[1]); print("%s|%s|%s" % (e["reason"], e.get("count", ""), e["message"]))' "$1"; }
done_ok() { case "$(ip_get "/pod-lifecycle-status/$1")" in *Succeeded*) return 0 ;; esac; return 1; }

echo "== 2. the rootfs directory cannot be written: the assembly fails, the Pod is told, it is retried =="
echo "not a directory" >"$D/rootfs"
ip_put "/config/image" "script=$D/assemble-image.sh;rootfs=$D/rootfs;blobs=$D/blobs"
ip_put "/pod-specs/u-asm" "image=app:v1;pull=IfNotPresent;pod=default/asm;iso=1;desired=running;restart=never"
ip_boot
ip_wait 60 eval '[ "$(field "$(rootfs "$M1")" fails)" = 1 ]' || fail "no failed assembly recorded: '$(rootfs "$M1")'"
t1=$SECONDS
R="$(rootfs "$M1")"
[ "$(field "$R" state)" = failed ] || [ "$(field "$R" state)" = retry ] || [ "$(field "$R" state)" = assembling ] \
  || fail "rootfs after a failure: '$R'"
echo "   /image-rootfs/${M1:0:12}… = $R"
ip_wait 30 eval '[ -n "$(event u-asm ErrImagePull)" ]' || fail "no ErrImagePull event: '$(ip_get "/image-pull-failed/$M1")'"
E="$(msg "$(event u-asm ErrImagePull)")"
[ "$E" = 'ErrImagePull|1|Failed to pull image "app:v1": the image could not be assembled from its layers (status 1)' ] \
  || fail "ErrImagePull event: '$E'"
echo "   $E"
ip_wait 40 eval '[ "$(field "$(rootfs "$M1")" fails)" = 2 ]' || fail "the assembly was not asked again: '$(rootfs "$M1")'"
gap=$((SECONDS - t1))
[ "$gap" -ge 7 ] || fail "asked again after ${gap} s: no backoff"
ip_wait 30 eval '[ -n "$(event u-asm ImagePullBackOff)" ]' || fail "no ImagePullBackOff event"
E="$(msg "$(event u-asm ImagePullBackOff)")"
case "$E" in 'ImagePullBackOff|2|Back-off pulling image "app:v1": the image could not be assembled from its layers (status 1)') ;;
  *) fail "ImagePullBackOff event: '$E'" ;; esac
echo "   asked again ${gap} s later: $E"
[ -z "$(ip_get /pod-resolved/u-asm)" ] || fail "the Pod resolved to an image that is not assembled"

echo "== 3. the cause is fixed: the next attempt assembles, the Pod runs — nothing deleted by hand =="
sudo -n rm -f "$D/rootfs"
sudo -n mkdir -p "$D/rootfs"
t0=$SECONDS
ip_wait 90 done_ok u-asm || fail "the Pod did not run once the rootfs could be written: '$(rootfs "$M1")' '$(ip_get /pod-lifecycle-status/u-asm)'"
[ "$(rootfs "$M1")" = "path=$D/rootfs/$M1;state=ready" ] || fail "rootfs record: '$(rootfs "$M1")'"
[ -z "$(ip_get "/image-pull-failed/$M1")" ] || fail "the failure outlived the assembly: '$(ip_get "/image-pull-failed/$M1")'"
case "$(ip_get /sandbox-logs/u-asm)" in *hello-from-assembly*) ;; *) fail "Pod logs: '$(ip_get /sandbox-logs/u-asm)'" ;; esac
echo "   assembled and Succeeded $((SECONDS - t0)) s after the fix; /image-rootfs/${M1:0:12}… = state=ready, the failure record gone"
ip_stop

echo "== 4. a cached layer is not its digest: found before extracting, fetched again, the Pod runs =="
F="$D/blobs/$L1"
sudo -n test -f "$F" || fail "layer ${L1:0:12}… is not cached"
sudo -n python3 -c 'import sys; p=sys.argv[1]; b=bytearray(open(p,"rb").read()); b[len(b)//2]^=0xff; open(p,"wb").write(b)' "$F"
asked="$(grep -c "GET /v2/app/blobs/sha256:$L1" "$D/reg.log" || true)"
ip_put "/pod-specs/u-heal" "image=app:v2;pull=IfNotPresent;pod=default/heal;iso=1;desired=running;restart=never"
ip_boot
ip_wait 90 done_ok u-heal || fail "the Pod did not run from the healed cache: '$(rootfs "$M2")' '$(ip_get /pod-lifecycle-status/u-heal)'"
[ "$(grep -c "GET /v2/app/blobs/sha256:$L1" "$D/reg.log" || true)" -gt "$asked" ] || fail "the corrupt layer was not fetched again"
[ "$(sudo -n sha256sum "$F" | cut -d' ' -f1)" = "$L1" ] || fail "the cache does not hold the layer as its digest"
[ "$(rootfs "$M2")" = "path=$D/rootfs/$M2;state=ready" ] || fail "rootfs record: '$(rootfs "$M2")'"
E="$(event u-heal ErrImagePull)"
[ -n "$E" ] && echo "   the Pod was told: $(msg "$E")"
case "$(ip_get /sandbox-logs/u-heal)" in *hello-from-assembly*) ;; *) fail "Pod logs: '$(ip_get /sandbox-logs/u-heal)'" ;; esac
echo "   exit 65 -> layers dropped, ${L1:0:12}… fetched again and verified, assembled, Succeeded"
ip_stop

echo "== E2E green: a failed assembly backs off and says so, recovers by itself once its cause is gone; a corrupt cache is fetched again =="
