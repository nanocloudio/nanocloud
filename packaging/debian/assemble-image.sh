#!/bin/sh
# assemble-image.sh <rootfs-dir> <blob-dir> <layer-hex64>...
#
# The rootfs-assembly EFFECT (container backend), run as a host workload projected by the image_assembler fmod through
# sandbox_runner. Extracts the digest-verified layer blobs (tar.gz, manifest
# order) into the rootfs, applies OCI whiteouts, and populates the /dev nodes
# a container entrypoint expects (best-effort — mknod needs root; a rootless
# assembly still yields a usable rootfs for payloads that skip /dev).
#
# Idempotent: re-running rebuilds the rootfs from scratch (a half-assembled
# dir from a crashed run is discarded).
set -eu

R="$1"; B="$2"; shift 2
[ $# -ge 1 ] || { echo "no layers" >&2; exit 2; }

rm -rf "$R"
mkdir -p "$R"

# 1. Layers in manifest order.
for d in "$@"; do
  [ -f "$B/$d" ] || { echo "missing blob $d" >&2; exit 3; }
  tar -xzf "$B/$d" -C "$R"
done

# 2. OCI whiteouts: `.wh.<name>` deletes <name>; `.wh..wh..opq` makes the
# directory opaque (lower entries hidden — with a single flattened extract the
# marker itself is all that must go).
find "$R" -name '.wh.*' | while IFS= read -r w; do
  base="$(basename "$w")"
  dir="$(dirname "$w")"
  if [ "$base" != ".wh..wh..opq" ]; then
    rm -rf "$dir/${base#.wh.}"
  fi
  rm -f "$w"
done

# 3. Runtime skeleton + /dev nodes (fluxor's workload backend does not
# populate /dev). Best-effort: without root the mknods fail silently and the
# rootfs still serves payloads that don't touch /dev.
mkdir -p "$R/dev" "$R/proc" "$R/sys" "$R/tmp" "$R/run"
chmod 1777 "$R/tmp"
mknod -m 666 "$R/dev/null"    c 1 3 2>/dev/null || true
mknod -m 666 "$R/dev/zero"    c 1 5 2>/dev/null || true
mknod -m 666 "$R/dev/full"    c 1 7 2>/dev/null || true
mknod -m 666 "$R/dev/random"  c 1 8 2>/dev/null || true
mknod -m 666 "$R/dev/urandom" c 1 9 2>/dev/null || true
mknod -m 666 "$R/dev/tty"     c 5 0 2>/dev/null || true
ln -sf /proc/self/fd "$R/dev/fd" 2>/dev/null || true
mkdir -p "$R/dev/shm" && chmod 1777 "$R/dev/shm"

echo "assembled $R from $# layer(s)"
