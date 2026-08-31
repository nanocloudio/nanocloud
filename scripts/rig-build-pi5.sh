#!/usr/bin/env bash
# Build a nanocloud graph into a Pi 5 boot image for the rig.
#
# nanocloud has no kernel of its own: `fluxor build` packs a graph on top of a
# firmware binary, and that firmware is fluxor's. Rather than duplicate the
# kernel build here, this stages the one fluxor produces — and BUILDS it if it
# is missing or older than fluxor's sources, so a rig run cannot silently boot
# a stale kernel while testing new modules.
set -euo pipefail

GRAPH="${1:?usage: rig-build-pi5.sh <graph.yaml>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLUXOR_SRC="${FLUXOR_SRC:-$ROOT/../fluxor}"

[ -d "$FLUXOR_SRC" ] || { echo "rig-build-pi5: no fluxor checkout at $FLUXOR_SRC" >&2; exit 2; }

echo "==> firmware (fluxor)"
( cd "$FLUXOR_SRC" && make firmware TARGET=pi5 >/dev/null )
mkdir -p "$ROOT/target/pi5"
cp "$FLUXOR_SRC/target/pi5/firmware.bin" "$ROOT/target/pi5/firmware.bin"

echo "==> nanocloud modules (bcm2712)"
( cd "$ROOT" && fluxor modules build --target bcm2712 >/dev/null )

echo "==> pack $GRAPH"
( cd "$ROOT" && fluxor build "$GRAPH" >/dev/null )

IMG="$ROOT/target/pi5/images/$(basename "$GRAPH" .yaml).img"
[ -f "$IMG" ] || { echo "rig-build-pi5: no image at $IMG" >&2; exit 1; }
cp "$IMG" "$ROOT/target/pi5/packed.img"
echo "==> staged $(basename "$IMG") -> target/pi5/packed.img"
