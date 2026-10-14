#!/bin/sh
# The fluxor ABI this tree builds against, as a node advertises it: the first
# 8 hex digits of the SDK's ABI surface digest (the epoch's usual name). A
# node's `nanocloud.io/fluxor-abi` label; a workload image built here pins
# the same digest (`fluxor build --emit=workload`'s `abi_surface`).
#
#   fluxor-abi.sh [project-root]
set -eu
ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
PIN="$ROOT/target/fluxor/fluxor-abi/sdk/abi_surface_srcpin.rs"
[ -r "$PIN" ] || { echo "fluxor-abi.sh: no $PIN (fluxor sync)" >&2; exit 1; }
sed -n '/ABI_SURFACE_DIGEST/,/\];/p' "$PIN" | grep -o '0x[0-9a-f][0-9a-f]' \
    | head -4 | sed 's/^0x//' | tr -d '\n'
echo
