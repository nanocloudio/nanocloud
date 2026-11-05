#!/usr/bin/env bash
# workload_effect holds the node's blob cache path in a bounded buffer. A path
# past the bound refuses the module at construction and says why, rather than
# leaving a module with no blob cache whose every pack CREATE fails -22 with no
# log line. A path at the bound is held.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

D="$(mktemp -d /tmp/nc-blobs-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

REFUSAL='workload_effect\] refused: blobs param is'

run_with() { # run_with <path-length>: boot the module with a blobs path that long
  local path
  path="/$(printf 'b%.0s' $(seq 2 "$1"))"
  cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
modules:
  - name: runner
    type: workload_effect
    blobs: "$path"
wiring: []
YAML
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
  FLUXOR_STORE_DIR="$D" RUST_LOG=info timeout 3 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true
}

echo "== 1. a path past the bound is refused, by name, with its length =="
run_with 97
grep -aq "$REFUSAL 97 bytes, past the 96 it holds" "$D/run.log" || fail "no refusal for a 97-byte path"
grep -aq "new failed rc=-22" "$D/run.log" || fail "the module was not refused at construction"
echo "   $(grep -a "$REFUSAL" "$D/run.log" | head -1)"

echo "== 2. a path at the bound is held =="
run_with 96
grep -aq "$REFUSAL" "$D/run.log" && fail "a 96-byte path was refused"
grep -aq "1 of 1 modules loaded" "$D/run.log" || fail "the module did not load"
echo "   96 bytes: no refusal"

echo "== E2E green: the blob cache path is held whole or refused aloud =="
