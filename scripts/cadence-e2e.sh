#!/usr/bin/env bash
# Cadence E2E — proves fluxor's upward-only adaptive-tick
# relaxation on the nanocloud control plane, nanocloud-side, no fluxor changes.
#
# It boots the SAME scheduler fmod twice, differing only in the domain config:
#   fixed    = packaging/debian/fluxor-scheduler.yaml  (tick_us=1000, no adaptive)
#   adaptive = packaging/debian/fluxor-cadence.yaml    (adaptive_flags=3,
#                                                        floor 1ms / ceiling 50ms)
#
# With an EMPTY store the scheduler cold-starts, finds nothing to bind, and goes
# idle. We then count the runtime's VOLUNTARY CONTEXT SWITCHES over a fixed
# wall-clock idle window: the linux loop parks (futex/nanosleep) once per tick
# deadline, and each park→wake is exactly one voluntary switch — so the count is
# the effective tick rate. The fixed graph parks ~1000x/s; the adaptive graph
# relaxes its idle deadline toward the 50ms ceiling and parks far less often.
#
# Assertion: the adaptive graph parks at least 4x less than the fixed graph
# (expected ~50x at the ceiling; 4x is a wide flake margin). That is the proof
# that mechanism-(b) cadence engages, is bounded by the floor, and that an idle
# control plane genuinely stops waking the core every millisecond.
#
# The counter (/proc/<pid>/status voluntary_ctxt_switches) is an exact integer
# and needs no special kernel config — a more robust signal than CPU-time
# sampling at the 10ms /proc/stat granularity.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
FIXED_GRAPH="$ROOT/packaging/debian/fluxor-scheduler.yaml"
ADAPTIVE_GRAPH="$ROOT/packaging/debian/fluxor-cadence.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$FIXED_GRAPH" "$ADAPTIVE_GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

WARMUP="${NC_CADENCE_WARMUP:-2}"   # seconds: let cold-start settle into idle
WINDOW="${NC_CADENCE_WINDOW:-4}"   # seconds: idle measurement window

vctx() { awk '/^voluntary_ctxt_switches:/{print $2}' "/proc/$1/status" 2>/dev/null; }

# measure_idle <graph.yaml> — print the voluntary-ctxt-switch delta over WINDOW
# seconds of idle, for a fresh runtime booted on <graph.yaml> with an empty store.
measure_idle() {
  local graph="$1"
  local D; D="$(mktemp -d /tmp/nc-cadence-XXXXXX)"
  if ! nc_build_workload "$ROOT" "$graph" "$D/config.bin" "$D/modules.bin" >"$D/build.log" 2>&1; then
    echo "FAIL: fluxor build failed for $graph" >&2; tail -20 "$D/build.log" >&2; rm -rf "$D"; return 1
  fi
  : > "$D/store.log"   # empty store => scheduler idles after cold-start
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  local pid=$!
  sleep "$WARMUP"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "FAIL: runtime exited early for $graph" >&2; tail -10 "$D/run.log" >&2; rm -rf "$D"; return 1
  fi
  local v0 v1
  v0="$(vctx "$pid")"
  sleep "$WINDOW"
  v1="$(vctx "$pid")"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -rf "$D"
  if [ -z "${v0:-}" ] || [ -z "${v1:-}" ]; then
    echo "FAIL: could not read voluntary_ctxt_switches for $graph" >&2; return 1
  fi
  echo "$(( v1 - v0 ))"
}

echo "== 1. fixed-cadence baseline (fluxor-scheduler.yaml, 1ms tick) =="
FIXED="$(measure_idle "$FIXED_GRAPH")" || exit 1
echo "   fixed idle wakeups over ${WINDOW}s: $FIXED"

echo "== 2. adaptive cadence (fluxor-cadence.yaml, floor 1ms / ceiling 50ms) =="
ADAPT="$(measure_idle "$ADAPTIVE_GRAPH")" || exit 1
echo "   adaptive idle wakeups over ${WINDOW}s: $ADAPT"

echo "== 3. assert relaxation =="
# Sanity: the fixed 1ms tick must actually wake ~1000/s. Require >= 250/s
# (a generous lower bound absorbing scheduling slack) or the baseline is wrong.
MIN_FIXED=$(( WINDOW * 250 ))
if [ "$FIXED" -lt "$MIN_FIXED" ]; then
  echo "FAIL: fixed baseline only $FIXED wakeups (< $MIN_FIXED over ${WINDOW}s) — not ticking at the 1ms floor?"; exit 1
fi
# The proof: adaptive parks at least 4x less often than fixed.
if [ $(( ADAPT * 4 )) -ge "$FIXED" ]; then
  echo "FAIL: adaptive ($ADAPT) not materially below fixed ($FIXED) — idle relaxation did not engage"; exit 1
fi
RATIO=$(( FIXED / (ADAPT + 1) ))
echo "== E2E green: idle cadence relaxed ~${RATIO}x under adaptive tick (${ADAPT} vs ${FIXED} wakeups/${WINDOW}s) =="
