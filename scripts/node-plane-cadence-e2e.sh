#!/usr/bin/env bash
# Node-plane cadence E2E — proves the payoff of converting
# pod_lifecycle + probe_runner from timer_class="tick_counted" to "wall_clock":
# the kubelet-side liveness plane is now admissible on a variable-cadence domain
# and relaxes when idle, just like the reconciler plane.
#
# Two proofs (both independent of the 0x1B EXEC/READ seam, so this does not
# depend on the sandbox_runner workload path):
#   1. ADMISSIBILITY — fluxor-node-adaptive.yaml (the pod-lifecycle chain + probe_runner on
#      a mechanism-(b) domain) builds. Before the wall_clock conversion the gate
#      rejected these tick_counted modules outright; now they pass.
#   2. RELAXATION — booted idle on an empty store, the plane parks far below the
#      1ms floor rate (voluntary_ctxt_switches over a wall-clock window).
#
# Correctness of the converted timing (grace/backoff/kill on dev_millis) is
# covered by the existing pod-lifecycle-e2e; readiness-probe behaviour is covered
# by probe-e2e (currently gated on the 0x1B exec seam, unrelated to this change).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-node-adaptive.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/pod_lifecycle.fmod" "$MODULES_DIR/probe_runner.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

WARMUP="${NC_CADENCE_WARMUP:-2}"
WINDOW="${NC_CADENCE_WINDOW:-4}"

vctx() { awk '/^voluntary_ctxt_switches:/{print $2}' "/proc/$1/status" 2>/dev/null; }

echo "== 1. admissibility: build pod_lifecycle + probe_runner on a variable-cadence domain =="
D="$(mktemp -d /tmp/nc-node-cad-XXXXXX)"
if ! nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin" >"$D/build.log" 2>&1; then
  echo "FAIL: the node/liveness plane did not build on an adaptive domain — is every module (store_source/store_effect/decision/probe_runner) still attesting a timer_class?"; tail -20 "$D/build.log"; rm -rf "$D"; exit 1
fi
echo "   ok: the whole node plane admitted on domain main (adaptive_flags=3) — the"
echo "       pod-lifecycle CHAIN and probe_runner, every engine attesting its timer_class"

echo "== 2. relaxation: boot idle (empty store), measure voluntary context switches =="
: > "$D/store.log"
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
PID=$!
sleep "$WARMUP"
if ! kill -0 "$PID" 2>/dev/null; then
  echo "FAIL: node-plane runtime exited early:"; tail -15 "$D/run.log"; rm -rf "$D"; exit 1
fi
V0="$(vctx "$PID")"; sleep "$WINDOW"; V1="$(vctx "$PID")"
kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true
rm -rf "$D"
if [ -z "${V0:-}" ] || [ -z "${V1:-}" ]; then echo "FAIL: no vctx counters"; exit 1; fi
IDLE=$(( V1 - V0 ))
echo "   idle wakeups over ${WINDOW}s: $IDLE"
MAX_IDLE=$(( WINDOW * 100 ))
if [ "$IDLE" -gt "$MAX_IDLE" ]; then
  echo "FAIL: idle wakeups $IDLE (> $MAX_IDLE over ${WINDOW}s) — node plane did not relax"; exit 1
fi
echo "== E2E green: node/liveness plane admitted on a variable-cadence domain and relaxed to ~$(( IDLE / WINDOW ))/s idle (vs ~1000/s at the 1ms floor) =="
