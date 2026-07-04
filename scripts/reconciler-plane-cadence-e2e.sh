#!/usr/bin/env bash
# Reconciler-plane cadence E2E — takes adaptive tick from a
# single-module toy to the realistic controller-manager plane, nanocloud-side,
# no fluxor changes. Four proofs over the 9 agnostic workload reconcilers of
# packaging/debian/fluxor-reconcilers-adaptive.yaml:
#
#   1. ADMISSIBILITY — the whole reconciler group builds on a mechanism-(b)
#      variable-cadence domain, i.e. all 9 modules pass validate_adaptive_tick's
#      positive-attestation gate (every one is timer_class = "agnostic").
#   2. RELAXATION — booted idle on an empty store, the plane's runtime parks far
#      below the 1ms floor rate (voluntary_ctxt_switches over a wall-clock window;
#      each loop park/wake is one switch). ~1000/s fixed collapses to ~20/s.
#   3. CORRECTNESS UNDER ADAPTIVE — with nodes + unbound pods seeded, the same
#      adaptive plane still cold-start reconciles: the scheduler binds every pod
#      to a ready node. Enabling variable cadence does not break reconcile logic.
#   4. FLEET INVARIANT — every nanocloud app module is adaptive-safe
#      (timer_class agnostic/wall_clock/replicated_clock; none tick_counted,
#      guaranteed, or unattested). After probe_runner + pod_lifecycle were
#      converted to wall_clock there is no tick_counted module left in the tree,
#      so the whole control plane AND node plane are admissible on a
#      variable-cadence domain. This guard fails if a future module reintroduces
#      tick-as-time timing, which would silently break under a relaxed tick.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH_ADAPTIVE="$ROOT/packaging/debian/fluxor-reconcilers-adaptive.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
NEED_FMODS="scheduler deployment_reconciler replicaset_reconciler daemonset_reconciler statefulset_reconciler job_reconciler hpa_reconciler garbage_collector namespace_gc"
for m in $NEED_FMODS; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $MODULES_DIR/$m.fmod (fluxor modules build --target bcm2712)"; exit 1; }
done
for f in "$FLUXOR_RUNTIME" "$GRAPH_ADAPTIVE"; do
  [ -e "$f" ] || { echo "FAIL: missing $f"; exit 1; }
done

WARMUP="${NC_CADENCE_WARMUP:-2}"   # seconds: let cold-start settle into idle
WINDOW="${NC_CADENCE_WINDOW:-4}"   # seconds: idle measurement window

vctx() { awk '/^voluntary_ctxt_switches:/{print $2}' "/proc/$1/status" 2>/dev/null; }

store_put() { # store_put <store.log> <key> <value>
  python3 - "$1" "$2" "$3" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

store_last() { # store_last <store.log> <key>
  python3 - "$1" "$2" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want:
        out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. admissibility: build the 9-reconciler plane on a variable-cadence domain =="
BUILD_DIR="$(mktemp -d /tmp/nc-recon-cad-XXXXXX)"
if ! nc_build_workload "$ROOT" "$GRAPH_ADAPTIVE" "$BUILD_DIR/config.bin" "$BUILD_DIR/modules.bin" >"$BUILD_DIR/build.log" 2>&1; then
  echo "FAIL: adaptive reconciler plane did not build (mechanism-(b) gate?):"; tail -20 "$BUILD_DIR/build.log"; rm -rf "$BUILD_DIR"; exit 1
fi
echo "   ok: all 9 agnostic reconcilers admitted on domain main (adaptive_flags=3)"

echo "== 2. relaxation: boot idle (empty store), measure voluntary context switches =="
: > "$BUILD_DIR/store.log"
FLUXOR_STORE_DIR="$BUILD_DIR" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$BUILD_DIR/config.bin" --modules "$BUILD_DIR/modules.bin" >"$BUILD_DIR/run.log" 2>&1 &
PID=$!
sleep "$WARMUP"
if ! kill -0 "$PID" 2>/dev/null; then
  echo "FAIL: adaptive reconciler runtime exited early:"; tail -15 "$BUILD_DIR/run.log"; rm -rf "$BUILD_DIR"; exit 1
fi
V0="$(vctx "$PID")"; sleep "$WINDOW"; V1="$(vctx "$PID")"
kill "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true
rm -rf "$BUILD_DIR"
if [ -z "${V0:-}" ] || [ -z "${V1:-}" ]; then echo "FAIL: no vctx counters"; exit 1; fi
IDLE=$(( V1 - V0 ))
echo "   idle wakeups over ${WINDOW}s: $IDLE"
# A fixed 1ms tick would be ~1000/s (~$(( WINDOW * 1000 )) over ${WINDOW}s). Relaxed
# toward the 50ms ceiling it is ~20/s. Require well under 100/s to prove the plane
# actually relaxed (a wide margin over the ~20/s expected).
MAX_IDLE=$(( WINDOW * 100 ))
if [ "$IDLE" -gt "$MAX_IDLE" ]; then
  echo "FAIL: idle wakeups $IDLE (> $MAX_IDLE over ${WINDOW}s) — plane did not relax toward the ceiling"; exit 1
fi
echo "   ok: relaxed to ~$(( IDLE / WINDOW ))/s (vs ~1000/s at the 1ms floor)"

echo "== 3. correctness under adaptive: seed nodes + unbound pods, expect bindings =="
RD="$(mktemp -d /tmp/nc-recon-cad-run-XXXXXX)"
nc_build_workload "$ROOT" "$GRAPH_ADAPTIVE" "$RD/config.bin" "$RD/modules.bin" >/dev/null 2>&1
store_put "$RD/store.log" "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'
store_put "$RD/store.log" "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":true}}'
store_put "$RD/store.log" "/pods/default/pod-a" '{"metadata":{"name":"pod-a"},"spec":{"containers":[{"image":"x"}]}}'
store_put "$RD/store.log" "/pods/default/pod-b" '{"metadata":{"name":"pod-b"},"spec":{"containers":[{"image":"x"}]}}'
FLUXOR_STORE_DIR="$RD" RUST_LOG=warn timeout 4 "$FLUXOR_RUNTIME" \
  --config "$RD/config.bin" --modules "$RD/modules.bin" >"$RD/run.log" 2>&1 || true
for pod in pod-a pod-b; do
  got="$(store_last "$RD/store.log" "/pods/default/$pod")"
  case "$got" in
    *'"nodeName":"node-1"'*|*'"nodeName":"node-2"'*) echo "   $pod bound: ${got##*nodeName\":\"}" ;;
    *) echo "FAIL: $pod not bound under adaptive cadence: '$got'"; tail -15 "$RD/run.log"; rm -rf "$RD"; exit 1 ;;
  esac
done
rm -rf "$RD"
echo "   ok: scheduler reconciled and bound both pods with adaptive tick enabled"

echo "== 4. fleet invariant: every nanocloud app module is adaptive-safe =="
bad=0
for mf in "$ROOT"/modules/app/*/manifest.toml; do
  tc="$(grep -oE 'timer_class *= *"[a-z_]+"' "$mf" | grep -oE '"[a-z_]+"' | tr -d '"')"
  case "$tc" in
    agnostic|wall_clock|replicated_clock) ;;
    "") echo "   UNATTESTED (blocks mechanism-b): ${mf#$ROOT/}"; bad=1 ;;
    *)  echo "   NOT cadence-safe: ${mf#$ROOT/} timer_class=$tc"; bad=1 ;;
  esac
done
[ "$bad" -eq 0 ] || { echo "FAIL: a module is not adaptive-safe (see above) — it cannot run on a variable-cadence domain"; exit 1; }
echo "   ok: all $(ls -1 "$ROOT"/modules/app/*/manifest.toml | wc -l) app modules are agnostic/wall_clock — no tick-counted timing remains"

echo "== E2E green: adaptive reconciler plane — admitted, relaxed to ~$(( IDLE / WINDOW ))/s idle, reconciles correctly, and the whole module fleet is adaptive-safe =="
