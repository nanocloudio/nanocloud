#!/usr/bin/env bash
# Live E2E for PAUSE/RESUME through the node pipeline: the paused-aware status
# loop in sandbox_runner and the paused-aware pod_lifecycle state machine.
#
# No CLI/API verb can issue PAUSE yet, so this script plays the orchestrator
# the way the other E2Es drive effects — through the store seam:
# /sandbox-pause/<id> = "pause" | "resume" (consumed by sandbox_runner, gated
# on the backend CAPS PAUSE bit).
#
# PRIVILEGE GATE: the Linux backend's PAUSE is the cgroup2 freezer on the
# per-sandbox cgroup, and moving the container INTO that cgroup needs write
# access on the cgroup2 common ancestor — unavailable to an unprivileged dev
# session (the fluxor harness root-gates its freezer tests the same way;
# unprivileged, the backend returns per-workload ENOSYS). So:
#   - as root:        the FULL path is asserted (state=paused, phase=Paused,
#                     no restart, resume back to running).
#   - unprivileged:   DEGRADED assertions — the pause intent must be benign
#                     (workload stays running, phase stays Running, no restart,
#                     no failed/killed latch) and the paused-state assertions
#                     are SKIPped with a message.
#
# The pod spec carries `tasks=64`: the backend only creates the per-sandbox
# cgroup (the freezer's home) when the resource envelope is non-empty.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-podlifecycle.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/sandbox_runner.fmod" "$MODULES_DIR/probe_runner.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-pause-e2e-XXXXXX)"
cleanup() {
  # A frozen container survives the runtime: thaw any leftover sandbox cgroup
  # and reap the distinctive sleeper (best-effort, root path only).
  if [ "$(id -u)" = 0 ]; then
    find /sys/fs/cgroup -maxdepth 6 -type d -name 'fluxor.oci.*' 2>/dev/null | while read -r cg; do
      echo 0 > "$cg/cgroup.freeze" 2>/dev/null || true
    done
    pkill -f 'sleep 8787' 2>/dev/null || true
  fi
  rm -rf "$D"
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D"/run*.log 2>/dev/null || true; exit 1; }

# Append one put record to the store's durable log (same wire the store writes:
# [rev:u64][op:u8=1][key_len:u16][val_len:u32][key][val]). Single-writer here —
# no flock, because nothing else has the file open (the runtime is not running).
store_put() { # store_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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

store_last() { # store_last <key>  — last-writer value of a key in the log
  python3 - "$D/store.log" "$1" <<'PY'
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

store_history() { # store_history <key> — every value ever put, one per line
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want and op == 1:
        print(v.decode(errors="replace"))
    p += 15 + kl + vl
PY
}

run_graph() { # run_graph <seconds> <logname>
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout "$1" "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/$2" 2>&1 || true
}

FULL=0
if [ "$(id -u)" = 0 ] && [ -r /sys/fs/cgroup/cgroup.controllers ]; then
  FULL=1
fi

echo "== 1. build the config + module table from the graph (3 modules) =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. a long-running pod with a resource envelope (tasks= → cgroup) =="
mkdir -p "$D/pz"
store_put "/pod-specs/pod-pz" "cmd=/bin/sleep 8787;desired=running;tasks=64"

echo "== 3. run: pod comes up Running =="
run_graph 4 run1.log
case "$(store_last /pod-lifecycle-status/pod-pz)" in
  phase=Running*) echo "   pod-pz: Running";;
  *) fail "pod-pz should be Running, got '$(store_last /pod-lifecycle-status/pod-pz)'";;
esac

echo "== 4. orchestrate PAUSE via the store seam =="
store_put "/sandbox-pause/pod-pz" "pause"
run_graph 4 run2.log

ST="$(store_last /sandbox-status/pod-pz)"
PH="$(store_last /pod-lifecycle-status/pod-pz)"
if [ "$FULL" = 1 ]; then
  echo "== 5. FULL (root): paused projected, phase=Paused, no restart =="
  [ "$ST" = "state=paused;code=0" ] || fail "pod-pz should project state=paused, got '$ST'"
  [ "$PH" = "phase=Paused" ] || fail "pod-pz should be phase=Paused, got '$PH'"
else
  echo "== 5. DEGRADED (unprivileged: cgroup containment unavailable — the"
  echo "      backend returns per-workload ENOSYS; paused-state assertions"
  echo "      SKIPped; asserting the pause intent is benign) =="
  case "$ST" in
    state=running*) ;;
    *) fail "unprivileged pause must leave the workload running, got '$ST'";;
  esac
  case "$PH" in
    phase=Running*) ;;
    *) fail "unprivileged pause must leave the pod Running, got '$PH'";;
  esac
fi
# Paused (or an unimplementable pause) must NEVER look like a failure: no
# terminal latch, no restart trigger, no backoff count.
if store_history /sandbox-status/pod-pz | grep -Eq 'state=(failed|killed|exited)'; then
  fail "pause must not latch a terminal state (history shows one)"
fi
if store_history /pod-lifecycle-status/pod-pz | grep -Eq 'Restarting|restarts='; then
  fail "pause must not trigger the restart machinery"
fi
echo "   no terminal latch, no restart — OK"

echo "== 6. orchestrate RESUME; pod returns to Running =="
store_put "/sandbox-pause/pod-pz" "resume"
run_graph 4 run3.log
ST="$(store_last /sandbox-status/pod-pz)"
PH="$(store_last /pod-lifecycle-status/pod-pz)"
case "$ST" in
  state=running*) ;;
  *) fail "pod-pz should be running after resume, got '$ST'";;
esac
case "$PH" in
  phase=Running*) ;;
  *) fail "pod-pz should be phase=Running after resume, got '$PH'";;
esac
if store_history /pod-lifecycle-status/pod-pz | grep -Eq 'Restarting|restarts='; then
  fail "resume must not trigger the restart machinery"
fi

if [ "$FULL" = 1 ]; then
  echo "== E2E green (FULL): pause → state=paused/phase=Paused (live, no restart) → resume → Running =="
else
  echo "== E2E green (DEGRADED): pause intent benign unprivileged; run as root for the freezer proof =="
fi
