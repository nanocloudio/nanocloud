#!/usr/bin/env bash
# Live E2E for PAUSE/RESUME through the node pipeline: the paused-aware status
# loop in the sandbox runtime and the paused-aware pod_lifecycle state machine.
#
# The plane's pause is the nanocloud-specific Pod annotation
# (`nanocloud.io/paused: "true"`), which pod_resolve writes into the Pod's
# /pod-specs/ record as `paused=true`. pod_containers.plan (pj_ctr -> pj_ctre)
# owns /sandbox-pause/<id>: `pause` while the spec asks, `resume` once it no
# longer does — so this script drives the SPEC, as pod_resolve would, and
# never writes /sandbox-pause/ itself (the plane would answer a bare
# `pause` with `resume`, the spec not asking for one). The sandbox runtime
# consumes /sandbox-pause/, gated on the backend CAPS PAUSE bit.
#
# FREEZER GATE: the Linux backend's PAUSE is the cgroup2 freezer on a child
# of the runtime's own cgroup, and moving the container into it needs write
# access on that cgroup. Whether this host gives that is asked of the host
# (nc_freezer_unavailable does what the backend does with a throwaway
# sleeper), not of `id -u`: root in a user namespace can freeze inside a
# cgroup delegated to its uid (a `systemd-run --user` scope), and cannot in
# one that was not. So:
#   - freezer works:  the FULL path is asserted (state=paused, phase=Paused,
#                     no restart, resume back to running).
#   - it does not:    DEGRADED assertions — the pause intent must be benign
#                     (workload stays running, phase stays Running, no restart,
#                     no failed/killed latch) and the paused-state assertions
#                     are SKIPped, the probe's reason printed.
#
# The pod spec carries `tasks=64`: the backend only creates the per-sandbox
# cgroup (the freezer's home) when the resource envelope is non-empty.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-podlifecycle.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/workload_effect.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-pause-e2e-XXXXXX)"
cleanup() {
  # A frozen container survives the runtime: thaw any sandbox cgroup the
  # backend left under ours and reap the distinctive sleeper.
  local base
  base="${FLUXOR_HOST_CGROUP_ROOT:-/sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)}"
  for cg in "${base%/}"/fluxor.hostproc.*; do
    [ -d "$cg" ] || continue
    echo 0 > "$cg/cgroup.freeze" 2>/dev/null || true
  done
  pkill -f 'sleep 8787' 2>/dev/null || true
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
NO_FREEZE="$(nc_freezer_unavailable)"
if [ -z "$NO_FREEZE" ]; then
  FULL=1
fi

echo "== 1. build the config + module table from the graph (3 modules) =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. a long-running pod with a resource envelope (tasks= → cgroup) =="
mkdir -p "$D/pz"
SPEC="cmd=/bin/sleep 8787;desired=running;tasks=64"
store_put "/pod-specs/pod-pz" "$SPEC"

echo "== 3. run: pod comes up Running =="
run_graph 4 run1.log
case "$(store_last /pod-lifecycle-status/pod-pz)" in
  phase=Running*) echo "   pod-pz: Running";;
  *) fail "pod-pz should be Running, got '$(store_last /pod-lifecycle-status/pod-pz)'";;
esac

echo "== 4. ask for PAUSE as pod_resolve does (the spec's paused=true) =="
store_put "/pod-specs/pod-pz" "$SPEC;paused=true"
run_graph 5 run2.log
[ "$(store_last /sandbox-pause/pod-pz)" = pause ] \
  || fail "pod_containers should ask the sandbox to pause, /sandbox-pause/pod-pz is '$(store_last /sandbox-pause/pod-pz)'"

ST="$(store_last /sandbox-status/pod-pz)"
PH="$(store_last /pod-lifecycle-status/pod-pz)"
if [ "$FULL" = 1 ]; then
  echo "== 5. FULL (the freezer works here): paused projected, phase=Paused, no restart =="
  [[ "$ST" =~ ^state=paused\;code=0\;started=([0-9]+|m[0-9]+@([0-9a-f]{32}|-))$ ]] || fail "pod-pz should project state=paused with its run's start, got '$ST'"
  [ "$PH" = "phase=Paused" ] || fail "pod-pz should be phase=Paused, got '$PH'"
else
  echo "== 5. DEGRADED — SKIP the paused-state assertions: no freezer here ($NO_FREEZE);"
  echo "      the backend returns per-workload ENOSYS; asserting the pause intent is benign =="
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

echo "== 6. stop asking (paused dropped from the spec); pod returns to Running =="
store_put "/pod-specs/pod-pz" "$SPEC"
run_graph 5 run3.log
[ "$(store_last /sandbox-pause/pod-pz)" = resume ] \
  || fail "pod_containers should ask the sandbox to resume, /sandbox-pause/pod-pz is '$(store_last /sandbox-pause/pod-pz)'"
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
  echo "== E2E green (DEGRADED, freezer SKIPPED: $NO_FREEZE): pause intent benign =="
fi
