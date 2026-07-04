#!/usr/bin/env bash
# Live E2E for nanocloud's job_reconciler module (modules/app/job_reconciler) —
# run-to-completion over the fluxor-native control-plane store, consumed through
# the standard storage contracts (`storage.object` 0x14 + `storage.namespace`
# 0x13). the store is
# single-process, owned by the fluxor-linux runtime, and seeded from its durable
# append-log at init.
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# the Job spec and simulated pod completion are replayed from `$D/store.log` at
# boot rather than raced in live. The reconciler cold-starts: SUBSCRIBE
# /jobs.batch/ and /pods/ onto its `changes` input channel, then a LIST pass
# launches `completions` Pods and writes /job-status. When Pods are marked
# Succeeded (the kubelet/sandbox side, simulated here), it tracks completion and
# flips complete=1 — without recreating finished Pods.
#
#   /jobs.batch/<ns>/<name>  = "completions=<N>;image=<img>"
#   /pods/<ns>/<name>-<i>    = "image=<img>;owner=<job>[;phase=Succeeded]"
#   /job-status/<ns>/<name>  = "succeeded=<s>;complete=<0|1>"
#
# Two phases: phase A seeds a Job and checks the pods launch + status incomplete;
# phase B appends Succeeded phases to the pods, re-runs, and checks completion.
#
# Prereqs: `fluxor sync` and `fluxor modules build --target bcm2712` in this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-job.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/job_reconciler.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-job-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

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

run_runtime() { # run_runtime — boot the runtime over the seeded log for one window
  nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 2.5 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true
}

echo "== phase A: Job completions=2 → launch pods; status starts incomplete =="
store_put "/jobs.batch/default/backup" '{"metadata":{"name":"backup"},"spec":{"completions":2,"template":{"spec":{"containers":[{"name":"backup","image":"busybox"}]}}}}'
run_runtime
case "$(store_last /pods/default/backup-0)" in *'"image":"busybox"'*) ;; *) fail "backup-0 not launched";; esac
case "$(store_last /pods/default/backup-1)" in *'"image":"busybox"'*) ;; *) fail "backup-1 not launched";; esac
[ "$(store_last /job-status/default/backup)" = "succeeded=0;complete=0" ] || fail "status should be incomplete"
echo "   launched backup-0, backup-1; status succeeded=0;complete=0"

echo "== phase B: pods Succeed (kubelet's role), re-boot → the Job completes =="
store_put "/pods/default/backup-0" '{"metadata":{"name":"backup-0"},"spec":{"containers":[{"image":"busybox"}]},"status":{"phase":"Succeeded"}}'
store_put "/pods/default/backup-1" '{"metadata":{"name":"backup-1"},"spec":{"containers":[{"image":"busybox"}]},"status":{"phase":"Succeeded"}}'
run_runtime
[ "$(store_last /job-status/default/backup)" = "succeeded=2;complete=1" ] \
  || fail "status should be complete: got '$(store_last /job-status/default/backup)'"
# run-to-completion: succeeded pods are not recreated/reset.
case "$(store_last /pods/default/backup-0)" in *Succeeded*) true ;; *) false ;; esac \
  || fail "backup-0 must keep its Succeeded phase (not recreated)"
echo "   status succeeded=2;complete=1; pods kept their Succeeded phase"

echo "== E2E green: Job run-to-completion + status over the fluxor-native store =="
