#!/usr/bin/env bash
# Live E2E for the pod-lifecycle chain -> sandbox_runner -> workload pipeline —
# the kubelet's decision half (Chronicle params) driving the effect half (a
# module) driving the workload capability surface, over the control-plane
# store, verified UNPRIVILEGED.
#
# The store is single-process, owned by the fluxor-linux runtime, and seeded
# from its durable append-log at init: this script (playing the control plane
# after it materialized the bundle) writes a /pod-specs/<uid> into
# `$D/store.log`. Inside the runtime: the lifecycle chain (DECISION) projects
# /sandboxes/<uid>; sandbox_runner (EFFECT) creates+starts the sandbox via the
# workload contract and writes /sandbox-status/<uid>; the chain maps that back
# to /pod-lifecycle-status/<uid>. The decision<->effect handshake happens live
# in the one in-process store during the run. A NULL sandbox (no isolate) keeps
# it unprivileged.
#
# Compact format:
#   /pod-specs/<uid>            = "cmd=<argv>;desired=<running|deleted>[;rootfs=<path>][;iso=1]"
#   /pod-lifecycle-status/<uid> = "phase=<Pending|Running|Succeeded|Failed|Terminating>"
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

D="$(mktemp -d /tmp/nc-pl-e2e-XXXXXX)"
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

echo "== 1. build the config + module table from the graph (3 modules) =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. materialize bundles (echo → exit 0, false → exit 1) + a restart matrix =="
# pod1: succeeds, no restart -> Succeeded (terminal)
store_put "/pod-specs/pod1"    "cmd=/bin/echo hello-pod;desired=running"
# pod-nf: fails, restart=never -> Failed (terminal)
store_put "/pod-specs/pod-nf"  "cmd=/bin/false;desired=running;restart=never"
# pod-of: fails, restart=onfailure -> restarts (never terminal)
store_put "/pod-specs/pod-of"  "cmd=/bin/false;desired=running;restart=onfailure"
# pod-al: succeeds, restart=always -> restarts anyway (never terminal)
store_put "/pod-specs/pod-al"  "cmd=/bin/echo hello-pod;desired=running;restart=always"
# pod-slp: long-running (sleep) — killed in phase 5 via the two-phase kill seam
store_put "/pod-specs/pod-slp" "cmd=/bin/sleep 30;desired=running"

echo "== 3. run the pipeline (pod_lifecycle -> sandbox_runner -> oci) =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 4 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the lifecycle + restart-policy decisions =="
SB="$(store_last /sandbox-status/pod1)"
[ "$SB" = "state=exited;code=0" ] || fail "pod1 sandbox status wrong: got '$SB'"
[ "$(store_last /pod-lifecycle-status/pod1)" = "phase=Succeeded" ] \
  || fail "pod1 (never, exit 0) should be Succeeded"
[ "$(store_last /pod-lifecycle-status/pod-nf)" = "phase=Failed" ] \
  || fail "pod-nf (never, exit 1) should be Failed"
not_terminal() { # not_terminal <uid>
  local p; p="$(store_last "/pod-lifecycle-status/$1")"
  case "$p" in
    phase=Succeeded*|phase=Failed*) fail "$1 should NOT be terminal, got '$p'";;
    "") fail "$1 has no phase";;
    *) echo "   $1: $p (restarting, non-terminal)";;
  esac
}
not_terminal pod-of   # onfailure + exit 1 → keeps restarting
not_terminal pod-al   # always + exit 0 → restarts despite success
# restart backoff: a failing pod's consecutive-failure count is persisted
# as `;restarts=<n>` on its lifecycle status (pod-of failed at least once).
case "$(store_last /pod-lifecycle-status/pod-of)" in
  *restarts=*) echo "   pod-of: backoff count persisted";;
  *) fail "pod-of should carry ;restarts=<n> (backoff bookkeeping), got '$(store_last /pod-lifecycle-status/pod-of)'";;
esac
case "$(store_last /pod-lifecycle-status/pod-slp)" in
  phase=Running*) echo "   pod-slp: Running";;
  *) fail "pod-slp (sleep 30) should be Running, got '$(store_last /pod-lifecycle-status/pod-slp)'";;
esac
echo "   pod1: Succeeded   pod-nf: Failed"

echo "== 5. kill-with-grace + no-restart-during-delete pin =="
# Flip a restart=always pod AND the live sleeper to desired=deleted. The
# pin: the decision evaluates desired==deleted BEFORE the restart branch, so
# neither pod may be relaunched by its restartPolicy. pod-slp is live, so the
# two-phase kill runs: /sandbox-kill/ sig=term → SIGTERM kills sleep →
# terminal → delete flow + the kill key is consumed.
store_put "/pod-specs/pod-al"  "cmd=/bin/echo hello-pod;desired=deleted;restart=always;grace=1"
store_put "/pod-specs/pod-slp" "cmd=/bin/sleep 30;desired=deleted;grace=5"
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 4 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run2.log" 2>&1 || true

echo "== 6. verify: Terminating, destroyed, kill key consumed, never relaunched =="
for uid in pod-al pod-slp; do
  P="$(store_last "/pod-lifecycle-status/$uid")"
  case "$P" in
    phase=Terminating*) ;;
    *) fail "$uid should be Terminating (delete wins over restart), got '$P'";;
  esac
  SB="$(store_last "/sandboxes/$uid")"
  case "$SB" in
    *phase=delete*) ;;
    *) fail "$uid sandbox spec should be phase=delete, got '$SB'";;
  esac
  [ "$(store_last "/sandbox-kill/$uid")" = "" ] \
    || fail "$uid kill key should be consumed, got '$(store_last "/sandbox-kill/$uid")'"
done
ST="$(store_last /sandbox-status/pod-slp)"
case "$ST" in
  state=killed*|state=destroyed*) echo "   pod-slp: $ST (SIGTERM delivered via /sandbox-kill/)";;
  *) fail "pod-slp should be killed/destroyed after sig=term, got '$ST'";;
esac

echo "== E2E green: decision→effect→oci + restartPolicy + backoff + kill-with-grace pin =="
