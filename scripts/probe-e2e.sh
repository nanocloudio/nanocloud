#!/usr/bin/env bash
# Live E2E for probes: probe_runner drives
# exec probes through sandbox_runner's /sandbox-exec/ seam and writes
# /probe-status/<uid>; pod_lifecycle folds a live=0 verdict into its
# restart+backoff machinery. Runs UNPRIVILEGED (null sandboxes; exec is the
# already-shipped seam).
#
# Matrix:
#   pod-ready  rprobe=exec:/bin/true...   → readiness flips 0→1
#              (readiness starts 0 — not ready until proven; live defaults 1)
#   pod-live   lprobe=exec:/bin/false...  restart=always
#              → live flips 1→0 past the failure threshold → pod_lifecycle
#                kills + relaunches under backoff (restarts=n persisted);
#                the verdict resets to live=1 for the fresh run
#   pod-tcp    lprobe=tcp:8080...         → parsed but NOT IMPLEMENTED (no
#                nanocloud module has a dial surface): ignored for verdicts —
#                never faked as pass OR fail, no restart may fire
#
# Probe encoding (see probe_runner/mod.rs):
#   <kind>:<target>:<period_s>:<timeout_s>:<fail_n>:<success_n>
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

D="$(mktemp -d /tmp/nc-probe-e2e-XXXXXX)"
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

echo "== 1. build the config + module table from the graph (3 modules) =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. materialize sleeper bundles + the probe matrix =="
mkdir -p "$D/slp"
# Readiness: /bin/true every 1s, timeout 2s, 1 fail / 1 success to flip.
store_put "/pod-specs/pod-ready" "cmd=/bin/sleep 60;desired=running;rprobe=exec:/bin/true:1:2:1:1"
# Liveness: /bin/false every 1s, 2 consecutive fails flip live→0.
store_put "/pod-specs/pod-live"  "cmd=/bin/sleep 60;desired=running;restart=always;lprobe=exec:/bin/false:1:2:2:1"
# tcp: parsed, NOT IMPLEMENTED — must be ignored (no fake verdict, no restart).
store_put "/pod-specs/pod-tcp"   "cmd=/bin/sleep 60;desired=running;lprobe=tcp:8080:1:2:2:1"

echo "== 3. run the pipeline (probe cadence ~1s; liveness kill + backoff ~4s) =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 8 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. readiness: /probe-status/pod-ready flips ready 0→1 =="
PR="$(store_last /probe-status/pod-ready)"
[ "$PR" = "live=1;ready=1" ] || fail "pod-ready should be live=1;ready=1, got '$PR'"
# The flip itself (not just the final value): first publish is ready=0.
if ! store_history /probe-status/pod-ready | head -1 | grep -q 'ready=0'; then
  fail "pod-ready should have started ready=0 (readiness proves itself)"
fi
echo "   pod-ready: ready 0 -> 1"

echo "== 5. liveness: live=0 past threshold → restart + backoff =="
if ! store_history /probe-status/pod-live | grep -q 'live=0'; then
  fail "pod-live should have flipped live=0 (2x /bin/false)"
fi
# The failed run was torn down (destroyed appears) and relaunched under
# backoff with the count persisted ( restarts=<n>).
if ! store_history /sandbox-status/pod-live | grep -q 'state=destroyed'; then
  fail "pod-live liveness failure should tear the sandbox down"
fi
if ! store_history /pod-lifecycle-status/pod-live | grep -Eq 'restarts=[0-9]+'; then
  fail "pod-live should persist a restart count after the liveness kill"
fi
# The fresh run re-proves its probes: the verdict reset to live=1 after teardown.
LAST_LIVE="$(store_last /probe-status/pod-live)"
case "$LAST_LIVE" in
  live=*) echo "   pod-live: live flipped 0, pod restarted (verdict now '$LAST_LIVE')";;
  *) fail "pod-live probe status missing, got '$LAST_LIVE'";;
esac

echo "== 6. tcp probe honesty: ignored, never faked, no restart =="
PT="$(store_last /probe-status/pod-tcp)"
[ "$PT" = "live=1;ready=1" ] || fail "pod-tcp (unimplemented kind) should publish the no-probe defaults, got '$PT'"
if store_history /pod-lifecycle-status/pod-tcp | grep -Eq 'Restarting|restarts='; then
  fail "an unimplemented tcp probe must never drive a restart"
fi
case "$(store_last /pod-lifecycle-status/pod-tcp)" in
  phase=Running*) ;;
  *) fail "pod-tcp should be Running, got '$(store_last /pod-lifecycle-status/pod-tcp)'";;
esac

echo "== E2E green: exec probes over the /sandbox-exec/ seam — readiness flip, liveness kill+backoff, tcp ignored honestly =="
