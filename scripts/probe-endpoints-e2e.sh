#!/usr/bin/env bash
# Full-chain E2E for the readiness bridge:
#
#   probe_runner drives an exec readiness probe through sandbox_runner's
#   /sandbox-exec/ seam and writes /probe-status/<uid> = live=..;ready=..
#          │  (the verdict, keyed by pod uid)
#          ▼
#   endpoints_reconciler joins that verdict onto the k8s Pod object at
#   /pods/<ns>/<name> by metadata.uid, and the verdict OVERRIDES the pod's
#   static status.ready when selecting a Service's endpoints
#          │
#          ▼
#   the pod appears in / stays out of /endpoints/<ns>/<name>
#
# This is the hop from a probe verdict to endpoint membership.
# All four modules run in ONE runtime over the shared store, so a REAL probe
# flip propagates to endpoints selection — nothing is hand-seeded on
# /probe-status/.
#
# Two pods behind Service default/web (selector app=web), each seeded with a
# k8s Pod object carrying metadata.uid:
#   web-ready  rprobe=exec:/bin/true   static status.ready=FALSE
#              → probe flips ready 0→1 → OVERRIDE includes it (proves the probe,
#                not the static field, put it in the endpoint set)
#   web-fail   rprobe=exec:/bin/false  static status.ready=TRUE
#              → probe holds ready=0 → OVERRIDE excludes it (proves the probe can
#                pull a statically-ready pod OUT)
#
# Runs UNPRIVILEGED (null sandboxes; exec is the already-shipped seam).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" \
         "$MODULES_DIR/pod_lifecycle.fmod" "$MODULES_DIR/sandbox_runner.fmod" \
         "$MODULES_DIR/probe_runner.fmod" "$MODULES_DIR/endpoints_reconciler.fmod"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-probe-ep-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

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

echo "== 1. compose the full-chain graph (pod_lifecycle + sandbox_runner + probe_runner + endpoints_reconciler) =="
cat > "$D/graph.yaml" <<'YAML'
# Full-chain readiness-bridge fixture: the
# pod-lifecycle pipeline plus the endpoints reconciler, all over the one store.
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: pod_lifecycle
  - name: sandbox_runner
  - name: probe_runner
  - name: endpoints_reconciler
wiring:
  - from: pod_lifecycle.status
    to: pod_lifecycle.changes
  - from: sandbox_runner.status
    to: sandbox_runner.changes
  - from: probe_runner.status
    to: probe_runner.changes
  - from: endpoints_reconciler.status
    to: endpoints_reconciler.changes
YAML
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 2. materialize a sleeper bundle + seed the service, pods, and pod-specs =="
mkdir -p "$D/slp"

# Service selects app=web.
store_put "/services/default/web" '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"}}}'

# web-ready: readiness /bin/true (flips 0->1). Static status.ready seeded FALSE,
# so inclusion can ONLY come from the probe override.
store_put "/pods/default/web-ready" '{"metadata":{"name":"web-ready","uid":"pod-ready","labels":{"app":"web"}},"status":{"podIP":"10.0.0.7","ready":false}}'
store_put "/pod-specs/pod-ready"    "cmd=/bin/sleep 60;desired=running;rprobe=exec:/bin/true:1:2:1:1"

# web-fail: readiness /bin/false (holds ready=0). Static status.ready seeded
# TRUE, so exclusion can ONLY come from the probe override pulling it out.
store_put "/pods/default/web-fail" '{"metadata":{"name":"web-fail","uid":"pod-fail","labels":{"app":"web"}},"status":{"podIP":"10.0.0.8","ready":true}}'
store_put "/pod-specs/pod-fail"    "cmd=/bin/sleep 60;desired=running;rprobe=exec:/bin/false:1:2:1:1"

echo "== 3. run the pipeline (sandbox launch + probe cadence ~1s) =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 10 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. probe_runner produced a REAL readiness flip on web-ready =="
PR="$(store_last /probe-status/pod-ready)"
[ "$PR" = "live=1;ready=1" ] || fail "pod-ready should be live=1;ready=1, got '$PR'"
store_history /probe-status/pod-ready | head -1 | grep -q 'ready=0' \
  || fail "pod-ready readiness should have STARTED at 0 (proves itself via the probe)"
echo "   /probe-status/pod-ready: ready 0 -> 1"

PF="$(store_last /probe-status/pod-fail)"
case "$PF" in
  *ready=0) echo "   /probe-status/pod-fail: ready held at 0 (/bin/false)";;
  *) fail "pod-fail readiness should be held at 0, got '$PF'";;
esac

echo "== 5. the bridge: endpoints reflect the PROBE verdict, not the static field =="
EP="$(store_last /endpoints/default/web)"
# web-ready: static=false but probe=1 → IN.  web-fail: static=true but probe=0 → OUT.
[ "$EP" = "web-ready=10.0.0.7" ] \
  || fail "endpoints should be exactly 'web-ready=10.0.0.7' (probe override in AND out), got '$EP'"
echo "   /endpoints/default/web: $EP"

echo "== 6. web-ready APPEARED only after the probe passed (disappear->appear) =="
# Every written endpoints doc: web-ready must not have been present before its
# probe flipped (static ready=false kept it out); the final doc contains it.
if store_history /endpoints/default/web | grep -q 'web-fail'; then
  fail "web-fail must NEVER enter the endpoint set (its readiness probe fails)"
fi
store_history /endpoints/default/web | tail -1 | grep -q 'web-ready=10.0.0.7' \
  || fail "web-ready must be in the final endpoint set once its probe passed"
echo "   web-fail never appeared; web-ready appeared on the flip"

echo "== E2E green: readiness bridge closed — a real probe flip reaches Service endpoints via metadata.uid join =="
