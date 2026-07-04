#!/usr/bin/env bash
# Live E2E for nanocloud's hpa_reconciler module (modules/app/hpa_reconciler) —
# closed-loop autoscaling over the fluxor-native control-plane store, consumed
# through the standard storage contracts (`storage.object` 0x14 +
# `storage.namespace` 0x13). Runs the HPA WITH deployment_reconciler +
# replicaset_reconciler so the whole feedback loop is visible: the HPA turns the
# Deployment's replicas knob from a metric, and the reconcilers cascade the new
# count to Pods. the store is
# single-process, owned by the fluxor-linux runtime, and seeded from its durable
# append-log at init.
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# metrics are replayed from `$D/store.log` at boot rather than raced in live. The
# HPA does NOT reconcile at cold-start (desired is a function of currentReplicas,
# which it mutates); instead it SUBSCRIBEs /hpa/ and /hpa-metrics/ with the
# include-initial-listing flag, so the seeded metrics replay as change events and
# drive exactly one scaling step. Each phase appends a new metric and re-runs a
# fresh process.
#
#   /hpa/<ns>/<name>             = "target=<dep>;min=<N>;max=<M>;cpu=<targetPct>"
#   /hpa-metrics/<ns>/<dep>      = "cpu=<avgUtilPct>"   (metrics-server's role)
#   /deployments.apps/<ns>/<dep> = "replicas=<R>;image=<img>"  (the knob)
#   /pods/<ns>/<dep>-<i>         = "image=<img>;owner=<dep>"
#
# Prereqs: `fluxor sync` and `fluxor modules build --target bcm2712` in this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-autoscale.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for m in hpa_reconciler deployment_reconciler replicaset_reconciler; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done
[ -e "$FLUXOR_RUNTIME" ] || { echo "FAIL: missing $FLUXOR_RUNTIME"; exit 1; }

D="$(mktemp -d /tmp/nc-hpa-e2e-XXXXXX)"
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
  FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 3 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true
}
present() { [ -n "$(store_last "$1")" ]; }
absent()  { [ -z "$(store_last "$1")" ]; }

echo "== phase A: Deployment replicas=2, HPA (50% CPU, 1..10), 90% load =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"image":"nginx"}]}}}}'
store_put "/hpa/default/web-hpa" '{"metadata":{"name":"web-hpa"},"spec":{"scaleTargetRef":{"name":"web"},"minReplicas":1,"maxReplicas":10,"targetCPUUtilizationPercentage":50}}'
store_put "/hpa-metrics/default/web" "cpu=90"
run_runtime
echo "   90% > 50% → scale up. desired=ceil(2*90/50)=4"
case "$(store_last /deployments.apps/default/web)" in *'"replicas":4'*) true ;; *) false ;; esac \
  || fail "HPA should scale the Deployment to 4: got '$(store_last /deployments.apps/default/web)'"
for i in 0 1 2 3; do present /pods/default/web-$i || fail "pod web-$i should exist after scale-up"; done
echo "   scaled Deployment to 4; pods web-0..web-3 materialised"

echo "== phase B: load falls to 20%, re-boot → scale down. desired=ceil(4*20/50)=2 =="
store_put "/hpa-metrics/default/web" "cpu=20"
run_runtime
case "$(store_last /deployments.apps/default/web)" in *'"replicas":2'*) true ;; *) false ;; esac \
  || fail "HPA should scale the Deployment back to 2: got '$(store_last /deployments.apps/default/web)'"
present /pods/default/web-0 || fail "web-0 should remain"
present /pods/default/web-1 || fail "web-1 should remain"
absent  /pods/default/web-2 || fail "web-2 should be scaled down"
absent  /pods/default/web-3 || fail "web-3 should be scaled down"
echo "   scaled Deployment back to 2; pods web-2, web-3 removed"

echo "== E2E green: closed-loop autoscaling — metric → replicas → Pods, over the fluxor-native store =="
