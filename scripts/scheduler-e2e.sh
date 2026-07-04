#!/usr/bin/env bash
# Live E2E for nanocloud's scheduler module (modules/app/scheduler) — pod
# placement over the fluxor-native control-plane store, consumed through the
# standard storage contracts (`storage.object` 0x14 + `storage.namespace`
# 0x13). The store is single-process, owned by the fluxor-linux runtime, and
# seeded from its durable append-log at init.
#
# Proves the kube-scheduler loop on real binaries: unbound Pods and Nodes are
# replayed from `$D/store.log` at boot; the scheduler fmod, inside the runtime,
# cold-starts (SUBSCRIBE /pods/ + /nodes/ onto its `changes` input channel, then
# a full LIST reconcile) and binds each unbound Pod to the least-loaded ready
# Node by writing node=<name> onto it — spreading evenly, skipping not-ready
# Nodes, and leaving already-bound Pods alone.
#
#   /nodes/<name>     = "ready=<0|1>"
#   /pods/<ns>/<name> = "image=<img>[;node=<node>]"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-scheduler.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/scheduler.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-sched-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

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

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project 2 ready nodes, 1 not-ready node, 4 unbound + 1 pre-bound pod =="
store_put "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":true}}'
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":false}}'  # never eligible
store_put "/pods/default/pod-a" '{"metadata":{"name":"pod-a"},"spec":{"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-b" '{"metadata":{"name":"pod-b"},"spec":{"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-c" '{"metadata":{"name":"pod-c"},"spec":{"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-d" '{"metadata":{"name":"pod-d"},"spec":{"containers":[{"image":"x"}]}}'
store_put "/pods/default/pod-z" '{"metadata":{"name":"pod-z"},"spec":{"nodeName":"node-3","containers":[{"image":"x"}]}}'  # already bound

echo "== 3. run the scheduler =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the least-loaded spread (byte-ordered ties → lower node) =="
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }
expect() { # expect <pod> <bound-node> — assert spec.nodeName is <bound-node>
  local got; got="$(store_last "/pods/default/$1")"
  case "$got" in *"\"nodeName\":\"$2\""*) ;; *) fail "$1 wrong: got '$got', want nodeName=$2";; esac
  echo "   $1 -> $2"
}
# a→n1, b→n2, c→n1, d→n2 — an even 2/2 spread; node-3 (not ready) never chosen.
expect pod-a node-1
expect pod-b node-2
expect pod-c node-1
expect pod-d node-2
# pre-bound pod is left untouched (no reschedule), even off a not-ready node.
expect pod-z node-3

# Each binding records a k8s Event (/events/<ns>/<pod>.Scheduled.0).
EV="$(store_last "/events/default/pod-a.Scheduled.0")"
if [[ "$EV" == *'"reason":"Scheduled"'* && "$EV" == *'"kind":"Pod"'* && "$EV" == *'"name":"pod-a"'* ]]; then
  echo "   event: pod-a Scheduled recorded"
else
  fail "no Scheduled event for pod-a: got '$EV'"
fi

echo "== E2E green: least-loaded pod placement + Scheduled events over the shared store =="
