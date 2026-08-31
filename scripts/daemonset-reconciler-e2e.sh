#!/usr/bin/env bash
# Live E2E for the DaemonSet chain (`ds_*`, Chronicle params) as it ships in
# packaging/debian/fluxor-daemonset.yaml — one Pod per ready Node over the
# control-plane store, consumed through the standard storage contracts
# (`storage.object` 0x14 + `storage.namespace` 0x13). The store is
# single-process, owned by the fluxor-linux runtime, and seeded from its
# durable append-log at init.
#
# Because the store lives INSIDE the runtime process (no shared WAL, no flock),
# desired state is replayed from `$D/store.log` at boot rather than raced in live
# from an external writer. The chain cold-starts: its sources SUBSCRIBE
# /daemonsets.apps/, /nodes/ and /pods/, then a full pass creates one pre-bound
# Pod per ready Node and prunes Pods on dead Nodes.
#
#   /daemonsets.apps/<ns>/<name> = "image=<img>"
#   /nodes/<node>                = "ready=<0|1>"
#   /pods/<ns>/<ds>-<node>       = "image=<img>;owner=<ds>;node=<node>"
#
# Two phases prove durable reconcile: phase A seeds a DaemonSet + 3 nodes (2
# ready) and checks per-node coverage; phase B appends node-2 not-ready to the
# same log and re-runs (a fresh process replays the updated state) and checks the
# now-orphaned Pod is pruned.
#
# Prereqs: `fluxor sync` and `fluxor modules build --target bcm2712` in this repo.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-daemonset.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-ds-e2e-XXXXXX)"
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

echo "== phase A: seed a DaemonSet and 3 nodes (2 ready, 1 not); one pod per ready node =="
store_put "/daemonsets.apps/default/log" '{"metadata":{"name":"log"},"spec":{"template":{"spec":{"containers":[{"name":"log","image":"fluentd"}]}}}}'
store_put "/nodes/node-1" '{"metadata":{"name":"node-1"},"status":{"ready":true}}'
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":true}}'
store_put "/nodes/node-3" '{"metadata":{"name":"node-3"},"status":{"ready":false}}'
run_runtime
case "$(store_last /pods/default/log-node-1)" in *'"nodeName":"node-1"'*'"image":"fluentd"'*|*'"image":"fluentd"'*'"nodeName":"node-1"'*) ;; *) fail "log-node-1 missing/wrong";; esac
case "$(store_last /pods/default/log-node-2)" in *'"nodeName":"node-2"'*) ;; *) fail "log-node-2 missing";; esac
[ -z "$(store_last /pods/default/log-node-3)" ] || fail "log-node-3 should NOT exist (node-3 not ready)"
echo "   pods on node-1, node-2; none on node-3"

echo "== phase B: node-2 goes not-ready, re-boot → its daemon pod is pruned =="
store_put "/nodes/node-2" '{"metadata":{"name":"node-2"},"status":{"ready":false}}'
run_runtime
case "$(store_last /pods/default/log-node-1)" in *'"nodeName":"node-1"'*) ;; *) fail "log-node-1 should remain";; esac
[ -z "$(store_last /pods/default/log-node-2)" ] || fail "log-node-2 should be pruned (node-2 not ready)"
echo "   node-1 pod kept; node-2 pod pruned"

echo "== E2E green: DaemonSet one-pod-per-ready-node over the fluxor-native store =="
