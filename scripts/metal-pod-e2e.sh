#!/usr/bin/env bash
# Live E2E: a graph-backed Pod runs as a module subgraph of the node's own
# image — the only kind of Pod a bare-metal node runs.
#
# A Pod with `runtimeClassName: fluxor` is asked of its node as
# `/pod-specs/<uid> = graph=<name>;…` (pod_specs); pod_project lands it on
# `/metal-sandboxes/`; the sandbox chain's metal arm asks workload_effect to
# CREATE it with the FLXA of the Graph object `/graphs.nanocloud.io/<name>`
# (what `kubectl apply` of a Graph stores; seeded here); the kernel's
# fmod-graph backend (the same one bcm2712 registers) stages it as an owned
# subgraph. The kubelet graph runs here with `churn_probe` packed under
# `workload_modules:` — the pod's one module, which steps forever.
#
# PASS = the sandbox reports running and the Pod's phase is Running; a pod
# naming a graph the node was not given fails closed (no such workload).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
D="$(mktemp -d /tmp/nc-metal-pod-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <value> — one put into the store's durable log
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}
store_last() { # store_last <key>
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. the kubelet graph, with churn_probe packed for workloads =="
GRAPH="$D/graph.yaml"
cp "$ROOT/packaging/debian/fluxor-podlifecycle.yaml" "$GRAPH"
printf '\nworkload_modules:\n  - churn_probe\n' >> "$GRAPH"
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. the graph the node is given, and two pods: one naming it, one not =="
FLXA="$(python3 "$ROOT/scripts/support/flxa.py" churn_probe:1=0)"
store_put "/graphs.nanocloud.io/probe" "{\"apiVersion\":\"nanocloud.io/v1\",\"kind\":\"Graph\",\"metadata\":{\"name\":\"probe\"},\"spec\":{\"flxa\":\"$FLXA\"}}"
store_put "/pod-specs/pm-ok"   "graph=probe;desired=running;restart=never;grace=0"
store_put "/pod-specs/pm-none" "graph=absent;desired=running;restart=never;grace=0"

echo "== 3. run =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 6 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify =="
ms="$(store_last /metal-sandboxes/pm-ok)"
case "$ms" in *graph=probe*) echo "   /metal-sandboxes/pm-ok: $ms" ;; *) fail "pm-ok never reached the metal sandbox plane: '$ms'" ;; esac
st="$(store_last /sandbox-status/pm-ok)"
case "$st" in state=running*) echo "   sandbox pm-ok: $st" ;; *) fail "pm-ok sandbox should be running, got '$st'" ;; esac
ph="$(store_last /pod-lifecycle-status/pm-ok)"
case "$ph" in phase=Running*) echo "   pod pm-ok: $ph" ;; *) fail "pm-ok should be Running, got '$ph'" ;; esac
st2="$(store_last /sandbox-status/pm-none)"
case "$st2" in state=failed*) echo "   sandbox pm-none: $st2 (no such graph)" ;; *) fail "pm-none should have failed to create, got '$st2'" ;; esac

echo "== E2E green: a graph-backed Pod runs as a subgraph of the node's own image, through the pod chain and the workload contract =="
