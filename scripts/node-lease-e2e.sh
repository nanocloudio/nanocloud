#!/usr/bin/env bash
# A node registers itself and keeps its lease, and the control plane takes a
# node that stops renewing out of service; chains from
# modules/app/_chronicle/node_lease.uproc, readiness projected by
# daemonset.uproc `rdy` as in the control plane.
#
#   renew  (node)   /nodes/<self>: absent or due -> ready, heartbeat now
#   seen   (plane)  /nodes/ -> /node-seen/<node> rev, at (the plane's clock)
#   stale  (plane)  unchanged 30 s -> /nodes/<node> status.ready false
#
# One store, two graphs: the node's chain plus the plane's, and the plane's
# alone — a node that has died.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-nodelease-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
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
store_last() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}
# how many records were written for <key> (PUT-if-changed must not spend revisions)
store_writes() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}

DEC_RENEW="$(nc_decision "$ROOT/modules/app/_chronicle/node_lease.uproc" renew_p)"
DEC_SEEN="$(nc_decision "$ROOT/modules/app/_chronicle/node_lease.uproc" seen_p)"
DEC_STALE="$(nc_decision "$ROOT/modules/app/_chronicle/node_lease.uproc" stale_p)"
DEC_RDY="$(nc_decision "$ROOT/modules/app/_chronicle/daemonset.uproc" rdy)"
for v in "$DEC_RENEW" "$DEC_SEEN" "$DEC_STALE" "$DEC_RDY"; do
  [ -n "$v" ] || { echo "FAIL: a decision did not compile"; exit 1; }
done

plane_modules() {
  cat <<YAML
  - name: nl_nodes
    type: store_source
    prefix: "/nodes/"
    rev: 1
    now: 1
    related: "/node-seen/!flat:rev#,at#"
  - name: nl_seen
    type: store_effect
    decision: "${DEC_SEEN}"
    key_prefix: "/node-seen/"
  - name: nl_leases
    type: store_source
    prefix: "/node-seen/"
    flat: 1
    ints: "at"
    now: 1
    due: "at+30000"
    related: "/nodes/!json:status.ready"
  - name: nl_stale
    type: store_effect
    decision: "${DEC_STALE}"
    key_prefix: "/nodes/"
  - name: ds_nodes
    type: store_source
    prefix: "/nodes/"
    paths: "status.ready"
  - name: ds_mark
    type: store_effect
    decision: "${DEC_RDY}"
    key_prefix: "/ready-nodes/"
YAML
}
plane_wiring() {
  cat <<YAML
  - from: nl_nodes.status
    to: nl_nodes.changes
  - from: nl_nodes.record_out
    to: nl_seen.request_in
  - from: nl_leases.status
    to: nl_leases.changes
  - from: nl_leases.record_out
    to: nl_stale.request_in
  - from: ds_nodes.status
    to: ds_nodes.changes
  - from: ds_nodes.record_out
    to: ds_mark.request_in
YAML
}
graph() { # graph <with-node: 0|1>
  {
    echo "target: linux"; echo "tick_us: 1000"
    echo "scheduler:"; echo "  accept_cycles: true"
    echo "modules:"
    plane_modules
    if [ "$1" = 1 ]; then cat <<YAML
  - name: nl_self
    type: store_source
    prefix: "/nodes/"
    where: "metadata.name=node-a"
    paths: "status.ready"
    ints: "status.heartbeat"
    count: 1
    empty: 1
    label: "node-a"
    now: 1
    due: "status.heartbeat+10000"
  - name: nl_renew
    type: store_effect
    decision: "${DEC_RENEW}"
    key_prefix: "/nodes/"
YAML
    fi
    echo "wiring:"
    plane_wiring
    if [ "$1" = 1 ]; then cat <<YAML
  - from: nl_self.status
    to: nl_self.changes
  - from: nl_self.record_out
    to: nl_renew.request_in
YAML
    fi
  } >"$D/graph.yaml"
}
run() { # run <with-node> <seconds>
  graph "$1"
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  sleep "$2"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== 1. a node with no record registers itself =="
run 1 5
N="$(store_last /nodes/node-a)"
has "$N" '"name":"node-a"' '"ready":true' '"heartbeat":' || fail "node-a did not register: '$N'"
python3 -c "import json,sys; d=json.loads(sys.argv[1]); assert d['status']['ready'] is True" "$N" || fail "not JSON: '$N'"
[ "$(store_last /ready-nodes/node-a)" = "1" ] || fail "node-a not projected ready"
has "$(store_last /node-seen/node-a)" "rev=" ";at=" || fail "the plane did not note node-a"
echo "   /nodes/node-a ready, heartbeat stamped; /ready-nodes/node-a; /node-seen/node-a"
W="$(store_writes /nodes/node-a)"
echo "   $W write(s) in 5 s: renewed only when due"
[ "$W" -le 2 ] || fail "renewed $W times in 5 s"

echo "== 2. a live node keeps its lease well past it =="
run 1 45
has "$(store_last /nodes/node-a)" '"ready":true' || fail "a renewing node went stale"
[ "$(store_last /ready-nodes/node-a)" = "1" ] || fail "a renewing node left /ready-nodes/"
W2="$(store_writes /nodes/node-a)"
[ "$W2" -ge $((W + 3)) ] && [ "$W2" -le $((W + 6)) ] || fail "$((W2 - W)) renewal(s) in 45 s"
echo "   still ready after 45 s: $((W2 - W)) renewal(s), every 10 s"

echo "== 3. the node dies: the plane alone, past the lease =="
run 0 40
has "$(store_last /nodes/node-a)" '"ready":false' || fail "a silent node is still ready: '$(store_last /nodes/node-a)'"
[ -z "$(store_last /ready-nodes/node-a)" ] || fail "a silent node is still projected ready"
echo "   status.ready false, /ready-nodes/node-a gone"

echo "== 4. the node comes back: it renews at once, and is ready again =="
run 1 5
has "$(store_last /nodes/node-a)" '"ready":true' || fail "the node did not renew"
[ "$(store_last /ready-nodes/node-a)" = "1" ] || fail "the renewed node is not ready"
echo "   ready again"

echo "== E2E green: a node registers and renews its lease; a silent one is taken out of service and comes back =="
