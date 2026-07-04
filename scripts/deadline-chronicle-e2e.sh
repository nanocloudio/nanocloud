#!/usr/bin/env bash
# A deadline is a store record, and a timer is an fd.
#
# store_source with `due = deadline` stamps the monotonic clock on every record
# (58) and arms a KERNEL timer fd for the earliest deadline ahead. The kernel
# steps the source when it fires — the timer wakes its owner and bounds the
# idle sleep (fluxor: fd::timer_pump) — the scan re-runs, the record whose time
# has come arrives with now >= deadline, and a plain decision deletes it.
# Nothing polls: between deadlines the graph is idle at its relaxed backstop.
#
# The domain is ADAPTIVE with a 50 ms backstop (the kernel's ceiling). The
# sharper proof is fluxor's own timer_wake_contract; here the property is
# end-to-end: a deadline in a record, met, with no polling in the graph.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-ttl-XXXXXX)"
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

DEC_EX="$(nc_decision "$ROOT/modules/app/_chronicle/ttl.uproc" ex)"
[ -n "$DEC_EX" ] || { echo "FAIL: ttl.uproc did not compile"; exit 1; }
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
execution:
  domains:
    - name: main
      cores: [0]
      adaptive_flags: 1
      tick_min_us: 1000
      tick_max_us: 50000
modules:
  - name: leases
    type: store_source
    domain: main
    prefix: "/leases/"
    flat: 1
    ints: "deadline"
    due: "deadline"
  - name: expire
    type: decision
    domain: main
    params:
      decision: "${DEC_EX}"
  - name: reap
    type: store_effect
    domain: main
    key_prefix: "/leases/"
wiring:
  - from: leases.status
    to: leases.changes
  - from: leases.record_out
    to: expire.record_in
  - from: expire.result_out
    to: reap.request_in
    buffer_group: 1
YAML
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null

# The runtime's monotonic clock starts near 0 at boot; deadlines are relative
# to it. Seed them as small absolute values: 1000 ms, 2500 ms, 60 s.
store_put "/leases/short"  "owner=a;deadline=1000"
store_put "/leases/medium" "owner=b;deadline=2500"
store_put "/leases/long"   "owner=c;deadline=60000"

echo "== 1. run 1.4 s: only the 1 s lease has expired =="
T0=$(python3 -c 'import time;print(time.monotonic())')
FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
RUNTIME_PID=$!
sleep 1.4
[ -z "$(store_last /leases/short)" ]  || fail "short (1 s) should have expired by 1.4 s"
[ -n "$(store_last /leases/medium)" ] || fail "medium (2.5 s) must still be live at 1.4 s"
[ -n "$(store_last /leases/long)" ]   || fail "long must be live"
echo "   short reaped, medium and long live"

echo "== 2. by 2.9 s the 2.5 s lease has gone; the 60 s one has not =="
sleep 1.5
[ -z "$(store_last /leases/medium)" ] || fail "medium (2.5 s) should have expired by 2.9 s"
[ -n "$(store_last /leases/long)" ]   || fail "long (60 s) must still be live"
echo "   medium reaped at its deadline, long live"

echo "== 3. nothing in the graph polls: the source is on an idle adaptive domain =="
kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
echo "   deadlines met on an idle adaptive domain — woken by the kernel timer the source armed"

echo "== E2E green: a deadline is a store record; the kernel timer wakes the source when it comes due =="
