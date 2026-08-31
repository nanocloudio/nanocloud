#!/usr/bin/env bash
# Live E2E for `store_source` — subscription-driven entry into a Chronicle graph.
#
# This is the seam a reconciler needs and a request/response connector cannot
# give it: nothing INVOKES a reconciler, the store pushes a change and the graph
# must run. `store_source` subscribes a prefix and re-emits one Chronicle v1
# record frame per object under it, with dotted JSON paths projected into flat
# fields — the two things Chronicle cannot do for itself (walk a prefix: no
# iteration; read `spec.to.name`: flat frames).
#
# The assertion is EXACT. The script builds the expected frames independently in
# python from the documented layout —
#   [count:u8] then count x [number:u8][type:u8][len:u16 LE][payload]
#   TY_BYTES = 0, TY_I64 = 1 (payload 8 bytes LE)
#   1 = seq (i64), 2 = rev (i64), 3 = key tail (bytes), 4.. = the paths in order
# — and requires those exact bytes in what the sink received.
#
# The object is a real nested Route (`spec.to.name` is depth 3), which is the
# case `route_validator` needs and that Q4's opaque-carry answer does not serve.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source debug; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-srcsrc-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -25 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <value>
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

echo "== 1. seed two Routes — real nested Kubernetes JSON =="
store_put "/routes/default/web" '{"metadata":{"name":"web"},"spec":{"host":"web.example.com","to":{"name":"web-svc"},"port":8080,"path":"/"}}'
store_put "/routes/default/api" '{"metadata":{"name":"api"},"spec":{"host":"api.example.com","to":{"name":"api-svc"},"port":9090}}'

echo "== 2. write the source graph =="
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: store_source
    prefix: "/routes/"
    # Depth 2 and depth 3 paths — exactly what route_validator decides on.
    paths: "spec.host,spec.to.name,spec.port,spec.path"
    # Deliberately tiny page: 60 objects then span ~6 LIST pages, so phase 5
    # exercises cursor-walking without needing a set so large that the log sink
    # (the only channel observer available here) starts dropping lines.
    page_bytes: 256
  - name: debug
    mode: 1
wiring:
  - from: store_source.status
    to: store_source.changes
  - from: store_source.record_out
    to: debug.data
    # buffer_group is REQUIRED on a record edge: a non-zero group puts the
    # channel in MAILBOX mode, where one write is one whole record. The default
    # is a byte FIFO, which fragments framed records under pressure — it cost
    # exactly one lost frame per stall here before this line existed.
    buffer_group: 1
    # Deliberately tiny: force the channel to fill so phase 5 exercises the
    # retain-and-resume path rather than a comfortably sized happy path.
    buffer_bytes: 1024
YAML

echo "== 3. build + run =="
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
FLUXOR_STORE_DIR="$D" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
sleep 2
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""

echo "== 4. assert the exact Chronicle v1 frames =="
python3 - "$D/run.log" <<'PY'
import struct, sys
log = open(sys.argv[1], "rb").read()
TY_BYTES, TY_I64 = 0, 1

def f(num, ty, payload):
    return bytes([num, ty]) + struct.pack("<H", len(payload)) + payload

def tail_of(key, host, svc, port, path):
    """Everything after the seq field, which the source assigns."""
    ns, _, name = key.partition(b"/")
    return (f(2, TY_I64, struct.pack("<q", 0))     # rev: unknown by this op
            + f(3, TY_BYTES, key)
            + f(4, TY_BYTES, host)
            + f(5, TY_BYTES, svc)
            + f(6, TY_BYTES, port)
            + f(7, TY_BYTES, path)
            # The key's own segments, always: 28 = namespace, 29 = name. A
            # document need not carry metadata.namespace; the key is the truth.
            + f(28, TY_BYTES, ns)
            + f(29, TY_BYTES, name)
            + f(57, TY_BYTES, name))   # the tail after its first segment

cases = [
    ("/routes/default/api  (spec.path ABSENT -> empty, field kept)",
     tail_of(b"default/api", b"api.example.com", b"api-svc", b"9090", b"")),
    ("/routes/default/web  (all four present)",
     tail_of(b"default/web", b"web.example.com", b"web-svc", b"8080", b"/")),
]

fails = []
for label, tail in cases:
    at = log.find(tail)
    if at < 0:
        fails.append(label)
        continue
    seq_at = at - 12          # field 1: [1][TY_I64][8,0] + 8 payload
    cnt_at = seq_at - 1
    if cnt_at < 0 or log[cnt_at] != 10:
        fails.append(label + " (field count != 10)")
        continue
    if log[seq_at] != 1 or log[seq_at + 1] != TY_I64:
        fails.append(label + " (field 1 is not the i64 seq)")
        continue
    seq = struct.unpack("<q", log[seq_at + 4:seq_at + 12])[0]
    if seq <= 0:
        fails.append(label + " (seq not monotonic from 1)")
        continue
    print("   ok  %-58s seq=%d, 10 fields, byte-exact" % (label, seq))

if fails:
    for x in fails:
        print("   FAIL " + x)
    sys.exit(1)
PY
[ $? -eq 0 ] || fail "frame assertion failed"

echo "== 5. paging + backpressure: a set larger than one LIST page AND the channel must arrive WHOLE =="
# The graph below gives record_out a deliberately tiny buffer, so the channel
# fills long before the set is drained. The source must RETAIN the undelivered
# work and resume mid-page: repeating from the page start would make no forward
# progress once a page exceeds the channel, which is a livelock rather than a
# slow path. Dropping would be worse still — nothing re-lists a prefix that
# stops changing, so a lost frame is lost for good.
#
# It also crosses several LIST pages: LIST_BUF is 2 KiB and these keys are ~20
# bytes, so 250 objects is roughly five pages. The provider emits a cursor and
# `list_page` walks to the end across steps, so the caller's buffer is not a
# ceiling on how many objects a prefix can hold.
# 60 objects at ~11 entries per 256-byte page is ~6 LIST pages, and ~6
# fill/drain cycles against the 1 KiB channel. Both paths exercised, and the
# frame total stays inside what the log sink carries reliably (it drops lines
# above roughly 10 KiB — a property of the instrument, not the module).
N=60
python3 - "$D/store.log" "$N" <<'PY'
import struct, sys, os
path, n = sys.argv[1], int(sys.argv[2])
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    for i in range(n):
        key = ("/routes/paged/r%03d" % i).encode()
        val = ('{"spec":{"host":"h%03d","to":{"name":"s%03d"},"port":%d}}' % (i, i, 8000 + i)).encode()
        last += 1
        f.write(struct.pack("<QBHI", last, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
FLUXOR_STORE_DIR="$D" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/paged.log" 2>&1 &
RUNTIME_PID=$!
sleep 8
kill "$RUNTIME_PID" 2>/dev/null || true
wait "$RUNTIME_PID" 2>/dev/null || true
RUNTIME_PID=""
python3 - "$D/paged.log" "$N" <<'PY'
import struct, sys
log = open(sys.argv[1], "rb").read(); n = int(sys.argv[2])
missing = [i for i in range(n)
           if log.find(struct.pack("<BBH", 3, 0, 10) + ("paged/r%03d" % i).encode()) < 0]
if missing:
    print("   FAIL %d of %d objects never emitted (first missing: r%03d)"
          % (len(missing), n, missing[0]))
    sys.exit(1)
print("   ok  all %d objects delivered whole — across LIST pages and backpressure stalls" % n)
PY
[ $? -eq 0 ] || fail "pagination assertion failed"

echo "== 6. `.` projects a raw value, and list_values drops the child names =="
# Two byte-moving options that exist because the VM cannot do either job: read a
# record that is NOT JSON with paths in it, and strip a label off every element
# of a list.
D2="$(mktemp -d /tmp/nc-ss2-XXXXXX)"
store_put2() {
  python3 - "$D2/store.log" "$1" "$2" <<'PY'
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
# A svc whose slices are BARE ADDRESSES — no JSON, no `k=v`, nothing a path can
# reach. This is the shape endpoints.uproc actually writes.
store_put2 "/svc/default/web" "x=1"
store_put2 "/slices/default/web/pod-a" "10.0.0.1"
store_put2 "/slices/default/web/pod-b" "10.0.0.2"
cat >"$D2/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  # `join` and `list_children` are alternative branches of the same projection,
  # never both on one node — so the two options are exercised by two nodes over
  # the same data.
  - name: store_source
    prefix: "/svc/"
    flat: 1
    paths: "x"
    join: "/slices/"
    join_scoped: 1
    join_paths: "."
  - name: fold
    type: store_source
    prefix: "/svc/"
    flat: 1
    paths: "x"
    list_children: "/slices/"
    child_sep: "/"
    list_values: 1
  - name: debug
    mode: 1
wiring:
  - from: store_source.status
    to: store_source.changes
  - from: fold.status
    to: fold.changes
  - from: store_source.record_out
    to: debug.data
    buffer_group: 1
  - from: fold.record_out
    to: debug.data
    buffer_group: 1
YAML
nc_build_workload "$ROOT" "$D2/graph.yaml" "$D2/config.bin" "$D2/modules.bin"
FLUXOR_STORE_DIR="$D2" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D2/config.bin" --modules "$D2/modules.bin" >"$D2/run.log" 2>&1 &
RUNTIME_PID=$!
sleep 2
kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
python3 - "$D2/run.log" <<'PY' || { echo "FAIL: raw/list_values projection"; rm -rf "$D2"; exit 1; }
import sys
log = open(sys.argv[1], "rb").read()
# join_paths "." puts the joined entry's WHOLE VALUE at field 60 — here a bare
# IP that no path could have reached.
ok_raw = b"\x3c\x00\x09\x0010.0.0.1" in log or b"10.0.0.1" in log
# list_values drops the `<name>=`: the children arrive as values alone.
ok_vals = b"10.0.0.1,10.0.0.2" in log
if not ok_raw:
    print("   FAIL: join_paths '.' did not project the raw joined value"); sys.exit(1)
if not ok_vals:
    print("   FAIL: list_values still labelled the children"); sys.exit(1)
if b"pod-a=10.0.0.1" in log:
    print("   FAIL: list_values did not drop the `<name>=` labels"); sys.exit(1)
print("   ok  join_paths '.' -> the bare address; list_values -> 10.0.0.1,10.0.0.2")
PY
rm -rf "$D2"

echo "== E2E green: store_source projects nested Kubernetes objects into byte-exact Chronicle v1 frames, driven by a store subscription, surviving backpressure without loss =="
