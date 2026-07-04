#!/usr/bin/env bash
# VolumeSnapshot as a Chronicle graph: create the bound VolumeSnapshotContent,
# then PATCH the VolumeSnapshot's status to bind it and mark it ready.
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

D="$(mktemp -d /tmp/nc-snapchron-XXXXXX)"
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

DEC_CT="$(nc_decision "$ROOT/modules/app/_chronicle/snapshot.uproc" ct)"
DEC_BD="$(nc_decision "$ROOT/modules/app/_chronicle/snapshot.uproc" bd)"
[ -n "$DEC_CT" ] && [ -n "$DEC_BD" ] || { echo "FAIL: snapshot.uproc did not compile"; exit 1; }
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: snaps
    type: store_source
    prefix: "/volumesnapshots.snapshot.storage.k8s.io/"
    paths: "status.readyToUse,spec.source.persistentVolumeClaimName"
  - name: content
    type: decision
    params:
      decision: "${DEC_CT}"
  - name: contents
    type: store_effect
    key_prefix: "/volumesnapshotcontents.snapshot.storage.k8s.io/"
  - name: bind
    type: decision
    params:
      decision: "${DEC_BD}"
  - name: status
    type: store_effect
    key_prefix: "/volumesnapshots.snapshot.storage.k8s.io/"
wiring:
  - from: snaps.status
    to: snaps.changes
  - from: snaps.record_out
    to: content.record_in
  - from: content.result_out
    to: contents.request_in
    buffer_group: 1
  - from: contents.response_out
    to: bind.record_in
  - from: bind.result_out
    to: status.request_in
    buffer_group: 2
YAML
run() { # run <seconds>
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  sleep "$1"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== 1. a VolumeSnapshot of PVC 'data' =="
store_put "/volumesnapshots.snapshot.storage.k8s.io/default/snap1" '{"metadata":{"name":"snap1"},"spec":{"source":{"persistentVolumeClaimName":"data"}}}'
run 6
VS="$(store_last /volumesnapshots.snapshot.storage.k8s.io/default/snap1)"
has "$VS" '"readyToUse":true' '"boundVolumeSnapshotContentName":"snapcontent-snap1"' '"persistentVolumeClaimName":"data"' || fail "VolumeSnapshot not bound/ready: '$VS'"
VSC="$(store_last /volumesnapshotcontents.snapshot.storage.k8s.io/snapcontent-snap1)"
has "$VSC" '"readyToUse":true' '"volumeHandle":"data"' '"name":"snap1"' '"namespace":"default"' '"snapshotHandle":"snap-snap1"' || fail "content wrong: '$VSC'"
python3 -c "import json,sys; json.loads(sys.argv[1]); json.loads(sys.argv[2])" "$VS" "$VSC" || fail "not valid JSON"
echo "   snap1 bound + ready; snapcontent-snap1 provisioned; both valid JSON"

echo "== 2. a second pass is quiet: readyToUse gates it =="
W1="$(python3 - "$D/store.log" <<'PY2'
import struct,sys
d=open(sys.argv[1],"rb").read();p=n=0
while p+15<=len(d):
    rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15]); n+=1; p+=15+kl+vl
print(n)
PY2
)"
run 4
W2="$(python3 - "$D/store.log" <<'PY2'
import struct,sys
d=open(sys.argv[1],"rb").read();p=n=0
while p+15<=len(d):
    rev,op,kl,vl=struct.unpack("<QBHI",d[p:p+15]); n+=1; p+=15+kl+vl
print(n)
PY2
)"
[ "$W1" = "$W2" ] || fail "a ready snapshot was rewritten ($W1 -> $W2 records)"
echo "   $W1 records before, $W2 after"

echo "== E2E green: VolumeSnapshot as params — content provisioned, status patched, idempotent =="
