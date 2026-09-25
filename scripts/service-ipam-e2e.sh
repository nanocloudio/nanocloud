#!/usr/bin/env bash
# Live E2E for Service ClusterIP allocation as Chronicle params
# (modules/app/_chronicle/ipam_svc.uproc; graph EXTRACTED from
# fluxor-controlplane.yaml into packaging/debian/fluxor-service-ipam.yaml) over
# the fluxor-native control-plane store, seeded from its durable append-log.
#
# Proves: a ClusterIP a Service already holds is CLAIMED before allocation
# starts and never handed out again (the allocator is refused by the store,
# 409, and skips it); unallocated Services get next-fit addresses PATCHed into
# spec.clusterIP; and a deleted Service's claim is released.
#
#   /services/<ns>/<name> = {"metadata":…,"spec":{…,"clusterIP":"<ip>"}}
#   /ipam/svc/<u32>       = "owner=<ns>/<name>;ip=<u32>"   (the claim)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-service-ipam.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_source.fmod" "$MODULES_DIR/store_effect.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor sync && fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-ipam-e2e-XXXXXX)"
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

store_last() { # store_last <key>
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    if data[p+15:p+15+kl] == want:
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else b""
    p += 15 + kl + vl
print(out.decode() if out else "")
PY
}

echo "== 1. build the config + module table from the packaged graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

RUNTIME_PID=""
trap 'kill $RUNTIME_PID 2>/dev/null || true; rm -rf "$D"' EXIT
start() {
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
}
stop() { kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; }
ip_of() { store_last "/services/default/$1" | python3 -c "import json,sys;d=sys.stdin.read();print(json.loads(d)['spec'].get('clusterIP','') if d else '')"; }
await_ip() { # await_ip <svc> <ip>
  for _ in $(seq 300); do
    [ "$(ip_of "$1")" = "$2" ] && return 0
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited while waiting for $1"
    sleep 0.1
  done
  fail "$1: clusterIP '$(ip_of "$1")', want '$2' after 30s"
}
ip_u32() { python3 -c "import sys;a=[int(x) for x in sys.argv[1].split('.')];print((a[0]<<24)|(a[1]<<16)|(a[2]<<8)|a[3])" "$1"; }
svc() { store_put "/services/default/$1" "{\"metadata\":{\"name\":\"$1\"},\"spec\":{\"selector\":{\"app\":\"$1\"}$2}}"; }

echo "== 2. five unallocated Services, and one that already holds 10.96.0.5 =="
svc pre ',"clusterIP":"10.96.0.5"'
for n in a b c d e; do svc "$n" ""; done

echo "== 3. run: the held address is claimed first, then next-fit =="
start
await_ip e 10.96.0.6
stop
for pair in a:10.96.0.1 b:10.96.0.2 c:10.96.0.3 d:10.96.0.4 e:10.96.0.6 pre:10.96.0.5; do
  n="${pair%%:*}"; want="${pair#*:}"
  [ "$(ip_of "$n")" = "$want" ] || fail "$n: clusterIP '$(ip_of "$n")', want $want"
  claim="$(store_last "/ipam/svc/$(ip_u32 "$want")")"
  [ "$claim" = "owner=default/$n;ip=$(ip_u32 "$want")" ] || fail "claim for $want: '$claim'"
  echo "   $n -> $want (claimed)"
done
grep -q '"image"\|"selector":{"app":"a"}' <(store_last /services/default/a) || fail "PATCH dropped the Service's other fields"
echo "   .5 was refused by the store and skipped; the PATCH kept every other field"

echo "== 4. release: Service b is deleted -> its claim goes; a new Service gets .7 =="
python3 - "$D/store.log" "/services/default/b" <<'PY'
import struct, sys, os
path, key = sys.argv[1], sys.argv[2].encode()
with open(path, "r+b") as f:
    data = f.read(); p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
    f.flush(); os.fsync(f.fileno())
PY
svc f ""
start
await_ip f 10.96.0.7
for _ in $(seq 300); do [ -z "$(store_last "/ipam/svc/$(ip_u32 10.96.0.2)")" ] && break; sleep 0.1; done
stop
[ -z "$(store_last "/ipam/svc/$(ip_u32 10.96.0.2)")" ] || fail "b's claim on 10.96.0.2 was not released"
echo "   b's claim on 10.96.0.2 released; f -> 10.96.0.7 (next-fit)"

echo "== E2E green: ClusterIPs allocated as params — claims make every address unique =="
