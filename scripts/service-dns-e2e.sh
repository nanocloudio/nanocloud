#!/usr/bin/env bash
# Live E2E for nanocloud's service_dns module (modules/app/service_dns) — the
# cluster DNS zone as a projection of endpoints. Runs WITH endpoints_reconciler
# so the whole chain is visible: services + ready pods become endpoints, and
# endpoints become /dns/<name>.<ns>.svc.cluster.local A-records.
#
# Over the fluxor-native control-plane store (storage.object 0x14 +
# storage.namespace 0x13). The store
# lives inside the runtime and is seeded from its durable append-log
# ($D/store.log) at boot — so state changes are proven across two phases: phase A
# seeds a Service + two ready pods and checks the zone; phase B appends a
# not-ready flip to the same log and re-boots (a fresh process replays it),
# checking the dropped backend follows.
#
#   /services/<ns>/<name>              = "sel=k=v"
#   /pods/<ns>/<name>                  = "ip=<addr>;l=k=v;r=0|1"
#   /endpoints/<ns>/<name>             = "<pod>=<ip>,<pod>=<ip>"   (endpoints_reconciler)
#   /dns/<name>.<ns>.svc.cluster.local = "a=<ip>;a=<ip>"           (service_dns)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-dns.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for m in service_dns; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done
[ -e "$FLUXOR_RUNTIME" ] || { echo "FAIL: missing $FLUXOR_RUNTIME"; exit 1; }

D="$(mktemp -d /tmp/nc-dns-e2e-XXXXXX)"
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

run_runtime() {
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}

echo "== 1. build the config + module table from the dns graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. a Service and two ready backend pods =="
store_put "/services/default/web" '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"}}}'
store_put "/pods/default/web-1" '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.1","ready":true}}'
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","ready":true}}'

echo "== 3. phase A: endpoints → DNS A-records for both backends =="
run_runtime
DNS="$(store_last '/dns/web.default.svc.cluster.local')"
[ "$DNS" = "a=10.0.0.1;a=10.0.0.2" ] \
  || fail "DNS zone wrong: got '$DNS', want 'a=10.0.0.1;a=10.0.0.2'"
echo "   web.default.svc.cluster.local -> $DNS"

echo "== 4. phase B: a backend goes not-ready → its A-record drops out =="
store_put "/pods/default/web-2" '{"metadata":{"name":"web-2","labels":{"app":"web"}},"status":{"podIP":"10.0.0.2","ready":false}}'
run_runtime
DNS="$(store_last '/dns/web.default.svc.cluster.local')"
[ "$DNS" = "a=10.0.0.1" ] \
  || fail "DNS zone should drop the not-ready backend: got '$DNS', want 'a=10.0.0.1'"
echo "   web.default.svc.cluster.local -> $DNS (web-2 removed)"

echo "== E2E green: service discovery — endpoints projected onto the DNS zone =="
