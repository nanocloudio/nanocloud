#!/usr/bin/env bash
# Live E2E for nanocloud's proxy_compiler module (modules/app/proxy_compiler) —
# the store -> nft-ruleset seam for ClusterIP DNAT, verified UNPRIVILEGED.
#
# An EXTERNAL process (this script, playing the endpoints chain's output +
# the service projection) writes Services + Endpoints into the shared WAL; the
# proxy compiler fmod, inside the fluxor-linux runtime, compiles the node's nft
# NAT ruleset (ClusterIP -> backend DNAT load-balancing) and PUBLISHES it to
# /dataplane/proxy. The decision is what this asserts — the ruleset text, from
# the store — so it needs no root and no nft: programming it is the node
# network backend's half.
#
# Compact format:
#   /services/<ns>/<name>  = "sel=...;clusterip=<ip>;port=<p>;targetport=<tp>"
#   /endpoints/<ns>/<name> = "<pod>=<addr>,<pod>=<addr>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-proxy.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/proxy_compiler.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-px-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; echo "--- recorded ruleset ---"; cat "$D/applied.nft" 2>/dev/null; tail -10 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value>
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
    f.flush()
    os.fsync(f.fileno())
PY
}

store_last() { # store_last <key> — the published ruleset value
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
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else None
    p += 15 + kl + vl
sys.stdout.buffer.write(out if out is not None else b"")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project services + endpoints: multi-backend, single, headless, no-endpoints =="
wal_put "/services/default/web"      '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"},"clusterIP":"10.96.0.10","ports":[{"port":80,"targetPort":8080}]}}'
wal_put "/endpoints/default/web"     "web-1=10.0.0.1,web-2=10.0.0.2"
wal_put "/services/default/single"   '{"metadata":{"name":"single"},"spec":{"clusterIP":"10.96.0.20","ports":[{"port":443,"targetPort":8443}]}}'
wal_put "/endpoints/default/single"  "s-1=10.0.0.3"
wal_put "/services/default/headless" '{"metadata":{"name":"headless"},"spec":{"selector":{"app":"x"}}}'  # no clusterIP -> skipped
wal_put "/services/default/noep"     '{"metadata":{"name":"noep"},"spec":{"clusterIP":"10.96.0.30","ports":[{"port":80}]}}'  # no endpoints -> skipped

echo "== 3. run the compiler; it PUBLISHES the NAT ruleset to /dataplane/proxy =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn \
  timeout 2.5 "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
  >"$D/run.log" 2>&1 || true

echo "== 4. verify the published nft NAT ruleset in the store =="
RS="$(store_last /dataplane/proxy)"
[ -n "$RS" ] || fail "no ruleset published (compiler didn't PUBLISH to /dataplane/proxy)"
grep -q "table ip nanocloud-nat" <<<"$RS"                                  || fail "missing nat table"
grep -q "type nat hook prerouting priority -100" <<<"$RS"                  || fail "missing prerouting base chain"
grep -q "type nat hook output priority -100" <<<"$RS"                      || fail "missing output base chain"
grep -q "ip daddr 10.96.0.10 tcp dport 80 dnat to numgen random mod 2 map { 0 : 10.0.0.1:8080, 1 : 10.0.0.2:8080 }" <<<"$RS" \
  || fail "multi-backend DNAT load-balance wrong for web"
grep -q "ip daddr 10.96.0.20 tcp dport 443 dnat to 10.0.0.3:8443" <<<"$RS" || fail "single-backend DNAT wrong for single"
grep -q "10.96.0.30" <<<"$RS"                                              && fail "noep service (no endpoints) must NOT be programmed"

echo "== E2E green: ClusterIP DNAT compiled to nft and APPLYd over the shared store =="
echo "   web  -> LB(10.0.0.1:8080, 10.0.0.2:8080);  single -> 10.0.0.3:8443;"
echo "   headless (no clusterIP) and noep (no endpoints) correctly skipped."
