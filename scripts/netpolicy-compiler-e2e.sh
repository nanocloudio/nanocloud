#!/usr/bin/env bash
# Live E2E for nanocloud's netpolicy_compiler module (modules/app/
# netpolicy_compiler) — the store -> nft-ruleset seam, verified UNPRIVILEGED.
#
# Proves the compiler on real binaries: an EXTERNAL process (this script,
# playing the control-plane projection) writes a NetworkPolicy + pods into the
# shared WAL; the compiler fmod, inside the fluxor-linux runtime, compiles the
# node's nft ruleset and PUBLISHES it to /dataplane/netpolicy. The compiler's
# *output* is what this asserts — the ruleset text, read back from the store —
# so it needs no root and no CAP_NET_ADMIN: programming nft is the node network
# backend's half.
#
# Compact format:
#   /networkpolicies/<ns>/<name> = k8s NetworkPolicy JSON (spec.podSelector,
#                                  policyTypes[], ingress[]/egress[] rules)
#   /pods/<ns>/<name>            = "ip=<addr>;l=<k=v,k=v>;r=0|1"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-netpolicy.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/netpolicy_compiler.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-np-e2e-XXXXXX)"
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

echo "== 2. project an ingress policy (app=web) + an egress policy (app=db) + pods =="
wal_put "/networkpolicies/default/web-allow" '{"metadata":{"name":"web-allow"},"spec":{"podSelector":{"matchLabels":{"app":"web"}},"policyTypes":["Ingress"],"ingress":[{"from":[{"ipBlock":{"cidr":"10.0.0.0/8"}}],"ports":[{"protocol":"TCP","port":80}]}]}}'
wal_put "/networkpolicies/default/db-egress" '{"metadata":{"name":"db-egress"},"spec":{"podSelector":{"matchLabels":{"app":"db"}},"policyTypes":["Egress"],"egress":[{"to":[{"ipBlock":{"cidr":"10.0.0.0/8"}}],"ports":[{"protocol":"TCP","port":5432}]}]}}'
wal_put "/pods/default/web-1"   '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.5","ready":true}}'  # ingress-governed
wal_put "/pods/default/db-1"    '{"metadata":{"name":"db-1","labels":{"app":"db"}},"status":{"podIP":"10.0.0.9","ready":true}}'  # egress-governed
wal_put "/pods/default/cache-1" '{"metadata":{"name":"cache-1","labels":{"app":"cache"}},"status":{"podIP":"10.0.0.7","ready":true}}'  # not selected

echo "== 3. run the compiler; it PUBLISHES the ruleset to /dataplane/netpolicy =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn \
  timeout 2.5 "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
  >"$D/run.log" 2>&1 || true

echo "== 4. verify the published nft ruleset in the store =="
RS="$(store_last /dataplane/netpolicy)"
[ -n "$RS" ] || fail "no ruleset published (compiler didn't PUBLISH to /dataplane/netpolicy)"
grep -q "table inet nanocloud" <<<"$RS"                          || fail "missing nanocloud table"
grep -q "type filter hook forward priority 0; policy accept;" <<<"$RS" || fail "missing base chain"
grep -q "ip daddr 10.0.0.5 counter jump NCLD-NPI" <<<"$RS"        || fail "missing ingress jump (daddr) for web-1"
grep -q "ip saddr 10.0.0.0/8 tcp dport 80 counter return" <<<"$RS" || fail "missing ingress allow rule"
grep -q "ip saddr 10.0.0.9 counter jump NCLD-NPE" <<<"$RS"        || fail "missing egress jump (saddr) for db-1"
grep -q "ip daddr 10.0.0.0/8 tcp dport 5432 counter return" <<<"$RS" || fail "missing egress allow rule"
grep -q "counter drop" <<<"$RS"                                  || fail "missing default-drop tail"
grep -q "10.0.0.7" <<<"$RS"                                      && fail "cache-1 (unselected) must NOT be in the ruleset"

echo "== E2E green: ingress + egress compiled to nft and APPLYd over the shared store =="
echo "   web-1 (10.0.0.5): ingress chain (daddr jump) allowing 10.0.0.0/8 tcp/80;"
echo "   db-1  (10.0.0.9): egress chain (saddr jump) allowing 10.0.0.0/8 tcp/5432;"
echo "   cache-1 (10.0.0.7): unselected, unrestricted (no chain)."
