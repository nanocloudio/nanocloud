#!/usr/bin/env bash
# Module-level E2E for Nanocloud's Wormhole modules, each in a graph of its
# own (scripts/support/graphs/wh-*.yaml) over the host store, fed what its
# producer sends (the harness's `emit_module_inputs_for_the_e2e`: Wormhole's
# and Kagi's own codecs) and read back from the store:
#
#   wormhole_ledger  a region controller's ledger → one row per entry, verbatim
#   wormhole_usage   usage_record.v1 (one sent twice) → the stream's row, applied once
#   wormhole_dns     Kagi-signed KSDN → the Service's row; a forged one refused
#   wormhole_fence   region rows → the RegionFence confirmation, only on proof
#   wormhole_fd      the controller's status → a Preflight and an Observation
#                    from the nodes, Pods and projections; a Placement merged
#                    into the zone's nodes
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
G="$ROOT/scripts/support/graphs"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in wormhole_ledger wormhole_usage wormhole_dns wormhole_fence wormhole_fd; do
  [ -e "$ROOT/target/fluxor/aarch64/modules/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

W="$(mktemp -d /tmp/nc-whmod-e2e-XXXXXX)"
trap 'rm -rf "$W"' EXIT
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null || true; exit 1; }
mkdir -p "$W/in"
(cd "$ROOT/tests/harness" && NC_WH_OUT="$W/in" cargo test -q --test wormhole_portfolio \
  -- --ignored --exact emit_module_inputs_for_the_e2e) >"$W/emit.log" 2>&1 \
  || { tail -20 "$W/emit.log"; echo "FAIL: the harness did not emit the inputs"; exit 1; }

# fresh <name>: a new store and work dir for one graph.
fresh() { D="$W/$1"; mkdir -p "$D"; : >"$D/store.log"; }
. "$ROOT/scripts/support/np.sh"
# build <graph> [k=v ...]: the graph with its params' defaults replaced.
build() {
  local g="$1"; shift
  python3 - "$g" "$D/graph.yaml" "$@" <<'PY'
import sys
src, dst, kv = sys.argv[1], sys.argv[2], sys.argv[3:]
t = open(src).read()
for p in kv:
    k, v = p.split("=", 1)
    t = t.replace("${param:%s}" % k, '"%s"' % v)
# Every other param takes its default, and the declarations go: a graph
# that still declares params is built at deploy time, not here.
import re
m = re.search(r"^params:\n((?:  .*\n|\n)*?)(?=^\S)", t, re.M)
if m:
    block = m.group(1)
    for name, body in re.findall(r"^  (\w+):\n((?:    .*\n)*)", block, re.M):
        d = re.search(r"^    default: (.*)$", body, re.M)
        if d:
            t = t.replace("${param:%s}" % name, d.group(1).strip())
    t = t[:m.start()] + t[m.end():]
open(dst, "w").write(t)
PY
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null \
    || fail "$(basename "$g") did not build"
}
# run <seconds> [stdin-file]
run() {
  local input="${2:-/dev/null}"
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$1" "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" <"$input" >"$D/out.bin" 2>>"$D/run.log" || true
}
# rowhex <key>: the live value as hex, or empty.
rowhex() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = 0; out = None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want: out = data[p+15+kl:p+15+kl+vl] if op == 1 else None
    p += 15 + kl + vl
sys.stdout.write(out.hex() if out is not None else "")
PY
}

echo "== 1. wormhole_ledger: three FailoverExplain entries, one row each, verbatim =="
fresh ledger; build "$G/wh-ledger.yaml"; run 5 "$W/in/ledger.bin"
python3 - "$W/in/ledger.bin" >"$D/recs.hex" <<'PY'
import struct, sys
b = open(sys.argv[1], "rb").read(); at = 0
while at < len(b):
    n = 36 + struct.unpack_from("<I", b, at + 8)[0] + 32
    print(b[at:at+n].hex()); at += n
PY
i=0
while read -r key; do
  i=$((i + 1))
  want="$(sed -n "${i}p" "$D/recs.hex")"
  [ "$(rowhex "$key")" = "$want" ] || fail "ledger row $key is not entry $i verbatim"
done <"$W/in/ledger.keys"
[ "$i" = 3 ] || fail "expected three ledger keys"
echo "   three rows, each its entry's bytes"
# A replay after a restart leaves every row as it is.
before="$(wc -c <"$D/store.log")"; run 4 "$W/in/ledger.bin"
[ "$(wc -c <"$D/store.log")" = "$before" ] || fail "a replayed ledger rewrote a row"
echo "   a replay rewrote nothing"

echo "== 2. wormhole_usage: five records, one repeated → the stream's row, applied once =="
fresh usage; build "$G/wh-usage.yaml"; run 5 "$W/in/usage.bin"
R="$(np_last "$(cat "$W/in/usage.key")")"
case "$R" in "seq=5;"*";connections=5;"*) ;; *) fail "the usage row: '$R'" ;; esac
echo "   $R"

echo "== 3. wormhole_dns: the signed projection written; its repeat and a forgery not =="
fresh dns
# The emitter writes one line without a newline: `read` then answers 1.
read -r ISS KID PUB <"$W/in/ksdn.params" || [ -n "${PUB:-}" ]
build "$G/wh-dns.yaml" "issuer=$ISS" "kid=$KID" "public=$PUB"; run 5 "$W/in/ksdn.bin"
[ "$(rowhex "$(cat "$W/in/ksdn.key")")" = "$(cat "$W/in/ksdn.hex")" ] || fail "the /wh-dns row is not the signed record"
writes="$(python3 -c "
import struct,sys
d=open('$D/store.log','rb').read();p=0;n=0
while p+15<=len(d):
    r,o,kl,vl=struct.unpack('<QBHI',d[p:p+15]);n+=d[p+15:p+15+kl].startswith(b'/wh-dns/');p+=15+kl+vl
print(n)")"
[ "$writes" = 1 ] || fail "wormhole_dns wrote $writes rows, not one"
echo "   one row, the signed record; the repeat left, the forgery refused"

echo "== 4. wormhole_fence: a confirmation only on proof =="
fresh fence
np_put /wh-fence/7/custody "gen=3"
np_put /wh-fence/7/link/transit-a "down=1;ack=1"
np_put /wh-fence/7/link/transit-b "down=1;ack=0"
build "$G/wh-fence.yaml"; run 3
[ ! -s "$D/out.bin" ] || fail "a fence was confirmed with a cut unacknowledged"
[ -z "$(np_last /wh-fence/7/confirmed)" ] || fail "a confirmation row without proof"
np_put /wh-fence/7/link/transit-b "down=1;ack=1"
run 3
python3 - "$D/out.bin" <<'PY' || fail "the confirmation record"
import struct, sys, hashlib
b = open(sys.argv[1], "rb").read()
assert b[:4] == b"WHR1", b[:8]
kind = struct.unpack_from("<H", b, 4)[0]; n = struct.unpack_from("<I", b, 8)[0]
rec = b[:36 + n + 32]
assert kind == 0xB4, hex(kind)
assert hashlib.sha256(rec[:-32]).digest() == rec[-32:]
p = rec[36:36 + n]
region = struct.unpack_from("<I", p, 0)[0]
assert region == 7, region
assert len(b) == len(rec), "confirmed once"
PY
case "$(np_last /wh-fence/7/confirmed)" in "kind=upstream;gen=3;at="*) ;; *) fail "the confirmed row" ;; esac
echo "   unproven while one cut was unacknowledged; confirmed once under custody 3"

echo "== 5. wormhole_fd: preflight, observation and placement against the store =="
fresh fd
node() { # node <name> <zone> <alloc> <voter>
  np_put "/nodes/$1" "{\"metadata\":{\"name\":\"$1\",\"labels\":{\"topology.kubernetes.io/zone\":\"$2\",\"nanocloud.io/store-voter\":\"$4\"}},\"spec\":{},\"status\":{\"allocatable\":{\"cpu\":\"$3\"},\"conditions\":[{\"type\":\"MemoryPressure\",\"status\":\"False\"},{\"type\":\"Ready\",\"status\":\"True\"}]}}"
}
node a1 zone-a 4 true
node a2 zone-a 2000m false
node b1 zone-b 8 true
node b2 zone-b 8 true
np_put /pods/default/p1 '{"metadata":{"name":"p1"},"spec":{"nodeName":"a1","containers":[{"name":"c","resources":{"requests":{"cpu":"1500m"}}},{"name":"d"}]}}'
np_put /pods/default/p2 '{"metadata":{"name":"p2"},"spec":{"nodeName":"b1","containers":[{"name":"c","resources":{"requests":{"cpu":"2"}}}]}}'
np_put /wh-fd/zone-a/facts "endpoints=4;rgen=11;standbys=1;route=1;keys=1;sovereignty=1;fence=1;conflicting=0;epochs=3,4"
np_put /wh-fd/zone-a/health "error_ppm=12;breaks=0;pinned=2;alarm=0"
build "$G/wh-fd.yaml"; run 5 "$W/in/fd.bin"
python3 - "$D/out.bin" <<'PY' || fail "the preflight and the observation"
import struct, sys, hashlib
b = open(sys.argv[1], "rb").read(); at = 0; recs = []
while at < len(b):
    n = struct.unpack_from("<I", b, at + 8)[0]
    rec = b[at:at + 36 + n + 32]
    assert hashlib.sha256(rec[:-32]).digest() == rec[-32:]
    recs.append((struct.unpack_from("<H", rec, 4)[0], struct.unpack_from("<Q", rec, 12)[0], rec[36:36 + n]))
    at += len(rec)
assert [r[0] for r in recs] == [0xA2, 0xA3], [hex(r[0]) for r in recs]
p = recs[0][2]
assert p[0] == 1 and p[1] == 1, "evacuate, quorum outside (2 of 3 voters)"
assert struct.unpack_from("<I", p, 8)[0] == 4, "scope"
spare, demand, rgen = struct.unpack_from("<QQQ", p, 16)
assert (spare, demand, rgen) == (14000, 1500, 11), (spare, demand, rgen)
assert struct.unpack_from("<QQ", p, 40) == (3, 4)
o = recs[1][2]
assert recs[1][1] == 7 and struct.unpack_from("<Q", o, 0)[0] == 7, "dial generation 7"
ppm, breaks, pinned = struct.unpack_from("<III", o, 8)
assert (ppm, breaks, pinned, o[20]) == (12, 0, 2, 0)
assert struct.unpack_from("<Q", o, 24)[0] == 1500, "the zone's demand"
cap = struct.unpack_from("<4Q", o, 40)
assert cap[:2] == (4500, 14000), cap
PY
for n in a1 a2; do
  case "$(np_last "/nodes/$n")" in *'"unschedulable":true'*'"wormhole.nanocloud.io/evacuated-by":"0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"'* | *'"wormhole.nanocloud.io/evacuated-by":"0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"'*'"unschedulable":true'*) ;;
    *) fail "node $n was not cordoned and marked: $(np_last "/nodes/$n")" ;; esac
done
case "$(np_last /nodes/b1)" in *unschedulable*) fail "a node outside the zone was patched" ;; esac
echo "   preflight (spare 14000m outside, demand 1500m, quorum), observation (gen 7, capacity per arm), zone-a cordoned"
echo "PASS"
