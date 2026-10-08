#!/usr/bin/env bash
# Service proxying as params over net.policy (packaging/debian/fluxor-proxy.yaml).
# Services and their endpoint slices become rows under /np-rows/proxy/, and
# net_apply replaces the `proxy` table from them. The platform runs in DRY RUN:
# the rendered nftables script is checked, then handed to a real kernel in a
# private network namespace; the status says the table is NOT enforced, as a
# dry run must.
#
#   /services/<ns>/<svc>             = Service JSON (clusterIP, ports[])
#   /endpointslices/<ns>/<svc>/<pod> = <ready backend address>
#   /np-rows/proxy/<ns>/<svc>/<pod>  = kind=dnat;vip=…;vports=…;tports=…;protos=…;backend=…
#   /np-status/proxy                 = enforced=0|1;rules=<n>;generation=<g>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-proxy.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/net_apply.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-px-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; echo "--- rendered ---"; cat "$D/np/proxy.nft" 2>/dev/null; tail -10 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/np.sh"

echo "== 1. build =="
np_build

echo "== 2. four Services: two backends, one backend on two ports, headless, no backends =="
np_put "/services/default/web"      '{"metadata":{"name":"web"},"spec":{"selector":{"app":"web"},"clusterIP":"10.96.0.10","ports":[{"port":80,"targetPort":8080}]}}'
np_put "/endpointslices/default/web/web-1" "10.0.0.1"
np_put "/endpointslices/default/web/web-2" "10.0.0.2"
np_put "/services/default/multi"    '{"metadata":{"name":"multi"},"spec":{"clusterIP":"10.96.0.20","ports":[{"port":443,"targetPort":8443},{"port":53,"protocol":"UDP"}]}}'
np_put "/endpointslices/default/multi/m-1" "10.0.0.3"
np_put "/services/default/headless" '{"metadata":{"name":"headless"},"spec":{"clusterIP":"None","ports":[{"port":80}]}}'
np_put "/endpointslices/default/headless/h-1" "10.0.0.4"
np_put "/services/default/noep"     '{"metadata":{"name":"noep"},"spec":{"clusterIP":"10.96.0.30","ports":[{"port":80}]}}'
np_run 5

echo "== 3. the rows =="
ROWS="$(np_keys /np-rows/proxy/)"
[ "$(printf '%s\n' "$ROWS" | grep -c .)" = 3 ] || fail "want 3 rows, got: $ROWS"
[ "$(np_last /np-rows/proxy/default/multi/m-1)" = "kind=dnat;vip=10.96.0.20;vports=443,53;tports=8443,-;protos=-,UDP;backend=10.0.0.3" ] \
  || fail "multi row: '$(np_last /np-rows/proxy/default/multi/m-1)'"
echo "   web-1, web-2, m-1 — headless and backend-less Services have none"

echo "== 4. the rendered table =="
RS="$(cat "$D/np/proxy.nft" 2>/dev/null)" || fail "no proxy table rendered"
grep -q "ip daddr 10.96.0.10 tcp dport 80 dnat ip to numgen random mod 2 map { 0 : 10.0.0.1 . 8080, 1 : 10.0.0.2 . 8080 }" <<<"$RS" \
  || fail "web: two balanced backends"
grep -q "ip daddr 10.96.0.20 tcp dport 443 dnat to 10.0.0.3:8443" <<<"$RS" || fail "multi: 443 -> 8443"
grep -q "ip daddr 10.96.0.20 udp dport 53 dnat to 10.0.0.3:53" <<<"$RS"   || fail "multi: udp 53, targetPort defaulted"
grep -q "10.96.0.30" <<<"$RS" && fail "noep has no backends and no rule"
[ "$(np_last /np-status/proxy)" = "enforced=0;rules=4;generation=$(np_last /np-status/proxy | sed 's/.*generation=//')" ] \
  || fail "status: '$(np_last /np-status/proxy)' (a dry run enforces nothing, and says so)"
echo "   balanced, multi-port, UDP; status: $(np_last /np-status/proxy)"
np_kernel_check proxy

echo "== 5. a backend leaves its slice; a Service is deleted =="
np_put "/endpointslices/default/web/web-2" "-DELETE-"
np_put "/services/default/multi" "-DELETE-"
np_run 5
ROWS="$(np_keys /np-rows/proxy/)"
[ "$ROWS" = "/np-rows/proxy/default/web/web-1" ] || fail "after: want only web-1, got: $ROWS"
RS="$(cat "$D/np/proxy.nft")"
grep -q "ip daddr 10.96.0.10 tcp dport 80 dnat to 10.0.0.1:8080" <<<"$RS" || fail "web now has one backend"
grep -q "10.96.0.20" <<<"$RS" && fail "multi is gone"
echo "   the table follows: web -> 10.0.0.1 only; multi gone"

echo "== E2E green: Service proxying as params — rows to a net.policy table the kernel accepts =="
