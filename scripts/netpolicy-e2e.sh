#!/usr/bin/env bash
# NetworkPolicy as params over net.policy (packaging/debian/fluxor-netpolicy.yaml).
# Policies select pods; each selection expands into rule and isolation rows;
# net_apply replaces the `netpolicy` table from them. The platform runs in DRY
# RUN: the rendered nftables script is checked, then handed to a real kernel in
# a private network namespace.
#
#   /np-sel/<ns>/<pol>/<pod>                 = the policy's spec + the pod's address
#   /np-rows/netpolicy/<ns>/<pol>/<pod>/<x>  = kind=allow|isolate;dir=…;subj=…;…
#   /np-status/netpolicy                     = enforced=0|1;rules=<n>;generation=<g>
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-netpolicy.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/net_apply.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-np-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; echo "--- rows ---"; np_keys /np-rows/netpolicy/ 2>/dev/null; echo "--- rendered ---"; cat "$D/np/netpolicy.nft" 2>/dev/null; tail -10 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/np.sh"

echo "== 1. build =="
np_build

echo "== 2. an ingress policy (app=web), an egress policy (app=db), a policy with a selector peer, a matchExpressions policy; pods =="
np_put "/networkpolicies/default/web-allow" '{"metadata":{"name":"web-allow"},"spec":{"podSelector":{"matchLabels":{"app":"web"}},"policyTypes":["Ingress"],"ingress":[{"from":[{"ipBlock":{"cidr":"10.0.0.0/8"}}],"ports":[{"protocol":"TCP","port":80},{"protocol":"UDP","port":53}]}]}}'
np_put "/networkpolicies/default/db-egress" '{"metadata":{"name":"db-egress"},"spec":{"podSelector":{"matchLabels":{"app":"db"}},"policyTypes":["Egress"],"egress":[{"to":[{"ipBlock":{"cidr":"10.0.0.0/8"}}],"ports":[{"port":5432}]}]}}'
np_put "/networkpolicies/default/api-peers" '{"metadata":{"name":"api-peers"},"spec":{"podSelector":{"matchLabels":{"app":"api"}},"ingress":[{"from":[{"podSelector":{"matchLabels":{"app":"web"}}}]}]}}'
# matchExpressions only: app In (db, cache) AND tier DoesNotExist — selects
# cache-1 (no tier label), NOT db-1 (tier=data): upstream LabelSelector
# semantics, where DoesNotExist holds exactly for a pod lacking the key.
np_put "/networkpolicies/default/expr-sel" '{"metadata":{"name":"expr-sel"},"spec":{"podSelector":{"matchExpressions":[{"key":"app","operator":"In","values":["db","cache"]},{"key":"tier","operator":"DoesNotExist"}]},"policyTypes":["Ingress"],"ingress":[{"from":[{"ipBlock":{"cidr":"192.168.0.0/16"}}]}]}}'
# A peer with BOTH selectors: pods labelled role=monitor in namespaces
# labelled team=ops — the two ANDed within one peer, as upstream.
np_put "/networkpolicies/default/metrics-in" '{"metadata":{"name":"metrics-in"},"spec":{"podSelector":{"matchLabels":{"app":"db"}},"policyTypes":["Ingress"],"ingress":[{"from":[{"namespaceSelector":{"matchLabels":{"team":"ops"}},"podSelector":{"matchLabels":{"role":"monitor"}}}],"ports":[{"protocol":"TCP","port":9100}]}]}}'
np_put "/namespaces/default" '{"metadata":{"name":"default","labels":{"kubernetes.io/metadata.name":"default"}}}'
np_put "/namespaces/tools"   '{"metadata":{"name":"tools","labels":{"kubernetes.io/metadata.name":"tools","team":"ops"}}}'
np_put "/pods/tools/ops-1"   '{"metadata":{"name":"ops-1","labels":{"role":"monitor"}},"status":{"podIP":"10.0.1.3"}}'
np_put "/pods/tools/ops-2"   '{"metadata":{"name":"ops-2","labels":{"role":"batch"}},"status":{"podIP":"10.0.1.4"}}'
np_put "/pods/default/mon-1" '{"metadata":{"name":"mon-1","labels":{"role":"monitor"}},"status":{"podIP":"10.0.0.13"}}'
# ipBlock with except: 172.16.0.0/12 less 172.16.5.0/24, rendered as the
# twelve ranges that remain — never the whole /12.
np_put "/networkpolicies/default/vault-in" '{"metadata":{"name":"vault-in"},"spec":{"podSelector":{"matchLabels":{"app":"vault"}},"policyTypes":["Ingress"],"ingress":[{"from":[{"ipBlock":{"cidr":"172.16.0.0/12","except":["172.16.5.0/24"]}}]}]}}'
np_put "/pods/default/vault-1" '{"metadata":{"name":"vault-1","labels":{"app":"vault"}},"status":{"podIP":"10.0.0.21"}}'
np_put "/pods/default/web-1"   '{"metadata":{"name":"web-1","labels":{"app":"web"}},"status":{"podIP":"10.0.0.5"}}'
np_put "/pods/default/db-1"    '{"metadata":{"name":"db-1","labels":{"app":"db","tier":"data"}},"status":{"podIP":"10.0.0.9"}}'
np_put "/pods/default/api-1"   '{"metadata":{"name":"api-1","labels":{"app":"api"}},"status":{"podIP":"10.0.0.11"}}'
np_put "/pods/default/cache-1" '{"metadata":{"name":"cache-1","labels":{"app":"cache"}},"status":{"podIP":"10.0.0.7"}}'
np_run 6

echo "== 3. selections and rows =="
[ "$(np_keys /np-sel/ | tr '\n' ' ')" = "/np-sel/default/api-peers/api-1 /np-sel/default/db-egress/db-1 /np-sel/default/expr-sel/cache-1 /np-sel/default/metrics-in/db-1 /np-sel/default/vault-in/vault-1 /np-sel/default/web-allow/web-1 " ] \
  || fail "selections: $(np_keys /np-sel/ | tr '\n' ' ')"
[ "$(np_keys /np-rows/netpolicy/ | tr '\n' ' ')" = "/np-rows/netpolicy/default/api-peers/api-1/in-0 /np-rows/netpolicy/default/api-peers/api-1/in-iso /np-rows/netpolicy/default/api-peers/api-1/peer~api-peers~in-0~0~default~web-1 /np-rows/netpolicy/default/db-egress/db-1/out-0 /np-rows/netpolicy/default/db-egress/db-1/out-iso /np-rows/netpolicy/default/expr-sel/cache-1/in-0 /np-rows/netpolicy/default/expr-sel/cache-1/in-iso /np-rows/netpolicy/default/metrics-in/db-1/in-0 /np-rows/netpolicy/default/metrics-in/db-1/in-iso /np-rows/netpolicy/default/metrics-in/db-1/peer~metrics-in~in-0~0~tools~ops-1 /np-rows/netpolicy/default/vault-in/vault-1/in-0 /np-rows/netpolicy/default/vault-in/vault-1/in-iso /np-rows/netpolicy/default/web-allow/web-1/in-0 /np-rows/netpolicy/default/web-allow/web-1/in-iso " ] \
  || fail "rows: $(np_keys /np-rows/netpolicy/ | tr '\n' ' ')"
echo "   web-1 in, db-1 out, api-1 in; cache-1 by matchExpressions (db-1 has tier, so not)"

echo "== 4. the rendered table =="
RS="$(cat "$D/np/netpolicy.nft" 2>/dev/null)" || fail "no netpolicy table rendered"
grep -q "ip daddr 10.0.0.5 ip saddr 10.0.0.0/8 tcp dport 80 accept" <<<"$RS" || fail "web: tcp 80 from 10/8"
grep -q "ip daddr 10.0.0.5 ip saddr 10.0.0.0/8 udp dport 53 accept" <<<"$RS" || fail "web: udp 53 from 10/8"
grep -q "ip daddr 10.0.0.5 drop" <<<"$RS"                                    || fail "web: isolated for ingress"
grep -q "ip saddr 10.0.0.9 ip daddr 10.0.0.0/8 tcp dport 5432 accept" <<<"$RS" || fail "db: tcp 5432 to 10/8 (protocol defaulted)"
grep -q "ip saddr 10.0.0.9 drop" <<<"$RS"                                    || fail "db: isolated for egress"
grep -q "ip daddr 10.0.0.9 drop" <<<"$RS"                                    || fail "db: isolated for ingress by metrics-in"
grep -q "ip daddr 10.0.0.11 drop" <<<"$RS"                                   || fail "api: isolated for ingress"
grep -q "ip daddr 10.0.0.11 ip saddr 10.0.0.5 accept" <<<"$RS"               || fail "api: its selector peer (app=web) is web-1's address"
grep -q "ip daddr 10.0.0.11 ip saddr 10.0.0.13" <<<"$RS"                     && fail "api: mon-1 is not app=web"
grep -q "ip daddr 10.0.0.9 ip saddr 10.0.1.3 tcp dport 9100 accept" <<<"$RS" || fail "db: ops-1 (role=monitor in a team=ops namespace) on 9100"
grep -q "ip saddr 10.0.1.4" <<<"$RS"                                         && fail "db: ops-2 is not role=monitor"
grep -q "ip daddr 10.0.0.9 ip saddr 10.0.0.13" <<<"$RS"                      && fail "db: mon-1 is role=monitor but its namespace is not team=ops"
grep -q "ip daddr 10.0.0.7 ip saddr 192.168.0.0/16 accept" <<<"$RS"          || fail "cache-1: allowed from 192.168/16 (selected by matchExpressions)"
grep -q "ip daddr 10.0.0.7 drop" <<<"$RS"                                    || fail "cache-1: isolated for ingress"
grep -q "ip daddr 10.0.0.9 ip saddr 192.168.0.0/16" <<<"$RS"                 && fail "db-1 has tier=data: DoesNotExist must not select it"
grep -q "ip daddr 10.0.0.21 ip saddr 172.16.4.0/24 accept" <<<"$RS"           || fail "vault: 172.16.4.0/24 remains of the /12"
grep -q "ip daddr 10.0.0.21 ip saddr 172.24.0.0/13 accept" <<<"$RS"          || fail "vault: 172.24.0.0/13 remains of the /12"
grep -q "ip saddr 172.16.0.0/12" <<<"$RS"                                    && fail "vault: the /12 must not be allowed whole — 172.16.5.0/24 is excepted"
grep -q "172.16.5.0" <<<"$RS"                                                && fail "vault: the excepted range is allowed"
[ "$(grep -c "ip daddr 10.0.0.21 ip saddr 172\." <<<"$RS")" = 12 ]         || fail "vault: the /12 less a /24 is exactly 12 ranges"
grep -q "ct state established,related accept" <<<"$RS"                     || fail "replies to an isolated pod's own connections must pass"
case "$(np_last /np-status/netpolicy)" in enforced=0\;rules=24\;*) ;; *) fail "status: '$(np_last /np-status/netpolicy)'" ;; esac
echo "   allows before isolations; status: $(np_last /np-status/netpolicy)"
np_kernel_check netpolicy

echo "== 5. web-1 is relabelled out; the db policy narrows to no rules; api-1 is deleted =="
np_put "/pods/default/web-1" '{"metadata":{"name":"web-1","labels":{"app":"other"}},"status":{"podIP":"10.0.0.5"}}'
np_put "/networkpolicies/default/db-egress" '{"metadata":{"name":"db-egress"},"spec":{"podSelector":{"matchLabels":{"app":"db"}},"policyTypes":["Egress"]}}'
np_put "/pods/default/api-1" "-DELETE-"
np_run 6
[ "$(np_keys /np-rows/netpolicy/ | tr '\n' ' ')" = "/np-rows/netpolicy/default/db-egress/db-1/out-iso /np-rows/netpolicy/default/expr-sel/cache-1/in-0 /np-rows/netpolicy/default/expr-sel/cache-1/in-iso /np-rows/netpolicy/default/metrics-in/db-1/in-0 /np-rows/netpolicy/default/metrics-in/db-1/in-iso /np-rows/netpolicy/default/metrics-in/db-1/peer~metrics-in~in-0~0~tools~ops-1 /np-rows/netpolicy/default/vault-in/vault-1/in-0 /np-rows/netpolicy/default/vault-in/vault-1/in-iso " ] \
  || fail "after: $(np_keys /np-rows/netpolicy/ | tr '\n' ' ')"
RS="$(cat "$D/np/netpolicy.nft")"
grep -q "10.0.0.5" <<<"$RS" && fail "web-1 is no longer selected"
grep -q "5432" <<<"$RS" && fail "db's rule was removed"
grep -q "ip saddr 10.0.0.9 drop" <<<"$RS" || fail "db stays isolated: a policy with no egress rules denies all egress"
echo "   relabel, narrowing and deletion all reach the table"

echo "== 5b. selection FAILS CLOSED: many labels, and a selector past the term cap =="
# A pod's label count must not decide whether it is isolated: its labels no
# longer ride in the record (each term is paired with the pod's value at the
# source), so 30 labels select exactly like one.
np_put "/pods/default/many-1" '{"metadata":{"name":"many-1","labels":{"app":"many","l00":"v","l01":"v","l02":"v","l03":"v","l04":"v","l05":"v","l06":"v","l07":"v","l08":"v","l09":"v","l10":"v","l11":"v","l12":"v","l13":"v","l14":"v","l15":"v","l16":"v","l17":"v","l18":"v","l19":"v","l20":"v","l21":"v","l22":"v","l23":"v","l24":"v","l25":"v","l26":"v","l27":"v","l28":"v"}},"status":{"podIP":"10.0.0.30"}}'
np_put "/networkpolicies/default/many-sel" '{"metadata":{"name":"many-sel"},"spec":{"podSelector":{"matchLabels":{"app":"many"}},"policyTypes":["Ingress"]}}'
# A selector past `terms_max` cannot be evaluated whole: the pod it might
# select is SELECTED (isolated) rather than left open. wide-1 matches only
# one of the nine terms, and is isolated all the same.
np_put "/pods/default/wide-1" '{"metadata":{"name":"wide-1","labels":{"w0":"1"}},"status":{"podIP":"10.0.0.31"}}'
np_put "/networkpolicies/default/wide-sel" '{"metadata":{"name":"wide-sel"},"spec":{"podSelector":{"matchLabels":{"w0":"1","w1":"1","w2":"1","w3":"1","w4":"1","w5":"1","w6":"1","w7":"1","w8":"1"}},"policyTypes":["Ingress"]}}'
np_run 6
np_keys /np-sel/ | grep -qx "/np-sel/default/many-sel/many-1" || fail "a pod with 30 labels was not selected: $(np_keys /np-sel/ | tr '\n' ' ')"
np_keys /np-sel/ | grep -qx "/np-sel/default/wide-sel/wide-1" || fail "a selector past the term cap left its pod open"
RS="$(cat "$D/np/netpolicy.nft")"
grep -q "ip daddr 10.0.0.30 drop" <<<"$RS" || fail "many-1 is not isolated"
grep -q "ip daddr 10.0.0.31 drop" <<<"$RS" || fail "wide-1 is not isolated"
np_put "/networkpolicies/default/many-sel" "-DELETE-"
np_put "/networkpolicies/default/wide-sel" "-DELETE-"
echo "   30 labels: selected; a 9-term selector: isolates (fails closed)"

echo "== 6. the policy is deleted: nothing left =="
np_put "/networkpolicies/default/db-egress" "-DELETE-"
np_put "/networkpolicies/default/web-allow" "-DELETE-"
np_put "/networkpolicies/default/api-peers" "-DELETE-"
np_put "/networkpolicies/default/expr-sel" "-DELETE-"
np_put "/networkpolicies/default/metrics-in" "-DELETE-"
np_put "/networkpolicies/default/vault-in" "-DELETE-"
np_run 8
[ -z "$(np_keys /np-rows/netpolicy/)" ] || fail "rows remain: $(np_keys /np-rows/netpolicy/ | tr '\n' ' ')"
[ -z "$(np_keys /np-sel/)" ] || fail "selections remain: $(np_keys /np-sel/ | tr '\n' ' ')"
for lvl in /np-rule/ /np-peer/ /np-peerns/ /np-peerip/; do
  [ -z "$(np_keys $lvl)" ] || fail "$lvl rows remain: $(np_keys $lvl | tr '\n' ' ')"
done
grep -q "drop" "$D/np/netpolicy.nft" && fail "the table still drops"
echo "   no rows, no selections, an empty table"

echo "== E2E green: NetworkPolicy as params — selections to rows to a net.policy table the kernel accepts =="
