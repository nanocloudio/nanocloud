#!/usr/bin/env bash
# Every baked Chronicle param must still match the .uproc it was compiled from.
#
# A compiled decision carried in a graph has no link back to its source. Edit
# the .uproc and the graph keeps the old bytes — and a stale decision does NOT
# error, it decides something else. Moving two key parts between fields, say,
# leaves a graph probing `/replicasets.apps/default` instead of
# `.../default/web`: every owner looks absent and a live-owned pod is
# collected, visible only to a test that asserts that exact case.
#
# So every graph carrying a param declares its provenance:
#
#     # chronicle-source: <path-to-.uproc> <entry>
#     decision: "<hex>"
#
# and this recompiles each one and compares. The param stays baked in the graph
# because a deb ships no compiler; this is what stops it drifting silently.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

checked=0
fails=0
while IFS= read -r graph; do
  # Pair each `chronicle-source:` marker with the param line that follows —
  # `decision:` OR `ir_stages:`, whichever comes FIRST. Matching only
  # `decision:` was not merely incomplete: a marker sitting above an
  # `ir_stages:` param would scan past it and pair with the NEXT node's
  # decision, so one param went unchecked and another was checked against the
  # wrong source.
  python3 - "$graph" > /tmp/nc-drift-$$.txt <<'PY'
import re, sys
lines = open(sys.argv[1]).read().split("\n")
for i, l in enumerate(lines):
    m = re.search(r'chronicle-source:\s*(\S+)\s+(\S+)', l)
    if not m:
        continue
    for j in range(i + 1, min(i + 12, len(lines))):
        d = re.search(r'(decision|ir_stages):\s*"([0-9a-f]+)"', lines[j])
        if d:
            print(m.group(1), m.group(2), d.group(1), d.group(2))
            break
PY
  while read -r uproc entry kind baked; do
    [ -n "$baked" ] || continue
    checked=$((checked + 1))
    if [ "$kind" = "ir_stages" ]; then
      fresh="$(nc_stages "$ROOT/$uproc" "$entry")"
    else
      fresh="$(nc_decision "$ROOT/$uproc" "$entry")"
    fi
    if [ -z "$fresh" ]; then
      echo "   FAIL $graph: could not compile $uproc $entry"; fails=$((fails + 1)); continue
    fi
    if [ "$fresh" != "$baked" ]; then
      echo "   FAIL $graph: the baked param is STALE against $uproc $entry"
      echo "        recompile and update the graph — a stale decision does not error, it decides something else"
      fails=$((fails + 1)); continue
    fi
    echo "   ok   $(basename "$graph") <- $(basename "$uproc") $entry ($kind)"
  done < /tmp/nc-drift-$$.txt
  rm -f /tmp/nc-drift-$$.txt
done < <(grep -rl 'chronicle-source:' "$ROOT/packaging" 2>/dev/null)

[ "$checked" -gt 0 ] || { echo "FAIL: no chronicle-source markers found — the guard would pass vacuously"; exit 1; }
[ "$fails" -eq 0 ] || { echo "FAIL: $fails baked param(s) drifted"; exit 1; }
echo "== E2E green: all $checked baked Chronicle param(s) match their source =="
