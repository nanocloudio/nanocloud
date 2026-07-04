#!/usr/bin/env bash
# Recompile every `# chronicle-source: <uproc> <entry>` param in the given
# graphs and rewrite the `decision:` line that follows it. The inverse of
# chronicle-param-drift-e2e.sh: that one refuses a stale param, this one
# refreshes it. Run after editing a .uproc.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
for graph in "$@"; do
  python3 - "$graph" "$ROOT" <<'PY'
import re, subprocess, sys
graph, root = sys.argv[1], sys.argv[2]
lines = open(graph).read().split("\n")
changed = 0
for i, l in enumerate(lines):
    m = re.search(r'chronicle-source:\s*(\S+)\s+(\S+)', l)
    if not m:
        continue
    fresh = subprocess.run(["bash", "-c", f'. "{root}/scripts/fluxor-env.sh"; . "{root}/scripts/chronicle-params.sh"; nc_decision "{root}/{m.group(1)}" "{m.group(2)}"'],
                           capture_output=True, text=True).stdout.strip()
    if not fresh:
        print(f"FAIL: {m.group(1)} {m.group(2)} did not compile"); sys.exit(1)
    for j in range(i + 1, min(i + 12, len(lines))):
        d = re.match(r'(\s*decision:\s*")([0-9a-f]+)(".*)', lines[j])
        if d:
            if d.group(2) != fresh:
                lines[j] = d.group(1) + fresh + d.group(3); changed += 1
            break
open(graph, "w").write("\n".join(lines))
print(f"   {graph}: {changed} param(s) re-baked")
PY
done
