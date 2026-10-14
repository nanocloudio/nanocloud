#!/usr/bin/env bash
# Recompile every `# chronicle-source: <uproc> <entry>` param in the given
# graphs and rewrite the `decision: {hex: "…"}` line that follows it (a
# program param is a `blob`, written as its inline hex entry). The inverse of
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
    r = re.search(r'rd-source:\s*(\S+)', l)
    if r:
        fresh = subprocess.run(["python3", f"{root}/scripts/rdasm.py", f"{root}/{r.group(1)}"],
                               capture_output=True, text=True).stdout.strip()
        if not fresh:
            print(f"FAIL: {r.group(1)} did not assemble"); sys.exit(1)
        for j in range(i + 1, min(i + 12, len(lines))):
            d = re.match(r'(\s*decode:\s*\{hex:\s*")([0-9a-f]*)("\}.*)', lines[j])
            if d:
                if d.group(2) != fresh:
                    lines[j] = d.group(1) + fresh + d.group(3); changed += 1
                break
        continue
    # `agg-source: <script> <args…>`: an aggregation `ir_def` built by a
    # script (the window and its programs), not by one .uproc entry.
    a = re.search(r'agg-source:\s*(\S+)((?:\s+\S+)*)', l)
    if a:
        fresh = subprocess.run(["bash", "-c", f'. "{root}/scripts/fluxor-env.sh"; bash "{root}/{a.group(1)}" {a.group(2)}'],
                               capture_output=True, text=True).stdout.strip()
        if not fresh:
            print(f"FAIL: {a.group(1)} did not build"); sys.exit(1)
        for j in range(i + 1, min(i + 12, len(lines))):
            d = re.match(r'(\s*ir_def:\s*\{hex:\s*")([0-9a-f]*)("\}.*)', lines[j])
            if d:
                if d.group(2) != fresh:
                    lines[j] = d.group(1) + fresh + d.group(3); changed += 1
                break
        continue
    m = re.search(r'chronicle-source:\s*(\S+)\s+(\S+)', l)
    if not m:
        continue
    # Which key follows decides which compiler answer to ask for: a decision
    # container or an ir_stages one.
    # `count_where` is a decision container too, a store_effect's per-child
    # predicate.
    key = "decision"
    for j in range(i + 1, min(i + 12, len(lines))):
        k = re.match(r'\s*(decision|ir_stages|count_where):\s*\{hex:', lines[j])
        if k:
            key = k.group(1)
            break
    fn = "nc_stages" if key == "ir_stages" else "nc_decision"
    fresh = subprocess.run(["bash", "-c", f'. "{root}/scripts/fluxor-env.sh"; . "{root}/scripts/chronicle-params.sh"; {fn} "{root}/{m.group(1)}" "{m.group(2)}"'],
                           capture_output=True, text=True).stdout.strip()
    if not fresh:
        print(f"FAIL: {m.group(1)} {m.group(2)} did not compile"); sys.exit(1)
    for j in range(i + 1, min(i + 12, len(lines))):
        d = re.match(r'(\s*' + key + r':\s*\{hex:\s*")([0-9a-f]*)("\}.*)', lines[j])
        if d:
            if d.group(2) != fresh:
                lines[j] = d.group(1) + fresh + d.group(3); changed += 1
            # A container with map stages carries its kinds beside it.
            if key == "ir_stages":
                kinds = subprocess.run(["bash", "-c", f'. "{root}/scripts/fluxor-env.sh"; . "{root}/scripts/chronicle-params.sh"; nc_stage_kinds "{root}/{m.group(1)}" "{m.group(2)}"'],
                                       capture_output=True, text=True).stdout.strip()
                nxt = lines[j + 1] if j + 1 < len(lines) else ""
                k = re.match(r'(\s*stage_kinds:\s*\{hex:\s*")([0-9a-f]*)("\}.*)', nxt)
                if kinds and k:
                    if k.group(2) != kinds:
                        lines[j + 1] = k.group(1) + kinds + k.group(3); changed += 1
                elif kinds:
                    indent = re.match(r'(\s*)', lines[j]).group(1)
                    lines.insert(j + 1, f'{indent}stage_kinds: {{hex: "{kinds}"}}'); changed += 1
                elif k:
                    del lines[j + 1]; changed += 1
            break
open(graph, "w").write("\n".join(lines))
print(f"   {graph}: {changed} param(s) re-baked")
PY
done
