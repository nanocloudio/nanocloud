#!/usr/bin/env python3
"""The container runtime block of packaging/debian/fluxor-sandbox.yaml.

    sandbox-block.py mods|wire        print the modules / wiring block
    sandbox-block.py sync <graph>...  rewrite each graph's embedded copy

The runtime (sandbox.uproc and volume.uproc over workload_effect) is stated
once, in that graph, between `# ── sandbox` markers. A graph that runs
sandboxes carries the same block between the same markers; `sync` makes each
copy the source's again after it changes (then re-bake the params).
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
START, END = "  # ── sandbox ─", "  # ── sandbox end"


def block(src, part):
    section = src[src.index("modules:\n"):src.index("\nwiring:")] if part == "mods" else src[src.index("wiring:\n"):]
    return section[section.index(START):section.index(END)].rstrip("\n")


src = open(os.path.join(ROOT, "packaging/debian/fluxor-sandbox.yaml")).read()
if sys.argv[1] in ("mods", "wire"):
    print(block(src, sys.argv[1]))
elif sys.argv[1] == "sync":
    for path in sys.argv[2:]:
        text = open(path).read()
        out, rest = "", text
        for part in ("mods", "wire"):
            a = rest.index(START)
            b = rest.index(END, a)
            out += rest[:a] + block(src, part) + "\n"
            rest = rest[b:]
        open(path, "w").write(out + rest)
        print(f"{path}: synced")
