#!/usr/bin/env python3
"""Extract a fixture graph from fluxor-controlplane.yaml by node-name prefix.

    extract-fixture.py [--from=<graph>] <out.yaml> <header-comment-file|-> <prefix> [<prefix>…]

Keeps every node whose name starts with one of the prefixes, and every wiring
edge whose BOTH ends do, verbatim — so a component's E2E hosts the same nodes
and baked params production runs, and cannot drift from them. The chains'
params are checked against their .uproc by chronicle-param-drift-e2e.sh.
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "packaging/debian/fluxor-controlplane.yaml")


def blocks(lines, start_re):
    """Split `lines` into blocks, each starting at a line matching start_re."""
    out, cur = [], None
    for l in lines:
        if re.match(start_re, l):
            if cur:
                out.append(cur)
            cur = [l]
        elif cur is not None:
            if l.startswith("  ") or l.strip() == "":
                cur.append(l)
            else:
                out.append(cur)
                cur = None
    if cur:
        out.append(cur)
    return out


def main():
    args = sys.argv[1:]
    src = SRC
    if args and args[0].startswith("--from="):
        src = os.path.join(ROOT, args.pop(0)[len("--from="):])
    out_path, header, prefixes = args[0], args[1], tuple(args[2:])
    text = open(src).read()
    mods = text.split("\nmodules:\n", 1)[1].split("\nwiring:\n", 1)[0].split("\n")
    wires = text.split("\nwiring:\n", 1)[1].split("\n")
    keep_nodes = []
    for b in blocks(mods, r"  - name: "):
        name = b[0].split("name:", 1)[1].strip()
        if name.startswith(prefixes):
            # Trailing comment lines belong to the NEXT node; drop them here.
            while b and (b[-1].strip() == "" or b[-1].strip().startswith("#")):
                b.pop()
            keep_nodes.append("\n".join(b))
    keep_wires = []
    for b in blocks(wires, r"  - from: "):
        a = b[0].split("from:", 1)[1].strip().split(".")[0]
        to = [l for l in b if l.strip().startswith("to:")]
        z = to[0].split("to:", 1)[1].strip().split(".")[0] if to else ""
        if a.startswith(prefixes) and z.startswith(prefixes):
            while b and (b[-1].strip() == "" or b[-1].strip().startswith("#")):
                b.pop()
            keep_wires.append("\n".join(b))
    head = "" if header == "-" else open(header).read().rstrip("\n") + "\n"
    body = (
        f"{head}# EXTRACTED from {os.path.relpath(src, ROOT)} by\n"
        f"# scripts/extract-fixture.py ({' '.join(prefixes)}) — do not edit; re-extract.\n"
        "target: linux\ntick_us: 1000\n\nscheduler:\n  accept_cycles: true\n\n"
        "modules:\n" + "\n".join(keep_nodes) + "\n\nwiring:\n" + "\n".join(keep_wires) + "\n"
    )
    open(out_path, "w").write(body)
    print(f"{out_path}: {len(keep_nodes)} nodes, {len(keep_wires)} edges")


main()
