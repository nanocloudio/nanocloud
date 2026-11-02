#!/usr/bin/env python3
"""A base graph as a workload image's graph.

A workload's `wiring:` is written as a base graph's — `module.port` ends, and
the node's network as the platform's `linux_net` (or `ip`), each module wired
to it a lane of its own (fluxor `--emit=workload`) — so this moves the graph's
modules and wiring under `workloads:` as they are, but for what a workload
cannot hold:

    to-workload.py <graph.yaml> <out.yaml> [--variant <module>=<variant>]... [--drop <module>]... [--param <module>.<key>=<value>]... <module-dir>...

The platform's own entry (`linux_net` / `ip`, when the graph declares one) is
left out: the node's network is not a workload module.
Module params, variants and edge buffer sizes carry over as written.
`--variant` selects a module's variant, and an edge to a port that variant
omits (its manifest's `omit_ports`) is dropped, on stderr.
`--drop` leaves a module out, and every edge to or from it (on stderr):
what a tenant may not hold — a module with a node privilege, which the
kernel refuses in a workload — or does not need.
`--param` sets one of a module's params (an integer when it reads as one) —
what a variant asks of its peers: clustor's volatile durability is paired
with consensus `durability_mode: 2` (relaxed) and `persist_meta: 0`.
`module_search_paths` stays (the build resolves modules by it); the
graph-level keys a workload has no place for (platform, scheduler,
continuity) are dropped, and say so on stderr.
"""
import os
import re
import sys

import yaml

PLATFORM_NET = ("linux_net", "ip")


def manifest(dirs, mtype):
    """The text of <dir>/**/<mtype>/manifest.toml."""
    for d in dirs:
        for root, _subdirs, files in os.walk(d):
            if os.path.basename(root) == mtype and "manifest.toml" in files:
                return open(os.path.join(root, "manifest.toml")).read()
    sys.exit(f"to-workload: no manifest for module type `{mtype}` under {dirs}")


def omitted(text, variant):
    """The ports `variant` omits."""
    for block in re.split(r"(?m)^\[\[variant\]\]\s*$", text)[1:]:
        block = re.split(r"(?m)^\[", block)[0]
        if re.search(r'(?m)^name\s*=\s*"%s"' % re.escape(variant), block):
            m = re.search(r"(?m)^omit_ports\s*=\s*\[([^\]]*)\]", block)
            return set(re.findall(r'"([^"]+)"', m.group(1))) if m else set()
    sys.exit(f"to-workload: no variant `{variant}`")


def main():
    args = sys.argv[1:]
    variants = {}
    while "--variant" in args:
        i = args.index("--variant")
        m, v = args[i + 1].split("=", 1)
        variants[m] = v
        del args[i:i + 2]
    params = []
    while "--param" in args:
        i = args.index("--param")
        mk, v = args[i + 1].split("=", 1)
        m, k = mk.split(".", 1)
        params.append((m, k, int(v) if v.lstrip("-").isdigit() else v))
        del args[i:i + 2]
    drops = set()
    while "--drop" in args:
        i = args.index("--drop")
        drops.add(args[i + 1])
        del args[i:i + 2]
    src, out, dirs = args[0], args[1], args[2:]
    g = yaml.safe_load(open(src))
    for k in ("platform", "scheduler", "continuity"):
        if k in g:
            print(f"to-workload: dropped graph-level `{k}`", file=sys.stderr)
    mods = [m for m in (g.get("modules") or [])
            if m["name"] not in drops and m["name"] not in PLATFORM_NET]
    for d in sorted(drops):
        print(f"to-workload: dropped module `{d}`", file=sys.stderr)
    for m in mods:
        if m["name"] in variants:
            m["variant"] = variants[m["name"]]
    for mname, k, v in params:
        mod = next((m for m in mods if m["name"] == mname), None)
        if mod is None:
            sys.exit(f"to-workload: --param names no module `{mname}`")
        # Params as the graph wrote them: under `params:` when it has one.
        (mod.setdefault("params", {}) if "params" in mod else mod)[k] = v
    types = {m["name"]: m.get("type", m["name"]) for m in mods}
    gone = {n: omitted(manifest(dirs, types[n]), v) for n, v in variants.items()}
    lanes = []
    wiring = []
    for e in g.get("wiring") or []:
        (fm, fp), (tm, tp) = (e["from"].split(".", 1) + [""])[:2], (e["to"].split(".", 1) + [""])[:2]
        if fm in drops or tm in drops:
            print(f"to-workload: dropped {e['from']} -> {e['to']} (a dropped module's)", file=sys.stderr)
            continue
        if fp in gone.get(fm, ()) or tp in gone.get(tm, ()):
            print(f"to-workload: dropped {e['from']} -> {e['to']} (the variant omits it)", file=sys.stderr)
            continue
        for end, other in ((fm, tm), (tm, fm)):
            if end in PLATFORM_NET and other not in lanes:
                lanes.append(other)
        wiring.append(dict(e))
    work = []
    for m in mods:
        m = dict(m)
        m.setdefault("type", m["name"])
        work.append(m)
    doc = {"target": g.get("target", "linux"), "tick_us": g.get("tick_us", 1000)}
    if "module_search_paths" in g:
        doc["module_search_paths"] = g["module_search_paths"]
    doc.update({"modules": [], "wiring": [],
           "workloads": [{"modules": work, "wiring": wiring}]})
    with open(out, "w") as f:
        f.write(f"# Derived from {os.path.basename(src)} by scripts/support/to-workload.py;"
                f" lanes: {', '.join(lanes)}\n")
        yaml.safe_dump(doc, f, sort_keys=False)


main()
