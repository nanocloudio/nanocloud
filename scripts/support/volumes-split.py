#!/usr/bin/env python3
"""The encrypted-volume pipelines placed across the control plane and a node.

    volumes-split.py plane|node [<fluxor-volumes.yaml>]

packaging/debian/fluxor-volumes.yaml states the pipelines once, the control
plane and its node in one graph. Placed across machines, each side keeps the
chains and effectors that are its own, and a `volume_relay` stands in for the
effector it does not have:

  node    the attach, second-epoch, detach and claim chains (their source
          selects this node's attachments), the node effector (volume_host,
          its Kagi client) and the mount actuator; the relay carries the
          Kagi policy effects its chains issue (kagi_storage_effect's ops)
          to the control plane, and serves the requests the control plane
          addresses to it through volume_host.
  plane   every other chain, and Kagi's storage-key service with its ledger
          and custodians; the relay carries the node-only effects its chains
          issue (volume_host's ops) to the node holding the claim, and
          serves the nodes' asks through kagi_storage_effect.

Both keep Loam's lease and admin effector and the record writes. The output
is a template: the store's replica is spliced in at `__REPLICA_MODULES__` /
`__REPLICA_WIRING__`, and the node's name is `__NODE_NAME__`
(packaging/debian/nanocloud-graph.sh renders both).
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

NODE_ONLY = re.compile(r"(va_.*|vc_.*|mount_effect|volume_host|kagi_client|kagi_client_tls)")
BOTH = re.compile(r"(loam_effect|loam_tls|vw_.*)")
# What each side's effector answers: the ops the other side's relay carries.
HOST_OPS = "10,11,12,13,14,15,16,22,23,67,68,69,72,79"
KAGI_OPS = "20,21,24,25,31,32,60,61,65,71,78,80,81"


def blocks(text, head):
    """`[(name, lines)]` of the `  - <head>: ` items in a section, the
    comment lines before an item kept with it."""
    out, pending = [], []
    for line in text.split("\n"):
        m = re.match(r"^  - %s: (\S+)" % head, line)
        if m:
            out.append([m.group(1), pending + [line]])
            pending = []
        elif line.startswith("    ") and out and not pending:
            out[-1][1].append(line)
        elif line.startswith("  #") or line.strip() == "":
            pending.append(line)
        else:
            pending.append(line)
    if out and pending:
        out[-1][1].extend(l for l in pending if l.strip())
    return out


def endpoint(line):
    m = re.match(r"^\s+(?:- from|to): ([^.\s]+)\.(\S+)", line)
    return (m.group(1), m.group(2)) if m else (None, None)


def split(src, side):
    head, rest = src.split("\nmodules:\n", 1)
    mods_text, wire_text = rest.split("\nwiring:\n", 1)
    mods = blocks(mods_text, "name")
    edges = blocks(wire_text, "from")

    def here(name):
        node = bool(NODE_ONLY.fullmatch(name))
        return bool(BOTH.fullmatch(name)) or (node if side == "node" else not node)

    declared = {n for n, _ in mods}
    # An endpoint no module declares is the platform's (linux_net): both
    # sides have it.
    names = {n for n in declared if here(n)}
    present = lambda n: n is not None and (n in names or n not in declared)
    missing = "kagi_storage_effect" if side == "node" else "volume_host"
    served = "volume_host" if side == "node" else "kagi_storage_effect"
    kept_mods = [l for n, b in mods if n in names for l in b]
    kept_edges = []
    for _, lines in edges:
        at = next(i for i, l in enumerate(lines) if l.startswith("  - from: "))
        notes, edge = lines[:at], lines[at:]
        f = endpoint(edge[0])
        t = next((endpoint(l) for l in edge[1:] if l.lstrip().startswith("to:")), (None, None))
        if present(f[0]) and present(t[0]):
            kept_edges.extend(lines)
        elif t == (missing, "request_in") and present(f[0]):
            # A chain's effect to the effector this side lacks: the relay.
            kept_edges.extend(notes)
            kept_edges.append(edge[0])
            kept_edges.append("    to: volume_relay.request_in")
        elif f == (missing, "response_out") and present(t[0]):
            # Its outcomes to the chain's route: the relay's.
            kept_edges.extend(notes)
            kept_edges.append("  - from: volume_relay.response_out")
            kept_edges.extend(edge[1:])

    if side == "node":
        head_out = head
        kept_mods = [l.replace('where: "device=node-a"', 'where: "device=__NODE_NAME__"') for l in kept_mods]
    else:
        # The node's subgraph types are the node's.
        head_out = re.sub(r"\n# The volume subgraph's types.*?\nworkload_modules:\n(  - \S+\n)+", "\n", head, flags=re.S)
    relay = f"""  # The effects only the other side answers, through the store
  # (scripts/support/volumes-split.py).
  - name: volume_relay
    params:
      side: {1 if side == 'node' else 0}
      relay_ops: "{KAGI_OPS if side == 'node' else HOST_OPS}"
__REPLICA_MODULES__"""
    relay_wire = f"""  - from: volume_relay.effect_out
    to: {served}.request_in
  - from: {served}.response_out
    to: volume_relay.answer_in
__REPLICA_WIRING__"""
    title = {
        "node": "# A node's half of the encrypted-volume pipelines (scripts/support/volumes-split.py,\n"
                "# from packaging/debian/fluxor-volumes.yaml): its attachments and claims, the node\n"
                "# effector, and the relay to the control plane. A template (nanocloud-graph.sh).\n",
        "plane": "# The control plane's half of the encrypted-volume pipelines (scripts/support/\n"
                 "# volumes-split.py, from packaging/debian/fluxor-volumes.yaml): provisioning,\n"
                 "# grants, rotation, snapshots, recovery, deletion, Kagi's service, and the relay\n"
                 "# to the nodes. A template (nanocloud-graph.sh).\n",
    }[side]
    return (title + head_out + "\nmodules:\n" + "\n".join(kept_mods) + "\n" + relay + "\n\nwiring:\n"
            + "\n".join(kept_edges) + "\n" + relay_wire + "\n")


if __name__ == "__main__":
    side = sys.argv[1]
    if side not in ("plane", "node"):
        sys.exit("usage: volumes-split.py plane|node [graph]")
    path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(ROOT, "packaging/debian/fluxor-volumes.yaml")
    sys.stdout.write(split(open(path).read(), side))
