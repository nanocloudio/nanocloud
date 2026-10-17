#!/usr/bin/env bash
# Render the control plane for the bare-metal Pi 5 rig (no Linux): the SHIPPED
# fluxor-controlplane.yaml, rendered as replica 0 of a one-voter group exactly
# as the E2Es render it, then carried onto metal.
#
#   rig-render-controlplane.sh [out.yaml]     (default: target/pi5/render/pi5-controlplane.yaml)
#
# What changes on metal, and nothing else (the HPA chain stays off: its
# metrics are another graph's, which a board does not run):
#   - target pi5; the platform's net stack (rp1_gem -> ip) replaces linux_net,
#     and the debug stack reports over UDP;
#   - the API listens on :6443 through `tls` (no host_tcp);
#   - the store's disk is the board's: nvme -> fat32 under lattice's store and
#     clustor's write-ahead log, both at the FAT32 root (root_path: 1), with
#     the step deadlines lattice requires on bare metal, and 1024 revisions
#     of history rather than 65536;
#   - every module that calls the store (its manifest requires storage.object
#     or storage.namespace) gets the same deadline: a call is answered inside
#     the caller's step, and on the board's disk one can take longer than the
#     2 ms default — the guard would kill the API plane's own store_effect;
#     so do the modules doing public-key work in a step (tls handshakes,
#     token_verify's signature checks), and fat32, whose own step issues
#     synchronous device commands;
#   - `sntp` anchors the wall clock, so token expiry and certificate validity
#     are judged against trusted time;
#   - this node registers itself and keeps its lease (node_lease.uproc `renew`,
#     as a worker does): a k3s-style single node is server and agent;
#   - the node is its own PKI (node_pki): the API's TLS key is born in the
#     board's vault (`identity_label`) and certified on the board by kagi's
#     authority (`nanocloud.ca`, the key that also signs ServiceAccount
#     tokens), and the operator's first credential is minted there and
#     released once by a join secret the node logs with its authority's key
#     digest (scripts/rig-join.sh). The image carries no API key, no API
#     certificate and no ServiceAccount key; what is seeded is policy — the
#     namespaces and a cluster-admin binding for kube-system/admin;
#   - the store's peer links are trusted by that same authority
#     (NANOCLOUD_PEER_TRUST=authority): each link end's key is born in the
#     vault and certified as …/replica-<id>.server / .client, and peers bind
#     by those principals. No peer key is rendered either. A worker's slot
#     is declared by address (NANOCLOUD_RIG_PEERS, "id=host:port ...", or
#     the file target/pi5/render/peers): node_pki declares its Devices
#     (nanocloud-peers/replica-<id>.server, .client) and a ServiceAccount,
#     node-bootstrap-<id>, that may patch and read those two alone, and the
#     join hands the operator that ServiceAccount's credential (rig-join.sh
#     writes it as node-<id>.token). The worker is certified through the API
#     with it, and admitted as a learner once its principal binds
#     (scripts/node-join-e2e.sh; NC_PLANE=<board> runs its worker half here).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/target/pi5/render/pi5-controlplane.yaml}"
NODE="${NANOCLOUD_RIG_NODE:-pi5-a}"
PKI="$ROOT/target/pi5/pki"   # no longer read: nothing is rendered from it
WORK="$(dirname "$OUT")"
mkdir -p "$WORK"
# The rig build renders without the caller's environment, so the group's
# peers are also a file beside the render (`target/pi5/render/peers`, one
# "id=host:port ..." line), as the pod plane's switch is.
PEERS="${NANOCLOUD_RIG_PEERS:-$(cat "$WORK/peers" 2>/dev/null || echo 0=127.0.0.1:7450)}"

# ── The shipped template, rendered as the E2Es render it (one voter), with
# the cluster's own authority behind the peer links.
NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PEER_TRUST=authority NANOCLOUD_NODE_NAME="$NODE" \
  NANOCLOUD_PEER_PORT=7450 NANOCLOUD_PEERS="$PEERS" \
  NANOCLOUD_REPLICA_FRAGMENT="$ROOT/packaging/debian/fluxor-replica.yaml" \
  sh "$ROOT/packaging/debian/nanocloud-graph.sh" \
  "$ROOT/packaging/debian/fluxor-controlplane.yaml" "$WORK/cp.linux.yaml"

# ── Onto metal.
# The registry's CA (public: the cluster's certificate, never its key), as
# DER for the image plane's tls client.
REGISTRY_CA="${NANOCLOUD_RIG_REGISTRY_CA:-/var/lib/nanocloud.io/secure_assets/ca.crt}"
if [ -r "$REGISTRY_CA" ]; then
  openssl x509 -in "$REGISTRY_CA" -outform DER -out "$WORK/registry-ca.der"
fi
NANOCLOUD_RIG_PEERS="$PEERS" python3 - "$WORK/cp.linux.yaml" "$OUT" "$ROOT" "$PKI" "$NODE" <<'PY'
import json, os, re, sys

src, out, root, pki, node = sys.argv[1:6]
g = open(src).read()

# Module types that call the store, from their manifests.
import glob
consumers = {
    mf.split("/")[-2]
    for mf in glob.glob(f"{root}/modules/*/*/manifest.toml")
    if re.search(r'requires_contract\s*=\s*"storage\.(object|namespace)"', open(mf).read())
}

def sub(old, new, count=1):
    global g
    n = g.count(old)
    if n != count:
        sys.exit(f"render: expected {count} x {old[:60]!r}, found {n}")
    g = g.replace(old, new)

def sub_re(pat, new, count=1):
    global g
    g, n = re.subn(pat, new, g, flags=re.M)
    if n != count:
        sys.exit(f"render: expected {count} x /{pat}/, found {n}")

sub("target: linux\n", "target: pi5\n")
sub_re(r"^platform:\n(?:  .*\n|  #.*\n)*?    write_buf_kib: \d+\n",
       "platform:\n  net: {}\n  debug:\n    to: net\n    monitor: true\n")
sub("    host_tcp: 1\n", "")
sub("    port: 7443\n", "    port: 6443\n")
# The API's identity is the board's: a vault key, certified by node_pki
# (rendered so by NANOCLOUD_PEER_TRUST=authority, with the join).
if g.count('    identity_label: "nanocloud.api"\n') != 1 or "  - name: node_pki\n" not in g:
    sys.exit("render: the API's identity is not the vault's, or no node_pki certifies it")
sub_re(r"^(\s+- from: |\s+to: )linux_net\.", r"\1ip.", count=g.count("linux_net."))

# The store and the log on the board's disk, at the FAT32 root.
deadline = ("    step_deadline_us: 12000\n    step_deadline_burst_us: 16000\n"
            "    fault_policy: tolerate\n")
sub("  - name: kv_state_worker_store\n    params:\n      state_store: 1\n"
    "      store_dir: lattice\n      import_log: store.log\n",
    "  - name: kv_state_worker_store\n" + deadline +
    "    params:\n      state_store: 1\n      root_path: 1\n")
# This node is its own plane AND its own worker, in one store: the pod
# plane's private records (what a worker's replica keeps under its scope)
# live under /~node/<node>/ here too, which is where the plane's pod_specs
# writes them — the kubelet chains name them plainly and the store maps.
# NANOCLOUD_RIG_PODPLANE=1 opts in (M2 work): with the pod plane the store
# sits past its 12 ms budget on this NVMe (7 writes/s, 5 ms each, plus its
# flushes) and the API starves — rig run 1791590538. Off, the render is the
# one M1 passes with.
# The rig build renders without the caller's environment, so the switch is
# also a file beside the render: `touch target/pi5/render/.podplane`.
work = os.path.dirname(out)
podplane = (os.environ.get("NANOCLOUD_RIG_PODPLANE", "0") == "1"
            or os.path.exists(os.path.join(work, ".podplane")))
# E3, a stateful Pod: `target/pi5/render/volume/` holds the Graph's FLXA
# (`flxa.hex`: tls → loam_volume → stateful_probe, built off-node by
# scripts/metal-volume-e2e.sh's recipe) and this node's Loam capability
# (`node.cap`). The render then packs the Pod's module types and writes the
# capability onto the board's FAT32 root at bring-up (fat32_seed, the path
# the FLXA names) and seeds the Graph object `probe-vol`.
# Pods of workload images (`NANOCLOUD_RIG_IMAGES=1`, or `.images` beside the
# render): the image plane runs on the board — http_fetch through a `tls`
# client that authenticates the registry as registry.nanocloud.io by the
# cluster's CA, image.uproc/image_asm.uproc, the pod plane's resolution
# chain (pod_image.uproc) — and its blob cache is the FAT32's /blobs.
images = podplane and (os.environ.get("NANOCLOUD_RIG_IMAGES", "0") == "1"
                       or os.path.exists(os.path.join(work, ".images")))
registry = os.environ.get("NANOCLOUD_RIG_REGISTRY", "192.168.1.10:5000")
ca_der = os.path.join(work, "registry-ca.der")
volume_dir = os.path.join(work, "volume")
volume = podplane and os.path.exists(os.path.join(volume_dir, "flxa.hex"))
# The pod plane's writes (sandbox phases, pod runtime) land on the same
# store, and beside the plane's they put the store past its 12 ms step
# (run 1791590538: 711 store step-timeouts, the API starved). A larger
# budget is not available — a burst may not exceed 16 × the domain tick —
# so the store's per-write cost has to fall (a run-block cache; the plan's
# A4) before the pod plane runs on this board.
SCOPED = ("/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,"
          "/volume-binds/,/image-,/volume-ops/")
if podplane:
    sub('      scope: ""\n      scoped: ""\n',
        f'      scope: "{node}"\n      scoped: "{SCOPED}"\n')
# A shorter watch-resume window on the board (past it a watch is answered
# 410 and relists, as from any etcd that has compacted). Every block a
# lookup walks on bare metal is a device read, and history is what the
# runs are mostly made of: 65536 revisions of it put ~2,000 block reads
# into each replicated write.
sub("      history_revisions: 65536\n", "      history_revisions: 1024\n")
sub("  - name: durability\n    params:\n",
    "  - name: durability\n" + deadline +
    "    params:\n      root_path: 1\n      segment_bytes: 8192\n")

js = lambda o: json.dumps(json.dumps(o, separators=(",", ":")))
# The namespaces, who the joined admin is, and each worker slot (its
# Devices and the one credential that may give them keys) are node_pki's to
# declare: the join writes them once, if absent, beside the credentials it
# mints — no module spent per key on a board seated near its module ceiling.
seeds = []
# This node's lease, as a worker keeps it (fluxor-worker.yaml).
worker = open(f"{root}/packaging/debian/fluxor-worker.yaml").read()
m = re.search(r"(  - name: nl_self\n.*?)\nwiring:\n", worker, re.S)
# Its labels: a Pi 5 (arm64) running fluxor on the metal, not Linux; the ABI
# its image is built against.
import subprocess
abi = subprocess.run(["sh", f"{root}/scripts/fluxor-abi.sh", root], capture_output=True,
                     text=True, check=True).stdout.strip()
facts = (f'"kubernetes.io/arch":"arm64","kubernetes.io/os":"fluxor",'
         f'"kubernetes.io/hostname":"{node}","nanocloud.io/fluxor-abi":"{abi}"')
# Its address: the board's (status.addresses InternalIP, hence its Pods'
# hostIP/podIP), the one the driver and the render's peer address use.
board_ip = os.environ.get("NANOCLOUD_RIG_IP", "192.168.1.9")
lease = (m.group(1).replace("__NODE_NAME__", node).replace("__NODE_FACTS__", facts)
         .replace("__NODE_ADDRESS__", board_ip).rstrip("\n") + "\n")

# The board's disk FIRST, as lattice's own pi5 graphs declare it: the store
# opens its disk at construction, and a store constructed before its FS
# provider exists cannot open one at all (it quarantines, and the kernel
# refuses the graph). Constructed first, fat32 answers "not yet" until it
# has mounted, which the store waits out.
disk = ("  # ── Metal: the board's disk under the store and the log ──────────────\n"
        "  - name: nvme\n    bind: m2_primary\n    namespace: 1\n    queue_depth: 32\n"
        "  - name: fat32\n    path: /\n    init_free_hint: 20000\n    clear_free_region: 4096\n")
head, rest = g.split("\nmodules:\n", 1)
g = head + "\nmodules:\n" + disk + rest

extra = [
    "  # ── Metal: trusted wall-clock time (four public servers, quorum two) ──\n",
    "  - name: sntp\n    params:\n"
    '      server0: "0xA29FC801"\n      server1: "0xA29FC87B"\n'
    '      server2: "0xD8EF2300"\n      server3: "0xD8EF2304"\n'
    "      quorum: 2\n      boot_delay_ms: 15000\n",
]
# The Graphs the Pods name are applied through the API, as an operator
# applies them (the driver's `probe`, the E3 driver's `probe-vol`), not
# seeded: a seed is a module, and the board sits near its module ceiling.
for name, k, obj, hexval in seeds:
    v = f"      value_hex: \"{hexval}\"\n" if hexval else f"      value: {js(obj)}\n"
    extra.append(f"  - name: {name}\n    type: store_seed\n    params:\n      key: \"{k}\"\n{v}")
extra.append("  # ── This node's lease (node_lease.uproc renew) ───────────────────────\n")
extra.append(lease)
if volume:
    cap = open(os.path.join(volume_dir, "node.cap")).read()
    extra.append("  # ── E3: this node's Loam capability, on the root at bring-up ──────────\n")
    extra.append("  - name: seed_node_cap\n    type: fat32_seed\n" + deadline +
                 "    params:\n      path: \"/NODE.CAP\"\n      data: " + json.dumps(cap) + "\n")
# The pod plane (the deb's kubelet.yaml, a second process on Linux) runs in
# this one graph on metal, over the scoped store above — the slice of it a
# graph-backed Pod needs. A metal node runs module subgraphs, not
# processes: the image pull and assembly, probes, volume binds, TTY and the
# host sandbox chain stay out (they are 39 of its 63 modules, and the pi5
# kernel seats 255).
kube = open(f"{root}/packaging/debian/fluxor-podlifecycle.yaml").read()
kube_mods = kube[kube.index("modules:\n") + len("modules:\n"):kube.index("\nwiring:")]
kube_wire = kube[kube.index("wiring:\n") + len("wiring:\n"):]
KEEP_PREFIX = ("pj_", "pl_", "pr_") + (("pi_",) if images else ())
KEEP = {"wl", "sb_mspec", "sb_mspecd", "sb_sig", "sb_sigd", "sb_pause", "sb_paused",
        "sb_ev", "sb_evout"}
def kept(name):
    return name in KEEP or name.startswith(KEEP_PREFIX)
blocks = re.split(r"(?m)^(?=  - name: )", kube_mods)
kept_mods = [b for b in blocks if b.startswith("  - name: ") and kept(b.split()[2])]
pairs, frm = [], None
for line in kube_wire.splitlines():
    m = re.match(r"  - from: (\S+)$", line)
    if m:
        frm = m.group(1)
        continue
    m = re.match(r"    to: (\S+)$", line)
    if m and frm:
        pairs.append((frm, m.group(1)))
        frm = None
kube_edges = "".join(f"  - from: {a}\n    to: {b}\n" for a, b in pairs
                     if kept(a.split(".")[0]) and kept(b.split(".")[0]))
if podplane:
    extra.append("  # ── The pod plane: packaging/debian/fluxor-podlifecycle.yaml, its graph-pod slice ──\n")
    pod = "".join(kept_mods).rstrip("\n") + "\n"
    if images:
        pod = pod.replace('    blobs: "/var/lib/nanocloud.io/image/blobs"\n', '    blobs: "/blobs"\n')
    extra.append(pod)
else:
    kube_edges = ""
if images:
    pull = open(f"{root}/packaging/debian/fluxor-imagepull.yaml").read()
    pull_mods = pull[pull.index("modules:\n") + len("modules:\n"):pull.index("\nwiring:")]
    pull_mods = (pull_mods.replace('authority: "registry.nanocloud.io:5000"', f'authority: "{registry}"')
                 .replace('dir: "/var/lib/nanocloud.io/image/blobs"', 'dir: "/blobs"'))
    extra.append("  # ── The image plane: packaging/debian/fluxor-imagepull.yaml, behind tls ──\n")
    extra.append(pull_mods.rstrip("\n") + "\n")
    extra.append("  - name: fetch_tls\n    type: tls\n    mode: 0\n    alpn_h1_only: 1\n"
                 "    peer_auth: 2\n    trust: \"${file:" + ca_der + "}\"\n"
                 "    verify_hostname: \"registry.nanocloud.io\"\n")
    for line in pull[pull.index("wiring:\n") + len("wiring:\n"):].split("\n  - from: "):
        line = line.strip()
        if not line:
            continue
        line = line if line.startswith("- from: ") else "- from: " + line
        if "linux_net." in line:
            continue  # fetch's net pair: through fetch_tls below
        kube_edges += "  " + line.rstrip("\n") + "\n"
    kube_edges += ("  - from: ip.net_out\n    to: fetch_tls.cipher_in\n"
                   "  - from: fetch_tls.cipher_out\n    to: ip.net_in\n"
                   "  - from: fetch_tls.clear_out\n    to: fetch.net_in\n"
                   "  - from: fetch.net_out\n    to: fetch_tls.clear_in\n")
    consumers.add("http_fetch")

edges = [kube_edges,
         "  - from: nvme.blocks\n    to: fat32.blocks\n",
         "  - from: sntp.net_out\n    to: ip.net_in\n",
         "  - from: ip.net_out\n    to: sntp.net_in\n"]
for name, *_ in seeds:
    edges.append(f"  - from: {name}.status\n    to: {name}.changes\n")
consumers.add("store_seed")
consumers.update({"tls", "token_verify"})
# The node's authority and its ServiceAccount mint sign (and, on a vault
# that cannot keep keys, generate) P-256 keys in a step: past 2 ms on this
# board, and a terminated certificate_endpoint never certifies the API.
consumers.update({"certificate_endpoint", "token_mint"})
# The API's front: an HTTP/2 connection's setup (kubectl's first request)
# crossed the 2 ms default once on this board, and a terminated http never
# answers again — the API looked dead with every module behind it healthy.
consumers.update({"http"})
# The disk: fat32's own step frees unlinked chains and completes fences with
# synchronous NVMe commands, past 16 ms on this NVMe, and a terminated fat32
# answers every FS call ENOSYS (the store quarantines). NOT nvme: with a
# budget and `tolerate` on it the board boots dark.
consumers.update({"fat32"})
edges.append("  - from: nl_self.status\n    to: nl_self.changes\n")
edges.append("  - from: nl_self.record_out\n    to: nl_renew.request_in\n")

mods, wiring = g.split("\nwiring:\n", 1)
mods = mods.rstrip("\n") + "\n" + "".join(extra)

# Store callers get the store's deadline (see the header).
def with_deadline(m):
    name, rest = m.group(1), m.group(0)
    t = re.search(r"^    type: (\S+)$", rest, re.M)
    if (t.group(1) if t else name) not in consumers or "step_deadline_us:" in rest:
        return rest
    head = f"  - name: {name}\n"
    return head + deadline + rest[len(head):]
mods = re.sub(r"^  - name: (\S+)\n(?:(?!  - name: ).*\n)*", with_deadline, mods, flags=re.M)

# Not on metal: the HorizontalPodAutoscaler chain (hp_*). It scales on
# /hpa-metrics/, which the autoscale graph's metrics pipeline writes — and
# that graph does not run on a board — so here it would only seat six of the
# kernel's 255 module slots that graph-backed Pods need.
NOT_ON_METAL = ("hp_",)
def on_metal(name):
    return not name.startswith(NOT_ON_METAL)
mods = "".join(b for b in re.split(r"(?m)^(?=  - name: )", mods)
               if not b.startswith("  - name: ") or on_metal(b.split()[2]))
wired = re.split(r"(?m)^(?=  - from: )", wiring)
def edge_on_metal(e):
    m = re.match(r"  - from: (\S+?)\.\S+\n    to: (\S+?)\.", e)
    return not m or (on_metal(m.group(1)) and on_metal(m.group(2)))
wiring = "".join(e for e in wired if edge_on_metal(e))
g = mods + "\nwiring:\n" + wiring.rstrip("\n") + "\n" + "".join(edges)
# Modules a workload may be staged with: resolved by type hash against this
# image, so they are packed without being instantiated. `probe` is the
# demo Pod's graph (scripts/support/flxa.py churn_probe:1=0).
if podplane:
    g += "\nworkload_modules:\n  - churn_probe\n"
    if volume:
        g += "  - tls\n  - loam_volume\n  - stateful_probe\n"
open(out, "w").write(
    "# GENERATED by scripts/rig-render-controlplane.sh from\n"
    "# packaging/debian/fluxor-controlplane.yaml — do not edit; re-render.\n" + g)
print(f"rendered {out}: {g.count(chr(10) + '  - name:')} modules, "
      f"{g.count(chr(10) + '  - from:')} edges")
PY
