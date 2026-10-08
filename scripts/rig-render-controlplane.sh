#!/usr/bin/env bash
# Render the control plane for the bare-metal Pi 5 rig (no Linux): the SHIPPED
# fluxor-controlplane.yaml, rendered as replica 0 of a one-voter group exactly
# as the E2Es render it, then carried onto metal.
#
#   rig-render-controlplane.sh [out.yaml]     (default: target/pi5/render/pi5-controlplane.yaml)
#
# What changes on metal, and nothing else:
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
#   - the operator's first credential is SEEDED (store_seed): a dev
#     ServiceAccount verification key at /authn-keys/sa and a cluster-admin
#     binding for kube-system/admin. The node's own token mint announces under
#     /authn-keys/node meanwhile. This is the M1 stand-in for the join-token
#     bootstrap (M3), which replaces both.
#
# Keys live in target/pi5/pki and are kept across renders, so a kubeconfig
# (scripts/rig-kubeconfig.sh) stays valid across rebuilds. Certificate files
# are read by `fluxor build` and embedded; the board reads no file.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/target/pi5/render/pi5-controlplane.yaml}"
NODE="${NANOCLOUD_RIG_NODE:-pi5-a}"
IP="${NANOCLOUD_RIG_IP:-192.168.1.9}"
PKI="$ROOT/target/pi5/pki"
WORK="$(dirname "$OUT")"
mkdir -p "$WORK" "$PKI"

# ── Keys: the peer PKI and the API certificate (with this node's address as a
# SAN, so a client can verify it by IP), and the dev ServiceAccount key.
if [ ! -f "$PKI/pki/peer.der" ]; then
  # shellcheck source=scripts/fluxor-env.sh
  . "$ROOT/scripts/fluxor-env.sh"
  nc_node_pki "$PKI" >/dev/null 2>&1
fi
if [ ! -f "$PKI/api.der" ]; then
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$PKI/api.key.pem" -out "$PKI/api.pem" -days 3650 -subj "/CN=nanocloud" \
    -addext "subjectAltName=IP:$IP,DNS:$NODE,DNS:nanocloud,DNS:kubernetes.default.svc" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "extendedKeyUsage=serverAuth" 2>/dev/null
  openssl x509 -in "$PKI/api.pem" -outform DER -out "$PKI/api.der"
  openssl ec -in "$PKI/api.key.pem" -outform DER -out "$PKI/api.key.der" 2>/dev/null
fi
[ -f "$PKI/sa.key.pem" ] || openssl ecparam -name prime256v1 -genkey -noout -out "$PKI/sa.key.pem" 2>/dev/null

# ── The shipped template, rendered as the E2Es render it (one voter).
NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PKI="$PKI/pki" NANOCLOUD_NODE_NAME="$NODE" \
  NANOCLOUD_PEER_PORT=7450 NANOCLOUD_PEERS="0=127.0.0.1:7450" \
  NANOCLOUD_REPLICA_FRAGMENT="$ROOT/packaging/debian/fluxor-replica.yaml" \
  sh "$ROOT/packaging/debian/nanocloud-graph.sh" \
  "$ROOT/packaging/debian/fluxor-controlplane.yaml" "$WORK/cp.linux.yaml"

# ── Onto metal.
python3 - "$WORK/cp.linux.yaml" "$OUT" "$ROOT" "$PKI" "$NODE" "${NANOCLOUD_RIG_KEY_AFTER_MS:-120000}" <<'PY'
import binascii, json, os, re, struct, sys
from cryptography.hazmat.primitives import serialization

src, out, root, pki, node = sys.argv[1:6]
key_after_ms = int(sys.argv[6]) if len(sys.argv) > 6 else 0
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
sub('cert_file: "/etc/nanocloud.io/certs/server.der"', f'cert_file: "{pki}/api.der"')
sub('key_file: "/etc/nanocloud.io/certs/server.key.der"', f'key_file: "{pki}/api.key.der"')
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

# The node's own mint announces apart from the seeded dev key (M1 only).
sub('    announce_path: "/authn-keys/sa"\n', '    announce_path: "/authn-keys/node"\n')

# The dev ServiceAccount key, as kagi's key frame (kagi_verify's wire).
key = serialization.load_pem_private_key(open(f"{pki}/sa.key.pem", "rb").read(), None)
pub = key.public_key().public_bytes(serialization.Encoding.X962,
                                    serialization.PublicFormat.UncompressedPoint)
f8 = lambda b: bytes([len(b)]) + b
f16 = lambda b: struct.pack("<H", len(b)) + b
body = (f8(b"https://kubernetes.default.svc") + struct.pack("<H", 1) + f8(b"sa-1")
        + struct.pack("<H", 1) + bytes([1]) + bytes([0x01]) + struct.pack("<I", 1)
        + struct.pack("<Q", 0) + struct.pack("<Q", 0) + f16(pub))
frame = binascii.hexlify(bytes([0x22]) + struct.pack("<H", len(body)) + body).decode()

js = lambda o: json.dumps(json.dumps(o, separators=(",", ":")))
rbac = "rbac.authorization.k8s.io/v1"
seeds = [
    ("seed_sa_key", "/authn-keys/sa", None, frame),
    ("seed_ns_default", "/namespaces/default",
     {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "default"}}, None),
    ("seed_ns_system", "/namespaces/kube-system",
     {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "kube-system"}}, None),
    ("seed_admin_role", "/clusterroles/cluster-admin",
     {"apiVersion": rbac, "kind": "ClusterRole", "metadata": {"name": "cluster-admin"},
      "rules": [{"apiGroups": ["*"], "resources": ["*"], "verbs": ["*"]},
                {"nonResourceURLs": ["*"], "verbs": ["*"]}]}, None),
    ("seed_admin_binding", "/clusterrolebindings/nanocloud-admin",
     {"apiVersion": rbac, "kind": "ClusterRoleBinding", "metadata": {"name": "nanocloud-admin"},
      "subjects": [{"kind": "ServiceAccount", "name": "admin", "namespace": "kube-system"}],
      "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                  "name": "cluster-admin"}}, None),
]

# This node's lease, as a worker keeps it (fluxor-worker.yaml).
worker = open(f"{root}/packaging/debian/fluxor-worker.yaml").read()
m = re.search(r"(  - name: nl_self\n.*?)\nwiring:\n", worker, re.S)
lease = m.group(1).replace("__NODE_NAME__", node).rstrip("\n") + "\n"

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
    "  # ── M1 bootstrap seeds (replaced by the join-token flow, M3) ──────────\n",
]
import subprocess
flxa = subprocess.check_output(["python3", f"{root}/scripts/support/flxa.py", "churn_probe:1=0"]).decode().strip()
if podplane:
    seeds.append(("seed_graph_probe", "/graphs.nanocloud.io/probe",
                  {"apiVersion": "nanocloud.io/v1", "kind": "Graph", "metadata": {"name": "probe"},
                   "spec": {"flxa": flxa}}, None))
# The volume Pod's Graph (~2.4 KiB with its TLS material) is past what one
# store_seed holds (1 KiB): it is applied through the API, as an operator
# applies it (the E3 driver), not seeded.
for name, k, obj, hexval in seeds:
    v = f"      value_hex: \"{hexval}\"\n" if hexval else f"      value: {js(obj)}\n"
    # The key changes what the graph does (it arms token verification), so
    # it lands only once the board's telemetry is up — 120 s by default, past
    # DHCP — and its effect is observed rather than happening in the dark.
    # NANOCLOUD_RIG_KEY_AFTER_MS overrides when rendering by hand (the rig
    # build does not pass the environment through).
    if name == "seed_sa_key" and key_after_ms:
        v += f"      after_ms: {key_after_ms}\n"
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
KEEP_PREFIX = ("pj_", "pl_", "pr_")
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
    extra.append("".join(kept_mods).rstrip("\n") + "\n")
else:
    kube_edges = ""

edges = [kube_edges,
         "  - from: nvme.blocks\n    to: fat32.blocks\n",
         "  - from: sntp.net_out\n    to: ip.net_in\n",
         "  - from: ip.net_out\n    to: sntp.net_in\n"]
for name, *_ in seeds:
    edges.append(f"  - from: {name}.status\n    to: {name}.changes\n")
consumers.add("store_seed")
consumers.update({"tls", "token_verify"})
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
