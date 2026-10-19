# Shared by the image-plane E2Es (image-platform, image-auth, image-assemble,
# image-gc): the store's host log, a registry, images authored into it, and
# the node's image plane spliced from what ships. Sourced after
# scripts/fluxor-env.sh, with D, ROOT and fail set. With IP_SUDO=1 the store
# log is read and written as root (a graph run as root owns it).

ip_py() { if [ "${IP_SUDO:-0}" = 1 ]; then sudo -n python3 "$@"; else python3 "$@"; fi; }

# ip_put <key> <value>: appended to the host log the store imports at boot.
ip_put() {
  ip_py - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

# ip_del <key>: a delete of it, appended the same way.
ip_del() {
  ip_py - "$D/store.log" "$1" <<'PY'
import struct, sys, os
path, key = sys.argv[1], sys.argv[2].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
    f.flush(); os.fsync(f.fileno())
PY
}

# ip_get <key>: its live value ("" when absent or deleted).
ip_get() {
  ip_py - "$D/store.log" "$1" <<'PY'
import struct, sys
try:
    data = open(sys.argv[1], "rb").read()
except OSError:
    data = b""
want = sys.argv[2].encode(); p = 0; out = None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode(errors="replace") if out is not None else "")
PY
}

# ip_keys <prefix>: the live keys under it, one a line.
ip_keys() {
  ip_py - "$D/store.log" "$1" <<'PY'
import struct, sys
try:
    data = open(sys.argv[1], "rb").read()
except OSError:
    data = b""
want = sys.argv[2].encode(); p = 0; live = {}
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]
    if k.startswith(want):
        if op == 1: live[k] = 1
        else: live.pop(k, None)
    p += 15 + kl + vl
for k in live: print(k.decode(errors="replace"))
PY
}

# ip_registry <port> <log> [stub args…]: scripts/support/registry-stub.py over
# $D/reg, its pid appended to IP_PIDS, up before this returns.
IP_PIDS=""
ip_registry() {
  local port="$1" log="$2"; shift 2
  python3 "$ROOT/scripts/support/registry-stub.py" "$D/reg" "$port" "$@" >>"$log" 2>&1 &
  IP_PIDS="$IP_PIDS $!"
  for _ in $(seq 1 50); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$port),timeout=0.2).close()" 2>/dev/null && return 0
    sleep 0.1
  done
  fail "the registry on :$port did not come up"
}

# ip_image <repo> <tag> <arch> <file>…: an image in $D/reg — each file a gzip
# tar layer, the config naming <arch> — and the tag naming its manifest. The
# manifest's hex is printed. ip_index <repo> <tag> <arch[/variant]=hex>…: an
# index of those manifests at the tag (an entry `arch=-` is a decoy naming a
# manifest the registry does not have).
ip_image() {
  python3 - "$D/reg" "$@" <<'PY'
import hashlib, json, os, sys
d, repo, tag, arch = sys.argv[1:5]
os.makedirs(d, exist_ok=True)
def land(b):
    h = hashlib.sha256(b).hexdigest()
    open(os.path.join(d, h), "wb").write(b)
    return h, len(b)
layers = [land(open(f, "rb").read()) for f in sys.argv[5:]]
cfg = json.dumps({"architecture": arch, "os": "linux",
                  "config": {"Env": ["PATH=/bin"], "Entrypoint": ["/bin/hello"], "Cmd": []}}).encode()
ch, cn = land(cfg)
m = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
     "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:" + ch, "size": cn},
     "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "sha256:" + h, "size": n}
                for h, n in layers]}
mh, _ = land(json.dumps(m).encode())
tp = os.path.join(d, "tags.json")
tags = json.load(open(tp)) if os.path.exists(tp) else {}
tags[f"{repo}:{tag}"] = mh
json.dump(tags, open(tp, "w"))
print(mh)
PY
}
ip_index() {
  python3 - "$D/reg" "$@" <<'PY'
import hashlib, json, os, sys
d, repo, tag = sys.argv[1:4]
ms = []
for e in sys.argv[4:]:
    plat, h = e.split("=", 1)
    arch, _, variant = plat.partition("/")
    p = {"architecture": arch, "os": "linux"}
    if variant: p["variant"] = variant
    if h == "-":
        h, size = "f" * 64, 7
    else:
        size = os.path.getsize(os.path.join(d, h))
    ms.append({"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + h,
               "size": size, "platform": p})
b = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
                "manifests": ms}).encode()
h = hashlib.sha256(b).hexdigest()
open(os.path.join(d, h), "wb").write(b)
tp = os.path.join(d, "tags.json")
tags = json.load(open(tp)) if os.path.exists(tp) else {}
tags[f"{repo}:{tag}"] = h
json.dump(tags, open(tp, "w"))
print(h)
PY
}

# ip_layer <out.tgz> <path=content>…: a gzip tar layer of those files.
ip_layer() {
  local out="$1" t; shift
  t="$(mktemp -d "$D/layer-XXXXXX")"
  local kv
  for kv in "$@"; do
    mkdir -p "$t/$(dirname "${kv%%=*}")"
    printf '%s\n' "${kv#*=}" >"$t/${kv%%=*}"
  done
  tar -czf "$out" -C "$t" .
  rm -rf "$t"
}

# ip_graph <out.yaml> <registry host:port> <blobs dir> <facts> [sandbox]: the
# node's image plane as it ships (fluxor-imagepull.yaml, dialling the
# registry in the clear) with the kubelet's image resolution (the pi_*
# chain, fluxor-podlifecycle.yaml) — and, with `sandbox`, the container
# runtime and the pod chain that runs what is resolved (as
# scripts/image-run-e2e.sh composes it).
ip_graph() {
  python3 - "$ROOT" "$@" <<'PY'
import os, re, subprocess, sys
root, out, registry, blobs, facts = sys.argv[1:6]
sandbox = len(sys.argv) > 6 and sys.argv[6] == "sandbox"
def parts(path, keep):
    y = open(path).read()
    mods = y[y.index("modules:\n") + len("modules:\n"):y.index("\nwiring:")]
    wire = y[y.index("wiring:\n") + len("wiring:\n"):]
    blocks = re.split(r"(?m)^(?=  - name: )", mods)
    pairs, frm = [], None
    for line in wire.splitlines():
        m = re.match(r"  - from: (\S+)$", line)
        if m: frm = m.group(1); continue
        m = re.match(r"    to: (\S+)$", line)
        if m and frm: pairs.append((frm, m.group(1))); frm = None
    blocks = [b for b in blocks if b.startswith("  - name: ") and keep(b.split()[2])]
    names = {b.split()[2] for b in blocks}
    return blocks, [(a, b) for a, b in pairs if a.split(".")[0] in names | {"linux_net"}
                    and b.split(".")[0] in names | {"linux_net"}]
img, iw = parts(f"{root}/packaging/debian/fluxor-imagepull.yaml", lambda n: True)
img = "".join(img)
for old, new in (('authority: "registry.nanocloud.io:5000"', f'authority: "{registry}"'),
                 ('label: "registry.nanocloud.io:5000"', f'label: "{registry}"'),
                 ('dir: "/var/lib/nanocloud.io/image/blobs"', f'dir: "{blobs}"'),
                 ("facts: '__NODE_FACTS__'", f"facts: '{facts}'"),
                 ('root: "/var/lib/nanocloud.io/image/rootfs"',
                  f'root: "{os.environ.get("IP_ROOTFS", "/var/lib/nanocloud.io/image/rootfs")}"')):
    assert img.count(old) == 1, old
    img = img.replace(old, new)
if sandbox:
    keep = lambda n: n.startswith(("pj_", "pl_", "pr_", "pi_")) and n != "pi_bad"
else:
    keep = lambda n: n.startswith("pi_") and n != "pi_bad"
kube, kw = parts(f"{root}/packaging/debian/fluxor-podlifecycle.yaml", keep)
kube = "".join(kube).replace('    blobs: "/var/lib/nanocloud.io/image/blobs"\n', f'    blobs: "{blobs}"\n')
mods = img + kube
wires = iw + kw
if sandbox:
    smods = subprocess.run(["python3", f"{root}/scripts/support/sandbox-block.py", "mods"],
                           capture_output=True, text=True, check=True).stdout
    swire = subprocess.run(["python3", f"{root}/scripts/support/sandbox-block.py", "wire"],
                           capture_output=True, text=True, check=True).stdout
    mods += smods.rstrip("\n") + "\n"
else:
    swire = ""
g = ("target: linux\ntick_us: 1000\nplatform:\n  net: {}\nscheduler:\n  accept_cycles: true\nmodules:\n"
     + mods.rstrip("\n") + "\nwiring:\n"
     + "".join(f"  - from: {a}\n    to: {b}\n" for a, b in wires) + swire)
open(out, "w").write(g)
PY
}

# ip_run <seconds> [sudo]: the graph built at $D/config.bin/$D/modules.bin, on
# the store in $D, for that long.
ip_boot() {
  if [ "${IP_SUDO:-0}" = 1 ]; then
    sudo -n env FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
      --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  else
    env FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
      --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  fi
  IP_RUNTIME=$!
}
ip_stop() {
  [ -n "${IP_RUNTIME:-}" ] || return 0
  if [ "${IP_SUDO:-0}" = 1 ]; then sudo -n kill "$IP_RUNTIME" 2>/dev/null || true
  else kill "$IP_RUNTIME" 2>/dev/null || true; fi
  for _ in $(seq 1 40); do
    if [ "${IP_SUDO:-0}" = 1 ]; then sudo -n kill -0 "$IP_RUNTIME" 2>/dev/null || break
    else kill -0 "$IP_RUNTIME" 2>/dev/null || break; fi
    sleep 0.25
  done
  IP_RUNTIME=""
}
# ip_wait <seconds> <cmd…>: until the command succeeds (the runtime alive).
ip_wait() {
  local n="$1"; shift
  local i
  for i in $(seq 1 $((n * 4))); do
    "$@" && return 0
    sleep 0.25
  done
  return 1
}
ip_cleanup() {
  ip_stop
  local p
  for p in $IP_PIDS; do kill "$p" 2>/dev/null || true; done
}
