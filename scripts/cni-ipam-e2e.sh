#!/usr/bin/env bash
# Live E2E for pod IP allocation as Chronicle params (ipam_pod.uproc,
# ipam_pod_release.uproc; graph EXTRACTED from fluxor-controlplane.yaml into
# packaging/debian/fluxor-ipam.yaml) over the fluxor-native control-plane
# store, seeded from its durable append-log ($D/store.log) at boot.
#
# Proves: an address in use before the allocator starts (a lease with no
# claim) is CLAIMED before any allocation runs, so it is never handed out
# again; a taken candidate is refused by the store (create-only claim, 409)
# and skipped; allocation is next-fit and a lease is never reassigned; the
# CIDR's first host address — the node's gateway on its segment — is never a
# candidate; a request whose sandbox is gone releases its lease, its request
# and its claim; and a request naming its node's Pod CIDR (pod_resolve's) is
# leased from that CIDR, and is not this store's to release.
#
# Compact format:
#   /ipam-pool          = "cidr=<a.b.c.0>/<prefix>"
#   /ipam-request/<id>  = ""                      (the sandbox runtime, net=own)
#                       | "node=<n>;cidr=<the node's podCIDR>;pod=<ns>/<name>"
#   /ipam-lease/<id>    = "<ip>"                  (create-only)
#   /ipam/pod/<u32>     = "owner=<id>;ip=<u32>"   (the claim)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-ipam.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_source.fmod" "$MODULES_DIR/store_effect.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-ipam-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() { # store_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 1, len(key), len(val)) + key + val)
    f.flush()
    os.fsync(f.fileno())
PY
}

store_last() { # store_last <key>
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want:
        out = v
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

ip_u32() { python3 -c "import sys;a=[int(x) for x in sys.argv[1].split('.')];print((a[0]<<24)|(a[1]<<16)|(a[2]<<8)|a[3])" "$1"; }

RUNTIME_PID=""
trap 'kill $RUNTIME_PID 2>/dev/null || true; rm -rf "$D"' EXIT
start() {
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
}
stop() { kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; }
# await <key> <want> — poll the store log until the key's latest value is <want>.
await() {
  for _ in $(seq 300); do
    [ "$(store_last "$1")" = "$2" ] && return 0
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "runtime exited while waiting for $1"
    sleep 0.1
  done
  fail "$1: got '$(store_last "$1")', want '$2' after 30s"
}

echo "== 2. seed: pool, a lease in use at .1 (pre), a LEGACY lease at .5 with no claim, four requests =="
store_put "/ipam-pool" "cidr=10.244.0.0/24"
for id in pre legacy pod1 pod2 pod3 pod4; do
  store_put "/sandboxes/$id" "net=own"
  store_put "/ipam-request/$id" ""
done
store_put "/ipam-lease/pre" "10.244.0.1"
store_put "/ipam-lease/legacy" "10.244.0.5"

echo "== 3. run the allocator: existing addresses claimed first, then next-fit =="
start
await /ipam-lease/pod4 "10.244.0.6"
stop
expect() { # expect <id> <want>
  local got; got="$(store_last "/ipam-lease/$1")"
  [ "$got" = "$2" ] || fail "lease for $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect pre    "10.244.0.1"   # in use before the allocator ran: kept, never reissued
expect legacy "10.244.0.5"   # ditto — claimed by the backfill before allocation began
expect pod1   "10.244.0.2"   # .1 is the gateway: never a candidate
expect pod2   "10.244.0.3"
expect pod3   "10.244.0.4"
expect pod4   "10.244.0.6"   # .5 refused (the backfilled claim), skipped
for pair in pre:10.244.0.1 legacy:10.244.0.5 pod1:10.244.0.2 pod4:10.244.0.6; do
  id="${pair%%:*}"; ip="${pair#*:}"; n="$(ip_u32 "$ip")"
  got="$(store_last "/ipam/pod/$n")"
  [ "$got" = "owner=$id;ip=$n" ] || fail "claim /ipam/pod/$n: got '$got', want 'owner=$id;ip=$n'"
done
echo "   every address in use holds exactly one create-only claim"

echo "== 4. release: pod2's sandbox is gone -> its lease, request and claim go; the next pod gets .7 =="
store_put "/sandboxes/pod5" "net=own"
store_put "/ipam-request/pod5" ""
python3 - "$D/store.log" "/sandboxes/pod2" <<'PY'
import struct, sys, os
path, key = sys.argv[1], sys.argv[2].encode()
with open(path, "r+b") as f:
    data = f.read(); p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
    f.flush(); os.fsync(f.fileno())
PY
start
await /ipam-lease/pod5 "10.244.0.7"
await /ipam-lease/pod2 ""
await /ipam-request/pod2 ""
await "/ipam/pod/$(ip_u32 10.244.0.3)" ""
stop
expect pod1 "10.244.0.2"     # untouched by another pod's release
echo "   pod2 released (lease, request, claim for .3); pod5 -> 10.244.0.7 (next-fit)"

echo "== 5. a node's request: leased from the CIDR it names, its gateway never; not released here =="
# The cursor stands at n=6 (six candidates tried so far): the node's /24
# gives its 2 + 6 = .8, and a /30 its one host past the gateway, .2.
store_put "/ipam-request/podn" "node=node-a;cidr=10.244.7.0/24;pod=default/podn"
store_put "/ipam-request/podt" "node=node-b;cidr=10.244.9.4/30;pod=default/podt"
start
await /ipam-lease/podn "10.244.7.8"
await /ipam-lease/podt "10.244.9.6"
# No /sandboxes/ here for either: a request naming its node is that node's
# to release (node_net), never this store's view of sandboxes.
sleep 2
stop
expect podn "10.244.7.8"
expect podt "10.244.9.6"
[ -n "$(store_last /ipam-request/podn)" ] || fail "a node's request was released by the plane's sandbox view"
echo "   node-a's /24 -> 10.244.7.8, node-b's /30 -> 10.244.9.6 (its gateway .5 skipped); both kept"

echo "== E2E green: pod IPs allocated as params — claims make every address unique =="
