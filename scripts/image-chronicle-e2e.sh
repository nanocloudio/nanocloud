#!/usr/bin/env bash
# An image reference resolved to something runnable, as params: the pull
# trigger, the two gates, and the answer. The repo/tag split, the `/`->`_`
# store-key spelling and the un-escaping of the OCI Entrypoint/Cmd lists are
# `!` projection modifiers on the connectors — transcription of the record's
# own encoding, which the VM has no bytes arithmetic to do.
#
# image_fetcher and image_assembler are stubbed here by writing what they
# would: this proves the DECISION half, on the same records they exchange.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgchron-XXXXXX)"
RUNTIME_PID=""
cleanup() { [ -n "$RUNTIME_PID" ] && kill "$RUNTIME_PID" 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

store_put() {
  python3 - "$D/store.log" "$1" "$2" <<'PY'
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
store_last() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode()
p, out = 0, None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}
# how many records were written for <key> (PUT-if-changed must not spend revisions)
store_writes() {
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = n = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want and op == 1: n += 1
    p += 15 + kl + vl
print(n)
PY
}

for e in rq ar ac rs; do
  v="$(nc_decision "$ROOT/modules/app/_chronicle/pod_image.uproc" $e)"
  [ -n "$v" ] || { echo "FAIL: pod_image.uproc $e did not compile"; exit 1; }
  eval "DEC_$(echo $e | tr a-z A-Z)=\"$v\""
done
python3 - "$ROOT/packaging/debian/fluxor-podlifecycle.yaml" "$D/graph.yaml" <<'PY2'
import re, sys
s = open(sys.argv[1]).read()
mods = re.search(r'(  # ── image:.*?)\n  # ── lifecycle', s, re.S).group(1)
wire = re.search(r'(  - from: pi_source\.status.*?buffer_group: 24)\n', s, re.S).group(1)
open(sys.argv[2], "w").write(
    "target: linux\ntick_us: 1000\nscheduler:\n  accept_cycles: true\nmodules:\n"
    + mods + "\nwiring:\n" + wire + "\n")
PY2
run() { # run <seconds>
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  sleep "$1"
  kill "$RUNTIME_PID" 2>/dev/null || true
  wait "$RUNTIME_PID" 2>/dev/null || true
  RUNTIME_PID=""
}

echo "== 1. a nested repo with a tag triggers ONE pull, keyed by the flat name =="
store_put "/pod-specs/pod-img" "image=nanocloud/hello:v2;desired=running"
store_put "/pod-specs/pod-plain" "cmd=/bin/echo hi;desired=running"
store_put "/pod-specs/pod-rootfs" "image=nginx;rootfs=/already/here;desired=running"
run 5
[ "$(store_last /image-requests/nanocloud_hello)" = "repo=nanocloud/hello;tag=v2" ] \
  || fail "pull trigger wrong: '$(store_last /image-requests/nanocloud_hello)'"
echo "   /image-requests/nanocloud_hello = repo=nanocloud/hello;tag=v2 (repo keeps the slash, the KEY does not)"
[ -z "$(store_last /image-requests/nginx)" ] || fail "an explicit rootfs must not trigger a pull"
[ -z "$(store_last /pod-resolved/pod-img)" ] || fail "nothing may resolve before the assembler is ready"
[ -z "$(store_last /pod-resolved/pod-plain)" ] || fail "a non-image pod must not resolve"
echo "   nginx (explicit rootfs) and the plain pod: no trigger, no resolve"

echo "== 2. the rootfs gate: not-ready resolves nothing =="
store_put "/image-rootfs/nanocloud_hello" "state=pending;path=/rootfs/nanocloud_hello"
run 4
[ -z "$(store_last /pod-resolved/pod-img)" ] || fail "state=pending must not resolve"
echo "   state=pending: held"

echo "== 3. ready + an OCI config: argv is Entrypoint ++ Cmd, unescaped =="
# What image_fetcher writes: `,`-joined, with %25/%2C/%3B escaped.
store_put "/image-rootfs/nanocloud_hello" "state=ready;path=/rootfs/nanocloud_hello"
store_put "/image-config/nanocloud_hello" "entrypoint=/usr/sbin/nginx;cmd=-g,--pid%2Cfile"
run 5
R="$(store_last /pod-resolved/pod-img)"
[ "$R" = "rootfs=/rootfs/nanocloud_hello;cmd=/usr/sbin/nginx -g --pid,file" ] \
  || fail "resolved wrong: '$R'"
echo "   $R"
echo "   (Entrypoint ++ Cmd, ',' tokens split, '%2C' restored to a literal comma)"

echo "== 4. an explicit spec cmd wins outright over the image's config =="
store_put "/pod-specs/pod-ovr" "image=nanocloud/hello:v2;cmd=/bin/sh -c true;desired=running"
run 5
[ "$(store_last /pod-resolved/pod-ovr)" = "rootfs=/rootfs/nanocloud_hello;cmd=/bin/sh -c true" ] \
  || fail "explicit cmd should win: '$(store_last /pod-resolved/pod-ovr)'"
echo "   pod-ovr keeps its own argv"

echo "== E2E green: an image reference resolved to a rootfs and an argv — the trigger, the gates, the un-escaping =="
