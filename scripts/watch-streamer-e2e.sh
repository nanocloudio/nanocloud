#!/usr/bin/env bash
# Live E2E for nanocloud's watch_streamer module (modules/app/watch_streamer) —
# ?watch=true as a projection of the store's revisions. A k8s watch is a pure
# store transform: LIST a prefix at a fence, then the entries whose revision is
# newer than the client's `since`, plus the new fence.
#
# An EXTERNAL process (this script, playing the API front) writes objects and
# /watch-req/ requests; the watch_streamer fmod LISTs the prefix and writes
# /watch-resp/ with the PUT events newer than `since` plus the fence.
#
# Compact format:
#   /watch-req/<reqid>  = "prefix=<p>;since=<rev>"
#   /watch-resp/<reqid> = "rev=<fence>;events=PUT:<name>:<rev>,..."
#
# Because every wal_put assigns the next revision, the seed order below fixes the
# revisions: a=1, b=2, w1=3, c=4, w2=5. So w1 (since=0) sees all of a,b,c and
# w2 (since=3) sees only c (rev4 > 3) — a watch resumed at a fence.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-watch.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/watch_streamer.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-watch-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, fcntl, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
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
    fcntl.flock(f, fcntl.LOCK_UN)
PY
}

wal_delete() { # wal_delete <key> — append a tombstone (WAL op 2, no value)
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys, fcntl, os
path, key = sys.argv[1], sys.argv[2].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
    data = f.read()
    p, last = 0, 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data):
            break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p)
    f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, 2, len(key), 0) + key)
    f.flush()
    os.fsync(f.fileno())
    fcntl.flock(f, fcntl.LOCK_UN)
PY
}

wal_rev() { # wal_rev <key> — revision of the last WAL record for a key
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p, out = 0, 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    if data[p+15:p+15+kl] == want:
        out = rev
    p += 15 + kl + vl
print(out)
PY
}

wal_last() { # wal_last <key>
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

echo "== 2. seed objects + watch requests (revs: a=1,b=2,w1=3,c=4,w2=5) =="
wal_put "/pods/default/a" "spec=a"
wal_put "/pods/default/b" "spec=b"
wal_put "/watch-req/w1" "prefix=/pods/;since=0"
wal_put "/pods/default/c" "spec=c"
wal_put "/watch-req/w2" "prefix=/pods/;since=3"

run_runtime() {
  FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 4 "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}
expect() { # expect <reqid> <want>
  local got; got="$(wal_last "/watch-resp/$1")"
  [ "$got" = "$2" ] || fail "watch-resp $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}

echo "== 3. run watch_streamer (phase A: snapshot + incremental adds) =="
run_runtime

echo "== 4. verify the add/snapshot projections =="
# w1 (since=0): the full initial state — every pod as a PUT, fence at the newest.
expect w1 "rev=4;events=PUT:a:1,PUT:b:2,PUT:c:4"
# w2 (since=3): resumed at fence 3 — only c (rev4 > 3) is new (a PUT).
expect w2 "rev=4;events=PUT:c:4"

echo "== 5. phase B: delete a pod, then watch resumes past the delete =="
# The delete happens AFTER w1/w2 were answered — so it doesn't retroactively
# appear in their replay. A watch resumed past it observes a DELETE event, which
# a LIST-only projection could never surface (the key is gone from the store).
wal_delete "/pods/default/a"
del_rev="$(wal_rev /pods/default/a)"
wal_put "/watch-req/w3" "prefix=/pods/;since=5"
run_runtime

echo "== 6. verify the DELETE projection =="
# w3 (since=5): the only /pods/ change past rev 5 is a's tombstone → a DELETE.
expect w3 "rev=${del_rev};events=DELETE:a:${del_rev}"

echo "== E2E green: ?watch=true projects adds AND deletes from the store =="
