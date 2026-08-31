#!/usr/bin/env bash
# Live E2E for nanocloud's webhook_validator module (modules/app/
# webhook_validator) — a validation reconciler over the ONE shared store.
#
# Proves webhook status on the store-watch seam with real binaries: an EXTERNAL
# process (this script, playing the webhook projection) writes Webhook specs into the shared
# WAL; the validator fmod, inside the fluxor-linux runtime, validates each and
# writes /webhook-status/<ns>/<name> = "ready=1" or "ready=0;msg=<issues>".
#
# Compact format:
#   /webhooks/<ns>/<name>       = "path=<p>;has_secret=<0|1>;secret_name=<n>;
#                                  secret_key=<k>;hmac=<0|1>;containers=<N>"
#   /webhook-status/<ns>/<name> = "ready=1" | "ready=0;msg=<issues>"
#
# Prereqs: `fluxor modules build --target bcm2712` in this repo, and a
# fluxor-linux runtime. FLUXOR_RUNTIME overrides the runtime path; it defaults
# to the fluxor checkout's release build.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-webhook.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/store_source.fmod" "$MODULES_DIR/decision.fmod" "$MODULES_DIR/store_effect.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-wh-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

# Append one put record to the store's durable log (same wire the store writes:
# [rev:u64][op:u8=1][key_len:u16][val_len:u32][key][val]). Single-writer here —
# no flock, because nothing else has the file open (the runtime is not running).
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
    f.flush(); os.fsync(f.fileno())
PY
}

store_last() { # store_last <key>  — last-writer value of a key in the log
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
        out = v if op == 1 else None
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project webhooks: valid, secret-valid, and three invalid cases =="
store_put "/webhooks/default/ok"        "path=/hook;containers=1"
store_put "/webhooks/default/ok-secret" "path=/hook;has_secret=1;secret_name=s;secret_key=k;hmac=1;containers=1"
store_put "/webhooks/default/bad-path"  "path=hook;containers=1"
store_put "/webhooks/default/no-cont"   "path=/hook;containers=0"
store_put "/webhooks/default/no-hmac"   "path=/hook;has_secret=1;secret_name=s;secret_key=k;containers=1"

echo "== 3. run the validator =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify each webhook's computed status =="
expect() { # expect <name> <want>
  local got; got="$(store_last "/webhook-status/default/$1")"
  [ "$got" = "$2" ] || fail "webhook '$1' status wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect ok        "ready=1"
expect ok-secret "ready=1"
expect bad-path   "ready=0;msg=path must start with '/'"
expect no-cont    "ready=0;msg=job template must have at least one container"
expect no-hmac    "ready=0;msg=hmac_header must be set when secretRef is set"

echo "== phase P: a prefix larger than ONE LIST page must be reconciled WHOLE =="
# Regression guard for the shared listing walk (modules/app/_shared/store.rs).
#
# storage.namespace LIST answers one page at a time and appends a cursor when
# more remain. A helper that sends cursor_len = 0 and discards that cursor sees
# page one and reports it as the whole listing — a SILENT PARTIAL reconcile in
# which the objects past page one are never seen again, because nothing
# re-lists a prefix that stops changing.
#
# The shared helper reads 512-byte pages into the caller's 2 KiB buffer. A
# `/webhooks/default/pgNNN` key is ~24 bytes, so ~18 fit per page and ~59 fit in
# `out` after the 10-byte-per-entry repack. 50 spans about three pages — more
# than one, which is the whole point — and stays inside what one pass can hold.
N=50
python3 - "$D/store.log" "$N" <<'PY'
import struct, sys, os
path, n = sys.argv[1], int(sys.argv[2])
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    for i in range(n):
        key = ("/webhooks/default/pg%03d" % i).encode()
        val = b"path=/hook;containers=1"
        last += 1
        f.write(struct.pack("<QBHI", last, 1, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 6 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
missing=0
for i in $(seq 0 $((N-1))); do
  printf -v nm "pg%03d" "$i"
  [ -n "$(store_last "/webhook-status/default/$nm")" ] || missing=$((missing+1))
done
[ "$missing" -eq 0 ] || fail "$missing of $N webhooks past the first LIST page were never validated"
echo "   all $N validated — the listing paged to the end"

echo "== E2E green: webhook validation over the shared store =="
