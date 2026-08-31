#!/usr/bin/env bash
# Live E2E for nanocloud's admission module (modules/app/admission) — the
# mutating/validating admission gate over the control-plane store.
#
# An EXTERNAL process (this script, playing the API front after rbac_gate) seeds
# admission policies + the live objects counted for quota, and writes
# /admit-req/ with (verb, resource, ns, obj); the admission fmod validates
# required fields, applies defaults (mutating the object), and enforces quota,
# writing /admit-resp/ (200;<mutated obj> or 403;<reason>).
#
# Compact format:
#   /admit-req/<reqid>     = "verb=<v>;resource=<r>;ns=<ns>;obj=<k=v,k=v,...>"
#   /admission-policy/<r>  = "required=<field>,...;defaults=<k>=<v>,...;quota=<n>"
#   /admit-resp/<reqid>    = "200;<mutated obj>" | "403;<reason>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-admission.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/api_admission.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-adm-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -20 "$D/run.log" 2>/dev/null || true; exit 1; }

wal_put() { # wal_put <key> <value> — append a put to the store's durable log.
  # Single-writer: the runtime isn't running while we seed, so no flock needed.
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

echo "== 2. seed policies + live objects (for quota) + a request matrix =="
# Policy fields are dotted JSON paths, so `spec.image` names the field one
# level inside the object rather than a flat key that happens to be spelled
# the same.
wal_put "/admission-policy/pods"     "required=spec.image;defaults=spec.priority=0;quota=5"
wal_put "/admission-policy/services" "required=;defaults=;quota=1"
# Quota counts keys under the prefix, so the live objects' bodies are never
# read — any non-empty byte does.
wal_put "/pods/default/p1" "x"        # 2 pods exist (quota 5 → room)
wal_put "/pods/default/p2" "x"
wal_put "/services/default/s1" "x"    # 1 service exists (quota 1 → full)
wal_put "/admit-req/a1" 'verb=create;resource=pods;ns=default;obj={"spec":{"image":"nginx"}}'
wal_put "/admit-req/a2" 'verb=create;resource=pods;ns=default;obj={"spec":{"replicas":1}}'
wal_put "/admit-req/a3" 'verb=create;resource=pods;ns=default;obj={"spec":{"image":"redis","priority":9}}'
wal_put "/admit-req/a4" 'verb=create;resource=services;ns=default;obj={"spec":{"port":80}}'

echo "== 3. run the admission controller =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify each admission decision =="
expect() { # expect <reqid> <want>
  local got; got="$(wal_last "/admit-resp/$1")"
  [ "$got" = "$2" ] || fail "admission for $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
# The mutated object comes back as JSON, byte-for-byte what the store will
# store — a defaulted field is inserted at the head of the object it belongs
# to, so the expectation pins position as well as content.
expect a1 '200;{"spec":{"priority":0,"image":"nginx"}}'   # valid + defaulted
expect a2 '403;missing spec.image'                        # required field missing
expect a3 '200;{"spec":{"image":"redis","priority":9}}'   # present → no default
expect a4 '403;quota exceeded'                            # services at quota 1

echo "== E2E green: admission (validate + default + quota) over the shared store =="