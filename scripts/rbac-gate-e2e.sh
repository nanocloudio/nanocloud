#!/usr/bin/env bash
# Live E2E for nanocloud's rbac_gate module (modules/app/rbac_gate) — the RBAC
# authorization decision over the control-plane store.
#
# An EXTERNAL process (this script, playing the API front after authn) seeds
# Roles + RoleBindings and writes /authz-req/ with (identity, verb, resource);
# the rbac_gate fmod resolves bindings → roles → rules and writes /authz-resp/
# (200;allow or 403;deny). Deny by default; `*` wildcards on verb/resource.
#
# Compact format:
#   /authz-req/<reqid>   = "id=<identity>;verb=<verb>;resource=<resource>"
#   /rolebindings/<name> = "subjects=<id>,<id>,...;role=<rolename>"
#   /roles/<name>        = "rules=<verb>:<resource>,..."
#   /authz-resp/<reqid>  = "200;allow" | "403;deny"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-rbac.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/rbac_gate.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-rbac-e2e-XXXXXX)"
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

echo "== 2. seed Roles + RoleBindings + an authorization matrix =="
store_put "/roles/pod-reader" "rules=get:pods,list:pods"
store_put "/roles/admin"      "rules=*:*"
store_put "/rolebindings/rb1" "subjects=web,api;role=pod-reader"
store_put "/rolebindings/rb2" "subjects=root;role=admin"
store_put "/authz-req/q1" "id=web;verb=get;resource=pods"        # allowed by pod-reader
store_put "/authz-req/q2" "id=web;verb=create;resource=pods"     # verb not granted
store_put "/authz-req/q3" "id=web;verb=get;resource=services"    # resource not granted
store_put "/authz-req/q4" "id=api;verb=list;resource=pods"       # api also bound to pod-reader
store_put "/authz-req/q5" "id=root;verb=delete;resource=secrets" # admin *:*
store_put "/authz-req/q6" "id=nobody;verb=get;resource=pods"     # no binding

echo "== 3. run the authorizer =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 2.5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify each authorization decision =="
expect() { # expect <reqid> <want>
  local got; got="$(store_last "/authz-resp/$1")"
  [ "$got" = "$2" ] || fail "authz for $1 wrong: got '$got', want '$2'"
  echo "   $1: $got"
}
expect q1 "200;allow"   # get pods (pod-reader)
expect q2 "403;deny"    # create pods (not granted)
expect q3 "403;deny"    # get services (not granted)
expect q4 "200;allow"   # api list pods
expect q5 "200;allow"   # root delete secrets (admin *:*)
expect q6 "403;deny"    # unbound identity

echo "== E2E green: RBAC authorization (bindings→roles→rules, * wildcards) =="