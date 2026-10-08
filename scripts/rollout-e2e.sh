#!/usr/bin/env bash
# Live E2E for Deployment rollout (rolling update) over the fluxor-native store.
# the Deployment chain stamps a pod-template-hash (a digest of spec.template) on
# the ReplicaSet; the rs_ chain stamps it on each Pod. When the template
# changes (image nginx → httpd) the hash changes, so running Pods become stale
# and are rolled — deleted (bounded per pass = a gradual rolling update) and
# recreated from the new template. Proven on real binaries in ONE process.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-workload.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-rollout-e2e-XXXXXX)"
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
    f.flush(); os.fsync(f.fileno())
PY
}

# The store is this node's replica: read between runs, as that replica.
store_last() { nc_replica_get "$D" "$1"; }

run() { env -C "$D" RUST_LOG=warn timeout 5 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true; }

echo "== 1. build config + module table =="
# The reconcilers run as this node's replica of the store (the packaged
# fragment spliced into the workload graph), and so does the CLI: one
# store, held by one process at a time. Seeds are appended to the host log,
# which each start imports through the replica's log.
python3 - "$GRAPH" "$D/workload-template.yaml" <<'PY'
import sys
g = open(sys.argv[1]).read()
assert "\nmodules:\n" in g and "\nwiring:\n" in g
if "\nplatform:\n" not in g:
    g = g.replace("\nmodules:\n", "\nplatform:\n  net: {}\n\nmodules:\n", 1)
g = g.replace("\nmodules:\n", "\nmodules:\n__REPLICA_MODULES__\n", 1)
g = g.replace("\nwiring:\n", "\nwiring:\n__REPLICA_WIRING__\n", 1)
open(sys.argv[2], "w").write(g)
PY
nc_node_pki "$D" || fail "no node PKI"
mkdir -p "$D/lattice"
nc_render_graph "$D" "$D/workload-template.yaml" "$D/workload.yaml" || fail "the workload graph did not render"
nc_build_workload "$ROOT" "$D/workload.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 2. rollout v1: Deployment web (replicas=2, image=nginx) =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run
RS="$(store_last /replicasets.apps/default/web)"
case "$RS" in *'"pod-template-hash"'*) : ;; *) fail "RS has no pod-template-hash: got '$RS'";; esac
H1="$(printf '%s' "$RS" | sed -n 's/.*"pod-template-hash":"\([0-9a-f]*\)".*/\1/p')"
for i in 0 1; do
  v="$(store_last /pods/default/web-$i)"
  case "$v" in *nginx*"$H1"*|*"$H1"*nginx*) : ;; *) fail "v1 pod web-$i not nginx+hash: got '$v'";; esac
done
echo "   v1 pods: image=nginx, pod-template-hash=$H1"

echo "== 3. rollout v2: change the image (nginx -> httpd) =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"name":"web","image":"httpd"}]}}}}'
run
RS2="$(store_last /replicasets.apps/default/web)"
H2="$(printf '%s' "$RS2" | sed -n 's/.*"pod-template-hash":"\([0-9a-f]*\)".*/\1/p')"
[ -n "$H2" ] && [ "$H2" != "$H1" ] || fail "pod-template-hash did not change on the template edit ($H1 -> $H2)"
for i in 0 1; do
  v="$(store_last /pods/default/web-$i)"
  case "$v" in *httpd*) : ;; *) fail "v2 pod web-$i not rolled to httpd: got '$v'";; esac
  case "$v" in *"$H2"*) : ;; *) fail "v2 pod web-$i not stamped with the new hash: got '$v'";; esac
done
echo "   v2 pods rolled: image=httpd, pod-template-hash=$H2 (was $H1)"

echo "== 4. rollout undo: the previous (nginx) revision was archived =="
PREV="$(store_last /controllerrevisions.apps/default/web/previous)"
case "$PREV" in *nginx*) : ;; *) fail "no archived previous revision: '$PREV'" ;; esac
echo "   archived previous revision (nginx) at /controllerrevisions.apps/default/web/previous"

# The CLI verbs run under the cli graph.
nc_cli_graph "$D" "$D/cli.yaml" || fail "the CLI did not render"
nc_build_workload "$ROOT" "$D/cli.yaml" "$D/cli-config.bin" "$D/cli-modules.bin"
cli() { env -C "$D" "$FLUXOR_RUNTIME" --config "$D/cli-config.bin" --modules "$D/cli-modules.bin" -- "$@" </dev/null 2>/dev/null; }

echo "== 5. nanocloud rollout status default/web =="
ST="$(cli rollout status default/web)"
case "$ST" in *"$H2"*) echo "   $ST" ;; *) fail "rollout status wrong: '$ST'" ;; esac

echo "== 6. nanocloud rollout undo default/web → restores the nginx template =="
cli rollout undo default/web | sed 's/^/   /'
DEP="$(store_last /deployments.apps/default/web)"
case "$DEP" in *nginx*) : ;; *) fail "undo did not restore the nginx spec: '$DEP'" ;; esac

echo "== 7. re-run: the Pods roll BACK to nginx =="
run
for i in 0 1; do
  v="$(store_last /pods/default/web-$i)"
  case "$v" in *nginx*) : ;; *) fail "post-undo pod web-$i not rolled back to nginx: got '$v'";; esac
done
echo "   pods rolled back to nginx (image=$(store_last /pods/default/web-0 | sed -n 's/.*"image":"\([^"]*\)".*/\1/p'))"

echo "== E2E green: Deployment rolling update + rollout status + undo (rollback) =="
