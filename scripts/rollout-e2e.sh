#!/usr/bin/env bash
# Live E2E for Deployment rollout (rolling update) over the fluxor-native store.
# deployment_reconciler stamps a pod-template-hash (a digest of spec.template) on
# the ReplicaSet; replicaset_reconciler stamps it on each Pod. When the template
# changes (image nginx → httpd) the hash changes, so running Pods become stale
# and are rolled — deleted (bounded per pass = a gradual rolling update) and
# recreated from the new template. Proven on real binaries in ONE process.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-workload.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/deployment_reconciler.fmod" \
         "$MODULES_DIR/replicaset_reconciler.fmod" "$GRAPH"; do
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
    if data[p+15:p+15+kl] == want:
        out = data[p+15+kl:p+15+kl+vl] if op == 1 else b""
    p += 15 + kl + vl
print(out.decode() if out else "")
PY
}

run() { FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true; }

echo "== 1. build config + module table =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

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
CLI_GRAPH="$ROOT/packaging/cli/linux.yaml"
nc_build_workload "$ROOT" "$CLI_GRAPH" "$D/cli-config.bin" "$D/cli-modules.bin"
cli() { FLUXOR_STORE_DIR="$D" "$FLUXOR_RUNTIME" --config "$D/cli-config.bin" --modules "$D/cli-modules.bin" -- "$@" </dev/null 2>/dev/null; }

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
