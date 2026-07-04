#!/usr/bin/env bash
# Live E2E for the fluxor-native nanocloud CLI:
# the `nanocloud` command is a cli-applet whose LOGIC is a PIC fmod
# (modules/app/nanocloud_cli), dispatching subcommands (status, get, describe,
# create, apply, delete, scale, help) over
# the `cli` host surface (cli_in/cli_out). It reads argv, drives the
# control-plane store via storage.namespace LIST, writes output to stdout,
# latches an exit code, and returns Done so the run-to-completion CLI exits.
#
# Proves the CLI is fluxor-native: nanocloud commands are a fmod graph, not
# traditional clap handlers. Run as `fluxor exec nanocloud -- <cmd>` (busybox
# symlink → `nanocloud <cmd>`).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/cli/linux.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/nanocloud_cli.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-cli-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; echo "--- got ---"; cat "$D/out.txt" 2>/dev/null; exit 1; }

echo "== 1. seed a control-plane store: 2 nodes, 1 deployment, 1 replicaset, 3 pods =="
python3 - "$D/store.log" <<'PY'
import struct, os
def rec(f,rev,k,v): f.write(struct.pack("<QBHI",rev,1,len(k),len(v))+k+v)
with open(os.sys.argv[1],"wb") as f:
    rec(f,1,b"/nodes/node-1",b"ready=1"); rec(f,2,b"/nodes/node-2",b"ready=1")
    rec(f,3,b"/deployments.apps/default/web",b'{"metadata":{"name":"web"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"image":"nginx"}]}}}}')
    rec(f,4,b"/replicasets.apps/default/web",b"replicas=3;owner=web")
    rec(f,5,b"/pods/default/web-0",b"image=nginx;node=node-1")
    rec(f,6,b"/pods/default/web-1",b"image=nginx;node=node-2")
    rec(f,7,b"/pods/default/web-2",b"image=nginx;node=node-1")
    # a sandbox's captured stdout/stderr + a terminal status (what sandbox_runner
    # publishes by draining the workload READ op) — for the `logs` verb.
    rec(f,8,b"/sandbox-logs/pod-uid-1",b"starting nginx\nready\n")
    rec(f,9,b"/sandbox-status/pod-uid-1",b"state=exited;code=0")
    # a CA cert (what cert_manager publishes) — for the `ca` verb.
    rec(f,10,b"/ca-cert",b"3082deadbeefca")
    # networkpolicy + compiled dataplane, a PVC, a device — for policy/volume/get.
    rec(f,11,b"/networkpolicies/default/allow-web",b'{"metadata":{"name":"allow-web"},"spec":{"podSelector":{}}}')
    rec(f,12,b"/dataplane/netpolicy",b"table inet nc { chain in { } }")
    rec(f,13,b"/persistentvolumeclaims/default/data",b'{"metadata":{"name":"data"},"spec":{"resources":{"requests":{"storage":"1Gi"}}}}')
    rec(f,14,b"/devices.nanocloud.io/default/sensor1",b'{"metadata":{"name":"sensor1"},"spec":{"kind":"sensor"}}')
PY

echo "== 2. build config + module table (cli stack expands cli_in/cli_out) =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

run() { # run <expected-exit> <args...>
  local want="$1"; shift
  local rc=0
  FLUXOR_STORE_DIR="$D" "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
    -- "$@" >"$D/out.txt" 2>/dev/null || rc=$?
  [ "$rc" = "$want" ] || fail "nanocloud $* — exit $rc, want $want"
}

echo "== 3. nanocloud status =="
run 0 status
grep -qE "nodes:[[:space:]]+2"       "$D/out.txt" || fail "status nodes"
grep -qE "deployments:[[:space:]]+1" "$D/out.txt" || fail "status deployments"
grep -qE "pods:[[:space:]]+3"        "$D/out.txt" || fail "status pods"
sed 's/^/   /' "$D/out.txt"

echo "== 4. nanocloud get pods =="
run 0 get pods
for name in web-0 web-1 web-2; do grep -qx "$name" "$D/out.txt" || fail "get pods missing $name"; done
sed 's/^/   /' "$D/out.txt"

echo "== 5. nanocloud get nodes =="
run 0 get nodes
grep -qx "node-1" "$D/out.txt" && grep -qx "node-2" "$D/out.txt" || fail "get nodes"

echo "== 6. nanocloud get pods default/web-0 (single object's fields) =="
run 0 get pods default/web-0
grep -q "image=nginx" "$D/out.txt" || fail "get single object fields"
sed 's/^/   /' "$D/out.txt"

echo "== 7. nanocloud delete pods default/web-1 (a store mutation) =="
run 0 delete pods default/web-1
grep -q "deleted" "$D/out.txt" || fail "delete report"
# the mutation persists to the durable log → a fresh get no longer lists web-1.
run 0 get pods
grep -qx "web-1" "$D/out.txt" && fail "web-1 still present after delete"
grep -qx "web-0" "$D/out.txt" || fail "web-0 wrongly removed"
echo "   web-1 deleted; web-0, web-2 remain"

echo "== 8. nanocloud scale deployments default/web 5 (read-modify-write) =="
run 0 scale deployments default/web 5
grep -q "scaled to 5" "$D/out.txt" || fail "scale report"
run 0 get deployments default/web
grep -q '"replicas":5' "$D/out.txt" || fail "scale did not set replicas=5"
grep -q '"image":"nginx"' "$D/out.txt" || fail "scale dropped the image field"
echo "   replicas=2 -> 5 (image preserved)"

API_JSON='{"metadata":{"name":"api"},"spec":{"replicas":2,"template":{"spec":{"containers":[{"image":"httpd"}]}}}}'
echo "== 9. nanocloud create deployments default/api (a new store object) =="
run 0 create deployments default/api "$API_JSON"
grep -q "created" "$D/out.txt" || fail "create report"
run 0 get deployments
grep -qx "api" "$D/out.txt" || fail "created object not listed"
grep -qx "web" "$D/out.txt" || fail "existing object dropped"
# create is not idempotent — a second create of the same key fails.
run 1 create deployments default/api "$API_JSON"
grep -q "already exists" "$D/out.txt" || fail "second create should report already exists"
echo "   created default/api; re-create refused"

echo "== 10. nanocloud apply (upsert): configured for existing, created for new =="
API_JSON5='{"metadata":{"name":"api"},"spec":{"replicas":5,"template":{"spec":{"containers":[{"image":"httpd"}]}}}}'
run 0 apply deployments default/api "$API_JSON5"
grep -q "configured" "$D/out.txt" || fail "apply of existing should be configured"
run 0 get deployments default/api
grep -q '"replicas":5' "$D/out.txt" || fail "apply did not replace the object"
run 0 apply services default/svc '{"metadata":{"name":"svc"},"spec":{"clusterIP":"10.0.0.1"}}'
grep -q "created" "$D/out.txt" || fail "apply of new object should be created"
echo "   apply upserted default/api (replicas=5) and created default/svc"

echo "== 11. nanocloud describe deployments default/web (labelled read) =="
run 0 describe deployments default/web
grep -qE "^Name:[[:space:]]+web"         "$D/out.txt" || fail "describe Name header"
grep -qE "^Namespace:[[:space:]]+default" "$D/out.txt" || fail "describe Namespace header"
grep -q '"image":"nginx"'                 "$D/out.txt" || fail "describe should include the body"
sed 's/^/   /' "$D/out.txt"

echo "== 12. nanocloud logs <id> (sandbox stdout/stderr from the store) =="
run 0 logs pod-uid-1
grep -q "starting nginx" "$D/out.txt" || fail "logs missing captured stdout"
grep -q "ready"          "$D/out.txt" || fail "logs missing second line"
sed 's/^/   /' "$D/out.txt"
run 0 logs no-such-pod
grep -q "(no logs)" "$D/out.txt" || fail "logs of unknown id should say (no logs)"

echo "== 13. nanocloud logs -f <id> (follow; exits when the sandbox is terminal) =="
run 0 logs -f pod-uid-1
grep -q "starting nginx" "$D/out.txt" || fail "logs -f did not stream the captured output"
grep -q "ready"          "$D/out.txt" || fail "logs -f missing second line"
echo "   streamed to end; exited 0 on terminal status"

echo "== 14. nanocloud exec <id> -- <cmd> (arms request + polls; times out sans runner) =="
# No sandbox_runner runs in this isolated CLI graph, so the exec request is
# written to the store but never serviced → the CLI polls to its backstop and
# reports a timeout (exit 1). The full exec round-trip (real command run inside a
# sandbox, exit code + output captured) is proven in sandbox-runner-e2e.
run 1 exec pod-uid-1 -- /bin/echo hi
grep -q "timed out" "$D/out.txt" || fail "exec with no runner should report a timeout"
echo "   exec armed + polled + timed out (exit 1); round-trip proven in sandbox-runner-e2e"

echo "== 15. nanocloud exec -it <id> -- <cmd> (opens a pty session; live-runner feature) =="
# No sandbox_runner runs here, so -it opens the session (writes the interactive
# ctl) then streams; we time-box it and assert the ctl was written. The full pty
# round-trip (stdin<->pty<->stdout) is proven in sandbox-runner-e2e.
timeout 2 env FLUXOR_STORE_DIR="$D" "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" \
  -- exec -it pod-uid-1 -- /bin/sh </dev/null >/dev/null 2>&1 || true
CTL="$(python3 - "$D/store.log" /tty/pod-uid-1/cli/ctl <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = 0; out = ""
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want: out = v.decode("utf-8", "replace")
    p += 15 + kl + vl
print(out)
PY
)"
case "$CTL" in
  open*cmd=/bin/sh*) echo "   -it opened session ctl: '$CTL'" ;;
  *) fail "exec -it did not write the interactive ctl key: got '$CTL'" ;;
esac

echo "== 16. nanocloud ca (cluster CA certificate, published by cert_manager) =="
run 0 ca
grep -q "3082deadbeefca" "$D/out.txt" || fail "ca did not print the CA cert: got '$(cat "$D/out.txt")'"
echo "   ca: $(cat "$D/out.txt")"

echo "== 17. nanocloud token <sa> (mint request + poll; times out sans sa_token) =="
# No sa_token runs here, so the request is written but never serviced → timeout.
# The real ES256 JWT mint is proven in sa-token-e2e.sh.
run 1 token builder default
grep -q "timed out" "$D/out.txt" || fail "token with no runner should report a timeout"
echo "   token armed + polled + timed out (exit 1); mint proven in sa-token-e2e"

echo "== 18. store-native admin verbs: diagnostics / policy / volume / config / get devices =="
run 0 diagnostics
grep -q "control-plane store: reachable" "$D/out.txt" || fail "diagnostics missing health line"
grep -qE "pods +2" "$D/out.txt" || fail "diagnostics wrong pod count: $(cat "$D/out.txt")"
run 0 policy
grep -q "allow-web" "$D/out.txt" || fail "policy did not list networkpolicies"
grep -q "table inet nc" "$D/out.txt" || fail "policy did not show the compiled dataplane"
run 0 volume
grep -q "data" "$D/out.txt" || fail "volume did not list the PVC"
run 0 config
grep -q "in-cluster" "$D/out.txt" || fail "config output wrong"
run 0 get devices
grep -qx "sensor1" "$D/out.txt" || fail "get devices did not list sensor1"
run 0 get networkpolicies
grep -qx "allow-web" "$D/out.txt" || fail "get networkpolicies did not list allow-web"
echo "   diagnostics/policy/volume/config/devices/networkpolicies all served from the store"

echo "== 19. nanocloud bundle export (dump every object as key\\tjson lines) =="
run 0 bundle export
grep -q "/pods/default/web-0	" "$D/out.txt" || fail "bundle export missing a pod: $(head -3 "$D/out.txt")"
grep -q "/deployments.apps/default/web	" "$D/out.txt" || fail "bundle export missing the deployment"
grep -q "/ca-cert" "$D/out.txt" && fail "bundle export should only dump resource objects, not /ca-cert"
echo "   exported $(wc -l < "$D/out.txt") objects"

echo "== 20. nanocloud watch <resource> (stream a listing; kubectl get -w semantics) =="
# watch runs until killed; time-box it and assert it emitted the initial snapshot.
timeout 2 env FLUXOR_STORE_DIR="$D" "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" -- watch pods </dev/null >"$D/watch.txt" 2>/dev/null || true
grep -q "^---- pods ----" "$D/watch.txt" || fail "watch did not emit a snapshot header: $(cat "$D/watch.txt")"
grep -qx "web-0" "$D/watch.txt" || fail "watch snapshot missing web-0"
echo "   watch streamed the pods snapshot"

echo "== 21. nanocloud bundle apply (round-trip: export | apply into a fresh store) =="
FLUXOR_STORE_DIR="$D" "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
  -- bundle export </dev/null >"$D/bundle.txt" 2>/dev/null || true
D2="$(mktemp -d /tmp/nc-cli-restore-XXXXXX)"; trap 'rm -rf "$D" "$D2"' EXIT
FLUXOR_STORE_DIR="$D2" "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
  -- bundle apply <"$D/bundle.txt" >"$D2/apply.txt" 2>/dev/null || true
grep -q "applied [1-9]" "$D2/apply.txt" || fail "bundle apply did not apply objects: $(cat "$D2/apply.txt")"
# the restored store now serves the same pods.
FLUXOR_STORE_DIR="$D2" "$FLUXOR_RUNTIME" --config "$D/config.bin" --modules "$D/modules.bin" \
  -- get pods </dev/null >"$D2/pods.txt" 2>/dev/null || true
grep -qx "web-0" "$D2/pods.txt" || fail "restored store missing web-0: $(cat "$D2/pods.txt")"
echo "   $(cat "$D2/apply.txt" | tr -d '\n') → restored store serves web-0"

echo "== 22. errors: unknown command / resource exit non-zero; help exits 0 =="
run 0 help
grep -q "commands:" "$D/out.txt" || fail "help output"
run 1 bogus
grep -q "unknown command" "$D/out.txt" || fail "bogus should report unknown command"
run 1 get widgets
grep -q "unknown resource" "$D/out.txt" || fail "get widgets should report unknown resource"
run 1 create pods default/bad "not json"
grep -q "must be a JSON object" "$D/out.txt" || fail "create with a non-JSON body should be rejected"

echo "== E2E green: the nanocloud CLI runs as a fluxor cli-applet (dispatch = PIC fmod logic) =="
