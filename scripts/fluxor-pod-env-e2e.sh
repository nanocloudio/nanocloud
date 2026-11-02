#!/usr/bin/env bash
# Live E2E: a graph-backed Pod's ENVIRONMENT and VOLUMES reach its graph — a
# Pod `runtimeClassName: fluxor` of a workload image whose module's params are
# bound to names (`{env: NAME}` in the workload graph), on a node in the
# bare-metal shape (scripts/workload-image-e2e.sh) with its volume chain:
#
#   the container's env, resolved on the plane as a container's is
#   (pod_resolve: a ConfigMap key, a Secret key, the downward API's
#   metadata.name and spec.nodeName) -> /pod-specs/<uid> env= ->
#   /metal-sandboxes/<uid> env= -> the CREATE's `env`/`var` entries -> the
#   params the graph binds by name (the kernel's bind_env)
#   its volumes (a Secret, read-only; a PersistentVolumeClaim bound to a
#   hostPath PersistentVolume) -> /volume-binds/<uid>/<i> -> the CREATE's
#   `mount`/`bind` entries -> the module's `fs` paths beneath them
#
# pod_env_probe (modules/fixtures) logs its params, reads a file of the
# Secret, fails to write beside it (read-only: -13), and counts a file of the
# claim up. The Secret's volume is upstream's AtomicWriter layout (each key a
# link through `..data`, which an update swaps whole), which the runtime lends
# following a link that stays beneath the volume: the graph reads
# `<mount>/key` through it, and the Secret updated, reads the new value from
# the same path. The Pod waits, refused with why, while its ConfigMap is
# missing, and runs once it is created; deleted and created again it counts
# 2 — its state lived on the claim. A Graph whose `spec.env` (the build's
# workload-image.json list) binds a name the container does not set is
# refused before its node creates anything (CreateContainerConfigError), and
# so is a Pod of the PULLED image that does not set one its config blob
# (application/vnd.nanocloud.fluxor.workload-image.config.v1+json) lists as
# required: the plane reads the bindings its node's pull recorded and
# refuses it, and the node makes no sandbox before the plane has checked.
# A Pod that keeps its OWN keys names its vault in its environment
# (FLUXOR_VAULT_DIR on its writable claim, FLUXOR_VAULT_KEY from a Secret):
# its CREATE carries Fluxor's `vault`/`store` option and it runs with the
# vault made on the claim; one that names the directory without the key is
# refused, never run with its keys in the node's store. (The running half
# needs Fluxor's workload vault in the pinned runtime; without it the step
# says so and is skipped.)
#
# PASS = each of those, as the module and the Pod's status say it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
REG_PORT=5396

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/workload-node.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }

D="$(mktemp -d /tmp/nc-podenv-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs" "$D/mnt" "$D/pv"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
REG_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  [ -n "$REG_PID" ] && kill "$REG_PID" 2>/dev/null || true
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. the workload image: pod_env_probe, its params bound to the Pod's environment =="
cat >"$D/envp.yaml" <<'YAML'
target: linux
tick_us: 1000
modules: []
wiring: []
workloads:
  - modules:
      - name: envp
        type: pod_env_probe
        pod: {env: POD_NAME}
        node: {env: NODE_NAME}
        cell: {env: CELL}
        token: {env: TOKEN}
        secret_file: "/run/enrol/key"
        state_file: "/var/lib/store/count"
YAML
wn_publish envp "$D/envp.yaml"
python3 - "$D/wimg-envp/workload-image.json" <<'PY' || fail "workload-image.json does not list the four bindings, required"
import json, sys
env = json.load(open(sys.argv[1]))["env"]
want = {("POD_NAME", "pod"), ("NODE_NAME", "node"), ("CELL", "cell"), ("TOKEN", "token")}
got = {(e["name"], e["param"]) for e in env if e["module"] == "pod_env_probe" and e["required"]}
sys.exit(0 if got == want else 1)
PY
echo "   workload-image.json env: POD_NAME NODE_NAME CELL TOKEN (required)"
python3 - "$D/reg/envp" <<'PY' || fail "the image's config blob is not its env bindings"
import json, os, sys
reg = sys.argv[1]
m = json.load(open(os.path.join(reg, "manifest.json")))
assert m["config"]["mediaType"] == "application/vnd.nanocloud.fluxor.workload-image.config.v1+json", m["config"]
cfg = json.load(open(os.path.join(reg, m["config"]["digest"][7:])))
assert {e["name"] for e in cfg["env"] if e["required"]} == {"POD_NAME", "NODE_NAME", "CELL", "TOKEN"}, cfg
PY
echo "   config blob (workload-image.config.v1+json): the same four, required"
wn_registry "$REG_PORT"

echo "== 2. seed: the operator, a ready node, the Pod's Secrets, a hostPath volume and its claim =="
wn_seed_identity
put_text "/secrets/default/tok" '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"tok","namespace":"default"},"data":{"token":"'"$(printf 'tok-123' | base64)"'"}}'
put_text "/secrets/default/vault" '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"vault","namespace":"default"},"data":{"seal":"'"$(printf '00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff\n' | base64 -w0)"'"}}'
put_text "/secrets/default/enrol" '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"enrol","namespace":"default"},"data":{"key":"'"$(printf 'enrol-key-1\n' | base64)"'"}}'
put_text "/persistentvolumes/store-pv" '{"apiVersion":"v1","kind":"PersistentVolume","metadata":{"name":"store-pv"},"spec":{"capacity":{"storage":"1Gi"},"accessModes":["ReadWriteOnce"],"hostPath":{"path":"'"$D/pv"'"},"claimRef":{"namespace":"default","name":"store"}}}'
put_text "/persistentvolumeclaims/default/store" '{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"store","namespace":"default"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}},"volumeName":"store-pv"}}'
# A Graph whose build binds TOKEN, as workload-image.json lists it.
FLXA="$(python3 -c 'import sys;print(open(sys.argv[1],"rb").read().hex())' "$D/wimg-envp/graph.flxa")"
GENV="$(python3 -c 'import json,sys;print(json.dumps(json.load(open(sys.argv[1]))["env"],separators=(",",":")))' "$D/wimg-envp/workload-image.json")"
put_text "/graphs.nanocloud.io/envg" '{"apiVersion":"nanocloud.io/v1","kind":"Graph","metadata":{"name":"envg"},"spec":{"flxa":"'"$FLXA"'","env":'"$GENV"'}}'

echo "== 3. render the node: the plane + the kubelet's graph-pod slice, its volume chain and its image plane =="
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
WN_KEEP="vb_" wn_splice "$D/graph.yaml" node-1 "$D/blobs" "localhost:$REG_PORT" || fail "the splice did not apply"
grep -q "type: volume_files" "$D/graph.yaml" || fail "the node has no volume chain"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"

echo "== 4. run it; wait for the API over TLS =="
env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" FLUXOR_MOUNT_ROOT="$D/mnt" RUST_LOG=info "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
RUNTIME_PID=$!
nc_store_ready "$STORE" 60 || fail "the store never answered"
for _ in $(seq 60); do
  r="$(api GET /version 2>/dev/null || true)"
  case "$r" in 200\|*) break ;; esac
  sleep 1
done
case "$r" in 200\|*) echo "   GET /version -> 200" ;; *) fail "no API: $r" ;; esac

pod_json() { # pod_json <name> <image> [no-token|vault|half-vault]
  python3 - "$1" "$2" "${3:-}" <<'PY'
import json, sys
name, image, mode = sys.argv[1:4]
env = [{"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
       {"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
       {"name": "CELL", "valueFrom": {"configMapKeyRef": {"name": "ident", "key": "cell"}}}]
if mode != "no-token":
    env.append({"name": "TOKEN", "valueFrom": {"secretKeyRef": {"name": "tok", "key": "token"}}})
if mode in ("vault", "half-vault"):
    # Its own keys, on its writable claim; the seal key a Secret's.
    env.append({"name": "FLUXOR_VAULT_DIR", "value": "/var/lib/store/keys"})
if mode == "vault":
    env.append({"name": "FLUXOR_VAULT_KEY", "valueFrom": {"secretKeyRef": {"name": "vault", "key": "seal"}}})
print(json.dumps({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": "default"},
  "spec": {"runtimeClassName": "fluxor", "nodeName": "node-1", "containers": [{"name": "envp", "image": image, "env": env,
    "volumeMounts": [{"name": "enrol", "mountPath": "/run/enrol", "readOnly": True},
                     {"name": "store", "mountPath": "/var/lib/store"},
                     {"name": "ident", "mountPath": "/etc/ident"}]}],
    "volumes": [{"name": "enrol", "secret": {"secretName": "enrol"}},
                {"name": "store", "persistentVolumeClaim": {"claimName": "store"}},
                {"name": "ident", "configMap": {"name": "ident", "items": [{"key": "cell", "path": "sub/cell"}]}}]}}, separators=(",", ":")))
PY
}
create() { # create <json> -> its uid
  local r
  for _ in $(seq 90); do
    r="$(api POST /api/v1/namespaces/default/pods "$1")"
    case "$r" in 503\|*|403\|*) sleep 1 ;; *) break ;; esac
  done
  case "$r" in 20[01]\|*) ;; *) fail "POST pod: ${r:0:200}" ;; esac
  printf '%s' "${r#*|}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["uid"])'
}
waiting() { # waiting <pod> -> "<reason>|<message>" of its container's waiting state
  api GET "/api/v1/namespaces/default/pods/$1" | python3 -c 'import json,sys
r = sys.stdin.read(); b = r.split("|", 1)[1]
try:
    cs = json.loads(b).get("status", {}).get("containerStatuses", [{}])[0].get("state", {}).get("waiting", {})
    print(cs.get("reason", "") + "|" + cs.get("message", ""))
except Exception:
    print("|")'
}
dump() {
  local k
  for k in "/~node/node-1/pod-specs/$UID_" "/~node/node-1/pod-resolved/$UID_" "/~node/node-1/metal-sandboxes/$UID_" \
           "/~node/node-1/volume-binds/$UID_/0" "/~node/node-1/volume-binds/$UID_/1" "/volumes/default/store" \
           "/~node/node-1/sandbox-status/$UID_" "/podstatus/$UID_"; do
    printf '   %-52s %s\n' "$k" "$(nc_store_get "$STORE" "$k" 2>/dev/null | cut -c1-160)"
  done
  grep -a "envp\]\|\[workload\]\|volume_files" "$D/run.log" | tail -8 | cut -c1-200
}

echo "== 5. the Pod, before its ConfigMap exists: refused with why, nothing created =="
UID_="$(create "$(pod_json envp nanocloud/envp:1)")"
echo "   uid $UID_"
w=""
for _ in $(seq 60); do
  w="$(waiting envp)"
  case "$w" in CreateContainerConfigError\|*) break ;; esac
  sleep 1
done
case "$w" in
  CreateContainerConfigError\|*'configmap "ident" not found'*) echo "   waiting: $w" ;;
  *) dump; fail "the Pod was not refused for its missing ConfigMap: '$w'" ;;
esac
grep -aq "\[envp\]" "$D/run.log" && fail "the workload ran without its environment"

echo "== 6. the ConfigMap is created: the Pod runs, its params its environment =="
r="$(api POST /api/v1/namespaces/default/configmaps '{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"ident","namespace":"default"},"data":{"cell":"cell-7"}}')"
case "$r" in 20[01]\|*) echo "   POST configmaps/ident -> ${r%%|*}" ;; *) fail "POST configmap: ${r:0:200}" ;; esac
for _ in $(seq 120); do
  grep -aq "\[envp\] state=" "$D/run.log" && break
  sleep 1
done
grep -aq "\[envp\] state=" "$D/run.log" || { dump; fail "the workload never finished its lines"; }
line() { grep -ao "\[envp\] $1[^\"]*" "$D/run.log" | sed -n "${2:-1}p" | sed 's/\\n.*//; s/ *$//'; }
[ "$(line pod=)" = "[envp] pod=envp node=node-1 cell=cell-7 token=tok-123" ] \
  || { dump; fail "params: '$(line pod=)'"; }
echo "   $(line pod=)  (metadata.name, spec.nodeName, a ConfigMap key, a Secret key)"
[ "$(line secret)" = "[envp] secret=enrol-key-1" ] || { dump; fail "secret: '$(line secret)'"; }
echo "   $(line secret)  (a file of the mounted Secret)"
[ "$(line ro_write=)" = "[envp] ro_write=-13" ] || { dump; fail "ro: '$(line ro_write=)'"; }
echo "   $(line ro_write=)  (the Secret's volume is read-only)"
[ "$(line state=)" = "[envp] state=1" ] || { dump; fail "state: '$(line state=)'"; }
[ "$(cat "$D/pv/count" 2>/dev/null)" = "1" ] || fail "the claim's directory does not hold the count: '$(cat "$D/pv/count" 2>/dev/null)'"
echo "   $(line state=)  (written to the claim: its hostPath holds count=1)"
case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_")" in
  *";env="*"CELL=cell-7"*) echo "   metal sandbox carries the env" ;;
  *) dump; fail "the metal sandbox carries no env" ;;
esac

echo "== 6b. the Secret's volume is the AtomicWriter layout; the Secret updated, the graph reads the new value =="
VD="$D/mnt/pods/$UID_/volumes/enrol"
[ -L "$VD/..data" ] && [ -L "$VD/key" ] && [ "$(readlink "$VD/key")" = "..data/key" ] \
  || fail "the Secret's volume is not the AtomicWriter layout: $(ls -la "$VD" 2>&1)"
stamp="$(readlink "$VD/..data")"
echo "   key -> ..data/key, ..data -> $stamp  (read through the links: $(line secret))"
r="$(api PUT /api/v1/namespaces/default/secrets/enrol '{"apiVersion":"v1","kind":"Secret","metadata":{"name":"enrol","namespace":"default"},"data":{"key":"'"$(printf 'enrol-key-2\n' | base64)"'","extra":"'"$(printf 'x' | base64)"'"}}')"
case "$r" in 200\|*) echo "   PUT secrets/enrol -> 200 (key changed, extra added)" ;; *) fail "PUT secret: ${r:0:200}" ;; esac
for _ in $(seq 60); do
  grep -aq "\[envp\] updated=enrol-key-2" "$D/run.log" && break
  sleep 1
done
[ "$(line updated=)" = "[envp] updated=enrol-key-2" ] || { dump; fail "the graph did not read the updated Secret: '$(line updated=)'"; }
echo "   $(line updated=)  (the same path, ..data swapped)"
[ "$(readlink "$VD/..data")" != "$stamp" ] || fail "..data was not swapped: $(ls -la "$VD")"
[ "$(cat "$VD/extra" 2>/dev/null)" = "x" ] || fail "the added key is not in the volume: $(ls -la "$VD")"
CD="$D/mnt/pods/$UID_/volumes/ident"
[ "$(cat "$CD/sub/cell" 2>/dev/null)" = "cell-7" ] \
  || fail "a ConfigMap item's nested path does not read: $(find "$CD" -printf '%y %p\n' 2>&1)"
echo "   the added key and a ConfigMap item's nested path sub/cell read through ..data"

echo "== 7. deleted and created again: its state lived on the claim =="
r="$(api DELETE /api/v1/namespaces/default/pods/envp)"
case "$r" in 20[0-9]\|*) echo "   DELETE -> ${r%%|*}" ;; *) fail "DELETE pod: ${r:0:200}" ;; esac
st=""
for _ in $(seq 60); do
  st="$(nc_store_get "$STORE" "/~node/node-1/sandbox-status/$UID_" 2>/dev/null || true)"
  case "$st" in ""|*destroyed*) break ;; esac
  sleep 1
done
case "$st" in ""|*destroyed*) echo "   sandbox: ${st:-gone}" ;; *) dump; fail "the workload was not destroyed: $st" ;; esac
for _ in $(seq 30); do
  r="$(api GET /api/v1/namespaces/default/pods/envp)"
  case "$r" in 404\|*) break ;; esac
  sleep 1
done
UID_="$(create "$(pod_json envp nanocloud/envp:1)")"
for _ in $(seq 120); do
  [ -n "$(line state= 2)" ] && break
  sleep 1
done
[ "$(line state= 2)" = "[envp] state=2" ] || { dump; fail "second life: '$(line state= 2)'"; }
echo "   $(line state= 2)"

echo "== 8. a Graph binding a name the container does not set is refused before its node creates it =="
UID_="$(create "$(pod_json envg envg no-token)")"
w=""
for _ in $(seq 60); do
  w="$(waiting envg)"
  case "$w" in CreateContainerConfigError\|*) break ;; esac
  sleep 1
done
case "$w" in
  CreateContainerConfigError\|*'binds pod_env_probe.token to env "TOKEN" but the container does not set it'*) echo "   waiting: $w" ;;
  *) dump; fail "the Pod was not refused for TOKEN: '$w'" ;;
esac
case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_" 2>/dev/null || true)" in
  *graph=*) fail "a sandbox was made for a refused Pod" ;;
esac
echo "   no sandbox made"

echo "== 9. a Pod of the pulled image not setting a name its config blob requires is refused on the plane =="
UID_="$(create "$(pod_json envq nanocloud/envp:1 no-token)")"
w=""
for _ in $(seq 60); do
  w="$(waiting envq)"
  case "$w" in CreateContainerConfigError\|*) break ;; esac
  sleep 1
done
case "$w" in
  CreateContainerConfigError\|*'the image "nanocloud/envp:1" binds pod_env_probe.token to env "TOKEN" but the container does not set it'*) echo "   waiting: $w" ;;
  *) dump; fail "the pulled image's Pod was not refused for TOKEN: '$w'" ;;
esac
case "$(nc_store_get "$STORE" "/~node/node-1/pod-specs/$UID_" 2>/dev/null || true)" in
  *";envok=pack:"*) echo "   checked against its pack (envok=pack:…)" ;;
  *) dump; fail "the record names no checked pack" ;;
esac
case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_" 2>/dev/null || true)" in
  *graph=*) fail "a sandbox was made for a refused Pod" ;;
esac
echo "   no sandbox made"

echo "== 10. a Pod that names its vault's directory but not its key: refused, never run =="
r="$(api DELETE /api/v1/namespaces/default/pods/envp)"
case "$r" in 20[0-9]\|*) ;; *) fail "DELETE envp: ${r:0:200}" ;; esac
for _ in $(seq 60); do
  r="$(api GET /api/v1/namespaces/default/pods/envp)"
  case "$r" in 404\|*) break ;; esac
  sleep 1
done
runs="$(grep -ac "\[envp\] state=" "$D/run.log" || true)"
UID_="$(create "$(pod_json envh nanocloud/envp:1 half-vault)")"
# Refused at its CREATE (-22): the sandbox fails, and the Pod waits
# CreateContainerError while its restarts are asked again.
st=""; ps=""
for _ in $(seq 90); do
  st="$(nc_store_get "$STORE" "/~node/node-1/sandbox-status/$UID_" 2>/dev/null || true)"
  ps="$(nc_store_get "$STORE" "/podstatus/$UID_" 2>/dev/null || true)"
  case "$st|$ps" in *state=failed\;code=-22*|*why=CreateContainerError*) break ;; esac
  sleep 1
done
case "$st|$ps" in
  *state=failed\;code=-22*|*why=CreateContainerError*) echo "   refused: sandbox '$st', Pod waiting ${ps#*why=}" ;;
  *) dump; fail "a half-stated vault was not refused: '$st' '$ps'" ;;
esac
[ "$(grep -ac "\[envp\] state=" "$D/run.log" || true)" = "$runs" ] || fail "the workload ran without its vault"
case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_" 2>/dev/null || true)" in
  *"FLUXOR_VAULT_DIR="*) echo "   its sandbox names the directory and no key: the CREATE was refused" ;;
  *) dump; fail "the metal sandbox does not carry the vault's directory" ;;
esac
r="$(api DELETE /api/v1/namespaces/default/pods/envh)"

echo "== 11. a Pod that keeps its own keys: its vault on its claim, sealed by its Secret =="
if ! grep -q "pub mod vault" "$ROOT/target/fluxor/fluxor-abi/sdk/contracts/workload.rs" 2>/dev/null; then
  echo "   SKIP: the pinned Fluxor has no workload vault (contracts::workload::vault, Fluxor FX3 F4) — its CREATE would be refused ENOSYS"
else
  UID_="$(create "$(pod_json envv nanocloud/envp:1 vault)")"
  for _ in $(seq 120); do
    [ "$(grep -ac "\[envp\] state=" "$D/run.log" || true)" -gt "$runs" ] && break
    sleep 1
  done
  [ "$(grep -ac "\[envp\] state=" "$D/run.log" || true)" -gt "$runs" ] || { dump; fail "the Pod with its own vault never ran"; }
  echo "   $(line state= $((runs + 1)))  (it ran, its claim lent)"
  [ -d "$D/pv/keys" ] || { dump; fail "the vault's directory was not made on the claim: $(ls -la "$D/pv")"; }
  echo "   the vault's directory is on the claim: $D/pv/keys"
  case "$(nc_store_get "$STORE" "/~node/node-1/metal-sandboxes/$UID_")" in
    *"FLUXOR_VAULT_KEY=00112233"*) echo "   the seal key reached the node from the Secret" ;;
    *) dump; fail "the metal sandbox carries no seal key" ;;
  esac
fi

echo "== E2E green: a graph-backed Pod's env (ConfigMap, Secret, downward API) fills its graph's params, its Secret (AtomicWriter, updated in place) and claim are lent to it, a name its Graph or its pulled image's config binds but the Pod does not set is refused, and its own vault is stated (a half-stated one refused) =="
