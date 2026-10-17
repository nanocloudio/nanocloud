#!/usr/bin/env bash
# Live E2E: an image pull that fails is retried with a growing delay, says so
# on the Pod, and recovers by itself — Kubernetes' ErrImagePull /
# ImagePullBackOff — on a node in the bare-metal shape (scripts/apps-e2e.sh's
# render) whose blob cache is empty:
#
#   1. a Deployment of a workload image (chronicle-http) while the registry
#      is DOWN (nothing listens on its port): the Pod is Pending, its Warning
#      events say ErrImagePull, then ImagePullBackOff with a growing count,
#      the registry is asked again at growing intervals — never one attempt,
#      then silence
#   2. the registry comes up FAILING (every answer 503): still backing off,
#      the event names the status
#   3. the registry serves: with nothing more done, the next retry pulls the
#      image, the Pod is Running and the app answers
#
#   4. a cached layer goes bad across a power cut — its file corrupted (same
#      size, other bytes), then another cut short, then one deleted with the
#      registry down: each time the node finds it on the CREATE after the
#      boot (every layer is hashed before every CREATE), says so on the Pod
#      (CorruptImageLayer, naming the layer), drops it and fetches it again
#      — backing off (ImagePullBackOff) while the registry is down — and the
#      Pod runs again once the layer verifies
#
# PASS = all four, and the retries' spacing grows.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
REG_PORT=5396
HTTP_PORT=18081
APP=chronicle-http

. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/support/workload-node.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v kubectl >/dev/null || { echo "FAIL: kubectl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-backoff-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE" "$D/blobs"
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

echo "== 1. the image, laid out as a registry would serve it — and the registry DOWN =="
sed "s/port: 8080/port: $HTTP_PORT/" "$ROOT/packaging/apps/$APP.yaml" >"$D/$APP.yaml"
wn_publish "$APP" "$D/$APP.yaml"
: >"$D/reg.log"
curl -s -m 2 "http://127.0.0.1:$REG_PORT/v2/" >/dev/null && fail "something already answers on :$REG_PORT"

echo "== 2. the node: the plane, its kubelet's graph-pod slice and image plane, an empty blob cache =="
wn_seed_identity
nc_controlplane_graph "$STORE" "$D/graph.yaml" || fail "the control plane did not render"
sed -i 's|announce_path: "/authn-keys/sa"|announce_path: "/authn-keys/node"|' "$D/graph.yaml"
wn_splice "$D/graph.yaml" node-1 "$D/blobs" "localhost:$REG_PORT" || fail "the splice did not apply"
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin"
boot() {
  env -C "$STORE" FLUXOR_OWNER_ROOT="$D/owners" RUST_LOG=info "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  nc_store_ready "$STORE" 60 || fail "the store never answered"
  local r=""
  for _ in $(seq 90); do
    r="$(api GET /version 2>/dev/null || true)"
    case "$r" in 200\|*) return 0 ;; esac
    sleep 1
  done
  fail "no API: $r"
}
boot
cat >"$D/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters:
- name: node
  cluster:
    server: https://127.0.0.1:$PORT
    insecure-skip-tls-verify: true
users:
- name: admin
  user:
    token: $(cat "$D/token.admin")
contexts:
- name: node
  context: {cluster: node, user: admin, namespace: default}
current-context: node
KC
k() { kubectl --kubeconfig "$D/kubeconfig" --request-timeout=20s "$@"; }

echo "== 3. kubectl apply a Deployment of nanocloud/$APP:1 while the registry is down =="
cat >"$D/deploy.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $APP
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels: {app: $APP}
  template:
    metadata:
      labels: {app: $APP}
    spec:
      runtimeClassName: fluxor
      containers:
      - name: $APP
        image: nanocloud/$APP:1
YAML
out=""
for _ in $(seq 60); do
  out="$(k apply -f "$D/deploy.yaml" 2>&1)" && break
  case "$out" in *ServiceUnavailable*|*Forbidden*) sleep 2 ;; *) fail "kubectl apply: $out" ;; esac
done
case "$out" in *created*) echo "   $out" ;; *) fail "kubectl apply: $out" ;; esac

phase() { k get pods -l "app=$APP" -o jsonpath='{.items[*].status.phase}' 2>/dev/null || true; }
uid=""
for _ in $(seq 60); do
  uid="$(k get pods -l "app=$APP" -o jsonpath='{.items[0].metadata.uid}' 2>/dev/null || true)"
  [ -n "$uid" ] && break
  sleep 1
done
[ -n "$uid" ] || fail "the Deployment made no Pod"
# event <reason>: "<count>|<type>|<message>" of the Pod's event of that
# reason, or "" (also while the API is not answering yet, after a boot).
event() {
  k get events -o json 2>/dev/null | python3 -c '
import json, sys
name = sys.argv[1]
for e in json.load(sys.stdin).get("items", []):
    if e["metadata"]["name"] == name:
        print("%s|%s|%s" % (e.get("count", ""), e.get("type", ""), e.get("message", "")))
' "$uid.$1" 2>/dev/null || true
}
count_of() { local c="${1%%|*}"; echo "${c:-0}"; }
# The failures as the Pod sees them: each new count, with when it was seen.
LASTN=""
note() { local n; n="$(count_of "$1")"; [ "$n" = 0 ] || [ "$n" = "$LASTN" ] || { echo "$SECONDS $n" >>"$D/counts.log"; LASTN="$n"; }; }

echo "== 4. the pull fails: Pending, ErrImagePull, then ImagePullBackOff with a growing count =="
got=""
for _ in $(seq 60); do
  got="$(event ErrImagePull)"
  [ -n "$got" ] && break
  sleep 1
done
[ -n "$got" ] || { grep -a "http_fetch" "$D/run.log" | tail -3; fail "no ErrImagePull event: the failed pull said nothing"; }
case "$got" in *"|Warning|Failed to pull image \"nanocloud/$APP:1\": the registry could not be reached"*) ;;
  *) fail "ErrImagePull event: '$got'" ;; esac
echo "   ErrImagePull: ${got#*|}"
[ "$(phase)" = "Pending" ] || fail "the Pod is '$(phase)' while its image cannot be pulled, not Pending"
bo=""
for _ in $(seq 60); do
  bo="$(event ImagePullBackOff)"
  note "$bo"
  [ "$(count_of "$bo")" -ge 2 ] && break
  sleep 1
done
[ "$(count_of "$bo")" -ge 2 ] || fail "no ImagePullBackOff event after the first failure: '$bo'"
echo "   ImagePullBackOff: count=$(count_of "$bo") ${bo#*|}"
[ "$(phase)" = "Pending" ] || fail "the Pod is '$(phase)' while it backs off, not Pending"

echo "== 5. the registry comes up failing (503 to everything): still backing off, the status named =="
echo 503 >"$D/reg.fail"
wn_registry "$REG_PORT"
for _ in $(seq 30); do curl -s -m 2 -o /dev/null "http://127.0.0.1:$REG_PORT/v2/" && break; sleep 0.2; done
n0="$(count_of "$bo")"
for _ in $(seq 90); do
  bo="$(event ImagePullBackOff)"
  note "$bo"
  case "$bo" in *"(status 503)"*) break ;; esac
  sleep 1
done
case "$bo" in *"(status 503)"*) ;; *) fail "the 503 is not on the Pod's event: '$bo'" ;; esac
[ "$(count_of "$bo")" -gt "$n0" ] || fail "the count did not grow: $n0 -> $(count_of "$bo")"
grep -q "manifests/1 503" "$D/reg.log" || fail "the registry was not asked again"
echo "   ImagePullBackOff: count=$(count_of "$bo") ${bo#*|}"
[ "$(phase)" = "Pending" ] || fail "the Pod is '$(phase)' while the registry fails, not Pending"

echo "== 6. the registry serves: the next retry pulls, the Pod runs and the app answers =="
rm -f "$D/reg.fail"
t0=$SECONDS
ok=0
for _ in $(seq 240); do
  if [ "$(phase)" = "Running" ] && curl -s -m 2 "http://127.0.0.1:$HTTP_PORT/" | grep -q chronicle; then ok=1; break; fi
  sleep 1
done
[ "$ok" = 1 ] || { k get pods -o wide 2>&1 | sed 's/^/   /'; fail "the Pod did not recover once the registry served: '$(phase)'"; }
echo "   Running after $((SECONDS - t0)) s more, GET / -> $(curl -s -m 2 "http://127.0.0.1:$HTTP_PORT/" | head -c 50)…"
grep -q "manifests/1 200" "$D/reg.log" || fail "the manifest was never served"
# The retries, spaced by the backoff (10 s, 20 s, 40 s, … at most 300 s): the
# gaps between the failures the Pod was told of grow.
gaps="$(awk 'NR > 1 { printf "%s%d", sep, $1 - t; sep = " " } { t = $1 }' "$D/counts.log")"
echo "   failures seen at (s, count): $(tr '\n' ' ' <"$D/counts.log")— gaps: $gaps s"
python3 - $gaps <<'PY' || fail "the retries did not back off: gaps $gaps s"
import sys
g = [int(x) for x in sys.argv[1:]]
# Polled once a second, so a gap reads +-2 s; the last must be a real backoff.
assert len(g) >= 1 and g[-1] >= 15, g
assert all(b >= a - 3 for a, b in zip(g, g[1:])), g
PY
serving() { [ "$(phase)" = "Running" ] && curl -s -m 2 "http://127.0.0.1:$HTTP_PORT/" | grep -q chronicle; }
cut() {
  kill -9 "$RUNTIME_PID"; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""
  curl -s -m 2 "http://127.0.0.1:$HTTP_PORT/" >/dev/null && fail "still serving with the node down"
  return 0
}
# The image's layers, in manifest order (0: the FLXA, then each module).
LAYERS=($(python3 -c 'import json,sys; print(" ".join(l["digest"][7:] for l in json.load(open(sys.argv[1]))["layers"]))' "$D/reg/$APP/manifest.json"))
[ "${#LAYERS[@]}" -ge 3 ] || fail "expected 3 layers, got ${#LAYERS[@]}"
# spoil <how> <layer index> [down]: cut the power, spoil that cached layer,
# boot (with the registry down when asked); the Pod is told, the layer is
# fetched again, the Pod runs.
spoil() {
  local how="$1" hex="${LAYERS[$2]}" f="$D/blobs/${LAYERS[$2]}"
  [ -f "$f" ] || fail "layer $2 is not in the cache: $f"
  k delete event "$uid.CorruptImageLayer" >/dev/null 2>&1 || true
  local asked; asked="$(grep -c "blobs/sha256:$hex 200" "$D/reg.log" || true)"
  cut
  case "$how" in
    corrupt) python3 -c 'import sys; p=sys.argv[1]; b=bytearray(open(p,"rb").read()); b[len(b)//2]^=0xff; open(p,"wb").write(b)' "$f" ;;
    truncate) truncate -s "$(( $(stat -c %s "$f") / 2 ))" "$f" ;;
    delete) rm -f "$f" ;;
  esac
  if [ "${3:-}" = down ]; then kill "$REG_PID" 2>/dev/null || true; wait "$REG_PID" 2>/dev/null || true; REG_PID=""; fi
  boot
  local got=""
  for _ in $(seq 120); do got="$(event CorruptImageLayer)"; [ -n "$got" ] && break; sleep 1; done
  case "$got" in *"|Warning|Cached layer sha256:$hex is missing, short or not its digest"*) ;;
    *) k get pods 2>&1 | sed 's/^/   /'; fail "no CorruptImageLayer event for layer $2 ($how): '$got'" ;; esac
  echo "   CorruptImageLayer: ${got#*|*|}"
  if [ "${3:-}" = down ]; then
    local bo=""
    for _ in $(seq 120); do
      bo="$(event ImagePullBackOff)"
      case "$bo" in *"could not be reached"*) break ;; esac
      sleep 1
    done
    case "$bo" in *"could not be reached"*) ;; *) fail "no ImagePullBackOff while the layer cannot be fetched: '$bo'" ;; esac
    serving && fail "serving without the layer"
    echo "   ImagePullBackOff: ${bo#*|}; the Pod is '$(phase)'"
    wn_registry "$REG_PORT"
  fi
  local ok=0 t0=$SECONDS
  for _ in $(seq 400); do serving && { ok=1; break; }; sleep 1; done
  [ "$ok" = 1 ] || { k get pods 2>&1 | sed 's/^/   /'; fail "the Pod did not run again after layer $2 was $how"; }
  [ "$(grep -c "blobs/sha256:$hex 200" "$D/reg.log" || true)" -gt "$asked" ] || fail "layer $2 was not fetched again"
  cmp -s "$f" "$D/reg/$APP/$hex" || fail "the cache does not hold layer $2 as its digest"
  echo "   layer $2 fetched again, verified; Running and serving after $((SECONDS - t0)) s"
}

echo "== 7. a power cut corrupts a cached layer (same size, other bytes) =="
spoil corrupt 1
echo "== 8. a power cut leaves a cached layer short =="
spoil truncate 2
echo "== 9. a power cut loses a cached layer, and the registry is down =="
spoil delete 0 down

k get pods 2>&1 | sed 's/^/   /'
echo "== E2E green: a failed pull backs off (ErrImagePull, ImagePullBackOff, growing), says why, and recovers by itself; a cached layer gone bad is found, said, fetched again and run =="
