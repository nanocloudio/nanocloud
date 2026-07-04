#!/usr/bin/env bash
# Deployment -> ReplicaSet as a Chronicle graph: the projection half of the
# workload chain, as params. Three jobs:
#
#   store_source  /deployments.apps/, projecting namespace, name, spec,
#                 spec.replicas, and (hash=) an FNV-1a of spec.template —
#                 the pod-template-hash, computed where the bytes are
#   read          GET the current ReplicaSet (hash + spec projected back)
#   archive       on a template change, PUT the OLD spec to
#                 /controllerrevisions.apps/<ns>/<name>/previous
#   write         PUT the ReplicaSet — unless nothing changed (the verdict
#                 rides from the read as a tag: create / roll / scale / "")
#   record        ScalingReplicaSet event, from the write's own reply
#
# PUT-if-changed is a decision over the read's projections, not a byte compare
# of a rendered document: the document is a function of (spec, hash, names),
# so "spec equal and hash equal" is "document equal".
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

D="$(mktemp -d /tmp/nc-depchron-XXXXXX)"
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

DEC_RD="$(nc_decision "$ROOT/modules/app/_chronicle/deployment.uproc" rd)"
DEC_ARCH="$(nc_decision "$ROOT/modules/app/_chronicle/deployment.uproc" arch)"
DEC_WR="$(nc_decision "$ROOT/modules/app/_chronicle/deployment.uproc" wr)"
DEC_REC="$(nc_decision "$ROOT/modules/app/_chronicle/deployment.uproc" rec)"
for v in "$DEC_RD" "$DEC_ARCH" "$DEC_WR" "$DEC_REC"; do
  [ -n "$v" ] || { echo "FAIL: a deployment.uproc entry did not compile"; exit 1; }
done
cat >"$D/graph.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
modules:
  - name: store_source
    prefix: "/deployments.apps/"
    paths: "spec,spec.replicas"
    hash: "spec.template"
  - name: read
    type: decision
    params:
      decision: "${DEC_RD}"
  - name: current
    type: store_effect
    key_prefix: "/replicasets.apps/"
    paths: "metadata.labels.pod-template-hash,spec"
  - name: archive
    type: decision
    params:
      decision: "${DEC_ARCH}"
  - name: history
    type: store_effect
    key_prefix: "/controllerrevisions.apps/"
  - name: write
    type: decision
    params:
      decision: "${DEC_WR}"
  - name: apply
    type: store_effect
    key_prefix: "/replicasets.apps/"
  - name: record
    type: decision
    params:
      decision: "${DEC_REC}"
  - name: events
    type: store_effect
    key_prefix: "/events/"
wiring:
  - from: store_source.status
    to: store_source.changes
  - from: store_source.record_out
    to: read.record_in
  - from: read.result_out
    to: current.request_in
    buffer_group: 1
  - from: current.response_out
    to: archive.record_in
  - from: archive.result_out
    to: history.request_in
    buffer_group: 2
  - from: history.response_out
    to: write.record_in
  - from: write.result_out
    to: apply.request_in
    buffer_group: 3
  - from: apply.response_out
    to: record.record_in
  - from: record.result_out
    to: events.request_in
    buffer_group: 4
YAML

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
has() { local v="$1"; shift; for w in "$@"; do case "$v" in *"$w"*) ;; *) return 1;; esac; done; }

echo "== 1. two Deployments, one without spec.replicas =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":3,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
store_put "/deployments.apps/default/api" '{"metadata":{"name":"api","namespace":"default"},"spec":{"replicas":1,"template":{"spec":{"containers":[{"name":"api","image":"redis"}]}}}}'
store_put "/deployments.apps/default/bare" '{"metadata":{"name":"bare","namespace":"default"},"spec":{"template":{"spec":{}}}}'
run 5
WEB="$(store_last /replicasets.apps/default/web)"
has "$WEB" '"name":"web"' '"namespace":"default"' '"kind":"Deployment"' '"replicas":3' '"image":"nginx"' '"pod-template-hash":"' \
  || fail "web ReplicaSet wrong: '$WEB'"
API="$(store_last /replicasets.apps/default/api)"
has "$API" '"replicas":1' '"image":"redis"' || fail "api ReplicaSet wrong: '$API'"
[ -z "$(store_last /replicasets.apps/default/bare)" ] || fail "a Deployment without spec.replicas must not project"
H1="$(printf '%s' "$WEB" | sed -n 's/.*"pod-template-hash":"\([0-9a-f]*\)".*/\1/p')"
[ ${#H1} = 8 ] || fail "pod-template-hash is not 8 hex digits: '$H1'"
echo "   web (hash $H1) and api projected, owned and labelled; bare skipped"
has "$(store_last /events/default/web.ScalingReplicaSet.0)" '"reason":"ScalingReplicaSet"' '"name":"web"' \
  || fail "no ScalingReplicaSet event for web"
echo "   ScalingReplicaSet recorded"

echo "== 2. PUT-if-changed: a second pass spends no revision =="
W1="$(store_writes /replicasets.apps/default/web)"
run 4
W2="$(store_writes /replicasets.apps/default/web)"
[ "$W1" = "$W2" ] || fail "a quiet pass rewrote the ReplicaSet ($W1 -> $W2 writes)"
echo "   $W1 write(s) before, $W2 after"

echo "== 3. scale: replicas 3 -> 5 keeps the hash, no archive =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":5,"template":{"spec":{"containers":[{"name":"web","image":"nginx"}]}}}}'
run 4
WEB5="$(store_last /replicasets.apps/default/web)"
has "$WEB5" '"replicas":5' "\"pod-template-hash\":\"$H1\"" || fail "scale wrong: '$WEB5'"
[ -z "$(store_last /controllerrevisions.apps/default/web/previous)" ] || fail "a replicas-only edit must not archive"
echo "   replicas=5, hash unchanged, nothing archived"

echo "== 4. rollout: the template changes, the hash changes, the OLD spec is archived =="
store_put "/deployments.apps/default/web" '{"metadata":{"name":"web","namespace":"default"},"spec":{"replicas":5,"template":{"spec":{"containers":[{"name":"web","image":"httpd"}]}}}}'
run 4
WEBV2="$(store_last /replicasets.apps/default/web)"
H2="$(printf '%s' "$WEBV2" | sed -n 's/.*"pod-template-hash":"\([0-9a-f]*\)".*/\1/p')"
[ -n "$H2" ] && [ "$H2" != "$H1" ] || fail "hash did not change on a template edit ($H1 -> $H2)"
has "$WEBV2" '"image":"httpd"' || fail "ReplicaSet not rolled: '$WEBV2'"
PREV="$(store_last /controllerrevisions.apps/default/web/previous)"
has "$PREV" '"image":"nginx"' '"replicas":5' || fail "previous revision not archived: '$PREV'"
echo "   hash $H1 -> $H2; previous (nginx) archived at /controllerrevisions.apps/default/web/previous"

echo "== E2E green: Deployment -> ReplicaSet as params — projection, PUT-if-changed, scale, rollout history, events =="
