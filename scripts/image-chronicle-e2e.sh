#!/usr/bin/env bash
# An image reference resolved to something runnable, as params: the pull
# trigger, the two gates, and the answer. The repo/tag split, the `/`->`_`
# store-key spelling and the un-escaping of the OCI Entrypoint/Cmd lists are
# `!` projection modifiers on the connectors — transcription of the record's
# own encoding, which the VM has no bytes arithmetic to do.
#
# The pull and the assembly (image.uproc / image_asm.uproc over http_fetch)
# are stubbed here by writing what they would: this proves the DECISION half,
# on the same records they exchange.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
. "$ROOT/scripts/fluxor-env.sh"
. "$ROOT/scripts/chronicle-params.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for m in store_source store_effect decision; do
  [ -e "$MODULES_DIR/$m.fmod" ] || { echo "FAIL: missing $m.fmod"; exit 1; }
done

D="$(mktemp -d /tmp/nc-imgchron-XXXXXX)"
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

for e in rq ad ar ac rs; do
  v="$(nc_decision "$ROOT/modules/app/_chronicle/pod_image.uproc" $e)"
  [ -n "$v" ] || { echo "FAIL: pod_image.uproc $e did not compile"; exit 1; }
  eval "DEC_$(echo $e | tr a-z A-Z)=\"$v\""
done
python3 - "$ROOT/packaging/debian/fluxor-podlifecycle.yaml" "$D/graph.yaml" <<'PY2'
import re, sys
s = open(sys.argv[1]).read()
mods = re.search(r'(  # ── image:.*?)\n  # ── lifecycle', s, re.S).group(1)
wire = re.search(r'(  - from: pi_source\.record_out.*?  - from: pi_resolved\.response_out\n    to: \S+)\n', s, re.S).group(1)
open(sys.argv[2], "w").write(
    "target: linux\ntick_us: 1000\nscheduler:\n  accept_cycles: true\nmodules:\n"
    + mods + "\nwiring:\n" + wire + "\n")
PY2
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

H1=1111111111111111111111111111111111111111111111111111111111111111
H2=2222222222222222222222222222222222222222222222222222222222222222
H3=3333333333333333333333333333333333333333333333333333333333333333

echo "== 1. a nested repo with a tag triggers ONE pull, keyed by the whole reference =="
store_put "/pod-specs/pod-img" "image=nanocloud/hello:v2;desired=running"
store_put "/pod-specs/pod-plain" "cmd=/bin/echo hi;desired=running"
store_put "/pod-specs/pod-rootfs" "image=nginx;rootfs=/already/here;desired=running"
store_put "/pod-specs/pod-bad" "image=Not/A:Reference:;desired=running"
run 5
[ "$(store_last '/image-requests/nanocloud~hello+v2')" = "reg=;repo=nanocloud/hello;tag=v2;pin=" ] \
  || fail "pull trigger wrong: '$(store_last '/image-requests/nanocloud~hello+v2')'"
echo "   /image-requests/nanocloud~hello+v2 = reg=;repo=nanocloud/hello;tag=v2;pin= (repo keeps the slash, the KEY does not)"
[ -z "$(store_last '/image-requests/nginx+latest')" ] || fail "an explicit rootfs must not trigger a pull"
[ -z "$(store_last /pod-resolved/pod-img)" ] || fail "nothing may resolve before the tag resolves"
[ -z "$(store_last /pod-resolved/pod-plain)" ] || fail "a non-image pod must not resolve"
[ -z "$(store_last /image-requests/pod-bad)" ] || fail "what is not a reference must trigger nothing"
echo "   nginx (explicit rootfs), the plain pod and a non-reference: no trigger, no resolve"

echo "== 2. the digest and rootfs gates: a resolved tag whose rootfs is not ready resolves nothing =="
store_put '/image-digests/nanocloud~hello+v2' "digest=$H1"
store_put "/image-rootfs/$H1" "state=pending;path=/rootfs/$H1"
run 4
[ -z "$(store_last /pod-resolved/pod-img)" ] || fail "state=pending must not resolve"
echo "   state=pending: held"

echo "== 3. ready + an OCI config: argv is Entrypoint ++ Cmd, every token's bounds kept; the digest recorded =="
# What image.uproc writes (`!esc`): `,`-joined, with %25/%2C/%3B escaped.
store_put "/image-rootfs/$H1" "state=ready;path=/rootfs/$H1"
store_put "/image-config/$H1" "entrypoint=/usr/sbin/nginx;cmd=-g,--pid%2Cfile"
run 5
R="$(store_last /pod-resolved/pod-img)"
[ "$R" = "rootfs=/rootfs/$H1;digest=$H1;env=;workdir=;user=;cmd=/usr/sbin/nginx -g --pid%2Cfile" ] \
  || fail "resolved wrong: '$R'"
echo "   $R"
echo "   (Entrypoint ++ Cmd, ',' tokens split; the comma inside one stays escaped until the spawn decodes it)"

echo "== 3b. a ';' inside an argv token (nginx's CMD [\"nginx\",\"-g\",\"daemon off;\"]) keeps the record whole =="
# image.uproc escapes the ';' (%3B) in the config list; !argv decodes it and
# re-escapes it (with the space) for the argv grammar, so the resolved record
# keeps its own ';' as the only separator and the spawn decodes the token.
store_put "/pod-specs/pod-nginx" "image=library/nginx:1.27;desired=running"
store_put '/image-digests/library~nginx+1.27' "digest=$H2"
store_put "/image-rootfs/$H2" "state=ready;path=/rootfs/$H2"
store_put "/image-config/$H2" "entrypoint=/docker-entrypoint.sh;cmd=nginx,-g,daemon off%3B"
run 5
R="$(store_last /pod-resolved/pod-nginx)"
[ "$R" = "rootfs=/rootfs/$H2;digest=$H2;env=;workdir=;user=;cmd=/docker-entrypoint.sh nginx -g daemon%20off%3B" ] \
  || fail "resolved wrong: '$R'"
echo "   $R"

echo "== 4. an explicit spec cmd wins outright over the image's config =="
store_put "/pod-specs/pod-ovr" "image=nanocloud/hello:v2;cmd=/bin/sh -c true;desired=running"
run 5
[ "$(store_last /pod-resolved/pod-ovr)" = "rootfs=/rootfs/$H1;digest=$H1;env=;workdir=;user=;cmd=/bin/sh -c true" ] \
  || fail "explicit cmd should win: '$(store_last /pod-resolved/pod-ovr)'"
echo "   pod-ovr keeps its own argv"

echo "== 4b. command/args against Entrypoint/Cmd, by the Kubernetes rules; the image's Env, WorkingDir, User carried =="
H5=5555555555555555555555555555555555555555555555555555555555555555
store_put '/image-digests/nanocloud~table+1' "digest=$H5"
store_put "/image-rootfs/$H5" "state=ready;path=/rootfs/$H5"
store_put "/image-config/$H5" "entrypoint=/ep,x;cmd=c1,c 2;env=PATH=/bin,A=1%2C2;workdir=/srv%3Bapp;user=app:staff"
store_put "/pod-specs/tbl-both" "image=nanocloud/table:1;cmd=/c;args=a1 a%202;desired=running"
store_put "/pod-specs/tbl-cmd" "image=nanocloud/table:1;cmd=/c;desired=running"
store_put "/pod-specs/tbl-args" "image=nanocloud/table:1;args=a1;desired=running"
store_put "/pod-specs/tbl-none" "image=nanocloud/table:1;desired=running"
run 6
B="rootfs=/rootfs/$H5;digest=$H5;env=PATH=/bin,A=1%2C2;workdir=/srv%3Bapp;user=app:staff"
for want in "tbl-both|$B;cmd=/c a1 a%202" "tbl-cmd|$B;cmd=/c" "tbl-args|$B;cmd=/ep x a1" "tbl-none|$B;cmd=/ep x c1 c%202"; do
  got="$(store_last "/pod-resolved/${want%%|*}")"
  [ "$got" = "${want#*|}" ] || fail "${want%%|*} resolved wrong:
   got  '$got'
   want '${want#*|}'"
done
echo "   command+args: command ++ args; command only: command (Cmd ignored); args only: Entrypoint ++ args; neither: Entrypoint ++ Cmd"

echo "== 5. imagePullPolicy: Always (stated, or :latest / no tag) is the Pod's own request; IfNotPresent shares =="
store_put "/pod-specs/pod-notag" "image=nanocloud/app;desired=running"
store_put "/pod-specs/pod-latest" "image=nanocloud/app:latest;desired=running"
store_put "/pod-specs/pod-always" "image=nanocloud/hello:v2;pull=Always;desired=running"
store_put "/pod-specs/pod-inp" "image=nanocloud/app:latest;pull=IfNotPresent;desired=running"
run 5
for u in pod-notag pod-latest pod-always; do
  r="$(store_last /image-requests/$u)"
  case "$u:$r" in
    pod-always:"reg=;repo=nanocloud/hello;tag=v2;pin=") ;;
    pod-notag:"reg=;repo=nanocloud/app;tag=latest;pin="|pod-latest:"reg=;repo=nanocloud/app;tag=latest;pin=") ;;
    *) fail "$u: Always must request under its uid: '$r'" ;;
  esac
done
[ "$(store_last '/image-requests/nanocloud~app+latest')" = "reg=;repo=nanocloud/app;tag=latest;pin=" ] \
  || fail "IfNotPresent :latest must share the reference's request"
[ "$(store_writes '/image-requests/nanocloud~hello+v2')" = 1 ] || fail "the shared request was rewritten"
echo "   no tag / :latest / Always -> /image-requests/<uid>; IfNotPresent :latest -> /image-requests/nanocloud~app+latest"

echo "== 6. a registry host:port and a digest pin are parsed as such =="
store_put "/pod-specs/pod-host" "image=registry.nanocloud.io:5000/nanocloud/hello:v1;desired=running"
store_put "/pod-specs/pod-pin" "image=nanocloud/hello@sha256:$H3;desired=running"
run 5
[ "$(store_last '/image-requests/registry.nanocloud.io+5000~nanocloud~hello+v1')" \
  = "reg=registry.nanocloud.io:5000;repo=nanocloud/hello;tag=v1;pin=" ] \
  || fail "host:port ref: '$(store_last '/image-requests/registry.nanocloud.io+5000~nanocloud~hello+v1')'"
[ "$(store_last "/image-requests/nanocloud~hello@$H3")" = "reg=;repo=nanocloud/hello;tag=;pin=sha256:$H3" ] \
  || fail "digest ref: '$(store_last "/image-requests/nanocloud~hello@$H3")'"
echo "   registry.nanocloud.io:5000/nanocloud/hello:v1 and nanocloud/hello@sha256:… keyed and split"

echo "== 7. a refused reference and a Never with nothing cached are the Pod's Warning events =="
store_put "/pod-specs/u-foreign" "image=other.example:5000/x/y:1;pod=default/foreign;desired=running"
store_put '/image-digests/other.example+5000~x~y+1' "refused=registry other.example:5000 is not this node's registry registry.nanocloud.io:5000"
store_put "/pod-specs/u-never" "image=nanocloud/absent:1;pull=Never;pod=default/never;desired=running"
run 5
E="$(store_last /events/default/u-foreign.ErrImagePull)"
python3 -c 'import json,sys; e=json.loads(sys.argv[1]); assert e["reason"]=="ErrImagePull" and e["involvedObject"]["name"]=="foreign" and "other.example:5000 is not" in e["message"], e' "$E" \
  || fail "no ErrImagePull event: '$E'"
echo "   $(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["message"])' "$E")"
E="$(store_last /events/default/u-never.ErrImageNeverPull)"
python3 -c 'import json,sys; e=json.loads(sys.argv[1]); assert e["reason"]=="ErrImageNeverPull", e' "$E" \
  || fail "no ErrImageNeverPull event: '$E'"
[ -z "$(store_last '/image-requests/nanocloud~absent+1')" ] || fail "Never must not request a pull"
[ -z "$(store_last /pod-resolved/u-foreign)" ] || fail "a refused reference must not resolve"
echo "   Never: no request, ErrImageNeverPull"

echo "== E2E green: an image reference resolved to a digest, a rootfs and an argv — the trigger, the policy, the gates, the refusals, the un-escaping =="
