#!/usr/bin/env bash
# Live E2E for probes as Chronicle params (probe.uproc, probe_collect.uproc;
# the pb_ nodes of fluxor-podlifecycle.yaml): exec probes run through
# the sandbox runtime's /sandbox-exec/ seam, each probe's state lives at
# /probe-rt-<l|r>/<uid> (its due/deadline arm the kernel timer), verdicts land
# at /probe-status/<uid>; pod_lifecycle folds a live=0 verdict into its
# restart+backoff machinery. Runs UNPRIVILEGED (null sandboxes; exec is the
# already-shipped seam).
#
# Matrix:
#   pod-ready  rp_cmd=/bin/true            → readiness flips 0→1
#              (readiness starts 0 — not ready until proven; live defaults 1)
#   pod-live   lp_cmd=/bin/false           restart=always
#              → live flips 1→0 past the failure threshold → pod_lifecycle
#                kills + relaunches under backoff (restarts=n persisted);
#                the verdict resets to live=1 for the fresh run
#   pod-tcp    lp_cmd=@tcp <listening>      → probe_dial connects: live stays 1,
#                                             no restart
#   pod-tcpdown lp_cmd=@tcp <closed port>   → refused: live 0, restarted
#   pod-http   rp_cmd=@http … /ok (200)      → ready 0→1
#   pod-http5  rp_cmd=@http … /bad (500)     → ready stays 0
#   pod-start  sp_cmd=/bin/true, rp_cmd=/bin/true
#                                           → not ready until the startup
#                                             probe passed (verdict 2→1)
#   pod-startfail sp_cmd=/bin/false, lp_cmd=/bin/true
#                                           → startup fails for good (0):
#                                             live 0, restarted; liveness
#                                             never ran meanwhile
#
# Probe fields on /pod-probes/<uid> (flat, per kind l|r|s; pod_resolve
# writes them from a Pod's probes — an httpGet / tcpSocket as `@http …` /
# `@tcp …`, dialled by probe_dial):
#   <k>p_cmd <k>p_period <k>p_timeout <k>p_fail <k>p_succ <k>p_delay
# (unstated: upstream's defaults, 10 s / 1 s / 3 / 1 / 0)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-podlifecycle.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/workload_effect.fmod" "$MODULES_DIR/store_source.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-probe-e2e-XXXXXX)"
SRV_PID=""
trap '[ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null; rm -rf "$D"' EXIT
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

store_history() { # store_history <key> — every value ever put, one per line
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read()
want = sys.argv[2].encode()
p = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data):
        break
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]
    if k == want and op == 1:
        print(v.decode(errors="replace"))
    p += 15 + kl + vl
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. materialize sleeper bundles + the probe matrix =="
mkdir -p "$D/slp"
# Readiness: /bin/true every 1s, timeout 2s, 1 fail / 1 success to flip.
store_put "/pod-specs/pod-ready" "cmd=/bin/sleep 60;desired=running"
store_put "/pod-probes/pod-ready" "rp_cmd=/bin/true;rp_period=1;rp_timeout=2;rp_fail=1;rp_succ=1"
# Liveness: /bin/false every 1s, 2 consecutive fails flip live→0.
store_put "/pod-specs/pod-live" "cmd=/bin/sleep 60;desired=running;restart=always"
store_put "/pod-probes/pod-live" "lp_cmd=/bin/false;lp_period=1;lp_timeout=2;lp_fail=2;lp_succ=1"
# The Pods' own server: 200 on /ok, 500 on anything else, a TCP listener
# besides; and a port nothing listens on.
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
HP="$(free_port)"; DEAD="$(free_port)"
python3 - "$HP" >"$D/srv.log" 2>&1 <<'PY' &
import http.server, socketserver, sys
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        ok = self.path == "/ok" and self.headers.get("User-Agent", "").startswith("kube-probe/")
        sys.stderr.write(f"{self.path} {self.headers.get('User-Agent')}\n"); sys.stderr.flush()
        self.send_response(200 if ok else 500)
        self.send_header("Content-Length", "0")
        self.end_headers()
    def log_message(self, *a): pass
socketserver.ThreadingTCPServer.allow_reuse_address = True
socketserver.ThreadingTCPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
PY
SRV_PID=$!
store_put "/pod-specs/pod-tcp" "cmd=/bin/sleep 60;desired=running;restart=always"
store_put "/pod-probes/pod-tcp" "lp_cmd=@tcp 127.0.0.1 $HP;lp_period=1;lp_timeout=2;lp_fail=2;lp_succ=1"
store_put "/pod-specs/pod-tcpdown" "cmd=/bin/sleep 60;desired=running;restart=always"
store_put "/pod-probes/pod-tcpdown" "lp_cmd=@tcp 127.0.0.1 $DEAD;lp_period=1;lp_timeout=2;lp_fail=2;lp_succ=1"
store_put "/pod-specs/pod-http" "cmd=/bin/sleep 60;desired=running"
store_put "/pod-probes/pod-http" "rp_cmd=@http 127.0.0.1 $HP /ok;rp_period=1;rp_timeout=2;rp_fail=1;rp_succ=1"
store_put "/pod-specs/pod-http5" "cmd=/bin/sleep 60;desired=running"
store_put "/pod-probes/pod-http5" "rp_cmd=@http 127.0.0.1 $HP /bad;rp_period=1;rp_timeout=2;rp_fail=1;rp_succ=1"
# startup: readiness held back until it passed; one that fails for good is
# the container's liveness failing.
store_put "/pod-specs/pod-start" "cmd=/bin/sleep 60;desired=running"
store_put "/pod-probes/pod-start" "sp_cmd=/bin/true;sp_period=1;sp_timeout=2;sp_fail=3;sp_succ=1;rp_cmd=/bin/true;rp_period=1;rp_timeout=2;rp_fail=1;rp_succ=1"
store_put "/pod-specs/pod-startfail" "cmd=/bin/sleep 60;desired=running;restart=always"
store_put "/pod-probes/pod-startfail" "sp_cmd=/bin/false;sp_period=1;sp_timeout=2;sp_fail=2;sp_succ=1;lp_cmd=/bin/true;lp_period=1;lp_timeout=2;lp_fail=1;lp_succ=1"

echo "== 3. run the pipeline (probe cadence ~1s; liveness kill + backoff ~4s) =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 14 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. readiness: /probe-status/pod-ready flips ready 0→1 =="
PR="$(store_last /probe-status/pod-ready)"
[ "$PR" = "live=1;ready=1" ] || fail "pod-ready should be live=1;ready=1, got '$PR'"
# The flip itself (not just the final value): first publish is ready=0.
if ! store_history /probe-status/pod-ready | head -1 | grep -q 'ready=0'; then
  fail "pod-ready should have started ready=0 (readiness proves itself)"
fi
echo "   pod-ready: ready 0 -> 1"

echo "== 5. liveness: live=0 past threshold → restart + backoff =="
if ! store_history /probe-status/pod-live | grep -q 'live=0'; then
  fail "pod-live should have flipped live=0 (2x /bin/false)"
fi
# The failed run was torn down (destroyed appears) and relaunched under
# backoff with the count persisted ( restarts=<n>).
if ! store_history /sandbox-status/pod-live | grep -q 'state=destroyed'; then
  fail "pod-live liveness failure should tear the sandbox down"
fi
if ! store_history /pod-lifecycle-status/pod-live | grep -Eq 'restarts=[0-9]+'; then
  fail "pod-live should persist a restart count after the liveness kill"
fi
# The fresh run re-proves its probes: the verdict reset to live=1 after teardown.
LAST_LIVE="$(store_last /probe-status/pod-live)"
case "$LAST_LIVE" in
  live=*) echo "   pod-live: live flipped 0, pod restarted (verdict now '$LAST_LIVE')";;
  *) fail "pod-live probe status missing, got '$LAST_LIVE'";;
esac

echo "== 6. tcpSocket: a listening port is live; a refused one is a liveness failure =="
[ "$(store_last /probe-status/pod-tcp)" = "live=1;ready=1" ] || fail "pod-tcp: '$(store_last /probe-status/pod-tcp)'"
store_history /sandbox-exec-result/pod-tcp/probe-l | grep -qx 0 || fail "pod-tcp was never dialled: $(store_history /sandbox-exec-result/pod-tcp/probe-l | head -3)"
if store_history /pod-lifecycle-status/pod-tcp | grep -Eq 'Restarting|restarts='; then
  fail "pod-tcp restarted though its port listens"
fi
store_history /probe-status/pod-tcpdown | grep -q 'live=0' || fail "pod-tcpdown never went live=0: $(store_history /probe-status/pod-tcpdown | tail -3)"
store_history /pod-lifecycle-status/pod-tcpdown | grep -Eq 'restarts=[0-9]+' || fail "pod-tcpdown was not restarted"
echo "   pod-tcp: dialled, live; pod-tcpdown: refused, live=0, restarted"

echo "== 7. httpGet: 200..399 passes, 500 does not =="
[ "$(store_last /probe-status/pod-http)" = "live=1;ready=1" ] || fail "pod-http: '$(store_last /probe-status/pod-http)'"
[ "$(store_last /probe-status/pod-http5)" = "live=1;ready=0" ] || fail "pod-http5: '$(store_last /probe-status/pod-http5)'"
grep -q "^/ok kube-probe/" "$D/srv.log" || fail "the server saw no probe GET /ok: $(head -3 "$D/srv.log")"
echo "   pod-http: ready (GET /ok 200, User-Agent kube-probe/…); pod-http5: never ready (500)"

echo "== 8. startupProbe: readiness held back until it passed; failing for good restarts =="
store_history /probe-status/pod-start | head -1 | grep -q 'ready=0' || fail "pod-start was ready before its startup probe passed: $(store_history /probe-status/pod-start | head -2)"
[ "$(store_last /probe-status/pod-start)" = "live=1;ready=1" ] || fail "pod-start: '$(store_last /probe-status/pod-start)'"
store_history /probe-rt-s/pod-start | grep -q 'verdict=1' || fail "pod-start's startup verdict never 1: $(store_last /probe-rt-s/pod-start)"
store_history /probe-status/pod-startfail | grep -q 'live=0' || fail "pod-startfail never live=0: $(store_history /probe-status/pod-startfail | tail -3)"
store_history /pod-lifecycle-status/pod-startfail | grep -Eq 'restarts=[0-9]+' || fail "pod-startfail was not restarted"
[ -z "$(store_history /sandbox-exec/pod-startfail/probe-l)" ] || fail "pod-startfail's liveness probe ran before its startup probe passed"
echo "   pod-start: ready only after startup passed; pod-startfail: startup failed for good -> restarted, liveness never ran"

echo "== E2E green: probes as params over the /sandbox-exec/ seam — exec, tcpSocket and httpGet (probe_dial), startup gating, liveness kill+backoff =="
