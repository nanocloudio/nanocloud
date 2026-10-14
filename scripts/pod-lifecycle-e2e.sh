#!/usr/bin/env bash
# Live E2E for the pod-lifecycle chain -> the sandbox runtime -> workload pipeline —
# the kubelet's decision half (Chronicle params) driving the effect half (a
# module) driving the workload capability surface, over the control-plane
# store, verified UNPRIVILEGED.
#
# The store is single-process, owned by the fluxor-linux runtime, and seeded
# from its durable append-log at init: this script (playing the control plane
# after it materialized the bundle) writes a /pod-specs/<uid> into
# `$D/store.log`. Inside the runtime: the lifecycle chain (DECISION) projects
# /sandboxes/<uid>; the sandbox runtime (EFFECT) creates+starts the sandbox via the
# workload contract and writes /sandbox-status/<uid>; the chain maps that back
# to /pod-lifecycle-status/<uid>. The decision<->effect handshake happens live
# in the one in-process store during the run. A NULL sandbox (no isolate) keeps
# it unprivileged.
#
# Compact format:
#   /pod-specs/<uid>            = "cmd=<argv>;desired=<running|deleted>[;rootfs=<path>][;iso=1]"
#   /pod-lifecycle-status/<uid> = "phase=<Pending|Running|Succeeded|Failed|Terminating>"
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/aarch64/modules"
GRAPH="$ROOT/packaging/debian/fluxor-podlifecycle.yaml"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/workload_effect.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-pl-e2e-XXXXXX)"
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

echo "== 1. build the config + module table from the graph (3 modules) =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. materialize bundles (echo → exit 0, false → exit 1) + a restart matrix =="
# pod1: succeeds, no restart -> Succeeded (terminal)
store_put "/pod-specs/pod1"    "cmd=/bin/echo hello-pod;desired=running"
# pod-nf: fails, restart=never -> Failed (terminal)
store_put "/pod-specs/pod-nf"  "cmd=/bin/false;desired=running;restart=never"
# pod-of: fails, restart=onfailure -> restarts (never terminal)
store_put "/pod-specs/pod-of"  "cmd=/bin/false;desired=running;restart=onfailure"
# pod-al: succeeds, restart=always -> restarts anyway (never terminal)
store_put "/pod-specs/pod-al"  "cmd=/bin/echo hello-pod;desired=running;restart=always"
# pod-slp: long-running (sleep) — deleted in phase 5, after a restart
store_put "/pod-specs/pod-slp" "cmd=/bin/sleep 30;desired=running"

echo "== 3. run the pipeline (pod_lifecycle -> the sandbox runtime -> oci) =="
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 4 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 4. verify the lifecycle + restart-policy decisions =="
SB="$(store_last /sandbox-status/pod1)"
[ "$SB" = "state=exited;code=0" ] || fail "pod1 sandbox status wrong: got '$SB'"
[ "$(store_last /pod-lifecycle-status/pod1)" = "phase=Succeeded" ] \
  || fail "pod1 (never, exit 0) should be Succeeded"
[ "$(store_last /pod-lifecycle-status/pod-nf)" = "phase=Failed" ] \
  || fail "pod-nf (never, exit 1) should be Failed"
not_terminal() { # not_terminal <uid>
  local p; p="$(store_last "/pod-lifecycle-status/$1")"
  case "$p" in
    phase=Succeeded*|phase=Failed*) fail "$1 should NOT be terminal, got '$p'";;
    "") fail "$1 has no phase";;
    *) echo "   $1: $p (restarting, non-terminal)";;
  esac
}
not_terminal pod-of   # onfailure + exit 1 → keeps restarting
not_terminal pod-al   # always + exit 0 → restarts despite success
# restart backoff: a failing pod's consecutive-failure count is persisted
# as `;restarts=<n>` on its lifecycle status (pod-of failed at least once).
case "$(store_last /pod-lifecycle-status/pod-of)" in
  *restarts=*) echo "   pod-of: backoff count persisted";;
  *) fail "pod-of should carry ;restarts=<n> (backoff bookkeeping), got '$(store_last /pod-lifecycle-status/pod-of)'";;
esac
case "$(store_last /pod-lifecycle-status/pod-slp)" in
  phase=Running*) echo "   pod-slp: Running";;
  *) fail "pod-slp (sleep 30) should be Running, got '$(store_last /pod-lifecycle-status/pod-slp)'";;
esac
echo "   pod1: Succeeded   pod-nf: Failed"

echo "== 5. kill-with-grace + no-restart-during-delete pin =="
# Flip a restart=always pod AND the sleeper to desired=deleted. The pin: the
# decision evaluates desired==deleted BEFORE the restart branch, so neither
# pod may be relaunched by its restartPolicy. Both are deleted across a
# RESTART (the next run is another boot): what ran in the last boot is gone,
# so the delete is direct — no kill to send, nothing created again. The
# graceful kill of a LIVE Pod, within one boot, is step 7.
store_put "/pod-specs/pod-al"  "cmd=/bin/echo hello-pod;desired=deleted;restart=always;grace=1"
store_put "/pod-specs/pod-slp" "cmd=/bin/sleep 30;desired=deleted;grace=5"
# And a Pod whose status says `running` under ANOTHER boot: the node
# restarted, so no process is left to signal. It must take the direct delete,
# never the graceful kill — a kill key nothing will consume would hold its
# delete forever (a stale status from before the restart once did, racily,
# whenever the first run was stopped right after a restart relaunched).
store_put "/pod-specs/pod-stale" "cmd=/bin/sleep 30;desired=deleted;grace=5"
store_put "/pod-runtime/pod-stale" "kill_phase=0;kill_deadline=0;restarts=0;was_terminal=0"
store_put "/sandboxes/pod-stale" "cmd=/bin/sleep 30;phase=start"
store_put "/sandbox-status/pod-stale" "state=running"
store_put "/sandbox-boot-of/pod-stale" "a-boot-before-this-one"
# Run until every condition step 6 asserts holds, rather than for a fixed
# time: SIGTERM, the sleeper's exit, the delete and each kill key's
# consumption are a chain of store round trips, so how long the chain takes
# is a property of the host. Both pods are waited for — pod-al's writes are
# the same chain as pod-slp's, and a wait that covered only one left the
# other to a race.
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 120 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run2.log" 2>&1 &
RUN2=$!
settled() { # every assertion step 6 makes, as a predicate
  local uid
  for uid in pod-al pod-slp; do
    case "$(store_last "/pod-lifecycle-status/$uid")" in phase=Terminating*) ;; *) return 1 ;; esac
    case "$(store_last "/sandboxes/$uid")" in *phase=delete*) ;; *) return 1 ;; esac
    [ -z "$(store_last "/sandbox-kill/$uid")" ] || return 1
  done
  case "$(store_last /sandbox-status/pod-slp)" in state=killed*|state=destroyed*) ;; *) return 1 ;; esac
  case "$(store_last /pod-lifecycle-status/pod-stale)" in phase=Terminating*) ;; *) return 1 ;; esac
  case "$(store_last /sandboxes/pod-stale)" in *phase=delete*) ;; *) return 1 ;; esac
}
# Settled AND still settled: the delete runs as a sequence (the kill key
# written, the grace deadline waited out, the key consumed, the sandbox
# projected gone), so a single sample can catch a state that holds for one
# step only. Three in a row is the end of the sequence, not a moment in it.
waited=0 hold=0
for _ in $(seq 300); do
  if settled; then
    hold=$((hold + 1))
    [ "$hold" -ge 3 ] && { waited=1; break; }
  else
    hold=0
  fi
  sleep 0.2
done
# Not settled in 60 s: stop anyway and let step 6 name the condition missing.
[ "$waited" = 1 ] || echo "   (the wait expired; step 6 reports what did not land)"
kill "$RUN2" 2>/dev/null || true; wait "$RUN2" 2>/dev/null || true

echo "== 6. verify: Terminating, destroyed, kill key consumed, never relaunched =="
for uid in pod-al pod-slp; do
  P="$(store_last "/pod-lifecycle-status/$uid")"
  case "$P" in
    phase=Terminating*) ;;
    *) fail "$uid should be Terminating (delete wins over restart), got '$P'";;
  esac
  SB="$(store_last "/sandboxes/$uid")"
  case "$SB" in
    *phase=delete*) ;;
    *) fail "$uid sandbox spec should be phase=delete, got '$SB'";;
  esac
  [ "$(store_last "/sandbox-kill/$uid")" = "" ] \
    || fail "$uid kill key should be consumed, got '$(store_last "/sandbox-kill/$uid")'"
done
# The stale-boot Pod: deleted directly, and never sent a kill.
case "$(store_last /pod-lifecycle-status/pod-stale)" in phase=Terminating*) ;;
  *) fail "pod-stale should be Terminating, got '$(store_last /pod-lifecycle-status/pod-stale)'" ;; esac
case "$(store_last /sandboxes/pod-stale)" in *phase=delete*) ;;
  *) fail "pod-stale (running under another boot) should be deleted directly, got '$(store_last /sandboxes/pod-stale)'" ;; esac
python3 - "$D/store.log" <<'PY2' || fail "pod-stale was sent a kill: a status from another boot is nothing live to signal"
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    k = data[p+15:p+15+kl]; p += 15 + kl + vl
    if k == b"/sandbox-kill/pod-stale" and op == 1: sys.exit(1)
PY2
echo "   pod-stale: running under another boot -> deleted directly, never signalled"
ST="$(store_last /sandbox-status/pod-slp)"
case "$ST" in
  state=killed*|state=destroyed*) echo "   pod-slp: $ST (the last boot's process, deleted across the restart)";;
  *) fail "pod-slp should be killed/destroyed, got '$ST'";;
esac

echo "== 7. a LIVE Pod deleted within its own boot: SIGTERM, then the delete =="
# The deletion arrives while the runtime that started the Pod still runs: a
# driver writes the spec over a remote_channel into a store_effect (the store
# replays its log only at open, so a write between runs is a restart).
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
DRV_PORT="$(free_port)"
python3 - "$GRAPH" "$D/live.yaml" "$DRV_PORT" <<'PY2'
import sys
src, dst, port = sys.argv[1], sys.argv[2], sys.argv[3]
g = open(src).read()
mods, wiring = g.split("\nwiring:\n", 1)
mods = mods.replace("\nmodules:\n", "\nplatform:\n  net: {}\nmodules:\n", 1)
mods += f"""
  - name: drv
    type: remote_channel
    transport: net
    role: listen
    port: {port}
    require_peer: 0
    channels: "OctetStream:bytes:16384"
  - name: drv_put
    type: store_effect
    key_prefix: "/pod-specs/"
"""
wiring = wiring.rstrip("\n") + """
  - from: linux_net.net_out
    to: drv.transport_in
  - from: drv.transport_out
    to: linux_net.net_in
  - from: drv.ch0_rx
    to: drv_put.request_in
  - from: drv_put.response_out
    to: drv.ch0
"""
open(dst, "w").write(mods + "\nwiring:\n" + wiring)
PY2
nc_build_workload "$ROOT" "$D/live.yaml" "$D/live.bin" "$D/live-modules.bin" >/dev/null \
  || fail "the graph with the driver's channel did not build"
store_put "/pod-specs/pod-grace" "cmd=/bin/sleep 30;desired=running;grace=5"
FLUXOR_STORE_DIR="$D" RUST_LOG="${RUST_LOG:-warn}" timeout 120 "$FLUXOR_RUNTIME" \
  --config "$D/live.bin" --modules "$D/live-modules.bin" >"$D/run3.log" 2>&1 &
RUN3=$!
for _ in $(seq 150); do
  case "$(store_last /pod-lifecycle-status/pod-grace)" in phase=Running*) break ;; esac
  sleep 0.2
done
case "$(store_last /pod-lifecycle-status/pod-grace)" in phase=Running*) echo "   pod-grace: Running in this boot" ;;
  *) kill "$RUN3" 2>/dev/null; fail "pod-grace never ran: '$(store_last /pod-lifecycle-status/pod-grace)'" ;; esac
PYTHONPATH="$ROOT/scripts/support" python3 -B - "$DRV_PORT" <<'PY2' || { kill "$RUN3" 2>/dev/null; fail "the driver's delete was not stored"; }
import struct, sys, fxrc
s = fxrc.connect(int(sys.argv[1]), timeout=30)
def field(n, v):
    b = struct.pack("<q", v) if isinstance(v, int) else v.encode()
    return bytes([n, 1 if isinstance(v, int) else 0]) + struct.pack("<H", len(b)) + b
fields = [field(1, 1), field(2, 3), field(3, "pod-grace"),
          field(4, "cmd=/bin/sleep 30;desired=deleted;grace=5")]
s.sendall(bytes([len(fields)]) + b"".join(fields))
buf = b""
while len(buf) < 1:
    buf += s.recv(4096)
PY2
graceful() {
  case "$(store_last /pod-lifecycle-status/pod-grace)" in phase=Terminating*) ;; *) return 1 ;; esac
  case "$(store_last /sandboxes/pod-grace)" in *phase=delete*) ;; *) return 1 ;; esac
  case "$(store_last /sandbox-status/pod-grace)" in state=killed*|state=destroyed*) ;; *) return 1 ;; esac
  [ -z "$(store_last /sandbox-kill/pod-grace)" ] || return 1
}
for _ in $(seq 150); do graceful && break; sleep 0.2; done
kill "$RUN3" 2>/dev/null || true; wait "$RUN3" 2>/dev/null || true
graceful || fail "pod-grace did not settle: lifecycle='$(store_last /pod-lifecycle-status/pod-grace)' sandbox='$(store_last /sandboxes/pod-grace)' status='$(store_last /sandbox-status/pod-grace)' kill='$(store_last /sandbox-kill/pod-grace)'"
python3 - "$D/store.log" <<'PY2' || fail "pod-grace was not killed gracefully (no sig=term, or the sleeper did not die of it)"
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0; term = killed = False
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    k = data[p+15:p+15+kl]; v = data[p+15+kl:p+15+kl+vl]; p += 15 + kl + vl
    if k == b"/sandbox-kill/pod-grace" and op == 1 and v.startswith(b"sig=term"): term = True
    if k == b"/sandbox-status/pod-grace" and op == 1 and v.startswith(b"state=killed;code=15"): killed = True
sys.exit(0 if term and killed else 1)
PY2
echo "   pod-grace: sig=term sent, the sleeper died of SIGTERM (code 15), the key consumed, the sandbox deleted"

echo "== E2E green: decision→effect→oci + restartPolicy + backoff + kill-with-grace pin =="
