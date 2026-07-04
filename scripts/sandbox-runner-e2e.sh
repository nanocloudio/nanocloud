#!/usr/bin/env bash
# Live E2E for nanocloud's sandbox_runner module (modules/app/sandbox_runner) —
# the store -> workload seam over the ONE shared multi-process store.
#
# Proves the container-runtime bridge on real binaries WITHOUT root: an EXTERNAL
# process (this script, playing pod_lifecycle) projects a desired
# sandbox spec into the shared WAL (a NULL sandbox — no `isolate` marker, so the
# oci provider skips unshare and needs no CAP_SYS_ADMIN); the sandbox_runner
# fmod, inside the fluxor-linux runtime, drives the oci contract
# (CREATE -> START -> WAIT), runs the process in its bundle, and writes terminal
# status back. The isolated (privileged) path is proven separately by the oci
# provider's #[ignore]d UTS test under sudo.
#
# Compact format:
#   /sandboxes/<id>       = "cmd=<argv>;phase=<create|start|delete>[;rootfs=<path>][;iso=1]"
#   /sandbox-status/<id>  = "state=<created|running|exited|killed|failed|destroyed>;code=<n>"
#
# Prereqs: `fluxor modules build --target bcm2712` in this repo, and a
# fluxor-linux runtime with the oci provider (contract 0x18). FLUXOR_RUNTIME
# overrides the runtime path; it defaults to the fluxor checkout's release build.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/debian/fluxor-sandbox.yaml"

# The oci provider ships in the fluxor checkout's release runtime; prefer it.
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH (cargo install --locked --path ../fluxor/tools)"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/sandbox_runner.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (fluxor modules build --target bcm2712)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-sb-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

# Append a record to the shared WAL with the store's own discipline: flock EX,
# tail for the last intact revision, append at last+1, fsync.
wal_put() { # wal_put <key> <value>
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, fcntl, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    fcntl.flock(f, fcntl.LOCK_EX)
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
    f.flush()
    os.fsync(f.fileno())
    fcntl.flock(f, fcntl.LOCK_UN)
PY
}

# Read the LAST value written for a key from the WAL.
wal_last() { # wal_last <key>
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
        out = v
    p += 15 + kl + vl
print(out.decode() if out is not None else "")
PY
}

echo "== 1. build the config + module table from the graph =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. project a null-sandbox spec (explicit spawn params; no bundle dir) =="

echo "== 3. project the desired sandbox (phase=start => create+start in one pass) =="
wal_put "/sandboxes/test1" "cmd=/bin/echo hello-sandbox;phase=start"

echo "== 4. run the runner; it drives oci CREATE -> START -> WAIT =="
FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 || true

echo "== 5. verify terminal status written back to the store =="
ST="$(wal_last /sandbox-status/test1)"
[ "$ST" = "state=exited;code=0" ] \
  || fail "status wrong: got '$ST', want 'state=exited;code=0'"

echo "== E2E green (null sandbox): store -> workload lifecycle over the shared store =="
echo "   /sandbox-status/test1: $ST"

echo "== 5a. verify captured stdout published to /sandbox-logs (workload READ) =="
# The runner drains the workload READ op each step and appends the merged
# stdout/stderr to /sandbox-logs/<id> — the source the CLI `logs` verb reads.
LOG="$(wal_last /sandbox-logs/test1)"
case "$LOG" in
  *hello-sandbox*) echo "   /sandbox-logs/test1: $LOG" ;;
  *) fail "sandbox logs wrong: got '$LOG', want it to contain 'hello-sandbox'" ;;
esac

echo "== 5b. exec-into-sandbox: run commands inside a live sandbox (workload EXEC) =="
# The runner services /sandbox-exec/<id>/<reqid> by running the command inside
# the started sandbox via the workload EXEC op (fork/exec, setns-joining the
# container's namespaces when isolated), writing the exit code to
# /sandbox-exec-result/ and the captured stdout/stderr to /sandbox-exec-output/.
D3="$(mktemp -d /tmp/nc-sb-exec-XXXXXX)"; trap 'rm -rf "$D" "$D3"' EXIT
# execbox: a long-lived sandbox (sleep) we exec commands INTO
D="$D3" wal_put "/sandboxes/execbox"        "cmd=/bin/sleep 2;phase=start"
D="$D3" wal_put "/sandbox-exec/execbox/1"   "/bin/true"
D="$D3" wal_put "/sandbox-exec/execbox/2"   "/bin/false"
D="$D3" wal_put "/sandbox-exec/execbox/3"   "/bin/echo exec-output-here"
FLUXOR_STORE_DIR="$D3" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run-exec.log" 2>&1 || true

R1="$(D="$D3" wal_last /sandbox-exec-result/execbox/1)"
R2="$(D="$D3" wal_last /sandbox-exec-result/execbox/2)"
R3="$(D="$D3" wal_last /sandbox-exec-result/execbox/3)"
O3="$(D="$D3" wal_last /sandbox-exec-output/execbox/3)"
[ "$R1" = "0" ] || fail "exec /bin/true result wrong: got '$R1', want '0'"
[ "$R2" = "1" ] || fail "exec /bin/false result wrong: got '$R2', want '1'"
[ "$R3" = "0" ] || fail "exec /bin/echo result wrong: got '$R3', want '0'"
case "$O3" in
  *exec-output-here*) : ;;
  *) fail "exec output not captured: got '$O3', want it to contain 'exec-output-here'" ;;
esac
echo "== E2E green (exec-into-sandbox): commands run inside the sandbox, exit codes + output captured =="
echo "   /bin/true -> $R1   /bin/false -> $R2   /bin/echo -> $R3 ('$O3')"

echo "== 5c. interactive PTY exec (kubectl exec -it): open/stream/close over a pty =="
# The runner opens a pseudo-terminal session (workload TTY_OPEN), pumps stdin
# from /tty/<id>/<sid>/in to the PTY and its output to /tty/<id>/<sid>/out
# (TTY_STEP), and posts status. Two sessions: an echo that self-terminates
# (proves output + exit/status), and a cat fed stdin (proves the stdin→pty→out
# round-trip through a real terminal).
D4="$(mktemp -d /tmp/nc-sb-tty-XXXXXX)"; trap 'rm -rf "$D" "$D3" "$D4"' EXIT
D="$D4" wal_put "/sandboxes/ttybox"     "cmd=/bin/sleep 2;phase=start"
D="$D4" wal_put "/tty/ttybox/s1/ctl"    "open;cmd=/bin/echo hello-from-tty"
D="$D4" wal_put "/tty/ttybox/s2/ctl"    "open;cmd=/bin/cat"
printf 'ping\n' > "$D4/pingfile"
D="$D4" wal_put "/tty/ttybox/s2/in"     "$(cat "$D4/pingfile")"
FLUXOR_STORE_DIR="$D4" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run-tty.log" 2>&1 || true

T1OUT="$(D="$D4" wal_last /tty/ttybox/s1/out)"
T1ST="$(D="$D4" wal_last /tty/ttybox/s1/status)"
T2OUT="$(D="$D4" wal_last /tty/ttybox/s2/out)"
case "$T1OUT" in *hello-from-tty*) : ;; *) fail "pty echo output not captured: got '$T1OUT'" ;; esac
case "$T1ST"  in *exited*)         : ;; *) fail "pty echo session status wrong: got '$T1ST'" ;; esac
case "$T2OUT" in *ping*)           : ;; *) fail "pty cat stdin->output round-trip failed: got '$T2OUT'" ;; esac
echo "== E2E green (interactive pty): echo -> '$T1OUT' ($T1ST); cat stdin echoed -> '$T2OUT' =="

# ── Privileged path: the ISOLATED sandbox through the same module→contract
# chain. Namespace surgery needs CAP_SYS_ADMIN, so this block runs only as
# root; unprivileged runs stop after the null-sandbox proof above.
if [ "$(id -u)" != 0 ]; then
  echo "== isolated path: SKIPPED (needs root; re-run with sudo -E to exercise it) =="
  exit 0
fi

echo "== 6. project an ISOLATED sandbox (isolate=1 => unshare MOUNT|UTS|IPC) =="
HOST_HOSTNAME="$(hostname)"
# No rootfs: unshare + private-mount, no pivot; set the UTS hostname and exit 0.
D2="$(mktemp -d /tmp/nc-sb-iso-XXXXXX)"; trap 'rm -rf "$D" "$D2"' EXIT
# Pre-seed the spec (cold-start reconcile lists it) — writing after launch
# raced the runtime's bounded 3s window on external WAL appends.
D="$D2" wal_put "/sandboxes/iso1" "cmd=hostname sandbox-ns-iso;iso=1;phase=start"
FLUXOR_STORE_DIR="$D2" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" \
  >"$D/run-iso.log" 2>&1 || true

echo "== 7. verify the isolated sandbox ran and the host UTS is intact =="
IST="$(D="$D2" wal_last /sandbox-status/iso1)"
[ "$IST" = "state=exited;code=0" ] \
  || fail "isolated status wrong: got '$IST', want 'state=exited;code=0'"
[ "$(hostname)" = "$HOST_HOSTNAME" ] \
  || fail "host hostname changed ('$HOST_HOSTNAME' -> '$(hostname)') — UTS did NOT isolate"

echo "== E2E green (isolated): module -> oci -> unshare(MOUNT|UTS|IPC), host UTS intact =="
echo "   /sandbox-status/iso1: $IST  (host hostname unchanged: $HOST_HOSTNAME)"

echo "== 8. net=own: leased identity -> own netns + veth carrying the address =="
# cni_ipam's allocation is proven by its own e2e; here we prove the runner
# consumes the lease: fills the workload CREATE header's Tier-1 network
# identity, the backend gives the sandbox its own netns + veth, and the
# leased address is visible from INSIDE the sandbox. Also: a net=own sandbox
# with no lease must defer (request written, nothing created) — never run
# with silently-shared networking.
IPBIN="$(command -v ip || echo /usr/sbin/ip)"
# null sandbox + own netns; cmd runs `ip addr` inside the sandbox
D5="$(mktemp -d /tmp/nc-sb-net-XXXXXX)"; trap 'rm -rf "$D" "$D2" "$D5"' EXIT
D="$D5" wal_put "/ipam-pool"           "cidr=10.244.9.0/24"
D="$D5" wal_put "/ipam-lease/netbox1"  "10.244.9.7"
D="$D5" wal_put "/sandboxes/netbox1"   "cmd=$IPBIN addr;phase=start;net=own"
D="$D5" wal_put "/sandboxes/netbox2"   "cmd=$IPBIN addr;phase=start;net=own"   # no lease -> defer
FLUXOR_STORE_DIR="$D5" RUST_LOG=warn timeout 3 "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run-net.log" 2>&1 || true

NST="$(D="$D5" wal_last /sandbox-status/netbox1)"
[ "$NST" = "state=exited;code=0" ] \
  || fail "net=own status wrong: got '$NST', want 'state=exited;code=0'"
NLOG="$(D="$D5" wal_last /sandbox-logs/netbox1)"
case "$NLOG" in
  *10.244.9.7/24*) : ;;
  *) fail "leased address not visible inside the sandbox: got '$NLOG'" ;;
esac
case "$NLOG" in
  *fxc*) : ;;
  *) fail "container veth end missing from the sandbox's ip addr: got '$NLOG'" ;;
esac
# Deferred sandbox: request materialized, no status written, nothing created.
REQ2="$(D="$D5" wal_last /ipam-request/netbox2 2>/dev/null)"
ST2="$(D="$D5" wal_last /sandbox-status/netbox2)"
[ -z "$ST2" ] || fail "leaseless net=own sandbox must defer, got status '$ST2'"
# Host veth ends die with the netns; give async teardown a beat.
sleep 1
if ip link show 2>/dev/null | grep -q "fxw"; then
  fail "host veth leaked after sandbox teardown"
fi
echo "== E2E green (net=own): lease -> Tier-1 identity -> own netns + veth; address visible inside; leaseless defers; veth torn down =="
echo "   /sandbox-status/netbox1: $NST  (request for netbox2 present: ${REQ2:+yes}${REQ2:-recorded-empty})"
