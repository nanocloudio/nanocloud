#!/usr/bin/env bash
# Live E2E: the join, on the board's store, right after every restart.
#
# The node-PKI plane of scripts/node-pki-e2e.sh (rendered as the board's:
# NANOCLOUD_PEER_TRUST=authority, nothing seeded) runs with lattice's
# TEST-ONLY read-wait knob on — `fault_read_eagain_every: N` makes one store
# step in N busy, every read in it answering EAGAIN, as the Pi 5's store
# answers while its device is taken by a write (and, just after boot, while
# it recovers). The node is restarted again and again; each boot announces a
# fresh join secret and the operator presents it at once (scripts/rig-join.sh).
#
# The join spends the secret with a TAKE (store_effect / store_doc
# `take_value`, _shared/store.rs): read with its revision, claimed on that
# revision, deleted. A read that waits, a write that pends, a write whose
# answer the store drops and a record that moved are all "not yet" or "try
# again" — never "not recognised".
#
# PASS = on every boot of NC_JOIN_BOOTS (default 10):
#   - the announced secret joins (200, a kubeconfig) — never 401;
#   - presented again, it is 401 (spent exactly once);
# and on the last boot NC_JOIN_RACE (default 6) presentations of one secret
# at once are answered exactly one 200, the rest 401, and nothing else.
#
# The knob ships in no graph (lattice's tools/ci/no_fault_injection.sh): it
# is set here, in the rendered copy under $D only. NC_FAULT_EVERY (default 3;
# >= 2; 0 runs the same joins with the knob off).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT=7443
EVERY="${NC_FAULT_EVERY:-3}"
BOOTS="${NC_JOIN_BOOTS:-10}"
RACE="${NC_JOIN_RACE:-6}"

. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
command -v curl >/dev/null || { echo "FAIL: curl not found"; exit 1; }
case "$EVERY" in ''|*[!0-9]*) echo "FAIL: NC_FAULT_EVERY must be a number"; exit 1 ;; esac
[ "$EVERY" = 0 ] || [ "$EVERY" -ge 2 ] || { echo "FAIL: NC_FAULT_EVERY must be 0 or >= 2"; exit 1; }

D="$(mktemp -d /tmp/nc-join-wait-XXXXXX)"
STORE="$D/store"
mkdir -p "$STORE"
nc_lattice_prepare "$STORE"
RUNTIME_PID=""
stop() { if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; fi; }
cleanup() { stop; if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; grep -a "node_pki\|\[join\]\|FAULT" "$D/run.log" 2>/dev/null | tail -12; tail -15 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. render the node-PKI plane as the board's, the read-wait knob on (N=$EVERY) =="
NANOCLOUD_PEER_TRUST=authority nc_controlplane_graph "$STORE" "$D/graph.yaml" \
  || fail "the control plane did not render"
python3 - "$D/graph.yaml" "$EVERY" <<'PY' || fail "the knob did not apply"
import os, sys
path, every = sys.argv[1:]
g = open(path).read()
old = "      history_revisions: 65536\n"
if g.count(old) != 1:
    sys.exit(f"expected one store, found {g.count(old)}")
if every != "0":
    # Busy steps reach the store's own lookup (a HEAD that waits), and each
    # single-store write's log stays pending a few steps so another caller's
    # write interleaves: the two board conditions behind the join 401.
    g = g.replace(old, old + f"      fault_read_eagain_every: {every}\n"
                  "      fault_busy_reaches_store: 1\n"
                  f"      fault_log_hold_steps: {os.environ.get('NC_FAULT_LOG_HOLD', '4')}\n")
if "join: 1" not in g:
    sys.exit("nothing mints the join")
open(path, "w").write(g)
PY
nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"
if [ "$EVERY" != 0 ]; then
  grep -aq "FAULT INJECTION ON" "$D/modules.bin" || fail "the built store does not know the fault knob"
fi

# A device key, so the vault keeps the authority across restarts (as the
# deb's unit supplies one): every boot's digest is the first one's.
SEAL="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
mkdir -p "$D/vault"
boot() {
  : >"$D/run.log"
  env -C "$STORE" FLUXOR_SEAL_KEY="$SEAL" FLUXOR_VAULT_DIR="$D/vault" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  JOIN=""
  for _ in $(seq 400); do
    JOIN="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/run.log" | tail -1 || true)"
    [ -n "$JOIN" ] && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the runtime exited"
    sleep 0.25
  done
  [ -n "$JOIN" ] || fail "no join secret announced in 100 s"
  SECRET="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
  CA_SHA="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$JOIN")"
  if [ "$EVERY" != 0 ]; then
    grep -aq "FAULT INJECTION ON (test only): one store step in N busy, every read EAGAIN, N=$EVERY" "$D/run.log" \
      || fail "the store did not turn the fault knob on"
  fi
}
present() { # present <secret> -> the HTTP status of one join presentation
  curl -sk --max-time 20 -o /dev/null -w '%{http_code}\n' -X POST --data '' \
    -H "Authorization: Bearer $1" "https://127.0.0.1:$PORT/apis/nanocloud.io/v1/join" || true
}

echo "== 2. $BOOTS restarts: each boot's secret joins at once, and only once =="
SEEN=""
for i in $(seq "$BOOTS"); do
  boot
  case " $SEEN " in *" $SECRET "*) fail "boot $i announced a secret an earlier boot did" ;; esac
  SEEN="$SEEN $SECRET"
  if [ "$i" -lt "$BOOTS" ]; then
    bash "$ROOT/scripts/rig-join.sh" "$SECRET" "$CA_SHA" "127.0.0.1:$PORT" "$D/kc-$i" 2>"$D/join.err" >/dev/null \
      || fail "boot $i: the announced secret did not join: $(cat "$D/join.err")"
    code="$(present "$SECRET")"
    [ "$code" = 401 ] || fail "boot $i: the spent secret answered $code, not 401"
    echo "   boot $i: joined; presented again -> 401"
    stop
  fi
done

echo "== 3. boot $BOOTS: $RACE presentations of one secret at once — exactly one joins =="
for k in $(seq "$RACE"); do present "$SECRET" >"$D/race-$k" & done
wait $(jobs -p | grep -v "^$RUNTIME_PID$") 2>/dev/null || true
codes="$(cat "$D"/race-* | sort | tr '\n' ' ')"
ok="$(cat "$D"/race-* | grep -cx 200 || true)"
refused="$(cat "$D"/race-* | grep -cx 401 || true)"
[ "$ok" = 1 ] && [ "$refused" = $((RACE - 1)) ] || fail "the race answered: $codes (want one 200, $((RACE - 1)) x 401)"
echo "   answered: $codes"
code="$(present "$SECRET")"
[ "$code" = 401 ] || fail "after the race the secret answered $code, not 401"

if [ "$EVERY" != 0 ]; then
  echo "== 4. the store really made reads wait =="
  inj=""
  # The store's heartbeat comes every few seconds.
  for _ in $(seq 60); do
    inj="$(grep -ao '\[kvw\] store hb .* fault=[0-9]*' "$D/run.log" | tail -1 | sed 's/.*fault=//' || true)"
    [ -n "$inj" ] && [ "$inj" -gt 0 ] && break
    sleep 1
  done
  [ -n "$inj" ] && [ "$inj" -gt 0 ] || fail "no injected read waits counted on [kvw] store hb"
  echo "   reads answered EAGAIN by injection (this boot): $inj"
fi
echo "== E2E green: $BOOTS boots under read waits, every secret joined at once and once only, and a race had one winner =="
