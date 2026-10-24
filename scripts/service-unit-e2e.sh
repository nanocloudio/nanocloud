#!/usr/bin/env bash
# Live E2E for the INSTALLED control plane's store, as nanocloud.service runs it.
#
# The claim under test: the shipped unit runs the control-plane graph on the
# lattice store in its state directory, never on fluxor's host store — and a
# node upgraded from the host store keeps its objects: the host store.log it
# still has is imported into lattice once, at the first start, and not again.
#
# The unit's own WorkingDirectory and Environment= lines are read from
# packaging/debian/nanocloud.service (with /var/lib/nanocloud.io mapped into a
# temp dir), so the test runs what the package installs, not a copy of it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT="$ROOT/packaging/debian/nanocloud.service"
GRAPH="$ROOT/packaging/debian/fluxor-controlplane.yaml"
PORT=7443

. "$ROOT/scripts/fluxor-env.sh"
command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }

D="$(mktemp -d /tmp/nc-unit-XXXXXX)"
RUNTIME_PID=""
cleanup() {
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; fi
  if [ -n "${KEEP:-}" ]; then echo "kept: $D"; else rm -rf "$D"; fi
}
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -30 "$D/run.log" 2>/dev/null || true; exit 1; }

echo "== 1. the unit's store settings =="
grep -q '^Environment=FLUXOR_STORE_DIR=' "$UNIT" \
  && fail "the unit sets FLUXOR_STORE_DIR: fluxor would bring up its host store beside lattice's"
unit_val() { sed -n "s#^$1=##p" "$UNIT" | head -1; }
WD="$(unit_val WorkingDirectory)"
case "$WD" in
  /var/lib/nanocloud.io/*) ;;
  *) fail "WorkingDirectory is $WD, not the state directory: the lattice store would not live with the node's state" ;;
esac
map() { printf '%s' "${1//\/var\/lib\/nanocloud.io/$D/var}"; }
STORE="$(map "$WD")"
declare -a ENVV=()
while IFS= read -r kv; do
  k="${kv%%=*}"; v="${kv#*=}"
  case "$k" in
    FLUXOR_PROJECT_ROOT) v="$ROOT" ;;   # the package installs the tree there
    *) v="$(map "$v")" ;;
  esac
  ENVV+=("$k=$v")
done < <(sed -n 's#^Environment=##p' "$UNIT")
printf '   %s\n' "WorkingDirectory=$WD" "${ENVV[@]}"
VAULT=""
for kv in "${ENVV[@]}"; do [ "${kv%%=*}" = FLUXOR_VAULT_DIR ] && VAULT="${kv#*=}"; done
[ -n "$VAULT" ] || fail "the unit names no FLUXOR_VAULT_DIR: the vault would have no home without FLUXOR_STORE_DIR"
# ExecStartPre: the directories the unit makes.
mkdir -p "$STORE/lattice" "$VAULT"
chmod 0700 "$STORE" "$STORE/lattice"

echo "== 2. an upgraded node: its host store.log holds an object =="
python3 - "$STORE/store.log" <<'PY'
import struct, sys
key = b"/configmaps/default/kept"
val = b'{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"kept","namespace":"default"},"data":{"from":"host store"}}'
with open(sys.argv[1], "ab") as f:
    f.write(struct.pack("<QBHI", 1, 1, len(key), len(val)) + key + val)
PY

echo "== 3. the shipped graph, rendered as the unit renders it, with a test certificate =="
grep -q '^ExecStartPre=/usr/lib/nanocloud/nanocloud-graph.sh /etc/nanocloud.io/fluxor /run/nanocloud/graph.yaml$' "$UNIT" \
  || fail "the unit does not render this node's graph before it runs"
grep -q '^ExecStart=/usr/bin/fluxor run /run/nanocloud/graph.yaml$' "$UNIT" \
  || fail "the unit does not run the graph it rendered"
# This node as postinst provisions it: the control plane, replica 0 of a
# one-voter group, its peer PKI; and a reader for the test.
nc_controlplane_graph "$STORE" "$D/controlplane.yaml" || fail "the control plane did not render"

EXTRA=()   # more environment for a start (step 6: a private host registry)
start() { # start <log>: `fluxor run` as ExecStart, in the unit's environment
  env -C "$STORE" -u FLUXOR_STORE_DIR "${ENVV[@]}" "${EXTRA[@]}" RUST_LOG=warn \
    FLUXOR_SEAL_KEY="$(printf '%064x' 7)" \
    fluxor run "$D/controlplane.yaml" >"$1" 2>&1 &
  RUNTIME_PID=$!
  for _ in $(seq 600); do
    python3 -c "import socket;socket.create_connection(('127.0.0.1',$PORT),timeout=0.2).close()" 2>/dev/null && return 0
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the graph exited before binding :$PORT"
    sleep 0.2
  done
  fail "the graph never bound :$PORT"
}
stop() { kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; }

echo "== 4. first start: the host log is imported into lattice =="
start "$D/run.log"
nc_store_ready "$STORE" || fail "the store never answered"
for _ in $(seq 40); do
  grep -q '\[kvw\] store.log imported' "$D/run.log" && break
  sleep 0.25
done
grep -q '\[kvw\] store.log imported' "$D/run.log" || fail "the host store.log was not imported"
grep -q '\[kvw\] store provider=1' "$D/run.log" || fail "the lattice store is not the provider"
if grep -q 'FLUXOR_STORE_DIR is set' "$D/run.log"; then fail "the host store came up"; fi
echo "   imported through the group's log; lattice serves the storage contracts; no host store"
got="$(nc_store_get "$STORE" /configmaps/default/kept)"
stop
case "$got" in *'"from":"host store"'*) ;; *) fail "the imported object is not in lattice: '$got'" ;; esac
echo "   /configmaps/default/kept is in the lattice store"

echo "== 5. restart: the store is kept, and the log is not imported again =="
start "$D/run2.log"
nc_store_ready "$STORE" || fail "the store never answered after the restart"
for _ in $(seq 40); do
  got="$(nc_store_get "$STORE" /configmaps/default/kept)"
  [ -n "$got" ] && break
  sleep 0.25
done
if grep -q '\[kvw\] store.log imported' "$D/run2.log"; then fail "the host log was imported a second time"; fi
stop
case "$got" in *'"from":"host store"'*) ;; *) fail "the object did not survive the restart: '$got'" ;; esac
[ -f "$STORE/store.log" ] || fail "the host log was removed (it is the record of what was imported)"
echo "   kept across the restart, the host log imported once"

echo "== 6. the instance: a state directory of its own, reclaimed after stop =="
# The unit gives the runtime its state directory (StateDirectory=, named
# by FLUXOR_STATE_DIR): the instance lock lives there and the instance's
# tag is a digest of it, so a restart is the same instance. And once the
# runtime is gone, ExecStopPost collects what it left on the host,
# privileged (`+`), through the runtime the package links into /usr/bin.
SD="$(unit_val StateDirectory)"
SDIR=""
for kv in "${ENVV[@]}"; do [ "${kv%%=*}" = FLUXOR_STATE_DIR ] && SDIR="${kv#*=}"; done
[ -n "$SD" ] || fail "the unit has no StateDirectory="
[ "$SDIR" = "$(map "/var/lib/$SD")" ] || fail "FLUXOR_STATE_DIR ($SDIR) is not the unit's StateDirectory ($SD)"
[ -f "$SDIR/instance.lock" ] || fail "the runtime did not hold its instance lock in $SDIR"
grep -qx 'ExecStopPost=-+/usr/bin/fluxor-linux reclaim' "$UNIT" \
  || fail "the unit does not reclaim its instance, privileged, after stop"
grep -q '"${STAGING_ROOT}/usr/bin/fluxor-linux"' "$ROOT/packaging/debian/build.sh" \
  || fail "the package does not install /usr/bin/fluxor-linux, which ExecStopPost runs"
RT="$(nc_fluxor_runtime "$ROOT")" || fail "no runtime to reclaim with"
reclaim() { # reclaim [privileged]: the unit's ExecStopPost, in its environment
  local pre=()
  # Privileged as `+` runs it, in a throwaway network namespace: the host's
  # own packet-policy tables are never touched.
  [ "${1:-}" = privileged ] && pre=(sudo -n unshare -n)
  "${pre[@]}" env -u FLUXOR_STORE_DIR "${ENVV[@]}" FLUXOR_RUN_DIR="$D/run" RUST_LOG=warn "$RT" reclaim
}
mkdir -p "$D/run"
EXTRA=(FLUXOR_RUN_DIR="$D/run")
start "$D/run3.log"
rc=0; out="$(reclaim 2>&1)" || rc=$?
[ "$rc" = 2 ] || fail "reclaim of a live instance exited $rc, not 2: $out"
echo "   live: $(printf '%s\n' "$out" | grep '^reclaim:')"
stop
# Unprivileged, its packet-policy tables cannot even be listed: the reclaim
# stays pending (exit 1), which is why the unit runs it with `+`.
rc=0; out="$(reclaim 2>&1)" || rc=$?
[ "$rc" = 1 ] || fail "an unprivileged reclaim after stop exited $rc, not 1 (pending): $out"
echo "   stopped, unprivileged: $(printf '%s\n' "$out" | grep '^reclaim:')"
if sudo -n true 2>/dev/null; then
  rc=0; out="$(reclaim privileged 2>&1)" || rc=$?
  [ "$rc" = 0 ] || fail "a privileged reclaim after stop exited $rc, not 0: $out"
  case "$out" in *"reclaimed"*) ;; *) fail "reclaim after stop named no instance: $out" ;; esac
  echo "   stopped, privileged: $(printf '%s\n' "$out" | grep '^reclaim:')"
else
  echo "   (no passwordless sudo: the privileged reclaim is not exercised)"
fi

echo "== E2E green: the installed control plane keeps its store in lattice, and an upgraded node's objects come with it =="
