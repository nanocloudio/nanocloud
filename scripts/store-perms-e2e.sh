#!/usr/bin/env bash
# The control-plane store is readable by root only.
#
# It holds Secrets, minted ServiceAccount tokens (the lattice store, in its
# lattice/) and, under vault/, the sealed keys; files are written 0644, so
# the DIRECTORY is the boundary.
# This runs nanocloud.service's own ExecStartPre lines (the store path swapped
# for a temp one) over a fresh install and over an upgrade from a 0755 store
# holding a 0644 file, and requires 0700 afterwards both times.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT="$ROOT/packaging/debian/nanocloud.service"
REAL=/var/lib/nanocloud.io/control-plane/store
fail() { echo "FAIL: $1"; exit 1; }

D="$(mktemp -d /tmp/nc-perms-XXXXXX)"
trap 'rm -rf "$D"' EXIT

pre() { # run every ExecStartPre with the store path redirected to $1
  grep '^ExecStartPre=' "$UNIT" | sed 's/^ExecStartPre=-\{0,1\}//' | while read -r cmd; do
    case "$cmd" in *"$REAL"*) ;; *) continue ;; esac
    eval "${cmd//$REAL/$1}"
  done
}
mode() { stat -c '%a' "$1"; }

echo "== 1. the unit names the store path it hardens =="
grep -q "^ExecStartPre=.*install -d -m 0700 $REAL\$" "$UNIT" || fail "no 0700 install -d for $REAL"
# The store is lattice's, in the directory the graph runs in (its lattice/),
# and the vault keeps its sealed keys under it: both inside the boundary.
grep -q "^WorkingDirectory=$REAL\$" "$UNIT" || fail "the graph does not run in the hardened path (its store would not be inside it)"
grep -q "^Environment=FLUXOR_VAULT_DIR=$REAL/" "$UNIT" || fail "the vault is not under the hardened path"

echo "== 2. fresh install =="
pre "$D/fresh/store"
[ "$(mode "$D/fresh/store")" = 700 ] || fail "fresh store is $(mode "$D/fresh/store"), want 700"
[ "$(mode "$D/fresh/store/lattice")" = 700 ] || fail "fresh lattice store is $(mode "$D/fresh/store/lattice"), want 700"
echo "   store: 700, lattice/: 700"

echo "== 3. upgrade from a world-readable store =="
mkdir -p "$D/old/store" && chmod 0755 "$D/old/store"
echo secret >"$D/old/store/store.log" && chmod 0644 "$D/old/store/store.log"
pre "$D/old/store"
[ "$(mode "$D/old/store")" = 700 ] || fail "upgraded store is $(mode "$D/old/store"), want 700"
echo "   store: 700 (its 0644 log is unreachable to others)"

echo "== E2E green: the store, its tokens and its sealed keys are root-only =="
