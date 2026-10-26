#!/usr/bin/env bash
# RPKI fetching E2E: `rpki_fetch` (scripts/support/graphs/wh-rpki.yaml) over
# RRDP from a repository served on loopback, inside an unprivileged user +
# network namespace of its own. The repository publishes the RPKI fixtures
# (tests/harness/fixtures/rpki: a good ROA, one under an `inherit` EE, and
# three that must be refused — outside the EE's resources, a max length
# below the prefix, another anchor's EE). Proves: the snapshot is taken and
# every valid ROA's payloads land in /wh-rpki/vrps, the bad ones counted in
# /wh-rpki/status; a new serial whose snapshot does not hash to the
# notification's hash is refused and the rows stay as they were.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -z "${NC_RPKI_INNER:-}" ]; then
  exec unshare -Urn env NC_RPKI_INNER=1 bash "$0" "$@"
fi
ip link set lo up
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi
[ -e "$ROOT/target/fluxor/aarch64/modules/rpki_fetch.fmod" ] || { echo "FAIL: missing rpki_fetch.fmod"; exit 1; }
D="$(mktemp -d /tmp/nc-rpki-e2e-XXXXXX)"
SRV=""
cleanup() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$D"; }
trap cleanup EXIT
fail() { echo "FAIL: $1"; tail -15 "$D/run.log" 2>/dev/null || true; exit 1; }
. "$ROOT/scripts/support/np.sh"
: >"$D/store.log"
mkdir -p "$D/www"
F="$ROOT/tests/harness/fixtures/rpki"
# publish <serial> <tamper>: the repository's notification and snapshot.
publish() {
  python3 - "$F" "$D/www" "$1" "$2" <<'PY'
import base64, hashlib, sys
f, www, serial, tamper = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1"
sid = "9df4b597-af9e-4dca-bdda-719cce2c4e28"
pubs = "".join(
    '<publish uri="rsync://repo.example/ca/%s">%s</publish>\n'
    % (n, base64.b64encode(open(f"{f}/{n}", "rb").read()).decode())
    for n in ("good.roa", "inherit.roa", "outside.roa", "badmax.roa", "rogue.roa"))
snap = ('<snapshot xmlns="http://www.ripe.net/rpki/rrdp" version="1" session_id="%s" serial="%s">\n%s</snapshot>\n'
        % (sid, serial, pubs)).encode()
h = hashlib.sha256(snap).hexdigest()
if tamper:
    snap = snap.replace(b"</snapshot>", b"<!-- -->\n</snapshot>")
open(f"{www}/snapshot-{serial}.xml", "wb").write(snap)
note = ('<notification xmlns="http://www.ripe.net/rpki/rrdp" version="1" session_id="%s" serial="%s">\n'
        '<snapshot uri="http://127.0.0.1:8099/snapshot-%s.xml" hash="%s"/>\n</notification>\n'
        % (sid, serial, serial, h))
open(f"{www}/notification.xml", "w").write(note)
PY
}
publish 1 0
(cd "$D/www" && exec python3 -m http.server 8099 --bind 127.0.0.1 >"$D/http.log" 2>&1) &
SRV=$!
np_put /wh-rpki/anchor "$(base64 -w0 "$F/ta.der")"
nc_build_workload "$ROOT" "$ROOT/scripts/support/graphs/wh-rpki.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null \
  || fail "the graph did not build"
run() { FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$1" "$FLUXOR_RUNTIME" \
  --config "$D/config.bin" --modules "$D/modules.bin" </dev/null >>"$D/run.log" 2>&1 || true; }
sleep 0.5
echo "== 1. serial 1: the snapshot taken, valid ROAs' payloads stored, bad ones refused =="
run 8
ST="$(np_last /wh-rpki/status)"
case "$ST" in "session=9df4b597-af9e-4dca-bdda-719cce2c4e28;serial=1;roas=5;refused=3;vrps=6") ;; *) fail "status: '$ST'" ;; esac
V="$(np_last /wh-rpki/vrps)"
for want in "192.0.2.0/24 24 AS64500" "192.0.2.128/25 28 AS64500" "2001:0db8:0000:0000:0000:0000:0000:0000/32 48 AS64500"; do
  echo "$V" | grep -qx "$want" || fail "no payload '$want' in: $V"
done
echo "   $ST"
echo "== 2. serial 2 with a snapshot that does not match its hash: refused, the rows kept =="
publish 2 1
run 5
[ "$(np_last /wh-rpki/status)" = "$ST" ] || fail "a tampered snapshot changed the status"
[ "$(np_last /wh-rpki/vrps)" = "$V" ] || fail "a tampered snapshot changed the payloads"
grep -q "\[rpki_fetch\]" "$D/run.log" || true
echo "   refused; /wh-rpki/vrps and /wh-rpki/status unchanged"
echo "PASS"
