#!/usr/bin/env bash
# Live E2E for the interactive-terminal path of the nanocloud CLI applet. Runs a
# CLI command under a real pseudo-terminal and proves the fluxor cli_io terminal
# handling:
#   - a live-TTY stdin (which never sends EOF) still completes — cli_in stops
#     pumping once the applet latches its exit code (the CLI_EXIT_LATCHED fix);
#   - stdin is put into interactive mode (ICANON + ECHO off) so `exec -it` gets
#     char-at-a-time input with no double echo — but OUTPUT is untouched (OPOST
#     on, so `\n`→`\r\n` still works for normal commands);
#   - the original terminal settings are RESTORED on exit (no broken terminal).
#
# Needs the fluxor-linux runtime from the synced tree (the cli_io raw-mode +
# completion fix live there).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES_DIR="$ROOT/target/fluxor/bcm2712/modules"
GRAPH="$ROOT/packaging/cli/linux.yaml"
. "$ROOT/scripts/fluxor-env.sh"
if [ -z "${FLUXOR_RUNTIME:-}" ]; then
  FLUXOR_RUNTIME="$(nc_fluxor_runtime "$ROOT")"
fi

command -v fluxor >/dev/null || { echo "FAIL: fluxor CLI not on PATH"; exit 1; }
for f in "$FLUXOR_RUNTIME" "$MODULES_DIR/nanocloud_cli.fmod" "$GRAPH"; do
  [ -e "$f" ] || { echo "FAIL: missing $f (build the fluxor-linux release + modules)"; exit 1; }
done

D="$(mktemp -d /tmp/nc-tty-e2e-XXXXXX)"
trap 'rm -rf "$D"' EXIT

echo "== 1. build config + module table =="
nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin"

echo "== 2. run 'nanocloud help' under a pseudo-terminal =="
RESULT="$(timeout 20 python3 - "$FLUXOR_RUNTIME" "$D/config.bin" "$D/modules.bin" <<'PY'
import os, sys, pty, termios, select
rt, cfg, mods = sys.argv[1:4]
mfd, sfd = pty.openpty()
# start from a normal canonical + echo terminal.
a = termios.tcgetattr(sfd); a[3] |= (termios.ICANON | termios.ECHO); termios.tcsetattr(sfd, termios.TCSANOW, a)
pid = os.fork()
if pid == 0:
    os.setsid(); os.dup2(sfd, 0); os.dup2(sfd, 1); os.dup2(sfd, 2)
    os.environ["FLUXOR_STORE_DIR"] = os.path.dirname(cfg); os.environ["RUST_LOG"] = "error"
    os.execv(rt, [rt, "--config", cfg, "--modules", mods, "--", "help"])
os.close(sfd)
out = b""
while True:
    try:
        r, _, _ = select.select([mfd], [], [], 5)
        if not r:
            break
        d = os.read(mfd, 4096)
        if not d:
            break
        out += d
    except OSError:
        break
_, status = os.waitpid(pid, 0)
after = termios.tcgetattr(mfd)
text = out.decode(errors="replace")
exited   = os.WIFEXITED(status)
has_help = "commands:" in text
opost    = "\r\n" in text
restored = bool(after[3] & termios.ICANON) and bool(after[3] & termios.ECHO)
print("exited" if exited else "HUNG",
      "help" if has_help else "NOHELP",
      "opost" if opost else "NOOPOST",
      "restored" if restored else "NOTRESTORED")
PY
)"
echo "   $RESULT"
case "$RESULT" in
  "exited help opost restored") : ;;
  *HUNG*)        echo "FAIL: runtime hung on a live-TTY stdin (completion fix missing)"; exit 1 ;;
  *NOOPOST*)     echo "FAIL: output mangled (raw mode disabled OPOST)"; exit 1 ;;
  *NOTRESTORED*) echo "FAIL: terminal left in raw mode (restore missing)"; exit 1 ;;
  *)             echo "FAIL: $RESULT"; exit 1 ;;
esac

echo "== E2E green: interactive-terminal CLI — completes on a live TTY, output intact, terminal restored =="
