# Shared by proxy-e2e.sh and netpolicy-e2e.sh: the store-log helpers, a run of
# the graph with net.policy in DRY RUN (the rendered nft script is written to
# $D/np/<table>.nft, and the provider honestly reports ENFORCED clear), and the
# kernel check of that script in a private user+net namespace.
# Expects $D, $ROOT, $GRAPH, $FLUXOR_RUNTIME, and a `fail` function.

np_put() { # np_put <key> <value>   (op 2 when <value> is the literal -DELETE-)
  python3 - "$D/store.log" "$1" "$2" <<'PY'
import struct, sys, os
path, key, val = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode()
op = 2 if val == b"-DELETE-" else 1
if op == 2: val = b""
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, o, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    f.write(struct.pack("<QBHI", last + 1, op, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

np_last() { # np_last <key> — the live value, or empty
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); want = sys.argv[2].encode(); p = 0; out = None
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    if data[p+15:p+15+kl] == want: out = data[p+15+kl:p+15+kl+vl] if op == 1 else None
    p += 15 + kl + vl
sys.stdout.write(out.decode() if out is not None else "")
PY
}

np_keys() { # np_keys <prefix> — live keys under it, one per line
  python3 - "$D/store.log" "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); pre = sys.argv[2].encode(); p = 0; live = {}
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl]
    if k.startswith(pre):
        if op == 1: live[k] = 1
        else: live.pop(k, None)
    p += 15 + kl + vl
for k in sorted(live): print(k.decode())
PY
}

np_build() {
  nc_build_workload "$ROOT" "$GRAPH" "$D/config.bin" "$D/modules.bin" >/dev/null || fail "graph did not build"
}

np_run() { # np_run <seconds>
  mkdir -p "$D/np"
  FLUXOR_NET_POLICY_DRYRUN="$D/np" FLUXOR_STORE_DIR="$D" RUST_LOG=warn timeout "$1" "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >>"$D/run.log" 2>&1 || true
}

np_kernel_check() { # np_kernel_check <table> — the kernel accepts the rendered script
  local f="$D/np/$1.nft" out
  command -v nft >/dev/null && command -v unshare >/dev/null || { echo "   (kernel check skipped: no nft/unshare)"; return 0; }
  if out="$(unshare -Urn nft -f "$f" 2>&1)"; then
    echo "   the kernel accepted the rendered $1 table (private netns)"
  elif printf '%s' "$out" | grep -q "Operation not permitted"; then
    echo "   (kernel check skipped: no user namespaces)"
  else
    fail "the kernel refused the rendered $1 table: $out"
  fi
}
