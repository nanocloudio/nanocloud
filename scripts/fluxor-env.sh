# Shared fluxor environment for the nanocloud e2e scripts. Sourced, not run.
#
# Two jobs, both anchoring the e2es to the production delivery path:
#
#  1. Runtime resolution — `fluxor sync` materialises the fluxor-linux
#     runtime into the CONSUMING project's tree at
#     <project>/target/<host-triple>/release/fluxor-linux
#     (standards/fluxor-modules.md). That synced path is the only
#     runtime the e2es use; nothing here reads ~/.fluxor/registry.
#
#  2. Workload assembly — `fluxor build <app.fluxor.toml>` emits the
#     workload bundle (workload.json + per-target graph.yaml,
#     resources.json, config.bin, modules.bin) under
#     <project>/target/fluxor/<name>/. That flat triple is exactly what
#     `fluxor publish bundle` publishes and `fluxor agent commit
#     --bundle` commits, and its config.bin/modules.bin are what the
#     runtime boots. The e2es boot the runtime off the bundle's blobs,
#     so the artifact set under test is the one production ships — not
#     a hand-assembled pair of low-level blob commands.

NC_HOST_TRIPLE="${NC_HOST_TRIPLE:-aarch64-unknown-linux-gnu}"

# nc_fluxor_runtime <project-root>
#   Print the synced runtime path, running `fluxor sync` once if it is
#   not materialised yet. Fails (non-zero, message on stderr) if sync
#   cannot produce it.
nc_fluxor_runtime() {
  local root="$1"
  local rt="$root/target/$NC_HOST_TRIPLE/release/fluxor-linux"
  if [ ! -e "$rt" ]; then
    (cd "$root" && fluxor sync >/dev/null 2>&1) || true
  fi
  if [ ! -e "$rt" ]; then
    echo "FAIL: fluxor-linux not materialised at $rt (run 'fluxor sync' in $root)" >&2
    return 1
  fi
  printf '%s\n' "$rt"
}

# nc_build_workload <project-root> <graph.yaml> <config.bin-out> <modules.bin-out>
#   Assemble the graph's runnable blobs through the production bundle
#   path: write a minimal workload source manifest referencing the
#   graph, run `fluxor build` on it (which emits the committed bundle —
#   workload.json/resources.json/graph.yaml — and builds
#   config.bin/modules.bin per target through the same cores), then
#   stage the two blobs where the caller wants them. The throwaway
#   bundle directory is removed afterwards so parallel e2e runs never
#   collide under target/fluxor/.
nc_build_workload() {
  local root="$1" graph="$2" config_out="$3" modules_out="$4"
  local stem base mdir name
  base="$(basename "$graph")"
  stem="${base%.*}"
  name="e2e-${stem}-$$"
  mdir="$(mktemp -d "${TMPDIR:-/tmp}/nc-workload-XXXXXX")"
  # Absolute graph path: the manifest may live outside the tree.
  case "$graph" in
    /*) ;;
    *) graph="$(cd "$(dirname "$graph")" && pwd)/$base" ;;
  esac
  cat >"$mdir/app.fluxor.toml" <<EOF
[workload]
name = "$name"
version = "0.0.0"

[[implementation]]
target = "linux"
graph = "$graph"
EOF
  if ! FLUXOR_PROJECT_ROOT="$root" fluxor build "$mdir/app.fluxor.toml" \
      >/dev/null 2>"$mdir/build.err"; then
    cat "$mdir/build.err" >&2
    rm -rf "$mdir"
    echo "FAIL: fluxor build (workload bundle) failed for $graph" >&2
    return 1
  fi
  rm -rf "$mdir"
  local tdir="$root/target/fluxor/$name/linux"
  cp "$tdir/config.bin" "$config_out" && cp "$tdir/modules.bin" "$modules_out"
  local rc=$?
  rm -rf "$root/target/fluxor/$name"
  return $rc
}

# ── The lattice store ─────────────────────────────────────────────────
#
# The packaged control-plane graphs keep their store in lattice
# (`kv_state_worker_store`): the runtime runs IN the state directory
# (`store_dir: lattice`, `import_log: store.log`, both relative), with NO
# FLUXOR_STORE_DIR — a lattice store that refused to start would otherwise
# be replaced, silently, by the host store. Seeding stays what it was
# (append to `$D/store.log`: the store imports what was appended at its
# next start); a single store is read back through `lattice-store-dump`,
# the lattice store's own reader, a replicated one live (below).

# nc_lattice_prepare <dir>
#   Make <dir> a state directory the lattice store can open.
nc_lattice_prepare() {
  mkdir -p "$1/lattice"
}

# nc_lattice_dump_bin
#   Print the path of `lattice-store-dump`, building it from the lattice
#   checkout in the fluxor workspace (LATTICE_ROOT overrides) if needed.
nc_lattice_dump_bin() {
  local lroot="${LATTICE_ROOT:-}"
  if [ -z "$lroot" ]; then
    lroot="$(sed -n 's/^ *"\(.*\/lattice\)",$/\1/p' "$HOME/.fluxor/workspace.toml" 2>/dev/null | head -1)"
  fi
  local manifest="$lroot/tools/store_dump/Cargo.toml"
  local bin="$lroot/tools/store_dump/target/release/lattice-store-dump"
  if [ ! -f "$manifest" ]; then
    echo "FAIL: no lattice checkout with tools/store_dump (set LATTICE_ROOT)" >&2
    return 1
  fi
  if [ ! -x "$bin" ] || [ -n "$(find "$lroot/tools/store_dump/src" "$lroot/modules/common" -newer "$bin" -print -quit 2>/dev/null)" ]; then
    cargo build -q --release --manifest-path "$manifest" >&2 || return 1
  fi
  printf '%s\n' "$bin"
}

# ── The control plane's replicated store ─────────────────────────────
#
# fluxor-controlplane.yaml is a template: its store is one replica of a
# replicated group (clustor's consensus and durability under lattice's
# store), rendered per node by packaging/debian/nanocloud-graph.sh. Its
# writes live in the group's log until the store flushes them, so it is
# read live: a test's rendering adds lattice's redis edge as its reader
# (`<dir>/read.port`), and nc_store_get / nc_store_keys read through it
# while the runtime runs.

# nc_node_pki <dir>
#   A node's peer PKI in <dir>/pki (its peer key, itself as replica 0, an
#   operator key among the anchors) and the API's certificate
#   (<dir>/server.der), as postinst provisions them.
nc_node_pki() {
  local dir="$1" pki="$1/pki" n
  mkdir -p "$pki/peers" "$pki/operators"
  for n in peer operator; do
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
      -keyout "$pki/$n.key.pem" -out "$pki/$n.pem" -days 1 -subj "/CN=nc-$n" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=digitalSignature" \
      -addext "extendedKeyUsage=serverAuth,clientAuth" 2>/dev/null || return 1
  done
  openssl x509 -in "$pki/peer.pem" -outform DER -out "$pki/peer.der" || return 1
  openssl ec -in "$pki/peer.key.pem" -outform DER -out "$pki/peer.key.der" 2>/dev/null || return 1
  cp "$pki/peer.pem" "$pki/peers/0.pem"
  cp "$pki/operator.pem" "$pki/operators/test.pem"
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
    -keyout "$dir/server.key.pem" -out "$dir/server.pem" -days 1 -subj "/CN=nanocloud" 2>/dev/null \
    && openssl x509 -in "$dir/server.pem" -outform DER -out "$dir/server.der" \
    && openssl ec -in "$dir/server.key.pem" -outform DER -out "$dir/server.key.der" 2>/dev/null
}

# nc_render_graph <dir> <template> <out.yaml>
#   Render <template> as replica 0 of a one-voter group, with the PKI
#   nc_node_pki made in <dir>, and the API's certificate paths at <dir>.
nc_render_graph() {
  local dir="$1" template="$2" out="$3" root base
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  base=$((23000 + ($$ % 500) * 2))
  NANOCLOUD_CLUSTER_ENV=/nonexistent NANOCLOUD_PKI="$dir/pki" NANOCLOUD_NODE_NAME=node-a \
    NANOCLOUD_PEER_PORT="$base" NANOCLOUD_PEERS="0=127.0.0.1:$base" \
    NANOCLOUD_REPLICA_FRAGMENT="$root/packaging/debian/fluxor-replica.yaml" \
    sh "$root/packaging/debian/nanocloud-graph.sh" "$template" "$out.rendered" || return 1
  sed -e "s#/etc/nanocloud.io/certs/server.der#$dir/server.der#" \
      -e "s#/etc/nanocloud.io/certs/server.key.der#$dir/server.key.der#" \
      "$out.rendered" >"$out"
}

# nc_controlplane_graph <dir> <out.yaml>
#   The control plane for a one-voter group in <dir> (nc_node_pki, then
#   rendered), with lattice's redis edge as the test's reader.
nc_controlplane_graph() {
  local dir="$1" out="$2" root port
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  port=$((23000 + ($$ % 500) * 2 + 1))
  [ -f "$dir/pki/peer.der" ] || nc_node_pki "$dir" || return 1
  nc_render_graph "$dir" "$root/packaging/debian/fluxor-controlplane.yaml" "$dir/cp.rendered.yaml" || return 1
  python3 - "$dir/cp.rendered.yaml" "$out" "$port" <<'PY' || return 1
import sys
src, out, port = sys.argv[1:]
mods, wiring = open(src).read().split("\nwiring:\n", 1)
reader = (
    "  # The test's reader: lattice's redis edge, a GET answered by this\n"
    "  # replica's store.\n"
    "  - name: redis_edge_anchor\n    params:\n      listen_port: " + port + "\n")
edges = (
    "  - from: linux_net.net_out\n    to: redis_edge_anchor.net_in\n    buffer_bytes: 65536\n"
    "  - from: redis_edge_anchor.net_out\n    to: linux_net.net_in\n"
    "  - from: redis_edge_anchor.kv_out\n    to: kv_request_router.redis_in\n"
    "  - from: kv_request_router.redis_out\n    to: redis_edge_anchor.kv_in\n")
open(out, "w").write(mods + "\n" + reader + "\nwiring:\n" + wiring.rstrip("\n") + "\n" + edges)
PY
  echo "$port" >"$dir/read.port"
}

# nc_cli_graph <dir> <out.yaml>
#   The CLI applet as the node in <dir>'s replica (nc_node_pki, then the
#   packaged template rendered): run it with `env -C <dir>`, the replica's
#   service stopped.
nc_cli_graph() {
  local dir="$1" out="$2" root
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  [ -f "$dir/pki/peer.der" ] || nc_node_pki "$dir" || return 1
  mkdir -p "$dir/lattice"
  nc_render_graph "$dir" "$root/packaging/cli/linux.yaml" "$out"
}

# nc_replica_get <dir> <key>
#   The value the replica in <dir> holds at <key> (empty: none), read while
#   nothing else holds it: a reader graph runs as that replica — the
#   store's fragment and lattice's redis edge, nothing that writes — for as
#   long as one GET takes. The GET is linearizable (a ReadIndex fence): it
#   answers once the replica has applied everything its log committed. Built once per <dir> against <project-root>
#   (NC_BUILD_ROOT, default this checkout).
nc_replica_get() {
  local dir="$1" key="$2" root port pid rc=0
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  port=$((23000 + ($$ % 500) * 2 + 1))
  if [ ! -f "$dir/reader.bin" ]; then
    [ -f "$dir/pki/peer.der" ] || nc_node_pki "$dir" || return 1
    cat >"$dir/reader-template.yaml" <<YAML
target: linux
tick_us: 1000
scheduler:
  accept_cycles: true
platform:
  net: {}
modules:
__REPLICA_MODULES__
  - name: redis_edge_anchor
    params:
      listen_port: $port
  - name: kv_request_router
    params:
      replicated: 1
      lin_reads: 1
wiring:
__REPLICA_WIRING__
  - from: linux_net.net_out
    to: redis_edge_anchor.net_in
    buffer_bytes: 65536
  - from: redis_edge_anchor.net_out
    to: linux_net.net_in
  - from: redis_edge_anchor.kv_out
    to: kv_request_router.redis_in
  - from: kv_request_router.redis_out
    to: redis_edge_anchor.kv_in
  - from: kv_request_router.kv_out
    to: kv_state_worker_store.commands
  - from: kv_state_worker_store.responses
    to: kv_request_router.kv_in
  - from: kv_request_router.proposal_out
    to: gateway.client_requests
  - from: gateway.client_responses
    to: kv_request_router.gateway_reject
  - from: kv_request_router.lin_read_out
    to: consensus.read
  - from: consensus.applied
    to: kv_request_router.read_release
YAML
    nc_render_graph "$dir" "$dir/reader-template.yaml" "$dir/reader.yaml" || return 1
    nc_build_workload "${NC_BUILD_ROOT:-$root}" "$dir/reader.yaml" "$dir/reader.bin" "$dir/reader.mods" \
      >/dev/null || return 1
  fi
  mkdir -p "$dir/lattice"
  env -C "$dir" RUST_LOG=warn "${FLUXOR_RUNTIME:-$(nc_fluxor_runtime "$root")}" \
    --config "$dir/reader.bin" --modules "$dir/reader.mods" >>"$dir/reader.log" 2>&1 &
  pid=$!
  local end=$((SECONDS + 30))
  until [ "$(redis-cli -p "$port" PING 2>/dev/null)" = PONG ]; do
    [ "$SECONDS" -lt "$end" ] || { rc=1; break; }
    sleep 0.2
  done
  # The fence needs the group's authority (leadership, the control plane's
  # proof): until then the read answers CLUSTERDOWN, and is asked again.
  local v=""
  while [ "$rc" = 0 ]; do
    v="$(redis-cli -p "$port" --raw GET "$key" 2>/dev/null || true)"
    case "$v" in CLUSTERDOWN*) ;; *) break ;; esac
    [ "$SECONDS" -lt "$end" ] || { rc=1; break; }
    sleep 0.2
  done
  [ "$rc" = 0 ] && printf '%s\n' "$v"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return "$rc"
}

# nc_store_ready <dir> [seconds]
#   Wait until the store in <dir> answers its reader.
nc_store_ready() {
  local port end=$((SECONDS + ${2:-30}))
  port="$(cat "$1/read.port")"
  until [ "$(redis-cli -p "$port" PING 2>/dev/null)" = PONG ]; do
    [ "$SECONDS" -lt "$end" ] || return 1
    sleep 0.2
  done
}

# nc_store_get <dir> <key>
#   The value the lattice store in <dir> holds at <key> (empty: none).
nc_store_get() {
  if [ -f "$1/read.port" ]; then
    redis-cli -p "$(cat "$1/read.port")" --raw GET "$2" 2>/dev/null || true
    return 0
  fi
  local bin
  bin="$(nc_lattice_dump_bin)" || return 1
  "$bin" "$1/lattice" --prefix "$2" | python3 -c '
import sys
key = sys.argv[1].encode()
for line in sys.stdin.buffer.read().split(b"\n"):
    k, sep, v = line.partition(b"\t")
    if sep and k == key:
        sys.stdout.buffer.write(v)
        break
' "$2"
}

# nc_store_keys <dir> [prefix]
#   Every live key in the lattice store in <dir>, one per line.
nc_store_keys() {
  if [ -f "$1/read.port" ]; then
    # lattice answers KEYS as one SCAN of everything, `[cursor, keys]`: the
    # cursor dropped, the prefix applied here.
    redis-cli -p "$(cat "$1/read.port")" --raw KEYS "*" 2>/dev/null | sed 1d \
      | awk -v p="${2:-}" 'index($0, p) == 1' | sort
    return 0
  fi
  local bin
  bin="$(nc_lattice_dump_bin)" || return 1
  "$bin" "$1/lattice" --keys --prefix "${2:-}"
}
