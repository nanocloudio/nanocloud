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
