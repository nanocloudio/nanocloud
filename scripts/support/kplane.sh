# The packaged control plane, up and joined, for an E2E that drives it with
# kubectl. Sourced, after fluxor-env.sh, with ROOT and D set and `fail`
# defined:
#
#   kplane_up             render the shipped control plane (authority trust),
#                         build it, run it on a fresh store under $D, join as
#                         its operator -> $KC (a kubeconfig), $RUNTIME_PID
#   k <args…>             kubectl against it
#   kplane_down           stop the runtime
#   kplane_node <name> [labels-json] [status-json-members]
#                         register a ready Node (as its lease would), and
#                         keep it renewed in the background while the plane
#                         runs (the plane takes a node silent for 30 s out
#                         of service)
#
# The node itself is not here: an E2E that needs a Pod to finish sets its
# phase through the status subresource, as a kubelet does.
# A port of its own, not the packaged 7443: other control planes (other
# E2Es) may be up on this host at the same time.
KPLANE_PORT="${KPLANE_PORT:-$((17000 + $$ % 2000))}"
RUNTIME_PID=""
KPLANE_RENEW=""

kplane_up() {
  STORE="$D/store"
  mkdir -p "$STORE"
  nc_lattice_prepare "$STORE"
  NANOCLOUD_PEER_TRUST=authority nc_controlplane_graph "$STORE" "$D/graph.yaml" \
    || fail "the control plane did not render"
  grep -q '^    port: 7443$' "$D/graph.yaml" || fail "the control plane's API port is not 7443"
  sed -i "s/^    port: 7443\$/    port: $KPLANE_PORT/" "$D/graph.yaml"
  if [ -n "${KPLANE_EDIT:-}" ]; then "$KPLANE_EDIT" "$D/graph.yaml"; fi
  nc_build_workload "$ROOT" "$D/graph.yaml" "$D/config.bin" "$D/modules.bin" >/dev/null \
    || fail "graph did not build"
  local seal
  seal="$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
  mkdir -p "$D/vault"
  env -C "$STORE" FLUXOR_SEAL_KEY="$seal" FLUXOR_VAULT_DIR="$D/vault" RUST_LOG=warn "$FLUXOR_RUNTIME" \
    --config "$D/config.bin" --modules "$D/modules.bin" >"$D/run.log" 2>&1 &
  RUNTIME_PID=$!
  local join="" secret ca
  for _ in $(seq 240); do
    join="$(grep -ao '\[join\] secret=[0-9a-f]\{32\} ca-sha256=[0-9a-f]\{64\}' "$D/run.log" | tail -1 || true)"
    [ -n "$join" ] && break
    kill -0 "$RUNTIME_PID" 2>/dev/null || fail "the runtime exited"
    sleep 0.25
  done
  [ -n "$join" ] || fail "no join secret announced in 60 s"
  secret="$(sed 's/.*secret=\([0-9a-f]*\).*/\1/' <<<"$join")"
  ca="$(sed 's/.*ca-sha256=\([0-9a-f]*\).*/\1/' <<<"$join")"
  KC="$D/kubeconfig"
  bash "$ROOT/scripts/rig-join.sh" "$secret" "$ca" "127.0.0.1:$KPLANE_PORT" "$KC" >/dev/null \
    || fail "the join did not complete"
  k get --raw /version >/dev/null || fail "kubectl could not reach the API"
}

k() { kubectl --kubeconfig "$KC" --request-timeout=20s "$@"; }

kplane_down() {
  if [ -n "$KPLANE_RENEW" ]; then kill "$KPLANE_RENEW" 2>/dev/null || true; wait "$KPLANE_RENEW" 2>/dev/null || true; KPLANE_RENEW=""; fi
  if [ -n "$RUNTIME_PID" ]; then kill "$RUNTIME_PID" 2>/dev/null || true; wait "$RUNTIME_PID" 2>/dev/null || true; RUNTIME_PID=""; fi
}

KPLANE_NODES=()
kplane_node() { # kplane_node <name> [labels-json] [extra status members, JSON]
  local name="$1" labels="${2:-}" extra="${3:-}"
  [ -n "$labels" ] || labels='{}'
  [ -n "$extra" ] && extra=",$extra"
  printf '%s' "{\"apiVersion\":\"v1\",\"kind\":\"Node\",\"metadata\":{\"name\":\"$name\",\"labels\":$labels},\"status\":{\"ready\":true,\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\"}]$extra}}" \
    >"$D/node-$name.json"
  k apply -f "$D/node-$name.json" >/dev/null || fail "node $name not registered"
  KPLANE_NODES+=("$name")
  if [ -z "$KPLANE_RENEW" ]; then
    ( while sleep 8; do
        for f in "$D"/node-*.json; do
          kubectl --kubeconfig "$KC" --request-timeout=10s annotate --overwrite -f "$f" \
            "nanocloud.io/renewed=$(date +%s%N)" >/dev/null 2>&1 || true
        done
      done ) &
    KPLANE_RENEW=$!
  fi
}

# kplane_pod_phase <pod> <phase> [ns]: the Pod's phase set through the status
# subresource, as its node's report would.
kplane_pod_phase() {
  k -n "${3:-default}" patch pod "$1" --subresource=status --type=merge \
    -p "{\"status\":{\"phase\":\"$2\"}}" >/dev/null || fail "could not set $1 $2"
}

# kplane_until <seconds> <description> <command…>: retry until it holds.
kplane_until() {
  local secs="$1" what="$2"; shift 2
  local i
  for i in $(seq $((secs * 2))); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  fail "$what (not within ${secs}s)"
}
