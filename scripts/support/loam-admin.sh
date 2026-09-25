# Loam's admin plane for an E2E, as its operators run it: the loam-admin
# template from a Loam checkout (packaging/mtls/loam-admin), rendered with
# its values and run under `fluxor run`, and the mesh capabilities its gate
# admits sessions under.
#
# Source after setting nothing; the caller gets:
#   LOAM_ROOT              the checkout ($LOAM_ROOT, default ../loam)
#   nc_loam_present        true when the checkout has the template
#   nc_loam_mesh <dir>     a mesh root's seed at <dir>/root.seed; echoes its key
#   nc_loam_mint <seed> <--scope s|--object hex> <perms> [<not-before> <not-after>]
#                          one chain, by default valid from a minute ago for a day
#   nc_loam_body_plane     the object the body plane's capabilities name
#   nc_loam_node_cap <seed> <root> <out>
#                          a node's capability file: `<root>/` and the body
#                          plane, read and command
#   nc_loam_start <dir> <port> <mesh> <ca> <cert> <key.der> <log>
#                          the node, its state under <dir>; LOAM_PID is its
#                          `fluxor run`; returns once the gate listens
# The server's key is DER (`openssl ec -outform DER`).

LOAM_ROOT="${LOAM_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/../loam}"
LOAM_PID=""

nc_loam_present() {
  [ -f "$LOAM_ROOT/packaging/mtls/loam-admin/linux.yaml" ] && [ -f "$LOAM_ROOT/tools/e2e/render_service.py" ]
}

nc_loam_mesh() {
  head -c 32 /dev/urandom > "$1/root.seed"
  fluxor modules keygen --key "$1/root.seed" 2>/dev/null | tail -1
}

nc_loam_mint() {
  local now
  now="$(date +%s)"
  fluxor modules cap mint --key "$1" "$2" "$3" --perms "$4" \
    --not-before "${5:-$((now - 60))}" --not-after "${6:-$((now + 86400))}" 2>/dev/null | tail -1
}

nc_loam_body_plane() {
  printf 'loam.body-plane\0' | sha256sum | cut -c1-32
}

nc_loam_node_cap() {
  { nc_loam_mint "$1" --scope "$2/" read_state,send_command
    nc_loam_mint "$1" --object "$(nc_loam_body_plane)" read_state,send_command; } > "$3"
  [ "$(grep -c '^fxcap1\.' "$3")" = 2 ]
}

nc_loam_start() {
  local dir="$1" port="$2" mesh="$3" ca="$4" cert="$5" key="$6" log="$7"
  mkdir -p "$dir/bodies"
  python3 -B "$LOAM_ROOT/tools/e2e/render_service.py" "$LOAM_ROOT/packaging/mtls/loam-admin/linux.yaml" \
    "$dir/loam.yaml" port="$port" mesh_roots="$mesh" ns_wal="$dir/ns.wal" obj_wal="$dir/obj.wal" \
    body_dir="$dir/bodies" gc_interval=0 ca="$ca" cert="$cert" key="$key" || return 1
  env -C "$LOAM_ROOT" fluxor run "$dir/loam.yaml" > "$log" 2>&1 < /dev/null &
  LOAM_PID=$!
  local _
  for _ in $(seq 1 300); do
    grep -q "listening on port $port" "$log" 2>/dev/null && return 0
    kill -0 "$LOAM_PID" 2>/dev/null || return 1
    sleep 0.2
  done
  return 1
}
