# RBAC fixtures for the E2Es — sourced, not run.
#
# Real rbac.authorization.k8s.io/v1 objects, written straight into a store log
# (the same wire the store writes), and a settle step that runs a graph until
# the grant index (rbac_*.uproc) holds — or no longer holds — given keys.
#
#   rbac_clusterrole <log> <name> <verbs,…> <resources,…> [resourceNames,…]
#   rbac_role        <log> <ns> <name> <verbs,…> <resources,…>
#   rbac_crb         <log> <name> <clusterrole> <ServiceAccount|User|Group> <subject> [subject-ns]
#   rbac_rb          <log> <ns> <name> <Role|ClusterRole> <role> <kind> <subject> [subject-ns]
#   rbac_delete      <log> <key>
#   rbac_settle      <runtime> <config> <modules> <store-dir> <present|absent> <key>…
#
# A grant key is /rbac-g/<identity>/<scope>|<verb>|<resource>, `*` kept
# literally (a ClusterRoleBinding's scope is `*`).

_rbac_append() { # _rbac_append <log> <op> <key> <value>
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import struct, sys, os
path, op, key, val = sys.argv[1], int(sys.argv[2]), sys.argv[3].encode(), sys.argv[4].encode()
fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
with os.fdopen(fd, "r+b") as f:
    data = f.read(); p = last = 0
    while p + 15 <= len(data):
        rev, o, kl, vl = struct.unpack("<QBHI", data[p:p+15])
        if p + 15 + kl + vl > len(data): break
        last, p = rev, p + 15 + kl + vl
    f.truncate(p); f.seek(0, 2)
    if op == 2: val = b""
    f.write(struct.pack("<QBHI", last + 1, op, len(key), len(val)) + key + val)
    f.flush(); os.fsync(f.fileno())
PY
}

_rbac_json() { python3 -c "import json,sys;print(json.dumps(eval(sys.argv[1]),separators=(',',':')))" "$1"; }
_rbac_list() { python3 -c "import sys;print([x for x in sys.argv[1].split(',') if x])" "$1"; }

rbac_clusterrole() {
  local rule="{'verbs':$(_rbac_list "$3"),'resources':$(_rbac_list "$4"),'apiGroups':['*']"
  [ -n "${5:-}" ] && rule="$rule,'resourceNames':$(_rbac_list "$5")"
  _rbac_append "$1" 1 "/clusterroles/$2" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':'$2'},'rules':[$rule}]}")"
}

# A ClusterRole whose one rule names its apiGroups:
#   rbac_clusterrole_g <log> <name> <groups> <verbs> <resources>
rbac_clusterrole_g() {
  _rbac_append "$1" 1 "/clusterroles/$2" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':'$2'},'rules':[{'verbs':$(_rbac_list "$4"),'resources':$(_rbac_list "$5"),'apiGroups':$(_rbac_list "$3")}]}")"
}

# A ClusterRole granting non-resource URLs:
#   rbac_clusterrole_urls <log> <name> <verbs> <urls>
rbac_clusterrole_urls() {
  _rbac_append "$1" 1 "/clusterroles/$2" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':'$2'},'rules':[{'verbs':$(_rbac_list "$3"),'nonResourceURLs':$(_rbac_list "$4")}]}")"
}

# A ClusterRole carrying labels (as JSON object body, e.g. "'k':'v'"):
#   rbac_clusterrole_l <log> <name> <labels> <verbs> <resources>
rbac_clusterrole_l() {
  _rbac_append "$1" 1 "/clusterroles/$2" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':'$2','labels':{$3}},'rules':[{'verbs':$(_rbac_list "$4"),'resources':$(_rbac_list "$5"),'apiGroups':['*']}]}")"
}

# An aggregating ClusterRole (no rules of its own), optionally labelled:
#   rbac_clusterrole_agg <log> <name> <selectors JSON list body> [labels]
rbac_clusterrole_agg() {
  _rbac_append "$1" 1 "/clusterroles/$2" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':'$2','labels':{${4:-}}},'aggregationRule':{'clusterRoleSelectors':[$3]},'rules':[]}")"
}

rbac_role() {
  _rbac_append "$1" 1 "/roles/$2/$3" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':{'name':'$3','namespace':'$2'},'rules':[{'verbs':$(_rbac_list "$4"),'resources':$(_rbac_list "$5"),'apiGroups':['']}]}")"
}

_rbac_subject() { # <kind> <name> [ns]
  if [ -n "${3:-}" ]; then echo "{'kind':'$1','name':'$2','namespace':'$3'}"; else echo "{'kind':'$1','name':'$2'}"; fi
}

rbac_crb() {
  _rbac_append "$1" 1 "/clusterrolebindings/$2" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':{'name':'$2'},'subjects':[$(_rbac_subject "$4" "$5" "${6:-}")],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':'$3'}}")"
}

rbac_rb() {
  _rbac_append "$1" 1 "/rolebindings/$2/$3" "$(_rbac_json "{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':{'name':'$3','namespace':'$2'},'subjects':[$(_rbac_subject "$6" "$7" "${8:-}")],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'$4','name':'$5'}}")"
}

rbac_delete() { _rbac_append "$1" 2 "$2" ""; }

# The live keys of a store log (puts minus deletes), one per line.
rbac_keys() {
  python3 - "$1" <<'PY'
import struct, sys
data = open(sys.argv[1], "rb").read(); p = 0; live = {}
while p + 15 <= len(data):
    rev, op, kl, vl = struct.unpack("<QBHI", data[p:p+15])
    if p + 15 + kl + vl > len(data): break
    k = data[p+15:p+15+kl].decode(errors="replace")
    if op == 1: live[k] = 1
    else: live.pop(k, None)
    p += 15 + kl + vl
print("\n".join(sorted(live)))
PY
}

# The live keys under /rbac of the store in <dir>: the lattice store's when
# the directory holds one (a graph with `kv_state_worker_store`, prepared
# by `nc_lattice_prepare`), else the host store log's.
_rbac_live_keys() {
  if [ -d "$1/lattice" ]; then
    nc_store_keys "$1" /rbac || true
  else
    rbac_keys "$1/store.log" | grep '^/rbac' || true
  fi
}

# rbac_settle <runtime> <config> <modules> <dir> <present|absent> <key>…
#   Run the graph over the store in <dir> (the seeded store.log is the host
#   store's, or — a lattice graph — imported by lattice's) until the RBAC
#   index holds (or no longer holds) every key.
rbac_settle() {
  local rt="$1" cfg="$2" mods="$3" dir="$4" want="$5"; shift 5
  if [ -d "$dir/lattice" ]; then
    # The lattice store: run in its state directory, no host store.
    (cd "$dir" && exec env RUST_LOG=warn "$rt" --config "$cfg" --modules "$mods" >"$dir/rbac-settle.log" 2>&1) &
  else
    FLUXOR_STORE_DIR="$dir" RUST_LOG=warn "$rt" --config "$cfg" --modules "$mods" >"$dir/rbac-settle.log" 2>&1 &
  fi
  local pid=$! ok=0
  # A lattice store is read by copying it (lattice-store-dump), which
  # competes with the runtime it waits on — a seeded host log is imported
  # before the graph serves — so it is polled once a second, for as long
  # as the import may take, rather than ten times a second for a fixed
  # count.
  local poll=0.1 limit=30
  if [ -d "$dir/lattice" ]; then poll=1; limit=300; fi
  local until=$(( $(date +%s) + limit ))
  while [ "$(date +%s)" -lt "$until" ]; do
    ok=1
    local keys; keys="$(_rbac_live_keys "$dir")"
    for k in "$@"; do
      if [ "$want" = present ]; then printf '%s\n' "$keys" | grep -qxF "$k" || { ok=0; break; }
      else printf '%s\n' "$keys" | grep -qxF "$k" && { ok=0; break; }; fi
    done
    [ "$ok" = 1 ] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep "$poll"
  done
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  if [ "$ok" != 1 ]; then
    echo "FAIL: RBAC index did not settle ($want): $*" >&2
    _rbac_live_keys "$dir" | head -40 >&2
    return 1
  fi
}
