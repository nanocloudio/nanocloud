#!/bin/sh
# Render this node's graph from its template: the control plane (a voter of
# the replicated store) or a worker (a learner of it), by role.
#
#   nanocloud-graph.sh <template|dir> <out>
#
# Given a directory (the installed /etc/nanocloud.io/fluxor), the template
# is the role's: controlplane.yaml (NANOCLOUD_ROLE=plane, the default) or
# worker.yaml (NANOCLOUD_ROLE=node).
#
# A template names the graphs it is built from at `__<NAME>_MODULES__` and
# `__<NAME>_WIRING__`; their modules and wiring are spliced there first,
# then every value is rendered:
#
#   REPLICA  the store's replica (fluxor-replica.yaml, installed as
#            /etc/nanocloud.io/fluxor/replica.yaml;
#            NANOCLOUD_REPLICA_FRAGMENT overrides)
#   KUBELET  the node's pod plane (fluxor-podlifecycle.yaml, installed as
#            /etc/nanocloud.io/fluxor/kubelet.yaml;
#            NANOCLOUD_KUBELET_FRAGMENT overrides)
#
# Values come from /etc/nanocloud.io/cluster.env (NANOCLOUD_CLUSTER_ENV
# overrides the path), each overridable from the environment:
#
#   NANOCLOUD_ROLE        plane or node                (default: plane)
#   NANOCLOUD_NODE_NAME   this node's name             (default: hostname -s)
#   NANOCLOUD_REPLICA_ID  its replica id in the group  (default: 0)
#   NANOCLOUD_VOTERS      voters, ids 0..N-1           (default: 1)
#   NANOCLOUD_PEERS       "id=host:port ..." for every replica this node
#                         may link to, itself included
#                         (default: "0=127.0.0.1:<NANOCLOUD_PEER_PORT>")
#   NANOCLOUD_PEER_PORT   this node's peer listener    (default: 7450)
#   NANOCLOUD_PKI         the peer PKI directory       (default:
#                         /etc/nanocloud.io/pki)
#   NANOCLOUD_SCOPED      a worker's private prefixes  (default: the
#                         kubelet's, its sandboxes', probes' and image
#                         cache's; see below)
#
# A worker's replica is scoped to its node: its private records are kept
# under /~node/<node> in the store, apart from every other node's, and its
# graph names them plainly. The control plane is not scoped.
#
# The PKI directory holds this node's peer key (peer.der, peer.key.der),
# each replica's certificate as peers/<id>.pem, and the admin keys as
# operators/*.pem and observers/*.pem. A replica's pin and an admin's are
# the SHA-256 of the key's raw point; trust.pem, written here, is every one
# of those certificates — the anchors the peer links accept.
set -eu

TEMPLATE="$1"
OUT="$2"
ENV_FILE="${NANOCLOUD_CLUSTER_ENV:-/etc/nanocloud.io/cluster.env}"
# `NANOCLOUD_*=value` lines; a value already in the environment wins.
if [ -r "$ENV_FILE" ]; then
    while IFS= read -r line; do
        case "$line" in
            NANOCLOUD_*=*)
                k="${line%%=*}"
                eval "[ -n \"\${$k+set}\" ] || $line"
                ;;
        esac
    done < "$ENV_FILE"
fi

fail() { echo "nanocloud-graph: $1" >&2; exit 1; }

if [ -d "$TEMPLATE" ]; then
    case "${NANOCLOUD_ROLE:-plane}" in
        plane) TEMPLATE="$TEMPLATE/controlplane.yaml" ;;
        node) TEMPLATE="$TEMPLATE/worker.yaml" ;;
        *) fail "NANOCLOUD_ROLE is '${NANOCLOUD_ROLE}', not plane or node" ;;
    esac
fi

NODE_NAME="${NANOCLOUD_NODE_NAME:-$(hostname -s)}"
REPLICA_ID="${NANOCLOUD_REPLICA_ID:-0}"
VOTERS="${NANOCLOUD_VOTERS:-1}"
PEER_PORT="${NANOCLOUD_PEER_PORT:-7450}"
PEERS="${NANOCLOUD_PEERS:-0=127.0.0.1:$PEER_PORT}"
PKI="${NANOCLOUD_PKI:-/etc/nanocloud.io/pki}"
SCOPED="${NANOCLOUD_SCOPED:-/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,/volume-binds/,/image-,/volume-ops/}"
case "${NANOCLOUD_ROLE:-plane}" in
    node) SCOPE="$NODE_NAME" ;;
    *) SCOPE=""; SCOPED="" ;;
esac

# fragment <override> <installed name> <source name>: the graph to splice.
fragment() {
    for f in "$1" "$(dirname "$TEMPLATE")/$2" "$(dirname "$TEMPLATE")/$3" \
             "/etc/nanocloud.io/fluxor/$2"; do
        [ -n "$f" ] && [ -r "$f" ] && { echo "$f"; return 0; }
    done
    return 1
}
modules_of() { sed -n '/^modules:$/,/^wiring:$/p' "$1" | sed '1d;$d'; }
wiring_of() { sed -n '/^wiring:$/,$p' "$1" | sed '1d'; }

F="$(fragment "${NANOCLOUD_REPLICA_FRAGMENT:-}" replica.yaml fluxor-replica.yaml)" \
    || fail "no replica fragment beside $TEMPLATE"
REPLICA_MODULES="$(modules_of "$F")"
REPLICA_WIRING="$(wiring_of "$F")"
# The template's router, if it has one, reads the replies to the store's
# own proposals; without one nothing does (the gateway's client_replies).
CLIENT_REPLIES=0
grep -q "^  - from: gateway.client_responses" "$TEMPLATE" && CLIENT_REPLIES=1
KUBELET_MODULES=""
KUBELET_WIRING=""
if grep -q "__KUBELET_MODULES__" "$TEMPLATE"; then
    F="$(fragment "${NANOCLOUD_KUBELET_FRAGMENT:-}" kubelet.yaml fluxor-podlifecycle.yaml)" \
        || fail "no kubelet graph beside $TEMPLATE"
    KUBELET_MODULES="$(modules_of "$F")"
    KUBELET_WIRING="$(wiring_of "$F")"
fi

pin() { # the SHA-256 of a certificate's raw P-256 point
    openssl x509 -in "$1" -noout -pubkey \
        | openssl pkey -pubin -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1
}

[ -r "$PKI/peer.der" ] && [ -r "$PKI/peer.key.der" ] || fail "no peer key in $PKI"

# peer_router: one address and one pin per replica, ids 0..max.
MAX=-1
for p in $PEERS; do
    id="${p%%=*}"
    [ "$id" -gt "$MAX" ] && MAX="$id"
done
[ "$REPLICA_ID" -le "$MAX" ] || fail "replica $REPLICA_ID is not in NANOCLOUD_PEERS"
PEER_LINES="peer_count: $((MAX + 1))"
for p in $PEERS; do
    id="${p%%=*}"
    addr="${p#*=}"
    cert="$PKI/peers/$id.pem"
    [ -r "$cert" ] || fail "no certificate for replica $id ($cert)"
    PEER_LINES="$PEER_LINES
      peer$id: \"$addr\"
      peer${id}_key: \"$(pin "$cert")\""
done

# operations: the keys wire admin is judged by.
ADMIN_LINES=""
n=0
for c in "$PKI"/operators/*.pem; do
    [ -r "$c" ] || continue
    ADMIN_LINES="$ADMIN_LINES
      operator_key$n: \"$(pin "$c")\""
    n=$((n + 1))
done
n=0
for c in "$PKI"/observers/*.pem; do
    [ -r "$c" ] || continue
    ADMIN_LINES="$ADMIN_LINES
      observer_key$n: \"$(pin "$c")\""
    n=$((n + 1))
done
# A key line, or nothing: `operations` with no key pinned admits no one.
ADMIN_LINES="${ADMIN_LINES#
      }"

# The anchors a peer link accepts: every replica and every admin key.
umask 022
cat "$PKI"/peers/*.pem $(ls "$PKI"/operators/*.pem "$PKI"/observers/*.pem 2>/dev/null) \
    > "$PKI/trust.pem.new"
mv "$PKI/trust.pem.new" "$PKI/trust.pem"

mkdir -p "$(dirname "$OUT")"
# The fragment first: its own placeholders are rendered with the rest.
fluxor render-template "$TEMPLATE" \
    --var "REPLICA_MODULES=$REPLICA_MODULES" \
    --var "REPLICA_WIRING=$REPLICA_WIRING" \
    --var "KUBELET_MODULES=$KUBELET_MODULES" \
    --var "KUBELET_WIRING=$KUBELET_WIRING" \
    --var "NODE_NAME=$NODE_NAME" \
    --var "REPLICA_ID=$REPLICA_ID" \
    --var "VOTERS=$VOTERS" \
    --var "QUORUM=$((VOTERS / 2 + 1))" \
    --var "PEER_PORT=$PEER_PORT" \
    --var "PKI=$PKI" \
    --var "SCOPE=$SCOPE" \
    --var "SCOPED=$SCOPED" \
    --var "CLIENT_REPLIES=$CLIENT_REPLIES" \
    --var "PEERS=$PEER_LINES" \
    --var "ADMIN_KEYS=$ADMIN_LINES" \
    --output "$OUT"
