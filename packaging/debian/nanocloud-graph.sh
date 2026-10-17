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
#   KUBELET  the node's pod plane: the kubelet's chains and its sandbox
#            runtime (fluxor-podlifecycle.yaml, installed as
#            /etc/nanocloud.io/fluxor/kubelet.yaml; NANOCLOUD_KUBELET_FRAGMENT
#            overrides) and its image pull (fluxor-imagepull.yaml, installed
#            as /etc/nanocloud.io/fluxor/image.yaml; NANOCLOUD_IMAGE_FRAGMENT
#            overrides) — on a worker always; on the control plane when it
#            runs Pods too (NANOCLOUD_PODS=1), with this node's lease
#            (worker.yaml's), so a cluster of one is its own node
#
# Values come from /etc/nanocloud.io/cluster.env (NANOCLOUD_CLUSTER_ENV
# overrides the path), each overridable from the environment:
#
#   NANOCLOUD_ROLE        plane or node                (default: plane)
#   NANOCLOUD_NODE_NAME   this node's name             (default: hostname -s)
#   NANOCLOUD_NODE_ADDRESS  this node's address: its Node's InternalIP, the
#                         hostIP of its Pods (and their podIP, sharing its
#                         network)  (default: the source address of its
#                         default route; none when it has none)
#   NANOCLOUD_FLUXOR_ABI  the fluxor ABI its runtime and modules are built
#                         against, its `nanocloud.io/fluxor-abi` label
#                         (default: /usr/share/nanocloud/fluxor-abi)
#   NANOCLOUD_REPLICA_ID  its replica id in the group  (default: 0)
#   NANOCLOUD_VOTERS      voters, ids 0..N-1           (default: 1)
#   NANOCLOUD_PEERS       "id=host:port ..." for every replica this node
#                         may link to, itself included
#                         (default: "0=127.0.0.1:<NANOCLOUD_PEER_PORT>")
#   NANOCLOUD_PEER_PORT   this node's peer listener    (default: 7450)
#   NANOCLOUD_PKI         the peer PKI directory       (default:
#                         /etc/nanocloud.io/pki)
#   NANOCLOUD_PEER_TRUST  how a peer link is trusted: `pins` (the PKI
#                         directory's keys, the default) or `authority` (the
#                         cluster's own authority certifies each node's vault
#                         keys as …/replica-<id>.<role>; no PKI directory)
#   NANOCLOUD_TRUST_DOMAIN  the SPIFFE trust domain under `authority`
#                         (default: nanocloud.local)
#
# Under `authority` a control plane also holds no API key: its API's TLS
# key is born in the vault too and certified by the same authority, and
# node_pki mints the operator's join — announced as
#   [join] secret=<hex> ca-sha256=<hex>
# and spent with scripts/rig-join.sh — with a bootstrap credential for each
# worker slot (every replica in NANOCLOUD_PEERS past the voters).
#   NANOCLOUD_PLANE_API   a worker's control plane API under `authority`,
#                         host:port: its link ends are certified there
#   NANOCLOUD_SCOPED      a node's private prefixes  (default: the
#                         kubelet's, its sandboxes', probes' and image
#                         cache's — every image record, its blob markers
#                         and its fetch queue among them, are /image-…;
#                         see below)
#   NANOCLOUD_PODS        on the control plane: 1 runs Pods here too
#                         (default: 0; a fresh install's cluster.env says 1)
#
# The image pull, from the node's own config (never from the graph's
# artifacts: the CA is named by path, credentials are a store key):
#
#   NANOCLOUD_REGISTRY    the registry a bare image reference means, and
#                         the one registry this node pulls from: a
#                         reference naming another is refused (the Pod's
#                         ErrImagePull event), never pulled from this one
#                         (default: registry.nanocloud.io:5000)
#   NANOCLOUD_REGISTRY_ADDR  where it is dialled, host:port (default: the
#                         registry's own name)
#   NANOCLOUD_REGISTRY_TLS   1: through a `tls` client that authenticates
#                         the registry by its CA and its name (the default);
#                         0: plaintext (a registry on a trusted link only)
#   NANOCLOUD_REGISTRY_CA    that CA, DER (default:
#                         /etc/nanocloud.io/certs/ca.der). Absent: the node
#                         renders without the pull, and says so
#   NANOCLOUD_IMAGE_BLOBS    the blob cache (default:
#                         /var/lib/nanocloud.io/image/blobs)
#
# A registry's credentials (OCI token auth) are the value `user:password`
# of the store key /registry-auth/<registry host>, when the operator writes
# one; anonymous otherwise. imagePullSecrets are not read (a Pod's own
# credentials for its own registry): one registry, one credential, per node.
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
#
# Under `authority` it holds no key. A worker's holds what joining it
# gave (scripts/rig-join.sh writes both beside the operator's kubeconfig):
# plane-ca.der, the cluster's authority its API is verified by, and
# node.token, its slot's bootstrap credential — which may patch and read
# that slot's two Devices (nanocloud-peers/replica-<id>.server, .client)
# and nothing else. Its link ends are certified through them.
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
# The node's labels, as JSON members: what a Pod's nodeSelector names.
case "$(uname -m)" in
    aarch64|arm64) ARCH=arm64 ;;
    x86_64|amd64) ARCH=amd64 ;;
    *) ARCH="$(uname -m)" ;;
esac
FLUXOR_ABI="${NANOCLOUD_FLUXOR_ABI:-$(cat /usr/share/nanocloud/fluxor-abi 2>/dev/null || true)}"
NODE_FACTS="\"kubernetes.io/arch\":\"$ARCH\",\"kubernetes.io/os\":\"linux\",\"kubernetes.io/hostname\":\"$NODE_NAME\""
[ -z "$FLUXOR_ABI" ] || NODE_FACTS="$NODE_FACTS,\"nanocloud.io/fluxor-abi\":\"$FLUXOR_ABI\""
# The node's address, as its Node advertises it (node_lease.uproc `renew`).
NODE_ADDRESS="${NANOCLOUD_NODE_ADDRESS-$(ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | head -1)}"
REPLICA_ID="${NANOCLOUD_REPLICA_ID:-0}"
VOTERS="${NANOCLOUD_VOTERS:-1}"
PEER_PORT="${NANOCLOUD_PEER_PORT:-7450}"
PEERS="${NANOCLOUD_PEERS:-0=127.0.0.1:$PEER_PORT}"
PKI="${NANOCLOUD_PKI:-/etc/nanocloud.io/pki}"
PEER_TRUST="${NANOCLOUD_PEER_TRUST:-pins}"
TRUST_DOMAIN="${NANOCLOUD_TRUST_DOMAIN:-nanocloud.local}"
SCOPED="${NANOCLOUD_SCOPED:-/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,/volume-binds/,/image-,/volume-ops/}"
PODS="${NANOCLOUD_PODS:-0}"
case "${NANOCLOUD_ROLE:-plane}" in
    node) SCOPE="$NODE_NAME"; PODS=1 ;;
    *)
        case "$PODS" in
            1) SCOPE="$NODE_NAME" ;;
            0) SCOPE=""; SCOPED="" ;;
            *) fail "NANOCLOUD_PODS is '$PODS', not 0 or 1" ;;
        esac
        ;;
esac
REGISTRY="${NANOCLOUD_REGISTRY:-registry.nanocloud.io:5000}"
REGISTRY_ADDR="${NANOCLOUD_REGISTRY_ADDR:-$REGISTRY}"
REGISTRY_TLS="${NANOCLOUD_REGISTRY_TLS:-1}"
REGISTRY_CA="${NANOCLOUD_REGISTRY_CA:-/etc/nanocloud.io/certs/ca.der}"
IMAGE_BLOBS="${NANOCLOUD_IMAGE_BLOBS:-/var/lib/nanocloud.io/image/blobs}"
REGISTRY_HOST="${REGISTRY%%:*}"
case "$REGISTRY" in
    ""|*/*|*" "*|*'"'*) fail "NANOCLOUD_REGISTRY is '$REGISTRY', not host[:port]" ;;
esac
case "$REGISTRY_ADDR$IMAGE_BLOBS$REGISTRY_CA" in
    *'"'*|*" "*) fail "a registry setting holds a quote or a space" ;;
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
if [ "$PODS" = 1 ] && grep -q "__KUBELET_MODULES__" "$TEMPLATE"; then
    F="$(fragment "${NANOCLOUD_KUBELET_FRAGMENT:-}" kubelet.yaml fluxor-podlifecycle.yaml)" \
        || fail "no kubelet graph beside $TEMPLATE"
    # Its blob cache is the pull's.
    KUBELET_MODULES="$(modules_of "$F" \
        | sed "s#^    blobs: \"/var/lib/nanocloud.io/image/blobs\"\$#    blobs: \"$IMAGE_BLOBS\"#")"
    KUBELET_WIRING="$(wiring_of "$F")"
    # The image pull: the fragment as installed, given this node's registry.
    F="$(fragment "${NANOCLOUD_IMAGE_FRAGMENT:-}" image.yaml fluxor-imagepull.yaml)" \
        || fail "no image graph beside $TEMPLATE"
    if [ "$REGISTRY_TLS" = 1 ] && [ ! -r "$REGISTRY_CA" ]; then
        echo "nanocloud-graph: no registry CA at $REGISTRY_CA: this node does not pull images" \
            "(NANOCLOUD_REGISTRY_CA, or NANOCLOUD_REGISTRY_TLS=0)" >&2
    else
        IMAGE_MODULES="$(modules_of "$F" | sed \
            -e "s#^    authority: \"registry.nanocloud.io:5000\"\$#    authority: \"$REGISTRY_ADDR\"#" \
            -e "s#^    label: \"registry.nanocloud.io:5000\"\$#    label: \"$REGISTRY\"#" \
            -e "s#^    credentials: \"/registry-auth/registry.nanocloud.io\"\$#    credentials: \"/registry-auth/$REGISTRY_HOST\"#" \
            -e "s#^    dir: \"/var/lib/nanocloud.io/image/blobs\"\$#    dir: \"$IMAGE_BLOBS\"#")"
        IMAGE_WIRING="$(wiring_of "$F")"
        if [ "$REGISTRY_TLS" = 1 ]; then
            # fetch <-> fetch_tls <-> linux_net, in place of fetch <-> linux_net.
            IMAGE_MODULES="$IMAGE_MODULES
  - name: fetch_tls
    type: tls
    mode: 0
    alpn_h1_only: 1
    peer_auth: 2
    trust: \"\${file:$REGISTRY_CA}\"
    verify_hostname: \"$REGISTRY_HOST\""
            IMAGE_WIRING="$(printf '%s\n' "$IMAGE_WIRING" | awk '
                /^  - from: / { f = $3; next }
                /^    to: / { t = $2
                    if (!((f ~ /^linux_net\./ && t ~ /^fetch\./) || (f ~ /^fetch\./ && t ~ /^linux_net\./)))
                        printf "  - from: %s\n    to: %s\n", f, t
                    next }
                { print }')
  - from: linux_net.net_out
    to: fetch_tls.cipher_in
  - from: fetch_tls.cipher_out
    to: linux_net.net_in
  - from: fetch_tls.clear_out
    to: fetch.net_in
  - from: fetch.net_out
    to: fetch_tls.clear_in"
        fi
        KUBELET_MODULES="$KUBELET_MODULES
$IMAGE_MODULES"
        KUBELET_WIRING="$KUBELET_WIRING
$IMAGE_WIRING"
    fi
    # The control plane's own lease, as a worker keeps its (worker.yaml).
    if [ "${NANOCLOUD_ROLE:-plane}" != node ]; then
        F="$(fragment "" worker.yaml fluxor-worker.yaml)" || fail "no worker graph beside $TEMPLATE"
        LEASE="$(sed -n '/^  - name: nl_self$/,/^wiring:$/p' "$F" | sed '$d')"
        [ -n "$LEASE" ] || fail "no node lease (nl_self) in $F"
        KUBELET_MODULES="$KUBELET_MODULES
$LEASE"
        KUBELET_WIRING="$KUBELET_WIRING
  - from: nl_self.status
    to: nl_self.changes
  - from: nl_self.record_out
    to: nl_renew.request_in"
    fi
fi

pin() { # the SHA-256 of a certificate's raw P-256 point
    openssl x509 -in "$1" -noout -pubkey \
        | openssl pkey -pubin -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1
}

case "$PEER_TRUST" in
    pins)
        [ -r "$PKI/peer.der" ] && [ -r "$PKI/peer.key.der" ] || fail "no peer key in $PKI"
        ;;
    authority) ;;
    *) fail "NANOCLOUD_PEER_TRUST is '$PEER_TRUST', not pins or authority" ;;
esac
# The peers' SPIFFE path under `authority`: kagi's device identities,
# tenant nanocloud-peers (spiffe://<domain>/device/<tenant>/<device>).
PEER_DOMAIN="spiffe://$TRUST_DOMAIN/device/nanocloud-peers/"

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
    PEER_LINES="$PEER_LINES
      peer$id: \"$addr\""
    [ "$PEER_TRUST" = pins ] || continue
    cert="$PKI/peers/$id.pem"
    [ -r "$cert" ] || fail "no certificate for replica $id ($cert)"
    PEER_LINES="$PEER_LINES
      peer${id}_key: \"$(pin "$cert")\""
done

# How each link end is trusted, and how the router binds a replica.
if [ "$PEER_TRUST" = authority ]; then
    peer_tls() { # peer_tls <role>
        printf '    peer_auth: ca_uri\n    identity_label: "nanocloud.peer.%s"\n    trust_issuer: 1\n    verify_uri: "%s"' \
            "$1" "$PEER_DOMAIN"
    }
    PEER_SERVER_TRUST="$(peer_tls server)"
    PEER_CLIENT_TRUST="$(peer_tls client)"
    PEER_POLICY="require_identity: principal
      principal_prefix: \"${PEER_DOMAIN}replica-\""
    # The control plane admits a learner slot (an id past its voters) once
    # that slot's principal binds: only its join could have certified it.
    [ "${NANOCLOUD_ROLE:-plane}" = node ] || PEER_POLICY="$PEER_POLICY
      voters: $VOTERS"
else
    peer_tls() {
        printf '    peer_auth: pinned\n    trust: "${file:%s/trust.pem}"\n    cert_file: "%s/peer.der"\n    key_file: "%s/peer.key.der"' \
            "$PKI" "$PKI" "$PKI"
    }
    PEER_SERVER_TRUST="$(peer_tls)"
    PEER_CLIENT_TRUST="$(peer_tls)"
    PEER_POLICY="require_identity: pinned"
fi

# Under `authority`, who certifies each link end's vault key: on the
# control plane, its own authority (kagi's certificate_endpoint, in its
# graph); on a worker, that authority through the plane's API — the slot's
# Devices, patched and read back with the slot's bootstrap credential, over
# TLS verified by the joined authority.
PEER_PKI_MODULES=""
PEER_PKI_WIRING=""
API_IDENTITY='    cert_file: "/etc/nanocloud.io/certs/server.der"
    key_file: "/etc/nanocloud.io/certs/server.key.der"'
if [ "$PEER_TRUST" = authority ]; then
    if [ "${NANOCLOUD_ROLE:-plane}" = node ]; then
        PLANE_API="${NANOCLOUD_PLANE_API:-}"
        [ -n "$PLANE_API" ] || fail "a worker under authority needs NANOCLOUD_PLANE_API (host:port)"
        [ -r "$PKI/plane-ca.der" ] && [ -r "$PKI/node.token" ] \
            || fail "no join credentials in $PKI (plane-ca.der, node.token)"
        for role in server client; do
            PEER_PKI_MODULES="$PEER_PKI_MODULES
  - name: node_pki_peer_$role
    type: node_pki
    remote: \"/apis/nanocloud.io/v1/namespaces/nanocloud-peers/devices/replica-$REPLICA_ID.$role\"
    bearer: \"\${file:$PKI/node.token}\""
            PEER_PKI_WIRING="$PEER_PKI_WIRING
  - from: peer_tls_$role.identity_req
    to: node_pki_peer_$role.identity_req
  - from: node_pki_peer_$role.identity_cert
    to: peer_tls_$role.identity_cert
  - from: node_pki_peer_$role.request_out
    to: plane_api.request_in
  - from: plane_api.response_out
    to: node_pki_peer_$role.response_in"
        done
        PEER_PKI_MODULES="$PEER_PKI_MODULES
  - name: plane_api
    type: http
    variant: h1_exchange
    params:
      mode: 1
      authority: \"$PLANE_API\"
  - name: plane_tls
    type: tls
    mode: 0
    alpn_h1_only: 1
    peer_auth: 2
    trust: \"\${file:$PKI/plane-ca.der}\"
    verify_hostname: \"kubernetes.default.svc.cluster.local\""
        PEER_PKI_WIRING="$PEER_PKI_WIRING
  - from: plane_api.net_out
    to: plane_tls.clear_in
  - from: plane_tls.clear_out
    to: plane_api.net_in
  - from: linux_net.net_out
    to: plane_tls.cipher_in
  - from: plane_tls.cipher_out
    to: linux_net.net_in"
    else
        # A plane graph without the authority of its own (the CLI applet,
        # this node's replica run offline) carries it: the same vault key
        # (`nanocloud.ca`) in the same vault certifies the same principals.
        if ! grep -q "^  - name: certificate_endpoint$" "$TEMPLATE"; then
            OWN_AUTHORITY=1
        fi
        # The API's identity, and the join — where the graph serves the API
        # (the control plane's; not the CLI applet's).
        if grep -q "__API_IDENTITY__" "$TEMPLATE"; then
            # An admin credential for the operator, and one per worker slot
            # (a replica past the voters).
            SLOTS=""
            for p in $PEERS; do
                id="${p%%=*}"
                [ "$id" -ge "$VOTERS" ] && SLOTS="${SLOTS:+$SLOTS,}$id"
            done
            API_IDENTITY='    identity_label: "nanocloud.api"'
            PEER_PKI_MODULES="
  - name: node_pki
    subject: \"CN=kubernetes\"
    tenant: \"nanocloud\"
    device: \"$NODE_NAME\"
    dns: \"kubernetes.default.svc.cluster.local\"
    join: 1${SLOTS:+
    join_nodes: \"$SLOTS\"}"
            PEER_PKI_WIRING="
  - from: tls.identity_req
    to: node_pki.identity_req
  - from: node_pki.identity_cert
    to: tls.identity_cert
  - from: node_pki.request_out
    to: certificate_endpoint.request_in
  - from: certificate_endpoint.response_out
    to: node_pki.response_in"
        fi
        # Both peer link ends in one node_pki: extra identities of the
        # API's where the graph serves the API; otherwise (the CLI applet)
        # a node_pki of their own, the server end its first identity.
        if grep -q "__API_IDENTITY__" "$TEMPLATE"; then
            PKI_NAME=node_pki
            n=2
            ROLES="server client"
        else
            PKI_NAME=node_pki_peers
            n=2
            ROLES="client"
            PEER_PKI_MODULES="$PEER_PKI_MODULES
  - name: node_pki_peers
    type: node_pki
    subject: \"CN=replica-$REPLICA_ID\"
    tenant: \"nanocloud-peers\"
    device: \"replica-$REPLICA_ID.server\"
    join: 0"
            PEER_PKI_WIRING="$PEER_PKI_WIRING
  - from: peer_tls_server.identity_req
    to: node_pki_peers.identity_req
  - from: node_pki_peers.identity_cert
    to: peer_tls_server.identity_cert
  - from: node_pki_peers.request_out
    to: certificate_endpoint.request_in
  - from: certificate_endpoint.response_out
    to: node_pki_peers.response_in"
        fi
        for role in $ROLES; do
            PEER_PKI_MODULES="$PEER_PKI_MODULES
    id${n}_subject: \"CN=replica-$REPLICA_ID\"
    id${n}_tenant: \"nanocloud-peers\"
    id${n}_device: \"replica-$REPLICA_ID.$role\""
            PEER_PKI_WIRING="$PEER_PKI_WIRING
  - from: peer_tls_$role.identity_req
    to: $PKI_NAME.identity_req_$n
  - from: $PKI_NAME.identity_cert_$n
    to: peer_tls_$role.identity_cert"
            n=$((n + 1))
        done
    fi
fi
if [ "${OWN_AUTHORITY:-0}" = 1 ]; then
    PEER_PKI_MODULES="$PEER_PKI_MODULES
  - name: certificate_endpoint
    issuer_cn: \"nanocloud-ca\"
    trust_domain: \"$TRUST_DOMAIN\"
    dns_suffix: \".svc.cluster.local\"
    key_label: \"nanocloud.ca\""
fi
PEER_PKI_MODULES="${PEER_PKI_MODULES#
}"
PEER_PKI_WIRING="${PEER_PKI_WIRING#
}"

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
# A key line, or nothing: `operations` with no key pinned admits no one
# over the wire (under `authority` none is pinned: the plane's own learner
# admission is local). Its params map is never rendered empty — an empty
# one does not build — so it then states its emit cadence (the default).
ADMIN_LINES="${ADMIN_LINES#
      }"
[ -n "$ADMIN_LINES" ] || ADMIN_LINES="emit_interval_ms: 1000"

# The anchors a pinned peer link accepts: every replica and every admin key.
if [ "$PEER_TRUST" = pins ]; then
    umask 022
    cat "$PKI"/peers/*.pem $(ls "$PKI"/operators/*.pem "$PKI"/observers/*.pem 2>/dev/null) \
        > "$PKI/trust.pem.new"
    mv "$PKI/trust.pem.new" "$PKI/trust.pem"
fi

mkdir -p "$(dirname "$OUT")"
# The fragment first: its own placeholders are rendered with the rest.
fluxor render-template "$TEMPLATE" \
    --var "REPLICA_MODULES=$REPLICA_MODULES" \
    --var "REPLICA_WIRING=$REPLICA_WIRING" \
    --var "KUBELET_MODULES=$KUBELET_MODULES" \
    --var "KUBELET_WIRING=$KUBELET_WIRING" \
    --var "NODE_NAME=$NODE_NAME" \
    --var "NODE_FACTS=$NODE_FACTS" \
    --var "NODE_ADDRESS=$NODE_ADDRESS" \
    --var "REPLICA_ID=$REPLICA_ID" \
    --var "VOTERS=$VOTERS" \
    --var "QUORUM=$((VOTERS / 2 + 1))" \
    --var "PEER_PORT=$PEER_PORT" \
    --var "PKI=$PKI" \
    --var "SCOPE=$SCOPE" \
    --var "SCOPED=$SCOPED" \
    --var "CLIENT_REPLIES=$CLIENT_REPLIES" \
    --var "PEERS=$PEER_LINES" \
    --var "PEER_SERVER_TRUST=$PEER_SERVER_TRUST" \
    --var "PEER_CLIENT_TRUST=$PEER_CLIENT_TRUST" \
    --var "PEER_POLICY=$PEER_POLICY" \
    --var "PEER_PKI_MODULES=$PEER_PKI_MODULES" \
    --var "PEER_PKI_WIRING=$PEER_PKI_WIRING" \
    --var "API_IDENTITY=$API_IDENTITY" \
    --var "ADMIN_KEYS=$ADMIN_LINES" \
    --output "$OUT"
