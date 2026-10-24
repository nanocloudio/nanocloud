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
#            (worker.yaml's), so a cluster of one is its own node — and the
#            node's dataplane enforcement: the `net_apply` nodes of the proxy
#            and the NetworkPolicy graphs (below), which realize the
#            cluster's rows as this node's packet policy (nftables DNAT and
#            filters) — on every node that runs Pods, a worker as much as the
#            control plane; each knows its own name, so a `Local` traffic
#            policy keeps to the backends on it, and its host ports are its
#            own
#   SERVICES the cluster's Service plane, on the control plane (a template
#            naming `__SERVICES_MODULES__`): the endpoints controller
#            (fluxor-endpoints.yaml, installed as endpoints.yaml), the cluster
#            DNS (fluxor-dns.yaml, as dns.yaml), and the proxy's chain
#            (fluxor-proxy.yaml, as proxy.yaml) and the NetworkPolicy chain
#            (fluxor-netpolicy.yaml, as netpolicy.yaml) — every node of those
#            but their `net_apply`, which each node runs for itself (KUBELET).
#            The chains write the cluster's rows once; what is per node is
#            decided where they are enforced
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
#   NANOCLOUD_NODE_CAPACITY  its capacity, as JSON members (default: its
#                         CPUs, its MemTotal, 110 Pods)
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
#   NANOCLOUD_NETPOLICY   1 (the default): NetworkPolicy is computed (the
#                         control plane) and enforced (each node); 0 leaves
#                         it out, and a NetworkPolicy then constrains nothing
#   NANOCLOUD_NODE_PORTS  the NodePort range (default: 30000-32767)
#   NANOCLOUD_LB_POOL     the addresses LoadBalancer Services are given,
#                         <a.b.c.d>/<n> (default: none — such a Service stays
#                         pending). Each node DNATs them; ONE node answers
#                         for each on the link that covers it (node_net:
#                         the holder of its claim, taken over when that node
#                         goes; for `Local`, only a node with an endpoint)
#   NANOCLOUD_CLUSTER_CIDR  the cluster's Pod CIDR (default: 10.244.0.0/16):
#                         each node is given a /24 of it at registration
#                         (svc_alloc, Node.spec.podCIDR), its Pods their
#                         addresses from that (ipam_pod), and every node
#                         routes each other node's /24 to it (node_net)
#   NANOCLOUD_DNS_PORT    the cluster DNS's port on the control plane
#                         (default: 53)
#   NANOCLOUD_DNS_SERVICE_IP  the ClusterIP of the Service fronting it,
#                         kube-system/kube-dns, inside the Service range
#                         (10.96.0.0/16) — what a Pod's resolv.conf names
#                         (default: 10.96.0.10; empty: no Service, Pods name
#                         the control plane's address)
#   NANOCLOUD_DNS_UPSTREAM  where it forwards every name outside
#                         cluster.local, an IPv4 address (default: the first
#                         nameserver of /etc/resolv.conf that is neither
#                         loopback nor this node; none: such names are
#                         REFUSED)
#
# The image pull, from the node's own config (never from the graph's
# artifacts: the CA is named by path, credentials are a store key):
#
#   NANOCLOUD_REGISTRY    the registry a bare image reference means (one
#                         naming another registry is asked of that
#                         registry, by its name — over TLS, only one the
#                         `tls` client below trusts and may name)
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
# A registry's challenge (OCI token auth, or Basic) is answered with the
# imagePullSecrets of the Pods a pull serves (image_auth.uproc), then the
# value `user:password` of the store key /registry-auth/<registry host>, when
# the operator writes one, then anonymously.
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
# The node's capacity, as its Node reports it (status.capacity and
# allocatable, node_lease.uproc `renew`): what the scheduler fits requests
# within — its CPUs, its memory, and upstream's 110 Pods.
NODE_CAPACITY="${NANOCLOUD_NODE_CAPACITY-\"cpu\":\"$(nproc 2>/dev/null || echo 1)\",\"memory\":\"$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)Ki\",\"pods\":\"110\"}"
REPLICA_ID="${NANOCLOUD_REPLICA_ID:-0}"
VOTERS="${NANOCLOUD_VOTERS:-1}"
PEER_PORT="${NANOCLOUD_PEER_PORT:-7450}"
PEERS="${NANOCLOUD_PEERS:-0=127.0.0.1:$PEER_PORT}"
PKI="${NANOCLOUD_PKI:-/etc/nanocloud.io/pki}"
PEER_TRUST="${NANOCLOUD_PEER_TRUST:-pins}"
TRUST_DOMAIN="${NANOCLOUD_TRUST_DOMAIN:-nanocloud.local}"
SCOPED="${NANOCLOUD_SCOPED:-/pod-,/sandboxes/,/metal-sandboxes/,/sandbox-,/tty/,/probe-,/volume-binds/,/image-,/volume-ops/,/np-status/}"
PODS="${NANOCLOUD_PODS:-0}"
CLUSTER_CIDR="${NANOCLOUD_CLUSTER_CIDR:-10.244.0.0/16}"
case "$CLUSTER_CIDR" in
    *[!0-9./]*|"") fail "NANOCLOUD_CLUSTER_CIDR is '$CLUSTER_CIDR', not <a.b.c.d>/<n>" ;;
esac
NETPOLICY="${NANOCLOUD_NETPOLICY:-1}"
case "$NETPOLICY" in 0|1) ;; *) fail "NANOCLOUD_NETPOLICY is '$NETPOLICY', not 0 or 1" ;; esac
# The dataplane graphs a node enforces (their net_apply) and the control
# plane computes (the rest).
DATAPLANE="proxy.yaml:fluxor-proxy.yaml"
[ "$NETPOLICY" = 0 ] || DATAPLANE="$DATAPLANE netpolicy.yaml:fluxor-netpolicy.yaml"
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
# A dataplane graph split in two: the node's enforcement (its `net_apply`
# nodes) and the rest (the cluster's chains).
#   applied_of <graph>            the net_apply nodes' names
#   split_modules <graph> in|out  their module blocks / every other one
#   split_wiring <graph> in|out   the edges touching them / every other one
applied_of() { modules_of "$1" | awk '/^  - name: /{n=$3} /^    type: net_apply$/{print n}'; }
split_modules() {
    modules_of "$1" | awk -v names=" $(applied_of "$1" | tr '\n' ' ')" -v want="$2" '
        /^  - name: / { hit = index(names, " " $3 " ") > 0 }
        { if ((want == "in") == hit) print }'
}
split_wiring() {
    wiring_of "$1" | awk -v names=" $(applied_of "$1" | tr '\n' ' ')" -v want="$2" '
        function end() { if (e != "" && (want == "in") == hit) printf "%s", e; e = ""; hit = 0 }
        /^  - from: / { end(); split($3, a, "."); hit = index(names, " " a[1] " ") > 0; e = $0 "\n"; next }
        /^    to: / { split($2, a, "."); if (index(names, " " a[1] " ") > 0) hit = 1; e = e $0 "\n"; next }
        /^    / { e = e $0 "\n"; next }
        { end() }
        END { end() }'
}

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
    # The node's dataplane enforcement: the proxy's (and NetworkPolicy's)
    # rows, realized as this node's packet policy.
    # Its node and host ports are DNATed at its own address, and its name
    # says which backends are its own.
    NODE_ADDR_LINE="\\n    node_name: \"$NODE_NAME\""
    [ -z "$NODE_ADDRESS" ] || NODE_ADDR_LINE="$NODE_ADDR_LINE\\n    node_address: \"$NODE_ADDRESS\""
    for pair in $DATAPLANE; do
        F="$(fragment "" "${pair%%:*}" "${pair#*:}")" || fail "no ${pair%%:*} beside $TEMPLATE"
        KUBELET_MODULES="$KUBELET_MODULES
$(split_modules "$F" in | sed "s#^    table: \"proxy\"\$#    table: \"proxy\"$NODE_ADDR_LINE#")"
        KUBELET_WIRING="$KUBELET_WIRING
$(split_wiring "$F" in)"
    done
    # The node's net_route writer: its segment from its Pod CIDR, its peer
    # routes to every other node's, and the load-balancer addresses it
    # answers for (node_net). Before any Pod of it is given an address of
    # its own: pod_resolve gives one only once the node says its segment is
    # in place.
    KUBELET_MODULES="$KUBELET_MODULES
  - name: node_net
    type: node_net
    node_name: \"$NODE_NAME\"
    cluster_cidr: \"$CLUSTER_CIDR\""
    # The control plane's own lease, as a worker keeps its (worker.yaml).
    if [ "${NANOCLOUD_ROLE:-plane}" != node ]; then
        F="$(fragment "" worker.yaml fluxor-worker.yaml)" || fail "no worker graph beside $TEMPLATE"
        LEASE="$(sed -n '/^  - name: nl_self$/,/^wiring:$/p' "$F" | sed '$d')"
        [ -n "$LEASE" ] || fail "no node lease (nl_self) in $F"
        KUBELET_MODULES="$KUBELET_MODULES
$LEASE"
        KUBELET_WIRING="$KUBELET_WIRING
  - from: nl_self.record_out
    to: nl_renew.request_in"
    fi
fi

# The cluster's Service plane, where the template names it (the control
# plane): endpoints, DNS, and the proxy's and NetworkPolicy's chains.
SERVICES_MODULES=""
SERVICES_WIRING=""
if grep -q "__SERVICES_MODULES__" "$TEMPLATE"; then
    DNS_PORT="${NANOCLOUD_DNS_PORT:-53}"
    if [ -n "${NANOCLOUD_DNS_UPSTREAM+set}" ]; then
        DNS_UPSTREAM="$NANOCLOUD_DNS_UPSTREAM"
    else
        DNS_UPSTREAM="$(awk -v self="$NODE_ADDRESS" '$1 == "nameserver" && $2 ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/ \
            && $2 !~ /^127\./ && $2 != self { print $2; exit }' /etc/resolv.conf 2>/dev/null || true)"
    fi
    case "$DNS_PORT$DNS_UPSTREAM" in
        *[!0-9.]*) fail "NANOCLOUD_DNS_PORT or NANOCLOUD_DNS_UPSTREAM is not a number or an address" ;;
    esac
    NODE_PORTS="${NANOCLOUD_NODE_PORTS:-30000-32767}"
    LB_POOL="${NANOCLOUD_LB_POOL:-}"
    case "$NODE_PORTS" in *[!0-9-]*|"") fail "NANOCLOUD_NODE_PORTS is '$NODE_PORTS', not <lo>-<hi>" ;; esac
    case "$LB_POOL" in *[!0-9./]*) fail "NANOCLOUD_LB_POOL is '$LB_POOL', not <a.b.c.d>/<n>" ;; esac
    for pair in endpoints.yaml:fluxor-endpoints.yaml dns.yaml:fluxor-dns.yaml $DATAPLANE; do
        F="$(fragment "" "${pair%%:*}" "${pair#*:}")" || fail "no ${pair%%:*} beside $TEMPLATE"
        SERVICES_MODULES="$SERVICES_MODULES
$(split_modules "$F" out)"
        SERVICES_WIRING="$SERVICES_WIRING
$(split_wiring "$F" out)"
    done
    UP_LINE=""
    [ -z "$DNS_UPSTREAM" ] || UP_LINE="\\n    upstream: \"$DNS_UPSTREAM\""
    # Where Pods reach it: this node's address, behind the ClusterIP of the
    # Service fronting it (/cluster-dns).
    DNS_SERVICE_IP="${NANOCLOUD_DNS_SERVICE_IP-10.96.0.10}"
    case "$DNS_SERVICE_IP" in *[!0-9.]*) fail "NANOCLOUD_DNS_SERVICE_IP is '$DNS_SERVICE_IP', not an address" ;; esac
    [ -z "$NODE_ADDRESS" ] || UP_LINE="$UP_LINE\\n    address: \"$NODE_ADDRESS\""
    [ -z "$NODE_ADDRESS" ] || [ -z "$DNS_SERVICE_IP" ] || UP_LINE="$UP_LINE\\n    service_ip: \"$DNS_SERVICE_IP\""
    LB_LINE=""
    [ -z "$LB_POOL" ] || LB_LINE="\\n    lb_pool: \"$LB_POOL\""
    # Every node's Pod CIDR, carved from the cluster's.
    LB_LINE="$LB_LINE\\n    cluster_cidr: \"$CLUSTER_CIDR\""
    SERVICES_MODULES="$(printf '%s\n' "$SERVICES_MODULES" \
        | sed -e "/^  - name: cluster_dns\$/,/^  - name: /s#^    port: 53\$#    port: $DNS_PORT$UP_LINE#" \
              -e "s#^    node_ports: \"30000-32767\"\$#    node_ports: \"$NODE_PORTS\"$LB_LINE#")"
    SERVICES_MODULES="${SERVICES_MODULES#
}"
    SERVICES_WIRING="${SERVICES_WIRING#
}"
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
    --var "SERVICES_MODULES=$SERVICES_MODULES" \
    --var "SERVICES_WIRING=$SERVICES_WIRING" \
    --var "NODE_NAME=$NODE_NAME" \
    --var "NODE_FACTS=$NODE_FACTS" \
    --var "NODE_ADDRESS=$NODE_ADDRESS" \
    --var "NODE_CAPACITY=$NODE_CAPACITY" \
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
