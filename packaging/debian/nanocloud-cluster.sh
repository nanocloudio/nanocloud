#!/bin/sh
# The cluster's membership, as an operator changes it.
#
#   nanocloud-cluster pin              this node's peer certificate and pin:
#                                      what every other node is given as
#                                      peers/<id>.pem
#   nanocloud-cluster add-node <id>    on a control-plane node: add replica
#                                      <id> (a worker) to the group as a
#                                      learner
#   nanocloud-cluster remove-node <id> remove it
#
# Adding and removing are wire admin commands to this node's replica, over
# mutual TLS from the operator key provisioned here
# (/etc/nanocloud.io/pki/operator.pem, pinned as an operator by every
# control-plane node); the group decides them through its log. Values come
# from /etc/nanocloud.io/cluster.env, as nanocloud-graph.sh reads them.
set -eu

ENV_FILE="${NANOCLOUD_CLUSTER_ENV:-/etc/nanocloud.io/cluster.env}"
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
PKI="${NANOCLOUD_PKI:-/etc/nanocloud.io/pki}"
PEER_PORT="${NANOCLOUD_PEER_PORT:-7450}"

fail() { echo "nanocloud-cluster: $1" >&2; exit 1; }

pin() {
    openssl x509 -in "$1" -noout -pubkey \
        | openssl pkey -pubin -outform DER 2>/dev/null | tail -c 65 | sha256sum | cut -d' ' -f1
}

# admin <op> <id>: one wire admin command; prints its status byte.
admin() {
    id="$2"
    case "$id" in ''|*[!0-9]*) fail "replica id '$id' is not a number" ;; esac
    [ "$id" -le 255 ] || fail "replica id $id is past 255"
    [ -r "$PKI/operator.pem" ] && [ -r "$PKI/operator.key.pem" ] || fail "no operator key in $PKI"
    # MSG_ADMIN_COMMAND, u16 LE length 2, op, replica id; the answer is
    # MSG_ADMIN_RESPONSE (0x13), u16 LE length, status.
    frame="\\022\\002\\000\\$(printf '%03o' "$1")\\$(printf '%03o' "$id")"
    out="$( (printf "$frame"; sleep 3) \
        | timeout 10 openssl s_client -quiet -connect "127.0.0.1:$PEER_PORT" \
            -cert "$PKI/operator.pem" -key "$PKI/operator.key.pem" 2>/dev/null \
        | od -An -tx1 | tr -s ' \n' ' ')" || true
    status="$(printf '%s' "$out" | sed -n 's/.* 13 [0-9a-f][0-9a-f] [0-9a-f][0-9a-f] \([0-9a-f][0-9a-f]\).*/\1/p')"
    [ -n "$status" ] || fail "no answer from the replica on :$PEER_PORT"
    [ "$status" = 00 ] || fail "refused (status 0x$status): is this a control-plane node, and the key an operator's?"
    echo "ok"
}

case "${1:-}" in
    pin)
        echo "certificate: $PKI/peer.pem"
        echo "pin:         $(pin "$PKI/peer.pem")"
        ;;
    add-node) admin 12 "${2:-}" ;;     # ADMIN_OP_ADD_LEARNER 0x0C
    remove-node) admin 13 "${2:-}" ;;  # ADMIN_OP_REMOVE_LEARNER 0x0D
    *)
        echo "usage: nanocloud-cluster pin | add-node <id> | remove-node <id>" >&2
        exit 2
        ;;
esac
