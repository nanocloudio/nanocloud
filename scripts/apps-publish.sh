#!/usr/bin/env bash
# Build the applications as workload images and push them to the cluster's
# registry, where a node's image plane pulls them:
#
#   lattice-kv       packaging/apps/lattice-kv.yaml       RESP on :6379
#   chronicle-http   packaging/apps/chronicle-http.yaml   HTTP on :8080
#   quantum-mqtt     quantum's examples/linux/minimal.yaml (scripts/support/
#                    to-workload.py, its durable WAL in its own storage root,
#                    without the admin plane whose `monitor` privilege a
#                    tenant may not hold)
#                                                          MQTT on :9090
#
#   apps-publish.sh [registry host:port]     (default registry.nanocloud.io:5000)
#
# Each is `fluxor build --emit=workload` (from the project its modules belong
# to), `fluxor publish workload` into the local store as <app>:1, then
# `fluxor store push` to <registry>/nanocloud/<app>:1, trusting the cluster CA
# (NANOCLOUD_REGISTRY_CA, default /var/lib/nanocloud.io/secure_assets/ca.crt).
# APPS_TAG (default 1) names the tag; APPS_KV_PORT / APPS_HTTP_PORT move the
# lattice and Chronicle listeners (a test's images, off the packaged ports).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QROOT="$(cd "$ROOT/../quantum" && pwd)"
REGISTRY="${1:-registry.nanocloud.io:5000}"
CA="${NANOCLOUD_REGISTRY_CA:-/var/lib/nanocloud.io/secure_assets/ca.crt}"
[ -r "$CA" ] || { echo "apps-publish: no registry CA at $CA" >&2; exit 1; }
TAG="${APPS_TAG:-1}"
W="$(mktemp -d /tmp/nc-apps-publish-XXXXXX)"
trap 'rm -rf "$W"' EXIT

publish() { # publish <app> <graph.yaml> <project-dir>
  (cd "$3" && fluxor build "$2" --emit=workload -o "$W/$1") >"$W/$1.log" 2>&1 \
    || { cat "$W/$1.log"; echo "apps-publish: $1 did not build" >&2; exit 1; }
  fluxor publish workload "$W/$1" --tag "$1:$TAG" >>"$W/$1.log" 2>&1 \
    || { cat "$W/$1.log"; echo "apps-publish: $1 did not publish" >&2; exit 1; }
  fluxor store push "$1:$TAG" "https://$REGISTRY/nanocloud/$1:$TAG" --ca "$CA" >>"$W/$1.log" 2>&1 \
    || { cat "$W/$1.log"; echo "apps-publish: $1 did not push" >&2; exit 1; }
  echo "   $1 -> $REGISTRY/nanocloud/$1:$TAG"
}

sed "s/listen_port: 6379/listen_port: ${APPS_KV_PORT:-6379}/" "$ROOT/packaging/apps/lattice-kv.yaml" >"$W/lattice-kv.yaml"
sed "s/port: 8080/port: ${APPS_HTTP_PORT:-8080}/" "$ROOT/packaging/apps/chronicle-http.yaml" >"$W/chronicle-http.yaml"
publish lattice-kv "$W/lattice-kv.yaml" "$ROOT"
publish chronicle-http "$W/chronicle-http.yaml" "$ROOT"
mkdir -p "$QROOT/target/nanocloud-apps"
python3 -B "$ROOT/scripts/support/to-workload.py" "$QROOT/examples/linux/minimal.yaml" \
  "$QROOT/target/nanocloud-apps/quantum-mqtt.yaml" --drop operations --drop http \
  "$QROOT/modules" "$QROOT/../clustor/modules" "$QROOT/../wave/modules" 2>/dev/null
publish quantum-mqtt "$QROOT/target/nanocloud-apps/quantum-mqtt.yaml" "$QROOT"
