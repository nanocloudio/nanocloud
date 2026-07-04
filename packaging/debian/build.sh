#!/usr/bin/env bash
# Build the nanocloud Debian package — a fluxor-only artifact. There is NO host
# binary: nanocloud IS the fluxor CLI + runtime plus the nanocloud PIC fmods and
# the graphs that compose them. `/usr/bin/nanocloud` is a busybox-style symlink
# to `fluxor`, so `nanocloud <cmd>` runs the `nanocloud_cli` applet fmod
# (`fluxor exec nanocloud -- <cmd>`); the control plane runs as
# `fluxor run controlplane.yaml` under systemd.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
TARGET_DIR="${REPO_ROOT}/target/debian"

detect_arch() {
    if command -v dpkg >/dev/null 2>&1; then dpkg --print-architecture; return; fi
    case "$(uname -m)" in
        x86_64) echo amd64 ;; aarch64) echo arm64 ;; armv7l) echo armhf ;; *) echo unknown ;;
    esac
}

# Version from fluxor.toml (there is no Cargo.toml).
VERSION=$(grep '^version' "${REPO_ROOT}/fluxor.toml" | head -n1 | cut -d '"' -f2)
ARCH=$(detect_arch)
[[ "${ARCH}" != "unknown" ]] || { echo "Unsupported architecture: $(uname -m)" >&2; exit 1; }

STAGING_ROOT="${TARGET_DIR}/nanocloud_${VERSION}_${ARCH}"
DEBIAN_DIR="${STAGING_ROOT}/DEBIAN"
FXROOT="${STAGING_ROOT}/usr/lib/nanocloud/fluxor"
SILICON=bcm2712
FLUXOR_TARGET=aarch64-unknown-linux-gnu

rm -rf "${STAGING_ROOT}"
mkdir -p "${DEBIAN_DIR}"

# ── fluxor CLI (sibling checkout — bootstrap-only) + synced-tree artefacts ──
# The runtime binary and the foundation fmods come from THIS project's tree,
# materialised by `fluxor sync` (standards/fluxor-modules.md) — never
# from the fluxor checkout or ~/.fluxor/registry. The checkout is still the
# source of the CLI binary and the project scaffolding (fluxor.toml, stacks,
# targets, module manifests) staged below.
FLUXOR_DIR="${FLUXOR_DIR:-${REPO_ROOT}/../fluxor}"
FLUXOR_CLI="${FLUXOR_DIR}/target/${FLUXOR_TARGET}/release/fluxor"
FLUXOR_RUNTIME="${REPO_ROOT}/target/${FLUXOR_TARGET}/release/fluxor-linux"
NANO_MODULES="${REPO_ROOT}/target/fluxor/${SILICON}/modules"
FLUXOR_MODULES="${NANO_MODULES}"

for f in "${FLUXOR_CLI}" "${FLUXOR_RUNTIME}" "${FLUXOR_DIR}/fluxor.toml" \
         "${FLUXOR_DIR}/stacks" "${FLUXOR_DIR}/targets" "${NANO_MODULES}/api_ingress.fmod"; do
    [[ -e "$f" ]] || { echo "missing artifact: $f" >&2
        echo "  build the fluxor release CLI, then 'fluxor sync' and 'fluxor modules build --target ${SILICON}' here" >&2; exit 1; }
done

echo "==> Staging fluxor CLI + runtime"
install -Dm755 "${FLUXOR_CLI}"     "${STAGING_ROOT}/usr/bin/fluxor"
install -Dm755 "${FLUXOR_RUNTIME}" "${FXROOT}/target/${FLUXOR_TARGET}/release/fluxor-linux"
strip --strip-debug "${STAGING_ROOT}/usr/bin/fluxor" \
    "${FXROOT}/target/${FLUXOR_TARGET}/release/fluxor-linux" 2>/dev/null || true

# `nanocloud` is a busybox multi-call symlink to fluxor: argv[0]=nanocloud →
# `fluxor exec nanocloud` (the nanocloud_cli applet fmod).
ln -sf fluxor "${STAGING_ROOT}/usr/bin/nanocloud"

echo "==> Staging the fluxor project (palette fmods + scaffolding)"
mkdir -p "${FXROOT}/target/fluxor/${SILICON}/modules"
cp "${FLUXOR_MODULES}"/*.fmod "${FXROOT}/target/fluxor/${SILICON}/modules/"
install -Dm644 "${FLUXOR_DIR}/fluxor.toml" "${FXROOT}/fluxor.toml"
cp -r "${FLUXOR_DIR}/stacks" "${FLUXOR_DIR}/targets" "${FXROOT}/"
(cd "${FLUXOR_DIR}" && find modules -name manifest.toml -exec install -Dm644 {} "${FXROOT}/{}" \;)

echo "==> Staging nanocloud fmods (the apiserver, pipeline, reconcilers, CLI)"
for MODDIR in "${REPO_ROOT}"/modules/app/*/; do
    NAME=$(basename "${MODDIR}")
    # Shared include-only dirs (e.g. _shared/json.rs) carry no manifest and are
    # not modules — skip anything without a manifest.toml.
    [[ -f "${MODDIR}manifest.toml" ]] || continue
    FMOD="${NANO_MODULES}/${NAME}.fmod"
    [[ -f "${FMOD}" ]] || { echo "nanocloud module missing: ${FMOD}" >&2
        echo "  run: fluxor modules build --target ${SILICON}" >&2; exit 1; }
    install -Dm644 "${FMOD}" "${FXROOT}/target/fluxor/${SILICON}/modules/${NAME}.fmod"
    install -Dm644 "${MODDIR}manifest.toml" "${FXROOT}/modules/app/${NAME}/manifest.toml"
done

echo "==> Staging graphs + service files"
# The control-plane graph the systemd service runs (apiserver + reconcilers).
install -Dm644 "${SCRIPT_DIR}/fluxor-controlplane.yaml" "${STAGING_ROOT}/etc/nanocloud.io/fluxor/controlplane.yaml"
# The node graph (dataplane workload substrate).
install -Dm644 "${SCRIPT_DIR}/fluxor-node.yaml" "${STAGING_ROOT}/etc/nanocloud.io/fluxor/node.yaml"
# The image plane (pod-image pull → assemble → run).
install -Dm644 "${SCRIPT_DIR}/fluxor-image-fetch.yaml" "${STAGING_ROOT}/etc/nanocloud.io/fluxor/image-plane.yaml"
install -Dm644 "${SCRIPT_DIR}/nanocloud.service" "${STAGING_ROOT}/lib/systemd/system/nanocloud.service"
install -Dm644 "${SCRIPT_DIR}/nanocloud.default" "${STAGING_ROOT}/etc/default/nanocloud"

# The `nanocloud` CLI applet bundle source (postinst runs `fluxor install`) + the
# profile snippet that points interactive shells at the system applet registry.
install -Dm644 "${REPO_ROOT}/packaging/cli/workload.toml" "${STAGING_ROOT}/usr/share/nanocloud/fluxor/cli/app.fluxor.toml"
install -Dm644 "${REPO_ROOT}/packaging/cli/linux.yaml"       "${STAGING_ROOT}/usr/share/nanocloud/fluxor/cli/linux.yaml"
install -Dm644 "${SCRIPT_DIR}/nanocloud-profile.sh"       "${STAGING_ROOT}/etc/profile.d/nanocloud.sh"

# The image-assembly effect script: run as a host workload by sandbox_runner
# when image_assembler projects an assembly job.
install -Dm755 "${SCRIPT_DIR}/assemble-image.sh"          "${STAGING_ROOT}/usr/lib/nanocloud/assemble-image.sh"

# Dataplane system-service bundle sources (dns/edge/sandbox/webhook/route/
# netpolicy/proxy) — postinst publishes these into the node's OCI store so the
# node can run them as fluxor workloads.
for pair in dns edge endpoints sandbox webhook route netpolicy proxy; do
    APP="${SCRIPT_DIR}/fluxor-${pair}-app.fluxor.toml"
    GRAPH="${SCRIPT_DIR}/fluxor-${pair}.yaml"
    [[ -f "${APP}" ]]   && install -Dm644 "${APP}"   "${STAGING_ROOT}/usr/share/nanocloud/fluxor/${pair}/app.fluxor.toml"
    [[ -f "${GRAPH}" ]] && install -Dm644 "${GRAPH}" "${STAGING_ROOT}/usr/share/nanocloud/fluxor/${pair}/linux.yaml"
done
# DNS's linux graph is the node graph, which carries the dns subgraph.
install -Dm644 "${SCRIPT_DIR}/fluxor-node.yaml" "${STAGING_ROOT}/usr/share/nanocloud/fluxor/dns/linux.yaml"

echo "==> Staging maintainer scripts"
install -Dm755 "${SCRIPT_DIR}/postinst" "${DEBIAN_DIR}/postinst"
install -Dm755 "${SCRIPT_DIR}/prerm"    "${DEBIAN_DIR}/prerm"
install -Dm755 "${SCRIPT_DIR}/postrm"   "${DEBIAN_DIR}/postrm"
install -Dm644 "${SCRIPT_DIR}/conffiles" "${DEBIAN_DIR}/conffiles"
install -Dm644 "${SCRIPT_DIR}/nanocloud.logrotate" "${STAGING_ROOT}/etc/logrotate.d/nanocloud"

INSTALLED_SIZE=$(du -sk "${STAGING_ROOT}" | cut -f1)
cat >"${DEBIAN_DIR}/control" <<EOF
Package: nanocloud
Version: ${VERSION}
Section: admin
Priority: optional
Architecture: ${ARCH}
Maintainer: Nanocloud Authors <support@nanocloud.io>
Depends: systemd (>= 245), ca-certificates
Installed-Size: ${INSTALLED_SIZE}
Description: Nanocloud — a fluxor-native Kubernetes control plane
 Nanocloud is a Kubernetes control plane composed entirely of fluxor PIC
 modules: the apiserver, the request pipeline, and every reconciler run as
 cooperating fmods in a single fluxor runtime. This package installs the
 fluxor CLI + runtime, the nanocloud modules and graphs, and a systemd unit
 that runs the control-plane graph.
EOF

OUTPUT_DEB="${TARGET_DIR}/nanocloud_${VERSION}_${ARCH}.deb"
echo "==> Building Debian package at ${OUTPUT_DEB}"
dpkg-deb --build --root-owner-group "${STAGING_ROOT}" "${OUTPUT_DEB}"
echo "Package created: ${OUTPUT_DEB}"
