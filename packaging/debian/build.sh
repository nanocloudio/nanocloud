#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
TARGET_DIR="${REPO_ROOT}/target/debian"

detect_arch() {
    if command -v dpkg >/dev/null 2>&1; then
        dpkg --print-architecture
        return
    fi

    case "$(uname -m)" in
        x86_64) echo amd64 ;;
        aarch64) echo arm64 ;;
        armv7l) echo armhf ;;
        *) echo "unknown" ;;
    esac
}

VERSION=$(grep '^version = "' "${REPO_ROOT}/Cargo.toml" | head -n1 | cut -d '"' -f2)
ARCH=$(detect_arch)

if [[ "${ARCH}" == "unknown" ]]; then
    echo "Unsupported architecture: $(uname -m)" >&2
    exit 1
fi

STAGING_ROOT="${TARGET_DIR}/nanocloud_${VERSION}_${ARCH}"
DEBIAN_DIR="${STAGING_ROOT}/DEBIAN"

rm -rf "${STAGING_ROOT}"
mkdir -p "${DEBIAN_DIR}"

if [[ ! -f "${REPO_ROOT}/target/release/nanocloud" ]]; then
    echo "Missing release binary at target/release/nanocloud. Build it before packaging." >&2
    exit 1
fi

echo "==> Staging filesystem"
install -Dm755 "${REPO_ROOT}/target/release/nanocloud" "${STAGING_ROOT}/usr/bin/nanocloud"
install -Dm644 "${SCRIPT_DIR}/nanocloud.service" "${STAGING_ROOT}/lib/systemd/system/nanocloud.service"
install -Dm644 "${SCRIPT_DIR}/nanocloud.default" "${STAGING_ROOT}/etc/default/nanocloud"

install -Dm755 "${SCRIPT_DIR}/postinst" "${DEBIAN_DIR}/postinst"
install -Dm755 "${SCRIPT_DIR}/prerm" "${DEBIAN_DIR}/prerm"
install -Dm755 "${SCRIPT_DIR}/postrm" "${DEBIAN_DIR}/postrm"
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
Description: Tools and service for managing a Nanocloud deployment
 Nanocloud provides container lifecycle management tailored for
 personal or edge clusters. This package installs the nanocloud CLI
 and configures it to run as a systemd-managed API server.
EOF

OUTPUT_DEB="${TARGET_DIR}/nanocloud_${VERSION}_${ARCH}.deb"

echo "==> Building Debian package at ${OUTPUT_DEB}"
dpkg-deb --build --root-owner-group "${STAGING_ROOT}" "${OUTPUT_DEB}"

echo "Package created: ${OUTPUT_DEB}"
