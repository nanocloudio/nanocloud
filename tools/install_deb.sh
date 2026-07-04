#!/usr/bin/env bash
# Install the newest built Debian package (target/debian/nanocloud_*.deb)
# and restart the nanocloud service if it is currently running.
# Formerly `make install`; build the package first with
# packaging/debian/build.sh.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
deb_dir="${repo_root}/target/debian"
service_name=nanocloud

deb_file="$(ls -1t "${deb_dir}"/nanocloud_*.deb 2>/dev/null | head -n1 || true)"
if [[ -z "${deb_file}" ]]; then
    echo "No Debian package found in ${deb_dir}" >&2
    exit 1
fi

echo "Installing ${deb_file}"
sudo dpkg -i "${deb_file}"

if systemctl is-active --quiet "${service_name}"; then
    echo "Restarting ${service_name} service"
    sudo systemctl restart "${service_name}"
else
    echo "${service_name} service not running; skipping restart"
fi
