#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
output_dir="${1:-"${repo_root}/target/schema"}"
bundle_src="${repo_root}/docs/schema/bundle"

if [[ ! -d "${bundle_src}" ]]; then
    echo "Bundle schema directory '${bundle_src}' not found" >&2
    exit 1
fi

rm -rf "${output_dir}"
mkdir -p "${output_dir}"

cp "${bundle_src}/manifest.json" "${output_dir}/manifest.json"
cp "${bundle_src}/v1alpha1.json" "${output_dir}/v1alpha1.json"

(
    cd "${output_dir}"
    sha256sum manifest.json > manifest.json.sha256
    sha256sum v1alpha1.json > v1alpha1.json.sha256
    tar czf bundle-schema-v1alpha1.tar.gz manifest.json v1alpha1.json
)

echo "Bundle schema artifacts written to ${output_dir}"
