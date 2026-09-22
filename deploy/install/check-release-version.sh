#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
version=${1#v}

if [ -z "$version" ] || [ "$version" = "$1" ]; then
    echo "usage: $0 vMAJOR.MINOR.PATCH[-PRERELEASE]" >&2
    exit 2
fi

cargo_version=$(awk '
    /^\[package\]$/ { package = 1; next }
    /^\[/ { package = 0 }
    package && /^version = / { gsub(/[" ]/, "", $3); print $3; exit }
' "$root/worker/Cargo.toml")
extension_version=$(sed -n "s/^default_version = '\([^']*\)'/\1/p" \
    "$root/extensions/lake_replication/lake_replication.control")

if [ "$version" != "$cargo_version" ] || [ "$version" != "$extension_version" ]; then
    echo "release version mismatch: tag=$version worker=$cargo_version extension=$extension_version" >&2
    exit 1
fi

echo "release version $version matches worker and extension metadata"
