#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
version=${VERSION:-0.1.0-beta.3}
output=${1:-$root/dist}
platform=${PLATFORM:-}
revision=${REVISION:-$(git -C "$root" rev-parse HEAD 2>/dev/null || printf unknown)}
if [ "$revision" != unknown ] \
   && [ -n "$(git -C "$root" status --porcelain --untracked-files=normal)" ]; then
    revision="$revision-dirty"
fi

mkdir -p "$output"
if [ -n "$platform" ]; then
    docker build \
        --platform "$platform" \
        --file "$script_dir/Dockerfile" \
        --target bundle \
        --build-arg "VERSION=$version" \
        --build-arg "REVISION=$revision" \
        --output "type=local,dest=$output" \
        "$root"
else
    docker build \
        --file "$script_dir/Dockerfile" \
        --target bundle \
        --build-arg "VERSION=$version" \
        --build-arg "REVISION=$revision" \
        --output "type=local,dest=$output" \
        "$root"
fi

echo "Bundle created at $output/lake_replication-$version.tar.gz"
