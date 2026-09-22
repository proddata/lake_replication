#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
version=${VERSION:-0.1.0}
output=${1:-$root/dist/release}
revision=${REVISION:-$(git -C "$root" rev-parse HEAD)}

if [ -n "$(git -C "$root" status --porcelain --untracked-files=normal)" ] \
   && [ "${ALLOW_DIRTY:-0}" != 1 ]; then
    echo "release build refused: source tree is dirty (set ALLOW_DIRTY=1 for a non-release test)" >&2
    exit 1
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
mkdir -p "$output"

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

for arch in amd64 arm64; do
    stage="$work/$arch"
    mkdir -p "$stage"
    PLATFORM="linux/$arch" VERSION="$version" REVISION="$revision" \
        "$script_dir/build-bundle.sh" "$stage"
    source_archive="$stage/pg_lake_replication-$version.tar.gz"
    artifact="pg_lake_replication-$version-linux-$arch.tar.gz"
    cp "$source_archive" "$output/$artifact"
    digest=$(sha256_file "$output/$artifact")
    printf '%s  %s\n' "$digest" "$artifact" >"$output/$artifact.sha256"
done

echo "Release bundles created in $output"
