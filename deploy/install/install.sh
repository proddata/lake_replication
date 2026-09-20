#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
pg_config=${PG_CONFIG:-pg_config}
destination_root=${DESTDIR:-}
worker_dir=/usr/local/bin

if ! command -v sha256sum >/dev/null 2>&1; then
    echo "lake-replication install: sha256sum is required to verify the bundle" >&2
    exit 1
fi
if ! (cd "$script_dir" && sha256sum -c --quiet SHA256SUMS); then
    echo "lake-replication install: bundle checksum verification failed" >&2
    exit 1
fi

if ! command -v "$pg_config" >/dev/null 2>&1; then
    echo "lake-replication install: pg_config was not found" >&2
    exit 1
fi

postgres_version=$($pg_config --version)
case "$postgres_version" in
    "PostgreSQL 18"*) ;;
    *)
        echo "lake-replication install: PostgreSQL 18 is required; found $postgres_version" >&2
        exit 1
        ;;
esac

shared_extension_dir=$($pg_config --sharedir)/extension
library_dir=$($pg_config --pkglibdir)
if [ ! -f "$shared_extension_dir/pg_lake.control" ]; then
    echo "lake-replication install: pg_lake must be installed before this bundle" >&2
    exit 1
fi
if [ ! -f "$shared_extension_dir/dblink.control" ]; then
    echo "lake-replication install: the PostgreSQL dblink extension is required" >&2
    exit 1
fi

case "$(cat "$script_dir/TARGETARCH")" in
    amd64) expected_machine=x86_64 ;;
    arm64) expected_machine=aarch64 ;;
    *)
        echo "lake-replication install: unsupported bundle architecture" >&2
        exit 1
        ;;
esac
if [ "$(uname -m)" != "$expected_machine" ]; then
    echo "lake-replication install: bundle architecture $expected_machine does not match $(uname -m)" >&2
    exit 1
fi

install -d "$destination_root$worker_dir"
install -d "$destination_root$shared_extension_dir"
install -d "$destination_root$library_dir"
for binary in "$script_dir"/payload/bin/lake-replication-*; do
    install -m 0755 "$binary" "$destination_root$worker_dir/$(basename -- "$binary")"
done
install -m 0755 "$script_dir/payload/lib/lake_subscription_sugar.so" \
    "$destination_root$library_dir/lake_subscription_sugar.so"
for extension_file in "$script_dir"/payload/extension/*; do
    install -m 0644 "$extension_file" \
        "$destination_root$shared_extension_dir/$(basename -- "$extension_file")"
done

cat <<EOF
Installed pg_lake_replication $(cat "$script_dir/VERSION") for $postgres_version
from source revision $(cat "$script_dir/SOURCE_REVISION").

Before creating subscriptions:
1. Add lake_subscription_sugar to shared_preload_libraries.
2. Set lake_subscription_sugar.database_name to the destination database.
3. Restart PostgreSQL.
4. Run CREATE EXTENSION lake_replication; and CREATE EXTENSION lake_subscription_sugar;

Removing these files later does not remove database objects. Use PostgreSQL's
DROP EXTENSION commands separately only when you intend to delete them.
EOF
