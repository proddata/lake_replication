#!/bin/sh
set -eu

pg_config=${PG_CONFIG:-pg_config}
destination_root=${DESTDIR:-}
worker_dir=/usr/local/bin

if ! command -v "$pg_config" >/dev/null 2>&1; then
    echo "lake-replication uninstall: pg_config was not found" >&2
    exit 1
fi

postgres_version=$($pg_config --version)
case "$postgres_version" in
    "PostgreSQL 18"*) ;;
    *)
        echo "lake-replication uninstall: PostgreSQL 18 is required; found $postgres_version" >&2
        exit 1
        ;;
esac

shared_extension_dir=$($pg_config --sharedir)/extension
library_dir=$($pg_config --pkglibdir)

for file in \
    "$destination_root$worker_dir/lake-replication-receiver" \
    "$destination_root$worker_dir/lake-replication-publisher" \
    "$destination_root$worker_dir/lake-replication-initial-sync" \
    "$destination_root$worker_dir/lake-replication-supervisor" \
    "$destination_root$library_dir/lake_replication.so" \
    "$destination_root$library_dir/lake_subscription_sugar.so" \
    "$destination_root$shared_extension_dir/lake_subscription_sugar.control" \
    "$destination_root$shared_extension_dir/lake_subscription_sugar--1.0.sql" \
    "$destination_root$shared_extension_dir/lake_subscription_sugar--1.1.sql" \
    "$destination_root$shared_extension_dir/lake_subscription_sugar--1.0--1.1.sql" \
    "$destination_root$shared_extension_dir/lake_replication.control" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0-beta.1.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0--0.1.0-beta.1.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0-beta.2.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0--0.1.0-beta.2.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0-beta.1--0.1.0-beta.2.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0-beta.3.sql" \
    "$destination_root$shared_extension_dir/lake_replication--0.1.0-beta.2--0.1.0-beta.3.sql"
do
    if [ -e "$file" ]; then
        rm -f -- "$file"
        echo "Removed $file"
    fi
done

cat <<'EOF'
Removed lake_replication program and extension files.

PostgreSQL configuration, roles, extension objects, replication slots, Iceberg
tables, object-storage data, and credential/log files were not changed. Stop or
drop subscriptions and remove lake_replication (and the legacy
lake_subscription_sugar name, if present) from preload settings
before restarting PostgreSQL without the shared library. Use DROP EXTENSION
only when you separately intend to remove database objects.
EOF
