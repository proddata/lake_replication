#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 OUTPUT" >&2
    exit 2
fi

output=$1
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
temporary="$output.tmp"
trap 'rm -f "$temporary"' EXIT HUP INT TERM

{
    cat "$script_dir/lake_subscription_sugar--1.0.sql"
    printf '%s\n' \
        "" \
        "SELECT pg_catalog.pg_extension_config_dump(" \
        "    'lake_subscription_sugar.pending_cleanup', '');" \
        "SELECT pg_catalog.pg_extension_config_dump(" \
        "    'lake_subscription_sugar.worker_launch_config', '');"
} >"$temporary"

mv "$temporary" "$output"
trap - EXIT HUP INT TERM
