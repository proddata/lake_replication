#!/usr/bin/env bash
set -euo pipefail

exec pgduck_server --unix_socket_directory /var/run/pgduck \
  --port 5332 --memory_limit 1GB --cache_dir /tmp/pgduck-cache \
  --init_file_path /qualification/pgduck.sql
