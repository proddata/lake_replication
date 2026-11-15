#!/bin/bash
# Fault fixture only: keep the private WAL tmpfs mounted after a server PANIC.
set -eu
/usr/local/bin/docker-entrypoint.sh "$@" &
database_pid=$!
trap 'kill -TERM "$database_pid" 2>/dev/null || true; wait "$database_pid" || true; exit 0' TERM INT
set +e
wait "$database_pid"
status=$?
set -e
echo "WAL fault fixture: database process exited with status $status; awaiting repair"
while true; do sleep 3600 & wait $!; done
