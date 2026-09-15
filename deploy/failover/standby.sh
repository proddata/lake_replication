#!/usr/bin/env bash
set -euo pipefail

if [[ ! -s "${PGDATA}/PG_VERSION" ]]; then
  if [[ -n "$(find "${PGDATA}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "standby data directory is non-empty but uninitialized" >&2
    exit 1
  fi
  chmod 700 "${PGDATA}"
  pg_basebackup \
    --host=primary \
    --username=postgres \
    --pgdata="${PGDATA}" \
    --write-recovery-conf \
    --wal-method=stream \
    --slot=ha_standby
  printf "\nprimary_conninfo = 'host=primary port=5432 user=postgres dbname=application application_name=ha_standby'\n" >> "${PGDATA}/postgresql.auto.conf"
  printf "primary_slot_name = 'ha_standby'\n" >> "${PGDATA}/postgresql.auto.conf"
fi

exec postgres \
  -c wal_level=logical \
  -c max_replication_slots=16 \
  -c max_wal_senders=16 \
  -c hot_standby_feedback=on \
  -c sync_replication_slots=on
