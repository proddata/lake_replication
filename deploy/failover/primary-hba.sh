#!/usr/bin/env bash
set -euo pipefail

printf 'host replication all all trust\n' >> "${PGDATA}/pg_hba.conf"
