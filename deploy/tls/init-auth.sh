#!/bin/bash
set -euo pipefail

psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=source_password="$TLS_LAB_SOURCE_PASSWORD" <<'SQL'
CREATE ROLE lake_source LOGIN REPLICATION PASSWORD :'source_password';
GRANT CONNECT ON DATABASE application TO lake_source;
GRANT USAGE ON SCHEMA public TO lake_source;
SQL

sed -i '1ihostnossl all all all reject' "$PGDATA/pg_hba.conf"
