#!/bin/sh
set -eu

psql -X -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    --set=source_password="$QUAL_SOURCE_PASSWORD" <<'SQL'
CREATE ROLE lake_source LOGIN REPLICATION PASSWORD :'source_password';
GRANT CONNECT ON DATABASE application TO lake_source;

CREATE TABLE public.orders (
    id bigint PRIMARY KEY,
    note text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO public.orders (id, note) VALUES (1, 'initial');
CREATE PUBLICATION orders_pub FOR TABLE public.orders;
GRANT USAGE ON SCHEMA public TO lake_source;
GRANT SELECT ON public.orders TO lake_source;
SQL
