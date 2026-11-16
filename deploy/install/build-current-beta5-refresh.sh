#!/bin/sh
# Cumulative same-version development refresh; ordinary extension upgrades stay frozen.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=$(CDPATH= cd -- "$script_dir/../.." && pwd)
cat <<'SQL'
BEGIN;
DO $$ BEGIN
    IF EXISTS (SELECT FROM lake_replication.worker_launch_config WHERE desired_state <> 'stopped')
       OR EXISTS (SELECT FROM pg_stat_activity WHERE datname=current_database()
                  AND backend_type='lake_replication supervisor') THEN
        RAISE EXCEPTION 'stop subscriptions before applying the current beta5 refresh'
            USING ERRCODE='55000';
    END IF;
END $$;
SQL
for migration in 0030_update_key_filter 0031_jsonb_wire_values 0032_independent_insert_groups 0033_delete_key_filter 0034_update_row_conversion; do
    sed '1d;$d' "$root/migrations/$migration.sql" |
        sed 's/CREATE FUNCTION lake_replication.lock_insert_group_targets/CREATE OR REPLACE FUNCTION lake_replication.lock_insert_group_targets/;s@MODULE_PATHNAME@$libdir/lake_replication@g'
done
cat <<'SQL'
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_depend d JOIN pg_extension e ON e.oid=d.refobjid
        WHERE d.classid='pg_proc'::regclass
          AND d.objid='lake_replication.lock_insert_group_targets(regclass[])'::regprocedure
          AND d.refclassid='pg_extension'::regclass
          AND e.extname='lake_replication' AND d.deptype='e'
    ) THEN
        ALTER EXTENSION lake_replication ADD FUNCTION lake_replication.lock_insert_group_targets(regclass[]);
    END IF;
END $$;
COMMIT;
SQL
