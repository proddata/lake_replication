\echo Use "CREATE EXTENSION lake_subscription_sugar" to load this file. \quit

-- Force the ProcessUtility_hook active in the session that just ran
-- CREATE EXTENSION, without waiting for a fresh connection to pick up
-- session_preload_libraries (see the operator note in
-- docs/native-subscription-spike.md for making that automatic for every
-- future session too).
LOAD 'lake_subscription_sugar';

-- Owned by this extension; DROP EXTENSION lake_subscription_sugar drops
-- this table too. Deliberately separate from lake_replication's own
-- shipped schema, since this state exists only to support this
-- prototype's DROP SUBSCRIPTION handling (dropping the source-side
-- replication slot for real), not the replication path itself.
CREATE TABLE pending_cleanup (
    subscription_id bigint PRIMARY KEY,
    source_conninfo text NOT NULL,
    source_slot_name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE pending_cleanup IS
    'Source conninfo/slot name recorded at CREATE SUBSCRIPTION time so '
    'DROP SUBSCRIPTION can drop the source-side replication slot for '
    'real; DropSubscriptionStmt carries no conninfo of its own.';
