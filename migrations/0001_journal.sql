BEGIN;

CREATE SCHEMA lake_replication;

CREATE TABLE lake_replication.sources (
    source_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    system_identifier numeric(20, 0) NOT NULL,
    database_oid oid NOT NULL,
    timeline bigint NOT NULL CHECK (timeline > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (system_identifier, database_oid)
);

CREATE TABLE lake_replication.subscriptions (
    subscription_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id bigint NOT NULL REFERENCES lake_replication.sources,
    subscription_name name NOT NULL UNIQUE,
    slot_name name NOT NULL,
    publication_names text[] NOT NULL CHECK (cardinality(publication_names) > 0),
    state text NOT NULL DEFAULT 'disabled'
        CHECK (state IN ('disabled', 'streaming', 'paused', 'error')),
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_id, slot_name)
);

CREATE TABLE lake_replication.relations (
    subscription_id bigint NOT NULL REFERENCES lake_replication.subscriptions,
    remote_relation_id oid NOT NULL,
    schema_version bigint NOT NULL CHECK (schema_version > 0),
    source_namespace name NOT NULL,
    source_relation name NOT NULL,
    replica_identity "char" NOT NULL,
    columns jsonb NOT NULL CHECK (jsonb_typeof(columns) = 'array'),
    destination_relation regclass,
    state text NOT NULL DEFAULT 'ready'
        CHECK (state IN ('copying', 'ready', 'paused', 'error')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subscription_id, remote_relation_id, schema_version)
);

CREATE TABLE lake_replication.checkpoints (
    subscription_id bigint PRIMARY KEY REFERENCES lake_replication.subscriptions,
    received_lsn pg_lsn NOT NULL DEFAULT '0/0',
    staged_lsn pg_lsn NOT NULL DEFAULT '0/0',
    published_lsn pg_lsn NOT NULL DEFAULT '0/0',
    acknowledged_lsn pg_lsn NOT NULL DEFAULT '0/0',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (received_lsn >= staged_lsn),
    CHECK (staged_lsn >= published_lsn),
    CHECK (staged_lsn >= acknowledged_lsn)
);

CREATE TABLE lake_replication.batches (
    batch_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id bigint NOT NULL REFERENCES lake_replication.subscriptions,
    batch_key text NOT NULL,
    first_end_lsn pg_lsn NOT NULL,
    last_end_lsn pg_lsn NOT NULL,
    state text NOT NULL DEFAULT 'publishing'
        CHECK (state IN ('publishing', 'published')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    CHECK (first_end_lsn <= last_end_lsn),
    UNIQUE (subscription_id, batch_key),
    UNIQUE (subscription_id, last_end_lsn)
);

CREATE TABLE lake_replication.transactions (
    transaction_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription_id bigint NOT NULL REFERENCES lake_replication.subscriptions,
    xid bigint NOT NULL CHECK (xid BETWEEN 0 AND 4294967295),
    begin_lsn pg_lsn NOT NULL,
    commit_lsn pg_lsn NOT NULL,
    end_lsn pg_lsn NOT NULL,
    commit_time timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'array'),
    state text NOT NULL DEFAULT 'staged'
        CHECK (state IN ('staged', 'publishing', 'published')),
    batch_id bigint REFERENCES lake_replication.batches,
    staged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    CHECK (begin_lsn <= end_lsn),
    CHECK (commit_lsn <= end_lsn),
    UNIQUE (subscription_id, commit_lsn)
);

CREATE TABLE lake_replication.changes (
    transaction_id bigint NOT NULL
        REFERENCES lake_replication.transactions ON DELETE CASCADE,
    change_index integer NOT NULL CHECK (change_index >= 0),
    remote_relation_id oid NOT NULL,
    relation_schema_version bigint NOT NULL CHECK (relation_schema_version > 0),
    operation text NOT NULL CHECK (operation = 'insert'),
    new_tuple jsonb NOT NULL CHECK (jsonb_typeof(new_tuple) = 'array'),
    PRIMARY KEY (transaction_id, change_index)
);

CREATE INDEX transactions_unpublished_idx
    ON lake_replication.transactions (subscription_id, end_lsn)
    WHERE state = 'staged';

CREATE OR REPLACE FUNCTION lake_replication.create_subscription(
    p_system_identifier numeric,
    p_database_oid oid,
    p_timeline bigint,
    p_subscription_name name,
    p_slot_name name,
    p_publication_names text[]
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_source_id bigint;
    v_subscription_id bigint;
BEGIN
    INSERT INTO sources (system_identifier, database_oid, timeline)
    VALUES (p_system_identifier, p_database_oid, p_timeline)
    ON CONFLICT (system_identifier, database_oid) DO UPDATE
      SET timeline = EXCLUDED.timeline
    RETURNING source_id INTO v_source_id;

    INSERT INTO subscriptions (
        source_id, subscription_name, slot_name, publication_names
    ) VALUES (
        v_source_id, p_subscription_name, p_slot_name, p_publication_names
    )
    RETURNING subscription_id INTO v_subscription_id;

    INSERT INTO checkpoints (subscription_id) VALUES (v_subscription_id);
    RETURN v_subscription_id;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.stage_transaction(
    p_subscription_id bigint,
    p_xid bigint,
    p_begin_lsn pg_lsn,
    p_commit_lsn pg_lsn,
    p_end_lsn pg_lsn,
    p_commit_time timestamptz,
    p_changes jsonb
) RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_transaction_id bigint;
    v_existing transactions%ROWTYPE;
    v_staged_lsn pg_lsn;
BEGIN
    IF jsonb_typeof(p_changes) IS DISTINCT FROM 'array' OR jsonb_array_length(p_changes) = 0 THEN
        RAISE EXCEPTION 'changes must be a non-empty JSON array' USING ERRCODE = '22023';
    END IF;
    IF p_begin_lsn > p_end_lsn OR p_commit_lsn > p_end_lsn THEN
        RAISE EXCEPTION 'invalid transaction LSN interval' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_changes) AS change
        WHERE change->>'operation' IS DISTINCT FROM 'insert'
           OR jsonb_typeof(change->'remote_relation_id') IS DISTINCT FROM 'number'
           OR jsonb_typeof(change->'relation_schema_version') IS DISTINCT FROM 'number'
           OR jsonb_typeof(change->'new_tuple') IS DISTINCT FROM 'array'
    ) THEN
        RAISE EXCEPTION 'invalid or non-insert change in insert-only journal' USING ERRCODE = '22023';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM jsonb_array_elements(p_changes) AS change
          LEFT JOIN relations r
            ON r.subscription_id = p_subscription_id
           AND r.remote_relation_id = (change->>'remote_relation_id')::oid
           AND r.schema_version = (change->>'relation_schema_version')::bigint
         WHERE r.subscription_id IS NULL
            OR jsonb_array_length(change->'new_tuple') <> jsonb_array_length(r.columns)
    ) THEN
        RAISE EXCEPTION 'change references an unknown relation version or has the wrong tuple width'
            USING ERRCODE = '22023';
    END IF;

    SELECT staged_lsn INTO STRICT v_staged_lsn
      FROM checkpoints WHERE subscription_id = p_subscription_id FOR UPDATE;

    SELECT * INTO v_existing FROM transactions
     WHERE subscription_id = p_subscription_id AND commit_lsn = p_commit_lsn;
    IF FOUND THEN
        IF v_existing.xid <> p_xid
           OR v_existing.begin_lsn <> p_begin_lsn
           OR v_existing.end_lsn <> p_end_lsn
           OR v_existing.commit_time <> p_commit_time
           OR v_existing.payload <> p_changes THEN
            RAISE EXCEPTION 'conflicting replay at commit LSN %', p_commit_lsn
                USING ERRCODE = '23505';
        END IF;
        RETURN false;
    END IF;

    IF p_end_lsn <= v_staged_lsn THEN
        RAISE EXCEPTION 'new transaction end LSN % does not advance staged LSN %',
            p_end_lsn, v_staged_lsn USING ERRCODE = '22023';
    END IF;

    INSERT INTO transactions (
        subscription_id, xid, begin_lsn, commit_lsn, end_lsn, commit_time, payload
    ) VALUES (
        p_subscription_id, p_xid, p_begin_lsn, p_commit_lsn, p_end_lsn,
        p_commit_time, p_changes
    ) RETURNING transaction_id INTO v_transaction_id;

    INSERT INTO changes (
        transaction_id, change_index, remote_relation_id,
        relation_schema_version, operation, new_tuple
    )
    SELECT v_transaction_id,
           ordinality::integer - 1,
           (change->>'remote_relation_id')::oid,
           (change->>'relation_schema_version')::bigint,
           change->>'operation',
           change->'new_tuple'
      FROM jsonb_array_elements(p_changes) WITH ORDINALITY AS item(change, ordinality);

    UPDATE checkpoints
       SET received_lsn = p_end_lsn,
           staged_lsn = p_end_lsn,
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.prepare_batch(
    p_subscription_id bigint,
    p_batch_key text,
    p_first_end_lsn pg_lsn,
    p_last_end_lsn pg_lsn
) RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_batch batches%ROWTYPE;
    v_staged_lsn pg_lsn;
BEGIN
    SELECT staged_lsn INTO STRICT v_staged_lsn
      FROM checkpoints WHERE subscription_id = p_subscription_id FOR UPDATE;
    IF p_last_end_lsn > v_staged_lsn THEN
        RAISE EXCEPTION 'batch end LSN % exceeds staged LSN %', p_last_end_lsn, v_staged_lsn
            USING ERRCODE = '22023';
    END IF;

    SELECT * INTO v_batch FROM batches
     WHERE subscription_id = p_subscription_id AND batch_key = p_batch_key;
    IF FOUND THEN
        IF v_batch.first_end_lsn <> p_first_end_lsn
           OR v_batch.last_end_lsn <> p_last_end_lsn THEN
            RAISE EXCEPTION 'batch key % was reused with different bounds', p_batch_key
                USING ERRCODE = '23505';
        END IF;
        RETURN v_batch.batch_id;
    END IF;

    INSERT INTO batches (
        subscription_id, batch_key, first_end_lsn, last_end_lsn
    ) VALUES (
        p_subscription_id, p_batch_key, p_first_end_lsn, p_last_end_lsn
    ) RETURNING * INTO v_batch;

    UPDATE transactions
       SET state = 'publishing', batch_id = v_batch.batch_id
     WHERE subscription_id = p_subscription_id
       AND state = 'staged'
       AND end_lsn BETWEEN p_first_end_lsn AND p_last_end_lsn;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'batch % selected no staged transactions', p_batch_key
            USING ERRCODE = '22023';
    END IF;
    RETURN v_batch.batch_id;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.finish_batch(p_batch_id bigint)
RETURNS pg_lsn
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_batch batches%ROWTYPE;
BEGIN
    SELECT * INTO STRICT v_batch FROM batches WHERE batch_id = p_batch_id FOR UPDATE;
    IF v_batch.state = 'published' THEN
        RETURN v_batch.last_end_lsn;
    END IF;

    UPDATE transactions
       SET state = 'published', published_at = clock_timestamp()
     WHERE batch_id = p_batch_id AND state = 'publishing';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'batch % has no publishing transactions', p_batch_id
            USING ERRCODE = '22023';
    END IF;

    UPDATE batches SET state = 'published', published_at = clock_timestamp()
     WHERE batch_id = p_batch_id;
    UPDATE checkpoints
       SET published_lsn = GREATEST(published_lsn, v_batch.last_end_lsn),
           updated_at = clock_timestamp()
     WHERE subscription_id = v_batch.subscription_id;
    RETURN v_batch.last_end_lsn;
END;
$$;

CREATE OR REPLACE FUNCTION lake_replication.mark_acknowledged(
    p_subscription_id bigint,
    p_acknowledged_lsn pg_lsn
) RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, lake_replication
AS $$
DECLARE
    v_staged_lsn pg_lsn;
BEGIN
    SELECT staged_lsn INTO STRICT v_staged_lsn
      FROM checkpoints WHERE subscription_id = p_subscription_id FOR UPDATE;
    IF p_acknowledged_lsn > v_staged_lsn THEN
        RAISE EXCEPTION 'acknowledged LSN % exceeds staged LSN %',
            p_acknowledged_lsn, v_staged_lsn USING ERRCODE = '22023';
    END IF;
    UPDATE checkpoints
       SET acknowledged_lsn = GREATEST(acknowledged_lsn, p_acknowledged_lsn),
           updated_at = clock_timestamp()
     WHERE subscription_id = p_subscription_id;
END;
$$;

COMMIT;
