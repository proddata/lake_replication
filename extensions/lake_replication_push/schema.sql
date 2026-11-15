-- Optional source-push metadata; normal logical-pull installations do not need this companion.
CREATE TABLE lake_replication.push_bindings (
    subscription_id bigint PRIMARY KEY REFERENCES lake_replication.subscriptions ON DELETE CASCADE,
    stream_id uuid NOT NULL,
    system_identifier numeric(20,0) NOT NULL,
    database_oid oid NOT NULL,
    capture_generation bigint NOT NULL CHECK (capture_generation > 0),
    schema_enabled boolean NOT NULL DEFAULT false,
    imported_sequence bigint NOT NULL DEFAULT 0,
    imported_commit_lsn pg_lsn NOT NULL DEFAULT '0/0',
    imported_end_lsn pg_lsn NOT NULL DEFAULT '0/0',
    snapshot_end_lsn pg_lsn,
    observed_captured_sequence bigint CHECK(observed_captured_sequence>=0),
    source_observed_at timestamptz,
    CHECK((observed_captured_sequence IS NULL)=(source_observed_at IS NULL))
);
CREATE TABLE lake_replication.push_tables (
    subscription_id bigint REFERENCES lake_replication.push_bindings ON DELETE CASCADE,
    table_id uuid NOT NULL,
    source_schema_version integer NOT NULL CHECK (source_schema_version > 0),
    remote_relation_id oid NOT NULL,
    relation_schema_version bigint NOT NULL,
    PRIMARY KEY (subscription_id, table_id, source_schema_version),
    UNIQUE (subscription_id, remote_relation_id),
    FOREIGN KEY (subscription_id, remote_relation_id, relation_schema_version)
        REFERENCES lake_replication.relations(subscription_id, remote_relation_id, schema_version) ON DELETE CASCADE
);
CREATE TABLE lake_replication.push_receipts (
    subscription_id bigint REFERENCES lake_replication.push_bindings ON DELETE CASCADE,
    sequence bigint NOT NULL,
    batch_id uuid NOT NULL,
    digest text NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    commit_lsn pg_lsn NOT NULL,
    end_lsn pg_lsn NOT NULL,
    PRIMARY KEY (subscription_id, sequence),
    UNIQUE (subscription_id, batch_id)
);
REVOKE ALL ON lake_replication.push_bindings, lake_replication.push_tables,
    lake_replication.push_receipts FROM PUBLIC;

CREATE FUNCTION lake_replication.import_push_batch(
    sub bigint, stream uuid, source_system numeric, source_database oid,
    capture_gen bigint, seq bigint, batch uuid, payload_digest text,
    source_xid bigint, source_begin pg_lsn, source_commit pg_lsn, source_end pg_lsn,
    source_time timestamptz, changes jsonb, snapshot boolean DEFAULT false
) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
DECLARE
    binding push_bindings%ROWTYPE;
    receipt push_receipts%ROWTYPE;
BEGIN
    -- ManagedClient has already acquired the normal generation fence. The
    -- journal enforces the existing generation and byte/count admission too.
    SELECT * INTO STRICT binding FROM push_bindings WHERE subscription_id=sub FOR UPDATE;
    IF (binding.stream_id,binding.system_identifier,binding.database_oid,binding.capture_generation)
        IS DISTINCT FROM (stream,source_system,source_database,capture_gen) THEN
        RAISE EXCEPTION 'push stream identity or generation mismatch' USING ERRCODE='55000';
    END IF;
    IF binding.schema_enabled THEN
        RAISE EXCEPTION 'schema-enabled push binding requires ordered admission' USING ERRCODE='55000';
    END IF;
    SELECT * INTO receipt FROM push_receipts WHERE subscription_id=sub AND sequence=seq;
    IF FOUND THEN
        IF (receipt.batch_id,receipt.digest,receipt.commit_lsn,receipt.end_lsn)
            IS DISTINCT FROM (batch,payload_digest,source_commit,source_end) THEN
            RAISE EXCEPTION 'conflicting push replay' USING ERRCODE='23505';
        END IF;
        RETURN 'replayed';
    END IF;
    IF seq <> binding.imported_sequence+1 OR source_end <= binding.imported_end_lsn
       OR source_commit <= binding.imported_commit_lsn THEN
        RAISE EXCEPTION 'push manifest gap or backwards source coordinates' USING ERRCODE='22023';
    END IF;
    IF snapshot AND (seq<>1 OR binding.imported_sequence<>0) THEN
        RAISE EXCEPTION 'snapshot activation requires a new private destination generation';
    END IF;
    IF EXISTS (SELECT FROM push_receipts WHERE subscription_id=sub AND batch_id=batch) THEN
        RAISE EXCEPTION 'push batch identity reused at another sequence' USING ERRCODE='23505';
    END IF;
    IF NOT try_stage_transaction(sub,source_xid,source_begin,source_commit,source_end,source_time,changes) THEN
        RETURN 'full';
    END IF;
    INSERT INTO push_receipts VALUES(sub,seq,batch,payload_digest,source_commit,source_end);
    UPDATE push_bindings SET imported_sequence=seq, imported_commit_lsn=source_commit,
        imported_end_lsn=source_end,snapshot_end_lsn=CASE WHEN snapshot THEN source_end ELSE snapshot_end_lsn END
        WHERE subscription_id=sub;
    RETURN 'imported';
END;
$$;
REVOKE ALL ON FUNCTION lake_replication.import_push_batch(bigint,uuid,numeric,oid,bigint,bigint,uuid,text,bigint,pg_lsn,pg_lsn,pg_lsn,timestamptz,jsonb,boolean) FROM PUBLIC;

-- This is an observation, never an import or publication acknowledgement.
CREATE FUNCTION lake_replication.observe_push_source(sub bigint,stream uuid,
    source_system numeric,source_database oid,capture_gen bigint,captured bigint)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,lake_replication AS $$
DECLARE binding push_bindings%ROWTYPE;
BEGIN
    SELECT * INTO STRICT binding FROM push_bindings WHERE subscription_id=sub FOR UPDATE;
    IF (binding.stream_id,binding.system_identifier,binding.database_oid,binding.capture_generation)
        IS DISTINCT FROM (stream,source_system,source_database,capture_gen) THEN
        RAISE EXCEPTION 'push observation identity or generation mismatch' USING ERRCODE='55000';
    END IF;
    IF captured IS NULL OR captured<0 OR captured<binding.imported_sequence OR captured<binding.observed_captured_sequence THEN
        RAISE EXCEPTION 'observed source capture frontier moved backwards; controlled rebuild required' USING ERRCODE='55000';
    END IF;
    IF binding.observed_captured_sequence IS DISTINCT FROM captured
        OR binding.source_observed_at<clock_timestamp()-INTERVAL '5 seconds' THEN
        UPDATE push_bindings SET observed_captured_sequence=captured,source_observed_at=clock_timestamp()
            WHERE subscription_id=sub;
    END IF;
END $$;
REVOKE ALL ON FUNCTION lake_replication.observe_push_source(bigint,uuid,numeric,oid,bigint,bigint) FROM PUBLIC;

-- These are independent frontiers. A published sequence is derived from the
-- existing publisher checkpoint, never from a successful importer process.
CREATE VIEW lake_replication.push_status AS
SELECT frontier.*,
    CASE WHEN observed_captured_sequence IS NULL THEN NULL
        ELSE greatest(observed_captured_sequence-imported_sequence,0) END AS observed_capture_to_import_batches,
    greatest(imported_sequence-published_sequence,0) AS imported_to_published_batches,
    extract(epoch FROM clock_timestamp()-source_observed_at)::double precision AS source_observation_age_seconds,
    journal.pending_transactions AS journal_pending_transactions,journal.pending_payload_bytes AS journal_pending_payload_bytes,
    a.max_transactions AS journal_max_transactions,a.max_payload_bytes AS journal_max_payload_bytes
FROM (SELECT b.*,c.published_lsn,
    (SELECT coalesce(max(r.sequence),0) FROM lake_replication.push_receipts r
        WHERE r.subscription_id=b.subscription_id AND r.end_lsn<=c.published_lsn) AS published_sequence,
    greatest(pg_wal_lsn_diff(b.imported_end_lsn,c.published_lsn),0) AS imported_to_published_wal_bytes
FROM lake_replication.push_bindings b
JOIN lake_replication.checkpoints c USING(subscription_id)) frontier
LEFT JOIN lake_replication.journal_limits a USING(subscription_id)
CROSS JOIN LATERAL (SELECT count(*) AS pending_transactions,coalesce(sum(t.journal_payload_bytes),0) AS pending_payload_bytes
    FROM lake_replication.transactions t WHERE t.subscription_id=frontier.subscription_id AND t.state<>'published') journal;
REVOKE ALL ON lake_replication.push_status FROM PUBLIC;
