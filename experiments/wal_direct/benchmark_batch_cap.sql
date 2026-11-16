-- Disposable benchmark only: make the managed journal publisher honor the
-- comparison's explicit transaction cap. Never installed in a release.
ALTER FUNCTION lake_replication.next_batch(bigint,bigint,bigint,bigint)
 RENAME TO benchmark_original_next_batch;
CREATE FUNCTION lake_replication.next_batch(
 p_subscription_id bigint,p_max_transactions bigint,p_max_rows bigint,p_max_bytes bigint
) RETURNS TABLE(first_end_lsn pg_lsn,last_end_lsn pg_lsn,transaction_count bigint,row_count numeric,byte_count numeric)
LANGUAGE sql SET search_path=pg_catalog,lake_replication AS $$
 SELECT * FROM lake_replication.benchmark_original_next_batch(
 p_subscription_id,least(p_max_transactions,current_setting('lake_direct.benchmark_batch_cap')::bigint),least(p_max_rows,4096),least(p_max_bytes,8388608))
$$;
REVOKE ALL ON FUNCTION lake_replication.next_batch(bigint,bigint,bigint,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_replication.next_batch(bigint,bigint,bigint,bigint)
 TO lake_replication_worker,lake_replication_admin;
