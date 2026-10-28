-- Diagnostic filesystem snapshots, not an extension migration or recovery state.
CREATE SCHEMA receiver_progress;
REVOKE ALL ON SCHEMA receiver_progress FROM PUBLIC;
CREATE FUNCTION receiver_progress.snapshots() RETURNS SETOF jsonb
LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$
 SELECT pg_read_file('/worker-logs/'||name)::jsonb
 FROM pg_ls_dir('/worker-logs') AS f(name)
 WHERE name ~ '^receiver-progress-[0-9a-f]+-[0-9]+[.]json$'
$$;
REVOKE ALL ON FUNCTION receiver_progress.snapshots() FROM PUBLIC;
