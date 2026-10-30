"""Isolated 1,000-row append plans; rollback work is diagnostic, not throughput evidence."""
import json


def run(q,c):
    database='staging_cost'
    q.sql('destination','analytics',f'CREATE DATABASE {database}')
    q.sql('destination',database,'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    def sql(text):return q.sql('destination',database,text)
    session=None
    result=dict(scope=__doc__,rows_per_chunk=1000,repetitions=3,observations=[],
        limitations='Isolated warm small-table plans, including rollback WAL; no concurrent workers or engine publication. Trigger and nested execution costs overlap; do not sum them.')
    try:
        sid=int(sql("SELECT lake_replication.create_subscription(42,1,1,'staging_cost','staging_cost',ARRAY['pub'])"))
        sql(f"""CREATE TABLE public.cost_target(id bigint,payload text);
          INSERT INTO lake_replication.relations(subscription_id,remote_relation_id,schema_version,source_namespace,source_relation,replica_identity,columns)
          VALUES({sid},42,1,'public','cost_target','d','[]');
          INSERT INTO lake_replication.initial_copies(subscription_id,remote_relation_id,relation_schema_version,destination_relation,consistent_lsn,snapshot_name,state,overwrite_destination)
          VALUES({sid},42,1,'public.cost_target','0/1','diagnostic','copying',false)""")
        result['indexes']=json.loads(sql("SELECT jsonb_agg(to_jsonb(i)) FROM pg_indexes i WHERE schemaname='lake_replication' AND tablename='initial_copy_rows'"))
        result['constraints']=json.loads(sql("SELECT jsonb_agg(jsonb_build_object('name',conname,'definition',pg_get_constraintdef(oid))) FROM pg_constraint WHERE conrelid='lake_replication.initial_copy_rows'::regclass"))
        session=c.Psql('destination',database)
        session.query("SET search_path=pg_catalog,lake_replication; SET track_io_timing=on; CREATE TEMP TABLE cost_payload AS SELECT jsonb_agg(jsonb_build_object('id',n,'payload',repeat('c',128))) AS rows FROM generate_series(1,1000) n")
        statements={
            'append':f'SELECT lake_replication.append_initial_copy_rows({sid},42,1,0,(SELECT rows FROM cost_payload))',
            'insert':f'''INSERT INTO lake_replication.initial_copy_rows(subscription_id,remote_relation_id,relation_schema_version,row_ordinal,row_data)
              SELECT {sid},42,1,ordinality-1,row_data FROM jsonb_array_elements((SELECT rows FROM cost_payload)) WITH ORDINALITY source(row_data,ordinality)''',
        }
        for repetition in range(3):
            for kind,statement in statements.items():
                session.query('BEGIN; SELECT lake_replication.admit_staging()')
                plan=json.loads('\n'.join(session.query('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING,FORMAT JSON) '+statement)))[0]
                session.query('ROLLBACK')
                result['observations'].append(dict(repetition=repetition+1,statement=kind,sql=statement,explain=plan))
                q.check(f'{kind} profile {repetition+1} rolls back every row',sql('SELECT count(*) FROM lake_replication.initial_copy_rows'),'0')
                q.check(f'{kind} profile {repetition+1} rolls back row progress',sql('SELECT rows_copied FROM lake_replication.initial_copies'),'0')
        result['status']='passed'
    finally:
        if session:session.close()
        (q.RESULTS/'staging-cost.json').write_text(json.dumps(result,indent=2)+'\n')
        q.sql('destination','analytics',f'DROP DATABASE {database} WITH (FORCE)')
