"""Integrity and transaction boundaries for the disposable chunk staging store."""


def run(q,c):
    database='staging_chunk_integrity'
    q.sql('destination','analytics',f'CREATE DATABASE {database}')
    q.sql('destination',database,'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    def sql(text):return q.sql('destination',database,text)
    def rejected(name,text,detail):
        result=q.sql_result('destination',database,text,check_result=False)
        q.check(name,result.returncode!=0 and detail in result.stderr,True)
    sid=int(sql("SELECT lake_replication.create_subscription(42,1,1,'chunk_integrity','chunk_integrity',ARRAY['pub'])"))
    sql(f"""CREATE TABLE public.chunk_target(id bigint);
      INSERT INTO lake_replication.relations(subscription_id,remote_relation_id,schema_version,source_namespace,source_relation,replica_identity,columns)
      VALUES({sid},42,1,'public','chunk_target','d','[]');
      INSERT INTO lake_replication.initial_copies(subscription_id,remote_relation_id,relation_schema_version,destination_relation,consistent_lsn,snapshot_name,state,overwrite_destination)
      VALUES({sid},42,1,'public.chunk_target','0/1','diagnostic','copying',false)""")
    def append(start,rows):return f"SELECT lake_replication.append_initial_copy_rows({sid},42,1,{start},'{rows}'::jsonb)"
    def direct(start,rows='[{}]',parent=sid):return f"INSERT INTO lake_replication.initial_copy_chunks(subscription_id,remote_relation_id,relation_schema_version,row_ordinal,rows) VALUES({parent},42,1,{start},'{rows}'::jsonb)"
    def digest():return sql("SELECT coalesce(jsonb_agg(jsonb_build_array(row_ordinal,row_data) ORDER BY row_ordinal)::text,'[]') FROM lake_replication.initial_copy_rows")
    owner=c.Psql('destination',database)
    try:
        q.check('chunk append accepts empty input without creating storage',sql(append(0,'[]')),'0')
        rejected('chunk rejects non-object row',append(0,'[{},1]'),'JSON objects')
        rejected('chunk rejects negative ordinal',append(-1,'[{}]'),'invalid initial-copy')
        rejected('chunk FK rejects orphan',direct(0,parent=sid+1),'foreign key')
        rejected('chunk direct write validates object rows',direct(0,'[1]'),'check constraint')
        rejected('chunk rejects ordinal overflow',append(9223372036854775807,'[{},{}]'),'check constraint')
        rejected('chunk rejects larger row cap',f"SELECT lake_replication.append_initial_copy_rows({sid},42,1,0,(SELECT jsonb_agg('{{}}'::jsonb) FROM generate_series(1,1001)))",'check constraint')
        q.check('chunk accepts adjacent first range',sql(append(0,'[{"id":1},{"id":2}]')),'2')
        q.check('chunk accepts adjacent second range',sql(append(2,'[{"id":3}]')),'1')
        original=digest()
        rejected('chunk rejects identical range replay',append(0,'[{"id":9}]'),'duplicate key')
        rejected('chunk excludes partial range overlap',append(1,'[{"id":9},{"id":10}]'),'exclusion constraint')
        q.check('failed chunk preserves exact rows',digest(),original)
        q.check('failed chunk preserves progress',sql('SELECT rows_copied FROM lake_replication.initial_copies'),'3')
        owner.query('BEGIN; '+append(3,'[{"id":4}]'))
        q.check('uncommitted chunk and progress remain invisible',digest(),original)
        rejected('chunk parent delete waits for committed FK reference',"SET lock_timeout='100ms'; DELETE FROM lake_replication.initial_copies",'lock timeout')
        rejected('chunk competing append serializes at parent',"SET lock_timeout='100ms'; "+append(3,'[{"id":5}]'),'lock timeout')
        owner.query('ROLLBACK')
        q.check('chunk rollback preserves rows',digest(),original)
        q.check('chunk rollback preserves progress',sql('SELECT rows_copied FROM lake_replication.initial_copies'),'3')
        owner.query('BEGIN; '+append(3,'[{"id":4}]')+'; COMMIT')
        q.check('chunk commit advances rows and progress together',sql('SELECT (SELECT count(*) FROM lake_replication.initial_copy_rows)||\':\'||rows_copied FROM lake_replication.initial_copies'),'4:4')
        rejected('chunk parent key update preserves FK',"UPDATE lake_replication.initial_copies SET remote_relation_id=43",'foreign key')
        rejected('chunk fenced physical mutation refuses unknown generation',f"SET lake_replication.worker_generation='1'; SET lake_replication.worker_subscription='{sid}'; "+direct(10),'stopped or obsolete')
        sql("UPDATE lake_replication.initial_copies SET state='ready'")
        rejected('chunk append refuses completed copy',append(4,'[{}]'),'not accepting rows')
        sql('DELETE FROM lake_replication.initial_copies')
        q.check('chunk parent deletion cascades every chunk',sql('SELECT count(*) FROM lake_replication.initial_copy_chunks'),'0')
        q.check('chunk parent deletion clears expanded rows',sql('SELECT count(*) FROM lake_replication.initial_copy_rows'),'0')
        rejected('retired row table cannot accept hidden rows',f"INSERT INTO lake_replication.initial_copy_rows_per_row VALUES({sid},42,1,0,'{{}}')",'check constraint')
    finally:
        owner.close()
        q.sql('destination','analytics',f'DROP DATABASE {database} WITH (FORCE)')
