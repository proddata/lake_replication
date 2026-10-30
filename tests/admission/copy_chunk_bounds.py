"""Audit real native copy transaction sizes, byte-budget flushing and cleanup."""
import json
from urllib.parse import quote


def run(q,c,rows):
    database='copy_chunk_bounds'
    q.sql('destination','analytics',f'CREATE DATABASE {database}')
    q.sql('destination',database,'CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; CREATE EXTENSION lake_replication')
    def sql(statement):return q.sql('destination',database,statement)
    connection=f"postgresql://lake_source:{quote(q.SOURCE_PASSWORD,safe='')}@source:5432/application"
    cases=[('narrow',5001,'128'),('wide',220,'65536'),('oversized',3,'CASE WHEN n=1 THEN 9437184 ELSE 32 END')]
    evidence=[]
    try:
        for name,count,width in cases:
            table='chunk_'+name;pub=table+'_pub';sub=table+'_sub'
            q.sql('source','application',f"CREATE TABLE {table}(id bigint PRIMARY KEY,payload text); INSERT INTO {table} SELECT n,repeat('c',({width})::integer) FROM generate_series(1,{count}) n; CREATE PUBLICATION {pub} FOR TABLE {table}; GRANT SELECT ON {table} TO lake_source")
            sql(f'CREATE TABLE {table}(id bigint NOT NULL,payload text) USING iceberg')
            owner=c.Psql('destination','analytics')
            try:
                owner.query('BEGIN; SELECT lake_replication.admit_bulk()')
                sql(f"CREATE SUBSCRIPTION {sub} CONNECTION '{connection}' PUBLICATION {pub} WITH(lake_target='iceberg',lake_credential_file='/credentials/destination-only.env')")
                q.check(name+' native copy stages all rows while publication waits',q.wait_for('destination',database,f"SELECT coalesce(max(rows_copied),0) FROM lake_replication.initial_copies",str(count),timeout=60),str(count))
                if getattr(q,'STAGING_ROWS','initial_copy_rows')=='initial_copy_chunks':
                    chunk_query="SELECT jsonb_agg(t) FROM (SELECT c.xmin::text AS xid,count(*) AS rows,sum(octet_length(r.row_data::text)) AS bytes FROM lake_replication.initial_copy_chunks c CROSS JOIN LATERAL jsonb_array_elements(c.rows) r(row_data) GROUP BY c.xmin::text) t"
                else:
                    chunk_query="SELECT jsonb_agg(t) FROM (SELECT xmin::text AS xid,count(*) AS rows,sum(octet_length(row_data::text)) AS bytes FROM lake_replication.initial_copy_rows GROUP BY xmin::text) t"
                chunks=json.loads(sql(chunk_query))
                q.check(name+' native copy obeys configured row cap',max(x['rows'] for x in chunks)<=rows,True)
                q.check(name+' native copy retains byte budget and singleton exception',all(x['bytes']<=8388608 or x['rows']==1 for x in chunks),True)
                if name=='narrow':q.check('narrow copy actually uses selected row cap',max(x['rows'] for x in chunks),rows)
                if name=='wide':q.check('wide copy flushes on bytes before row cap',len(chunks)>1 and max(x['rows'] for x in chunks)<rows,True)
                if name=='oversized':q.check('single oversized row retains existing behavior',sum(x['bytes']>8388608 and x['rows']==1 for x in chunks),1)
                q.check(name+' copy ordinals stay contiguous',sql(f"SELECT count(*)||':'||min(row_ordinal)||':'||max(row_ordinal) FROM lake_replication.initial_copy_rows"),f'{count}:0:{count-1}')
                evidence.append(dict(case=name,chunks=chunks))
                owner.query('ROLLBACK')
                q.wait_for('destination',database,'SELECT state FROM lake_replication.subscriptions','streaming',timeout=60)
                expected_width=width.replace('n=1','id=1')
                expected=f"count(*)={count} AND count(DISTINCT id)={count} AND min(id)=1 AND max(id)={count} AND sum(id)={count*(count+1)//2} AND bool_and(payload=repeat('c',({expected_width})::integer))"
                q.check(name+' copy publishes exact data after admission release',sql(f'SELECT ({expected})::text FROM {table}'),'true')
                sql(f'DROP SUBSCRIPTION {sub}')
                q.check(name+' drop removes source slot',q.sql('source','application',f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{sub}'"),'0')
                sql(f'DROP TABLE {table}')
                q.sql('source','application',f'DROP PUBLICATION {pub}; DROP TABLE {table}')
            finally:owner.close()
    finally:
        (q.RESULTS/'copy-chunk-bounds.json').write_text(json.dumps(evidence,indent=2)+'\n')
        q.sql('destination','analytics',f'DROP DATABASE {database} WITH (FORCE)')
