#!/usr/bin/env python3
"""Isolate UPDATE row conversion and plans in disposable Iceberg targets."""
import json
import statistics
import sys
from probe import LocalIcebergProbe


def literal(value):
    return "E'"+str(value).replace('\\','\\\\').replace("'","''")+"'"


class UpdateConversionProbe(LocalIcebergProbe):
    def exercise(self):
        self.evidence.update(scope=__doc__,diagnostic_only=True,
            methodology='Four target widths and single/bulk rows. Four repeated trials per variant, reversed order on alternating repetitions. EXPLAIN ANALYZE executes each UPDATE inside a transaction rolled back after exact value checks. Planning/execution wall times exclude COMMIT and full capture pipeline; caches are warm/shared. Candidate converts new tuple once through a lateral function scan. No production SQL is changed.')
        self.setup(mirror=False,seed=False)
        results=[]
        for columns in [2,16,32,48]:
            fields=['note']+[f'c{i}' for i in range(columns-2)]
            table=f'iceberg_mirror.update_width_{columns}'
            self.sql(f'CREATE TABLE {table}(id bigint NOT NULL,'+','.join(f'{c} text' for c in fields)+') USING iceberg')
            self.sql(f'INSERT INTO {table} SELECT i,'+','.join("repeat('o',64)" for c in fields)+' FROM generate_series(1,1000) i')
            for rows in [1,16,32]:
                old=[{'id':str(i)} for i in range(1,rows+1)]
                new=[dict(id=str(i),**{c:'n'*64 for c in fields}) for i in range(1,rows+1)]
                array=lambda values:'ARRAY['+','.join(literal(json.dumps(v))+'::jsonb' for v in values)+']::jsonb[]'
                for repeat in range(4):
                    for candidate in ([False,True] if repeat%2==0 else [True,False]):
                        assignments=','.join(f'{c}='+ (f'typed_new.{c}' if candidate else f'(jsonb_populate_record(NULL::{table},source.new_tuple)).{c}') for c in ['id']+fields)
                        lateral=f' CROSS JOIN LATERAL jsonb_populate_record(NULL::{table},source.new_tuple) AS typed_new' if candidate else ''
                        statement=f'''WITH source AS (
                            SELECT old_tuple,new_tuple,ordinality FROM unnest({array(old)},{array(new)})
                            WITH ORDINALITY AS input(old_tuple,new_tuple,ordinality)), applied AS (
                            UPDATE {table} AS destination SET {assignments} FROM source {lateral}
                            WHERE destination.id=(jsonb_populate_record(NULL::{table},source.old_tuple)).id
                            AND destination.id=ANY(ARRAY[{','.join(str(i) for i in range(1,rows+1))}]::bigint[])
                            RETURNING source.ordinality)
                            SELECT count(*),count(DISTINCT ordinality) FROM applied'''
                        response=self.sql('BEGIN;EXPLAIN(ANALYZE,VERBOSE,BUFFERS,FORMAT JSON) '+statement+';'+
                            f"SELECT bool_and("+' AND '.join(f"{c}=repeat('n',64)" for c in fields)+f') FROM {table} WHERE id<={rows};ROLLBACK;').stdout.strip()
                        plan,end=json.JSONDecoder().raw_decode(response)
                        self.check(f'width={columns} rows={rows} candidate={candidate} values exact',response[end:].strip(),'t')
                        results.append(dict(columns=columns,rows=rows,repetition=repeat+1,candidate=candidate,
                            planning_ms=plan[0]['Planning Time'],execution_ms=plan[0]['Execution Time'],plan=plan))
        summary=[]
        for columns in [2,16,32,48]:
            for rows in [1,16,32]:
                item=dict(columns=columns,rows=rows)
                for candidate in [False,True]:
                    ms=[r['execution_ms'] for r in results if (r['columns'],r['rows'],r['candidate'])==(columns,rows,candidate)]
                    item['candidate' if candidate else 'control']=dict(median_ms=statistics.median(ms),minimum_ms=min(ms),maximum_ms=max(ms))
                summary.append(item)
        self.evidence['conversion_trials']=results;self.evidence['conversion_summary']=summary
        print(json.dumps(summary,indent=2),flush=True)


if __name__=='__main__':sys.exit(UpdateConversionProbe().run())
