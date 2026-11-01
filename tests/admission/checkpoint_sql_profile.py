"""Opt-in local statement profiling; capture after measurement, not qualification."""
import json


class Profile:
    def __init__(self,q,sql,transaction_timing=False):
        self.q,self.sql=q,sql
        self.transaction_timing=transaction_timing
        self.captured=False;self.error=None

    def compose_text(self,text):
        anchor='      - shared_preload_libraries=pg_extension_base\n'
        if text.count(anchor)!=1:raise ValueError('destination preload hook changed')
        configured=text.replace(anchor,
            '      - shared_preload_libraries=pg_extension_base,pg_stat_statements\n'
            '      - -c\n      - pg_stat_statements.track=all\n'
            '      - -c\n      - pg_stat_statements.track_planning=on\n'
            '      - -c\n      - pg_stat_statements.track_utility=off\n'
            '      - -c\n      - track_io_timing=on\n')
        return configured.replace('pg_stat_statements.track_utility=off','pg_stat_statements.track_utility=on') if self.transaction_timing else configured

    def install(self,database):
        self.sql('destination',database,'CREATE EXTENSION pg_stat_statements')

    def capture(self):
        if self.captured:return
        self.captured=True
        result=dict(scope=__doc__,inclusive_nested_timings=True,transaction_timing=self.transaction_timing,
            warning='Profiling changes overhead; compare only equivalently instrumented observations. Nested execution totals must not be added. COMMIT utility statistics do not measure full transaction finalization; use server duration logs or client round trips for that boundary.')
        try:
            # Utility DDL can contain connection secrets. With transaction timing
            # enabled, export only normalized SELECTs and transaction controls.
            where=" WHERE s.query ~ '^(SELECT|BEGIN|COMMIT|ROLLBACK)([[:space:];]|$)'" if self.transaction_timing else ''
            rows=self.sql('destination','analytics',"SELECT coalesce(jsonb_agg(to_jsonb(s)||jsonb_build_object('database',d.datname,'query',left(s.query,1500))),'[]'::jsonb) FROM pg_stat_statements s JOIN pg_database d ON d.oid=s.dbid"+where)
            result['statements']=json.loads(rows)
        except Exception as error:
            self.error=str(error);result['error']=self.error
        (self.q.RESULTS/'sql-profile.json').write_text(json.dumps(result,indent=2)+'\n')
