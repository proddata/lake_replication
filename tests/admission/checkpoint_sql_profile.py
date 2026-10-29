"""Opt-in local statement profiling; capture after measurement, not qualification."""
import json


class Profile:
    def __init__(self,q,sql):
        self.q,self.sql=q,sql
        self.captured=False;self.error=None

    def compose_text(self,text):
        anchor='      - shared_preload_libraries=pg_extension_base\n'
        if text.count(anchor)!=1:raise ValueError('destination preload hook changed')
        return text.replace(anchor,
            '      - shared_preload_libraries=pg_extension_base,pg_stat_statements\n'
            '      - -c\n      - pg_stat_statements.track=all\n'
            '      - -c\n      - pg_stat_statements.track_planning=on\n'
            '      - -c\n      - pg_stat_statements.track_utility=off\n'
            '      - -c\n      - track_io_timing=on\n')

    def install(self,database):
        self.sql('destination',database,'CREATE EXTENSION pg_stat_statements')

    def capture(self):
        if self.captured:return
        self.captured=True
        result=dict(scope=__doc__,inclusive_nested_timings=True,
            warning='Profiling changes overhead; compare only equivalently instrumented observations. Nested execution totals must not be added.')
        try:
            rows=self.sql('destination','analytics',"SELECT coalesce(jsonb_agg(to_jsonb(s)||jsonb_build_object('database',d.datname,'query',left(s.query,1500))),'[]'::jsonb) FROM pg_stat_statements s JOIN pg_database d ON d.oid=s.dbid")
            result['statements']=json.loads(rows)
        except Exception as error:
            self.error=str(error);result['error']=self.error
        (self.q.RESULTS/'sql-profile.json').write_text(json.dumps(result,indent=2)+'\n')
