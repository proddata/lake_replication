#!/usr/bin/env python3
"""Inspect destination DELETE pushdown without changing replication behavior."""
import json
import sys
from probe import LocalIcebergProbe


class DeletePredicateProbe(LocalIcebergProbe):
    def exercise(self):
        self.setup()
        expressions={
            'legacy': "id IS NOT DISTINCT FROM (jsonb_populate_record(NULL::iceberg_mirror.a,'{\"id\":\"1\"}'::jsonb)).id",
            'equality': "id = (jsonb_populate_record(NULL::iceberg_mirror.a,'{\"id\":\"1\"}'::jsonb)).id",
            'scalar': "id = 1::bigint",
            'membership': "id = ANY(ARRAY[1]::bigint[])",
        }
        plans={}
        for name,expression in expressions.items():
            plans[name]=json.loads(self.value(f'EXPLAIN(VERBOSE,FORMAT JSON) DELETE FROM iceberg_mirror.a WHERE {expression}'))
        self.evidence['delete_plans']=plans
        self.equal()


if __name__=='__main__':sys.exit(DeletePredicateProbe().run())
