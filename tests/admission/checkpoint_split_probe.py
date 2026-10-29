#!/usr/bin/env python3
"""Local checkpoint partition prototype; SQL overlay, never release qualification."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cases',nargs='+',choices=['managed','fairness'],default=['managed','fairness'])
    parser.add_argument('--cpuset',default='0-1')
    args=parser.parse_args()
    if not re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*',args.cpuset):
        parser.error('invalid CPU affinity')
    spec=importlib.util.spec_from_file_location('production_probe',Path(__file__).with_name('probe.py'))
    p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
    original_sql=p.q.sql;original_compose=p.q.compose;original_text=p.m.compose_text
    installed=set();installs=[]
    overlay=Path(__file__).with_name('checkpoint_split.sql').read_text()
    spec=importlib.util.spec_from_file_location('split_operations',Path(__file__).with_name('checkpoint_split_operations.py'))
    operations=importlib.util.module_from_spec(spec);spec.loader.exec_module(operations)

    def install(database):
        if database not in installed:
            original_sql('destination',database,overlay)
            installed.add(database);installs.append(database)

    def sql(service,database,statement,**kwargs):
        result=original_sql(service,database,statement,**kwargs)
        if service=='destination' and 'CREATE EXTENSION lake_replication' in statement:
            install(database)
        return result

    def compose(*values,**kwargs):
        result=original_compose(*values,**kwargs)
        if values and values[0]=='down':installed.clear()
        if values and values[0]=='up' and 'analytics' not in installed:
            install('analytics')
            operations.run(p.q,p.c)
        return result

    def compose_text():
        text=original_text()
        for service in ('source','destination','pgduck','storage'):
            text=re.sub(rf'^  {service}:$',f'  {service}:\n    cpuset: "{args.cpuset}"',text,flags=re.MULTILINE)
        return text

    paths=[Path(__file__),Path(__file__).with_name('checkpoint_split.sql'),Path(__file__).with_name('checkpoint_split_operations.py')]
    inputs={str(path.relative_to(p.ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    p.m.diagnostic_sources=paths
    p.q.sql=sql;p.q.compose=compose;p.m.compose_text=compose_text
    sys.argv=[sys.argv[0],'--cases',*args.cases]
    code=p.main()
    metadata=dict(scope=__doc__,sql_overlay=True,production_migrations_changed=False,
        service_cpu_affinity=args.cpuset,installations=installs,
        inputs=inputs)
    (p.q.RESULTS/'checkpoint-split-prototype.json').write_text(json.dumps(metadata,indent=2)+'\n')
    result_path=p.q.RESULTS/'results.json'
    result=json.loads(result_path.read_text())
    result['scope']=__doc__;result['prototype']=metadata
    for case in result['cases']:
        case['sql_overlay']=True
        case_path=p.q.RESULTS/case['case']/'results.json'
        case_path.write_text(json.dumps(case,indent=2)+'\n')
    result_path.write_text(json.dumps(result,indent=2)+'\n')
    return code


if __name__=='__main__':raise SystemExit(main())
