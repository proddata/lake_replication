"""Audit complete chunk accounting and observed staging-lock exclusivity."""
import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('budget_results',Path(__file__).with_name('budget_results.py'))
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)


def audit(case):
    final=case.get('diagnostic',{}).get('staging_gate',{}).get('final',{})
    requests,expected=b.expected_accounting(case['copy_rows'],case['bulk_copies'])
    observed=[t.get('staging_gate') for t in case.get('trace',[])]
    counters=[v for k,v in final.items() if k!='error' and isinstance(v,dict)]
    checks={'stable_clock':case.get('timing_validation',{}).get('valid',False),
        'final_capture_succeeded':bool(counters) and 'error' not in final,
        'all_chunks_committed_once':sum(c.get('chunks',0) for c in counters)==requests,
        'all_serialized_bytes_accounted':sum(c.get('bytes',0) for c in counters)==expected,
        'lock_snapshots_captured':bool(observed) and all(isinstance(rows,list) for rows in observed),
        'at_most_one_sampled_holder':all(sum(r['state']=='admitted' for r in rows)<=1 for rows in observed if isinstance(rows,list))}
    per_requests,per_bytes=b.expected_accounting(case['copy_rows'],1)
    names=['fair_copy' if i==0 else f'fair_copy_{i}' for i in range(case['bulk_copies'])]
    checks['each_copy_accounted_independently']=all(final.get(n,{}).get('chunks')==per_requests and final.get(n,{}).get('bytes')==per_bytes for n in names)
    mode=case['diagnostic']['staging_gate'].get('mode','serial')
    if mode=='serial':checks['permit_holder_observed']=any(any(r['state']=='admitted' for r in rows) for rows in observed if isinstance(rows,list))
    else:checks['accounting_control_never_locks']=all(not rows for rows in observed if isinstance(rows,list))
    return {'valid':all(checks.values()),'checks':checks,'expected_chunks':requests,'expected_bytes':expected,
        'scope':'Successful append transactions and sampled lock state; count/bytes do not bound total memory or source work'}
