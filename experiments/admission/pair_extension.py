#!/usr/bin/env python3
"""Assess one fixed ten-pair copy extension; retain the original cohort result."""
import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path

VARIANTS=('bulk_only','stream_pool')


def compositions(total,parts,prefix=()):
    if parts==1:
        yield prefix+(total,)
    else:
        for value in range(total+1):
            yield from compositions(total-value,parts-1,prefix+(value,))


def exact_bootstrap(ratios):
    """Weight count-vectors instead of enumerating n**n ordered draws.

    Each vector has n!/product(count!) ordered resamples. This produces the same
    exact whole-pair median distribution as exhaustive enumeration, up to n=10.
    """
    n=len(ratios)
    if not 1<=n<=10 or any(not math.isfinite(x) or x<=0 for x in ratios):
        raise ValueError('one to ten finite positive whole-pair ratios required')
    values=sorted(ratios);factorials=[math.factorial(i) for i in range(n+1)]
    weights=defaultdict(int);vectors=0
    for counts in compositions(n,n):
        cumulative=0;left=right=None;denominator=1
        for value,count in zip(values,counts):
            cumulative+=count;denominator*=factorials[count]
            if left is None and cumulative>(n-1)//2:left=value
            if right is None and cumulative>n//2:right=value
        weights[(left+right)/2]+=factorials[n]//denominator;vectors+=1
    total=n**n
    assert sum(weights.values())==total
    def percentile(fraction):
        target=math.ceil(total*fraction);cumulative=0
        for value,weight in sorted(weights.items()):
            cumulative+=weight
            if cumulative>=target:return value
        raise AssertionError('incomplete bootstrap distribution')
    return dict(pairs=n,paired_ratios=list(ratios),median_ratio=(values[(n-1)//2]+values[n//2])/2,
                min_ratio=min(values),max_ratio=max(values),bootstrap_median_95_interval=[percentile(.025),percentile(.975)],
                bootstrap_samples=total,enumerated_count_vectors=vectors,
                method='exact multinomial weighting of whole-pair resamples; descriptive interval conditional on these local runs')


def analyze(roots):
    if len(roots)!=2:raise ValueError('exactly two fixed five-pair cohorts required')
    cohorts=[json.loads((root/'results.json').read_text()) for root in roots]
    for cohort in cohorts:
        if cohort.get('policy')!='stream_pool' or cohort.get('variants')!=list(VARIANTS):raise ValueError('wrong policy or variant labels')
    a,b=cohorts
    if a['images']!=b['images']:raise ValueError('cohorts use different images')
    if a['archive_provenance']['archive_sha256']!=b['archive_provenance']['archive_sha256']:raise ValueError('cohorts use different archives')
    if {k:v for k,v in a['protocol'].items() if k!='order'}!={k:v for k,v in b['protocol'].items() if k!='order'}:raise ValueError('protocol drift')
    if [a['protocol']['order'],b['protocol']['order']]!=['alternating AB/BA by repetition','alternating BA/AB by repetition']:raise ValueError('ten pairs must balance starting order')
    fixed=['admission_measure.py','source_workload.py','compare.py','sample.py','analyze.py','stream_pool.c.inc','stream_pool.sql','Dockerfile.stream-pool']
    keys=['experiments/admission/'+name for name in fixed]+['extensions/lake_replication/lake_replication.c','tests/qualification/probe.py','worker/Cargo.toml','Cargo.lock']
    keys+=sorted(k for k in a['inputs'] if k.startswith('migrations/'))
    if any(a['inputs'].get(k)!=b['inputs'].get(k) for k in keys):raise ValueError('measurement or runtime input drift')
    ratios=[];selection=[]
    for index,cohort in enumerate(cohorts):
        pairs={}
        for case in cohort['cases']:
            if case['profile']!='single_copy':continue
            pair=pairs.setdefault(case['repetition'],{})
            if case['variant'] in pair:raise ValueError('duplicate copy observation')
            if case['status']!='passed' or not case['timing_validation']['valid']:raise ValueError('invalid copy observation')
            pair[case['variant']]=case
        if sorted(pairs)!=list(range(1,6)) or any(set(pair)!=set(VARIANTS) for pair in pairs.values()):raise ValueError('five complete copy pairs per cohort required')
        for repetition,pair in sorted(pairs.items()):
            ratios.append(pair['stream_pool']['copy_elapsed_seconds']/pair['bulk_only']['copy_elapsed_seconds'])
            selection.append(dict(cohort=index+1,repetition=repetition,combined_pair=index*5+repetition))
    result=exact_bootstrap(ratios);low,high=result['bootstrap_median_95_interval']
    result.update(proposed_ratio_limit=1.10,assessment='within_proposed_margin_in_this_cohort' if high<=1.10 else 'outside_proposed_margin' if low>1.10 else 'inconclusive')
    return dict(scope=__doc__,copy_duration=result,selection=selection,original_cohort_status=a['status'],
                raw_results_sha256=[hashlib.sha256((root/'results.json').read_bytes()).hexdigest() for root in roots],
                original_result_preserved=True,production_adoption_qualified=False,
                extension_rule='After the first five copy pairs were inconclusive, collect exactly five more, reverse starting order, retain all ten, keep the 1.10 margin. No further extension is implied.')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('cohorts',nargs=2,type=Path);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    result=analyze(args.cohorts);args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(result['copy_duration']['assessment'])


if __name__=='__main__':main()
