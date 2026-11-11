#!/usr/bin/env python3
"""Exact whole-pair resampling for measured performance ratios."""
from collections import defaultdict
import math



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
