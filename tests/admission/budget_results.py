"""Audit shared-budget accounting independently of the injected worker."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path


def expected_accounting(rows,copies):
    # PG jsonb::text uses these separators for the fixed id/payload fixture.
    base=len(json.dumps({'id':0,'payload':'b'*128}))-1
    digits=sum((min(rows,10**width-1)-10**(width-1)+1)*width
               for width in range(1,len(str(rows))+1))
    chunks=(rows+999)//1000
    expected_bytes=copies*(rows*base+digits+rows+chunks)
    return copies*chunks,expected_bytes


def audit(case):
    budget=case['diagnostic']['shared_byte_budget']
    stats=budget['stats']
    state=stats.get('credit') or {}
    requests,expected_bytes=expected_accounting(case['copy_rows'],case['bulk_copies'])
    checks={'stable_measurement_clock':case.get('timing_validation',{}).get('valid',False),
            'stats_captured':not stats.get('error') and bool(state),
            'worker_logs_captured':bool(stats.get('worker_log_sha256')),
            'all_chunks_reserved_once':state.get('requests')==requests,
            'exact_serialized_bytes_reserved':state.get('reserved_bytes')==expected_bytes,
            'bounded_reservation_wait':0<=state.get('max_wait_seconds',float('inf'))<=2.0}
    rate=budget['rate_bytes_per_second']
    settings=stats.get('settings',{})
    checks['configuration_matches_request']=(settings.get('rate_bytes_per_second')==rate
        and settings.get('burst_bytes')==budget['burst_bytes'])
    envelope=None
    if rate==0:
        checks['accounting_only_never_slept']=state.get('sum_wait_seconds')==0
    elif state.get('first_reserved_at') and state.get('refilled_at'):
        span=(datetime.fromisoformat(state['refilled_at'])-
              datetime.fromisoformat(state['first_reserved_at'])).total_seconds()
        tail=max(0,-state['available_bytes'])/rate
        # A millisecond covers separate SQL clock samples, not an extra chunk.
        allowed=budget['burst_bytes']+rate*(span+tail+0.001)
        checks['one_aggregate_reservation_envelope']=state['reserved_bytes']<=allowed
        envelope={'clock_span_seconds':span,'last_reservation_wait_seconds':tail,
                  'allowed_bytes_with_1ms_clock_tolerance':allowed,
                  'copy_duration_lower_bound_seconds':max(0,(expected_bytes-budget['burst_bytes'])/rate)}
    else:
        checks['one_aggregate_reservation_envelope']=False
    return {'valid':all(checks.values()),'checks':checks,'expected_bytes':expected_bytes,
            'expected_requests':requests,'reservation_envelope':envelope,
            'scope':'Eligibility reservations, not a hard instantaneous backend-write rate cap'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results',type=Path)
    args=parser.parse_args()
    result=audit(json.loads(args.results.read_text()))
    result['raw_results_sha256']=hashlib.sha256(args.results.read_bytes()).hexdigest()
    result['analysis_source_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (args.results.parent/'budget-analysis.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
    return int(not result['valid'])


if __name__=='__main__':raise SystemExit(main())
