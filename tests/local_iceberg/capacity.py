#!/usr/bin/env python3
"""Map bounded transaction-rate/size trade-offs without a production estimate."""
from datetime import datetime,timezone
import hashlib
import json
import sys
import uuid
from pathlib import Path
from benchmark import LocalBenchmark,ROOT

# The first three cases offer the same 800 rows/second across two tables,
# exposing the cost of transaction geometry. The last raises bytes/transaction.
CASES=[
    dict(name='4 transactions per second, 100 rows per table',transactions=40,rows=100,interval=.25),
    dict(name='40 transactions per second, 10 rows per table',transactions=400,rows=10,interval=.025),
    dict(name='100 transactions per second, 4 rows per table',transactions=1000,rows=4,interval=.01),
    dict(name='4 transactions per second, 1000 rows per table',transactions=40,rows=1000,interval=.25),
]


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case',type=int,choices=range(1,len(CASES)+1))
    parser.add_argument('--group-inserts',action='store_true')
    parser.add_argument('--journal-transactions',type=int)
    args=parser.parse_args()
    if args.journal_transactions is not None and not 1<=args.journal_transactions<=128:parser.error('journal transaction bound must be 1..128')
    output=ROOT/'.qualification'/('local-iceberg-capacity-'+datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]);output.mkdir(parents=True)
    result=dict(scope='bounded local transaction-rate and transaction-size map',status='running',cases=[],settings=vars(args),
        methodology='Fresh private baseline and mirror volumes per case; order alternates. One-second publication coalescing. One persistent serial application writer. Requested rate is offered load, not guaranteed achieved throughput. Each write phase targets ten seconds; publication may drain afterward for at most ninety seconds. Resource and backlog observations include observer cost and sampling delay. Cases are sequential; no competing qualification jobs. No cold-cache isolation, capacity maximum, production sizing or confidence interval is established. Failed points are retained and do not stop the remaining cases.',
        files_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),ROOT/'tests/local_iceberg/benchmark.py']})
    def save():
        temporary=output/'results.tmp';temporary.write_text(json.dumps(result,indent=2)+'\n');temporary.replace(output/'results.json')
    save()
    try:
        for index,case in enumerate(CASES):
            if args.case is not None and args.case!=index+1:continue
            item=dict(workload=case,runs=[]);result['cases'].append(item);save()
            for mirror in ([False,True] if index%2==0 else [True,False]):
                print('Measuring '+case['name']+(' with local mirroring' if mirror else ' without mirroring'),flush=True)
                probe=LocalBenchmark(mirror,case['transactions'],case['rows'],case['interval'],1000,args.group_inserts,args.journal_transactions)
                code=probe.run()
                item['runs'].append(dict(mode='local_mirror' if mirror else 'application_only',passed=code==0,
                    evidence=str((probe.output/'results.json').relative_to(ROOT)),measurement=probe.evidence.get('measurement',{}),
                    images=probe.evidence.get('images',{}),error=probe.evidence.get('error')));save()
            item['status']='passed' if all(r['passed'] for r in item['runs']) else 'failed';save()
        failed=sum(c['status']=='failed' for c in result['cases'])
        result.update(status='completed_with_failures' if failed else 'passed',failed_cases=failed);save()
        return 1 if failed else 0
    except Exception as error:
        result.update(status='error',error=str(error));save();print('ERROR: '+str(error),file=sys.stderr,flush=True);return 1
    finally:print('Capacity evidence: '+str(output/'results.json'),flush=True)


if __name__=='__main__':sys.exit(main())
