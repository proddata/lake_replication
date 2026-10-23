#!/usr/bin/env python3
"""Five alternating paired cohorts for the current bulk-only candidate."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

from reserved_probe import ROOT, m, c, q, s

a = m.a
VARIANTS = ('unbounded', 'bulk_only')


def analyze(cases, profiles):
    results = {}
    for profile in profiles:
        pairs = {}
        for case in cases:
            if case['profile'] != profile:
                continue
            pair = pairs.setdefault(case['repetition'], {})
            if case['variant'] in pair:
                raise ValueError('duplicate paired observation')
            pair[case['variant']] = case
        if sorted(pairs) != list(range(1, 6)) or any(set(p) != set(VARIANTS) for p in pairs.values()):
            raise ValueError('five complete pairs required')
        ordered = [pairs[i] for i in sorted(pairs)]
        if profile == 'single_copy':
            if any(c['status'] != 'passed' or not c['timing_validation']['valid'] for p in ordered for c in p.values()):
                raise ValueError('invalid copy case cannot qualify overhead')
            ratios = [p['bulk_only']['copy_elapsed_seconds']/p['unbounded']['copy_elapsed_seconds'] for p in ordered]
            results[profile] = {'copy_duration': a.assess(ratios, 1.10, False),
                                'absolute_checks_all_pass': True}
        else:
            for pair in ordered:
                for case in pair.values():
                    if not all(m.timing_validation(case[phase])['valid'] for phase in ('warmup', 'workload')):
                        raise ValueError('clock discontinuity invalidates cohort')
            # Reuse the frozen admission metrics with an explicit candidate alias;
            # preserve original variant labels in all raw evidence.
            aliased = [{**case, 'variant': 'bounded' if case['variant'] == 'bulk_only' else 'unbounded'}
                       for pair in ordered for case in pair.values()]
            results[profile] = m.comparisons(aliased)[profile]
    gates = []
    for profile, values in results.items():
        for metric in ('p95', 'copy_duration'):
            if metric in values:
                gates.append(values[metric]['assessment'] == 'within_proposed_margin_in_this_cohort')
        if profile == 'catchup':
            gates.append(values['throughput']['assessment'] == 'within_proposed_margin_in_this_cohort')
        gates.append(values.get('absolute_checks_all_pass', True))
    return {'profiles': results, 'all_requested_gates_pass': all(gates),
            'production_adoption_qualified': False,
            'scope': 'Local paired cohort only; packaging and release soak remain required.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profiles', nargs='+', choices=('steady', 'catchup', 'single_copy'), default=['steady', 'catchup', 'single_copy'])
    args = parser.parse_args()
    if len(set(args.profiles)) != len(args.profiles):
        parser.error('duplicate profiles')
    root = ROOT / '.qualification' / (q.RUN + '_bulk_performance')
    root.mkdir(parents=True)
    q.RESULTS = root
    q.PROJECT += '-bulk-performance'
    q.SOURCE_IMAGE += '-bulk-performance'
    q.DESTINATION_IMAGE += '-bulk-performance'
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE, QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result = dict(status='running', scope=__doc__, profiles=args.profiles, cases=[], production_changed=False,
                  protocol=dict(pairs=5, order='alternating AB/BA by repetition', transactions=120,
                                rows_per_transaction=100, payload_bytes=128, warmup_transactions=10,
                                idle_seconds=5, poll_ms=100, observer='journal',
                                steady_interval_ms=500, catchup_interval_ms=100,
                                single_copy_rows=2000000, single_copy_stream_transactions=120,
                                p95_ratio_limit=1.10, catchup_throughput_ratio_floor=.95,
                                single_copy_duration_ratio_limit=1.10))
    paths = [p for p in Path(__file__).parent.glob('*') if p.is_file()]
    paths += [ROOT/'extensions/lake_replication/lake_replication.c']
    paths += list((ROOT/'migrations').glob('*.sql'))
    result['inputs'] = {}
    for path in paths:
        relative = path.relative_to(ROOT)
        result['inputs'][str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
        snapshot = root/'sources'/relative
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, snapshot)
    try:
        archive = q.archive_path()
        q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-bulk-performance-') as temp:
            q.build_images(archive, Path(temp))
        base = q.DESTINATION_IMAGE
        candidate = f'lake-replication-bulk-performance:{q.RUN}'
        build = q.command(['docker','build','--build-arg',f'QUAL_IMAGE={base}',
                           '--build-arg','RESERVED=1','--build-arg','BULK_ONLY=1',
                           '-t',candidate,'-f',str(Path(__file__).with_name('Dockerfile.reserved')),str(ROOT)], timeout=600)
        (root/'build.log').write_text(build.stdout+build.stderr)
        images = dict(unbounded=base, bulk_only=candidate)
        result['images'] = {v:q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip() for v,image in images.items()}
        text = m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
        for profile in args.profiles:
            for repetition in range(1,6):
                order = VARIANTS if repetition % 2 else VARIANTS[::-1]
                for variant in order:
                    label = f'{profile}-{repetition}-{variant}'
                    print('CASE',label,flush=True)
                    directory = root/label
                    if profile == 'single_copy':
                        # One bulk job does not require a queue to prove contention.
                        # Streaming limits and exact contents still apply.
                        params = argparse.Namespace(copy_rows=2000000,stream_transactions=120,
                                                    bulk_copies=1,seed_after_subscriptions=False,require_queue=False)
                        measured = m.fairness(params,variant,images[variant],text,directory)
                        measured['timing_validation'] = m.fairness_timing_validation(measured)
                        measured['queue_required'] = False
                        if measured['status'] != 'passed' or not measured['timing_validation']['valid']:
                            raise RuntimeError(f'{label} failed: {measured.get("error",measured.get("checks"))}')
                    else:
                        directory.mkdir()
                        params = argparse.Namespace(transactions=120,rows_per_transaction=100,payload_bytes=128,
                            warmup_transactions=10,idle_seconds=5,poll_ms=100,observer='journal',
                            interval_ms=500 if profile=='steady' else 100,catchup_timeout=180,base_preload=False)
                        measured = c.run_case(params,'base',repetition,images[variant],text,directory)
                        measured['variant'] = variant
                        for phase in ('warmup','workload'):
                            if not m.timing_validation(measured[phase])['valid']:
                                raise RuntimeError(f'{label} {phase} clock audit failed')
                        logs = (directory/'compose.log').read_text()
                        measured['engine_oom'] = 'Out of Memory Error' in logs
                        measured['unexpected_pg_crash'] = 'reinitializing' in logs
                        if measured['engine_oom'] or measured['unexpected_pg_crash']:
                            raise RuntimeError(f'{label} engine OOM or PostgreSQL crash')
                    measured.update(profile=profile,repetition=repetition,variant=variant)
                    result['cases'].append(measured)
                    s.write_json(directory/'results.json',measured)
                    s.write_json(root/'results.json',result)
                    print('PASS',label,flush=True)
        result['analysis'] = analyze(result['cases'],args.profiles)
        result['status'] = 'passed' if result['analysis']['all_requested_gates_pass'] else 'completed_with_findings'
    except Exception as error:
        result.update(status='error',error=str(error),error_type=type(error).__name__)
        print('ERROR:',error,flush=True)
    finally:
        result['archive_provenance'] = q.EVIDENCE
        s.write_json(root/'results.json',result)
        print('Evidence:',root,flush=True)
    return int(result['status']=='error')


if __name__ == '__main__':
    raise SystemExit(main())
