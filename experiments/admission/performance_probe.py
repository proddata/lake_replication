#!/usr/bin/env python3
"""Five alternating paired cohorts for historical engine-admission candidates."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import tarfile

from reserved_probe import ROOT, m, c, q, s

a = m.a
VARIANTS = ('unbounded', 'bulk_only')


def analyze(cases, profiles, variants=VARIANTS):
    if len(variants) != 2 or len(set(variants)) != 2:
        raise ValueError('two distinct variants required')
    if not profiles or len(set(profiles)) != len(profiles):
        raise ValueError('distinct nonempty profiles required')
    baseline, candidate = variants
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
        if sorted(pairs) != list(range(1, 6)) or any(set(p) != set(variants) for p in pairs.values()):
            raise ValueError('five complete pairs required')
        ordered = [pairs[i] for i in sorted(pairs)]
        if profile == 'single_copy':
            if any(c['status'] != 'passed' or not c['timing_validation']['valid'] for p in ordered for c in p.values()):
                raise ValueError('invalid copy case cannot qualify overhead')
            ratios = [p[candidate]['copy_elapsed_seconds']/p[baseline]['copy_elapsed_seconds'] for p in ordered]
            results[profile] = {'copy_duration': a.assess(ratios, 1.10, False),
                                'absolute_checks_all_pass': True}
        else:
            for pair in ordered:
                for case in pair.values():
                    if not all(m.timing_validation(case[phase])['valid'] for phase in ('warmup', 'workload')):
                        raise ValueError('clock discontinuity invalidates cohort')
            # Reuse the frozen admission metrics with an explicit candidate alias;
            # preserve original variant labels in all raw evidence.
            aliased = [{**case, 'variant': 'bounded' if case['variant'] == candidate else 'unbounded'}
                       for pair in ordered for case in pair.values()]
            results[profile] = m.comparisons(aliased)[profile]
            for check in results[profile].get('absolute_checks', []):
                check['variant'] = candidate if check['variant'] == 'bounded' else baseline
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
            'variants': list(variants),
            'scope': 'Local paired cohort only; packaging and release soak remain required.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profiles', nargs='+', choices=('steady', 'catchup', 'single_copy'), default=['steady', 'catchup', 'single_copy'])
    parser.add_argument('--policy', choices=('bulk_only', 'stream_pool'), default='bulk_only')
    parser.add_argument('--order-offset', type=int, choices=(0,1), default=0)
    args = parser.parse_args()
    if args.policy == 'stream_pool' and 'PG_FUNCTION_INFO_V1(lake_replication_admit_stream)' in (ROOT/'extensions/lake_replication/lake_replication.c').read_text():
        parser.error('historical overlay requires its retained pre-adoption source snapshot; use tests/admission/probe.py for production packages')
    expected_version = '0.1.0-beta.3' if args.policy == 'stream_pool' else '0.1.0-beta.2'
    if q.VERSION != expected_version:
        parser.error(f'{args.policy} cohort requires VERSION={expected_version}')
    variants = ('bulk_only', 'stream_pool') if args.policy == 'stream_pool' else VARIANTS
    suffix = 'stream_pool_performance' if args.policy == 'stream_pool' else 'bulk_performance'
    if len(set(args.profiles)) != len(args.profiles):
        parser.error('duplicate profiles')
    root = ROOT / '.qualification' / (q.RUN + '_' + suffix)
    root.mkdir(parents=True)
    q.RESULTS = root
    q.PROJECT += '-' + suffix.replace('_', '-')
    q.SOURCE_IMAGE += '-' + suffix.replace('_', '-')
    q.DESTINATION_IMAGE += '-' + suffix.replace('_', '-')
    q.ENV.update(QUALIFICATION_SOURCE_IMAGE=q.SOURCE_IMAGE, QUALIFICATION_DESTINATION_IMAGE=q.DESTINATION_IMAGE)
    result = dict(status='running', scope='Paired experimental admission performance; no production policy changes',
                  policy=args.policy, variants=list(variants), profiles=args.profiles, cases=[], production_changed=False,
                  protocol=dict(pairs=5, order='alternating BA/AB by repetition' if args.order_offset else 'alternating AB/BA by repetition', transactions=120,
                                rows_per_transaction=100, payload_bytes=128, warmup_transactions=10,
                                idle_seconds=5, poll_ms=100, observer='journal',
                                steady_interval_ms=500, catchup_interval_ms=100,
                                single_copy_rows=2000000, single_copy_stream_transactions=120,
                                p95_ratio_limit=1.10, catchup_throughput_ratio_floor=.95,
                                single_copy_duration_ratio_limit=1.10))
    paths = [p for p in Path(__file__).parent.glob('*') if p.is_file()]
    paths += [ROOT/'extensions/lake_replication/lake_replication.c', ROOT/'tests/qualification/probe.py', ROOT/'worker/Cargo.toml', ROOT/'Cargo.lock']
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
        with tarfile.open(archive) as bundle:
            versions = [x for x in bundle.getmembers() if x.name.endswith('/VERSION')]
            if len(versions) != 1 or bundle.extractfile(versions[0]).read().decode().strip() != expected_version:
                raise ValueError('archive version does not match the selected policy cohort')
        result['archive_version'] = expected_version
        with tempfile.TemporaryDirectory(prefix='lake-bulk-performance-') as temp:
            q.build_images(archive, Path(temp))
        base = q.DESTINATION_IMAGE
        candidate = f'lake-replication-{suffix.replace("_", "-")}:{q.RUN}'
        dockerfile = 'Dockerfile.stream-pool' if args.policy == 'stream_pool' else 'Dockerfile.reserved'
        build_args = [] if args.policy == 'stream_pool' else ['--build-arg','RESERVED=1','--build-arg','BULK_ONLY=1']
        build = q.command(['docker','build','--build-arg',f'QUAL_IMAGE={base}', *build_args,
                           '-t',candidate,'-f',str(Path(__file__).with_name(dockerfile)),str(ROOT)], timeout=600)
        (root/'build.log').write_text(build.stdout+build.stderr)
        images = dict(zip(variants, (base, candidate)))
        result['images'] = {v:q.command(['docker','image','inspect','--format','{{.Id}}',image]).stdout.strip() for v,image in images.items()}
        text = m.compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
        for profile in args.profiles:
            for repetition in range(1,6):
                order = variants if (repetition + args.order_offset) % 2 else variants[::-1]
                for variant in order:
                    label = f'{profile}-{repetition}-{variant}'
                    print('CASE',label,flush=True)
                    directory = root/label
                    if profile == 'single_copy':
                        # One bulk job does not require a queue to prove contention.
                        # Streaming limits and exact contents still apply.
                        params = argparse.Namespace(copy_rows=2000000,stream_transactions=120,
                                                    bulk_copies=1,seed_after_subscriptions=False,require_queue=False)
                        measured_variant = variant
                        if args.policy == 'stream_pool':
                            params.admission_view = 'lake_replication.bulk_admission'
                            measured_variant = 'bulk_only'
                        measured = m.fairness(params,measured_variant,images[variant],text,directory)
                        measured['timing_validation'] = m.fairness_timing_validation(measured)
                        measured['queue_required'] = False
                        s.write_json(directory/'results.json',measured)
                        if measured['status'] != 'passed' or not measured['timing_validation']['valid']:
                            raise RuntimeError(f'{label} failed: {measured.get("error",measured.get("checks"))}')
                    else:
                        directory.mkdir()
                        params = argparse.Namespace(transactions=120,rows_per_transaction=100,payload_bytes=128,
                            warmup_transactions=10,idle_seconds=5,poll_ms=100,observer='journal',
                            interval_ms=500 if profile=='steady' else 100,catchup_timeout=180,base_preload=False)
                        measured = c.run_case(params,'base',repetition,images[variant],text,directory)
                        measured['variant'] = variant
                        s.write_json(directory/'results.json',measured)
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
        result['analysis'] = analyze(result['cases'],args.profiles,variants)
        result['status'] = 'passed' if result['analysis']['all_requested_gates_pass'] else 'completed_with_findings'
    except Exception as error:
        result.update(status='error',error=str(error),error_type=type(error).__name__)
        print('ERROR:',error,flush=True)
    finally:
        result['archive_provenance'] = q.EVIDENCE
        s.write_json(root/'results.json',result)
        print('Evidence:',root,flush=True)
    return int(result['status']!='passed')


if __name__ == '__main__':
    raise SystemExit(main())
