"""Validate paired performance cohorts without building historical overlays."""
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('admission_measure', Path(__file__).with_name('admission_measure.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

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
