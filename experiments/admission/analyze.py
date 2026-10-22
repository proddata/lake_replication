#!/usr/bin/env python3
"""Evaluate one matched extended cohort; never equate it with adoption approval."""
import argparse
import hashlib
import itertools
import json
import math
import statistics
from pathlib import Path


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def paired_bootstrap(ratios):
    if not ratios or len(ratios) > 6:
        raise ValueError('exact paired bootstrap requires one to six complete pairs')
    medians = [statistics.median(sample) for sample in itertools.product(ratios, repeat=len(ratios))]
    return {'pairs': len(ratios), 'paired_ratios': ratios,
            'median_ratio': statistics.median(ratios),
            'min_ratio': min(ratios), 'max_ratio': max(ratios),
            'bootstrap_median_95_interval': [percentile(medians, .025), percentile(medians, .975)],
            'bootstrap_samples': len(medians),
            'method': 'exact resampling of whole pairs; descriptive interval conditional on these local runs'}


def assess(ratios, limit, higher_is_better):
    result = paired_bootstrap(ratios)
    low, high = result['bootstrap_median_95_interval']
    if higher_is_better:
        state = 'within_proposed_margin_in_this_cohort' if low >= limit else 'outside_proposed_margin' if high < limit else 'inconclusive'
    else:
        state = 'within_proposed_margin_in_this_cohort' if high <= limit else 'outside_proposed_margin' if low > limit else 'inconclusive'
    result.update(proposed_ratio_limit=limit, assessment=state)
    if len(ratios) < 5:
        result['assessment'] = 'insufficient_repetitions'
    return result


def analyze(evidence, profile):
    if evidence['status'] != 'passed':
        raise ValueError('cohort did not complete successfully')
    parameters = evidence['parameters']
    observer_settings = {(case['workload']['observer_kind'], case['workload']['observer_poll_ms'])
                         for case in evidence['cases']}
    if len(observer_settings) != 1:
        raise ValueError('matched analysis requires one observer configuration')
    pairs = {}
    checks = []
    for case in evidence['cases']:
        repetition = case['repetition']
        by_variant = pairs.setdefault(repetition, {})
        if case['variant'] in by_variant:
            raise ValueError('duplicate variant in a pair')
        by_variant[case['variant']] = case
        work = case['workload']
        first_tx = work['transactions'][0]['transaction']
        last_tx = work['transactions'][-1]['transaction']
        poll = work['last_poll_while_source_active']
        end_backlog_upper = last_tx - max(first_tx - 1, poll['maximum_transaction']) if poll else len(work['transactions'])
        lateness = max(tx['insert_started_seconds'] - index * parameters['interval_ms'] / 1000
                       for index, tx in enumerate(work['transactions']))
        values = {'variant': case['variant'], 'repetition': repetition,
                  'p95_publication_commit_lag_ms': work['publication_commit_lag_ms']['p95'],
                  'publication_catchup_seconds': work['publication_catchup_seconds'],
                  'end_write_backlog_upper_transactions': end_backlog_upper,
                  'source_schedule_max_lateness_seconds': lateness,
                  'first_quarter_p95_publication_commit_lag_ms': work['first_quarter_p95_publication_commit_lag_ms'],
                  'last_quarter_p95_publication_commit_lag_ms': work['last_quarter_p95_publication_commit_lag_ms']}
        if profile == 'steady':
            values['absolute_checks'] = {
                'p95_below_two_seconds': values['p95_publication_commit_lag_ms'] < 2000,
                'publication_catchup_within_two_seconds': values['publication_catchup_seconds'] <= 2,
                'end_write_backlog_upper_at_most_four': end_backlog_upper <= 4,
                'source_schedule_lateness_within_one_interval': lateness <= parameters['interval_ms'] / 1000}
        checks.append(values)
    if any(set(pair) != {'static', 'base'} for pair in pairs.values()):
        raise ValueError('each repetition must contain both variants')
    ordered = [pairs[index] for index in sorted(pairs)]
    throughput_ratios = [pair['base']['workload']['publication_rows_per_second'] /
                         pair['static']['workload']['publication_rows_per_second'] for pair in ordered]
    lag_ratios = [pair['base']['workload']['publication_commit_lag_ms']['p95'] /
                  pair['static']['workload']['publication_commit_lag_ms']['p95'] for pair in ordered]
    expected = {'repetitions': 5, 'transactions': 120, 'warmup_transactions': 10,
                'rows_per_transaction': 100, 'payload_bytes': 128, 'idle_seconds': 5,
                'poll_ms': 100, 'observer': 'journal', 'interval_ms': 500 if profile == 'steady' else 100}
    protocol_match = (all(parameters.get(key) == value for key, value in expected.items())
                      and not parameters.get('base_preload')
                      and not parameters.get('observers') and not parameters.get('poll_intervals_ms')
                      and sorted(pairs) == list(range(1, 6))
                      and all(case.get('verified_ids_markers_and_payload') is True for case in evidence['cases']))
    return {'profile': profile, 'observer': list(next(iter(observer_settings))),
            'frozen_protocol_match': protocol_match,
            'case_checks': checks,
            'publication_p95_comparison': assess(lag_ratios, 1.10, False),
            'publication_throughput_comparison': assess(throughput_ratios, .95, True)
                if profile == 'catchup' else {'assessment': 'offered_load_limited_not_a_capacity_gate',
                                            **paired_bootstrap(throughput_ratios)},
            'steady_absolute_checks_all_pass': all(all(case['absolute_checks'].values()) for case in checks)
                if profile == 'steady' else None,
            'adoption_qualified': False,
            'limits': 'One database/subscription and inserts only. No ownership/upgrade, crash, scale, architecture or soak qualification.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path)
    parser.add_argument('--profile', choices=('steady', 'catchup'), required=True)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = analyze(json.loads(args.results.read_text()), args.profile)
    result['input_sha256'] = hashlib.sha256(args.results.read_bytes()).hexdigest()
    result['analyzer_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    text = json.dumps(result, indent=2, sort_keys=True) + '\n'
    if args.output:
        args.output.write_text(text)
        args.output.with_suffix('.py').write_bytes(Path(__file__).read_bytes())
    print(text, end='')


if __name__ == '__main__':
    main()
