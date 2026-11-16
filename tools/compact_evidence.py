#!/usr/bin/env python3
"""Derive checkout-sized evidence summaries from a pinned, archived Git revision."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess

ROOT = Path(__file__).resolve().parents[1]
RAW_KEYS = {
    'files_sha256', 'worktree_status', 'observation_timeline',
    'visibility_timeline', 'file_samples', 'retained_wal_samples_bytes',
    'samples', 'other_running_containers', 'docker_info', 'trace',
    'application_xids', 'resources_before', 'resources_after',
    'catalog_statements',
}

def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()

def omitted(value, reason):
    result = dict(_omitted_detail=True, reason=reason,
                  canonical_json_sha256=hashlib.sha256(encoded(value)).hexdigest())
    if isinstance(value, (dict, list, str)):
        result['entries_or_characters'] = len(value)
    return result

def window_ranges(windows):
    phases = {}
    measured = {'worker_wall_seconds', 'worker_phases_ms', 'cpu_seconds',
                'operation_elapsed', 'native_catalog', 'files_before', 'files_after'}
    def numbers(value, path, result):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result.setdefault(path, []).append(value)
        elif isinstance(value, dict):
            for key, child in value.items():
                numbers(child, path + '.' + key, result)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                numbers(child, path + '[' + str(index) + ']', result)
    for window in windows:
        phase = phases.setdefault(window['phase'], dict(windows=0, metrics={}))
        phase['windows'] += 1
        for key in measured & window.keys():
            numbers(window[key], key, phase['metrics'])
    for phase in phases.values():
        phase['metrics'] = {key: dict(count=len(values), minimum=min(values),
            median=statistics.median(values), maximum=max(values))
            for key, values in sorted(phase['metrics'].items())}
    return phases

def compact(value, key=''):
    if key == 'windows' and isinstance(value, list) and value and all(
            isinstance(v, dict) and 'phase' in v for v in value):
        return dict(**omitted(value, 'Per-window dumps are archived; measured ranges are grouped by phase'),
                    phase_ranges=window_ranges(value))
    if key == 'checks' and isinstance(value, list) and value and all(
            isinstance(v, dict) and isinstance(v.get('passed'), bool) for v in value):
        return dict(**omitted(value, 'Individual successful assertion values are archived'),
                    total=len(value), passed=sum(v['passed'] for v in value),
                    failed=sum(not v['passed'] for v in value),
                    failures=[compact(v) for v in value if not v['passed']])
    if key == 'failure_container_states' and isinstance(value, list):
        return dict(**omitted(value, 'Full container inspection is archived'),
                    states=[{k: v[k] for k in ('Id', 'Name', 'State') if k in v}
                            for v in value if isinstance(v, dict)])
    if key in RAW_KEYS and isinstance(value, (dict, list)):
        return omitted(value, 'Raw timeline, inspection or repeated provenance is archived')
    if isinstance(value, str) and len(value.encode()) > 8192:
        return omitted(value, 'Full log, source text or row-value dump is archived')
    if isinstance(value, dict):
        return {k: compact(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [compact(v) for v in value]
    return value

def verify(original, summary):
    """Every unabridged field must retain its original value and type."""
    if isinstance(summary, dict) and summary.get('_omitted_detail') is True:
        assert summary['canonical_json_sha256'] == hashlib.sha256(encoded(original)).hexdigest()
        if 'total' in summary:
            assert summary['total'] == len(original)
            assert summary['failed'] == sum(not v['passed'] for v in original)
            assert summary['passed'] + summary['failed'] == summary['total']
            for raw, kept in zip((v for v in original if not v['passed']), summary['failures']):
                verify(raw, kept)
        if 'phase_ranges' in summary:
            assert summary['phase_ranges'] == window_ranges(original)
        if 'states' in summary:
            expected = [{k: v[k] for k in ('Id', 'Name', 'State') if k in v}
                        for v in original if isinstance(v, dict)]
            assert summary['states'] == expected
        return
    assert type(original) is type(summary)
    if isinstance(original, dict):
        assert original.keys() == summary.keys()
        for key in original:
            verify(original[key], summary[key])
    elif isinstance(original, list):
        assert len(original) == len(summary)
        for raw, kept in zip(original, summary):
            verify(raw, kept)
    else:
        assert original == summary

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-ref', required=True, help='Pinned raw archive commit or tag')
    parser.add_argument('--apply', action='store_true', help='Write summaries and remove archived-only files')
    args = parser.parse_args()
    def git(*arguments):
        return subprocess.check_output(['git', *arguments], cwd=ROOT)
    revision = git('rev-parse', '--verify', args.source_ref + '^{commit}').decode().strip()
    names = [n for n in git('ls-tree', '-r', '--name-only', revision, '--', 'docs/evidence').decode().splitlines()
             if n.endswith('.json') and Path(n).parent.as_posix() == 'docs/evidence']
    active_text = '\n'.join(p.read_text() for section in ('guides', 'design', 'performance', 'qualification', 'roadmap')
                            for p in (ROOT / 'docs' / section).glob('*.md'))
    keep = {name for name in names if ('wal-direct' in name or Path(name).name in active_text)
            and '-push-' not in name}
    entries = []
    before = after = 0
    for name in names:
        raw = git('show', revision + ':' + name)
        original = json.loads(raw)
        source = dict(commit=revision, path=name, sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw),
                      url=f'https://github.com/proddata/lake_replication/blob/{revision}/{name}')
        entry = dict(name=Path(name).name, retention='compact_summary' if name in keep else 'archive_only', source=source)
        for key in ('status', 'scope', 'all_configurations_passed', 'all_load_envelopes_passed', 'adoption_qualified'):
            if key in original:
                entry[key] = original[key]
        entries.append(entry)
        before += len(raw)
        target = ROOT / name
        if name in keep:
            summary = compact(original)
            verify(original, summary)
            assert '_retention' not in summary
            summary = dict(_retention=dict(format='derived_summary_v1', raw_source=source,
                           detail_policy='Raw timelines, repeated source hashes, large dumps and successful assertion values are archived; outcomes, reported metrics, failures and scalar metadata remain. Per-window measurements retain phase ranges.'), **summary)
            data = json.dumps(summary, indent=2, ensure_ascii=False) + '\n'
            after += len(data.encode())
            if args.apply:
                target.write_text(data)
        elif args.apply and target.exists():
            target.unlink()
    catalog = dict(format='evidence_archive_index_v1', raw_source_commit=revision,
                   original_files=len(names), original_bytes=before,
                   compact_files=len(keep), compact_bytes=after, entries=entries)
    if args.apply:
        (ROOT / 'docs/evidence/archive-index.json').write_text(json.dumps(catalog, indent=2) + '\n')
    print(json.dumps({k: v for k, v in catalog.items() if k != 'entries'}, indent=2))

if __name__ == '__main__':
    main()
