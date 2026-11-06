#!/usr/bin/env python3
"""Independently check current Iceberg references in a live private MinIO fixture."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import time
from urllib.parse import urlparse


def object_file(uri):
    value = urlparse(uri)
    if (value.scheme != 's3' or value.netloc != 'lake-qualification' or
            not value.path.startswith('/iceberg/') or '..' in PurePosixPath(value.path).parts or
            '\n' in uri or '\r' in uri or value.query or value.fragment):
        raise ValueError('object reference is outside the private fixture')
    return '/data/' + value.netloc + value.path + '/xl.meta'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('result_directory', type=Path)
    args = parser.parse_args()
    directory = args.result_directory.resolve()
    result = json.loads((directory / 'results.json').read_text())
    project = 'lake-replication-' + result['run'].replace('_', '-') + '-soak'
    stamp = str(time.time_ns())
    evidence = dict(status='running', scope=__doc__, source_transactions_at_start=result['source_transactions'],
                    observer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    def command(arguments, **kwargs):
        return subprocess.run(arguments, text=True, capture_output=True, timeout=20, check=True, **kwargs)
    def container(service):
        found = command(['docker', 'ps', '-q', '--filter', 'label=com.docker.compose.project=' + project,
                         '--filter', 'label=com.docker.compose.service=' + service]).stdout.strip()
        if not found or '\n' in found:
            raise ValueError('expected one live fixture container: ' + service)
        return found
    try:
        sql = """SET statement_timeout='15s'; WITH current AS MATERIALIZED (
SELECT table_name::text AS name,metadata_location FROM lake_iceberg.tables_internal
WHERE table_name IN ('soak_items'::regclass,'soak_progress'::regclass)), refs AS (
SELECT name,metadata_location AS path FROM current UNION
SELECT name,ref.path FROM current CROSS JOIN LATERAL lake_iceberg.find_all_referenced_files(metadata_location) ref)
SELECT jsonb_agg(jsonb_build_array(name,path) ORDER BY name,path) FROM refs;"""
        rows = json.loads(command(['docker', 'exec', '-i', container('destination'), 'psql', '-XAtq',
            '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'analytics'], input=sql).stdout)
        files = sorted({object_file(uri) for _, uri in rows})
        command(['docker', 'exec', '-i', container('storage'), 'sh', '-ec',
                 'while IFS= read -r p; do test -f "$p" || { echo "missing object: $p"; exit 1; }; done'],
                input='\n'.join(files) + '\n')
        evidence.update(status='passed', tables=sorted({name for name, _ in rows}), references=len(files),
                        reference_sha256=hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest())
        filename = 'independent-references-' + stamp + '.json'
        (directory / filename).write_text(json.dumps(rows, indent=2) + '\n')
        evidence['reference_list_file'] = filename
    except Exception as error:
        evidence.update(status='error', error=str(error), stderr=getattr(error, 'stderr', None))
    finally:
        (directory / ('physical-reference-check-' + stamp + '.json')).write_text(json.dumps(evidence, indent=2) + '\n')
        print(json.dumps(evidence))
    return int(evidence['status'] != 'passed')


if __name__ == '__main__':
    raise SystemExit(main())
