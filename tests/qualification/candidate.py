#!/usr/bin/env python3
"""Accept qualification artifacts only from trusted CI at the dispatched commit."""
import json
import os
from pathlib import Path
import re
import sys


def validate(run, *, repository, revision, ref, default_branch):
    if not re.fullmatch(r'[0-9a-f]{40}', revision):
        raise ValueError('Expected an exact workflow commit')
    if not default_branch or ref != f'refs/heads/{default_branch}':
        raise ValueError('Qualification must be dispatched from the default branch')
    if run.get('status') != 'completed' or run.get('conclusion') != 'success':
        raise ValueError('Candidate CI must pass first')
    if run.get('path') != '.github/workflows/ci.yml':
        raise ValueError('Expected bundle-producing CI workflow')
    if (run.get('head_repository') or {}).get('full_name') != repository:
        raise ValueError('Expected this repository')
    if run.get('event') != 'push' or run.get('head_branch') != default_branch:
        raise ValueError('Candidate CI must come from a default-branch push')
    if run.get('head_sha') != revision:
        raise ValueError('Candidate CI must match the exact dispatched workflow commit')


def main():
    validate(json.loads(Path(sys.argv[1]).read_text()),
             repository=os.environ['GITHUB_REPOSITORY'], revision=os.environ['GITHUB_SHA'],
             ref=os.environ['GITHUB_REF'], default_branch=os.environ['DEFAULT_BRANCH'])
    print('Candidate CI matches the trusted workflow commit')


if __name__ == '__main__':
    main()
