"""Reject untrusted runs before qualification downloads or executes their artifacts."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('candidate', Path(__file__).with_name('candidate.py'))
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.context = dict(repository='proddata/lake_replication', revision='a' * 40,
                            ref='refs/heads/main', default_branch='main')
        self.run = dict(status='completed', conclusion='success',
                        path='.github/workflows/ci.yml', event='push', head_branch='main',
                        head_repository={'full_name': self.context['repository']}, head_sha='a' * 40)

    def test_accepts_successful_default_branch_push_at_exact_workflow_commit(self):
        candidate.validate(self.run, **self.context)

    def test_rejects_same_repository_pull_request_even_at_matching_commit(self):
        self.run['event'] = 'pull_request'
        with self.assertRaisesRegex(ValueError, 'default-branch push'):
            candidate.validate(self.run, **self.context)

    def test_rejects_failed_incomplete_wrong_workflow_fork_branch_and_old_commit(self):
        for change in ({'conclusion': 'failure'}, {'status': 'in_progress'},
                       {'path': '.github/workflows/other.yml'}, {'head_repository': None},
                       {'head_repository': {'full_name': 'attacker/lake_replication'}},
                       {'head_branch': 'unreviewed'}, {'head_sha': 'b' * 40}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                candidate.validate({**self.run, **change}, **self.context)

    def test_rejects_dispatch_from_unreviewed_branch_or_tag(self):
        for ref in ('refs/heads/unreviewed', 'refs/tags/v0.1.0-beta.5'):
            with self.subTest(ref=ref), self.assertRaisesRegex(ValueError, 'default branch'):
                candidate.validate(self.run, **{**self.context, 'ref': ref})

    def test_rejects_missing_or_nonexact_trust_context(self):
        for change in ({'default_branch': ''}, {'revision': 'main'}, {'revision': 'a' * 39}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                candidate.validate(self.run, **{**self.context, **change})


if __name__ == '__main__':
    unittest.main()
