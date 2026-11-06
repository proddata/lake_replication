"""Reject modified or mixed prerequisite sources before patching or installing."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('pg_lake_patchset', Path(__file__).resolve().parents[2] / 'deploy/pg_lake/patchset.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class PatchsetTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / 'source'
        self.source.mkdir()
        self.here = self.root / 'patchset'
        (self.here / 'patches').mkdir(parents=True)
        self.patch = self.here / 'patches/skip-clean-manifests.patch'
        self.patch.write_text('--- a/manifest.c\n+++ b/manifest.c\n@@ -1 +1 @@\n-before\n+after\n')
        (self.source / 'manifest.c').write_text('before\n')
        manifest = dict(patchset='fixture', upstream_revisions=['a' * 40],
                        patch_sha256=p.digest(self.patch), files={'manifest.c': {
                            state + '_sha256': hashlib.sha256((state + '\n').encode()).hexdigest()
                            for state in ('before', 'after')}})
        (self.here / 'patchset.json').write_text(json.dumps(manifest))
        self.revision = 'a' * 40
        self.changes = ''
        self.original = p.command
        def run(args, **kwargs):
            if 'rev-parse' in args:
                return SimpleNamespace(stdout=self.revision + '\n')
            if 'diff' in args:
                return SimpleNamespace(stdout=self.changes)
            return self.original(args, **kwargs)
        self.addCleanup(patch.stopall)
        patch.object(p, 'HERE', self.here).start()
        patch.object(p, 'command', side_effect=run).start()

    def test_applies_once_and_verifies_already_applied_sources(self):
        self.assertEqual(p.apply(self.source)['state'], 'after')
        self.assertEqual((self.source / 'manifest.c').read_text(), 'after\n')
        self.assertEqual(p.apply(self.source)['state'], 'after')

    def test_rejects_other_revision_before_mutation(self):
        self.revision = 'b' * 40
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            p.apply(self.source)
        self.assertEqual((self.source / 'manifest.c').read_text(), 'before\n')

    def test_rejects_modified_patch_before_mutation(self):
        self.patch.write_text(self.patch.read_text() + '\n')
        with self.assertRaisesRegex(ValueError, 'patch checksum'):
            p.apply(self.source)

    def test_rejects_modified_source(self):
        (self.source / 'manifest.c').write_text('unexpected\n')
        with self.assertRaisesRegex(ValueError, 'partly patched'):
            p.apply(self.source)

    def test_rejects_unrelated_source_changes(self):
        self.changes = 'another-module.c\n'
        with self.assertRaisesRegex(ValueError, 'unrelated'):
            p.apply(self.source)

    def test_second_module_failure_does_not_install_the_first_module(self):
        run = p.command
        calls = []
        def fail_second(args, **kwargs):
            calls.append(args)
            if args == ['/fixture/pg_config', '--version']:
                return SimpleNamespace(stdout='PostgreSQL 18.6\n')
            if args[0] == 'make':
                if args[2].endswith('pg_lake_table'):
                    raise subprocess.CalledProcessError(1, args)
                return SimpleNamespace(stdout='')
            return run(args, **kwargs)
        with patch.object(p, 'command', side_effect=fail_second), patch.object(p.shutil, 'which', return_value='/fixture/pg_config'):
            with self.assertRaises(subprocess.CalledProcessError):
                p.install(self.source, 2)
        self.assertEqual(sum(args[0] == 'make' for args in calls), 2)
        self.assertFalse(any('--pkglibdir' in args or '--sharedir' in args for args in calls))
        self.assertTrue(all('PG_CONFIG=/fixture/pg_config' in args for args in calls if args[0] == 'make'))

    def test_wrong_postgresql_major_is_rejected_before_source_changes(self):
        with patch.object(p.shutil, 'which', return_value='/fixture/pg_config'), patch.object(p, 'command', return_value=SimpleNamespace(stdout='PostgreSQL 17.6\n')):
            with self.assertRaisesRegex(ValueError, 'requires PostgreSQL 18'):
                p.install(self.source, 2)
        self.assertEqual((self.source / 'manifest.c').read_text(), 'before\n')


if __name__ == '__main__':
    unittest.main()
