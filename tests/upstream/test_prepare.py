"""Exercise offline bundle creation with real full and shallow Git repositories."""
import contextlib
import importlib.util
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / 'deploy/prepare-upstream.py'
spec = importlib.util.spec_from_file_location('prepare_upstream', SCRIPT)
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


def git(path, *args):
    return subprocess.check_output(
        ['git', '-C', str(path), *args], text=True, stderr=subprocess.PIPE
    ).strip()


class BundleTests(unittest.TestCase):
    def test_full_history_bundle_clones_offline_at_requested_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.repository(root)
            revision = git(source, 'rev-parse', 'HEAD')
            self.export(root, source, revision)
            clone = root / 'consumer'
            git(root, 'clone', str(root / '.upstream/pg_lake.bundle'), str(clone))
            self.assertEqual(git(clone, 'rev-parse', 'HEAD'), revision)
            self.assertEqual(git(clone, 'rev-list', '--count', 'HEAD'), '3')
            self.assertEqual(git(source, 'rev-parse', 'HEAD'), revision)

    def test_shallow_history_is_rejected_before_writing_bundle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.repository(root)
            shallow = root / 'shallow'
            git(root, 'clone', '--depth=1', source.as_uri(), str(shallow))
            self.assertEqual(git(shallow, 'rev-parse', '--is-shallow-repository'), 'true')
            with self.assertRaisesRegex(RuntimeError, 'history is shallow'):
                self.export(root, shallow, git(shallow, 'rev-parse', 'HEAD'))
            self.assertFalse((root / '.upstream/pg_lake.bundle').exists())

    def repository(self, root):
        source = root / 'source'
        git(root, 'init', str(source))
        git(source, 'config', 'user.name', 'Offline fixture')
        git(source, 'config', 'user.email', 'fixture@example.invalid')
        git(source, 'config', 'commit.gpgsign', 'false')
        git(source, 'config', 'core.hooksPath', '/dev/null')
        for number in range(3):
            (source / 'content').write_text(str(number))
            git(source, 'add', 'content')
            git(source, 'commit', '-m', f'fixture {number}')
        return source

    def export(self, root, source, revision):
        with patch.object(prepare, 'ROOT', root), patch.object(prepare, 'SOURCE', source):
            with patch.object(sys, 'argv', [str(SCRIPT), revision]):
                with contextlib.redirect_stdout(io.StringIO()):
                    prepare.main()


if __name__ == '__main__':
    unittest.main()
