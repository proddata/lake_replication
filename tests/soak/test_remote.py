"""Remote pilots must reject a wrong candidate, corruption and inadequate disk."""
import hashlib
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import Mock


spec = importlib.util.spec_from_file_location('remote_soak', Path(__file__).resolve().parents[2] / 'deploy/soak/run.py')
remote = importlib.util.module_from_spec(spec)
spec.loader.exec_module(remote)


class RemoteTests(unittest.TestCase):
    def archive(self, directory, duplicate=False):
        path = Path(directory) / 'candidate.tar.gz'
        with tarfile.open(path, 'w:gz') as bundle:
            fields = [('SOURCE_REVISION', 'a' * 40), ('TARGETARCH', 'amd64')]
            if duplicate:
                fields.append(('SOURCE_REVISION', 'b' * 40))
            for number, (name, value) in enumerate(fields):
                member = tarfile.TarInfo(f'bundle-{number}/{name}')
                member.size = len(value)
                bundle.addfile(member, io.BytesIO(value.encode()))
        Path(str(path) + '.sha256').write_text(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + path.name + '\n')
        return path

    def test_candidate_must_match_both_revision_and_native_architecture(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = self.archive(directory)
            self.assertEqual(remote.archive_identity(archive, 'a' * 40, 'amd64')['TARGETARCH'], 'amd64')
            for revision, architecture in [('b' * 40, 'amd64'), ('a' * 40, 'arm64')]:
                with self.assertRaisesRegex(ValueError, 'revision or native architecture'):
                    remote.archive_identity(archive, revision, architecture)

    def test_corruption_is_rejected_before_tar_is_read(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = self.archive(directory)
            with archive.open('ab') as output:
                output.write(b'corruption')
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                remote.archive_identity(archive, 'a' * 40, 'amd64')

    def test_ambiguous_archive_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'invalid archive identity'):
                remote.archive_identity(self.archive(directory, duplicate=True), 'a' * 40, 'amd64')

    def test_capacity_includes_object_budget_and_free_space_reserve(self):
        remote.capacity(80 * remote.GIB, 65536, 16384)
        with self.assertRaisesRegex(ValueError, 'insufficient free space'):
            remote.capacity(79 * remote.GIB, 65536, 16384)
