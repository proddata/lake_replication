"""Exercise the launcher's actual POSIX log opener without PostgreSQL headers."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class SecureLogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory(prefix='lake-log-test-')
        cls.binary = Path(cls.build.name) / 'open-log'
        source = Path(cls.build.name) / 'open-log.c'
        source.write_text(r'''
#include "secure_log.h"
#include <string.h>
int main(int argc, char **argv) {
    int fd;
    if (argc != 3) return 2;
    fd = lake_open_private_log(argv[1], argv[2]);
    if (fd < 0) return 1;
    if (!(fcntl(fd, F_GETFD) & FD_CLOEXEC)) return 3;
    if (!(fcntl(fd, F_GETFL) & O_APPEND)) return 4;
    if (write(fd, "safe\n", 5) != 5) return 5;
    return close(fd) == 0 ? 0 : 6;
}
''')
        try:
            subprocess.run([
                shutil.which('cc') or 'cc', '-Wall', '-Wextra', '-Werror',
                '-I', str(ROOT / 'extensions/lake_replication'),
                str(source), '-o', str(cls.binary),
            ], check=True, capture_output=True, text=True)
        except BaseException:
            cls.build.cleanup()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='lake-log-files-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.directory = self.root / 'logs'
        self.directory.mkdir(mode=0o700)
        self.log = self.directory / 'worker.log'

    def open_log(self, directory=None):
        return subprocess.run([
            str(self.binary), str(directory or self.directory), 'worker.log',
        ], timeout=5).returncode

    def test_creates_private_log_and_appends_on_reopen(self):
        self.assertEqual(self.open_log(), 0)
        self.assertEqual(self.log.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.open_log(), 0)
        self.assertEqual(self.log.read_text(), 'safe\nsafe\n')

    def test_rejects_nonprivate_directory(self):
        for mode in (0o750, 0o770, 0o755, 0o777):
            self.directory.chmod(mode)
            self.assertEqual(self.open_log(), 1)
            self.assertFalse(self.log.exists())

    def test_rejects_symlink_directory(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.directory, target_is_directory=True)
        self.assertEqual(self.open_log(alias), 1)
        self.assertFalse(self.log.exists())

    def test_rejects_symlink_file_without_changing_target(self):
        target = self.root / 'target'
        target.write_text('untouched')
        self.log.symlink_to(target)
        self.assertEqual(self.open_log(), 1)
        self.assertEqual(target.read_text(), 'untouched')

    def test_rejects_hardlinked_file_without_changing_target(self):
        target = self.root / 'target'
        target.write_text('untouched')
        target.chmod(0o600)
        os.link(target, self.log)
        self.assertEqual(self.open_log(), 1)
        self.assertEqual(target.read_text(), 'untouched')

    def test_rejects_readable_existing_log_without_chmod_or_write(self):
        self.log.write_text('untouched')
        self.log.chmod(0o644)
        self.assertEqual(self.open_log(), 1)
        self.assertEqual(self.log.read_text(), 'untouched')
        self.assertEqual(self.log.stat().st_mode & 0o777, 0o644)

    def test_rejects_fifo_without_blocking(self):
        os.mkfifo(self.log, 0o600)
        self.assertEqual(self.open_log(), 1)

    def test_rejects_directory_as_log(self):
        self.log.mkdir(mode=0o700)
        self.assertEqual(self.open_log(), 1)

    @unittest.skipUnless(os.geteuid() == 0, 'requires chown; also run in the Linux build container')
    def test_rejects_directory_owned_by_another_user(self):
        os.chown(self.directory, 65534, -1)
        self.assertEqual(self.open_log(), 1)
        self.assertFalse(self.log.exists())

    @unittest.skipUnless(os.geteuid() == 0, 'requires chown; also run in the Linux build container')
    def test_rejects_file_owned_by_another_user_without_writing(self):
        self.log.write_text('untouched')
        self.log.chmod(0o600)
        os.chown(self.log, 65534, -1)
        self.assertEqual(self.open_log(), 1)
        self.assertEqual(self.log.read_text(), 'untouched')


if __name__ == '__main__':
    unittest.main()
