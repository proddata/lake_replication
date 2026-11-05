"""Exercise the actual candidate C predicate at unknown/empty/deleted boundaries."""
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest
from commit_profile_overlay import CLEANUP_GUARD


class GuardTests(unittest.TestCase):
    def test_unknown_empty_and_deleted_manifests_keep_the_full_cleanup_path(self):
        compiler=shutil.which('cc')
        if compiler is None:self.skipTest('C compiler unavailable')
        predicate=CLEANUP_GUARD
        program='''#include <stdbool.h>
#include <stdint.h>
#include <limits.h>
#include <assert.h>
typedef struct { bool deleted_files_count_known; int32_t deleted_files_count,added_files_count,existing_files_count; } IcebergManifest;
'''+predicate+'''
int main(void) {
    IcebergManifest m={false,0,1,0};
    assert(!LakeCleanupCanSkip(&m));
    m.deleted_files_count_known=true; assert(LakeCleanupCanSkip(&m));
    m.added_files_count=0; assert(!LakeCleanupCanSkip(&m));
    m.existing_files_count=1; assert(LakeCleanupCanSkip(&m));
    m.deleted_files_count=1; assert(!LakeCleanupCanSkip(&m));
    m.deleted_files_count=-1; assert(!LakeCleanupCanSkip(&m));
    m.deleted_files_count=0; m.added_files_count=-1; assert(!LakeCleanupCanSkip(&m));
    m.added_files_count=INT_MAX; m.existing_files_count=INT_MAX; assert(LakeCleanupCanSkip(&m));
    m.deleted_files_count_known=false; assert(!LakeCleanupCanSkip(&m));
    return 0;
}
'''
        with tempfile.TemporaryDirectory() as directory:
            executable=str(Path(directory)/'guard')
            subprocess.run([compiler,'-std=c99','-Wall','-Werror','-x','c','-','-o',executable],input=program,text=True,check=True,capture_output=True)
            subprocess.run([executable],check=True)


if __name__=='__main__':unittest.main()
