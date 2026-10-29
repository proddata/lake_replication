"""Reject stale or corrupted prerequisite caches before passing them to Docker."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

spec=importlib.util.spec_from_file_location('prerequisite',Path(__file__).with_name('prerequisite.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
ROOT=Path(__file__).resolve().parents[2]
REVISION='8180647d4535fdb1ed223f92f301ba6094a8ec3e'


class PrerequisiteTests(unittest.TestCase):
    def test_source_architecture_and_recipe_change_the_cache_key(self):
        baseline=p.identity(ROOT,REVISION,'arm64')
        changed=[p.identity(ROOT,REVISION,'amd64'),p.identity(ROOT,'a'*40,'arm64')]
        recipe=copy.deepcopy(baseline)
        recipe['inputs']['deploy/poc/Dockerfile']='0'*64
        changed.append(recipe)
        self.assertTrue(all(p.cache_key(value)!=p.cache_key(baseline) for value in changed))

    def test_rejects_invalid_revision_or_architecture(self):
        for revision,architecture in [('main','arm64'),(REVISION,'riscv64')]:
            with self.subTest(revision=revision,architecture=architecture),self.assertRaises(ValueError):
                p.identity(ROOT,revision,architecture)

    def rejected_cache(self,*,wrong_identity=False,corrupt=False):
        expected=p.identity(ROOT,REVISION,'arm64')
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            archive=directory/'pg-lake-base.tar.gz';archive.write_bytes(b'original image')
            manifest=dict(identity=copy.deepcopy(expected),archive_sha256=p.digest(archive),image_id='sha256:expected')
            if wrong_identity:manifest['identity']['architecture']='amd64'
            (directory/'manifest.json').write_text(json.dumps(manifest))
            if corrupt:archive.write_bytes(b'modified image')
            calls=[]
            def docker(*args,**kwargs):
                calls.append(args)
                self.fail('unverified bytes reached Docker')
            with self.assertRaises(ValueError):p.load(docker,'image',expected,directory)
            self.assertEqual(calls,[])

    def test_wrong_architecture_is_rejected_before_docker_load(self):
        self.rejected_cache(wrong_identity=True)

    def test_modified_archive_is_rejected_before_docker_load(self):
        self.rejected_cache(corrupt=True)

    def test_image_labels_are_checked_against_the_requested_source(self):
        expected=p.identity(ROOT,REVISION,'arm64')
        class Result:
            stdout=json.dumps([dict(Os='linux',Architecture='arm64',Id='sha256:other',
                Config=dict(Labels={'org.opencontainers.image.revision':'b'*40,
                                    'org.opencontainers.image.source':'https://github.com/Snowflake-Labs/pg_lake'}))])
        with self.assertRaises(ValueError):p.image(lambda *args:Result(),'image',expected)

    def test_live_build_failure_keeps_a_useful_exit_error(self):
        spec=importlib.util.spec_from_file_location('qualification',Path(__file__).with_name('probe.py'))
        q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)
        result=SimpleNamespace(returncode=17,stdout=None,stderr=None)
        with patch.object(q.subprocess,'run',return_value=result):
            with self.assertRaisesRegex(RuntimeError,'docker exited with status 17'):
                q.command(['docker','build'],capture_output=False)


if __name__=='__main__':unittest.main()
