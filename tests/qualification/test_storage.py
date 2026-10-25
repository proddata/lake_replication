"""Fixture selection must fail before expensive builds or silently drifting images."""
import hashlib
import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

spec=importlib.util.spec_from_file_location('storage_fixtures',Path(__file__).with_name('storage.py'))
s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp=TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.results=self.root/'results'
        dockerfile=self.root/'tests/qualification/Dockerfile.storage';dockerfile.parent.mkdir(parents=True);dockerfile.write_text('fixture recipe\n')
        self.recipe=hashlib.sha256(dockerfile.read_bytes()).hexdigest();self.images={};self.calls=[]
        self.env={};self.evidence={}

    def command(self,args,**kwargs):
        self.calls.append(args)
        if args[:3]==['docker','image','inspect']:
            image=self.images.get(args[-1]);return SimpleNamespace(returncode=0 if image else 1,stdout=json.dumps(image) if image else '',stderr='')
        self.assertEqual(args[:2],['docker','build'])
        target=args[args.index('--target')+1];name=args[args.index('-t')+1]
        source=next(v['revision'] for v in s.FIXTURES.values() if v['target']==target)
        self.images[name]=dict(id='sha256:'+target,architecture='arm64',labels={'org.opencontainers.image.revision':source,'io.lake-replication.fixture.recipe':self.recipe})
        return SimpleNamespace(returncode=0,stdout='build succeeded',stderr='')

    def ensure(self):return s.ensure(self.command,self.env,self.evidence,self.root,self.results,'arm64')

    def test_builds_once_and_keeps_pinned_origin_on_reuse(self):
        first=self.ensure();second=self.ensure()
        self.assertEqual(first,second)
        builds=[c for c in self.calls if c[:2]==['docker','build']]
        self.assertEqual(len(builds),2)
        self.assertTrue(all(c[c.index('--platform')+1]=='linux/arm64' for c in builds))
        self.assertEqual({x['origin'] for x in second.values()},{'pinned_source_build'})
        self.assertEqual(len(list(self.results.glob('*build.log'))),2)

    def test_cached_identity_drift_is_rejected(self):
        self.ensure();self.images[next(iter(self.images))]['labels']['org.opencontainers.image.revision']='wrong'
        with self.assertRaisesRegex(RuntimeError,'identity mismatch'):self.ensure()

    def test_explicit_override_must_exist_and_match_architecture(self):
        self.env['QUALIFICATION_STORAGE_IMAGE']='my-local-fixture'
        with self.assertRaisesRegex(RuntimeError,'unavailable'):self.ensure()
        self.images['my-local-fixture']=dict(id='sha256:override',architecture='amd64',labels={})
        with self.assertRaisesRegex(RuntimeError,'architecture mismatch'):self.ensure()
        self.images['my-local-fixture']['architecture']='arm64'
        self.assertEqual(self.ensure()['storage']['origin'],'explicit_override')


if __name__=='__main__':unittest.main()
