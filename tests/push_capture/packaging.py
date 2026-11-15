#!/usr/bin/env python3
"""Verify optional archives and installation in disposable containers only."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT=Path(__file__).resolve().parents[2]


def main():
    evidence=dict(scope='native prototype archive checksums, staged installation and running-server refusal',checks=[],artifacts=[])
    output=ROOT/'.qualification/push-logical-refactor/packaging-results.json'
    def check(name,actual,expected=True):
        passed=actual==expected;evidence['checks'].append(dict(name=name,actual=actual,expected=expected,passed=passed))
        print(('PASS ' if passed else 'FAIL ')+name,flush=True)
        if not passed:raise RuntimeError(name)
    script=r'''
import hashlib,json,os,shutil,stat,subprocess
from pathlib import Path
package=Path('/package');manifest=json.loads((package/'manifest.json').read_text())
component=manifest['component'];env=dict(os.environ,DESTDIR='/tmp/staged')
result=subprocess.run(['sh',str(package/'install.sh')],env=env,text=True,capture_output=True)
if result.returncode:raise RuntimeError(result.stderr)
shared=subprocess.check_output(['pg_config','--sharedir'],text=True).strip()+'/extension'
library=subprocess.check_output(['pg_config','--pkglibdir'],text=True).strip()
for name,digest in manifest['files'].items():
 if not name.startswith('payload/'):continue
 source=Path(name)
 target=Path('/tmp/staged'+('/usr/local/bin' if source.parts[1]=='bin' else library if source.parts[1]=='lib' else shared))/source.name
 if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise RuntimeError('staged content differs')
 if source.parts[1] in ('bin','lib') and stat.S_IMODE(target.stat().st_mode)!=0o755:raise RuntimeError('executable mode differs')
shutil.copytree(package,'/tmp/corrupt');bad=Path('/tmp/corrupt/payload/lib')/(component+'.so');bad.write_bytes(bad.read_bytes()+b'corrupt')
failed=subprocess.run(['sh','/tmp/corrupt/install.sh'],env=dict(os.environ,DESTDIR='/tmp/rejected'),text=True,capture_output=True)
if failed.returncode==0 or Path('/tmp/rejected').exists():raise RuntimeError('corrupt bundle was installed')
data=Path('/tmp/fake-data');data.mkdir();(data/'PG_VERSION').write_text('18\n');(data/'postmaster.pid').write_text(str(os.getpid())+'\n')
env=dict(os.environ,PGDATA=str(data));env.pop('DESTDIR',None)
failed=subprocess.run(['sh',str(package/'install.sh')],env=env,text=True,capture_output=True)
if failed.returncode==0 or 'Stop the target server' not in failed.stderr:raise RuntimeError('running server was not refused')
(data/'postmaster.pid').unlink()
marker=Path(subprocess.check_output(['pg_config','--sharedir'],text=True).strip())/'pg_lake/lake-replication-patchset.json'
original=marker.read_bytes()
try:
 marker.write_text('{"unqualified_runtime":true}\n')
 failed=subprocess.run(['sh',str(package/'install.sh')],env=env,text=True,capture_output=True)
 if failed.returncode==0 or 'runtime marker differs' not in failed.stderr:raise RuntimeError('different runtime was not refused')
finally:marker.write_bytes(original)
if component=='lake_replication_push':
 worker=Path('/usr/local/bin/lake-replication-publisher');original=worker.read_bytes()
 try:
  worker.write_bytes(original+b'unqualified')
  failed=subprocess.run(['sh',str(package/'install.sh')],env=env,text=True,capture_output=True)
  if failed.returncode==0 or 'default destination workers or extension differ' not in failed.stderr:raise RuntimeError('different default destination bundle was not refused')
 finally:worker.write_bytes(original)
config=Path('/tmp/wrong-pg-config');config.write_text('#!/bin/sh\nif [ "$1" = --version ]; then echo "PostgreSQL 18.unqualified"; else exec pg_config "$@"; fi\n');config.chmod(0o755)
failed=subprocess.run(['sh',str(package/'install.sh')],env=dict(os.environ,PG_CONFIG=str(config),DESTDIR='/tmp/wrong-version'),text=True,capture_output=True)
if failed.returncode==0 or Path('/tmp/wrong-version').exists() or 'PostgreSQL build differs' not in failed.stderr:raise RuntimeError('different PostgreSQL build was not refused')
payloads=[]
for name in manifest['files']:
 if not name.startswith('payload/'):continue
 source=Path(name)
 payloads.append(Path(('/usr/local/bin' if source.parts[1]=='bin' else library if source.parts[1]=='lib' else shared))/source.name)
for fault,tool,threshold in [('preparation','install',2),('publication','mv',3),('termination','mv',3)]:
 root=Path('/tmp/fault-'+fault);tools=Path('/tmp/tools-'+fault);tools.mkdir()
 old=root/Path(library).relative_to('/')/(component+'.so');old.parent.mkdir(parents=True);old.write_bytes(b'previous qualified library');old.chmod(0o640)
 real=shutil.which(tool);counter=tools/'count'
 wrapper=tools/tool
 # Fail once, so subsequent rollback operations use the real tool normally.
 condition='"$1" = -m' if tool=='install' else '1 = 1'
 action='kill -TERM "$PPID"; exit 97' if fault=='termination' else 'exit 97'
 wrapper.write_text('#!/bin/sh\nif [ '+condition+' ]; then\n count=0; test ! -f '+str(counter)+' || count=$(cat '+str(counter)+')\n count=$((count+1)); echo "$count" > '+str(counter)+'\n if [ "$count" -eq '+str(threshold)+' ]; then '+action+'; fi\nfi\nexec '+real+' "$@"\n');wrapper.chmod(0o755)
 result=subprocess.run(['sh',str(package/'install.sh')],env=dict(os.environ,DESTDIR=str(root),PATH=str(tools)+':'+os.environ['PATH']),text=True,capture_output=True)
 if result.returncode==0:raise RuntimeError(fault+' injection did not fail')
 if old.read_bytes()!=b'previous qualified library' or stat.S_IMODE(old.stat().st_mode)!=0o640:raise RuntimeError(fault+' lost existing files or modes')
 if any((root/path.relative_to('/')).exists() for path in payloads if path!=Path(library)/(component+'.so')):raise RuntimeError(fault+' exposed a partial installation')
 if list(root.rglob('.lake-push-*')):raise RuntimeError(fault+' left installation locks or staging files')
locked=Path('/tmp/interrupted');lock=locked/Path(library).relative_to('/')/'.lake-push-install.lock';lock.mkdir(parents=True)
result=subprocess.run(['sh',str(package/'install.sh')],env=dict(os.environ,DESTDIR=str(locked)),text=True,capture_output=True)
if result.returncode==0 or 'installation lock' not in result.stderr or not lock.is_dir():raise RuntimeError('interrupted installation lock was not preserved')
root=Path('/tmp/symlink-target');target=root/'usr/local/bin'/next((package/'payload/bin').iterdir()).name;target.parent.mkdir(parents=True)
outside=Path('/tmp/untouched');outside.write_bytes(b'untouched');target.symlink_to(outside)
result=subprocess.run(['sh',str(package/'install.sh')],env=dict(os.environ,DESTDIR=str(root)),text=True,capture_output=True)
if result.returncode==0 or not target.is_symlink() or outside.read_bytes()!=b'untouched':raise RuntimeError('symlink target was overwritten')
# Kill the installer while a real publication rename is about to run. Existing
# binaries and libraries exercise retry-safe restoration across partial repair.
root=Path('/tmp/killed-installer');tools=Path('/tmp/kill-tools');tools.mkdir()
old=root/Path(library).relative_to('/')/(component+'.so');old.parent.mkdir(parents=True);old.write_bytes(b'old library');old.chmod(0o640)
binary=next((package/'payload/bin').iterdir()).name
old_binary=root/'usr/local/bin'/binary;old_binary.parent.mkdir(parents=True);old_binary.write_bytes(b'old worker');old_binary.chmod(0o750)
counter=tools/'count';real=shutil.which('mv');wrapper=tools/'mv'
wrapper.write_text('#!/bin/sh\ncount=0; test ! -f '+str(counter)+' || count=$(cat '+str(counter)+')\ncount=$((count+1)); echo "$count" > '+str(counter)+'\nif [ "$count" -eq '+str(len(list((package/'payload/bin').iterdir()))+2)+' ]; then kill -KILL "$PPID"; exit 97; fi\nexec '+real+' "$@"\n');wrapper.chmod(0o755)
env=dict(os.environ,DESTDIR=str(root));command=['sh',str(package/'install.sh')]
result=subprocess.run(command,env=dict(env,PATH=str(tools)+':'+os.environ['PATH']),text=True,capture_output=True)
lock=root/Path(library).relative_to('/')/'.lake-push-install.lock'
if result.returncode!=-9 or not (lock/'prepared').is_file():raise RuntimeError('SIGKILL did not preserve the prepared journal')
refused=subprocess.run(command,env=env,text=True,capture_output=True)
if refused.returncode==0 or not lock.is_dir():raise RuntimeError('SIGKILL journal was silently adopted')
repair=command+['--rollback']
holder=subprocess.Popen(['sh','-c','exec 8<&0; exec 9<>"$1"; flock 9; (echo ready; read done) <&8 & kill -KILL "$$"; wait','holder',str(lock/'actor')],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
try:
 if holder.stdout.readline().strip()!='ready':raise RuntimeError('repair lock fixture failed')
 refused=subprocess.run(repair,env=env,text=True,capture_output=True)
 if refused.returncode==0 or 'active installer or child' not in refused.stderr:raise RuntimeError('active recovery lock was ignored')
finally:holder.communicate(input='done\n',timeout=10)
entry=lock/'1'/'target';saved=entry.read_bytes();entry.write_text('/tmp/untouched\n')
refused=subprocess.run(repair,env=env,text=True,capture_output=True)
if refused.returncode==0 or outside.read_bytes()!=b'untouched' or not lock.is_dir():raise RuntimeError('unsafe recovery target was accepted')
entry.write_bytes(saved)
entry=lock/'1'/'stage';saved=entry.read_bytes()
relative=Path('unsafe-relative-recovery-stage');relative.mkdir();(relative/'keep').write_text('untouched')
try:
 entry.write_text(str(relative)+'\n')
 refused=subprocess.run(repair,env=env,text=True,capture_output=True)
 if refused.returncode==0 or (relative/'keep').read_text()!='untouched' or not lock.is_dir():raise RuntimeError('relative recovery stage was accepted')
finally:
 entry.write_bytes(saved);shutil.rmtree(relative)
backup=next(path for path in root.rglob('old') if path.parent.name.startswith('.lake-push-stage.'))
saved=backup.read_bytes();backup.write_bytes(saved+b'corrupt')
refused=subprocess.run(repair,env=env,text=True,capture_output=True)
if refused.returncode==0 or 'backup failed verification' not in refused.stderr or not lock.is_dir():raise RuntimeError('corrupt recovery backup was accepted')
backup.write_bytes(saved)
# Fail the second restoration after the first has succeeded. Backups must
# survive so a clean second repair restores both original files.
counter.unlink()
wrapper.write_text('#!/bin/sh\ncount=0; test ! -f '+str(counter)+' || count=$(cat '+str(counter)+')\ncount=$((count+1)); echo "$count" > '+str(counter)+'\nif [ "$count" -eq 2 ]; then exit 97; fi\nexec '+real+' "$@"\n');wrapper.chmod(0o755)
failed=subprocess.run(repair,env=dict(env,PATH=str(tools)+':'+os.environ['PATH']),text=True,capture_output=True)
if failed.returncode==0 or not lock.is_dir() or 'rollback needs repair' not in failed.stderr:raise RuntimeError('failed recovery did not retain its journal')
result=subprocess.run(repair,env=env,text=True,capture_output=True)
if result.returncode:raise RuntimeError('retry of interrupted rollback failed: '+result.stderr)
if old.read_bytes()!=b'old library' or stat.S_IMODE(old.stat().st_mode)!=0o640:raise RuntimeError('SIGKILL repair lost the old library')
if old_binary.read_bytes()!=b'old worker' or stat.S_IMODE(old_binary.stat().st_mode)!=0o750:raise RuntimeError('SIGKILL repair lost the old worker')
if any((root/path.relative_to('/')).exists() for path in payloads if path not in [Path(library)/(component+'.so'),Path('/usr/local/bin')/binary]):raise RuntimeError('SIGKILL repair left new payloads')
if list(root.rglob('.lake-push-*')):raise RuntimeError('successful SIGKILL repair left recovery files')
# Kill both planning and preparation. Neither phase may write a live target.
for phase in ['planning','preparation']:
 root=Path('/tmp/killed-'+phase);tools=Path('/tmp/'+phase+'-tools');tools.mkdir()
 old=root/Path(library).relative_to('/')/(component+'.so');old.parent.mkdir(parents=True);old.write_bytes(b'original unpublished library');old.chmod(0o640)
 tool='mkdir' if phase=='planning' else 'install';real=shutil.which(tool);wrapper=tools/tool
 condition='case "$1" in */.lake-push-install.lock/2)' if phase=='planning' else 'case "$3" in *.so)'
 wrapper.write_text('#!/bin/sh\n'+condition+' '+real+' "$@"; kill -KILL "$PPID"; exit 97;; esac\nexec '+real+' "$@"\n');wrapper.chmod(0o755)
 env=dict(os.environ,DESTDIR=str(root));command=['sh',str(package/'install.sh')]
 result=subprocess.run(command,env=dict(env,PATH=str(tools)+':'+os.environ['PATH']),text=True,capture_output=True)
 lock=root/Path(library).relative_to('/')/'.lake-push-install.lock'
 if result.returncode!=-9 or not lock.is_dir() or (lock/'prepared').exists():raise RuntimeError(phase+' kill missed the unpublished phase')
 for action in ['--rollback','--finish']:
  refused=subprocess.run(command+[action],env=env,text=True,capture_output=True)
  if refused.returncode==0 or not lock.is_dir():raise RuntimeError(phase+' permitted the wrong recovery action')
 result=subprocess.run(command+['--discard'],env=env,text=True,capture_output=True)
 if result.returncode:raise RuntimeError(phase+' discard failed: '+result.stderr)
 if old.read_bytes()!=b'original unpublished library' or stat.S_IMODE(old.stat().st_mode)!=0o640:raise RuntimeError(phase+' changed the original payload')
 if any((root/path.relative_to('/')).exists() for path in payloads if path!=Path(library)/(component+'.so')):raise RuntimeError(phase+' exposed unpublished payloads')
 if list(root.rglob('.lake-push-*')):raise RuntimeError(phase+' discard leaked staging or journal data')
# Interrupt committed staging cleanup after one directory has been removed.
root=Path('/tmp/killed-cleanup');tools=Path('/tmp/cleanup-tools');tools.mkdir()
real=shutil.which('rm');wrapper=tools/'rm'
wrapper.write_text('#!/bin/sh\ncase "$2" in */.lake-push-stage.*) '+real+' "$@"; kill -KILL "$PPID"; exit 97;; esac\nexec '+real+' "$@"\n');wrapper.chmod(0o755)
env=dict(os.environ,DESTDIR=str(root));command=['sh',str(package/'install.sh')]
result=subprocess.run(command,env=dict(env,PATH=str(tools)+':'+os.environ['PATH']),text=True,capture_output=True)
lock=root/Path(library).relative_to('/')/'.lake-push-install.lock'
if result.returncode!=-9 or not (lock/'committed').is_file():raise RuntimeError('cleanup interruption did not retain committed journal')
refused=subprocess.run(command+['--rollback'],env=env,text=True,capture_output=True)
if refused.returncode==0 or 'requires --finish' not in refused.stderr:raise RuntimeError('committed installation allowed rollback')
finish=command+['--finish']
target=root/Path(library).relative_to('/')/(component+'.so');original=target.read_bytes();target.write_bytes(original+b'corrupt')
refused=subprocess.run(finish,env=env,text=True,capture_output=True)
if refused.returncode==0 or 'payload failed verification' not in refused.stderr or not lock.is_dir():raise RuntimeError('committed cleanup accepted corrupt target')
target.write_bytes(original);target.chmod(0o640)
refused=subprocess.run(finish,env=env,text=True,capture_output=True)
if refused.returncode==0 or not lock.is_dir():raise RuntimeError('committed cleanup accepted incorrect mode')
target.chmod(0o755)
result=subprocess.run(finish,env=env,text=True,capture_output=True)
if result.returncode:raise RuntimeError('committed cleanup recovery failed: '+result.stderr)
if list(root.rglob('.lake-push-*')):raise RuntimeError('committed cleanup left staging files')
for name,digest in manifest['files'].items():
 if not name.startswith('payload/'):continue
 source=Path(name);target=root/Path('/usr/local/bin' if source.parts[1]=='bin' else library if source.parts[1]=='lib' else shared).relative_to('/')/source.name
 if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise RuntimeError('committed cleanup changed installed payload')
verified={'staged':True,'corruption_refused':True,'running_server_refused':True,'runtime_mismatch_refused':True,'postgres_build_mismatch_refused':True,'preparation_failure_unchanged':True,'publication_failure_rolled_back':True,'termination_rolled_back':True,'interrupted_lock_refused':True,'symlink_target_refused':True}
verified.update(interrupted_planning_discard=True,interrupted_preparation_discard=True,unpublished_recovery_action_fenced=True,relative_recovery_stage_refused=True,committed_cleanup_recovery=True,committed_rollback_refused=True,corrupt_committed_payload_refused=True,incorrect_committed_mode_refused=True,sigkill_recovery=True,surviving_installer_child_lock_refused=True,unsafe_recovery_target_refused=True,corrupt_recovery_backup_refused=True,partial_recovery_retry=True)
if component=='lake_replication_push':verified['default_bundle_mismatch_refused']=True
print(json.dumps(verified))
'''
    try:
        archives=sorted((ROOT/'dist/push').glob('*.tar.gz'))
        check('source and destination are separate optional native archives',len(archives),2)
        for archive in archives:
            with tempfile.TemporaryDirectory(prefix='verify-push-bundle-') as temporary:
                with tarfile.open(archive,'r:gz') as bundle:bundle.extractall(temporary,filter='data')
                package=next(Path(temporary).iterdir());manifest=json.loads((package/'manifest.json').read_text());component=manifest['component']
                expected=Path(str(archive)+'.sha256').read_text().split()[0]
                check(component+' archive checksum verifies',hashlib.sha256(archive.read_bytes()).hexdigest(),expected)
                for line in (package/'SHA256SUMS').read_text().splitlines():
                    digest,name=line.split('  ',1)
                    if hashlib.sha256((package/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('package checksum mismatch: '+name)
                check(component+' includes verified native dependency notices',(package/'THIRD-PARTY-LICENSES/inventory.json').is_file())
                binaries=sorted(p.name for p in (package/'payload/bin').iterdir())
                expected_bins=['lake-replication-capture'] if component=='lake_replication_capture' else ['lake-replication-log-importer','lake-replication-push-publisher','lake-replication-supervisor']
                check(component+' includes only its optional workers',binaries,expected_bins)
                result=subprocess.run(['docker','run','--pull=never','--rm','--network=none','--entrypoint','python3','-v',str(package)+':/package:ro',manifest['image_id'],'-c',script],text=True,capture_output=True)
                if result.returncode:raise RuntimeError(result.stderr)
                verified=json.loads(result.stdout.strip().splitlines()[-1])
                for name,passed in verified.items():check(component+' '+name,passed)
                evidence['artifacts'].append(dict(path=str(archive.relative_to(ROOT)),sha256=expected,manifest=manifest))
        evidence['status']='passed';returncode=0
    except Exception as error:
        evidence.update(status='error',error=str(error));print('ERROR: '+str(error),flush=True);returncode=1
    output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(evidence,indent=2)+'\n')
    return returncode

if __name__=='__main__':sys.exit(main())
