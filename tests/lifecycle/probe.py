#!/usr/bin/env python3
"""Qualify the packaged base-worker migration on private fresh/legacy volumes."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

q=module('qualification',ROOT/'tests/qualification/probe.py')
c=module('lifecycle_cases',Path(__file__).with_name('cases.py'))
c.q=q


def fresh():
    # Actual exported artifact, without source mounts or adapter modifications.
    q.verify_upgrade_contract()
    result=q.sql('destination','analytics',"SET ROLE lake_replication_admin; SELECT lake_replication.destination_system_identifier()>0 AND NOT lake_replication.worker_binding_matches(-1); RESET ROLE")
    q.check('admin can use narrowly scoped origin helpers', 't' in result.splitlines(),True)
    denied=q.sql_result('destination','analytics',"SET ROLE pg_read_all_data; SELECT lake_replication.destination_system_identifier()",check_result=False)
    q.check('read-all role cannot invoke origin-management helper',denied.returncode!=0 and 'permission denied' in denied.stderr,True)
    c.lifecycle_without_subscriptions()
    q.exercise_replication()
    q.exercise_interrupted_initial_copy()
    c.worker_churn()
    c.live_lifecycle()
    for database in ['analytics','lifecycle_second']:
        oid=q.sql('destination',database,'SELECT oid FROM pg_database WHERE datname=current_database()')
        assert oid.isdigit()
        logs=q.compose('exec','-T','destination','find','/worker-logs','-name',f'lake_replication_db{oid}_*_supervisor.log').stdout.strip()
        q.check(database+' has database-scoped worker log files',bool(logs),True)
    c.template_and_failed_drop()
    c.cloned_configuration()
    # Template subscriptions never dispatch initial sync; explicitly provision
    # the original slot whose deletion the copied binding must not authorize.
    q.sql('source','application',"SELECT slot_name FROM pg_create_logical_replication_slot('lifecycle_seed','pgoutput')")
    q.sql('destination','lifecycle_config_clone',"UPDATE lake_replication.worker_launch_config SET destination_database=current_database(),desired_state='running'")
    q.check('matching name cannot bypass copied database OID fence',q.wait_for('destination','lifecycle_config_clone',"SELECT desired_state FROM lake_replication.worker_launch_config",'stopped'),'stopped')
    rejected=q.sql_result('destination','lifecycle_config_clone','DROP SUBSCRIPTION lifecycle_seed',check_result=False)
    q.check('copied binding cannot authorize source slot cleanup',rejected.returncode!=0 and 'cannot authorize source slot cleanup' in rejected.stderr,True)
    q.check('copy cleanup rejection preserves original source slot',q.sql('source','application',"SELECT count(*) FROM pg_replication_slots WHERE slot_name='lifecycle_seed'"),'1')
    c.active_extension_drop()
    q.verify_uninstaller()


def upgrade(new_image,old_archive):
    # Start the unchanged, qualified beta.1 archive. Replacement requires a
    # drain and full server stop; retained explicit preload settings are tested.
    q.verify_archive(old_archive)
    q.EVIDENCE['old_archive']={key:q.EVIDENCE[key] for key in ('archive','archive_sha256','architecture','source_revision')}
    with tempfile.TemporaryDirectory(prefix='lake-legacy-') as temp:
        q.build_images(old_archive,Path(temp))
    q.ENV['QUALIFICATION_DESTINATION_IMAGE']=q.DESTINATION_IMAGE
    q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
    q.sql('destination','analytics',"ALTER DATABASE analytics SET session_preload_libraries='lake_replication'")
    u=module('legacy_upgrade',Path(__file__).with_name('upgrade.py'))
    u.q=q
    u.TARGET_VERSION='0.1.0-beta.4'
    # Reuse the packaged handoff assertions with the explicit adoption gate.
    original=q.sql
    def sql(service,db,statement,**kwargs):
        if statement == "ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.4'":
            denied=q.sql_result(service,db,statement,check_result=False)
            q.check('legacy binding upgrade requires explicit verified adoption',denied.returncode!=0 and 'explicit verified adoption' in denied.stderr,True)
            statement="BEGIN; SET LOCAL lake_replication.allow_legacy_binding_upgrade=on; "+statement+"; COMMIT"
        elif "BEGIN; ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.4'; ROLLBACK" == statement:
            original(service,db,"UPDATE lake_replication.worker_launch_config SET desired_state='running'")
            denied=q.sql_result(service,db,"BEGIN; SET LOCAL lake_replication.allow_legacy_binding_upgrade=on; ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.4'",check_result=False)
            q.check('SQL migration refuses enabled legacy subscriptions',denied.returncode!=0 and 'stop subscriptions and restart' in denied.stderr,True)
            original(service,db,"UPDATE lake_replication.worker_launch_config SET desired_state='stopped'")
            statement="BEGIN; SET LOCAL lake_replication.allow_legacy_binding_upgrade=on; ALTER EXTENSION lake_replication UPDATE TO '0.1.0-beta.4'; ROLLBACK"
        result=original(service,db,statement,**kwargs)
        if statement.startswith("BEGIN; SET LOCAL lake_replication.allow_legacy_binding_upgrade=on;") and statement.endswith('; COMMIT'):
            q.check('adopted original destination has valid origin',original(service,db,'SELECT lake_replication.worker_binding_matches(subscription_id)::text FROM lake_replication.worker_launch_config'),'true')
        return result
    q.sql=sql
    try:
        u.upgrade_case(new_image)
    finally:
        q.sql=original


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upgrade',action='store_true')
    parser.add_argument('--old-archive',type=Path,default=ROOT/'dist/release/lake_replication-0.1.0-beta.1-linux-arm64.tar.gz')
    args=parser.parse_args()
    q.RESULTS=q.RESULTS.with_name(q.RUN+('_base_upgrade' if args.upgrade else '_base_lifecycle'))
    q.PROJECT+='-base-upgrade' if args.upgrade else '-base-lifecycle'
    suffix='-base-upgrade' if args.upgrade else '-base-lifecycle'
    q.SOURCE_IMAGE+=suffix
    q.DESTINATION_IMAGE+=suffix
    q.ENV['QUALIFICATION_SOURCE_IMAGE']=q.SOURCE_IMAGE
    q.ENV['QUALIFICATION_DESTINATION_IMAGE']=q.DESTINATION_IMAGE
    q.RESULTS.mkdir(parents=True)
    q.EVIDENCE.update(scope='Production pg_extension_base migration; actual exported archives',findings=[])
    q.EVIDENCE['inputs']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),Path(__file__).with_name('cases.py'),Path(__file__).with_name('upgrade.py'),ROOT/'tests/qualification/probe.py',*sorted((ROOT/'worker/src').glob('*.rs')),*sorted((ROOT/'migrations').glob('*.sql')),ROOT/'extensions/lake_replication/lake_replication.c',ROOT/'extensions/lake_replication/lake_replication.control']}
    try:
        archive=q.archive_path()
        q.verify_archive(archive)
        with tempfile.TemporaryDirectory(prefix='lake-base-artifact-') as temp:
            q.build_images(archive,Path(temp))
        new_image=q.DESTINATION_IMAGE
        q.EVIDENCE['new_archive']={key:q.EVIDENCE[key] for key in ('archive','archive_sha256','architecture','source_revision')}
        q.EVIDENCE['new_image']=q.command(['docker','image','inspect','--format','{{.Id}}',new_image]).stdout.strip()
        text=q.COMPOSE_FILE.read_text()
        if args.upgrade:
            text=text.replace('shared_preload_libraries=pg_extension_base\n','shared_preload_libraries=pg_extension_base,lake_replication\n      - -c\n      - lake_replication.database_name=analytics\n')
        if not args.upgrade:
            text=text.replace('shared_preload_libraries=pg_extension_base,lake_replication','shared_preload_libraries=pg_extension_base').replace('      - -c\n      - lake_replication.database_name=analytics\n','')
        q.COMPOSE_FILE=q.RESULTS/'compose.yml'
        q.COMPOSE_FILE.write_text(text)
        if args.upgrade:
            # Distinct tags are needed to keep the new image after old build.
            tag='lake-replication-base-candidate:'+q.RUN
            q.command(['docker','tag',new_image,tag])
            upgrade(tag,args.old_archive)
        else:
            q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
            fresh()
        q.EVIDENCE.update(status='passed',passed=True)
    except Exception as error:
        q.EVIDENCE.update(status='error',passed=False,error=str(error))
        print('ERROR:',error,flush=True)
    finally:
        logs=q.compose('logs','--no-color',check_result=False)
        (q.RESULTS/'compose.log').write_text(logs.stdout+logs.stderr)
        worker=q.compose('exec','-T','destination','sh','-ec',"find /worker-logs -type f -name '*.log' -exec cat {} +",check_result=False)
        (q.RESULTS/'workers.jsonl').write_text(worker.stdout)
        cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
        q.EVIDENCE['cleanup_returncode']=cleanup.returncode
        (q.RESULTS/'results.json').write_text(json.dumps(q.EVIDENCE,indent=2)+'\n')
        print('Evidence:',q.RESULTS,flush=True)
    return 0 if q.EVIDENCE.get('passed') and cleanup.returncode==0 else 1

if __name__=='__main__':
    sys.exit(main())
