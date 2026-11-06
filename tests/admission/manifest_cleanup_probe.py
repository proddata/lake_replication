#!/usr/bin/env python3
"""Guarded upstream manifest-cleanup candidate; diagnostic binary overlay only."""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
import tempfile


def load(name,file):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(file))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def correctness(p,directory):
    q=p.q;client=p.c.Psql('destination','analytics');snapshots=[]
    try:
        client.query('SET pg_lake_iceberg.manifest_min_count_to_merge=2; SET pg_lake_table.vacuum_compact_min_input_files=10000')
        client.query('''CREATE FUNCTION public.cleanup_manifests(text) RETURNS TABLE(
            manifest_path text,manifest_length bigint,partition_spec_id int,manifest_content text,
            sequence_number bigint,min_sequence_number bigint,added_snapshot_id bigint,
            added_files_count int,existing_files_count int,deleted_files_count int,
            added_rows_count bigint,existing_rows_count bigint,deleted_rows_count bigint)
            LANGUAGE C IMMUTABLE STRICT AS 'pg_lake_iceberg','current_manifests' ''')
        client.query('CREATE TABLE public.cleanup_items(id int,v text) USING iceberg')
        steps=[('empty',None,[]),('insert1',"INSERT INTO cleanup_items VALUES(1,'a')",[[1,'a']]),
            ('insert2',"INSERT INTO cleanup_items VALUES(2,'b')",[[1,'a'],[2,'b']]),
            ('insert3',"INSERT INTO cleanup_items VALUES(3,'c')",[[1,'a'],[2,'b'],[3,'c']]),
            ('savepoint',"BEGIN; SAVEPOINT s; INSERT INTO cleanup_items VALUES(99,'x'); ROLLBACK TO s; INSERT INTO cleanup_items VALUES(4,'d'); COMMIT",[[1,'a'],[2,'b'],[3,'c'],[4,'d']]),
            ('delete',"DELETE FROM cleanup_items WHERE id=2",[[1,'a'],[3,'c'],[4,'d']]),
            ('update',"UPDATE cleanup_items SET v='changed' WHERE id=3",[[1,'a'],[3,'changed'],[4,'d']]),
            ('vacuum','VACUUM cleanup_items',[[1,'a'],[3,'changed'],[4,'d']]),
            ('delete_all','DELETE FROM cleanup_items',[]),('empty_vacuum','VACUUM cleanup_items',[]),
            ('truncate','TRUNCATE cleanup_items',[]),('insert_after_truncate',"INSERT INTO cleanup_items VALUES(6,'f')",[[6,'f']]),
            ('rollback',"BEGIN; TRUNCATE cleanup_items; INSERT INTO cleanup_items VALUES(7,'g'); ROLLBACK",[[6,'f']])]
        for name,statement,expected in steps:
            if statement:client.query(statement)
            state=json.loads(client.query('''WITH location AS (
                SELECT metadata_location AS uri FROM lake_iceberg.tables_internal WHERE table_name='cleanup_items'::regclass
                ), metadata AS MATERIALIZED (SELECT uri,lake_iceberg.metadata(uri) AS doc FROM location),
                current AS (SELECT uri,EXISTS(SELECT 1 FROM jsonb_array_elements(coalesce(doc->'snapshots','[]')) item WHERE item->'snapshot-id'=doc->'current-snapshot-id') AS has_snapshot FROM metadata)
                SELECT jsonb_build_object(
                'rows',(SELECT coalesce(jsonb_agg(jsonb_build_array(id,v) ORDER BY id),'[]') FROM cleanup_items),
                'has_snapshot',has_snapshot,
                'files',CASE WHEN has_snapshot THEN (SELECT coalesce(jsonb_agg(jsonb_build_array(content,record_count) ORDER BY content,record_count),'[]') FROM lake_iceberg.files(uri)) ELSE '[]'::jsonb END,
                'manifests',CASE WHEN has_snapshot THEN (SELECT coalesce(jsonb_agg(jsonb_build_array(manifest_content,added_files_count,existing_files_count,deleted_files_count,added_rows_count,existing_rows_count,deleted_rows_count) ORDER BY manifest_content,added_files_count,existing_files_count,deleted_files_count,added_rows_count,existing_rows_count,deleted_rows_count),'[]') FROM public.cleanup_manifests(uri)) ELSE '[]'::jsonb END) FROM current''')[0])
            q.check(name+' exact rows',state['rows'],expected);snapshots.append(dict(step=name,**state))
        client.query('DROP TABLE cleanup_items; DROP FUNCTION public.cleanup_manifests(text)')
    finally:client.close()
    (directory/'snapshots.json').write_text(json.dumps(snapshots,indent=2)+'\n')
    return snapshots


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variant',choices=['control','candidate'],required=True)
    parser.add_argument('--case',choices=['fairness','correctness'],default='fairness')
    parser.add_argument('--cpuset',default='0')
    args=parser.parse_args();p=load('production_probe','probe.py');q=p.q
    # This historical comparison intentionally brackets the original prerequisite.
    q.USE_RUNTIME_PATCHSET=False
    overlay=load('commit_profile_overlay','commit_profile_overlay.py').Overlay(q,p.ROOT,candidate=True)
    original_build=q.build_images
    def build_images(archive,context):original_build(archive,context);overlay.build(context)
    q.build_images=build_images
    original_text=p.m.compose_text;setting='on' if args.variant=='candidate' else 'off'
    def compose_text():
        text=original_text();anchor='      - lake_replication.log_directory=/worker-logs\n'
        if text.count(anchor)!=1:raise ValueError('destination hook changed')
        settings=[f'lake_commit_diagnostic.skip_clean_manifests={setting}',
            'lake_commit_diagnostic.enabled='+('on' if args.case=='correctness' else 'off')]
        return text.replace(anchor,anchor+''.join('      - -c\n      - '+item+'\n' for item in settings))
    p.m.compose_text=compose_text;p.__doc__=__doc__
    p.m.diagnostic_sources=[Path(__file__),Path(__file__).with_name('commit_profile_overlay.py')]
    if args.case=='fairness':
        original_inventory=p.m.s.inventory
        def inventory():
            result=original_inventory();names=['analytics','fair_stream_1','fair_stream_2','fair_copy','fair_copy_1','fair_copy_2','fair_copy_3']
            result['manifest_cleanup_setting']={db:q.sql('destination',db,'SHOW lake_commit_diagnostic.skip_clean_manifests') for db in names}
            q.check('all measured databases use declared cleanup setting',sorted(set(result['manifest_cleanup_setting'].values())),[setting])
            result['commit_timing_setting']={db:q.sql('destination',db,'SHOW lake_commit_diagnostic.enabled') for db in names}
            q.check('internal timing disabled during performance measurement',sorted(set(result['commit_timing_setting'].values())),['off'])
            q.check('worker query tracing disabled during performance measurement','"event":"query_timing"' not in q.worker_logs(),True)
            return result
        p.m.s.inventory=inventory
        sys.argv=[sys.argv[0],'--cases','fairness','--cpuset',args.cpuset];code=p.main()
        path=q.RESULTS/'results.json';result=json.loads(path.read_text())
    else:
        q.RESULTS=p.ROOT/'.qualification'/(q.RUN+'_manifest_cleanup');q.RESULTS.mkdir()
        q.PROJECT+='-manifest-cleanup'
        result=dict(status='running',scope=__doc__,variant=args.variant);path=q.RESULTS/'results.json';code=1
        try:
            archive=q.archive_path();q.verify_archive(archive)
            result['archive']={key:q.EVIDENCE[key] for key in ['archive','archive_sha256','architecture','source_revision']}
            with tempfile.TemporaryDirectory() as temp:q.build_images(archive,Path(temp))
            text=compose_text().replace('      - -c\n      - lake_replication.prototype_instance_id=measurement-owner\n','')
            q.COMPOSE_FILE=q.RESULTS/'compose.yml';q.COMPOSE_FILE.write_text(text)
            q.compose('up','-d','--wait','--wait-timeout','300',timeout=600)
            q.check('declared cleanup setting',q.sql('destination','analytics','SHOW lake_commit_diagnostic.skip_clean_manifests'),setting)
            correctness(p,q.RESULTS)
            result['status']='passed';code=0
        except Exception as error:result.update(status='error',error=str(error));print('ERROR',error,flush=True)
        finally:
            logs=q.compose('logs','--no-color',check_result=False);text=logs.stdout+logs.stderr;(q.RESULTS/'compose.log').write_text(text)
            result['skip_witness_count']=text.count('lake-commit|skip_clean_manifest|')
            if result['status']=='passed' and ((result['skip_witness_count']>0)!=(args.variant=='candidate')):
                result.update(status='error',error='cleanup guard execution witness differs');code=1
            cleanup=q.compose('down','-v','--remove-orphans',check_result=False,timeout=300)
            result['cleanup_returncode']=cleanup.returncode
            if cleanup.returncode:result['status']='error';code=1
            result['checks']=q.EVIDENCE['checks']
    result['manifest_cleanup']=dict(variant=args.variant,setting=setting,overlay=overlay.metadata,
        timing_instrumentation=args.case=='correctness',correctness_schema_instrumentation=args.case=='correctness')
    path.write_text(json.dumps(result,indent=2)+'\n');print('Evidence:',q.RESULTS,flush=True)
    return code


if __name__=='__main__':raise SystemExit(main())
