"""Diagnostic-only pg_lake binary overlay, built from verified pinned sources."""
import hashlib
import json
from pathlib import Path
import re

HEADER=r'''#include "portability/instr_time.h"
#include "utils/guc.h"
typedef struct LakeCommitTimer { instr_time start; bool enabled; } LakeCommitTimer;
static LakeCommitTimer LakeCommitStart(void)
{
    LakeCommitTimer timer = {0};
    const char *setting = GetConfigOptionByName("lake_commit_diagnostic.enabled", NULL, true);
    timer.enabled = setting != NULL && strcmp(setting, "on") == 0;
    if (timer.enabled) INSTR_TIME_SET_CURRENT(timer.start);
    return timer;
}
static void LakeCommitStop(LakeCommitTimer *timer, const char *phase, uint64 count)
{
    if (timer->enabled) {
        instr_time elapsed;
        INSTR_TIME_SET_CURRENT(elapsed);
        INSTR_TIME_SUBTRACT(elapsed, timer->start);
        ereport(LOG, (errmsg("lake-commit|%s|%.3f|" UINT64_FORMAT,
                            phase, INSTR_TIME_GET_MILLISEC(elapsed), count)));
    }
}
'''


CLEANUP_GUARD='''static bool
LakeCleanupCanSkip(const IcebergManifest *manifest)
{
    return manifest->deleted_files_count_known && manifest->deleted_files_count == 0 &&
           manifest->added_files_count >= 0 && manifest->existing_files_count >= 0 &&
           (manifest->added_files_count > 0 || manifest->existing_files_count > 0);
}

'''

def replace_once(text,old,new):
    if text.count(old)!=1:raise ValueError('pinned source anchor changed: '+old[:80])
    return text.replace(old,new)


def time_call(text,pattern,label,count='1'):
    matches=list(re.finditer(pattern,text,re.MULTILINE))
    if len(matches)!=1:raise ValueError('pinned call anchor changed: '+label)
    statement=matches[0].group();indent=re.match(r'\s*',statement).group()
    return replace_once(text,statement,indent+f'LakeCommitTimer profile_{label} = LakeCommitStart();\n'+statement+
        '\n'+indent+f'LakeCommitStop(&profile_{label}, "{label}", {count});')


def patch(path,text):
    if path.endswith('manifest_spec.h'):
        return replace_once(text,'\tsize_t\t\tkey_metadata_length;\n}',
            '\tsize_t\t\tkey_metadata_length;\n\tbool\t\tdeleted_files_count_known;\n}')
    text=replace_once(text,'#include "postgres.h"','#include "postgres.h"\n#include "/opt/lake-commit-profile.h"')
    if path.endswith('manifest_merge.c'):
        anchor=re.search(r'^RemoveDeletedManifestEntriesInternal\([^)]*\)\n\{',text,re.MULTILINE)
        if anchor is None:raise ValueError('cleanup function anchor changed')
        # Insert a file-scope predicate ahead of the definition, preserving its
        # preceding static bool declaration and explanatory comment.
        definition=text.rfind('static bool\n',0,anchor.start())
        if definition<0:raise ValueError('cleanup definition declaration missing')
        text=text[:definition]+CLEANUP_GUARD+text[definition:]
        guard='''
    const char *skip = GetConfigOptionByName("lake_commit_diagnostic.skip_clean_manifests", NULL, true);
    if (skip != NULL && strcmp(skip, "on") == 0 &&
        LakeCleanupCanSkip(*manifest))
    {
        LakeCommitTimer skipped = LakeCommitStart();
        LakeCommitStop(&skipped, "skip_clean_manifest", 1);
        return false;
    }
'''
        text=replace_once(text,anchor.group(),anchor.group()+guard)
    elif path.endswith('read_manifest.c'):
        known='''static bool
LakeDeletedCountKnown(avro_value_t *record, const char *name)
{
    avro_value_t value;
    if (avro_value_get_by_name(record, name, &value, NULL) != 0)
        return false;
    if (avro_value_get_type(&value) == AVRO_UNION)
    {
        avro_value_t branch;
        if (avro_value_get_current_branch(&value, &branch) != 0)
            return false;
        return avro_value_get_type(&branch) == AVRO_INT32;
    }
    return avro_value_get_type(&value) == AVRO_INT32;
}

'''
        text=replace_once(text,'static void\nReadIcebergManifestFromAvro(',known+'static void\nReadIcebergManifestFromAvro(')
        for field in ['deleted_data_files_count','deleted_files_count']:
            text=replace_once(text,f'AvroGetInt32Field(record, "{field}", AVRO_FIELD_REQUIRED, &manifest->deleted_files_count);',
                f'AvroGetInt32Field(record, "{field}", AVRO_FIELD_REQUIRED, &manifest->deleted_files_count);\n\t\tmanifest->deleted_files_count_known = LakeDeletedCountKnown(record, "{field}");')
    elif path.endswith('track_iceberg_metadata_changes.c'):
        text=replace_once(text,'ConsumeTrackedIcebergMetadataChanges(bool isVerbose)\n{',
            'ConsumeTrackedIcebergMetadataChanges(bool isVerbose)\n{\n\tLakeCommitTimer total = LakeCommitStart();\n\tHTAB *tracked = GetTrackedIcebergMetadataOperations();\n\tuint64 relations = tracked == NULL ? 0 : hash_get_num_entries(tracked);')
        text=replace_once(text,'\tApplyTrackedIcebergMetadataChanges(isVerbose);\n\tResetTrackedIcebergMetadataOperation();',
            '\tApplyTrackedIcebergMetadataChanges(isVerbose);\n\tResetTrackedIcebergMetadataOperation();\n\tLakeCommitStop(&total, "metadata_total", relations);')
        text=replace_once(text,'\t\tList\t   *allTransforms = AllPartitionTransformList(relationId);',
            '\t\tLakeCommitTimer prepare = LakeCommitStart();\n\t\tList\t   *allTransforms = AllPartitionTransformList(relationId);')
        text=replace_once(text,'\t\tif (metadataOperations != NIL)\n\t\t{',
            '\t\tLakeCommitStop(&prepare, "metadata_prepare", list_length(metadataOperations));\n\t\tif (metadataOperations != NIL)\n\t\t{\n\t\t\tLakeCommitTimer metadata = LakeCommitStart();')
        text=replace_once(text,'\t\t\tListCell   *requestCell = NULL;',
            '\t\t\tLakeCommitStop(&metadata, "iceberg_metadata", 1);\n\t\t\tListCell   *requestCell = NULL;')
        text=replace_once(text,'\tFinishAllUploads();',
            '\tLakeCommitTimer uploads = LakeCommitStart();\n\tFinishAllUploads();\n\tLakeCommitStop(&uploads, "uploads", 1);')
        text=time_call(text,r'^\tHTAB\s+\*currentFilesMap = GetTableDataFilesByPathHashFromCatalog\([^;]+;',
            'catalog_files','hash_get_num_entries(currentFilesMap)')
        text=time_call(text,r'^\tFindChangedFilesSinceMetadata\(opTracker, currentFilesMap, &addedFiles, &removedFilePaths\);','file_diff')
        text=time_call(text,r'^\tHTAB\s+\*metadataDataFilesMap = CreateDataFilesHashForMetadata\(metadata\);',
            'metadata_file_scan','hash_get_num_entries(metadataDataFilesMap)')
        text=time_call(text,r'^\tLoadColumnStatsForFiles\(opTracker->relationId, currentFilesMap, addedFiles\);',
            'new_file_stats','list_length(addedFiles)')
    elif path.endswith('s3_writer_utils.c'):
        text=replace_once(text,'\tExecuteCommandsInParallelInPGDuck(commandList, MaxParallelFileUploads);',
            '\tLakeCommitTimer uploads = LakeCommitStart();\n\tExecuteCommandsInParallelInPGDuck(commandList, MaxParallelFileUploads);\n\tLakeCommitStop(&uploads, "upload_commands", list_length(commandList));')
    elif path.endswith('parallel_command.c'):
        text=replace_once(text,'\t\t\t\tPGDuckConnection *pgDuckConnection = GetPGDuckConnection();',
            '\t\t\t\tLakeCommitTimer connect = LakeCommitStart();\n\t\t\t\tPGDuckConnection *pgDuckConnection = GetPGDuckConnection();\n\t\t\t\tLakeCommitStop(&connect, "upload_connect", 1);')
    elif path.endswith('metadata_operations.c'):
        text=time_call(text,r'^\t\t\tmetadata = ReadIcebergTableMetadata\(metadataPath\);','read_metadata')
        text=time_call(text,r'^\tProcessIcebergMetadataOperations\(relationId, metadataOperations, builder\);','process_operations')
        text=time_call(text,r'^\tIcebergSnapshot \*newSnapshot = FinalizeNewSnapshot\([^;]+;','finalize_snapshot')
        text=time_call(text,r'^\tUploadTableMetadataToURI\(metadata, newMetadataPath\);','write_metadata')
    else:raise ValueError('unsupported diagnostic source')
    return text


PATHS=[
    'pg_lake_table/src/transaction/track_iceberg_metadata_changes.c',
    'pg_lake_engine/src/utils/s3_writer_utils.c',
    'pg_lake_engine/src/pgduck/parallel_command.c',
    'pg_lake_iceberg/src/iceberg/metadata_operations.c',
]


class Overlay:
    def __init__(self,q,root,candidate=False):
        self.q,self.root=q,root
        self.paths=PATHS+(['pg_lake_iceberg/src/iceberg/operations/manifest_merge.c',
            'pg_lake_iceberg/src/iceberg/read_manifest.c',
            'pg_lake_iceberg/include/pg_lake/iceberg/manifest_spec.h'] if candidate else [])
        self.metadata=dict(scope=__doc__,binary_overlay=True,sql_overlay=False,
            upstream_revision='8180647d4535fdb1ed223f92f301ba6094a8ec3e',detail_version=2,
            guarded_cleanup_candidate=candidate,preserves_unknown_deleted_counts=candidate,sources={})

    def build(self,context):
        q=self.q
        revision=q.command(['git','-C',str(self.root/'vendor/pg_lake'),'rev-parse','HEAD']).stdout.strip()
        if revision!=self.metadata['upstream_revision']:raise ValueError('unsupported upstream revision')
        self.metadata['base_destination_image_id']=q.command(['docker','image','inspect','--format','{{.Id}}',q.DESTINATION_IMAGE]).stdout.strip()
        recipe=[f'FROM {q.DESTINATION_IMAGE}','USER root','COPY commit-profile.h /opt/lake-commit-profile.h']
        (context/'commit-profile.h').write_text(HEADER)
        for index,path in enumerate(self.paths):
            original=(self.root/'vendor/pg_lake'/path).read_bytes();before=hashlib.sha256(original).hexdigest()
            actual=q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,'/opt/pg_lake/'+path]).stdout.split()[0]
            if before!=actual:raise ValueError('cached upstream source differs: '+path)
            patched=patch(path,original.decode());name=f'commit-profile-{index}.c'
            (context/name).write_text(patched)
            (q.RESULTS/name).write_text(patched)
            self.metadata['sources'][path]=dict(original_sha256=before,patched_sha256=hashlib.sha256(patched.encode()).hexdigest())
            recipe.append(f'COPY {name} /opt/pg_lake/{path}')
        libraries=['pg_lake_engine','pg_lake_iceberg','pg_lake_table']
        self.metadata['original_libraries']={name:q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,f'/usr/lib/postgresql/18/lib/{name}.so']).stdout.split()[0] for name in libraries}
        for name in libraries:
            # The prototype extends an internal struct shared by these two
            # modules. Recompile their objects together to avoid mixing layouts.
            clean=f'find /opt/pg_lake/{name}/src -name "*.o" -delete && ' if self.metadata['guarded_cleanup_candidate'] and name in ['pg_lake_iceberg','pg_lake_table'] else ''
            recipe.append(f'RUN {clean}make -C /opt/pg_lake/{name} -j2 {name}.so with_llvm=no && cp /opt/pg_lake/{name}/{name}.so /usr/lib/postgresql/18/lib/{name}.so')
        text='\n'.join(recipe)+'\n';(context/'Dockerfile.commit-profile').write_text(text)
        q.command(['docker','build','-t',q.DESTINATION_IMAGE,'-f',str(context/'Dockerfile.commit-profile'),str(context)],timeout=600)
        self.metadata['profiled_libraries']={name:q.command(['docker','run','--rm','--entrypoint','sha256sum',q.DESTINATION_IMAGE,f'/usr/lib/postgresql/18/lib/{name}.so']).stdout.split()[0] for name in libraries}
        self.metadata['header_sha256']=hashlib.sha256(HEADER.encode()).hexdigest()
        (q.RESULTS/'commit-profile.h').write_text(HEADER)
        (q.RESULTS/'Dockerfile.commit-profile').write_text(text)
        (q.RESULTS/'commit-profile-overlay.json').write_text(json.dumps(self.metadata,indent=2)+'\n')
