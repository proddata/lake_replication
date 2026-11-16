#!/usr/bin/env python3
"""Backend-local diagnostic counters for the pinned native file catalog reader."""
import hashlib
import json
from pathlib import Path
import subprocess

source=Path('/opt/pg_lake/pg_lake_table/src/fdw/data_files_catalog.c')
expected='af8189311e0db7e5bc6f94770ea75db1dcad1145c1379696d111f88b21b02538'
before=source.read_bytes()
if hashlib.sha256(before).hexdigest()!=expected:raise RuntimeError('unsupported catalog-reader source')
s=before.decode().replace('#include "postgres.h"','#include "postgres.h"\n#include "portability/instr_time.h"',1)
marker='/* global hook override */'
s=s.replace(marker,'''/* Explicitly enabled only by the private diagnostic SQL function. */
typedef struct LakeDirectCatalogCounter {
 uint64 calls, rows, files;
 double queryMs, decodeMs;
} LakeDirectCatalogCounter;
static LakeDirectCatalogCounter lakeDirectCounters[16];
static bool lakeDirectCatalogEnabled = false;

'''+marker,1)
start=s.index('\nGetTableDataFilesHashFromCatalog(');end=s.index('\n\n/*\n * GetTableDataFilesByPathHashFromCatalog',start)
f=s[start:end];brace=f.index('\n{')+2
f=f[:brace]+'''
 bool profile = lakeDirectCatalogEnabled;
 instr_time profileStart, profileQueryEnd, profileEnd;
 uint64 profileRows = 0;
 if (profile) INSTR_TIME_SET_CURRENT(profileStart);
'''+f[brace:]
f=f.replace('SPI_START_EXTENSION_OWNER(PgLakeTable);','''SPI_START_EXTENSION_OWNER(PgLakeTable);
 if (profile)
  (void) set_config_option("pg_stat_statements.track", "all", PGC_SUSET,
    PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);''',1)
f=f.replace('\n\tfor (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)','''
 if (profile) { INSTR_TIME_SET_CURRENT(profileQueryEnd); profileRows = SPI_processed; }
\tfor (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)''',1)
f=f.replace('\n\tSPI_END();','''
 if (profile) INSTR_TIME_SET_CURRENT(profileEnd);
\tSPI_END();
 if (profile) {
  int mask = (dataOnly ? 1 : 0) | (newFilesOnly ? 2 : 0) |
             (forUpdate ? 4 : 0) | (skipColumnStats ? 8 : 0);
  instr_time queryTime = profileQueryEnd, decodeTime = profileEnd;
  INSTR_TIME_SUBTRACT(queryTime, profileStart);
  INSTR_TIME_SUBTRACT(decodeTime, profileQueryEnd);
  lakeDirectCounters[mask].calls++;
  lakeDirectCounters[mask].rows += profileRows;
  lakeDirectCounters[mask].files += hash_get_num_entries(dataFilesHash);
  lakeDirectCounters[mask].queryMs += INSTR_TIME_GET_MILLISEC(queryTime);
  lakeDirectCounters[mask].decodeMs += INSTR_TIME_GET_MILLISEC(decodeTime);
 }
''',1)
s=s[:start]+f+s[end:]
s+='''
/* No SQL entry point is installed by the production extension. */
PG_FUNCTION_INFO_V1(lake_direct_catalog_profile);
Datum lake_direct_catalog_profile(PG_FUNCTION_ARGS) {
 int command = PG_GETARG_INT32(0);
 if (command == 1) {
  memset(lakeDirectCounters, 0, sizeof(lakeDirectCounters));
  lakeDirectCatalogEnabled = true;
  PG_RETURN_TEXT_P(cstring_to_text("[]"));
 }
 if (command != 0) elog(ERROR, "unknown catalog profile command");
 lakeDirectCatalogEnabled = false;
 StringInfoData result; initStringInfo(&result); appendStringInfoChar(&result, '[');
 bool first = true;
 for (int i=0; i<16; i++) {
  LakeDirectCatalogCounter *c = &lakeDirectCounters[i];
  if (!c->calls) continue;
  if (!first) appendStringInfoChar(&result, ','); first=false;
  appendStringInfo(&result,
   "{\\"mask\\":%d,\\"data_only\\":%s,\\"new_only\\":%s,\\"for_update\\":%s,\\"skip_stats\\":%s,"
   "\\"calls\\":%llu,\\"rows\\":%llu,\\"files\\":%llu,\\"query_ms\\":%.6f,\\"decode_ms\\":%.6f}",
   i,(i&1)?"true":"false",(i&2)?"true":"false",(i&4)?"true":"false",(i&8)?"true":"false",
   (unsigned long long)c->calls,(unsigned long long)c->rows,(unsigned long long)c->files,
   c->queryMs,c->decodeMs);
 }
 appendStringInfoChar(&result, ']'); PG_RETURN_TEXT_P(cstring_to_text(result.data));
}
'''
source.write_text(s)
pg_config='/usr/bin/pg_config'
library=Path(subprocess.check_output([pg_config,'--pkglibdir'],text=True).strip())/'pg_lake_table.so'
record=dict(experimental=True,diagnostic_only=True,scope='Explicit backend-local file-catalog query/decode counters and nested pg_stat_statements visibility; production SQL entry points and read semantics unchanged',source_before_sha256=expected,source_after_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),library_before_sha256=hashlib.sha256(library.read_bytes()).hexdigest())
subprocess.run(['make','-C','/opt/pg_lake/pg_lake_table','-j2','pg_lake_table.so','with_llvm=no','PG_CONFIG='+pg_config],check=True)
library.write_bytes(Path('/opt/pg_lake/pg_lake_table/pg_lake_table.so').read_bytes())
record['library_after_sha256']=hashlib.sha256(library.read_bytes()).hexdigest()
marker=Path('/usr/share/pg_lake/lake-direct-catalog-profile.json');marker.parent.mkdir(parents=True,exist_ok=True);marker.write_text(json.dumps(record,indent=2)+'\n')
