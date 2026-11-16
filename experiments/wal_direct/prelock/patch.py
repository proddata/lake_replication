#!/usr/bin/env python3
"""Private SQL bridge to the pinned native transaction-scoped table update lock."""
import hashlib
import json
from pathlib import Path
import subprocess

source=Path('/opt/pg_lake/pg_lake_table/src/fdw/writable_table.c')
expected='45ac53c795bebd42907cbb3a5f553c81dd901c549fed423903db5752e343c09d'
before=source.read_bytes()
if hashlib.sha256(before).hexdigest()!=expected:
    raise RuntimeError('unsupported native table-lock source')
source.write_bytes(before+b'''
/* Private experiment: installed explicitly in lake_direct only. No native
 * extension SQL, existing lock semantics or production entry points change. */
PG_FUNCTION_INFO_V1(lake_direct_lock_target);
Datum lake_direct_lock_target(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("direct lock experiment requires superuser")));
    Oid relationId = PG_GETARG_OID(0);
    if (!IsWritableIcebergTable(relationId))
        ereport(ERROR, (errmsg("direct lock target must be writable Iceberg")));
    LockTableForUpdate(relationId);
    PG_RETURN_VOID();
}
''')
pg_config='/usr/bin/pg_config'
library=Path(subprocess.check_output([pg_config,'--pkglibdir'],text=True).strip())/'pg_lake_table.so'
record=dict(experimental=True,scope='Private superuser-only SQL bridge to the existing transaction-scoped native table update lock; optional publication wrapper locks every mapped target in OID order before writes',source_before_sha256=expected,source_after_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),library_before_sha256=hashlib.sha256(library.read_bytes()).hexdigest())
subprocess.run(['make','-C','/opt/pg_lake/pg_lake_table','-j2','pg_lake_table.so','with_llvm=no','PG_CONFIG='+pg_config],check=True)
library.write_bytes(Path('/opt/pg_lake/pg_lake_table/pg_lake_table.so').read_bytes())
record['library_after_sha256']=hashlib.sha256(library.read_bytes()).hexdigest()
marker=Path('/usr/share/pg_lake/lake-direct-prelock-prototype.json');marker.parent.mkdir(parents=True,exist_ok=True);marker.write_text(json.dumps(record,indent=2)+'\n')
