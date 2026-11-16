#!/usr/bin/env python3
"""Experimental position-delete metadata read; not a release prerequisite patch."""
import hashlib
import json
from pathlib import Path
import subprocess

source=Path('/opt/pg_lake/pg_lake_table/src/fdw/position_delete_dest.c')
expected='f58b267c01d98cda885b64722108a8610c12cdc2eb62acbb64870351aaa05514'
before=source.read_bytes()
if hashlib.sha256(before).hexdigest()!=expected:raise RuntimeError('unsupported position-delete source')
old=b'false /* skipColumnStats */'
if before.count(old)!=1:raise RuntimeError('unexpected metadata call shape')
after=before.replace(old,b'true /* skipColumnStats: only path and file-level row counts are used */')
source.write_bytes(after)
pg_config='/usr/bin/pg_config'
library=Path(subprocess.check_output([pg_config,'--pkglibdir'],text=True).strip())/'pg_lake_table.so'
record=dict(experimental=True,scope='Position-delete receiver omits unused per-column statistics; paths, IDs, row/deleted counts and partition metadata remain loaded',source_before_sha256=expected,source_after_sha256=hashlib.sha256(after).hexdigest(),library_before_sha256=hashlib.sha256(library.read_bytes()).hexdigest())
subprocess.run(['make','-C','/opt/pg_lake/pg_lake_table','-j2','pg_lake_table.so','with_llvm=no','PG_CONFIG='+pg_config],check=True)
library.write_bytes(Path('/opt/pg_lake/pg_lake_table/pg_lake_table.so').read_bytes())
record['library_after_sha256']=hashlib.sha256(library.read_bytes()).hexdigest()
marker=Path('/usr/share/pg_lake/lake-direct-metadata-prototype.json');marker.parent.mkdir(parents=True,exist_ok=True);marker.write_text(json.dumps(record,indent=2)+'\n')
