#!/usr/bin/env python3
"""Patch only the disposable C copy for a postmaster-level deployment identity."""
from pathlib import Path
import sys
from adapt import replace_once


def adapt(root):
    path = root / 'extensions/lake_replication/lake_replication.c'
    text = path.read_text()
    text = replace_once(text, 'static char *LauncherDatabaseName = NULL;', 'static char *LauncherDatabaseName = NULL;\nstatic char *PrototypeInstanceId = NULL;')
    text = replace_once(text, '_PG_init(void)\n{', '''_PG_init(void)
{
    DefineCustomStringVariable("lake_replication.prototype_instance_id",
        "External deployment identity; empty disables prototype ownership.",
        NULL, &PrototypeInstanceId, "", PGC_POSTMASTER, 0, NULL, NULL, NULL);
''')
    text = replace_once(text, '"origin_system_identifier, origin_database_oid) "', '"origin_system_identifier, origin_database_oid, origin_instance_id) "')
    text = replace_once(text, '"(SELECT oid FROM pg_database WHERE datname=current_database())) "',
                        '"(SELECT oid FROM pg_database WHERE datname=current_database()), "\n        "NULLIF(current_setting(\'lake_replication.prototype_instance_id\'),\'\')) "')
    path.write_text(text)


if __name__ == '__main__':
    adapt(Path(sys.argv[1]))
