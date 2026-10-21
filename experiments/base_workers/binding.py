#!/usr/bin/env python3
"""Add dispatch/cleanup origin checks only to an isolated extension source copy."""
from pathlib import Path
import sys
from adapt import replace_once


def adapt(root):
    source = root / 'extensions/lake_replication/lake_replication.c'
    text = source.read_text()
    old = 'WHERE destination_database <> current_database()'
    assert text.count(old) == 2
    text = text.replace(old, 'WHERE NOT lake_replication.prototype_binding_matches(subscription_id)')
    text = replace_once(text, '"FROM lake_replication.worker_launch_config "\n\t\t\t"WHERE subscription_id = %lld", (long long) subscriptionId),',
                        '"FROM lake_replication.worker_launch_config "\n\t\t\t"WHERE subscription_id = %lld AND lake_replication.prototype_binding_matches(subscription_id)", (long long) subscriptionId),')
    text = replace_once(text, '"destination_schema, credential_file, create_slot, desired_state) "',
                        '"destination_schema, credential_file, create_slot, desired_state, "\n        "origin_system_identifier, origin_database_oid) "')
    text = replace_once(text, '"VALUES (%lld, %s, %s, %s, %s, %s, %s, %s, %s, %s, \'running\') "',
                        '"VALUES (%lld, %s, %s, %s, %s, %s, %s, %s, %s, %s, \'running\', "\n        "(SELECT system_identifier FROM pg_control_system()), "\n        "(SELECT oid FROM pg_database WHERE datname=current_database())) "')
    anchor = 'static void\nhandle_drop_subscription(DropSubscriptionStmt *stmt, int64 subscriptionId)'
    before, after = text.split(anchor)
    end = 'static void\nhandle_enable_subscription'
    after, tail = after.split(end)
    after = replace_once(after, '\tSPI_connect();', '''\tSPI_connect();

    /* Check before deleting launch metadata or contacting the source. */
    if (SPI_execute(psprintf(
            "SELECT 1 WHERE lake_replication.prototype_binding_matches(%lld)",
            (long long) subscriptionId), true, 1) != SPI_OK_SELECT || SPI_processed != 1)
        ereport(ERROR,
                (errmsg("prototype: inherited destination binding cannot authorize source slot cleanup")));
''')
    source.write_text(before + anchor + after + end + tail)


if __name__ == '__main__':
    adapt(Path(sys.argv[1]))
