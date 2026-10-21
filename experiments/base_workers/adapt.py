#!/usr/bin/env python3
"""Adapt a disposable copy of the real launcher; never edit the shipped source."""

import sys
from pathlib import Path


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise RuntimeError(f"prototype adapter no longer matches source: {old[:80]}")
    return text.replace(old, new, 1)


def harden(text):
    """Apply the same correctness/performance fixes to both experimental variants."""
    text = replace_once(text, '#include <fcntl.h>', '#include <fcntl.h>\n#include <sys/prctl.h>')
    runtime = Path(__file__).with_name("runtime.c.inc").read_text()
    anchor = "static char *LauncherDatabaseName = NULL;"
    text = replace_once(text, anchor, anchor + "\n" + runtime)
    text = replace_once(text, "\t\t\t\t\t\t\tBackgroundWorkerHandle *handle;",
                        "\t\t\t\t\t\t\tBackgroundWorkerHandle *handle;\n"
                        "                            MemoryContext oldContext;\n"
                        "                            bool registered;")
    text = replace_once(text, "\t\t\t\t\t\t\tworker.bgw_notify_pid = 0;",
                        "\t\t\t\t\t\t\tworker.bgw_notify_pid = MyProcPid;")
    text = replace_once(text, "\t\t\t\t\t\t\tif (RegisterDynamicBackgroundWorker(&worker, &handle))", """
                            /* Outlive SPI_finish; independently measurable context. */
                            oldContext = MemoryContextSwitchTo(PrototypeHandleContext);
                            registered = RegisterDynamicBackgroundWorker(&worker, &handle);
                            MemoryContextSwitchTo(oldContext);
                            if (registered)""")
    # All retire paths wait before forgetting ownership or launching a replacement.
    old = 'TerminateBackgroundWorker(entry->handle);'
    assert text.count(old) == 3
    text = text.replace(old, old + '\n                        WaitForBackgroundWorkerShutdown(entry->handle);')
    old = 'hash_search(tracked, &subscriptionId, HASH_REMOVE, NULL);'
    assert text.count(old) == 4
    text = text.replace(old, 'pfree(entry->handle);\n                        ' + old)
    anchor = '\t\t\t\t\t\t  &hashCtl, HASH_ELEM | HASH_BLOBS);'
    text = replace_once(text, anchor, anchor + """
    PrototypeHandleContext = AllocSetContextCreate(TopMemoryContext,
        "lake_replication worker handles", ALLOCSET_DEFAULT_SIZES);
""")
    text = replace_once(text, "\tint\t\t\tfd;\n\n\tBackgroundWorkerUnblockSignals();", """\tint\t\t\tfd;

    pqsignal(SIGTERM, PrototypeWorkerSigterm);
    pqsignal(SIGCHLD, PrototypeWorkerSigchld);
    BackgroundWorkerUnblockSignals();""")
    text = replace_once(text, "\t\tpid_t\t\tchild = fork();", """        pid_t child;

        /* Reap orphaned Rust descendants instead of passing them to PID 1. */
        if (prctl(PR_SET_CHILD_SUBREAPER, 1) < 0)
        {
            elog(WARNING, "prototype: could not become a child subreaper: %m");
            proc_exit(0);
        }
        if (PrototypeWorkerTerminated)
            proc_exit(0);
        child = fork();""")
    text = replace_once(text, "\t\tif (child == 0)\n\t\t{",
                        "\t\tif (child == 0)\n\t\t{\n\t\t\tsetpgid(0, 0);")
    text = replace_once(text, "\t\t\tint\t\t\tstatus;\n\n\t\t\tif (waitpid(child, &status, 0) < 0)", """            int status;

            setpgid(child, child);
            PrototypeSupervisorPid = child;
            before_shmem_exit(PrototypeStopSupervisor, (Datum) 0);
            if (PrototypeWaitSupervisor(child, &status) < 0)""")
    anchor = '\t\t\tret = SPI_execute(\n\t\t\t\t"SELECT subscription_id, destination_database, desired_state, '
    guard = """            /* Fence copied bindings; unchanged errors do not produce tuple churn. */
            SPI_execute(
                "UPDATE lake_replication.subscriptions SET state='error', "
                "last_error='prototype: inherited destination binding requires an explicit rebuild' "
                "WHERE (state IS DISTINCT FROM 'error' OR last_error IS DISTINCT FROM "
                "'prototype: inherited destination binding requires an explicit rebuild') "
                "AND subscription_id IN (SELECT subscription_id "
                "FROM lake_replication.worker_launch_config "
                "WHERE destination_database <> current_database()); "
                "UPDATE lake_replication.worker_launch_config SET desired_state='stopped' "
                "WHERE destination_database <> current_database() AND desired_state <> 'stopped'",
                false, 0);

"""
    text = replace_once(text, anchor, guard + anchor)
    text = replace_once(text, "\tHTAB\t   *tracked;", """\tHTAB\t   *tracked;
    HASH_SEQ_STATUS cleanupScan;
    TrackedWorker *cleanupEntry;""")
    anchor = "\tproc_exit(0);\n}\n\n/*\n * The source slot name lives"
    cleanup = """    hash_seq_init(&cleanupScan, tracked);
    while ((cleanupEntry = (TrackedWorker *) hash_seq_search(&cleanupScan)) != NULL)
        TerminateBackgroundWorker(cleanupEntry->handle);
    hash_seq_init(&cleanupScan, tracked);
    while ((cleanupEntry = (TrackedWorker *) hash_seq_search(&cleanupScan)) != NULL)
        WaitForBackgroundWorkerShutdown(cleanupEntry->handle);
    MemoryContextDelete(PrototypeHandleContext);
    hash_destroy(tracked);
"""
    return replace_once(text, anchor, cleanup + anchor)


def adapt(root, hardened=False, variant="base"):
    if variant not in {"base", "static"}:
        raise ValueError(variant)
    if variant == "static" and not hardened:
        raise ValueError("static comparison requires shared hardening")
    directory = root / "extensions/lake_replication"
    source = directory / "lake_replication.c"
    text = source.read_text()
    if hardened:
        text = harden(text)
    if variant == "base":
        text = replace_once(text, '#include "commands/defrem.h"', '#include "commands/defrem.h"\n#include "commands/dbcommands.h"')
        text = replace_once(text, "static char *LauncherDatabaseName = NULL;", "static char *LauncherDatabaseName = NULL;\nstatic bool PrototypeAlreadyConnected = false;")
        anchor = "extern PGDLLEXPORT void lake_replication_worker_main(Datum main_arg);"
        text = replace_once(text, anchor, anchor + """

    PG_FUNCTION_INFO_V1(lake_replication_prototype_launcher);

    Datum
    lake_replication_prototype_launcher(PG_FUNCTION_ARGS)
    {
        StartTransactionCommand();
        LauncherDatabaseName = MemoryContextStrdup(TopMemoryContext,
                                                  get_database_name(MyDatabaseId));
        CommitTransactionCommand();
        PrototypeAlreadyConnected = true;
        lake_replication_launcher_main((Datum) 0);
        /* Base workers use the return value, not bgw_restart_time. */
        PG_RETURN_INT64(1000);
    }
    """)
        text = replace_once(text, "\tBackgroundWorkerInitializeConnection(LauncherDatabaseName, NULL, 0);", "\tif (!PrototypeAlreadyConnected)\n\t\tBackgroundWorkerInitializeConnection(LauncherDatabaseName, NULL, 0);")
        text = replace_once(text, "\tif (process_shared_preload_libraries_in_progress)\n", "\tif (false) /* prototype: pg_extension_base owns launcher registration */\n")
        text = replace_once(text, "\tproc_exit(0);\n}\n\n/*\n * The source slot name lives", "\treturn; /* return restart policy to pg_extension_base */\n}\n\n/*\n * The source slot name lives")
    source.write_text(text)
    if variant == "static":
        return
    control = directory / "lake_replication.control"
    text = replace_once(control.read_text(), "requires = 'pg_lake,dblink'", "requires = 'pg_lake,dblink,pg_extension_base'")
    control.write_text(text + "#!shared_preload_libraries\n")


if __name__ == "__main__":
    adapt(Path(sys.argv[1]), hardened="--hardened" in sys.argv[2:],
          variant="static" if "--static" in sys.argv[2:] else "base")
