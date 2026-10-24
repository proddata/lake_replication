/* Experimental pool: use any immediately available transaction slot. If full,
 * join one slot's native queue. This bounds concurrency but does not promise a
 * single global FIFO; PostgreSQL owns cancellation and rollback cleanup.
 */
PG_FUNCTION_INFO_V1(lake_replication_experiment_admit_stream);

Datum
lake_replication_experiment_admit_stream(PG_FUNCTION_ARGS)
{
    LOCKTAG tags[4];
    int i;

    for (i = 0; i < 4; i++)
    {
        SET_LOCKTAG_ADVISORY(tags[i], InvalidOid, 0x4c524550U, 3 + i, 1);
        if (LockHeldByMe(&tags[i], ExclusiveLock, false))
            PG_RETURN_VOID();
    }
    for (i = 0; i < 4; i++)
        if (LockAcquire(&tags[i], ExclusiveLock, false, true) != LOCKACQUIRE_NOT_AVAIL)
            PG_RETURN_VOID();
    (void) LockAcquire(&tags[MyProcPid % 4], ExclusiveLock, false, false);
    PG_RETURN_VOID();
}
