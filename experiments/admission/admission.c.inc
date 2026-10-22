/* One transaction at a time across this postmaster, including all databases.
 * Database OID zero reserves a cluster-wide advisory-lock namespace; ordinary
 * pg_advisory_* SQL uses MyDatabaseId and cannot accidentally share this key.
 * Use PostgreSQL's transaction lock ownership/queue and interrupt handling:
 * there is no user-space permit counter or stale-PID recovery protocol.
 * This is resource admission, not authorization or cross-server fencing.
 */
PG_FUNCTION_INFO_V1(lake_replication_prototype_admit_writer);

Datum
lake_replication_prototype_admit_writer(PG_FUNCTION_ARGS)
{
    LOCKTAG tag;

    SET_LOCKTAG_ADVISORY(tag, InvalidOid, 0x4c524550U, 1, 1);
    if (!LockHeldByMe(&tag, ExclusiveLock, false))
        (void) LockAcquire(&tag, ExclusiveLock, false, false);
    PG_RETURN_VOID();
}
