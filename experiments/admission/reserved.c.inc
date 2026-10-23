/* Experimental instance-wide engine admission. PostgreSQL owns cancellation,
 * savepoint rollback and backend-death cleanup. This is not authorization.
 * Reserved mode allows one bulk and one streaming transaction concurrently;
 * global mode serializes only engine work, to isolate the reservation effect.
 */
PG_FUNCTION_INFO_V1(lake_replication_experiment_admit_engine);

Datum
lake_replication_experiment_admit_engine(PG_FUNCTION_ARGS)
{
    bool bulk = PG_GETARG_BOOL(0);
    LOCKTAG tag;
#if LAKE_ADMISSION_BULK_ONLY
    if (!bulk)
        PG_RETURN_VOID();
#endif
#if LAKE_ADMISSION_RESERVED
    LOCKTAG other;
    SET_LOCKTAG_ADVISORY(other, InvalidOid, 0x4c524550U, bulk ? 1 : 2, 1);
    if (LockHeldByMe(&other, ExclusiveLock, false))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("cannot mix engine admission lanes in one transaction")));
#endif
    SET_LOCKTAG_ADVISORY(tag, InvalidOid, 0x4c524550U,
                        LAKE_ADMISSION_RESERVED && bulk ? 2 : 1, 1);
    if (!LockHeldByMe(&tag, ExclusiveLock, false))
        (void) LockAcquire(&tag, ExclusiveLock, false, false);
    PG_RETURN_VOID();
}
