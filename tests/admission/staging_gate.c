/* Disposable test helper, never installed by a production migration. */
#include "postgres.h"
#include "fmgr.h"
#include "storage/lock.h"

#ifndef STAGING_GATE_ENABLED
#define STAGING_GATE_ENABLED 1
#endif

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(staging_gate_admit);

Datum
staging_gate_admit(PG_FUNCTION_ARGS)
{
#if STAGING_GATE_ENABLED
    LOCKTAG tag;

    /* Instance-wide, distinct from bulk=2 and stream slots=3..6. */
    SET_LOCKTAG_ADVISORY(tag, InvalidOid, 0x4c524550U, 71, 1);
    if (!LockHeldByMe(&tag, ExclusiveLock, false))
        (void) LockAcquire(&tag, ExclusiveLock, false, false);
#endif
    PG_RETURN_VOID();
}
