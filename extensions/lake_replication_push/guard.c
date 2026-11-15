#include "postgres.h"
#include "fmgr.h"
#include "access/relation.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_type_d.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "commands/event_trigger.h"
#include "executor/spi.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(lake_push_guard_drop);
Datum lake_push_guard_drop(PG_FUNCTION_ARGS)
{
    EventTriggerData *event;
    DropStmt *drop;
    ListCell *cell;
    if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) elog(ERROR,"push guard requires event trigger context");
    event=(EventTriggerData *)fcinfo->context;
    if (!IsA(event->parsetree,DropStmt)) PG_RETURN_NULL();
    drop=(DropStmt *)event->parsetree;
    if (drop->removeType!=OBJECT_EXTENSION) PG_RETURN_NULL();
    SPI_connect();
    PushActiveSnapshot(GetLatestSnapshot());
    foreach(cell,drop->objects)
    {
        Node *object=lfirst(cell);
        char *name=IsA(object,String) ? strVal(object) : strVal(linitial((List *)object));
        Oid types[1]={TEXTOID};Datum args[1]={CStringGetTextDatum(name)};
        bool null;
        SPI_execute_with_args("WITH RECURSIVE required(oid) AS ("
            "SELECT oid FROM pg_catalog.pg_extension WHERE extname='lake_replication_push' UNION "
            "SELECT d.refobjid FROM pg_catalog.pg_depend d JOIN required r ON d.objid=r.oid "
            "WHERE d.classid='pg_catalog.pg_extension'::pg_catalog.regclass AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass) "
            "SELECT EXISTS(SELECT FROM required JOIN pg_catalog.pg_extension e USING(oid) WHERE e.extname=$1) "
            "AND EXISTS(SELECT FROM lake_replication.push_bindings)",1,types,args,NULL,true,1);
        if (DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&null)))
            ereport(ERROR,(errmsg("drop push subscriptions before removing the optional transport or its dependencies")));
    }
    PopActiveSnapshot();
    SPI_finish();PG_RETURN_NULL();
}

/* SQL LOCK TABLE rejects foreign tables. Hold ordinary transaction locks,
 * process invalidations after waits, and let SQL validate the pinned contract. */
PG_FUNCTION_INFO_V1(lake_push_lock_schema_targets);
Datum lake_push_lock_schema_targets(PG_FUNCTION_ARGS)
{
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(0);
    bool exclusive = PG_GETARG_BOOL(1);
    Datum *values;
    bool *nulls;
    int count,index;
    Oid targets[2],swap;
    LOCKMODE mode = exclusive ? AccessExclusiveLock : RowExclusiveLock;
    if (ARR_NDIM(array) != 1 || ARR_LBOUND(array)[0] != 1)
        ereport(ERROR,(errmsg("schema publication requires two destination locks")));
    deconstruct_array(array,REGCLASSOID,4,true,TYPALIGN_INT,&values,&nulls,&count);
    if (count != 2 || nulls[0] || nulls[1])
        ereport(ERROR,(errmsg("schema publication requires two destination locks")));
    targets[0] = DatumGetObjectId(values[0]); targets[1] = DatumGetObjectId(values[1]);
    if (!OidIsValid(targets[0]) || !OidIsValid(targets[1]) || targets[0] == targets[1])
        ereport(ERROR,(errmsg("schema publication requires distinct destinations")));
    if (targets[0] > targets[1]) { swap=targets[0];targets[0]=targets[1];targets[1]=swap; }
    for (index=0;index<2;index++)
    {
        Relation relation;
        LockRelationOid(targets[index],mode);
        AcceptInvalidationMessages();
        relation=relation_open(targets[index],NoLock);
        if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE ||
            relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
            ereport(ERROR,(errmsg("schema publication requires permanent foreign destinations")));
        relation_close(relation,NoLock);
    }
    PG_RETURN_VOID();
}
