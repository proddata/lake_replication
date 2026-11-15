#include "postgres.h"
#include "fmgr.h"
#include "access/relation.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* Only side-effect-free local Iceberg INSERT targets may be regrouped. */
PG_FUNCTION_INFO_V1(lake_replication_lock_insert_group_targets);

static int
compare_target_oid(const void *left, const void *right)
{
    Oid a = *((const Oid *) left), b = *((const Oid *) right);
    return a > b ? 1 : (a < b ? -1 : 0);
}

Datum
lake_replication_lock_insert_group_targets(PG_FUNCTION_ARGS)
{
    ArrayType *array = PG_GETARG_ARRAYTYPE_P(0);
    Datum *values;
    bool *nulls;
    Oid *targets;
    int count, index;
    bool eligible = true;

    if (ARR_NDIM(array) != 1 || ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array)) > 32)
        PG_RETURN_BOOL(false);
    deconstruct_array(array, REGCLASSOID, 4, true, TYPALIGN_INT, &values, &nulls, &count);
    if (count == 0)
        PG_RETURN_BOOL(false);
    targets = palloc(sizeof(Oid) * count);
    for (index = 0; index < count; index++)
    {
        if (nulls[index])
            PG_RETURN_BOOL(false);
        targets[index] = DatumGetObjectId(values[index]);
    }
    qsort(targets, count, sizeof(Oid), compare_target_oid);
    for (index = 0; index < count; index++)
    {
        if (index == 0 || targets[index] != targets[index - 1])
            LockRelationOid(targets[index], RowExclusiveLock);
    }
    /* DDL may have committed while acquiring locks, even under an old caller
     * snapshot. Inspect current relcache/syscache metadata after the wait.
     * These transaction locks also fence trigger/rule/schema changes. */
    AcceptInvalidationMessages();
    for (index = 0; index < count; index++)
    {
        Relation relation;
        TupleDesc descriptor;
        AclResult acl;
        int attribute;
        if (index > 0 && targets[index] == targets[index - 1])
            continue;
        relation = relation_open(targets[index], NoLock);
        acl = pg_class_aclcheck(targets[index], GetUserId(), ACL_INSERT);
        if (acl != ACLCHECK_OK)
            aclcheck_error(acl, OBJECT_TABLE, RelationGetRelationName(relation));
        descriptor = RelationGetDescr(relation);
        if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE ||
            relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT ||
            relation->rd_rel->relispartition || relation->rd_rel->relhassubclass ||
            relation->rd_rel->relrowsecurity || relation->rd_rel->relforcerowsecurity ||
            relation->rd_rel->relhasrules ||
            relation->trigdesc != NULL ||
            (descriptor->constr != NULL &&
             (descriptor->constr->num_check > 0 || descriptor->constr->num_defval > 0)))
            eligible = false;
        for (attribute = 0; attribute < descriptor->natts; attribute++)
        {
            Form_pg_attribute column = TupleDescAttr(descriptor, attribute);
            if (column->attisdropped)
                continue;
            if (column->attgenerated || column->attidentity)
                eligible = false;
            /* Domain checks and custom input functions may observe other tables. */
            switch (column->atttypid)
            {
                case BOOLOID: case INT2OID: case INT4OID: case INT8OID:
                case FLOAT4OID: case FLOAT8OID: case NUMERICOID:
                case TEXTOID: case VARCHAROID: case BPCHAROID: case BYTEAOID:
                case DATEOID: case TIMESTAMPOID: case TIMESTAMPTZOID:
                case UUIDOID: case JSONBOID:
                    break;
                default:
                    eligible = false;
                    break;
            }
        }
        if (relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        {
            ForeignTable *table = GetForeignTable(targets[index]);
            ForeignServer *server = GetForeignServer(table->serverid);
            ForeignDataWrapper *wrapper = GetForeignDataWrapper(server->fdwid);
            bool local_catalog = false;
            ListCell *cell;
            foreach(cell, table->options)
            {
                DefElem *option = lfirst_node(DefElem, cell);
                if (strcmp(option->defname, "catalog") == 0 &&
                    strcmp(defGetString(option), "postgres") == 0)
                    local_catalog = true;
            }
            if (strcmp(wrapper->fdwname, "pg_lake_iceberg") != 0 || !local_catalog)
                eligible = false;
        }
        relation_close(relation, NoLock);
    }
    PG_RETURN_BOOL(eligible);
}
