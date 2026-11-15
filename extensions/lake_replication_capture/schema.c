/* Bounded live descriptors for schema capture. Call at the source DDL boundary;
 * decoding must consume the stored descriptor, never call this against a later catalog. */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_type_d.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

static void
schema_name(StringInfo output, const char *name)
{
    Size length = strlen(name);
    if (length == 0 || length >= NAMEDATALEN)
        ereport(ERROR,(errmsg("invalid schema descriptor identifier")));
    pq_sendint32(output,length);
    pq_sendbytes(output,name,length);
}

static Datum
describe_relation(Oid relation_id, bool json_result, bool validate_only)
{
    Relation relation;
    TupleDesc descriptor;
    Bitmapset *identity;
    StringInfoData output;
    StringInfoData json;
    char *namespace;
    int index;
    bool have_key = false;
    Oid primary;

    /* Lock first, then process invalidations. The caller's statement snapshot
     * may predate a DDL transaction that committed while this lock waited. */
    LockRelationOid(relation_id,AccessShareLock);
    AcceptInvalidationMessages();
    relation = table_open(relation_id,NoLock);
    namespace = get_namespace_name(RelationGetNamespace(relation));
    if (object_aclcheck(NamespaceRelationId,RelationGetNamespace(relation),GetUserId(),ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV,OBJECT_SCHEMA,namespace);
    if (pg_class_aclcheck(relation_id,GetUserId(),ACL_SELECT) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV,OBJECT_TABLE,RelationGetRelationName(relation));
    primary = RelationGetPrimaryKeyIndex(relation,false);
    if (GetDatabaseEncoding() != PG_UTF8 || relation->rd_rel->relkind != RELKIND_RELATION ||
        relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT || relation->rd_rel->relispartition ||
        relation->rd_rel->relhassubclass || has_superclass(relation_id) ||
        relation->rd_rel->relrowsecurity || relation->rd_rel->relforcerowsecurity ||
        !OidIsValid(primary) || primary != RelationGetReplicaIndex(relation) ||
        (relation->rd_rel->relreplident != REPLICA_IDENTITY_DEFAULT && relation->rd_rel->relreplident != REPLICA_IDENTITY_INDEX))
        ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("schema descriptors require a UTF8 permanent heap with an immediate primary-key identity and no row-level security or inheritance")));
    descriptor = RelationGetDescr(relation);
    if (descriptor->natts < 1 || descriptor->natts > 1600)
        ereport(ERROR,(errmsg("schema descriptor column count exceeded")));
    identity = RelationGetIdentityKeyBitmap(relation);
    initStringInfo(&json);
    if (json_result)
    {
        appendStringInfoString(&json,"{\"namespace\":");escape_json(&json,namespace);
        appendStringInfoString(&json,",\"name\":");escape_json(&json,RelationGetRelationName(relation));
        appendStringInfo(&json,",\"replica_identity\":\"%c\",\"columns\":[",relation->rd_rel->relreplident);
    }
    pq_begintypsend(&output);
    pq_sendbytes(&output,"LKDSC1",6);
    schema_name(&output,namespace);
    schema_name(&output,RelationGetRelationName(relation));
    pq_sendbyte(&output,relation->rd_rel->relreplident);
    pq_sendint32(&output,descriptor->natts);
    for (index=0;index<descriptor->natts;index++)
    {
        Form_pg_attribute attribute = TupleDescAttr(descriptor,index);
        bool key = bms_is_member(attribute->attnum-FirstLowInvalidHeapAttributeNumber,identity);
        if (attribute->attnum != index+1 || attribute->attisdropped || attribute->attgenerated ||
            (attribute->atttypid != BOOLOID && attribute->atttypid != INT8OID &&
             attribute->atttypid != INT4OID && attribute->atttypid != TEXTOID) ||
            (key && !attribute->attnotnull))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("capture requires supported scalar columns and a primary-key identity")));
        if (!validate_only && (attribute->attidentity || attribute->atthasdef ||
            attribute->attinhcount || attribute->atttypmod != -1 ||
            attribute->attcollation != (attribute->atttypid == TEXTOID ? DEFAULT_COLLATION_OID : InvalidOid)))
            ereport(ERROR,(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("unsupported schema descriptor column: require fixed scalars without defaults, generation, custom collation or dropped/inherited columns")));
        have_key |= key;
        pq_sendint32(&output,attribute->attnum); /* Stable PostgreSQL column identity. */
        schema_name(&output,NameStr(attribute->attname));
        pq_sendint32(&output,attribute->atttypid);
        pq_sendint32(&output,attribute->atttypmod);
        pq_sendbyte(&output,key ? 1 : 0);
        pq_sendbyte(&output,attribute->attnotnull ? 0 : 1);
        if (json_result)
        {
            if (index) appendStringInfoChar(&json,',');
            appendStringInfo(&json,"{\"id\":%d,\"name\":",attribute->attnum);
            escape_json(&json,NameStr(attribute->attname));
            appendStringInfo(&json,",\"type_oid\":%u,\"type_modifier\":%d,\"key\":%s,\"nullable\":%s}",
                attribute->atttypid,attribute->atttypmod,key ? "true" : "false",attribute->attnotnull ? "false" : "true");
        }
    }
    if (!have_key || output.len > 1024*1024+VARHDRSZ)
        ereport(ERROR,(errmsg("invalid or oversized schema descriptor")));
    bms_free(identity);
    pfree(namespace);
    table_close(relation,NoLock);
    UnlockRelationOid(relation_id,AccessShareLock);
    if (validate_only) return (Datum) 0;
    if (json_result)
    {
        appendStringInfoString(&json,"]}");
        if (json.len > 1024*1024) ereport(ERROR,(errmsg("schema JSON descriptor byte limit exceeded")));
        return DirectFunctionCall1(jsonb_in,CStringGetDatum(json.data));
    }
    return PointerGetDatum(pq_endtypsend(&output));
}

PG_FUNCTION_INFO_V1(lake_capture_describe_relation);
Datum lake_capture_describe_relation(PG_FUNCTION_ARGS) { return describe_relation(PG_GETARG_OID(0),false,false); }

PG_FUNCTION_INFO_V1(lake_capture_describe_relation_json);
Datum lake_capture_describe_relation_json(PG_FUNCTION_ARGS) { return describe_relation(PG_GETARG_OID(0),true,false); }

PG_FUNCTION_INFO_V1(lake_capture_validate_relation);
Datum lake_capture_validate_relation(PG_FUNCTION_ARGS)
{
    describe_relation(PG_GETARG_OID(0),false,true);
    PG_RETURN_VOID();
}
