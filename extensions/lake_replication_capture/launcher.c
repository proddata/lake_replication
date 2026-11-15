/* pg_extension_base owns this per-database manager. Its Rust child performs
 * decoding reads and publication in separate source transactions. */
#include "postgres.h"
#include "fmgr.h"
#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "catalog/namespace.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace_d.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "parser/parse_func.h"
#include "nodes/parsenodes.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "libpq/pqsignal.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/inval.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "utils/snapmgr.h"
#include "pg_extension_base/base_workers.h"
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#include <sys/syscall.h>
#endif

/* SQL LOCK TABLE rejects foreign tables. Native relation locks serialize
 * catalog location checks with ALTER/DROP; session locks also span the separate
 * VACUUM transactions used by the capture driver's maintenance path. */
static LockRelId private_held[4];
static bool private_session_held[4];
static bool private_callbacks_registered = false;

static void
capture_release_private_locks(int code, Datum arg)
{
    int index;
    for (index = 0; index < 4; index++)
        if (private_session_held[index])
        {
            UnlockRelationIdForSession(&private_held[index], AccessShareLock);
            private_session_held[index] = false;
        }
}

static void
capture_private_lock_xact(XactEvent event, void *arg)
{
    /* Standard session locks are released by PostgreSQL on transaction abort. */
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
        MemSet(private_session_held, 0, sizeof(private_session_held));
}

PG_FUNCTION_INFO_V1(lake_capture_lock_private_logs);
Datum
lake_capture_lock_private_logs(PG_FUNCTION_ARGS)
{
    const char *names[4] = {"controls", "events_a", "events_b", "manifests"};
    Oid namespace = get_namespace_oid("lake_capture_log", false);
    Oid relations[4];
    bool session = PG_GETARG_BOOL(0);
    JsonbParseState *builder = NULL;
    JsonbValue *result;
    int index;
    if (object_aclcheck(NamespaceRelationId, namespace, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_SCHEMA, "lake_capture_log");
    if (session && !private_callbacks_registered)
    {
        before_shmem_exit(capture_release_private_locks, 0);
        RegisterXactCallback(capture_private_lock_xact, NULL);
        private_callbacks_registered = true;
    }
    for (index = 0; index < 4; index++)
    {
        LockRelId relation;
        relations[index] = get_relname_relid(names[index], namespace);
        if (!OidIsValid(relations[index]))
            ereport(ERROR, (errmsg("capture private log relation is missing")));
        if (pg_class_aclcheck(relations[index], GetUserId(), ACL_SELECT) != ACLCHECK_OK)
            aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLE, names[index]);
        relation.dbId = MyDatabaseId;
        relation.relId = relations[index];
        if (session)
        {
            if (private_session_held[index] && private_held[index].relId != relation.relId)
                ereport(ERROR, (errmsg("capture private log relation identity changed")));
            if (!private_session_held[index])
            {
                LockRelationIdForSession(&relation, AccessShareLock);
                private_held[index] = relation;
                private_session_held[index] = true;
            }
        }
        else
            LockRelationOid(relations[index], AccessShareLock);
    }
    /* A lock may have waited for committed DDL after the SQL statement's MVCC
     * snapshot was taken. Syscache + invalidation observes that catalog change;
     * an ordinary pg_foreign_table SELECT could still see its older snapshot. */
    AcceptInvalidationMessages();
    pushJsonbValue(&builder, WJB_BEGIN_OBJECT, NULL);
    for (index = 0; index < 4; index++)
    {
        HeapTuple tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relations[index]));
        Datum options;
        Datum *values;
        bool is_null, *nulls;
        int count, option;
        char *location = NULL;
        JsonbValue value;
        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errmsg("capture private log must remain a foreign table")));
        options = SysCacheGetAttr(FOREIGNTABLEREL, tuple, Anum_pg_foreign_table_ftoptions, &is_null);
        if (is_null) ereport(ERROR, (errmsg("capture private log has no location")));
        deconstruct_array(DatumGetArrayTypeP(options), TEXTOID, -1, false, TYPALIGN_INT,
                          &values, &nulls, &count);
        for (option = 0; option < count; option++)
        {
            char *option_text;
            if (nulls[option]) continue;
            option_text = TextDatumGetCString(values[option]);
            if (strncmp(option_text, "location=", 9) == 0) location = option_text + 9;
        }
        if (location == NULL || location[0] == '\0')
            ereport(ERROR, (errmsg("capture private log has no location")));
        value.type = jbvString;
        value.val.string.val = (char *) names[index];
        value.val.string.len = strlen(names[index]);
        pushJsonbValue(&builder, WJB_KEY, &value);
        value.val.string.val = location;
        value.val.string.len = strlen(location);
        pushJsonbValue(&builder, WJB_VALUE, &value);
        ReleaseSysCache(tuple);
    }
    result = pushJsonbValue(&builder, WJB_END_OBJECT, NULL);
    PG_RETURN_JSONB_P(JsonbValueToJsonb(result));
}

static volatile sig_atomic_t capture_shutdown = false;
static void capture_sigterm(SIGNAL_ARGS) { capture_shutdown = true; SetLatch(MyLatch); }
static void stop_child(pid_t child)
{
    int index;
    if (child <= 0) return;
    kill(child, SIGTERM);
    for (index = 0; index < 20; index++)
    {
        if (waitpid(child, NULL, WNOHANG) == child) return;
        pg_usleep(100000L);
    }
    kill(child, SIGKILL);
    while (waitpid(child, NULL, 0) < 0 && errno == EINTR) {}
}
static void begin_sql(void)
{
    StartTransactionCommand(); PushActiveSnapshot(GetTransactionSnapshot()); SPI_connect();
}
static void end_sql(void)
{
    SPI_finish(); PopActiveSnapshot(); CommitTransactionCommand();
}

PG_FUNCTION_INFO_V1(lake_capture_start_manager);
Datum
lake_capture_start_manager(PG_FUNCTION_ARGS)
{
    Oid argument=INTERNALOID;
    Oid entry;
    if (!superuser()) ereport(ERROR,(errmsg("capture manager repair requires source superuser")));
    entry=LookupFuncName(list_make2(makeString("lake_capture"),makeString("worker")),1,&argument,false);
    RegisterBaseWorker("lake_replication capture",entry,get_extension_oid("lake_replication_capture",false));
    PG_RETURN_VOID();
}

static bool
capture_private_schema(const char *name)
{
    return name && (strcmp(name, "lake_capture") == 0 || strcmp(name, "lake_capture_log") == 0);
}

static bool
capture_private_relation(RangeVar *target)
{
    Oid relation;
    if (target == NULL) return false;
    relation = RangeVarGetRelid(target, NoLock, true);
    return OidIsValid(relation) && capture_private_schema(get_namespace_name(get_rel_namespace(relation)));
}

/* Resolve DDL targets with the caller's search_path. Elevate only this fixed,
 * fully qualified metadata query, then restore the caller even on error. */
static bool
capture_registered_streams(void)
{
    Oid extension = get_extension_oid("lake_replication_capture", true);
    HeapTuple tuple;
    Oid saved_user, owner;
    int saved_context;
    bool registered = false;
    if (!OidIsValid(extension)) return false;
    tuple = SearchSysCache1(EXTENSIONOID, ObjectIdGetDatum(extension));
    if (!HeapTupleIsValid(tuple)) return false;
    owner = ((Form_pg_extension) GETSTRUCT(tuple))->extowner;
    ReleaseSysCache(tuple);
    GetUserIdAndSecContext(&saved_user, &saved_context);
    SetUserIdAndSecContext(owner, saved_context | SECURITY_LOCAL_USERID_CHANGE);
    PG_TRY();
    {
        SPI_connect();
        PushActiveSnapshot(GetLatestSnapshot());
        SPI_execute("SELECT 1 FROM lake_capture.streams LIMIT 1", true, 1);
        registered = SPI_processed != 0;
        PopActiveSnapshot();
        SPI_finish();
    }
    PG_FINALLY();
    {
        SetUserIdAndSecContext(saved_user, saved_context);
    }
    PG_END_TRY();
    return registered;
}

/* Event checks must see registrations committed after a repeatable-read
 * caller's snapshot. The STABLE checker keeps all its internal reads on this
 * explicitly supplied current snapshot. Its fixed search path excludes caller
 * objects; target resolution in the early guard still uses the caller's path. */
PG_FUNCTION_INFO_V1(lake_capture_guard_ddl);
Datum
lake_capture_guard_ddl(PG_FUNCTION_ARGS)
{
    EventTriggerData *event;
    Oid types[1] = {TEXTOID};
    Datum args[1];
    if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
        elog(ERROR, "capture guard requires event trigger context");
    event = (EventTriggerData *) fcinfo->context;
    args[0] = CStringGetTextDatum(event->event);
    SPI_connect();
    CommandCounterIncrement();
    PushActiveSnapshot(GetLatestSnapshot());
    SPI_execute_with_args("SELECT lake_capture.guard_ddl_check($1)",
                          1, types, args, NULL, true, 0);
    PopActiveSnapshot();
    SPI_execute_with_args("SELECT lake_capture.guard_record_schema_ddl($1)",
                          1, types, args, NULL, false, 0);
    SPI_finish();
    PG_RETURN_NULL();
}

/* Parse the actual DROP target, not current_query text. Unrelated extensions
 * must remain independently manageable while source capture is installed. */
PG_FUNCTION_INFO_V1(lake_capture_guard_extension_drop);
Datum
lake_capture_guard_extension_drop(PG_FUNCTION_ARGS)
{
    EventTriggerData *event;
    DropStmt *drop;
    ListCell *cell;
    bool private_target = false;
    if (!CALLED_AS_EVENT_TRIGGER(fcinfo)) elog(ERROR,"capture guard requires event trigger context");
    event=(EventTriggerData *)fcinfo->context;
    if (IsA(event->parsetree, AlterTableStmt))
        private_target = capture_private_relation(((AlterTableStmt *) event->parsetree)->relation);
    else if (IsA(event->parsetree, RenameStmt))
    {
        RenameStmt *rename = (RenameStmt *) event->parsetree;
        private_target = capture_private_relation(rename->relation);
        if (rename->renameType == OBJECT_SCHEMA)
            private_target = capture_private_schema(rename->subname);
    }
    else if (IsA(event->parsetree, AlterObjectSchemaStmt))
    {
        AlterObjectSchemaStmt *move = (AlterObjectSchemaStmt *) event->parsetree;
        private_target = capture_private_relation(move->relation) || capture_private_schema(move->newschema);
    }
    else if (IsA(event->parsetree, DropStmt))
    {
        DropStmt *target = (DropStmt *) event->parsetree;
        foreach(cell, target->objects)
        {
            Node *object = lfirst(cell);
            if (target->removeType == OBJECT_TABLE || target->removeType == OBJECT_FOREIGN_TABLE)
                private_target |= capture_private_relation(makeRangeVarFromNameList((List *) object));
            else if (target->removeType == OBJECT_SCHEMA)
                private_target |= capture_private_schema(IsA(object, String) ? strVal(object) : strVal(linitial((List *) object)));
        }
    }
    if (private_target)
    {
        if (capture_registered_streams())
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("capture private schema is frozen until capture streams are dropped")));
    }
    if (!IsA(event->parsetree,DropStmt)) PG_RETURN_NULL();
    drop=(DropStmt *)event->parsetree;
    if (drop->removeType!=OBJECT_EXTENSION) PG_RETURN_NULL();
    if (!capture_registered_streams()) PG_RETURN_NULL();
    SPI_connect();
    PushActiveSnapshot(GetLatestSnapshot());
    foreach(cell,drop->objects)
    {
        Node *object=lfirst(cell);
        char *name=IsA(object,String) ? strVal(object) : strVal(linitial((List *)object));
        Oid types[1]={TEXTOID}; Datum args[1]={CStringGetTextDatum(name)};
        bool is_null;
        bool critical;
        SPI_execute_with_args("WITH RECURSIVE required(oid) AS ("
            "SELECT oid FROM pg_catalog.pg_extension WHERE extname OPERATOR(pg_catalog.=) 'lake_replication_capture' UNION "
            "SELECT d.refobjid FROM pg_catalog.pg_depend d JOIN required r ON d.objid OPERATOR(pg_catalog.=) r.oid "
            "WHERE d.classid OPERATOR(pg_catalog.=) 'pg_catalog.pg_extension'::pg_catalog.regclass "
            "AND d.refclassid OPERATOR(pg_catalog.=) 'pg_catalog.pg_extension'::pg_catalog.regclass) "
            "SELECT EXISTS(SELECT FROM required r JOIN pg_catalog.pg_extension e ON r.oid OPERATOR(pg_catalog.=) e.oid "
            "WHERE e.extname OPERATOR(pg_catalog.=) $1)",1,types,args,NULL,true,1);
        critical=DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],SPI_tuptable->tupdesc,1,&is_null));
        if (critical) ereport(ERROR,(errmsg("drop capture streams and wait for owned slot cleanup before dropping capture dependencies")));
    }
    PopActiveSnapshot();
    SPI_finish();
    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(lake_capture_launcher);
Datum
lake_capture_launcher(PG_FUNCTION_ARGS)
{
    pid_t child = 0;
    int64 child_generation = 0;
    int64 failed_generation = 0;
    int lease_retries = 0;
    MemoryContext cycle = AllocSetContextCreate(TopMemoryContext, "capture manager cycle", ALLOCSET_SMALL_SIZES);
    pqsignal(SIGTERM, capture_sigterm);
    BackgroundWorkerUnblockSignals();
    while (!capture_shutdown && !TerminationRequested)
    {
        char *stream = NULL, *generation = NULL, *owner = NULL, *credentials = NULL, *database = NULL;
        char *state = NULL;
        bool origin_matches = false;
        MemoryContext previous;
        int status;
        if (child > 0 && waitpid(child, &status, WNOHANG) == child)
        {
            if (WIFEXITED(status) && WEXITSTATUS(status) != 0 &&
                (WEXITSTATUS(status) != 75 || ++lease_retries >= 10))
            {
                failed_generation=child_generation;
                ereport(LOG,(errmsg("capture child failed; explicit start/config generation change required before retry")));
            }
            child = 0;
            LightSleep(1000);
        }
        MemoryContextReset(cycle);
        previous = MemoryContextSwitchTo(cycle);
        begin_sql();
        SPI_execute("UPDATE lake_capture.streams SET desired_state='stopped',config_generation=config_generation+1,"
                    "last_error='postmaster changed; explicitly verify and restart or rebuild the capture stream' "
                    "WHERE desired_state<>'stopped' AND (bound_postmaster IS DISTINCT FROM pg_postmaster_start_time() "
                    "OR (desired_state='running' AND (origin_system_identifier<>(SELECT system_identifier FROM pg_control_system()) "
                    "OR origin_database_oid<>(SELECT oid FROM pg_database WHERE datname=current_database()))))", false, 0);
        SPI_execute("SELECT stream_id::text,config_generation::text,pg_get_userbyid(owner),credential_file,"
                    "current_database(),desired_state,(origin_system_identifier=(SELECT system_identifier FROM pg_control_system()) "
                    "AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()))::text "
                    "FROM lake_capture.streams", true, 1);
        if (SPI_processed == 1)
        {
            char *value;
            MemoryContextSwitchTo(cycle);
            stream = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
            generation = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
            owner = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3));
            value = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);
            credentials = value ? pstrdup(value) : NULL;
            database = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5));
            state = pstrdup(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6));
            origin_matches = strcmp(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 7), "true") == 0;
        }
        end_sql();
        if (child > 0 && (!stream || strcmp(state,"running") != 0 || child_generation != pg_strtoint64(generation)))
        { stop_child(child); child = 0; }
        if (stream && (strcmp(state,"drop") == 0 || strcmp(state,"reset") == 0))
        {
            begin_sql();
            SPI_execute(psprintf("SELECT 1 FROM lake_capture.streams WHERE stream_id=%s::uuid "
                        "AND config_generation=%s AND desired_state=%s FOR UPDATE",
                        quote_literal_cstr(stream),generation,quote_literal_cstr(state)),false,1);
            if (SPI_processed != 1)
            {
                end_sql();
                MemoryContextSwitchTo(previous);
                LightSleep(100);
                continue;
            }
            SPI_execute(psprintf("SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))::text",
                        quote_literal_cstr(psprintf("lake_capture/%s", stream))), true, 1);
            if (strcmp(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), "true") != 0)
            {
                end_sql();
                MemoryContextSwitchTo(previous);
                LightSleep(100);
                continue;
            }
            SPI_execute("SELECT (log_locations IS NOT DISTINCT FROM lake_capture.lock_private_logs(false))::text "
                        "FROM lake_capture.streams", true, 1);
            if (origin_matches && (SPI_processed != 1 ||
                strcmp(SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), "true") != 0))
            {
                SPI_execute("UPDATE lake_capture.streams SET desired_state='stopped',config_generation=config_generation+1,"
                            "last_error='capture private log locations changed; repair before reset or drop'", false, 0);
                end_sql();
                MemoryContextSwitchTo(previous);
                LightSleep(100);
                continue;
            }
            SPI_execute("SELECT 1 FROM pg_replication_slots r JOIN lake_capture.streams s ON r.slot_name=s.slot_name "
                        "WHERE r.active OR r.database<>current_database() OR r.plugin<>'lake_replication_capture'",true,1);
            if (origin_matches && SPI_processed != 0)
            {
                end_sql();
                MemoryContextSwitchTo(previous);
                LightSleep(100);
                continue;
            }
            if (origin_matches)
                SPI_execute("SELECT pg_drop_replication_slot(r.slot_name) FROM pg_replication_slots r JOIN lake_capture.streams s "
                            "ON r.slot_name=s.slot_name WHERE r.database=current_database() AND r.plugin='lake_replication_capture' AND NOT r.active", false, 0);
            SPI_execute("DELETE FROM lake_capture_log.events_a; DELETE FROM lake_capture_log.events_b; "
                        "DELETE FROM lake_capture_log.controls; DELETE FROM lake_capture_log.manifests; DELETE FROM lake_capture.receipts; "
                        "DELETE FROM lake_capture.upload_attempts; DELETE FROM lake_capture.progress_requests", false, 0);
            if (strcmp(state,"drop") == 0)
                SPI_execute("DELETE FROM lake_capture.streams",false,0);
            else
            {
                SPI_execute("UPDATE lake_capture.schema_definitions SET capture_generation=capture_generation+1",false,0);
                SPI_execute("UPDATE lake_capture.streams SET desired_state='stopped',capture_generation=capture_generation+1,"
                            "slot_name='lc_'||replace(stream_id::text,'-','')||'_g'||(capture_generation+1)::text,"
                            "baseline_lsn=NULL,snapshot_state='none',captured_sequence=0,captured_commit_lsn='0/0',captured_end_lsn='0/0',bound_postmaster=NULL,"
                            "retained_log_bytes=0,generation_log_bytes=0,pruned_through_sequence=0,"
                            "reserved_upload_bytes=0,upload_attempt_count=0,maintenance_attempt_count=0,progress_request_count=0,"
                            "last_error='reset complete; empty-table or coordinated snapshot activation required'",false,0);
            }
            end_sql();
            /* A new stream may reuse config generation 1 after drop. Fatal
             * retry fencing belongs to the removed stream, not that number. */
            failed_generation = 0;
            lease_retries = 0;
        }
        else if (stream && strcmp(state,"running") == 0 && child == 0 && failed_generation != pg_strtoint64(generation))
        {
            char **environment = palloc0(13 * sizeof(char *));
            int count = 0;
            pid_t parent = getpid();
            char *argv[] = {"/usr/local/bin/lake-replication-capture", NULL};
            environment[count++] = "PATH=/usr/local/bin:/usr/bin:/bin";
            environment[count++] = "LANG=C.UTF-8";
            environment[count++] = psprintf("LAKE_CAPTURE_STREAM=%s",stream);
            environment[count++] = psprintf("LAKE_CONFIG_GENERATION=%s",generation);
            environment[count++] = psprintf("LAKE_SOURCE_USER=%s",owner);
            environment[count++] = psprintf("LAKE_SOURCE_DATABASE=%s",database);
            environment[count++] = psprintf("LAKE_SOURCE_ADDRESS=127.0.0.1:%s",GetConfigOption("port",false,false));
            environment[count++] = "LAKE_SOURCE_TLS_MODE=disable";
            if (credentials) environment[count++] = psprintf("LAKE_CREDENTIAL_FILE=%s",credentials);
            if (child_generation != pg_strtoint64(generation)) lease_retries=0;
            child_generation = pg_strtoint64(generation);
            child = fork();
            if (child == 0)
            {
                int fd;
#ifdef __linux__
                prctl(PR_SET_PDEATHSIG,SIGKILL);
                if (getppid() != parent) _exit(1);
#ifdef SYS_close_range
                if (syscall(SYS_close_range,3,~0U,0) != 0)
#endif
#endif
                    for (fd=3;fd<65536;fd++) close(fd);
                execve(argv[0],argv,environment);
                _exit(127);
            }
            if (child < 0) ereport(ERROR,(errmsg("cannot start capture child")));
        }
        MemoryContextSwitchTo(previous);
        LightSleep(100);
        CHECK_FOR_INTERRUPTS();
    }
    stop_child(child);
    MemoryContextDelete(cycle);
    PG_RETURN_INT64(1000);
}
