/* Source frozen-schema prototype. All row decoding uses PostgreSQL's historic catalogs
 * and logical output-plugin callbacks. No SPI or object I/O in callbacks. */
#include "postgres.h"
#include <signal.h>
#include <unistd.h>
#include "fmgr.h"
#include "access/htup_details.h"
#include "access/detoast.h"
#include "access/sysattr.h"
#include "catalog/pg_class.h"
#include "utils/relcache.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/reorderbuffer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/uuid.h"
#include "utils/pgstat_internal.h"

PG_MODULE_MAGIC;

#define CAPTURE_MAX_EVENT (8 * 1024 * 1024)
/* Leave framing room for up to 32 manifests with bounded version histories. */
#define CAPTURE_MAX_BATCH (64 * 1024 * 1024 - 128 * 1024)
#define CAPTURE_MAX_EVENTS 100000

typedef struct CaptureDecode
{
    Oid tables[2];
    pg_uuid_t ids[2];
    Oid progress_table;
    Oid schema_table;
    uint32 versions[2];
    pg_uuid_t stream_id;
    int64 capture_generation;
    StringInfoData events;
    uint32 count;
    uint32 counts[2];
    uint64 read_bytes;
    uint64 read_events;
    int64 read_event_limit;
    int64 max_spill_bytes;
    bool test_spill_stop;
    bool check_spill;
    int64 initial_spill_bytes;
    TimestampTz initial_stats_reset;
} CaptureDecode;

static bool capture_filter_origin(LogicalDecodingContext *, RepOriginId);
static void capture_startup(LogicalDecodingContext *, OutputPluginOptions *, bool);
static void capture_begin(LogicalDecodingContext *, ReorderBufferTXN *);
static void capture_change(LogicalDecodingContext *, ReorderBufferTXN *, Relation, ReorderBufferChange *);
static void capture_commit(LogicalDecodingContext *, ReorderBufferTXN *, XLogRecPtr);
static void capture_truncate(LogicalDecodingContext *, ReorderBufferTXN *, int, Relation[], ReorderBufferChange *);

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    cb->startup_cb = capture_startup;
    cb->filter_by_origin_cb = capture_filter_origin;
    cb->begin_cb = capture_begin;
    cb->change_cb = capture_change;
    cb->commit_cb = capture_commit;
    cb->truncate_cb = capture_truncate;
}

/* ReorderBuffer resets its private spillBytes immediately after reporting
 * each spill. Read the slot's live shared-memory counter instead of a cached
 * SQL statistics snapshot; this performs no SQL or filesystem I/O. */
static int64
capture_spill_counter(LogicalDecodingContext *ctx, TimestampTz *reset)
{
    PgStat_EntryRef *entry = pgstat_get_entry_ref_locked(PGSTAT_KIND_REPLSLOT,
        InvalidOid, ReplicationSlotIndex(ctx->slot), false);
    PgStatShared_ReplSlot *shared;
    int64 bytes;
    if (entry == NULL) ereport(ERROR, (errmsg("capture native spill statistics unavailable")));
    shared = (PgStatShared_ReplSlot *) entry->shared_stats;
    bytes = shared->stats.spill_bytes;
    *reset = shared->stats.stat_reset_timestamp;
    pgstat_unlock_entry(entry);
    return bytes;
}

static void
capture_startup(LogicalDecodingContext *ctx, OutputPluginOptions *options, bool is_init)
{
    CaptureDecode *state;
    MemoryContext previous = MemoryContextSwitchTo(ctx->context);
    ListCell *cell;
    state = palloc0(sizeof(CaptureDecode));
    initStringInfo(&state->events);
    state->max_spill_bytes = 256 * 1024 * 1024;
    state->read_event_limit = CAPTURE_MAX_EVENTS;
    state->versions[0] = state->versions[1] = 1;
    ctx->output_plugin_private = state;
    options->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
    options->receive_rewrites = false;
    state->check_spill = !is_init;
    if (state->check_spill)
        state->initial_spill_bytes = capture_spill_counter(ctx, &state->initial_stats_reset);
    foreach(cell, ctx->output_plugin_options)
    {
        DefElem *option = lfirst(cell);
        char *value = defGetString(option);
        if (strcmp(option->defname, "table-a-oid") == 0)
            state->tables[0] = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(value)));
        else if (strcmp(option->defname, "table-b-oid") == 0)
            state->tables[1] = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(value)));
        else if (strcmp(option->defname, "table-a-id") == 0)
            memcpy(&state->ids[0], DatumGetUUIDP(DirectFunctionCall1(uuid_in, CStringGetDatum(value))), sizeof(pg_uuid_t));
        else if (strcmp(option->defname, "table-b-id") == 0)
            memcpy(&state->ids[1], DatumGetUUIDP(DirectFunctionCall1(uuid_in, CStringGetDatum(value))), sizeof(pg_uuid_t));
        else if (strcmp(option->defname, "table-a-version") == 0 || strcmp(option->defname, "table-b-version") == 0)
        {
            int index = strcmp(option->defname,"table-a-version") == 0 ? 0 : 1;
            int32 version = pg_strtoint32(value);
            if (version < 1 || version > 33) ereport(ERROR,(errmsg("invalid initial capture schema version")));
            state->versions[index] = version;
        }
        else if (strcmp(option->defname, "schema-table-oid") == 0)
            state->schema_table = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(value)));
        else if (strcmp(option->defname, "progress-table-oid") == 0)
            state->progress_table = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(value)));
        else if (strcmp(option->defname, "stream-id") == 0)
            memcpy(&state->stream_id, DatumGetUUIDP(DirectFunctionCall1(uuid_in, CStringGetDatum(value))), sizeof(pg_uuid_t));
        else if (strcmp(option->defname, "capture-generation") == 0)
            state->capture_generation = pg_strtoint64(value);
        else if (strcmp(option->defname, "test-spill-stop") == 0)
            state->test_spill_stop = strcmp(value,"1") == 0;
        else if (strcmp(option->defname, "max-spill-bytes") == 0)
        {
            state->max_spill_bytes = pg_strtoint64(value);
            if (state->max_spill_bytes < 65536 || state->max_spill_bytes > 1073741824)
                ereport(ERROR, (errmsg("invalid capture decode spill budget")));
        }
        else if (strcmp(option->defname, "max-read-events") == 0)
        {
            state->read_event_limit = pg_strtoint64(value);
            if (state->read_event_limit < 1 || state->read_event_limit > CAPTURE_MAX_EVENTS)
                ereport(ERROR, (errmsg("invalid capture read event budget")));
        }
        else
            ereport(ERROR, (errmsg("unknown capture decoding option")));
    }
    if (!is_init && (!OidIsValid(state->tables[0]) || !OidIsValid(state->tables[1]) ||
                    state->tables[0] == state->tables[1]))
        ereport(ERROR, (errmsg("capture requires two distinct table registrations")));
    if (OidIsValid(state->progress_table))
    {
        pg_uuid_t zero = {{0}};
        if (state->progress_table == state->tables[0] || state->progress_table == state->tables[1] ||
            state->capture_generation <= 0 || memcmp(&state->stream_id,&zero,sizeof(pg_uuid_t)) == 0)
            ereport(ERROR, (errmsg("invalid capture progress registration")));
    }
    if (OidIsValid(state->schema_table))
    {
        pg_uuid_t zero = {{0}};
        if (state->schema_table == state->tables[0] || state->schema_table == state->tables[1] ||
            state->schema_table == state->progress_table || state->capture_generation <= 0 ||
            memcmp(&state->stream_id,&zero,sizeof(pg_uuid_t)) == 0)
            ereport(ERROR,(errmsg("invalid capture schema registration")));
    }
    MemoryContextSwitchTo(previous);
}

/* Check before queued WAL changes, including unfinished and unselected
 * transactions. A statistics reset invalidates the read's budget measurement
 * and therefore stops the read instead of silently resetting its allowance. */
static bool
capture_filter_origin(LogicalDecodingContext *ctx, RepOriginId origin)
{
    CaptureDecode *state = ctx->output_plugin_private;
    TimestampTz reset;
    int64 bytes;
    CHECK_FOR_INTERRUPTS();
    if (!state->check_spill) return false;
    bytes = capture_spill_counter(ctx, &reset);
    if (reset != state->initial_stats_reset || bytes < state->initial_spill_bytes)
        ereport(ERROR, (errmsg("capture native spill statistics reset during decoding; restart explicitly")));
    bytes -= state->initial_spill_bytes;
    if (state->test_spill_stop && bytes > 0)
    {
        state->test_spill_stop = false;
        raise(SIGSTOP); /* Prototype harness: kill this actual backend mid-spill. */
    }
    if (bytes > state->max_spill_bytes)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("capture decode spill budget exceeded before commit (" INT64_FORMAT " bytes)",bytes)));
    return false;
}

static void
capture_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    CaptureDecode *state = ctx->output_plugin_private;
    resetStringInfo(&state->events);
    state->count = state->counts[0] = state->counts[1] = 0;
}

static int
selected_table(CaptureDecode *state, Oid relation_id)
{
    if (relation_id == state->tables[0]) return 0;
    if (relation_id == state->tables[1]) return 1;
    return -1;
}

/* A key tuple contains only identity columns. Unchanged external TOAST is
 * encoded without dereferencing its historical on-disk pointer, as pgoutput
 * does; the publisher retains the previous value. */
static void
capture_row(StringInfo event, Relation relation, HeapTuple tuple, bool key, bool update)
{
    TupleDesc descriptor = RelationGetDescr(relation);
    Bitmapset *identity = key ? RelationGetIdentityKeyBitmap(relation) : NULL;
    int index, count = 0;
    if (descriptor->natts > 1600 || tuple == NULL)
        ereport(ERROR, (errmsg("capture requires a bounded available row")));
    for (index = 0; index < descriptor->natts; index++)
        if (!key || bms_is_member(index + 1 - FirstLowInvalidHeapAttributeNumber, identity)) count++;
    if (count == 0) ereport(ERROR, (errmsg("capture identity has no columns")));
    pq_sendint32(event, count);
    for (index = 0; index < descriptor->natts; index++)
    {
        Form_pg_attribute attribute = TupleDescAttr(descriptor, index);
        bool is_null, variable;
        Datum value;
        Oid output;
        char *text;
        Size length;
        if (key && !bms_is_member(index + 1 - FirstLowInvalidHeapAttributeNumber, identity)) continue;
        if (attribute->attisdropped || attribute->attgenerated ||
            (attribute->atttypid != BOOLOID && attribute->atttypid != INT8OID &&
             attribute->atttypid != INT4OID && attribute->atttypid != TEXTOID))
            ereport(ERROR, (errmsg("capture requires the registered frozen scalar schema")));
        value = heap_getattr(tuple, index + 1, descriptor, &is_null);
        pq_sendint32(event, index + 1);
        if (is_null) { pq_sendbyte(event, 1); continue; }
        if (attribute->attlen == -1 && VARATT_IS_EXTERNAL_ONDISK(value))
        {
            if (!update || key) ereport(ERROR, (errmsg("capture requires a materialized identity/INSERT value")));
            pq_sendbyte(event, 2);
            continue;
        }
        if (attribute->atttypid == TEXTOID && toast_raw_datum_size(value) > CAPTURE_MAX_EVENT)
            ereport(ERROR, (errmsg("capture event byte budget exceeded before text materialization")));
        getTypeOutputInfo(attribute->atttypid, &output, &variable);
        text = OidOutputFunctionCall(output, value);
        length = strlen(text);
        if (length > CAPTURE_MAX_EVENT || event->len + length + 5 > CAPTURE_MAX_EVENT)
            ereport(ERROR, (errmsg("capture event byte budget exceeded")));
        pq_sendbyte(event, 3); pq_sendint32(event, length); pq_sendbytes(event, text, length);
        pfree(text);
    }
    bms_free(identity);
}

static void
capture_event_begin(CaptureDecode *state, int selected, StringInfo event, int tag)
{
    if (++state->counts[selected] > CAPTURE_MAX_EVENTS || state->count >= CAPTURE_MAX_EVENTS)
        ereport(ERROR, (errmsg("capture transaction event budget exceeded")));
    initStringInfo(event);
    pq_sendint32(event, state->count++);
    pq_sendbyte(event, 1);
    pq_sendbytes(event, (char *) state->ids[selected].data, UUID_LEN);
    pq_sendint32(event, state->versions[selected]);
    pq_sendbyte(event, tag);
}

static void
capture_event_end(CaptureDecode *state, StringInfo event)
{
    if (event->len > CAPTURE_MAX_EVENT || state->events.len + event->len + 4 > CAPTURE_MAX_BATCH)
        ereport(ERROR, (errmsg("capture transaction byte budget exceeded")));
    pq_sendint32(&state->events, event->len);
    pq_sendbytes(&state->events, event->data, event->len);
    pfree(event->data);
}

static void
capture_progress(CaptureDecode *state, Relation relation, ReorderBufferChange *change)
{
    TupleDesc descriptor = RelationGetDescr(relation);
    HeapTuple tuple;
    Datum stream, generation, request;
    bool is_null;
    StringInfoData event;
    pg_uuid_t zero = {{0}};
    if (change->action != REORDER_BUFFER_CHANGE_INSERT) return;
    if (descriptor->natts != 6 || TupleDescAttr(descriptor,0)->atttypid != UUIDOID ||
        TupleDescAttr(descriptor,1)->atttypid != INT8OID || TupleDescAttr(descriptor,2)->atttypid != UUIDOID)
        ereport(ERROR, (errmsg("invalid capture progress tuple descriptor")));
    tuple = change->data.tp.newtuple;
    if (tuple == NULL) ereport(ERROR, (errmsg("missing capture progress tuple")));
    stream = heap_getattr(tuple,1,descriptor,&is_null);
    if (is_null) ereport(ERROR, (errmsg("missing capture progress stream")));
    if (memcmp(DatumGetUUIDP(stream),&state->stream_id,sizeof(pg_uuid_t)) != 0) return;
    generation = heap_getattr(tuple,2,descriptor,&is_null);
    if (is_null) ereport(ERROR, (errmsg("missing capture progress generation")));
    if (DatumGetInt64(generation) != state->capture_generation) return;
    request = heap_getattr(tuple,3,descriptor,&is_null);
    if (is_null || memcmp(DatumGetUUIDP(request),&zero,sizeof(pg_uuid_t)) == 0)
        ereport(ERROR, (errmsg("invalid capture progress request")));
    if (state->count >= CAPTURE_MAX_EVENTS)
        ereport(ERROR, (errmsg("capture transaction event budget exceeded")));
    initStringInfo(&event);
    pq_sendint32(&event,state->count++);
    pq_sendbyte(&event,0); /* No table; v1 Progress control tag 9. */
    pq_sendbyte(&event,9);
    pq_sendint32(&event,UUID_LEN);
    pq_sendbytes(&event,(char *) DatumGetUUIDP(request)->data,UUID_LEN);
    capture_event_end(state,&event);
}

/* Audit tuples carry immutable boundary-time payloads. No catalog lookup,
 * SPI or object I/O is allowed here. Worker setup must derive initial versions
 * from retained history at the slot's confirmed boundary, never latest heads. */
static uint32
schema_u32(const unsigned char *bytes)
{
    return ((uint32) bytes[0] << 24) | ((uint32) bytes[1] << 16) |
           ((uint32) bytes[2] << 8) | (uint32) bytes[3];
}

static void
capture_schema(CaptureDecode *state, Relation relation, ReorderBufferChange *change)
{
    TupleDesc descriptor = RelationGetDescr(relation);
    HeapTuple tuple;
    Datum value;
    pg_uuid_t table_id;
    uint32 before, after, before_length, after_length;
    Size length, offset;
    bytea *payload;
    const unsigned char *bytes;
    bool is_null;
    int selected;
    StringInfoData event;
    Oid types[6] = {UUIDOID,INT8OID,UUIDOID,INT4OID,INT4OID,BYTEAOID};
    int index;
    if (change->action != REORDER_BUFFER_CHANGE_INSERT) return;
    tuple = change->data.tp.newtuple;
    if (descriptor->natts != 6 || tuple == NULL)
        ereport(ERROR,(errmsg("invalid capture schema audit tuple")));
    for (index=0;index<6;index++)
        if (TupleDescAttr(descriptor,index)->atttypid != types[index] || TupleDescAttr(descriptor,index)->attisdropped)
            ereport(ERROR,(errmsg("invalid capture schema audit descriptor")));
    value = heap_getattr(tuple,1,descriptor,&is_null);
    if (is_null) ereport(ERROR,(errmsg("missing schema audit stream")));
    if (memcmp(DatumGetUUIDP(value),&state->stream_id,sizeof(pg_uuid_t)) != 0) return;
    value = heap_getattr(tuple,2,descriptor,&is_null);
    if (is_null) ereport(ERROR,(errmsg("missing schema audit generation")));
    if (DatumGetInt64(value) != state->capture_generation) return;
    value = heap_getattr(tuple,3,descriptor,&is_null);
    if (is_null) ereport(ERROR,(errmsg("missing schema audit table")));
    memcpy(&table_id,DatumGetUUIDP(value),sizeof(pg_uuid_t));
    selected = memcmp(&table_id,&state->ids[0],sizeof(pg_uuid_t)) == 0 ? 0 :
               memcmp(&table_id,&state->ids[1],sizeof(pg_uuid_t)) == 0 ? 1 : -1;
    if (selected < 0) ereport(ERROR,(errmsg("unregistered schema audit table")));
    value = heap_getattr(tuple,4,descriptor,&is_null);
    if (is_null) ereport(ERROR,(errmsg("missing schema audit before version")));
    before = DatumGetInt32(value);
    value = heap_getattr(tuple,5,descriptor,&is_null);
    if (is_null) ereport(ERROR,(errmsg("missing schema audit after version")));
    after = DatumGetInt32(value);
    if (before != state->versions[selected] || before < 1 || after != before+1 || after > 33)
        ereport(ERROR,(errmsg("schema audit version does not follow retained history")));
    value = heap_getattr(tuple,6,descriptor,&is_null);
    if (is_null) ereport(ERROR,(errmsg("missing schema audit payload")));
    if (toast_raw_datum_size(value) > 2*1024*1024+64+VARHDRSZ)
        ereport(ERROR,(errmsg("schema audit payload byte budget exceeded")));
    payload = DatumGetByteaPP(value);
    bytes = (const unsigned char *) VARDATA_ANY(payload);
    length = VARSIZE_ANY_EXHDR(payload);
    if (length < 39 || length > 2*1024*1024+64 || memcmp(bytes,"LKDDL1",6) != 0 ||
        memcmp(bytes+6,table_id.data,UUID_LEN) != 0 || schema_u32(bytes+22) != before ||
        schema_u32(bytes+26) != after || bytes[30] > 1)
        ereport(ERROR,(errmsg("invalid bounded schema audit payload")));
    before_length = schema_u32(bytes+31);
    if (before_length < 6 || before_length > 1024*1024 || before_length > length-39)
        ereport(ERROR,(errmsg("invalid schema audit before definition length")));
    offset = 35 + before_length;
    after_length = schema_u32(bytes+offset);
    if (after_length < 6 || after_length > 1024*1024 || after_length != length-offset-4 ||
        memcmp(bytes+35,"LKDSC1",6) != 0 || memcmp(bytes+offset+4,"LKDSC1",6) != 0)
        ereport(ERROR,(errmsg("invalid schema audit after definition length")));
    if (state->count >= CAPTURE_MAX_EVENTS)
        ereport(ERROR,(errmsg("capture transaction event budget exceeded")));
    initStringInfo(&event);
    pq_sendint32(&event,state->count++);
    pq_sendbyte(&event,0);
    pq_sendbyte(&event,8); /* Schema control. */
    pq_sendint32(&event,length);
    pq_sendbytes(&event,(const char *) bytes,length);
    capture_event_end(state,&event);
    state->versions[selected] = after;
    if ((Pointer)payload != DatumGetPointer(value)) pfree(payload);
}

static void
capture_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
               Relation relation, ReorderBufferChange *change)
{
    CaptureDecode *state = ctx->output_plugin_private;
    int selected = selected_table(state, RelationGetRelid(relation));
    StringInfoData event;
    if (RelationGetRelid(relation) == state->schema_table)
    {
        capture_schema(state,relation,change);
        return;
    }
    if (RelationGetRelid(relation) == state->progress_table)
    {
        capture_progress(state,relation,change);
        return;
    }
    if (selected < 0) return;
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
            capture_event_begin(state, selected, &event, 0);
            capture_row(&event, relation, change->data.tp.newtuple, false, false);
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            capture_event_begin(state, selected, &event, 1);
            pq_sendbyte(&event, change->data.tp.oldtuple ? 1 : 0);
            if (change->data.tp.oldtuple)
                capture_row(&event, relation, change->data.tp.oldtuple, true, false);
            capture_row(&event, relation, change->data.tp.newtuple, false, true);
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            capture_event_begin(state, selected, &event, 2);
            pq_sendbyte(&event, 1);
            capture_row(&event, relation, change->data.tp.oldtuple, true, false);
            break;
        default:
            ereport(ERROR, (errmsg("unsupported capture row action")));
    }
    capture_event_end(state, &event);
}

static void
capture_truncate(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, int count,
                 Relation relations[], ReorderBufferChange *change)
{
    CaptureDecode *state = ctx->output_plugin_private;
    int index;
    for (index = 0; index < count; index++)
    {
        int selected = selected_table(state, RelationGetRelid(relations[index]));
        StringInfoData event;
        if (selected < 0) continue;
        capture_event_begin(state, selected, &event, 3);
        pq_sendbyte(&event, (change->data.truncate.cascade ? 1 : 0) |
                           (change->data.truncate.restart_seqs ? 2 : 0));
        capture_event_end(state, &event);
    }
}

static void
capture_commit(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
    CaptureDecode *state = ctx->output_plugin_private;
    /* Bound the entire SQL read, not only each constituent transaction. The
     * driver retries a smaller complete-transaction window on this diagnostic. */
    if (state->read_bytes + state->events.len + 58 > CAPTURE_MAX_BATCH ||
        state->read_events + state->count > state->read_event_limit)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            errmsg("capture read window budget exceeded")));
    state->read_bytes += state->events.len + 58;
    state->read_events += state->count;
    OutputPluginPrepareWrite(ctx, true);
    pq_sendbytes(ctx->out, "LKCAP1", 6);
    pq_sendint32(ctx->out, txn->xid);
    pq_sendint64(ctx->out, txn->first_lsn);
    pq_sendint64(ctx->out, commit_lsn);
    pq_sendint64(ctx->out, txn->end_lsn);
    pq_sendint64(ctx->out, txn->xact_time.commit_time);
    pq_sendint32(ctx->out, state->count);
    pq_sendint32(ctx->out, state->counts[0]); pq_sendint32(ctx->out, state->counts[1]);
    pq_sendbytes(ctx->out, state->events.data, state->events.len);
    OutputPluginWrite(ctx, true);
}
