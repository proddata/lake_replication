/*
 * Experiment only: intercepts CREATE/ALTER/DROP SUBSCRIPTION statements
 * that target this project's own Iceberg replication control plane
 * (lake_replication.*) via ProcessUtility_hook, instead of letting
 * PostgreSQL's real subscription/slot/apply-worker machinery run at all.
 *
 * - CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg') is claimed by
 *   this hook. The source system identity is discovered for real, via
 *   dblink against the statement's own CONNECTION string, then handed to
 *   lake_replication.create_subscription().
 * - ALTER/DROP SUBSCRIPTION are claimed only when `subname` matches a
 *   subscription this hook already created (looked up by name in
 *   lake_replication.subscriptions); anything else falls through
 *   unchanged, so ordinary native subscriptions keep working.
 *
 * Any statement not claimed by the rules above falls through to
 * standard_ProcessUtility (or the next hook) unmodified.
 *
 * Known limitation: ALTER SUBSCRIPTION ... ENABLE cannot actually start
 * anything -- there is no running process to hand off to here. It only
 * clears the desired_state; an operator still has to run
 * lake-replication-supervisor pointed at the subscription id. DISABLE maps
 * onto lake_replication.request_service_stop(), which a running supervisor
 * observes and exits on. Every other ALTER kind is accepted and logged as
 * a no-op rather than erroring, since there is no real pg_subscription row
 * for core to act on.
 */
#include "postgres.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"

PG_MODULE_MAGIC;

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static char *
get_string_option(List *options, const char *name)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}
	return NULL;
}

static bool
get_bool_option(List *options, const char *name, bool defaultValue)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, name) == 0)
			return defGetBoolean(def);
	}
	return defaultValue;
}

/*
 * lake_subscription_id looks up a subscription this hook previously
 * created, by name. Returns the id, or -1 if there is no such row (meaning
 * the statement belongs to a real, unrelated subscription and must fall
 * through untouched). Must be called with SPI already connected.
 */
static int64
lake_subscription_id(const char *subname)
{
	StringInfoData buf;
	int64		result = -1;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT subscription_id FROM lake_replication.subscriptions "
					 "WHERE subscription_name = %s", quote_literal_cstr(subname));
	if (SPI_execute(buf.data, true, 0) == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		value = SPI_getbinval(SPI_tuptable->vals[0],
											  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			result = DatumGetInt64(value);
	}
	return result;
}

/*
 * discover_source_identity connects to `conninfo` via dblink and reads the
 * real system_identifier/database_oid/timeline of that source, the same
 * three values every other probe in this project fetches from
 * pg_control_system()/pg_control_checkpoint(). Must be called with SPI
 * already connected. Raises an error if the source is unreachable or the
 * dblink extension cannot be created.
 */
static void
discover_source_identity(const char *conninfo, char **outSystemIdentifier,
						 char **outDatabaseOid, char **outTimeline)
{
	StringInfoData buf;

	if (SPI_execute("CREATE EXTENSION IF NOT EXISTS dblink", false, 0) < 0)
		elog(ERROR, "lake_subscription_sugar: could not create the dblink extension");

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT identity.system_identifier, identity.database_oid, "
					 "identity.timeline FROM dblink(%s, "
					 "'SELECT system_identifier, "
					 "(SELECT oid FROM pg_database WHERE datname = current_database()), "
					 "(SELECT timeline_id FROM pg_control_checkpoint()) "
					 "FROM pg_control_system()') "
					 "AS identity(system_identifier numeric, database_oid oid, "
					 "timeline bigint)", quote_literal_cstr(conninfo));

	if (SPI_execute(buf.data, true, 0) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR,
			 "lake_subscription_sugar: could not discover the source identity "
			 "via dblink for conninfo %s", conninfo);

	*outSystemIdentifier = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	*outDatabaseOid = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	*outTimeline = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
}

static void
handle_create_subscription(CreateSubscriptionStmt *stmt)
{
	StringInfoData buf;
	char	   *pubname;
	char	   *systemIdentifier;
	char	   *databaseOid;
	char	   *timeline;

	pubname = strVal(linitial(stmt->publication));

	SPI_connect();

	discover_source_identity(stmt->conninfo, &systemIdentifier, &databaseOid, &timeline);

	ereport(NOTICE,
			(errmsg("lake_subscription_sugar: intercepted CREATE SUBSCRIPTION \"%s\" "
					"(conninfo=%s, publication=%s, source system_identifier=%s "
					"database_oid=%s timeline=%s); routing to "
					"lake_replication.create_subscription instead of native apply",
					stmt->subname, stmt->conninfo, pubname,
					systemIdentifier, databaseOid, timeline)));

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT lake_replication.create_subscription("
					 "%s::numeric, %s::oid, %s::bigint, "
					 "%s::name, %s::name, ARRAY[%s]::text[])",
					 systemIdentifier, databaseOid, timeline,
					 quote_literal_cstr(stmt->subname),
					 quote_literal_cstr(psprintf("%s_slot", stmt->subname)),
					 quote_literal_cstr(pubname));
	if (SPI_execute(buf.data, false, 0) != SPI_OK_SELECT)
		elog(ERROR, "lake_subscription_sugar: create_subscription call failed");

	SPI_finish();
}

static void
handle_drop_subscription(DropSubscriptionStmt *stmt, int64 subscriptionId)
{
	StringInfoData buf;

	ereport(NOTICE,
			(errmsg("lake_subscription_sugar: intercepted DROP SUBSCRIPTION \"%s\"; "
					"tearing down lake_replication control-plane state instead of "
					"native apply", stmt->subname)));

	SPI_connect();
	SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
						  (long long) subscriptionId), false, 0);

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "DELETE FROM lake_replication.changes WHERE transaction_id IN "
					 "  (SELECT transaction_id FROM lake_replication.transactions "
					 "    WHERE subscription_id = %lld);"
					 "DELETE FROM lake_replication.transactions WHERE subscription_id = %lld;"
					 "DELETE FROM lake_replication.batches WHERE subscription_id = %lld;"
					 "DELETE FROM lake_replication.initial_copies WHERE subscription_id = %lld;"
					 "DELETE FROM lake_replication.relations WHERE subscription_id = %lld;"
					 "DELETE FROM lake_replication.checkpoints WHERE subscription_id = %lld;"
					 "DELETE FROM lake_replication.subscriptions WHERE subscription_id = %lld",
					 (long long) subscriptionId, (long long) subscriptionId,
					 (long long) subscriptionId, (long long) subscriptionId,
					 (long long) subscriptionId, (long long) subscriptionId,
					 (long long) subscriptionId);
	if (SPI_execute(buf.data, false, 0) < 0)
		elog(ERROR, "lake_subscription_sugar: failed to tear down subscription state");

	SPI_finish();
}

static void
handle_alter_subscription(AlterSubscriptionStmt *stmt, int64 subscriptionId)
{
	SPI_connect();

	if (stmt->kind == ALTER_SUBSCRIPTION_ENABLED)
	{
		bool		enabled = get_bool_option(stmt->options, "enabled", true);

		if (!enabled)
		{
			ereport(NOTICE,
					(errmsg("lake_subscription_sugar: intercepted ALTER SUBSCRIPTION "
							"\"%s\" DISABLE; requesting a graceful stop", stmt->subname)));
			SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
								  (long long) subscriptionId), false, 0);
		}
		else
		{
			ereport(NOTICE,
					(errmsg("lake_subscription_sugar: ALTER SUBSCRIPTION \"%s\" ENABLE "
							"recorded, but nothing runs the replication until an "
							"operator starts lake-replication-supervisor for "
							"subscription_id=%lld", stmt->subname,
							(long long) subscriptionId)));
		}
	}
	else
	{
		ereport(NOTICE,
				(errmsg("lake_subscription_sugar: ALTER SUBSCRIPTION \"%s\" kind %d "
						"is not implemented by this prototype; ignoring",
						stmt->subname, (int) stmt->kind)));
	}

	SPI_finish();
}

static void
lake_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context,
					ParamListInfo params,
					QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;

	if (IsA(parsetree, CreateSubscriptionStmt))
	{
		CreateSubscriptionStmt *stmt = (CreateSubscriptionStmt *) parsetree;
		char	   *target = get_string_option(stmt->options, "lake_target");

		if (target && strcmp(target, "iceberg") == 0)
		{
			handle_create_subscription(stmt);
			return;
		}
	}
	else if (IsA(parsetree, DropSubscriptionStmt))
	{
		DropSubscriptionStmt *stmt = (DropSubscriptionStmt *) parsetree;
		int64		subscriptionId;

		SPI_connect();
		subscriptionId = lake_subscription_id(stmt->subname);
		SPI_finish();

		if (subscriptionId >= 0)
		{
			handle_drop_subscription(stmt, subscriptionId);
			return;
		}
	}
	else if (IsA(parsetree, AlterSubscriptionStmt))
	{
		AlterSubscriptionStmt *stmt = (AlterSubscriptionStmt *) parsetree;
		int64		subscriptionId;

		SPI_connect();
		subscriptionId = lake_subscription_id(stmt->subname);
		SPI_finish();

		if (subscriptionId >= 0)
		{
			handle_alter_subscription(stmt, subscriptionId);
			return;
		}
	}

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
							queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
								params, queryEnv, dest, qc);
}

void
_PG_init(void)
{
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = lake_ProcessUtility;
}
