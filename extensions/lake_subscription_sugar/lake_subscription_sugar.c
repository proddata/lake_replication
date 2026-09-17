/*
 * Experiment only: intercepts CREATE SUBSCRIPTION ... WITH (lake_target =
 * 'iceberg') via ProcessUtility_hook and routes it to this project's own
 * lake_replication.create_subscription() SQL function instead of letting
 * PostgreSQL's real subscription/slot/apply-worker machinery run at all.
 * Any CREATE SUBSCRIPTION without that option falls through unchanged.
 *
 * Scoping note (not solved here): this stub hardcodes the source system
 * identity instead of connecting to `conninfo` to discover it, since the
 * point of this experiment is only to prove the interception + handoff
 * mechanism, not full identity discovery.
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
			StringInfoData buf;
			char	   *pubname;
			int			ret;

			pubname = strVal(linitial(stmt->publication));

			ereport(NOTICE,
					(errmsg("lake_subscription_sugar: intercepted CREATE SUBSCRIPTION \"%s\" "
							"(conninfo=%s, publication=%s); routing to "
							"lake_replication.create_subscription instead of "
							"native apply", stmt->subname, stmt->conninfo,
							pubname)));

			SPI_connect();
			initStringInfo(&buf);
			appendStringInfo(&buf,
							 "SELECT lake_replication.create_subscription("
							 "9999999999999999999::numeric, 0::oid, 1::bigint, "
							 "%s::name, %s::name, ARRAY[%s]::text[])",
							 quote_literal_cstr(stmt->subname),
							 quote_literal_cstr(psprintf("%s_slot", stmt->subname)),
							 quote_literal_cstr(pubname));
			ret = SPI_execute(buf.data, false, 0);
			if (ret != SPI_OK_SELECT)
				elog(ERROR, "lake_subscription_sugar: create_subscription call failed (%d)", ret);
			SPI_finish();

			/*
			 * Swallow the statement entirely: never call
			 * standard_ProcessUtility, so PostgreSQL's real subscription,
			 * slot, and apply-worker machinery never run.
			 */
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
