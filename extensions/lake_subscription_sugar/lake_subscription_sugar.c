/*
 * Experiment only: intercepts CREATE/ALTER/DROP SUBSCRIPTION statements
 * that target this project's own Iceberg replication control plane
 * (lake_replication.*) via ProcessUtility_hook, instead of letting
 * PostgreSQL's real subscription/slot/apply-worker machinery run at all.
 *
 * - CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg') is claimed by
 *   this hook. The source system identity is discovered for real, via
 *   dblink against the statement's own CONNECTION string, then handed to
 *   lake_replication.create_subscription(). It then spawns
 *   lake-replication-initial-sync and lake-replication-supervisor as
 *   detached OS processes (fork+execve, never a shell) so replication
 *   actually starts, the way native CREATE SUBSCRIPTION would spawn its
 *   own walreceiver/apply worker -- this project just does it with its own
 *   binaries instead of core's.
 * - ALTER/DROP SUBSCRIPTION are claimed only when `subname` matches a
 *   subscription this hook already created (looked up by name in
 *   lake_replication.subscriptions); anything else falls through
 *   unchanged, so ordinary native subscriptions keep working. DROP drops
 *   the source-side replication slot for real (via a second dblink
 *   connection, using conninfo recorded at CREATE time in this
 *   extension's own lake_subscription_sugar.pending_cleanup table -- see
 *   drop_source_slot), signals the running supervisor to stop
 *   cooperatively, then SIGTERMs its pid only as a backstop, and tears
 *   down the control-plane rows.
 *
 * LAKE_SOURCE_ADDRESS is the real host:port the CONNECTION string's own
 * dblink connection used to reach the source (inet_server_addr()/
 * inet_server_port()), correct whether the source is local or genuinely
 * remote. LAKE_DESTINATION_ADDRESS stays loopback deliberately: the
 * workers this hook spawns always run alongside this same instance
 * regardless of where the source is, so only its port is discovered
 * (current_setting('port')), not a routable address.
 *
 * Any statement not claimed by the rules above falls through to
 * standard_ProcessUtility (or the next hook) unmodified.
 *
 * ALTER SUBSCRIPTION ... ENABLE restarts a stopped subscription by
 * respawning only the supervisor (not initial sync, which already ran
 * once and is not safe to blindly re-run against a table that may
 * already be streaming) -- see handle_enable_subscription. It requires
 * this extension's own pending_cleanup row from CREATE time to reconnect
 * to the source and rediscover its address; a subscription created
 * before that tracking existed cannot be restarted this way.
 *
 * Known limitations, in addition to the ones noted inline below:
 * - LAKE_DESTINATION_SCHEMA is hardcoded to "public".
 * - the worker binaries are expected at /usr/local/bin/lake-replication-*;
 *   nothing here builds or installs them (see tests/subscription_sugar/probe.py).
 */
#include "postgres.h"
#include "access/xact.h"
#include "fmgr.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "lib/stringinfo.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

PG_MODULE_MAGIC;

#define WORKER_BIN_DIR "/usr/local/bin"

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
 * pg_control_system()/pg_control_checkpoint(); the source's own database
 * name (so the spawned workers connect to the same database this
 * statement's CONNECTION string pointed at, without re-parsing conninfo);
 * and the real "host:port" the dblink connection actually used to reach
 * it (inet_server_addr()/inet_server_port(), correct whether the source
 * is local or a genuinely remote cluster, unlike guessing from conninfo).
 * Must be called with SPI already connected. Raises an error if the
 * source is unreachable or the dblink extension cannot be created.
 */
static void
discover_source_identity(const char *conninfo, char **outSystemIdentifier,
						 char **outDatabaseOid, char **outTimeline,
						 char **outDatabaseName, char **outSourceAddress)
{
	StringInfoData buf;
	char	   *host;
	char	   *port;

	/* dblink is guaranteed installed: declared in the control file's `requires`. */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT identity.system_identifier, identity.database_oid, "
					 "identity.timeline, identity.database_name, "
					 "identity.host, identity.port FROM dblink(%s, "
					 "'SELECT system_identifier, "
					 "(SELECT oid FROM pg_database WHERE datname = current_database()), "
					 "(SELECT timeline_id FROM pg_control_checkpoint()), "
					 "current_database(), "
					 "host(inet_server_addr()), inet_server_port() "
					 "FROM pg_control_system()') "
					 "AS identity(system_identifier numeric, database_oid oid, "
					 "timeline bigint, database_name text, host text, port int)",
					 quote_literal_cstr(conninfo));

	if (SPI_execute(buf.data, true, 0) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR,
			 "lake_subscription_sugar: could not discover the source identity "
			 "via dblink for conninfo %s", conninfo);

	*outSystemIdentifier = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	*outDatabaseOid = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	*outTimeline = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	*outDatabaseName = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);
	host = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5);
	port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6);
	if (host == NULL || port == NULL)
		elog(ERROR,
			 "lake_subscription_sugar: source connection for conninfo %s did not "
			 "report a TCP address (inet_server_addr() is NULL for non-TCP "
			 "connections, e.g. a unix socket)", conninfo);
	*outSourceAddress = psprintf("%s:%s", host, port);
}

/*
 * spawn_detached execve()s `path` with `argv`/`envp` (never a shell -- argv
 * elements coming from SQL identifiers must never be interpolated into a
 * command string) as a fully detached process: double-forked so the
 * grandchild is reparented to init and this backend never waits on it, in
 * its own session (setsid) so it outlives this backend's process group,
 * with stdout/stderr redirected to `logPath`.
 *
 * Between fork() and execve() the child must only call
 * async-signal-safe functions -- no more SPI, no more elog -- which is why
 * every argv/envp string is fully built before this is called.
 */
static void
spawn_detached(const char *path, char *const argv[], char *const envp[],
			  const char *logPath)
{
	pid_t		first;

	first = fork();
	if (first < 0)
		elog(ERROR, "lake_subscription_sugar: fork failed: %m");

	if (first == 0)
	{
		pid_t		second = fork();

		if (second == 0)
		{
			int			logFd;
			int			fd;

			setsid();

			/*
			 * Close every inherited descriptor above stderr -- this
			 * backend's sockets and shared-memory mappings are otherwise
			 * still open in the child until execve() replaces the process
			 * image, and not all of them are marked close-on-exec.
			 */
			for (fd = 3; fd < 256; fd++)
				close(fd);

			logFd = open(logPath, O_CREAT | O_WRONLY | O_APPEND, 0644);
			if (logFd >= 0)
			{
				dup2(logFd, STDOUT_FILENO);
				dup2(logFd, STDERR_FILENO);
				close(logFd);
			}
			execve(path, argv, envp);
			_exit(127);
		}
		_exit(0);
	}

	waitpid(first, NULL, 0);
}

/*
 * build_worker_env allocates a NULL-terminated envp array (each entry
 * palloc'd via psprintf, so it lives in the current SPI/portal memory
 * context and does not need manual freeing here) with everything
 * lake-replication-initial-sync and lake-replication-supervisor need to
 * run inside this same PostgreSQL instance and pick up the subscription
 * this hook just created.
 */
static char **
build_worker_env(int64 subscriptionId, const char *slotName,
				 const char *publicationName, const char *sourceDatabase,
				 const char *destinationDatabase, const char *sourceAddress,
				 const char *destinationAddress)
{
	char	  **envp = palloc(15 * sizeof(char *));
	int			i = 0;

	envp[i++] = psprintf("LAKE_SLOT=%s", slotName);
	envp[i++] = psprintf("LAKE_PUBLICATION=%s", publicationName);
	envp[i++] = psprintf("LAKE_SUBSCRIPTION_ID=%lld", (long long) subscriptionId);
	envp[i++] = pstrdup("LAKE_DISCOVER_PUBLICATION=1");
	envp[i++] = pstrdup("LAKE_DESTINATION_SCHEMA=public");
	envp[i++] = pstrdup("LAKE_CREATE_TABLES_USING_ICEBERG=1");
	envp[i++] = pstrdup("LAKE_TYPE_MAPPING_VERSION=1");
	envp[i++] = psprintf("LAKE_SOURCE_ADDRESS=%s", sourceAddress);
	envp[i++] = psprintf("LAKE_DESTINATION_ADDRESS=%s", destinationAddress);
	envp[i++] = psprintf("LAKE_SOURCE_DATABASE=%s", sourceDatabase);
	envp[i++] = psprintf("LAKE_DESTINATION_DATABASE=%s", destinationDatabase);
	envp[i++] = pstrdup("LAKE_USER=postgres");

	/*
	 * Keep each supervisor cycle short so a stop request (checked only
	 * between cycles) is noticed quickly -- DROP SUBSCRIPTION waits for a
	 * clean exit before it dares fall back to a signal (see
	 * handle_drop_subscription for why a signal is dangerous here).
	 */
	envp[i++] = pstrdup("LAKE_RECEIVER_IDLE_TIMEOUT_MS=200");
	envp[i++] = pstrdup("LAKE_SUPERVISOR_POLL_MS=100");
	envp[i++] = NULL;
	Assert(i == 15);
	return envp;
}

/*
 * record_pending_cleanup writes to lake_subscription_sugar.pending_cleanup
 * (owned by this extension's own --1.0.sql install script, not created
 * here -- see that file) just enough -- the source conninfo and slot
 * name -- to later drop the source-side replication slot
 * lake-replication-initial-sync creates, since DropSubscriptionStmt
 * carries no conninfo of its own.
 *
 * Security note: this stores conninfo exactly as given in the original
 * CREATE SUBSCRIPTION statement, which already appears in cleartext in
 * the server log and pg_stat_activity for that statement; if a caller
 * embeds a password in CONNECTION instead of using out-of-band
 * credentials (matching how this project's own workers take
 * LAKE_SOURCE_PASSWORD from the environment, never from a connection
 * string), that password is stored here too. Must be called with SPI
 * already connected.
 */
static void
record_pending_cleanup(int64 subscriptionId, const char *conninfo,
					   const char *slotName)
{
	SPI_execute(psprintf(
		"INSERT INTO lake_subscription_sugar.pending_cleanup "
		"(subscription_id, source_conninfo, source_slot_name) "
		"VALUES (%lld, %s, %s)",
		(long long) subscriptionId, quote_literal_cstr(conninfo),
		quote_literal_cstr(slotName)), false, 0);
}

/*
 * drop_source_slot looks up the pending_cleanup row for `subscriptionId`,
 * if any, reconnects to the source via dblink, and drops the slot for
 * real. Best-effort: a source that is unreachable, or that has already
 * lost the slot on its own (e.g. a Milestone-21-style invalidation), only
 * gets a WARNING here, never an ERROR that would abort the whole DROP
 * SUBSCRIPTION over a cleanup step that no longer matters.
 */
static void
drop_source_slot(int64 subscriptionId)
{
	char	   *conninfo = NULL;
	char	   *slotName = NULL;

	if (SPI_execute(psprintf(
			"SELECT source_conninfo, source_slot_name FROM "
			"lake_subscription_sugar.pending_cleanup WHERE subscription_id = %lld",
			(long long) subscriptionId), true, 1) == SPI_OK_SELECT
		&& SPI_processed == 1)
	{
		conninfo = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		slotName = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	}

	if (conninfo != NULL && slotName != NULL)
	{
		{
			char	   *slotLiteral = quote_literal_cstr(slotName);
			char	   *innerQuery = psprintf(
				"DO $lake_sugar_drop_slot$ BEGIN "
				"IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s) "
				"THEN PERFORM pg_drop_replication_slot(%s); END IF; "
				"END $lake_sugar_drop_slot$",
				slotLiteral, slotLiteral);
			StringInfoData buf;

			initStringInfo(&buf);
			appendStringInfo(&buf, "SELECT dblink_exec(%s, %s)",
							 quote_literal_cstr(conninfo),
							 quote_literal_cstr(innerQuery));
			if (SPI_execute(buf.data, false, 0) < 0)
				ereport(WARNING,
						(errmsg("lake_subscription_sugar: could not drop "
								"source-side slot \"%s\"; it is left behind",
								slotName)));
		}
	}

	SPI_execute(psprintf(
		"DELETE FROM lake_subscription_sugar.pending_cleanup "
		"WHERE subscription_id = %lld", (long long) subscriptionId), false, 0);
}

/*
 * discover_destination reads this same instance's own database name and
 * port. The destination is always loopback: the workers this hook spawns
 * run alongside this same PostgreSQL instance no matter where the source
 * is, so only the port needs discovering, not a routable address. Must be
 * called with SPI already connected.
 */
static void
discover_destination(char **outDatabase, char **outAddress)
{
	if (SPI_execute("SELECT current_database(), current_setting('port')",
					true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "lake_subscription_sugar: could not determine "
					"current_database()/port");
	*outDatabase = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	*outAddress = psprintf("127.0.0.1:%s",
						   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
}

/*
 * spawn_workers fire-and-forgets lake-replication-supervisor (and, when
 * `spawnInitialSync`, lake-replication-initial-sync too) as detached
 * processes for `subscriptionId`. Shared by CREATE (which always needs
 * initial sync) and ALTER ... ENABLE (which only needs to restart the
 * supervisor, since initial sync already ran once at CREATE time).
 */
static void
spawn_workers(int64 subscriptionId, const char *slotName,
			 const char *publicationName, const char *sourceDatabase,
			 const char *destinationDatabase, const char *sourceAddress,
			 const char *destinationAddress, bool spawnInitialSync)
{
	char	  **envp;
	char	   *supervisorPath;
	char	   *supervisorArgv[2];
	char	   *supervisorLog;
	char	   *initialSyncPath;
	char	   *initialSyncArgv[2];
	char	   *initialSyncLog;

	envp = build_worker_env(subscriptionId, slotName, publicationName,
							sourceDatabase, destinationDatabase,
							sourceAddress, destinationAddress);
	supervisorPath = pstrdup(WORKER_BIN_DIR "/lake-replication-supervisor");
	supervisorArgv[0] = supervisorPath;
	supervisorArgv[1] = NULL;
	supervisorLog = psprintf("/tmp/lake_sugar_%lld_supervisor.log",
							 (long long) subscriptionId);

	if (spawnInitialSync)
	{
		initialSyncPath = pstrdup(WORKER_BIN_DIR "/lake-replication-initial-sync");
		initialSyncArgv[0] = initialSyncPath;
		initialSyncArgv[1] = NULL;
		initialSyncLog = psprintf("/tmp/lake_sugar_%lld_initial_sync.log",
								  (long long) subscriptionId);

		ereport(NOTICE,
				(errmsg("lake_subscription_sugar: starting initial sync and the "
						"supervisor for subscription_id=%lld (logs: %s, %s)",
						(long long) subscriptionId, initialSyncLog, supervisorLog)));

		/*
		 * Fire-and-forget both: the supervisor's own poll loop already
		 * reports "paused" and keeps waiting for as long as the
		 * subscription's state is not yet 'streaming', so starting it
		 * concurrently with initial sync (rather than waiting for initial
		 * sync to finish first) mirrors how native logical replication's
		 * tablesync and apply workers overlap.
		 */
		spawn_detached(initialSyncPath, initialSyncArgv, envp, initialSyncLog);
	}
	else
	{
		ereport(NOTICE,
				(errmsg("lake_subscription_sugar: restarting the supervisor for "
						"subscription_id=%lld (log: %s)",
						(long long) subscriptionId, supervisorLog)));
	}

	spawn_detached(supervisorPath, supervisorArgv, envp, supervisorLog);
}

static void
handle_create_subscription(CreateSubscriptionStmt *stmt)
{
	StringInfoData buf;
	char	   *pubname;
	char	   *systemIdentifier;
	char	   *databaseOid;
	char	   *timeline;
	char	   *sourceDatabase;
	char	   *destinationDatabase;
	char	   *sourceAddress;
	char	   *destinationAddress;
	char	   *slotName;
	int64		subscriptionId;

	pubname = strVal(linitial(stmt->publication));
	slotName = psprintf("%s_slot", stmt->subname);

	SPI_connect();

	discover_source_identity(stmt->conninfo, &systemIdentifier, &databaseOid,
							 &timeline, &sourceDatabase, &sourceAddress);
	discover_destination(&destinationDatabase, &destinationAddress);

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
					 quote_literal_cstr(slotName),
					 quote_literal_cstr(pubname));
	if (SPI_execute(buf.data, true, 0) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "lake_subscription_sugar: create_subscription call failed");
	{
		bool		isnull;

		subscriptionId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													 SPI_tuptable->tupdesc, 1,
													 &isnull));
	}

	record_pending_cleanup(subscriptionId, stmt->conninfo, slotName);

	spawn_workers(subscriptionId, slotName, pubname, sourceDatabase,
				 destinationDatabase, sourceAddress, destinationAddress,
				 true /* spawnInitialSync */ );

	SPI_finish();
}

static void
handle_drop_subscription(DropSubscriptionStmt *stmt, int64 subscriptionId,
						 bool isTopLevel)
{
	StringInfoData buf;
	pid_t		supervisorPid = -1;

	ereport(NOTICE,
			(errmsg("lake_subscription_sugar: intercepted DROP SUBSCRIPTION \"%s\"; "
					"tearing down lake_replication control-plane state instead of "
					"native apply", stmt->subname)));

	/*
	 * What follows commits a write partway through this statement (see
	 * below), the same technique pg_lake's own VACUUM (ICEBERG) uses. That
	 * is only safe as the sole statement in its own transaction, exactly
	 * like VACUUM -- hence the same guard VACUUM uses.
	 */
	PreventInTransactionBlock(isTopLevel, "DROP SUBSCRIPTION");

	if (!ActiveSnapshotSet())
		PushActiveSnapshot(GetTransactionSnapshot());

	SPI_connect();

	/*
	 * Ask the running supervisor to stop cooperatively (it polls
	 * desired_state and, on noticing 'stopped', calls
	 * report_service_stopped() and exits cleanly -- exit(0), no signal).
	 * This matters in this exact container: PostgreSQL runs as PID 1 here
	 * (no separate init), so a detached grandchild that later dies BY
	 * SIGNAL is reparented straight onto the postmaster, which logs it as
	 * an "untracked child ... terminated by signal" and restarts the
	 * entire cluster out of caution -- a clean exit(0) does not trigger
	 * that. So the SIGTERM below is a last resort for a supervisor that
	 * ignored the cooperative stop, not the normal path.
	 */
	SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
						  (long long) subscriptionId), false, 0);
	if (SPI_execute(psprintf("SELECT supervisor_pid FROM lake_replication.service_status "
							  "WHERE subscription_id = %lld", (long long) subscriptionId),
					true, 1) == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		value = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
											  1, &isnull);

		if (!isnull)
			supervisorPid = (pid_t) DatumGetInt64(value);
	}
	SPI_finish();

	/*
	 * Commit the stop request now, in its own transaction, so the
	 * supervisor's separate connection can actually see desired_state =
	 * 'stopped' -- it cannot while this statement's own transaction is
	 * still open, no matter how long the poll loop below waits.
	 */
	PopActiveSnapshot();
	CommitTransactionCommand();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (supervisorPid > 0)
	{
		int			waited;
		bool		exited = false;

		for (waited = 0; waited < 120; waited++)
		{
			/* kill(pid, 0) only checks existence; it never signals. */
			if (kill(supervisorPid, 0) != 0)
			{
				exited = true;
				break;
			}
			usleep(50000);		/* 50ms; up to 6s total */
		}
		if (!exited)
		{
			ereport(WARNING,
					(errmsg("lake_subscription_sugar: supervisor pid %d did not "
							"stop on its own within 6s; sending SIGTERM as a "
							"last resort", (int) supervisorPid)));
			kill(supervisorPid, SIGTERM);
		}
	}

	SPI_connect();

	drop_source_slot(subscriptionId);

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
	PopActiveSnapshot();
}

/*
 * handle_enable_subscription restarts replication for a subscription that
 * was previously disabled: only the supervisor is respawned (not initial
 * sync, which already ran once at CREATE time and is not safe to blindly
 * re-run against a table that may already be streaming). Requires this
 * extension's own pending_cleanup row from CREATE time to reconnect to
 * the source and rediscover its address; a subscription created before
 * that tracking existed cannot be restarted this way.
 */
static void
handle_enable_subscription(const char *subname, int64 subscriptionId)
{
	char	   *slotName;
	char	   *publicationName;
	char	   *conninfo;
	char	   *systemIdentifier;
	char	   *databaseOid;
	char	   *timeline;
	char	   *sourceDatabase;
	char	   *sourceAddress;
	char	   *destinationDatabase;
	char	   *destinationAddress;

	if (SPI_execute(psprintf(
			"SELECT slot_name, array_to_string(publication_names, ',') "
			"FROM lake_replication.subscriptions WHERE subscription_id = %lld",
			(long long) subscriptionId), true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		elog(ERROR, "lake_subscription_sugar: could not look up subscription %lld",
			 (long long) subscriptionId);
	slotName = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	publicationName = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

	if (SPI_execute(psprintf(
			"SELECT source_conninfo FROM lake_subscription_sugar.pending_cleanup "
			"WHERE subscription_id = %lld", (long long) subscriptionId),
			true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		ereport(ERROR,
				(errmsg("lake_subscription_sugar: cannot restart \"%s\": no "
						"recorded source connection info for subscription_id=%lld "
						"(it predates this extension's pending_cleanup tracking)",
						subname, (long long) subscriptionId)));
	conninfo = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	discover_source_identity(conninfo, &systemIdentifier, &databaseOid,
							 &timeline, &sourceDatabase, &sourceAddress);
	discover_destination(&destinationDatabase, &destinationAddress);

	spawn_workers(subscriptionId, slotName, publicationName, sourceDatabase,
				 destinationDatabase, sourceAddress, destinationAddress,
				 false /* spawnInitialSync */ );
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
					(errmsg("lake_subscription_sugar: intercepted ALTER SUBSCRIPTION "
							"\"%s\" ENABLE; restarting the supervisor", stmt->subname)));
			handle_enable_subscription(stmt->subname, subscriptionId);
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
			handle_drop_subscription(stmt, subscriptionId,
									 context == PROCESS_UTILITY_TOPLEVEL);
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
