/*
 * PostgreSQL integration for CREATE/ALTER/DROP SUBSCRIPTION statements
 * that target this project's own Iceberg replication control plane
 * (lake_replication.*) via ProcessUtility_hook, instead of letting
 * PostgreSQL's real subscription/slot/apply-worker machinery run at all.
 *
 * - CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg') is claimed by
 *   this hook. The source system identity is discovered for real, via
 *   dblink against the statement's own CONNECTION string, then handed to
 *   lake_replication.create_subscription(). The long-running supervisor
 *   starts and retries initial sync after the creating transaction commits.
 *   The supervisor is not spawned directly: this statement records the
 *   subscription's launch configuration (record_worker_launch_config); a
 *   separate, permanently-running launcher background worker
 *   (lake_replication_launcher_main, registered via
 *   shared_preload_libraries -- see _PG_init) notices the new
 *   configuration on its own and starts the supervisor as a genuine
 *   PostgreSQL background worker it supervises directly, the way native
 *   CREATE SUBSCRIPTION would spawn its own walreceiver/apply worker under
 *   the postmaster -- this project just does it with its own binary
 *   instead of core's, and by way of one extra indirection (the launcher)
 *   rather than the postmaster spawning it straight from this statement.
 *   See the big comment above lake_replication_launcher_main for
 *   why that indirection is required, not just convenient.
 * - ALTER/DROP SUBSCRIPTION are claimed only when `subname` matches a
 *   subscription this hook already created (looked up by name in
 *   lake_replication.subscriptions); anything else falls through
 *   unchanged, so ordinary native subscriptions keep working. DROP drops
 *   the source-side replication slot for real (via a second dblink
 *   connection, using conninfo recorded at CREATE time in this
 *   extension's protected lake_replication.source_connections table -- see
 *   drop_source_slot), deletes the worker_launch_config row (the
 *   launcher's authoritative signal to stop and permanently retire that
 *   supervisor), and tears down the control-plane rows -- all as one fast
 *   statement that never waits for the actual OS process to exit; the
 *   launcher does that asynchronously.
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
 * flipping worker_launch_config.desired_state back to 'running' -- see
 * handle_enable_subscription. The launcher notices and registers a fresh
 * supervisor. Completed subscriptions resume streaming; incomplete initial
 * copies restart against a fresh snapshot. No dblink round trip is needed:
 * source_address and everything else the supervisor needs were recorded
 * durably in worker_launch_config at CREATE SUBSCRIPTION time. A
 * subscription created before that tracking existed cannot be restarted
 * this way.
 *
 * Requiring shared_preload_libraries and a configured
 * lake_replication.database_name (see _PG_init) is a real
 * departure from how this extension used to work (CREATE EXTENSION plus
 * session_preload_libraries, no server restart, no extra GUC): a
 * BackgroundWorkerHandle from RegisterDynamicBackgroundWorker is only
 * valid inside the backend that obtained it, so nothing can reconstruct
 * one later, in a different session, to stop a worker it did not start
 * itself. The only way to manage a worker across the CREATE SUBSCRIPTION
 * session that starts it and a much later, different DROP SUBSCRIPTION
 * session is a single process that outlives both and keeps every handle
 * in its own memory for as long as it might need one -- hence the
 * launcher, and hence it has to be registered once, at postmaster
 * startup, the only time PostgreSQL allows a *static* background worker
 * registration (RegisterBackgroundWorker, as opposed to the *dynamic*
 * RegisterDynamicBackgroundWorker the launcher itself uses for each
 * supervisor). Without lake_replication.database_name set, CREATE
 * SUBSCRIPTION still records launch configuration correctly, but no
 * launcher exists to ever act on it -- see the GUC's own description.
 *
 * CREATE SUBSCRIPTION naming more than one publication is rejected with a
 * clear error rather than silently applying only the first: this
 * project's own worker binaries only support exactly one publication
 * end to end (lake-replication-initial-sync matches a single publication
 * name by SQL equality against pg_publication_tables, and the wire
 * protocol's publication_names option is populated from the same single
 * value), so accepting the statement and quietly dropping the rest would
 * be a silent correctness bug, not a convenience. (PostgreSQL 18's
 * CreateSubscriptionStmt also has no SERVER-form -- that grammar exists
 * only in newer/devel PostgreSQL branches, not the pinned version this
 * project builds against, so it is not a gap here at all.)
 *
 * WITH (lake_target_schema = '...') overrides the default destination
 * schema ("public") that lake-replication-initial-sync creates missing
 * Iceberg tables in. It is stored durably for incomplete-copy recovery.
 *
 * Known limitations, in addition to the ones noted inline below:
 * - the worker binaries are expected at /usr/local/bin/lake-replication-*;
 *   the release bundle installs them alongside this shared library.
 *
 * Authorization: every statement this hook claims (CREATE/ALTER/DROP
 * SUBSCRIPTION ... WITH (lake_target = 'iceberg')) requires the calling
 * role to be superuser or a member of the lake_replication_admin role this
 * extension's install script creates -- see RequireAuthorized. Without
 * this, any role that could load the extension and issue the statement
 * would get dblink-based access to whatever source the CONNECTION string
 * names, and the ability to spawn OS processes as this backend's user, at
 * whatever privilege the SPI calls below run with (invoker rights: this
 * hook runs inline in the calling backend, not as SECURITY DEFINER).
 *
 * Credential handling: the CONNECTION string is stored in source_connections
 * for source slot cleanup. Its user and password are also extracted for the
 * workers. Migration 0019 enables row-level security on that table because
 * pg_read_all_data overrides ordinary table ACLs. Only the admin policy and
 * the restricted worker getter expose its rows. The extension installer and
 * superusers remain trusted, as with native PostgreSQL subscriptions.
 *
 * A superuser caller can omit the password from CONNECTION entirely and
 * let dblink's underlying libpq connection resolve it from a ~/.pgpass
 * file (or PGPASSFILE) or a connection service file belonging to the OS
 * user running this PostgreSQL instance -- dblink imposes no restriction
 * on a superuser caller. A non-superuser lake_replication_admin member
 * cannot do this: dblink's own dblink_security_check refuses any
 * non-superuser connection unless PQconnectionUsedPassword() reports the
 * server actually verified a password (or GSSAPI-delegated credentials)
 * during the handshake -- a password merely present in the string is not
 * enough, and "trust"/"peer" auth on the source never satisfies this
 * check no matter what the string contains. In practice this means a
 * non-superuser can only use this hook against a source that requires
 * real password authentication (e.g. SCRAM, see Milestone 22), and the
 * password for it necessarily ends up stored in source_connections.
 *
 * Worker stdout/stderr go to lake_replication.log_directory
 * (default /tmp), one file per subscription; SUSET so an
 * unprivileged session cannot redirect a spawned worker's log output
 * before calling CREATE SUBSCRIPTION.
 *
 * Launcher-managed workers fetch source credentials through the destination
 * SQL connection and read optional destination credentials/TLS settings from
 * lake_replication.credential_file. Only a path and subscription ID are passed
 * through execve(); no password is placed in the worker environment.
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
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include <libpq-fe.h>

#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

PG_MODULE_MAGIC;

#define WORKER_BIN_DIR "/usr/local/bin"
#define ADMIN_ROLE_NAME "lake_replication_admin"

static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static char *LogDirectory = "/tmp";
static char *LauncherDatabaseName = NULL;
static char *CredentialFile = NULL;
static int	LauncherNaptimeMs = 1000;

static bool
check_credential_file(char **newValue, void **extra, GucSource source)
{
	(void) extra;
	(void) source;
	if (*newValue != NULL && (*newValue)[0] != '\0' && (*newValue)[0] != '/')
	{
		GUC_check_errdetail("lake_replication.credential_file must be an absolute path");
		return false;
	}
	return true;
}

static void
require_absolute_credential_file(const char *path)
{
	if (path != NULL && path[0] != '\0' && path[0] != '/')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lake_credential_file must be an absolute path")));
}

/*
 * TrackedWorker maps a subscription_id to the BackgroundWorkerHandle the
 * launcher itself obtained by registering that subscription's supervisor.
 * A BackgroundWorkerHandle is only meaningful to the backend that created
 * it (see the launcher-vs-handle-lifetime note above
 * lake_replication_launcher_main) -- this table, kept in the
 * launcher's own long-lived memory, is what makes DROP/ALTER SUBSCRIPTION
 * from an entirely different, later session able to stop a supervisor the
 * launcher started possibly hours earlier: that later session never touches
 * a handle at all, it only ever writes a row to worker_launch_config for
 * the launcher to notice.
 */
typedef struct TrackedWorker
{
	int64		subscriptionId;
	int64		configGeneration;
	BackgroundWorkerHandle *handle;
	bool		seenThisCycle;
}			TrackedWorker;

static volatile sig_atomic_t LauncherGotSigterm = false;

static void
LauncherSigtermHandler(SIGNAL_ARGS)
{
	LauncherGotSigterm = true;
	SetLatch(MyLatch);
}

/*
 * bgw_function_name entry points, looked up by dlsym() in a freshly forked
 * postmaster child that has not run this file's other code at all -- both
 * need external linkage and PGDLLEXPORT despite this translation unit
 * otherwise building with -fvisibility=hidden, and a declaration here keeps
 * -Wmissing-prototypes happy since neither is declared in any header.
 */
extern PGDLLEXPORT void lake_replication_launcher_main(Datum main_arg);
extern PGDLLEXPORT void lake_replication_worker_main(Datum main_arg);

/*
 * RequireAuthorized raises an error unless the current role is superuser
 * or a member of ADMIN_ROLE_NAME. Called at the top of every CREATE/ALTER/
 * DROP SUBSCRIPTION handler this hook claims, before anything else (dblink
 * connection, SPI writes, or spawning a process) happens.
 */
static void
RequireAuthorized(const char *action)
{
	Oid			adminRoleOid;

	if (superuser())
		return;

	adminRoleOid = get_role_oid(ADMIN_ROLE_NAME, true);
	if (OidIsValid(adminRoleOid) && is_member_of_role(GetUserId(), adminRoleOid))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 errmsg("must be superuser or a member of \"%s\" to %s",
					ADMIN_ROLE_NAME, action)));
}

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
						 char **outDatabaseName, char **outSourceAddress,
						 char **outSourceUser)
{
	StringInfoData buf;
	Oid argtypes[1] = {TEXTOID};
	Datum values[1] = {CStringGetTextDatum(conninfo)};
	char	   *host;
	char	   *port;

	/* dblink is guaranteed installed: declared in the control file's `requires`. */
	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "SELECT identity.system_identifier, identity.database_oid, "
					 "identity.timeline, identity.database_name, "
					 "identity.host, identity.port, identity.source_user FROM dblink($1, "
					 "'SELECT system_identifier, "
					 "(SELECT oid FROM pg_database WHERE datname = current_database()), "
					 "(SELECT timeline_id FROM pg_control_checkpoint()), "
					 "current_database(), "
					 "host(inet_server_addr()), inet_server_port(), current_user "
					 "FROM pg_control_system()') "
					 "AS identity(system_identifier numeric, database_oid oid, "
					 "timeline bigint, database_name text, host text, port int, source_user text)");

	if (SPI_execute_with_args(buf.data, 1, argtypes, values, NULL, true, 1) != SPI_OK_SELECT ||
		SPI_processed != 1)
		elog(ERROR,
			 "lake_replication: could not discover the source identity "
			 "via dblink");

	*outSystemIdentifier = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	*outDatabaseOid = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	*outTimeline = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3);
	*outDatabaseName = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4);
	host = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5);
	port = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6);
	*outSourceUser = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 7);
	if (host == NULL || port == NULL)
		elog(ERROR,
			 "lake_replication: source connection did not "
			 "report a TCP address (inet_server_addr() is NULL for non-TCP "
			 "connections, e.g. a unix socket)");
	*outSourceAddress = psprintf("%s:%s", host, port);
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
				 const char *destinationAddress, const char *destinationSchema,
				 const char *credentialFile, int64 configGeneration,
				 bool createSlot)
{
	char	  **envp = palloc(19 * sizeof(char *));
	int			i = 0;

	envp[i++] = psprintf("LAKE_SLOT=%s", slotName);
	envp[i++] = psprintf("LAKE_CREATE_SLOT=%d", createSlot ? 1 : 0);
	envp[i++] = psprintf("LAKE_PUBLICATION=%s", publicationName);
	envp[i++] = psprintf("LAKE_SUBSCRIPTION_ID=%lld", (long long) subscriptionId);
	envp[i++] = pstrdup("LAKE_LOAD_SOURCE_CREDENTIALS=1");
	envp[i++] = psprintf("LAKE_CONFIG_GENERATION=%lld", (long long) configGeneration);
	envp[i++] = pstrdup("LAKE_DISCOVER_PUBLICATION=1");
	envp[i++] = psprintf("LAKE_DESTINATION_SCHEMA=%s", destinationSchema);
	envp[i++] = pstrdup("LAKE_CREATE_TABLES_USING_ICEBERG=1");
	envp[i++] = pstrdup("LAKE_TYPE_MAPPING_VERSION=1");
	envp[i++] = psprintf("LAKE_SOURCE_ADDRESS=%s", sourceAddress);
	envp[i++] = psprintf("LAKE_DESTINATION_ADDRESS=%s", destinationAddress);
	envp[i++] = psprintf("LAKE_SOURCE_DATABASE=%s", sourceDatabase);
	envp[i++] = psprintf("LAKE_DESTINATION_DATABASE=%s", destinationDatabase);
	envp[i++] = pstrdup("LAKE_USER=postgres");

	/*
	 * Keep each supervisor cycle short so a cooperative stop request
	 * (checked only between cycles) is noticed quickly and the supervisor
	 * has a chance to call report_service_stopped() and exit(0) cleanly
	 * before the launcher's own TerminateBackgroundWorker backstop (see
	 * lake_replication_launcher_main) arrives.
	 */
	envp[i++] = pstrdup("LAKE_RECEIVER_IDLE_TIMEOUT_MS=200");
	envp[i++] = pstrdup("LAKE_SUPERVISOR_POLL_MS=100");
	if (credentialFile != NULL && credentialFile[0] != '\0')
		envp[i++] = psprintf("LAKE_CREDENTIAL_FILE=%s", credentialFile);
	envp[i++] = NULL;
	Assert(i <= 19);
	return envp;
}

/*
 * lake_replication_worker_main is the bgw_function_name every
 * per-subscription supervisor background worker runs. Deliberately thin:
 * connect to the database named in bgw_extra, look up this subscription's
 * non-secret launch configuration (recorded once at CREATE SUBSCRIPTION
 * time -- see worker_launch_config in migration 0018), then execve() into the
 * real lake-replication-supervisor binary, replacing this process's image
 * entirely. From that point on the postmaster is supervising the real
 * supervisor binary directly, under this same PID: bgw_restart_time (set
 * by the launcher at registration) makes the postmaster restart it
 * automatically if it ever exits non-zero or dies by signal, with no code
 * here or in the launcher needing to notice or react.
 *
 * No SPI writes happen here, and a lookup failure just logs and exits --
 * this process is a background worker the postmaster itself launched, so
 * an uncaught ERROR here is reported as this worker's own crash (and
 * triggers the same automatic restart as any other failure) rather than
 * being surfaced to any waiting SQL session.
 */
void
lake_replication_worker_main(Datum main_arg)
{
	int64		subscriptionId = DatumGetInt64(main_arg);
	char		dbname[BGW_EXTRALEN];
	char	   *slotName = NULL;
	char	   *publicationName = NULL;
	char	   *sourceDatabase = NULL;
	char	   *destinationDatabase = NULL;
	char	   *sourceAddress = NULL;
	char	   *destinationAddress = NULL;
	char	   *destinationSchema = NULL;
	char	   *credentialFile = NULL;
	int64		configGeneration = 0;
	bool		createSlot = true;
	char	  **envp;
	char	   *supervisorPath;
	char	   *supervisorArgv[2];
	char	   *supervisorLog;
	int			logFd;
	int			fd;

	BackgroundWorkerUnblockSignals();

	memcpy(dbname, MyBgworkerEntry->bgw_extra, BGW_EXTRALEN);
	dbname[BGW_EXTRALEN - 1] = '\0';

	BackgroundWorkerInitializeConnection(dbname, NULL, 0);

	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	if (SPI_execute(psprintf(
			"SELECT slot_name, publication_name, source_database, "
			"destination_database, source_address, destination_address, "
			"destination_schema, credential_file, config_generation, create_slot "
			"FROM lake_replication.worker_launch_config "
			"WHERE subscription_id = %lld", (long long) subscriptionId),
			true, 1) == SPI_OK_SELECT && SPI_processed == 1)
	{
		/* Copied into TopMemoryContext: SPI_finish() below frees SPI's own context. */
		slotName = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1));
		publicationName = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
		sourceDatabase = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3));
		destinationDatabase = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4));
		sourceAddress = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5));
		destinationAddress = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6));
		destinationSchema = MemoryContextStrdup(TopMemoryContext,
			SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 7));
		{
			char	   *storedCredentialFile = SPI_getvalue(
				SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 8);

			if (storedCredentialFile != NULL)
				credentialFile = MemoryContextStrdup(TopMemoryContext,
											 storedCredentialFile);
		}
		{
			bool		isnull;

			configGeneration = DatumGetInt64(SPI_getbinval(
				SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 9, &isnull));
			if (isnull)
				configGeneration = 0;
		}
		{
			bool		isnull;

			createSlot = DatumGetBool(SPI_getbinval(
				SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 10, &isnull));
			if (isnull)
				createSlot = true;
		}
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	if (slotName == NULL || configGeneration <= 0)
	{
		/*
		 * proc_exit(0), deliberately, not 1 -- see the fork/waitpid block
		 * below for why this bgworker must never report a non-zero exit
		 * to Postgres for an ordinary, expected condition.
		 */
		elog(WARNING, "lake_replication worker: no launch configuration "
			 "for subscription_id=%lld; exiting", (long long) subscriptionId);
		proc_exit(0);
	}
	if (credentialFile == NULL && CredentialFile != NULL && CredentialFile[0] != '\0')
		credentialFile = CredentialFile;

	envp = build_worker_env(subscriptionId, slotName, publicationName,
							sourceDatabase, destinationDatabase, sourceAddress,
							destinationAddress, destinationSchema, credentialFile,
							configGeneration, createSlot);
	supervisorPath = pstrdup(WORKER_BIN_DIR "/lake-replication-supervisor");
	supervisorArgv[0] = supervisorPath;
	supervisorArgv[1] = NULL;
	supervisorLog = psprintf("%s/lake_replication_%lld_supervisor.log",
							 LogDirectory, (long long) subscriptionId);

	/*
	 * Fork a child to exec the real supervisor binary into, rather than
	 * execve()-ing it directly into this bgworker process, and always
	 * report a clean exit(0) to Postgres ourselves afterward no matter
	 * what the child actually did. This is not optional hygiene: exec()
	 * would make the supervisor binary's own raw exit status become this
	 * background worker's exit status as far as the postmaster is
	 * concerned, and PostgreSQL treats ANY non-zero (or signaled) bgworker
	 * exit as a potential shared-memory-corrupting crash -- the same
	 * response as a real segfault: it terminates every other backend and
	 * reinitializes shared memory. lake-replication-supervisor calls
	 * `std::process::exit(1)` on perfectly ordinary conditions (e.g. losing
	 * a duplicate-supervisor race after its predecessor was killed -- see
	 * Milestone 13's fencing), so exec()-ing it directly was observed to
	 * take down the entire cluster on a single killed worker, not just
	 * that one subscription. The launcher, not Postgres's own
	 * bgw_restart_time, is what decides whether and when to start a
	 * replacement after this process exits -- see the liveness check in
	 * lake_replication_launcher_main -- which is also why the
	 * supervisor's own dynamic registration uses BGW_NEVER_RESTART: letting
	 * Postgres additionally restart based on exit status would be redundant
	 * with, and could race, the launcher's own decision.
	 */
	{
		pid_t		child = fork();

		if (child < 0)
		{
			elog(WARNING, "lake_replication worker: fork failed: %m");
			proc_exit(0);
		}

		if (child == 0)
		{
			/*
			 * Between fork() and execve() only async-signal-safe calls are
			 * allowed -- no more SPI, no more elog.
			 */
			for (fd = 3; fd < 256; fd++)
				close(fd);

			logFd = open(supervisorLog, O_CREAT | O_WRONLY | O_APPEND, 0644);
			if (logFd >= 0)
			{
				dup2(logFd, STDOUT_FILENO);
				dup2(logFd, STDERR_FILENO);
				close(logFd);
			}

			execve(supervisorPath, supervisorArgv, envp);
			_exit(127);
		}

		{
			int			status;

			if (waitpid(child, &status, 0) < 0)
				elog(WARNING, "lake_replication worker: waitpid failed: %m");
			else if (WIFSIGNALED(status))
				elog(WARNING, "lake_replication worker: supervisor for "
					 "subscription_id=%lld terminated by signal %d",
					 (long long) subscriptionId, WTERMSIG(status));
			else if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
				elog(WARNING, "lake_replication worker: supervisor for "
					 "subscription_id=%lld exited with status %d",
					 (long long) subscriptionId, WEXITSTATUS(status));
		}
	}

	proc_exit(0);
}

/*
 * lake_replication_launcher_main is the single, long-lived
 * background worker registered via shared_preload_libraries (see
 * _PG_init) that owns every dynamic per-subscription supervisor worker
 * this extension starts. It is the only thing in this extension that ever
 * calls RegisterDynamicBackgroundWorker or TerminateBackgroundWorker --
 * see the TrackedWorker comment for why that has to be true: a
 * BackgroundWorkerHandle is only valid inside the backend that obtained
 * it, so managing a worker across the CREATE SUBSCRIPTION session that
 * started it and a much later, entirely different DROP SUBSCRIPTION
 * session requires exactly one process that outlives both and remembers
 * every handle for as long as it might need to stop one.
 *
 * Each cycle: read every row of worker_launch_config in the configured
 * database, start a fresh supervisor for any row not yet tracked with
 * desired_state = 'running', and TerminateBackgroundWorker anything
 * tracked whose row is now gone or has desired_state = 'stopped'. Sleeps
 * for lake_replication.launcher_naptime_ms between cycles.
 */
void
lake_replication_launcher_main(Datum main_arg)
{
	HASHCTL		hashCtl;
	HTAB	   *tracked;

	pqsignal(SIGTERM, LauncherSigtermHandler);
	BackgroundWorkerUnblockSignals();

	if (LauncherDatabaseName == NULL || LauncherDatabaseName[0] == '\0')
	{
		ereport(LOG,
				(errmsg("lake_replication launcher: "
						"lake_replication.database_name is not set; "
						"exiting without managing any workers")));
		proc_exit(0);
	}

	BackgroundWorkerInitializeConnection(LauncherDatabaseName, NULL, 0);

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(int64);
	hashCtl.entrysize = sizeof(TrackedWorker);
	tracked = hash_create("lake_replication tracked workers", 64,
						  &hashCtl, HASH_ELEM | HASH_BLOBS);

	ereport(LOG,
			(errmsg("lake_replication launcher: watching database \"%s\"",
					LauncherDatabaseName)));

	while (!LauncherGotSigterm)
	{
		PG_TRY();
		{
			HASH_SEQ_STATUS seqStatus;
			TrackedWorker *entry;
			int			ret;
			uint64		row;

			/* Reset the seen marker; anything still false after this cycle's
			 * scan below no longer has a worker_launch_config row at all. */
			hash_seq_init(&seqStatus, tracked);
			while ((entry = (TrackedWorker *) hash_seq_search(&seqStatus)) != NULL)
				entry->seenThisCycle = false;

			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());

			ret = SPI_execute(
				"SELECT subscription_id, destination_database, desired_state, "
				"config_generation "
				"FROM lake_replication.worker_launch_config", true, 0);

			if (ret == SPI_OK_SELECT)
			{
				for (row = 0; row < SPI_processed; row++)
				{
					bool		isnull;
					int64		subscriptionId = DatumGetInt64(
						SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc,
									 1, &isnull));
					char	   *destinationDatabase = SPI_getvalue(
						SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 2);
					char	   *desiredState = SPI_getvalue(
						SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 3);
					int64		configGeneration = DatumGetInt64(
						SPI_getbinval(SPI_tuptable->vals[row], SPI_tuptable->tupdesc,
									 4, &isnull));
					bool		found;

					entry = (TrackedWorker *) hash_search(tracked, &subscriptionId,
														  HASH_FIND, &found);

					if (found)
					{
						pid_t		workerPid;
						BgwHandleStatus liveness = GetBackgroundWorkerPid(
							entry->handle, &workerPid);

						/*
						 * BGWH_STOPPED means the worker process is gone --
						 * either it crashed or (see
						 * lake_replication_worker_main) it always
						 * exits cleanly regardless, by design, precisely so
						 * that decision belongs to this launcher and not to
						 * Postgres's own bgw_restart_time. Forget the stale
						 * handle so the branch below registers a fresh one
						 * this same cycle if still wanted.
						 */
						if (liveness == BGWH_STOPPED)
						{
							hash_search(tracked, &subscriptionId, HASH_REMOVE, NULL);
							found = false;
						}
						else if (entry->configGeneration != configGeneration)
						{
							TerminateBackgroundWorker(entry->handle);
							hash_search(tracked, &subscriptionId, HASH_REMOVE, NULL);
							ereport(LOG,
									(errmsg("lake_replication launcher: "
											"restarting supervisor for subscription_id=%lld "
											"after launch configuration changed",
											(long long) subscriptionId)));
							continue;
						}
					}

					if (strcmp(desiredState, "running") == 0)
					{
						if (found)
							entry->seenThisCycle = true;
						else
						{
							BackgroundWorker worker;
							BackgroundWorkerHandle *handle;

							memset(&worker, 0, sizeof(worker));
							snprintf(worker.bgw_name, BGW_MAXLEN,
									"lake_replication supervisor %lld",
									(long long) subscriptionId);
							snprintf(worker.bgw_type, BGW_MAXLEN,
									"lake_replication supervisor");
							worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
								BGWORKER_BACKEND_DATABASE_CONNECTION;
							worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

							/*
							 * Never Postgres's own restart: this launcher's
							 * own liveness check above is what restarts a
							 * dead supervisor, every launcher_naptime_ms --
							 * see the fork/waitpid comment in
							 * lake_replication_worker_main for why
							 * bgw_restart_time must not also be in play here.
							 */
							worker.bgw_restart_time = BGW_NEVER_RESTART;
							snprintf(worker.bgw_library_name, BGW_MAXLEN,
									"lake_replication");
							snprintf(worker.bgw_function_name, BGW_MAXLEN,
									"lake_replication_worker_main");
							worker.bgw_main_arg = Int64GetDatum(subscriptionId);
							strlcpy(worker.bgw_extra, destinationDatabase,
									BGW_EXTRALEN);
							worker.bgw_notify_pid = 0;

							if (RegisterDynamicBackgroundWorker(&worker, &handle))
							{
								entry = (TrackedWorker *) hash_search(
									tracked, &subscriptionId, HASH_ENTER, NULL);
								entry->subscriptionId = subscriptionId;
								entry->configGeneration = configGeneration;
								entry->handle = handle;
								entry->seenThisCycle = true;
								ereport(LOG,
										(errmsg("lake_replication launcher: "
												"started supervisor for "
												"subscription_id=%lld",
												(long long) subscriptionId)));
							}
							else
								ereport(WARNING,
										(errmsg("lake_replication launcher: "
												"no free background worker slot for "
												"subscription_id=%lld (max_worker_processes "
												"too low?)", (long long) subscriptionId)));
						}
					}
					else if (found)
					{
						TerminateBackgroundWorker(entry->handle);
						hash_search(tracked, &subscriptionId, HASH_REMOVE, NULL);
						ereport(LOG,
								(errmsg("lake_replication launcher: stopped "
										"supervisor for subscription_id=%lld "
										"(desired_state = stopped)",
										(long long) subscriptionId)));
					}
				}
			}

			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();

			/* Sweep anything not seen this cycle: its row was dropped outright. */
			hash_seq_init(&seqStatus, tracked);
			while ((entry = (TrackedWorker *) hash_seq_search(&seqStatus)) != NULL)
			{
				if (!entry->seenThisCycle)
				{
					int64		subscriptionId = entry->subscriptionId;

					TerminateBackgroundWorker(entry->handle);
					hash_search(tracked, &subscriptionId, HASH_REMOVE, NULL);
					ereport(LOG,
							(errmsg("lake_replication launcher: stopped "
									"supervisor for subscription_id=%lld "
									"(worker_launch_config row is gone)",
									(long long) subscriptionId)));
				}
			}
		}
		PG_CATCH();
		{
			ErrorData  *edata;
			MemoryContext oldContext;

			/*
			 * The transaction abort below deletes its memory context, while
			 * FlushErrorState deletes ErrorContext.  Copy into the launcher's
			 * long-lived context before either operation so the diagnostic does
			 * not become a dangling pointer (and crash the postmaster when the
			 * extension schema has not been created yet).
			 */
			oldContext = MemoryContextSwitchTo(TopMemoryContext);
			edata = CopyErrorData();
			MemoryContextSwitchTo(oldContext);

			FlushErrorState();
			AbortCurrentTransaction();
			ereport(WARNING,
					(errmsg("lake_replication launcher: cycle failed: %s",
							edata->message)));
			FreeErrorData(edata);
		}
		PG_END_TRY();

		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 LauncherNaptimeMs, 0);
		ResetLatch(MyLatch);
	}

	proc_exit(0);
}

/*
 * The source slot name lives in pending_cleanup; the reusable connection and
 * parsed authentication data live separately in source_connections. DROP
 * SUBSCRIPTION needs both because its parse node carries no conninfo.
 *
 * The original CREATE/ALTER statement may appear in the server log or
 * pg_stat_activity. Internal SPI calls use bound values, and the protected
 * source_connections stores the conninfo for cleanup plus parsed credentials
 * for worker reconnects. Must be called with SPI already connected.
 */
static void
record_source_connection(int64 subscriptionId, const char *conninfo,
					 const char *sourceUser, bool replace)
{
	PQconninfoOption *options;
	PQconninfoOption *option;
	char *errorMessage = NULL;
	char *password = NULL;
	Oid argtypes[4] = {INT8OID, TEXTOID, TEXTOID, TEXTOID};
	Datum values[4];
	char nulls[4] = {' ', ' ', ' ', ' '};
	int result;

	options = PQconninfoParse(conninfo, &errorMessage);
	if (options == NULL)
	{
		if (errorMessage != NULL)
			PQfreemem(errorMessage);
		ereport(ERROR, (errmsg("lake_replication: invalid source CONNECTION string")));
	}
	for (option = options; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, "password") == 0 && option->val != NULL)
		{
			password = pstrdup(option->val);
			break;
		}
	}
	PQconninfoFree(options);
	values[0] = Int64GetDatum(subscriptionId);
	values[1] = CStringGetTextDatum(conninfo);
	values[2] = CStringGetTextDatum(sourceUser);
	values[3] = password == NULL ? (Datum) 0 : CStringGetTextDatum(password);
	if (password == NULL)
		nulls[3] = 'n';
	if (replace)
	{
		result = SPI_execute_with_args(
			"UPDATE lake_replication.source_connections SET conninfo = $2, "
			"source_user = $3, source_password = $4 "
			"WHERE subscription_id = $1",
			4, argtypes, values, nulls, false, 0);
		if (result != SPI_OK_UPDATE ||
			SPI_processed != 1)
			ereport(ERROR, (errmsg("lake_replication: source credential row is unavailable")));
	}
	else
	{
		result = SPI_execute_with_args(
			"INSERT INTO lake_replication.source_connections "
			"(subscription_id, conninfo, source_user, source_password) "
			"VALUES ($1, $2, $3, $4)",
			4, argtypes, values, nulls, false, 0);
		if (result != SPI_OK_INSERT || SPI_processed != 1)
			ereport(ERROR, (errmsg("lake_replication: could not store source credentials")));
	}
	if (password != NULL)
	{
		MemSet(password, 0, strlen(password));
		pfree(password);
	}
}

static void
record_pending_cleanup(int64 subscriptionId, const char *slotName)
{
	Oid argtypes[2] = {INT8OID, TEXTOID};
	Datum values[2] = {Int64GetDatum(subscriptionId), CStringGetTextDatum(slotName)};

	if (SPI_execute_with_args(
		"INSERT INTO lake_replication.pending_cleanup "
		"(subscription_id, source_slot_name) VALUES ($1, $2)",
		2, argtypes, values, NULL, false, 0) != SPI_OK_INSERT ||
		SPI_processed != 1)
		ereport(ERROR, (errmsg("lake_replication: could not record source slot cleanup")));
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
			"SELECT c.conninfo, p.source_slot_name FROM "
			"lake_replication.pending_cleanup AS p "
			"JOIN lake_replication.source_connections AS c USING (subscription_id) "
			"WHERE p.subscription_id = %lld",
			(long long) subscriptionId), true, 1) == SPI_OK_SELECT
		&& SPI_processed == 1)
	{
		conninfo = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		slotName = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);
	}

	if (conninfo != NULL && slotName != NULL)
	{
		char	   *slotLiteral = quote_literal_cstr(slotName);
		char	   *innerQuery = psprintf(
			"DO $lake_replication_drop_slot$ BEGIN "
			"IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s) "
			"THEN PERFORM pg_drop_replication_slot(%s); END IF; "
			"END $lake_replication_drop_slot$",
			slotLiteral, slotLiteral);
		StringInfoData buf;
		Oid			argtypes[2] = {TEXTOID, TEXTOID};
		Datum		values[2];
		MemoryContext oldContext = CurrentMemoryContext;
		bool		dropped = false;
		int			attempt;

		initStringInfo(&buf);
		appendStringInfoString(&buf, "SELECT dblink_exec($1, $2)");
		values[0] = CStringGetTextDatum(conninfo);
		values[1] = CStringGetTextDatum(innerQuery);

		/*
		 * The receiver may still hold this slot open for up to a couple of
		 * hundred ms after the cooperative stop request above: it only
		 * notices desired_state on its own poll interval
		 * (LAKE_SUPERVISOR_POLL_MS / LAKE_RECEIVER_IDLE_TIMEOUT_MS, see
		 * build_worker_env), and handle_drop_subscription no longer waits
		 * for the supervisor process to actually exit before reaching here
		 * (see the comment there). pg_drop_replication_slot raises a real
		 * ERROR while the slot is still active, not a mere warning, so
		 * retrying needs a subtransaction (the standard PL/pgSQL-EXCEPTION-
		 * style pattern) to survive that ERROR without aborting the whole
		 * DROP SUBSCRIPTION statement.
		 */
		for (attempt = 0; attempt < 20 && !dropped; attempt++)
		{
			if (attempt > 0)
				pg_usleep(300000); /* 300ms; up to ~6s total */

			BeginInternalSubTransaction("lake_replication_drop_slot");
			PG_TRY();
			{
				SPI_execute_with_args(buf.data, 2, argtypes, values, NULL,
								  false, 0);
				ReleaseCurrentSubTransaction();
				dropped = true;
			}
			PG_CATCH();
			{
				RollbackAndReleaseCurrentSubTransaction();
				FlushErrorState();
			}
			PG_END_TRY();
			MemoryContextSwitchTo(oldContext);
		}

		if (!dropped)
			ereport(WARNING,
					(errmsg("lake_replication: could not drop "
							"source-side slot \"%s\" after retrying for ~6s; "
							"it is left behind", slotName)));
	}

	SPI_execute(psprintf(
		"DELETE FROM lake_replication.pending_cleanup "
		"WHERE subscription_id = %lld", (long long) subscriptionId), false, 0);
	SPI_execute(psprintf(
		"DELETE FROM lake_replication.source_connections "
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
		elog(ERROR, "lake_replication: could not determine "
					"current_database()/port");
	*outDatabase = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
	*outAddress = psprintf("127.0.0.1:%s",
						   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2));
}

/*
 * record_worker_launch_config writes (or, for ALTER ... ENABLE,
 * overwrites) this subscription's non-secret launch configuration into
 * lake_replication.worker_launch_config with desired_state =
 * 'running'. This is the only thing that actually starts the supervisor:
 * the launcher background worker (lake_replication_launcher_main)
 * polls this table and reacts, typically within
 * lake_replication.launcher_naptime_ms. Must be called with SPI
 * already connected.
 */
static void
record_worker_launch_config(int64 subscriptionId, const char *slotName,
							const char *publicationName, const char *sourceDatabase,
							const char *destinationDatabase, const char *sourceAddress,
							const char *destinationAddress, const char *destinationSchema,
							const char *credentialFile, bool createSlot)
{
	char	   *credentialSql = credentialFile == NULL || credentialFile[0] == '\0'
		? pstrdup("NULL") : quote_literal_cstr(credentialFile);

	SPI_execute(psprintf(
		"INSERT INTO lake_replication.worker_launch_config "
		"(subscription_id, slot_name, publication_name, source_database, "
		"destination_database, source_address, destination_address, "
		"destination_schema, credential_file, create_slot, desired_state) "
		"VALUES (%lld, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'running') "
		"ON CONFLICT (subscription_id) DO UPDATE SET "
		"slot_name = EXCLUDED.slot_name, "
		"publication_name = EXCLUDED.publication_name, "
		"source_database = EXCLUDED.source_database, "
		"destination_database = EXCLUDED.destination_database, "
		"source_address = EXCLUDED.source_address, "
		"destination_address = EXCLUDED.destination_address, "
		"destination_schema = EXCLUDED.destination_schema, "
		"credential_file = EXCLUDED.credential_file, "
		"create_slot = EXCLUDED.create_slot, "
		"config_generation = "
		"lake_replication.worker_launch_config.config_generation + 1, "
		"desired_state = 'running'",
		(long long) subscriptionId,
		quote_literal_cstr(slotName), quote_literal_cstr(publicationName),
		quote_literal_cstr(sourceDatabase), quote_literal_cstr(destinationDatabase),
		quote_literal_cstr(sourceAddress), quote_literal_cstr(destinationAddress),
		quote_literal_cstr(destinationSchema), credentialSql,
		createSlot ? "true" : "false"), false, 0);
}

static void
handle_create_subscription(CreateSubscriptionStmt *stmt)
{
	StringInfoData buf;
	ListCell   *lc;
	char	   *pubname;
	char	   *systemIdentifier;
	char	   *databaseOid;
	char	   *timeline;
	char	   *sourceDatabase;
	char	   *destinationDatabase;
	char	   *sourceAddress;
	char	   *sourceUser;
	char	   *destinationAddress;
	char	   *destinationSchema;
	char	   *credentialFile;
	char	   *slotName;
	bool		createSlot;
	int64		subscriptionId;

	RequireAuthorized("CREATE SUBSCRIPTION ... WITH (lake_target = 'iceberg')");

	/* Do not silently accept core options that this worker path cannot honor. */
	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "lake_target") != 0 &&
			strcmp(def->defname, "lake_target_schema") != 0 &&
			strcmp(def->defname, "lake_credential_file") != 0 &&
			strcmp(def->defname, "create_slot") != 0 &&
			strcmp(def->defname, "slot_name") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("lake_replication: CREATE SUBSCRIPTION option \"%s\" is not supported for Iceberg subscriptions",
							def->defname),
					 errdetail("This path performs an initial copy after the creating transaction commits.")));
	}

	if (list_length(stmt->publication) != 1)
		ereport(ERROR,
				(errmsg("lake_replication: CREATE SUBSCRIPTION \"%s\" names "
						"%d publications, but this project's worker binaries only "
						"support exactly one (lake-replication-initial-sync matches "
						"a single publication name by equality; see initial_sync.rs)",
						stmt->subname, list_length(stmt->publication))));
	pubname = strVal(linitial(stmt->publication));
	createSlot = get_bool_option(stmt->options, "create_slot", true);
	slotName = get_string_option(stmt->options, "slot_name");
	if (slotName == NULL)
		slotName = stmt->subname;
	if (pg_strcasecmp(slotName, "none") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lake_replication: slot_name = NONE is not supported for Iceberg subscriptions")));
	destinationSchema = get_string_option(stmt->options, "lake_target_schema");
	if (destinationSchema == NULL)
		destinationSchema = "public";
	credentialFile = get_string_option(stmt->options, "lake_credential_file");
	if (credentialFile == NULL)
		credentialFile = CredentialFile;
	require_absolute_credential_file(credentialFile);

	SPI_connect();

	discover_source_identity(stmt->conninfo, &systemIdentifier, &databaseOid,
							 &timeline, &sourceDatabase, &sourceAddress,
							 &sourceUser);
	discover_destination(&destinationDatabase, &destinationAddress);

	ereport(NOTICE,
			(errmsg("lake_replication: intercepted CREATE SUBSCRIPTION \"%s\" "
					"(publication=%s, source system_identifier=%s "
					"database_oid=%s timeline=%s); routing to "
					"lake_replication.create_subscription instead of native apply",
					stmt->subname, pubname,
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
		elog(ERROR, "lake_replication: create_subscription call failed");
	{
		bool		isnull;

		subscriptionId = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
													 SPI_tuptable->tupdesc, 1,
													 &isnull));
	}

	record_pending_cleanup(subscriptionId, slotName);
	record_source_connection(subscriptionId, stmt->conninfo, sourceUser, false);
	record_worker_launch_config(subscriptionId, slotName, pubname, sourceDatabase,
								destinationDatabase, sourceAddress, destinationAddress,
								destinationSchema, credentialFile, createSlot);

	ereport(NOTICE,
			(errmsg("lake_replication: recorded launch configuration for "
					"subscription_id=%lld; the launcher will start its supervisor "
					"within lake_replication.launcher_naptime_ms",
					(long long) subscriptionId)));

	SPI_finish();
}

static void
handle_drop_subscription(DropSubscriptionStmt *stmt, int64 subscriptionId)
{
	StringInfoData buf;

	RequireAuthorized("DROP SUBSCRIPTION");

	ereport(NOTICE,
			(errmsg("lake_replication: intercepted DROP SUBSCRIPTION \"%s\"; "
					"tearing down lake_replication control-plane state instead of "
					"native apply", stmt->subname)));

	if (!ActiveSnapshotSet())
		PushActiveSnapshot(GetTransactionSnapshot());

	SPI_connect();

	/*
	 * Ask the running supervisor to stop cooperatively first (it polls
	 * desired_state and, on noticing 'stopped', calls
	 * report_service_stopped() and exits cleanly -- exit(0), no signal,
	 * which matters in a container where PostgreSQL runs as PID 1: a
	 * process that later dies BY SIGNAL there is reparented straight onto
	 * the postmaster, which can restart the entire cluster out of
	 * caution). Deleting the worker_launch_config row below is the
	 * authoritative stop signal, though, not this: the launcher
	 * (lake_replication_launcher_main) notices the row is gone
	 * within its own next cycle and calls TerminateBackgroundWorker
	 * regardless of whether the cooperative stop above finished in time.
	 * Unlike the old fork/exec design, this statement never waits for the
	 * worker process to actually exit -- the launcher does that
	 * asynchronously, which is also why this no longer needs
	 * PreventInTransactionBlock: there is no partial commit to protect.
	 */
	SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
						  (long long) subscriptionId), false, 0);
	SPI_execute(psprintf(
		"DELETE FROM lake_replication.worker_launch_config "
		"WHERE subscription_id = %lld", (long long) subscriptionId), false, 0);

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
		elog(ERROR, "lake_replication: failed to tear down subscription state");

	SPI_finish();
	PopActiveSnapshot();
}

/*
 * handle_enable_subscription restarts replication for a subscription that
 * was previously disabled: flips worker_launch_config.desired_state back
 * to 'running'. The launcher notices within its own next cycle and
 * registers a fresh supervisor -- a genuinely new OS process, since
 * ALTER ... DISABLE had the launcher TerminateBackgroundWorker the old
 * one, which permanently retires that registration (see
 * lake_replication_launcher_main). No dblink round trip and no
 * source rediscovery needed here any more: source_address and every other
 * value the supervisor needs were already recorded durably in
 * worker_launch_config at CREATE SUBSCRIPTION time. If initial sync did not
 * finish before DISABLE, the restarted supervisor retries it after ENABLE;
 * completed subscriptions only resume streaming. Nothing here depends on
 * the source being reachable at ALTER time.
 */
static void
handle_enable_subscription(const char *subname, int64 subscriptionId)
{
	if (SPI_execute(psprintf(
			"UPDATE lake_replication.worker_launch_config "
			"SET desired_state = 'running' WHERE subscription_id = %lld",
			(long long) subscriptionId), false, 0) != SPI_OK_UPDATE ||
		SPI_processed != 1)
		ereport(ERROR,
				(errmsg("lake_replication: cannot restart \"%s\": no "
						"recorded launch configuration for subscription_id=%lld "
						"(it predates this extension's worker_launch_config "
						"tracking)", subname, (long long) subscriptionId)));
}

static void
handle_credential_file_update(const char *subname, int64 subscriptionId,
							  const char *credentialFile)
{
	char	   *credentialSql;

	require_absolute_credential_file(credentialFile);
	credentialSql = credentialFile[0] == '\0'
		? pstrdup("NULL") : quote_literal_cstr(credentialFile);

	/*
	 * A cooperative stop lets the current supervisor release its advisory
	 * lock and all connections before the launcher replaces its wrapper
	 * process.  Incrementing config_generation is the launcher's durable
	 * signal to start the replacement with freshly read credentials.
	 */
	SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
						  (long long) subscriptionId), false, 0);
	if (SPI_execute(psprintf(
			"UPDATE lake_replication.worker_launch_config "
			"SET credential_file = %s, config_generation = config_generation + 1 "
			"WHERE subscription_id = %lld",
			credentialSql, (long long) subscriptionId), false, 0) != SPI_OK_UPDATE ||
		SPI_processed != 1)
		ereport(ERROR,
				(errmsg("lake_replication: cannot reload credentials for \"%s\": "
						"no recorded launch configuration for subscription_id=%lld",
						subname, (long long) subscriptionId)));

	ereport(NOTICE,
			(errmsg("lake_replication: credential configuration for \"%s\" "
					"updated; a running supervisor will be restarted", subname)));
}

static void
handle_connection_update(const char *subname, int64 subscriptionId,
					 const char *conninfo)
{
	char *systemIdentifier;
	char *databaseOid;
	char *timeline;
	char *sourceDatabase;
	char *sourceAddress;
	char *sourceUser;

	discover_source_identity(conninfo, &systemIdentifier, &databaseOid,
							 &timeline, &sourceDatabase, &sourceAddress,
							 &sourceUser);
	/* A new cluster or database cannot safely resume from this checkpoint. */
	if (SPI_execute(psprintf(
		"SELECT 1 FROM lake_replication.subscriptions AS s "
		"JOIN lake_replication.sources AS src USING (source_id) "
		"WHERE s.subscription_id = %lld AND src.system_identifier = %s::numeric "
		"AND src.database_oid = %s::oid",
		(long long) subscriptionId, systemIdentifier, databaseOid),
		true, 1) != SPI_OK_SELECT || SPI_processed != 1)
		ereport(ERROR,
				(errmsg("lake_replication: ALTER SUBSCRIPTION CONNECTION for \"%s\" "
						"must point to the original source cluster and database",
						subname)));

	SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
							  (long long) subscriptionId), false, 0);
	record_source_connection(subscriptionId, conninfo, sourceUser, true);
	if (SPI_execute(psprintf(
		"UPDATE lake_replication.worker_launch_config "
		"SET source_database = %s, source_address = %s, "
		"config_generation = config_generation + 1 "
		"WHERE subscription_id = %lld",
		quote_literal_cstr(sourceDatabase), quote_literal_cstr(sourceAddress),
		(long long) subscriptionId), false, 0) != SPI_OK_UPDATE ||
		SPI_processed != 1)
		ereport(ERROR,
				(errmsg("lake_replication: launch configuration for \"%s\" is unavailable",
						subname)));

	ereport(NOTICE,
				(errmsg("lake_replication: source connection for \"%s\" updated; "
						"a running supervisor will be restarted", subname)));
}

static void
handle_alter_subscription(AlterSubscriptionStmt *stmt, int64 subscriptionId)
{
	RequireAuthorized("ALTER SUBSCRIPTION");

	SPI_connect();

	if (stmt->kind == ALTER_SUBSCRIPTION_CONNECTION)
		handle_connection_update(stmt->subname, subscriptionId, stmt->conninfo);
	else if (stmt->kind == ALTER_SUBSCRIPTION_OPTIONS)
	{
		char	   *credentialFile;

		if (list_length(stmt->options) != 1 ||
			(credentialFile = get_string_option(stmt->options,
											 "lake_credential_file")) == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("lake_replication: ALTER SUBSCRIPTION SET only "
							"supports lake_credential_file for Iceberg subscriptions")));
		handle_credential_file_update(stmt->subname, subscriptionId,
								  credentialFile);
	}
	else if (stmt->kind == ALTER_SUBSCRIPTION_ENABLED)
	{
		bool		enabled = get_bool_option(stmt->options, "enabled", true);

		if (!enabled)
		{
			ereport(NOTICE,
					(errmsg("lake_replication: intercepted ALTER SUBSCRIPTION "
							"\"%s\" DISABLE; requesting a graceful stop", stmt->subname)));
			SPI_execute(psprintf("SELECT lake_replication.request_service_stop(%lld)",
								  (long long) subscriptionId), false, 0);
			/*
			 * Authoritative stop signal for the launcher (see
			 * handle_drop_subscription for why this, not the cooperative
			 * request above, is what actually guarantees the process goes
			 * away): the launcher's next cycle sees desired_state <>
			 * 'running' for a tracked subscription and calls
			 * TerminateBackgroundWorker on it.
			 */
			SPI_execute(psprintf(
				"UPDATE lake_replication.worker_launch_config "
				"SET desired_state = 'stopped' WHERE subscription_id = %lld",
				(long long) subscriptionId), false, 0);
		}
		else
		{
			ereport(NOTICE,
					(errmsg("lake_replication: intercepted ALTER SUBSCRIPTION "
							"\"%s\" ENABLE; restarting the supervisor", stmt->subname)));
			handle_enable_subscription(stmt->subname, subscriptionId);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lake_replication: ALTER SUBSCRIPTION \"%s\" kind %d "
						"is not supported for Iceberg subscriptions",
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
	DefineCustomStringVariable("lake_replication.log_directory",
							   "Directory spawned worker processes' stdout/stderr are redirected to.",
							   NULL,
							   &LogDirectory,
							   "/tmp",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	/*
	 * PGC_POSTMASTER: only meaningful to the launcher this same _PG_init
	 * call registers below (a static background worker, started once at
	 * postmaster startup, requiring shared_preload_libraries), so reading
	 * it later or in a different session would be misleading.
	 */
	/*
	 * PGC_SIGHUP, not PGC_POSTMASTER: this value only ever actually takes
	 * effect at the launcher's own startup (registered below, once, at
	 * postmaster start), so PGC_POSTMASTER would describe its real-world
	 * behavior more precisely -- but a value set via ALTER SYSTEM before
	 * this library has ever been loaded is held as a generic placeholder
	 * until some DefineCustom*Variable call gives it a real type and
	 * context, and PostgreSQL refuses to promote a placeholder into a
	 * PGC_POSTMASTER variable at this point in startup ("cannot create
	 * PGC_POSTMASTER variables after startup"), only into PGC_SIGHUP or
	 * lower. PGC_SIGHUP is otherwise inert here: reloading does not make
	 * the already-running launcher re-read it.
	 */
	DefineCustomStringVariable("lake_replication.database_name",
							   "Database the launcher background worker connects to and manages "
							   "subscriptions in. Must name the database pg_lake "
							   "and lake_replication are installed in. Unset: the launcher exits "
							   "immediately at startup and manages nothing (CREATE SUBSCRIPTION "
							   "still records launch configuration, but no supervisor ever starts). "
							   "Only takes effect at postmaster startup, despite the PGC_SIGHUP "
							   "context -- see the comment above this call.",
							   NULL,
							   &LauncherDatabaseName,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("lake_replication.credential_file",
							   "Default absolute path to the owner-only worker credential file. "
							   "The file may provide source/destination users, passwords, "
							   "and TLS settings. A subscription may override and reload it "
							   "with lake_credential_file.",
							   NULL,
							   &CredentialFile,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   check_credential_file, NULL, NULL);

	DefineCustomIntVariable("lake_replication.launcher_naptime_ms",
							"How often the launcher polls worker_launch_config for "
							"subscriptions to start or stop.",
							NULL,
							&LauncherNaptimeMs,
							1000, 50, 60000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	/*
	 * RegisterBackgroundWorker (the *static* registration API, as opposed
	 * to RegisterDynamicBackgroundWorker) is only legal while
	 * shared_preload_libraries is being processed at postmaster startup --
	 * calling it later, e.g. when a regular backend LOADs this library via
	 * session_preload_libraries or CREATE EXTENSION, would itself be an
	 * error. Guard on that explicitly: everything above (the ProcessUtility
	 * hook, the GUCs) still needs to work when loaded the ordinary way,
	 * with no launcher at all -- CREATE SUBSCRIPTION still records launch
	 * configuration in that case, it just never starts running until a
	 * launcher (from a properly configured shared_preload_libraries
	 * instance) notices it.
	 */
	if (process_shared_preload_libraries_in_progress)
	{
		BackgroundWorker worker;

		memset(&worker, 0, sizeof(worker));
		snprintf(worker.bgw_name, BGW_MAXLEN, "lake_replication launcher");
		snprintf(worker.bgw_type, BGW_MAXLEN, "lake_replication launcher");
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 5;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "lake_replication");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				"lake_replication_launcher_main");
		worker.bgw_main_arg = (Datum) 0;
		worker.bgw_notify_pid = 0;
		RegisterBackgroundWorker(&worker);
	}

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = lake_ProcessUtility;
}
