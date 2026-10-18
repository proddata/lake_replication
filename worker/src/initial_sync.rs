use lake_replication_worker::credentials::Settings;
use lake_replication_worker::logging::event as log_event;
use lake_replication_worker::wire::{Client, Error as WireError, TlsConfig, address_is_loopback};
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

fn main() {
    if let Err(error) = run() {
        log_event(
            "initial_sync",
            "ERROR",
            "process_failed",
            &error.to_string(),
            &[],
        );
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let mut sync_guard = Client::connect_secure(
        config.destination_address.as_str(),
        &config.destination_user,
        &config.destination_database,
        false,
        config.destination_password.as_deref(),
        config.destination_tls.as_ref(),
    )?;
    let lock_rows = sync_guard.simple_query(&format!(
        "SELECT pg_try_advisory_lock(hashtextextended({}, 0))::text",
        sql_literal(&format!(
            "lake_replication_initial_sync:{}",
            config.subscription_id
        ))
    ))?;
    if single_value(&lock_rows, "initial-sync lock")? != "true" {
        log_event(
            "initial_sync",
            "INFO",
            "already_running",
            "another initial copy owns this subscription",
            &[("subscription_id", config.subscription_id.to_string())],
        );
        return Ok(());
    }
    let reset_rows = sync_guard.simple_query(&format!(
        "SELECT lake_replication.reset_incomplete_initial_sync({})::text",
        config.subscription_id
    ))?;
    let reset_count = single_value(&reset_rows, "reset incomplete initial copy")?;
    if reset_count != "0" {
        log_event(
            "initial_sync",
            "WARN",
            "incomplete_copy_reset",
            &format!("discarded {reset_count} incomplete table copies before restart"),
            &[("subscription_id", config.subscription_id.to_string())],
        );
    }
    let mut destination = Client::connect_secure(
        config.destination_address.as_str(),
        &config.destination_user,
        &config.destination_database,
        false,
        config.destination_password.as_deref(),
        config.destination_tls.as_ref(),
    )?;

    let mut source = Client::connect_secure(
        config.source_address.as_str(),
        &config.source_user,
        &config.source_database,
        false,
        config.source_password.as_deref(),
        config.source_tls.as_ref(),
    )?;
    let persistent_slot_rows = source.simple_query(&format!(
        "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name={})::text",
        sql_literal(&config.slot)
    ))?;
    let persistent_slot_exists =
        single_value(&persistent_slot_rows, "persistent slot lookup")? == "true";
    let mut slot_owner = Client::connect_secure(
        config.source_address.as_str(),
        &config.source_user,
        &config.source_database,
        true,
        config.source_password.as_deref(),
        config.source_tls.as_ref(),
    )?;
    let slot_rows = if config.create_slot && !persistent_slot_exists {
        slot_owner.simple_query(&format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER {})",
            quote_identifier(&config.slot),
            config.slot_failover
        ))?
    } else {
        // The persistent slot was created separately, possibly on this same
        // server. A temporary slot supplies a fresh exported snapshot while
        // the existing slot retains WAL from before that snapshot.
        let sync_slot = format!(
            "lake_sync_{}_{}",
            config.subscription_id,
            std::process::id()
        );
        slot_owner.simple_query(&format!(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')",
            quote_identifier(&sync_slot)
        ))?
    };
    let slot_row = slot_rows.first().ok_or("slot creation returned no row")?;
    let consistent_lsn = field(slot_row, 1, "consistent_point")?.to_owned();
    let snapshot_name = field(slot_row, 2, "snapshot_name")?.to_owned();

    if persistent_slot_exists || !config.create_slot {
        validate_existing_slot(&mut source, &config, &consistent_lsn)?;
    }
    source.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")?;
    source.simple_query(&format!(
        "SET TRANSACTION SNAPSHOT {}",
        sql_literal(&snapshot_name)
    ))?;
    let mut plans = discover_tables(&mut source, &config)?;
    if plans.is_empty() {
        return Err("publication contains no eligible tables".into());
    }
    source.simple_query("COMMIT")?;

    for plan in &mut plans {
        let registered = destination.simple_query(&format!(
            "SELECT lake_replication.register_relation({}, {}, {}, {}, {}, {}::jsonb)::text",
            config.subscription_id,
            plan.relation_id,
            sql_literal(&plan.source.schema),
            sql_literal(&plan.source.relation),
            sql_literal(&plan.replica_identity),
            sql_literal(&plan.columns),
        ))?;
        plan.schema_version = single_value(&registered, "register_relation")?.parse()?;
        destination.simple_query(&format!(
            "SELECT lake_replication.register_type_contract({}, {}, {}, {}, {}::jsonb)",
            config.subscription_id,
            plan.relation_id,
            plan.schema_version,
            config.type_mapping_version,
            sql_literal(&plan.type_contract),
        ))?;
        destination.simple_query(&format!(
            "SELECT lake_replication.ensure_iceberg_destination({}, {}, {}, {}, {}, {})::text",
            config.subscription_id,
            plan.relation_id,
            plan.schema_version,
            sql_literal(&plan.destination.schema),
            sql_literal(&plan.destination.relation),
            config.create_missing_destinations,
        ))?;
        destination.simple_query(&format!(
            "SELECT lake_replication.begin_initial_copy({}, {}, {}, {}::regclass, {}::pg_lsn, {}, {})",
            config.subscription_id,
            plan.relation_id,
            plan.schema_version,
            sql_literal(&plan.destination.sql()),
            sql_literal(&consistent_lsn),
            sql_literal(&snapshot_name),
            config.overwrite_destination,
        ))?;
    }

    log_event(
        "initial_sync",
        "INFO",
        "snapshot_acquired",
        &format!(
            "snapshot={} consistent_lsn={} tables={}",
            snapshot_name,
            consistent_lsn,
            plans.len()
        ),
        &[
            ("subscription_id", config.subscription_id.to_string()),
            ("snapshot", snapshot_name.clone()),
            ("consistent_lsn", consistent_lsn.clone()),
            ("tables", plans.len().to_string()),
        ],
    );
    if config.pause_after_snapshot_ms > 0 {
        thread::sleep(Duration::from_millis(config.pause_after_snapshot_ms));
    }

    let mut total_rows = 0_u64;
    for plan in &plans {
        match copy_table_with_retry(
            &mut sync_guard,
            &mut destination,
            &config,
            plan,
            &snapshot_name,
            &consistent_lsn,
        ) {
            Ok(copied) => total_rows += copied,
            Err(error) => {
                for failed_plan in &plans {
                    mark_copy_failed(
                        &mut destination,
                        &config,
                        failed_plan.relation_id,
                        failed_plan.schema_version,
                        &error.to_string(),
                    )?;
                }
                return Err(error.into());
            }
        }
    }

    sync_guard.simple_query("SELECT 1")?;
    if persistent_slot_exists || !config.create_slot {
        // Check again on a fresh connection: the copy connection still uses
        // the exported snapshot and cannot see concurrent slot changes.
        let mut source_check = Client::connect_secure(
            config.source_address.as_str(),
            &config.source_user,
            &config.source_database,
            false,
            config.source_password.as_deref(),
            config.source_tls.as_ref(),
        )?;
        validate_existing_slot(&mut source_check, &config, &consistent_lsn)?;
    }
    let completed = destination.simple_query(&format!(
        "SELECT lake_replication.finish_initial_sync_group({}, {})::text",
        config.subscription_id,
        plans.len()
    ))?;
    let completed = single_value(&completed, "finish_initial_sync_group")?;
    log_event(
        "initial_sync",
        "INFO",
        "initial_copy_completed",
        &format!(
            "initial copy ready tables={} rows={}/{} consistent_lsn={}",
            plans.len(),
            completed,
            total_rows,
            consistent_lsn
        ),
        &[
            ("subscription_id", config.subscription_id.to_string()),
            ("tables", plans.len().to_string()),
            ("tables_completed", completed.to_owned()),
            ("rows", total_rows.to_string()),
            ("consistent_lsn", consistent_lsn.clone()),
        ],
    );
    Ok(())
}

fn copy_table_with_retry(
    sync_guard: &mut Client,
    destination: &mut Client,
    config: &Config,
    plan: &TablePlan,
    snapshot_name: &str,
    consistent_lsn: &str,
) -> Result<u64, WireError> {
    for attempt in 0..=config.table_copy_retries {
        let result = (|| {
            let mut source = Client::connect_secure(
                config.source_address.as_str(),
                &config.source_user,
                &config.source_database,
                false,
                config.source_password.as_deref(),
                config.source_tls.as_ref(),
            )?;
            source.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")?;
            source.simple_query(&format!(
                "SET TRANSACTION SNAPSHOT {}",
                sql_literal(snapshot_name)
            ))?;
            let copied = copy_table(&mut source, destination, config, plan)?;
            source.simple_query("COMMIT")?;
            Ok(copied)
        })();
        match result {
            Ok(copied) => return Ok(copied),
            Err(_error) if attempt < config.table_copy_retries => {
                // A lost guard means another worker may have taken over and
                // reset staging. Never reconnect and write in that case.
                sync_guard.simple_query("SELECT 1")?;
                log_event(
                    "initial_sync",
                    "WARN",
                    "table_copy_retry",
                    &format!(
                        "retrying table={} attempt={}",
                        plan.source.sql(),
                        attempt + 1
                    ),
                    &[
                        ("subscription_id", config.subscription_id.to_string()),
                        ("table", plan.source.sql()),
                        ("attempt", (attempt + 1).to_string()),
                    ],
                );
                // A COPY failure can leave either wire connection unusable.
                // The snapshot owner is independent and stays open, so both
                // sessions can be replaced without changing the copy point.
                *destination = Client::connect_secure(
                    config.destination_address.as_str(),
                    &config.destination_user,
                    &config.destination_database,
                    false,
                    config.destination_password.as_deref(),
                    config.destination_tls.as_ref(),
                )?;
                destination.simple_query(&format!(
                    "SELECT lake_replication.reset_initial_copy({}, {}, {})",
                    config.subscription_id, plan.relation_id, plan.schema_version
                ))?;
                destination.simple_query(&format!(
                    "SELECT lake_replication.begin_initial_copy({}, {}, {}, {}::regclass, {}::pg_lsn, {}, {})",
                    config.subscription_id,
                    plan.relation_id,
                    plan.schema_version,
                    sql_literal(&plan.destination.sql()),
                    sql_literal(consistent_lsn),
                    sql_literal(snapshot_name),
                    config.overwrite_destination,
                ))?;
                thread::sleep(Duration::from_millis(100 * u64::from(attempt + 1)));
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the retry loop always returns after its final attempt")
}

fn validate_existing_slot(
    source: &mut Client,
    config: &Config,
    consistent_lsn: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = source.simple_query(&format!(
        "SELECT plugin::text, slot_type, database::text, temporary::text, active::text, \
         COALESCE(wal_status, ''), COALESCE(invalidation_reason, ''), \
         COALESCE(confirmed_flush_lsn, '0/0')::text \
         FROM pg_replication_slots WHERE slot_name = {}",
        sql_literal(&config.slot)
    ))?;
    let row = rows.first().ok_or_else(|| {
        format!(
            "pre-created logical replication slot {} does not exist",
            config.slot
        )
    })?;
    if field(row, 0, "plugin")? != "pgoutput"
        || field(row, 1, "slot_type")? != "logical"
        || field(row, 2, "database")? != config.source_database
        || field(row, 3, "temporary")? != "false"
        || field(row, 4, "active")? != "false"
        || field(row, 5, "wal_status")? == "lost"
        || !field(row, 6, "invalidation_reason")?.is_empty()
    {
        return Err(format!(
            "pre-created slot {} is not a usable inactive persistent pgoutput slot for {}",
            config.slot, config.source_database
        )
        .into());
    }
    let confirmed = parse_slot_lsn(field(row, 7, "confirmed_flush_lsn")?)?;
    let snapshot = parse_slot_lsn(consistent_lsn)?;
    if confirmed > snapshot {
        return Err(format!(
            "pre-created slot {} has advanced beyond initial-copy snapshot {}",
            config.slot, consistent_lsn
        )
        .into());
    }
    Ok(())
}

fn parse_slot_lsn(value: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let (high, low) = value.split_once('/').ok_or("invalid source LSN")?;
    Ok((u64::from_str_radix(high, 16)? << 32) | u64::from_str_radix(low, 16)?)
}

fn copy_table(
    source: &mut Client,
    destination: &mut Client,
    config: &Config,
    plan: &TablePlan,
) -> Result<u64, WireError> {
    let mut decoder = HexCopyDecoder::default();
    let mut pending_rows = Vec::with_capacity(config.chunk_rows);
    let mut pending_bytes = 0_usize;
    let mut copied = 0_u64;
    let filter = if plan.row_filter.is_empty() {
        String::new()
    } else {
        format!(" WHERE ({})", plan.row_filter)
    };
    source.copy_out(
        &format!(
            "COPY (
                SELECT encode(convert_to((
                    SELECT jsonb_object_agg(entry.key, entry.value)
                      FROM jsonb_each(to_jsonb(source_row)) AS entry
                     WHERE entry.key = ANY({})
                )::text, 'UTF8'), 'hex')
                  FROM {} AS source_row{}
             ) TO STDOUT",
            plan.selected_names_sql,
            plan.source.sql(),
            filter,
        ),
        |data| {
            decoder.push(data, |row| {
                if !pending_rows.is_empty()
                    && pending_bytes.saturating_add(row.len()) > config.chunk_bytes
                {
                    stage_rows(
                        destination,
                        config,
                        plan.relation_id,
                        plan.schema_version,
                        &mut copied,
                        &mut pending_rows,
                        &mut pending_bytes,
                    )?;
                }
                pending_bytes = pending_bytes.saturating_add(row.len());
                pending_rows.push(row);
                if pending_rows.len() == config.chunk_rows || pending_bytes >= config.chunk_bytes {
                    stage_rows(
                        destination,
                        config,
                        plan.relation_id,
                        plan.schema_version,
                        &mut copied,
                        &mut pending_rows,
                        &mut pending_bytes,
                    )?;
                }
                Ok(())
            })
        },
    )?;
    decoder.finish()?;
    if !pending_rows.is_empty() {
        stage_rows(
            destination,
            config,
            plan.relation_id,
            plan.schema_version,
            &mut copied,
            &mut pending_rows,
            &mut pending_bytes,
        )?;
    }
    log_event(
        "initial_sync",
        "INFO",
        "table_copied",
        &format!("copied table={} rows={copied}", plan.source.sql()),
        &[
            ("subscription_id", config.subscription_id.to_string()),
            ("table", plan.source.sql()),
            ("rows", copied.to_string()),
        ],
    );
    Ok(copied)
}

fn discover_tables(
    source: &mut Client,
    config: &Config,
) -> Result<Vec<TablePlan>, Box<dyn std::error::Error>> {
    let (predicate, publication_join) = if config.discover_publication {
        (
            format!("pt.pubname = {}", sql_literal(&config.publication)),
            "JOIN pg_publication_tables pt
               ON pt.schemaname = n.nspname AND pt.tablename = c.relname
             JOIN pg_attribute a ON a.attrelid = c.oid
                                AND a.attnum > 0 AND NOT a.attisdropped
                                AND a.attname = ANY(pt.attnames)"
                .to_owned(),
        )
    } else {
        let table = QualifiedName::parse(
            config
                .source_table
                .as_deref()
                .ok_or("LAKE_SOURCE_TABLE is required outside discovery mode")?,
        )?;
        (
            format!(
                "n.nspname = {} AND c.relname = {}",
                sql_literal(&table.schema),
                sql_literal(&table.relation)
            ),
            "JOIN pg_attribute a ON a.attrelid = c.oid
                                AND a.attnum > 0 AND NOT a.attisdropped
             CROSS JOIN LATERAL (
                 SELECT n.nspname AS schemaname, c.relname AS tablename,
                        ARRAY(SELECT attname FROM pg_attribute
                               WHERE attrelid=c.oid AND attnum > 0 AND NOT attisdropped
                               ORDER BY attnum)::name[] AS attnames,
                        NULL::text AS rowfilter
             ) pt"
                .to_owned(),
        )
    };
    let rows = source.simple_query(&format!(
        "SELECT c.oid::text, c.relreplident::text,
                jsonb_agg(jsonb_build_object(
                    'flags', CASE WHEN EXISTS (
                        SELECT 1 FROM pg_index i
                         WHERE i.indrelid = c.oid
                           AND (i.indisprimary OR i.indisreplident)
                           AND a.attnum = ANY(i.indkey)
                    ) THEN 1 ELSE 0 END,
                    'name', a.attname,
                    'type_oid', a.atttypid::integer,
                    'type_modifier', a.atttypmod
                ) ORDER BY a.attnum)::text,
                n.nspname::text, c.relname::text,
                format('ARRAY[%s]::text[]',
                    string_agg(quote_literal(a.attname), ', ' ORDER BY a.attnum)),
                coalesce(pt.rowfilter, ''),
                jsonb_agg(jsonb_build_object(
                    'name', a.attname,
                    'source_type', format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'generated', a.attgenerated <> ''
                ) ORDER BY a.attnum)::text
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           {publication_join}
          WHERE c.relkind IN ('r', 'p') AND {predicate}
          GROUP BY c.oid, c.relreplident, n.nspname, c.relname, pt.rowfilter
          ORDER BY n.nspname, c.relname"
    ))?;

    let mut plans = Vec::with_capacity(rows.len());
    for row in rows {
        let source_name = QualifiedName {
            schema: field(&row, 3, "source schema")?.to_owned(),
            relation: field(&row, 4, "source relation")?.to_owned(),
        };
        let destination = if config.discover_publication {
            QualifiedName {
                schema: config
                    .destination_schema
                    .as_ref()
                    .ok_or("LAKE_DESTINATION_SCHEMA is required in discovery mode")?
                    .to_owned(),
                relation: format!(
                    "{}{}",
                    source_name.relation, config.destination_table_suffix
                ),
            }
        } else {
            QualifiedName::parse(
                config
                    .destination_table
                    .as_deref()
                    .ok_or("LAKE_DESTINATION_TABLE is required outside discovery mode")?,
            )?
        };
        plans.push(TablePlan {
            relation_id: field(&row, 0, "relation OID")?.parse()?,
            replica_identity: field(&row, 1, "replica identity")?.to_owned(),
            columns: field(&row, 2, "source columns")?.to_owned(),
            source: source_name,
            destination,
            selected_names_sql: field(&row, 5, "publication columns")?.to_owned(),
            row_filter: field(&row, 6, "publication row filter")?.to_owned(),
            type_contract: field(&row, 7, "source type contract")?.to_owned(),
            schema_version: 0,
        });
    }
    Ok(plans)
}

struct TablePlan {
    relation_id: u32,
    replica_identity: String,
    columns: String,
    source: QualifiedName,
    destination: QualifiedName,
    selected_names_sql: String,
    row_filter: String,
    type_contract: String,
    schema_version: i64,
}

fn stage_rows(
    destination: &mut Client,
    config: &Config,
    relation_id: u32,
    schema_version: i64,
    copied: &mut u64,
    rows: &mut Vec<String>,
    pending_bytes: &mut usize,
) -> Result<(), WireError> {
    let json_rows = json_array(rows);
    destination.simple_query(&format!(
        "SELECT lake_replication.append_initial_copy_rows({}, {}, {}, {}, {}::jsonb)::text",
        config.subscription_id,
        relation_id,
        schema_version,
        copied,
        sql_literal(&json_rows),
    ))?;
    *copied += rows.len() as u64;
    rows.clear();
    *pending_bytes = 0;
    log_event(
        "initial_sync",
        "INFO",
        "copy_chunk_staged",
        &format!("staged initial rows={copied}"),
        &[
            ("subscription_id", config.subscription_id.to_string()),
            ("relation_id", relation_id.to_string()),
            ("rows_copied", copied.to_string()),
        ],
    );
    if config.pause_after_chunk_ms > 0 {
        thread::sleep(Duration::from_millis(config.pause_after_chunk_ms));
    }
    if config.fail_after_rows > 0 && *copied >= config.fail_after_rows {
        return Err(WireError::Protocol(format!(
            "injected failure after {copied} rows"
        )));
    }
    if config.fail_once_after_rows > 0
        && *copied >= config.fail_once_after_rows
        && !config.fail_once_triggered.swap(true, Ordering::SeqCst)
    {
        return Err(WireError::Protocol(format!(
            "injected one-time failure after {copied} rows"
        )));
    }
    Ok(())
}

fn mark_copy_failed(
    destination: &mut Client,
    config: &Config,
    relation_id: u32,
    schema_version: i64,
    error: &str,
) -> Result<(), WireError> {
    destination.simple_query(&format!(
        "SELECT lake_replication.fail_initial_copy({}, {}, {}, {})",
        config.subscription_id,
        relation_id,
        schema_version,
        sql_literal(error),
    ))?;
    Ok(())
}

struct Config {
    source_address: String,
    destination_address: String,
    source_user: String,
    source_password: Option<String>,
    source_tls: Option<TlsConfig>,
    destination_user: String,
    destination_password: Option<String>,
    destination_tls: Option<TlsConfig>,
    source_database: String,
    destination_database: String,
    slot: String,
    publication: String,
    subscription_id: i64,
    source_table: Option<String>,
    destination_table: Option<String>,
    destination_schema: Option<String>,
    discover_publication: bool,
    create_missing_destinations: bool,
    type_mapping_version: i32,
    destination_table_suffix: String,
    overwrite_destination: bool,
    chunk_rows: usize,
    chunk_bytes: usize,
    fail_after_rows: u64,
    fail_once_after_rows: u64,
    fail_once_triggered: AtomicBool,
    pause_after_snapshot_ms: u64,
    pause_after_chunk_ms: u64,
    slot_failover: bool,
    create_slot: bool,
    table_copy_retries: u32,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let mut settings = Settings::from_env()?;
        let legacy_user = value("LAKE_USER", "postgres");
        let source_address = value("LAKE_SOURCE_ADDRESS", "127.0.0.1:55434");
        let destination_address = value("LAKE_DESTINATION_ADDRESS", "127.0.0.1:55434");
        let destination_user = settings.value("LAKE_DESTINATION_USER", &legacy_user);
        let destination_password = settings.optional("LAKE_DESTINATION_PASSWORD");
        let destination_tls = tls_config(&settings, "LAKE_DESTINATION", &[&destination_address])?;
        let destination_database = value("LAKE_DESTINATION_DATABASE", "analytics");
        let subscription_id: i64 = required("LAKE_SUBSCRIPTION_ID")?.parse()?;
        settings.load_subscription_source_credentials(
            subscription_id,
            &destination_address,
            &destination_user,
            &destination_database,
            destination_password.as_deref(),
            destination_tls.as_ref(),
        )?;
        let config = Self {
            source_tls: tls_config(&settings, "LAKE_SOURCE", &[&source_address])?,
            destination_tls,
            source_address,
            destination_address,
            source_user: settings.value("LAKE_SOURCE_USER", &legacy_user),
            source_password: settings.optional("LAKE_SOURCE_PASSWORD"),
            destination_user,
            destination_password,
            source_database: value("LAKE_SOURCE_DATABASE", "application"),
            destination_database,
            slot: required("LAKE_SLOT")?,
            publication: required("LAKE_PUBLICATION")?,
            subscription_id,
            source_table: env::var("LAKE_SOURCE_TABLE").ok(),
            destination_table: env::var("LAKE_DESTINATION_TABLE").ok(),
            destination_schema: env::var("LAKE_DESTINATION_SCHEMA").ok(),
            discover_publication: value("LAKE_DISCOVER_PUBLICATION", "0") == "1",
            create_missing_destinations: value("LAKE_CREATE_TABLES_USING_ICEBERG", "0") == "1",
            type_mapping_version: value("LAKE_TYPE_MAPPING_VERSION", "1").parse()?,
            destination_table_suffix: value("LAKE_DESTINATION_TABLE_SUFFIX", ""),
            overwrite_destination: value("LAKE_OVERWRITE_DESTINATION", "0") == "1",
            chunk_rows: value("LAKE_INITIAL_COPY_CHUNK_ROWS", "1000").parse()?,
            chunk_bytes: value("LAKE_INITIAL_COPY_CHUNK_BYTES", "8388608").parse()?,
            fail_after_rows: value("LAKE_INITIAL_COPY_FAIL_AFTER_ROWS", "0").parse()?,
            fail_once_after_rows: value("LAKE_INITIAL_COPY_FAIL_ONCE_AFTER_ROWS", "0").parse()?,
            fail_once_triggered: AtomicBool::new(false),
            pause_after_snapshot_ms: value("LAKE_INITIAL_COPY_PAUSE_MS", "0").parse()?,
            pause_after_chunk_ms: value("LAKE_INITIAL_COPY_CHUNK_PAUSE_MS", "0").parse()?,
            slot_failover: value("LAKE_SLOT_FAILOVER", "1") == "1",
            create_slot: value("LAKE_CREATE_SLOT", "1") == "1",
            table_copy_retries: value("LAKE_INITIAL_COPY_TABLE_RETRIES", "2").parse()?,
        };
        if config.chunk_rows == 0 || config.chunk_bytes == 0 || config.type_mapping_version <= 0 {
            return Err("initial-copy row and byte chunk limits must be positive".into());
        }
        Ok(config)
    }
}

fn tls_config(
    settings: &Settings,
    prefix: &str,
    addresses: &[&str],
) -> Result<Option<TlsConfig>, Box<dyn std::error::Error>> {
    let default_mode = if addresses.iter().all(|address| address_is_loopback(address)) {
        "disable"
    } else {
        "verify-full"
    };
    match settings
        .value(&format!("{prefix}_TLS_MODE"), default_mode)
        .as_str()
    {
        "disable" => Ok(None),
        "verify-full" => Ok(Some(TlsConfig::verify_full(
            settings.required(&format!("{prefix}_TLS_ROOT_CERT"))?,
            settings.optional(&format!("{prefix}_TLS_SERVER_NAME")),
        ))),
        mode => Err(format!(
            "unsupported {prefix}_TLS_MODE {mode}; expected disable or verify-full"
        )
        .into()),
    }
}

struct QualifiedName {
    schema: String,
    relation: String,
}

impl QualifiedName {
    fn parse(value: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let (schema, relation) = value
            .split_once('.')
            .ok_or("table name must be schema-qualified")?;
        if schema.is_empty() || relation.is_empty() || relation.contains('.') {
            return Err("table name must contain exactly one non-empty qualifier".into());
        }
        Ok(Self {
            schema: schema.to_owned(),
            relation: relation.to_owned(),
        })
    }

    fn sql(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.relation)
        )
    }
}

fn json_array(rows: &[String]) -> String {
    let mut result = String::from("[");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            result.push(',');
        }
        result.push_str(row);
    }
    result.push(']');
    result
}

#[derive(Default)]
struct HexCopyDecoder {
    pending: Vec<u8>,
}

impl HexCopyDecoder {
    fn push<F>(&mut self, bytes: &[u8], mut consume: F) -> Result<(), WireError>
    where
        F: FnMut(String) -> Result<(), WireError>,
    {
        self.pending.extend_from_slice(bytes);
        while let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=newline).collect();
            let text = decode_hex(&line[..line.len() - 1])?;
            consume(text)?;
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), WireError> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(WireError::Protocol(
                "COPY OUT ended with a partial row".into(),
            ))
        }
    }
}

fn decode_hex(bytes: &[u8]) -> Result<String, WireError> {
    if bytes.len() % 2 != 0 {
        return Err(WireError::Protocol("odd-length hex COPY row".into()));
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        decoded.push((high << 4) | low);
    }
    String::from_utf8(decoded).map_err(|_| WireError::Protocol("non-UTF-8 JSON row".into()))
}

fn hex_digit(byte: u8) -> Result<u8, WireError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(WireError::Protocol("invalid hex COPY row".into())),
    }
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("required environment variable {name} is not set").into())
}

fn value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn field<'a>(
    row: &'a [Option<String>],
    index: usize,
    name: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| format!("missing {name}").into())
}

fn single_value<'a>(
    rows: &'a [Vec<Option<String>>],
    operation: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    rows.first()
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .ok_or_else(|| format!("{operation} returned no value").into())
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('\"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_names_are_quoted() {
        let name = QualifiedName::parse("odd schema.a\"b").unwrap();
        assert_eq!(name.sql(), "\"odd schema\".\"a\"\"b\"");
    }

    #[test]
    fn malformed_qualified_names_are_rejected() {
        assert!(QualifiedName::parse("unqualified").is_err());
        assert!(QualifiedName::parse("a.b.c").is_err());
    }

    #[test]
    fn copy_rows_can_cross_wire_frames() {
        let mut decoder = HexCopyDecoder::default();
        let mut rows = Vec::new();
        decoder
            .push(b"7b2261223a", |row| {
                rows.push(row);
                Ok(())
            })
            .unwrap();
        decoder
            .push(b"317d\n7b2262223a226e5c5c6e227d\n", |row| {
                rows.push(row);
                Ok(())
            })
            .unwrap();
        decoder.finish().unwrap();
        assert_eq!(rows, [r#"{"a":1}"#, r#"{"b":"n\\n"}"#]);
    }
}
