use lake_replication_worker::credentials::Settings;
use lake_replication_worker::logging::event as log_event;
use lake_replication_worker::wire::{Client, Error as WireError, TlsConfig, address_is_loopback};
use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
enum CycleError {
    Retry(String),
    Fatal(String),
}

fn main() {
    if let Err(error) = run() {
        log_event(
            "supervisor",
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
    let executable_directory = env::current_exe()?
        .parent()
        .ok_or("supervisor executable has no parent directory")?
        .to_owned();
    let receiver = executable_directory.join(binary_name("lake-replication-receiver"));
    let publisher = executable_directory.join(binary_name("lake-replication-publisher"));
    require_binary(&receiver)?;
    require_binary(&publisher)?;

    let mut destination = config.connect()?;
    acquire_supervisor_lock(&mut destination, config.subscription_id)?;
    control_query(
        &mut destination,
        &config,
        &format!(
            "SELECT lake_replication.start_service({}, {})",
            config.subscription_id,
            std::process::id()
        ),
    )?;
    let mut consecutive_failures = 0_u32;
    let mut injected_failures = config.inject_failures;

    loop {
        let (desired_state, subscription_state) = service_state(&mut destination, &config)?;
        if desired_state == "stopped" {
            stop(&mut destination, &config)?;
            log_event(
                "supervisor",
                "INFO",
                "stopped",
                "graceful stop requested",
                &[("subscription_id", config.subscription_id.to_string())],
            );
            return Ok(());
        }
        if subscription_state != "streaming" {
            report_cycle(&mut destination, &config, "paused", 0, 0)?;
            thread::sleep(config.poll_interval);
            continue;
        }

        let cycle = if injected_failures > 0 {
            injected_failures -= 1;
            Err(CycleError::Retry(
                "lab-injected supervisor child failure".to_owned(),
            ))
        } else {
            run_cycle(&config, &receiver, &publisher, &mut destination)
        };
        match cycle {
            Ok((receiver_cycles, publisher_cycles)) => {
                consecutive_failures = 0;
                report_cycle(
                    &mut destination,
                    &config,
                    "running",
                    receiver_cycles,
                    publisher_cycles,
                )?;
                thread::sleep(config.poll_interval);
            }
            Err(CycleError::Fatal(error)) => {
                control_query(
                    &mut destination,
                    &config,
                    &format!(
                        "SELECT lake_replication.report_source_error({}, {})",
                        config.subscription_id,
                        sql_literal(&error)
                    ),
                )?;
                return Err(error.into());
            }
            Err(CycleError::Retry(error)) => {
                let delay = retry_delay(
                    config.base_backoff,
                    config.max_backoff,
                    consecutive_failures,
                );
                consecutive_failures = consecutive_failures.saturating_add(1);
                control_query(
                    &mut destination,
                    &config,
                    &format!(
                        "SELECT lake_replication.report_service_failure({}, {}, {})",
                        config.subscription_id,
                        sql_literal(&error),
                        delay.as_millis()
                    ),
                )?;
                log_event(
                    "supervisor",
                    "WARN",
                    "cycle_failed",
                    &format!(
                        "worker cycle failed; retrying in {}ms: {error}",
                        delay.as_millis()
                    ),
                    &[
                        ("subscription_id", config.subscription_id.to_string()),
                        ("retry_in_ms", delay.as_millis().to_string()),
                        ("consecutive_failures", consecutive_failures.to_string()),
                        ("error", error.clone()),
                    ],
                );
                if wait_for_retry_or_stop(&config, &mut destination, delay)? {
                    stop(&mut destination, &config)?;
                    log_event(
                        "supervisor",
                        "INFO",
                        "stopped",
                        "graceful stop requested during backoff",
                        &[("subscription_id", config.subscription_id.to_string())],
                    );
                    return Ok(());
                }
            }
        }
    }
}

fn acquire_supervisor_lock(
    destination: &mut Client,
    subscription_id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    if !try_acquire_supervisor_lock(destination, subscription_id)? {
        return Err(
            format!("subscription {subscription_id} already has an active supervisor").into(),
        );
    }
    Ok(())
}

fn control_query(
    destination: &mut Client,
    config: &Config,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, Box<dyn std::error::Error>> {
    loop {
        match destination.simple_query(sql) {
            Ok(rows) => return Ok(rows),
            Err(WireError::Io(error)) => {
                let connection_error = format!("destination control connection lost: {error}");
                log_event(
                    "supervisor",
                    "WARN",
                    "destination_connection_lost",
                    &connection_error,
                    &[("subscription_id", config.subscription_id.to_string())],
                );
                let mut failures = 0_u32;
                loop {
                    let delay = retry_delay(config.base_backoff, config.max_backoff, failures);
                    failures = failures.saturating_add(1);
                    thread::sleep(delay);
                    let mut replacement = match config.connect() {
                        Ok(client) => client,
                        Err(error) => {
                            log_event(
                                "supervisor",
                                "WARN",
                                "destination_reconnect_failed",
                                &format!(
                                    "destination reconnect failed; retrying in {}ms: {error}",
                                    delay.as_millis()
                                ),
                                &[
                                    ("subscription_id", config.subscription_id.to_string()),
                                    ("retry_in_ms", delay.as_millis().to_string()),
                                    ("error", error.to_string()),
                                ],
                            );
                            continue;
                        }
                    };
                    match try_acquire_supervisor_lock(&mut replacement, config.subscription_id) {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(format!(
                                "subscription {} was claimed by another supervisor during reconnect",
                                config.subscription_id
                            )
                            .into());
                        }
                        Err(error) => {
                            log_event(
                                "supervisor",
                                "WARN",
                                "destination_lock_check_failed",
                                &format!(
                                    "destination reconnect lock check failed; retrying in {}ms: {error}",
                                    delay.as_millis()
                                ),
                                &[
                                    ("subscription_id", config.subscription_id.to_string()),
                                    ("retry_in_ms", delay.as_millis().to_string()),
                                    ("error", error.to_string()),
                                ],
                            );
                            continue;
                        }
                    }
                    match replacement.simple_query(&format!(
                        "SELECT lake_replication.report_service_reconnected({}, {})",
                        config.subscription_id,
                        sql_literal(&connection_error)
                    )) {
                        Ok(_) => {
                            *destination = replacement;
                            log_event(
                                "supervisor",
                                "INFO",
                                "destination_connection_restored",
                                "destination control connection restored",
                                &[("subscription_id", config.subscription_id.to_string())],
                            );
                            break;
                        }
                        Err(WireError::Io(error)) => {
                            log_event(
                                "supervisor",
                                "WARN",
                                "destination_reconnect_interrupted",
                                &format!("destination reconnect was interrupted: {error}"),
                                &[
                                    ("subscription_id", config.subscription_id.to_string()),
                                    ("error", error.to_string()),
                                ],
                            );
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn try_acquire_supervisor_lock(
    destination: &mut Client,
    subscription_id: i64,
) -> Result<bool, Box<dyn std::error::Error>> {
    let rows = destination.simple_query(&format!(
        "SELECT pg_try_advisory_lock(hashtextextended('lake-replication-supervisor:{subscription_id}', 0))::text"
    ))?;
    Ok(field(
        rows.first().ok_or("advisory lock query returned no row")?,
        0,
        "advisory lock",
    )? == "true")
}

#[derive(Debug)]
struct ExpectedSource {
    system_identifier: String,
    database_oid: u32,
    database_name: String,
    timeline: u32,
    staged_lsn: u64,
}

#[derive(Debug)]
struct SourceObservation {
    system_identifier: String,
    database_oid: u32,
    timeline: u32,
    plugin: String,
    slot_type: String,
    database: String,
    temporary: bool,
    active: bool,
    wal_status: Option<String>,
    invalidation_reason: Option<String>,
    conflicting: bool,
    failover: bool,
    confirmed_lsn: u64,
    current_lsn: u64,
}

fn source_preflight(destination: &mut Client, config: &Config) -> Result<String, CycleError> {
    let expected_rows = control_query(
        destination,
        config,
        &format!(
            "SELECT src.system_identifier::text, src.database_oid::text, src.timeline::text, c.staged_lsn::text FROM lake_replication.subscriptions sub JOIN lake_replication.sources src USING (source_id) JOIN lake_replication.checkpoints c USING (subscription_id) WHERE sub.subscription_id={}",
            config.subscription_id
        ),
    )
    .map_err(|error| CycleError::Fatal(error.to_string()))?;
    let expected_row = expected_rows
        .first()
        .ok_or_else(|| CycleError::Fatal("subscription source metadata disappeared".into()))?;
    let expected = ExpectedSource {
        system_identifier: cycle_field(expected_row, 0, "expected system identifier")?.to_owned(),
        database_oid: parse_cycle(cycle_field(expected_row, 1, "expected database OID")?)?,
        database_name: config.source_database.clone(),
        timeline: parse_cycle(cycle_field(expected_row, 2, "expected timeline")?)?,
        staged_lsn: parse_lsn(cycle_field(expected_row, 3, "staged LSN")?)
            .map_err(CycleError::Fatal)?,
    };

    let mut candidate_errors = Vec::new();
    let mut retryable_failure = false;
    for address in &config.source_addresses {
        match inspect_source_candidate(config, address, &expected) {
            Ok(observation) => {
                control_query(
                    destination,
                    config,
                    &format!(
                        "SELECT lake_replication.report_source_healthy({}, {}, {}, {}, {}::pg_lsn, {}::pg_lsn, {}, {})",
                        config.subscription_id,
                        observation.system_identifier,
                        observation.database_oid,
                        observation.timeline,
                        sql_literal(&format_lsn(observation.confirmed_lsn)),
                        sql_literal(&format_lsn(observation.current_lsn)),
                        sql_literal(address),
                        sql_literal(&candidate_errors.join(" | "))
                    ),
                )
                .map_err(|error| CycleError::Fatal(error.to_string()))?;
                return Ok(address.clone());
            }
            Err(CycleError::Retry(error)) => {
                retryable_failure = true;
                candidate_errors.push(format!("{address}: {error}"));
            }
            Err(CycleError::Fatal(error)) => {
                candidate_errors.push(format!("{address}: {error}"));
            }
        }
    }
    let error = format!(
        "no safe writable source endpoint: {}",
        candidate_errors.join(" | ")
    );
    if retryable_failure {
        Err(CycleError::Retry(error))
    } else {
        Err(CycleError::Fatal(error))
    }
}

fn inspect_source_candidate(
    config: &Config,
    address: &str,
    expected: &ExpectedSource,
) -> Result<SourceObservation, CycleError> {
    let mut source = config.connect_source(address).map_err(source_query_error)?;
    let identity_rows = source
        .simple_query(
            "SELECT (SELECT system_identifier::text FROM pg_control_system()), (SELECT oid::text FROM pg_database WHERE datname=current_database()), pg_is_in_recovery()::text",
        )
        .map_err(source_query_error)?;
    let identity = identity_rows
        .first()
        .ok_or_else(|| CycleError::Fatal("source identity query returned no row".into()))?;
    if parse_bool(cycle_field(identity, 2, "source recovery state")?)? {
        return Err(CycleError::Retry(
            "endpoint is a standby, not the writable leader".into(),
        ));
    }
    let timeline_rows = source
        .simple_query(
            "SELECT (('x' || substr(pg_walfile_name(pg_current_wal_flush_lsn()), 1, 8))::bit(32)::bigint)::text, pg_current_wal_flush_lsn()::text",
        )
        .map_err(source_query_error)?;
    let timeline = parse_cycle(cycle_field(
        timeline_rows
            .first()
            .ok_or_else(|| CycleError::Fatal("source timeline query returned no row".into()))?,
        0,
        "observed timeline",
    )?)?;
    let current_lsn = parse_lsn(cycle_field(
        timeline_rows
            .first()
            .ok_or_else(|| CycleError::Fatal("source timeline query returned no row".into()))?,
        1,
        "source current LSN",
    )?)
    .map_err(CycleError::Fatal)?;
    let slot_rows = source
        .simple_query(&format!(
            "SELECT plugin::text, slot_type, database::text, temporary::text, active::text, wal_status, invalidation_reason, conflicting::text, failover::text, COALESCE(confirmed_flush_lsn, '0/0')::text FROM pg_replication_slots WHERE slot_name={}",
            sql_literal(&config.slot)
        ))
        .map_err(source_query_error)?;
    let slot = slot_rows.first().ok_or_else(|| {
        CycleError::Fatal(format!(
            "logical replication slot {} is missing; controlled rebuild required",
            config.slot
        ))
    })?;
    let observation = SourceObservation {
        system_identifier: cycle_field(identity, 0, "observed system identifier")?.to_owned(),
        database_oid: parse_cycle(cycle_field(identity, 1, "observed database OID")?)?,
        timeline,
        plugin: cycle_field(slot, 0, "slot plugin")?.to_owned(),
        slot_type: cycle_field(slot, 1, "slot type")?.to_owned(),
        database: cycle_field(slot, 2, "slot database")?.to_owned(),
        temporary: parse_bool(cycle_field(slot, 3, "slot temporary")?)?,
        active: parse_bool(cycle_field(slot, 4, "slot active")?)?,
        wal_status: optional_cycle_field(slot, 5),
        invalidation_reason: optional_cycle_field(slot, 6),
        conflicting: parse_bool(cycle_field(slot, 7, "slot conflicting")?)?,
        failover: parse_bool(cycle_field(slot, 8, "slot failover")?)?,
        confirmed_lsn: parse_lsn(cycle_field(slot, 9, "slot confirmed LSN")?)
            .map_err(CycleError::Fatal)?,
        current_lsn,
    };
    let history = if observation.timeline > expected.timeline {
        let history_rows = source
            .simple_query(&format!(
                "SELECT pg_read_file('pg_wal/' || upper(lpad(to_hex({}), 8, '0')) || '.history')",
                observation.timeline
            ))
            .map_err(source_query_error)?;
        Some(
            cycle_field(
                history_rows.first().ok_or_else(|| {
                    CycleError::Fatal("timeline history query returned no row".into())
                })?,
                0,
                "timeline history",
            )?
            .to_owned(),
        )
    } else {
        None
    };
    validate_source(expected, &observation, history.as_deref()).map_err(CycleError::Fatal)?;
    Ok(observation)
}

fn validate_source(
    expected: &ExpectedSource,
    observed: &SourceObservation,
    timeline_history: Option<&str>,
) -> Result<(), String> {
    if observed.system_identifier != expected.system_identifier
        || observed.database_oid != expected.database_oid
    {
        return Err(format!(
            "source identity mismatch: expected system/database {}/{}, observed {}/{}",
            expected.system_identifier,
            expected.database_oid,
            observed.system_identifier,
            observed.database_oid
        ));
    }
    if observed.plugin != "pgoutput"
        || observed.slot_type != "logical"
        || observed.database != expected.database_name
        || observed.temporary
    {
        return Err("replication slot does not have the required durable pgoutput identity".into());
    }
    if observed.active {
        return Err("replication slot is already active outside this worker cycle".into());
    }
    if observed.conflicting
        || observed.invalidation_reason.is_some()
        || observed.wal_status.as_deref() == Some("lost")
    {
        return Err(format!(
            "replication slot is invalidated or has lost WAL: wal_status={:?} reason={:?}",
            observed.wal_status, observed.invalidation_reason
        ));
    }
    if observed.confirmed_lsn > expected.staged_lsn {
        return Err(format!(
            "source slot confirmed LSN {} is ahead of durable staged LSN {}",
            format_lsn(observed.confirmed_lsn),
            format_lsn(expected.staged_lsn)
        ));
    }
    if observed.timeline < expected.timeline {
        return Err(format!(
            "source timeline regressed from {} to {}",
            expected.timeline, observed.timeline
        ));
    }
    if observed.timeline > expected.timeline {
        if !observed.failover {
            return Err(
                "source timeline advanced but the logical slot is not failover-enabled".into(),
            );
        }
        let fork_lsn = ancestor_fork_lsn(
            timeline_history.ok_or("source timeline history is unavailable")?,
            expected.timeline,
        )?
        .ok_or_else(|| {
            format!(
                "timeline {} is not a documented descendant of {}",
                observed.timeline, expected.timeline
            )
        })?;
        if expected.staged_lsn > fork_lsn {
            return Err(format!(
                "new timeline forked at {} before durable staged LSN {}",
                format_lsn(fork_lsn),
                format_lsn(expected.staged_lsn)
            ));
        }
    }
    Ok(())
}

fn ancestor_fork_lsn(history: &str, ancestor: u32) -> Result<Option<u64>, String> {
    for line in history.lines() {
        let content = line.split('#').next().unwrap_or("").trim();
        if content.is_empty() {
            continue;
        }
        let mut fields = content.split_whitespace();
        let timeline = fields.next().ok_or("timeline history is malformed")?;
        let lsn = fields.next().ok_or("timeline history is malformed")?;
        let timeline = u32::from_str_radix(timeline, 16)
            .map_err(|_| "timeline history contains an invalid timeline")?;
        if timeline == ancestor {
            return parse_lsn(lsn).map(Some);
        }
    }
    Ok(None)
}

fn source_query_error(error: WireError) -> CycleError {
    match error {
        WireError::Io(_) => CycleError::Retry(format!("source connection failed: {error}")),
        _ => CycleError::Fatal(format!("source inspection failed: {error}")),
    }
}

fn cycle_field<'a>(
    row: &'a [Option<String>],
    index: usize,
    name: &str,
) -> Result<&'a str, CycleError> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| CycleError::Fatal(format!("missing {name}")))
}

fn optional_cycle_field(row: &[Option<String>], index: usize) -> Option<String> {
    row.get(index).and_then(|value| value.clone())
}

fn parse_cycle<T: std::str::FromStr>(value: &str) -> Result<T, CycleError> {
    value
        .parse()
        .map_err(|_| CycleError::Fatal(format!("invalid source metadata value {value}")))
}

fn parse_bool(value: &str) -> Result<bool, CycleError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(CycleError::Fatal(format!("invalid boolean {value}"))),
    }
}

fn parse_lsn(value: &str) -> Result<u64, String> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| format!("invalid LSN {value}"))?;
    let high = u64::from_str_radix(high, 16).map_err(|_| format!("invalid LSN {value}"))?;
    let low = u64::from_str_radix(low, 16).map_err(|_| format!("invalid LSN {value}"))?;
    Ok((high << 32) | low)
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

fn run_cycle(
    config: &Config,
    receiver: &Path,
    publisher: &Path,
    destination: &mut Client,
) -> Result<(i64, i64), CycleError> {
    let source_address = source_preflight(destination, config)?;
    let receiver_output = Command::new(receiver)
        .env("LAKE_SOURCE_ADDRESS", source_address)
        .env(
            "LAKE_RECEIVER_IDLE_TIMEOUT_MS",
            config.receiver_idle_timeout.as_millis().to_string(),
        )
        .env("LAKE_MAX_TRANSACTIONS", "1")
        .output()
        .map_err(|error| CycleError::Retry(format!("could not start receiver: {error}")))?;
    ensure_success("receiver", &receiver_output).map_err(CycleError::Retry)?;

    let pending = has_pending_transactions(destination, config)
        .map_err(|error| CycleError::Fatal(error.to_string()))?;
    if !pending {
        return Ok((1, 0));
    }
    let publisher_output = Command::new(publisher)
        .env_remove("LAKE_SOURCE_PASSWORD")
        .env("LAKE_MAX_BATCHES", config.publisher_max_batches.to_string())
        .output()
        .map_err(|error| CycleError::Retry(format!("could not start publisher: {error}")))?;
    ensure_success("publisher", &publisher_output).map_err(CycleError::Retry)?;
    Ok((1, 1))
}

fn ensure_success(worker: &str, output: &Output) -> Result<(), String> {
    if output.status.success() {
        // Command::output captures child streams. Forward successful JSON-lines
        // records so receiver and publisher events reach the supervisor's log.
        let _ = io::stdout().write_all(&output.stdout);
        let _ = io::stdout().flush();
        let _ = io::stderr().write_all(&output.stderr);
        let _ = io::stderr().flush();
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let detail = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    Err(format!(
        "{worker} exited with {}: {}",
        output.status,
        truncate(detail, 2000)
    ))
}

fn service_state(
    destination: &mut Client,
    config: &Config,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let rows = control_query(
        destination,
        config,
        &format!(
            "SELECT ss.desired_state, s.state FROM lake_replication.service_status ss JOIN lake_replication.subscriptions s USING (subscription_id) WHERE ss.subscription_id={}",
            config.subscription_id
        ),
    )?;
    let row = rows.first().ok_or("service status row disappeared")?;
    Ok((
        field(row, 0, "desired state")?.to_owned(),
        field(row, 1, "subscription state")?.to_owned(),
    ))
}

fn has_pending_transactions(
    destination: &mut Client,
    config: &Config,
) -> Result<bool, Box<dyn std::error::Error>> {
    let rows = control_query(
        destination,
        config,
        &format!(
            "SELECT (staged_lsn > published_lsn)::text FROM lake_replication.checkpoints WHERE subscription_id={}",
            config.subscription_id
        ),
    )?;
    Ok(field(
        rows.first().ok_or("checkpoint disappeared")?,
        0,
        "pending state",
    )? == "true")
}

fn report_cycle(
    destination: &mut Client,
    config: &Config,
    state: &str,
    receiver_increment: i64,
    publisher_increment: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    control_query(
        destination,
        config,
        &format!(
            "SELECT lake_replication.report_service_cycle({}, {}, {receiver_increment}, {publisher_increment})",
            config.subscription_id,
            sql_literal(state)
        ),
    )?;
    Ok(())
}

fn stop(destination: &mut Client, config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    control_query(
        destination,
        config,
        &format!(
            "SELECT lake_replication.report_service_stopped({})",
            config.subscription_id
        ),
    )?;
    Ok(())
}

fn wait_for_retry_or_stop(
    config: &Config,
    destination: &mut Client,
    delay: Duration,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut remaining = delay;
    while !remaining.is_zero() {
        let interval = remaining.min(config.poll_interval);
        thread::sleep(interval);
        remaining = remaining.saturating_sub(interval);
        if service_state(destination, config)?.0 == "stopped" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn retry_delay(base: Duration, maximum: Duration, failures: u32) -> Duration {
    let multiplier = 1_u32.checked_shl(failures.min(20)).unwrap_or(u32::MAX);
    base.saturating_mul(multiplier).min(maximum)
}

struct Config {
    source_addresses: Vec<String>,
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
    subscription_id: i64,
    poll_interval: Duration,
    receiver_idle_timeout: Duration,
    base_backoff: Duration,
    max_backoff: Duration,
    publisher_max_batches: u64,
    inject_failures: u32,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let settings = Settings::from_env()?;
        let legacy_user = value("LAKE_USER", "postgres");
        let source_addresses = source_addresses()?;
        let destination_address = value("LAKE_DESTINATION_ADDRESS", "127.0.0.1:55434");
        let source_address_refs = source_addresses
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let config = Self {
            source_tls: tls_config(&settings, "LAKE_SOURCE", &source_address_refs)?,
            destination_tls: tls_config(&settings, "LAKE_DESTINATION", &[&destination_address])?,
            source_addresses,
            destination_address,
            source_user: settings.value("LAKE_SOURCE_USER", &legacy_user),
            source_password: settings.optional("LAKE_SOURCE_PASSWORD"),
            destination_user: settings.value("LAKE_DESTINATION_USER", &legacy_user),
            destination_password: settings.optional("LAKE_DESTINATION_PASSWORD"),
            source_database: value("LAKE_SOURCE_DATABASE", "application"),
            destination_database: value("LAKE_DESTINATION_DATABASE", "analytics"),
            slot: required("LAKE_SLOT")?,
            subscription_id: required("LAKE_SUBSCRIPTION_ID")?.parse()?,
            poll_interval: milliseconds("LAKE_SUPERVISOR_POLL_MS", "250")?,
            receiver_idle_timeout: milliseconds("LAKE_RECEIVER_IDLE_TIMEOUT_MS", "1000")?,
            base_backoff: milliseconds("LAKE_SUPERVISOR_BASE_BACKOFF_MS", "1000")?,
            max_backoff: milliseconds("LAKE_SUPERVISOR_MAX_BACKOFF_MS", "30000")?,
            publisher_max_batches: value("LAKE_SUPERVISOR_PUBLISHER_MAX_BATCHES", "10").parse()?,
            inject_failures: value("LAKE_SUPERVISOR_INJECT_FAILURES", "0").parse()?,
        };
        if config.poll_interval.is_zero()
            || config.receiver_idle_timeout.is_zero()
            || config.base_backoff.is_zero()
            || config.max_backoff < config.base_backoff
            || config.publisher_max_batches == 0
        {
            return Err("supervisor timing and batch limits must be positive and ordered".into());
        }
        Ok(config)
    }

    fn connect(&self) -> Result<Client, lake_replication_worker::wire::Error> {
        Client::connect_named_secure(
            self.destination_address.as_str(),
            &self.destination_user,
            &self.destination_database,
            false,
            &format!("lake-replication-supervisor:{}", self.subscription_id),
            self.destination_password.as_deref(),
            self.destination_tls.as_ref(),
        )
    }

    fn connect_source(
        &self,
        address: &str,
    ) -> Result<Client, lake_replication_worker::wire::Error> {
        Client::connect_named_secure(
            address,
            &self.source_user,
            &self.source_database,
            false,
            &format!("lake-replication-source-check:{}", self.subscription_id),
            self.source_password.as_deref(),
            self.source_tls.as_ref(),
        )
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

fn source_addresses() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let configured = env::var("LAKE_SOURCE_ADDRESSES")
        .unwrap_or_else(|_| value("LAKE_SOURCE_ADDRESS", "127.0.0.1:55434"));
    let addresses = configured
        .split(',')
        .map(str::trim)
        .filter(|address| !address.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err("LAKE_SOURCE_ADDRESSES must contain at least one endpoint".into());
    }
    Ok(addresses)
}

fn milliseconds(name: &str, default: &str) -> Result<Duration, Box<dyn std::error::Error>> {
    Ok(Duration::from_millis(value(name, default).parse()?))
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

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn binary_name(name: &str) -> String {
    format!("{name}{}", env::consts::EXE_SUFFIX)
}

fn require_binary(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if !path.is_file() {
        return Err(format!("required worker binary {} does not exist", path.display()).into());
    }
    Ok(())
}

fn truncate(value: &str, maximum: usize) -> &str {
    if value.len() <= maximum {
        value
    } else {
        let mut boundary = maximum;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        &value[..boundary]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected_source() -> ExpectedSource {
        ExpectedSource {
            system_identifier: "123".into(),
            database_oid: 42,
            database_name: "application".into(),
            timeline: 1,
            staged_lsn: 0x100,
        }
    }

    fn source_observation() -> SourceObservation {
        SourceObservation {
            system_identifier: "123".into(),
            database_oid: 42,
            timeline: 1,
            plugin: "pgoutput".into(),
            slot_type: "logical".into(),
            database: "application".into(),
            temporary: false,
            active: false,
            wal_status: Some("reserved".into()),
            invalidation_reason: None,
            conflicting: false,
            failover: false,
            confirmed_lsn: 0x100,
            current_lsn: 0x180,
        }
    }

    #[test]
    fn retry_delay_is_exponential_and_capped() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_millis(350);
        assert_eq!(retry_delay(base, cap, 0), Duration::from_millis(100));
        assert_eq!(retry_delay(base, cap, 1), Duration::from_millis(200));
        assert_eq!(retry_delay(base, cap, 2), cap);
    }

    #[test]
    fn truncation_respects_utf8_boundaries() {
        assert_eq!(truncate("abé", 3), "ab");
        assert_eq!(truncate("short", 20), "short");
    }

    #[test]
    fn healthy_source_identity_and_slot_are_accepted() {
        assert!(validate_source(&expected_source(), &source_observation(), None).is_ok());
    }

    #[test]
    fn identity_loss_and_unsafe_slot_progress_are_rejected() {
        let mut observation = source_observation();
        observation.system_identifier = "456".into();
        assert!(
            validate_source(&expected_source(), &observation, None)
                .unwrap_err()
                .contains("identity mismatch")
        );

        let mut observation = source_observation();
        observation.wal_status = Some("lost".into());
        assert!(
            validate_source(&expected_source(), &observation, None)
                .unwrap_err()
                .contains("lost WAL")
        );

        let mut observation = source_observation();
        observation.confirmed_lsn = 0x101;
        assert!(
            validate_source(&expected_source(), &observation, None)
                .unwrap_err()
                .contains("ahead of durable staged")
        );
    }

    #[test]
    fn timeline_advance_requires_failover_slot_and_safe_ancestry() {
        let mut observation = source_observation();
        observation.timeline = 2;
        assert!(
            validate_source(&expected_source(), &observation, Some("1 0/200 promoted"))
                .unwrap_err()
                .contains("not failover-enabled")
        );

        observation.failover = true;
        assert!(
            validate_source(&expected_source(), &observation, Some("1 0/200 promoted")).is_ok()
        );
        assert!(
            validate_source(&expected_source(), &observation, Some("1 0/80 promoted"))
                .unwrap_err()
                .contains("before durable staged")
        );
    }
}
