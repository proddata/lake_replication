use lake_replication_worker::wire::{Client, Error as WireError};
use std::env;
use std::path::Path;
use std::process::{Command, Output};
use std::thread;
use std::time::Duration;

fn main() {
    if let Err(error) = run() {
        eprintln!("lake-replication-supervisor: {error}");
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
            println!("graceful stop requested");
            return Ok(());
        }
        if subscription_state != "streaming" {
            report_cycle(&mut destination, &config, "paused", 0, 0)?;
            thread::sleep(config.poll_interval);
            continue;
        }

        let cycle = if injected_failures > 0 {
            injected_failures -= 1;
            Err("lab-injected supervisor child failure".to_owned())
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
            Err(error) => {
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
                eprintln!(
                    "worker cycle failed; retrying in {}ms: {error}",
                    delay.as_millis()
                );
                if wait_for_retry_or_stop(&config, &mut destination, delay)? {
                    stop(&mut destination, &config)?;
                    println!("graceful stop requested during backoff");
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
                eprintln!("{connection_error}");
                let mut failures = 0_u32;
                loop {
                    let delay = retry_delay(config.base_backoff, config.max_backoff, failures);
                    failures = failures.saturating_add(1);
                    thread::sleep(delay);
                    let mut replacement = match config.connect() {
                        Ok(client) => client,
                        Err(error) => {
                            eprintln!(
                                "destination reconnect failed; retrying in {}ms: {error}",
                                delay.as_millis()
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
                            eprintln!(
                                "destination reconnect lock check failed; retrying in {}ms: {error}",
                                delay.as_millis()
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
                            println!("destination control connection restored");
                            break;
                        }
                        Err(WireError::Io(error)) => {
                            eprintln!("destination reconnect was interrupted: {error}");
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

fn run_cycle(
    config: &Config,
    receiver: &Path,
    publisher: &Path,
    destination: &mut Client,
) -> Result<(i64, i64), String> {
    let receiver_output = Command::new(receiver)
        .env(
            "LAKE_RECEIVER_IDLE_TIMEOUT_MS",
            config.receiver_idle_timeout.as_millis().to_string(),
        )
        .env("LAKE_MAX_TRANSACTIONS", "1")
        .output()
        .map_err(|error| format!("could not start receiver: {error}"))?;
    ensure_success("receiver", &receiver_output)?;

    let pending =
        has_pending_transactions(destination, config).map_err(|error| error.to_string())?;
    if !pending {
        return Ok((1, 0));
    }
    let publisher_output = Command::new(publisher)
        .env("LAKE_MAX_BATCHES", config.publisher_max_batches.to_string())
        .output()
        .map_err(|error| format!("could not start publisher: {error}"))?;
    ensure_success("publisher", &publisher_output)?;
    Ok((1, 1))
}

fn ensure_success(worker: &str, output: &Output) -> Result<(), String> {
    if output.status.success() {
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
    destination_address: String,
    user: String,
    destination_database: String,
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
        let config = Self {
            destination_address: value("LAKE_DESTINATION_ADDRESS", "127.0.0.1:55434"),
            user: value("LAKE_USER", "postgres"),
            destination_database: value("LAKE_DESTINATION_DATABASE", "analytics"),
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
        Client::connect_named(
            self.destination_address.as_str(),
            &self.user,
            &self.destination_database,
            false,
            &format!("lake-replication-supervisor:{}", self.subscription_id),
        )
    }
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
}
