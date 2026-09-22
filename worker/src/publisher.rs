use lake_replication_worker::credentials::Settings;
use lake_replication_worker::logging::event as log_event;
use lake_replication_worker::wire::{Client, TlsConfig, address_is_loopback};
use std::env;

fn main() {
    if let Err(error) = run() {
        log_event(
            "publisher",
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
    let mut destination = Client::connect_secure(
        config.destination_address.as_str(),
        &config.destination_user,
        &config.destination_database,
        false,
        config.destination_password.as_deref(),
        config.destination_tls.as_ref(),
    )?;
    if let Some(crash_point) = &config.lab_crash_point {
        destination.simple_query(&format!(
            "SET pg_lake_table.lab_crash_point = {}",
            sql_literal(crash_point)
        ))?;
    }
    let mut published_batches = 0_u64;
    while published_batches < config.max_batches {
        let rows = destination.simple_query(&format!(
            "SELECT first_end_lsn::text, last_end_lsn::text, transaction_count::text, row_count::text, byte_count::text FROM lake_replication.next_batch({}, {}, {}, {})",
            config.subscription_id,
            config.max_transactions,
            config.max_rows,
            config.max_bytes,
        ))?;
        let Some(row) = rows.first() else {
            if published_batches == 0 {
                log_event(
                    "publisher",
                    "INFO",
                    "queue_empty",
                    "no staged transactions",
                    &[("subscription_id", config.subscription_id.to_string())],
                );
            }
            return Ok(());
        };
        let first_lsn = field(row, 0, "first_end_lsn")?;
        let last_lsn = field(row, 1, "last_end_lsn")?;
        let transaction_count = field(row, 2, "transaction_count")?;
        let planned_rows = field(row, 3, "row_count")?;
        let byte_count = field(row, 4, "byte_count")?;
        let batch_key = format!(
            "subscription-{}:{}-{}",
            config.subscription_id, first_lsn, last_lsn
        );
        let result = destination.simple_query(&format!(
            "SELECT batch_id::text, published_lsn::text, inserted_rows::text FROM lake_replication.publish_insert_batch({}, {}, {}::pg_lsn, {}::pg_lsn)",
            config.subscription_id,
            sql_literal(&batch_key),
            sql_literal(first_lsn),
            sql_literal(last_lsn),
        ))?;
        let result = result.first().ok_or("publisher returned no batch")?;
        let batch_id = field(result, 0, "batch_id")?;
        let published_lsn = field(result, 1, "published_lsn")?;
        let inserted_rows = field(result, 2, "inserted_rows")?;
        log_event(
            "publisher",
            "INFO",
            "batch_published",
            &format!(
                "published batch={batch_id} lsn={published_lsn} transactions={transaction_count} rows={inserted_rows}/{planned_rows} bytes={byte_count}"
            ),
            &[
                ("subscription_id", config.subscription_id.to_string()),
                ("batch_id", batch_id.to_owned()),
                ("published_lsn", published_lsn.to_owned()),
                ("transactions", transaction_count.to_owned()),
                ("inserted_rows", inserted_rows.to_owned()),
                ("planned_rows", planned_rows.to_owned()),
                ("bytes", byte_count.to_owned()),
            ],
        );
        published_batches += 1;
    }
    Ok(())
}

struct Config {
    destination_address: String,
    destination_user: String,
    destination_password: Option<String>,
    destination_tls: Option<TlsConfig>,
    destination_database: String,
    subscription_id: i64,
    max_transactions: i64,
    max_rows: i64,
    max_bytes: i64,
    max_batches: u64,
    lab_crash_point: Option<String>,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let settings = Settings::from_env()?;
        let destination_address = value("LAKE_DESTINATION_ADDRESS", "127.0.0.1:55434");
        let config = Self {
            destination_tls: tls_config(&settings, "LAKE_DESTINATION", &[&destination_address])?,
            destination_address,
            destination_user: settings
                .value("LAKE_DESTINATION_USER", &value("LAKE_USER", "postgres")),
            destination_password: settings.optional("LAKE_DESTINATION_PASSWORD"),
            destination_database: value("LAKE_DESTINATION_DATABASE", "analytics"),
            subscription_id: required("LAKE_SUBSCRIPTION_ID")?.parse()?,
            max_transactions: value("LAKE_BATCH_MAX_TRANSACTIONS", "1000").parse()?,
            max_rows: value("LAKE_BATCH_MAX_ROWS", "100000").parse()?,
            max_bytes: value("LAKE_BATCH_MAX_BYTES", "67108864").parse()?,
            max_batches: value("LAKE_MAX_BATCHES", "1").parse()?,
            lab_crash_point: env::var("LAKE_LAB_CRASH_POINT").ok(),
        };
        if config.max_transactions <= 0
            || config.max_rows <= 0
            || config.max_bytes <= 0
            || config.max_batches == 0
        {
            return Err("batch limits must be positive".into());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_literal_escapes_quotes() {
        assert_eq!(sql_literal("a'b"), "'a''b'");
    }
}
