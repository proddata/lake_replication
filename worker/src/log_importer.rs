//! Opt-in harness executable; absent from default release builds.
use lake_replication_worker::{
    credentials::Settings,
    logging::event,
    managed::ManagedClient,
    push_import,
    wire::{Client, TlsConfig, address_is_loopback},
};
use std::{env, thread, time::Duration};

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    Ok(env::var(name).map_err(|_| format!("required environment variable {name} is not set"))?)
}
fn tls(
    settings: &Settings,
    prefix: &str,
    address: &str,
) -> Result<Option<TlsConfig>, Box<dyn std::error::Error>> {
    let default = if address_is_loopback(address) {
        "disable"
    } else {
        "verify-full"
    };
    match settings
        .value(&format!("{prefix}_TLS_MODE"), default)
        .as_str()
    {
        "disable" => Ok(None),
        "verify-full" => Ok(Some(TlsConfig::verify_full(
            settings.required(&format!("{prefix}_TLS_ROOT_CERT"))?,
            settings.optional(&format!("{prefix}_TLS_SERVER_NAME")),
        ))),
        _ => Err("unsupported importer TLS mode".into()),
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut settings = Settings::from_env()?;
    let subscription = required("LAKE_SUBSCRIPTION_ID")?.parse()?;
    let source_address = required("LAKE_SOURCE_ADDRESS")?;
    let destination_address = required("LAKE_DESTINATION_ADDRESS")?;
    let destination_tls = tls(&settings, "LAKE_DESTINATION", &destination_address)?;
    settings.load_subscription_source_credentials(
        subscription,
        &destination_address,
        &settings.destination_user()?,
        &required("LAKE_DESTINATION_DATABASE")?,
        settings.optional("LAKE_DESTINATION_PASSWORD").as_deref(),
        destination_tls.as_ref(),
    )?;
    let source_tls = tls(&settings, "LAKE_SOURCE", &source_address)?;
    let mut destination = ManagedClient::connect(
        &destination_address,
        &settings.destination_user()?,
        &required("LAKE_DESTINATION_DATABASE")?,
        settings.optional("LAKE_DESTINATION_PASSWORD").as_deref(),
        destination_tls.as_ref(),
        subscription,
        env::var("LAKE_CONFIG_GENERATION")
            .unwrap_or("0".into())
            .parse()?,
    )?;
    let registry = push_import::registry(&mut destination, subscription)?;
    let schema_mode = push_import::schema_enabled(&mut destination, subscription)?;
    let binding = push_import::stream_binding(&mut destination, subscription)?;
    let continuity = push_import::continuity_point(&mut destination, subscription)?;
    let mut source = Client::connect_secure(
        &source_address,
        &settings.required("LAKE_SOURCE_USER")?,
        &required("LAKE_SOURCE_DATABASE")?,
        false,
        settings.optional("LAKE_SOURCE_PASSWORD").as_deref(),
        source_tls.as_ref(),
    )?;
    let mut sequence = match env::var("LAKE_PUSH_REPLAY_FROM") {
        Ok(value) => value.parse()?,
        Err(_) => push_import::next_sequence(&mut destination, subscription)?,
    };
    let limit: u64 = env::var("LAKE_PUSH_MAX_BATCHES")
        .unwrap_or("1".into())
        .parse()?;
    for _ in 0..limit {
        let observation = push_import::read_batch(
            &mut source,
            sequence,
            &registry,
            &binding,
            continuity.as_ref(),
        )?;
        push_import::observe_source(
            &mut destination,
            subscription,
            &binding,
            observation.captured_sequence,
        )?;
        let Some(batch) = observation.batch else {
            return Ok(());
        };
        loop {
            let outcome = if schema_mode {
                push_import::admit_ordered(&mut destination, subscription, &batch, &registry)?
            } else {
                push_import::admit(&mut destination, subscription, &batch, &registry)?
            };
            if outcome != "full" {
                // Compiled only into this opt-in prototype executable. The
                // harness kills the stopped child to test durable admission
                // recovery without consuming its successful result.
                if env::var("LAKE_PUSH_TEST_STOP").as_deref() == Ok("after-admission") {
                    unsafe {
                        libc::raise(libc::SIGSTOP);
                    }
                }
                event(
                    "log-importer",
                    "INFO",
                    "batch_admitted",
                    &outcome,
                    &[("sequence", sequence.to_string())],
                );
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        sequence += 1;
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        event(
            "log-importer",
            "ERROR",
            "process_failed",
            &error.to_string(),
            &[],
        );
        std::process::exit(1);
    }
}
