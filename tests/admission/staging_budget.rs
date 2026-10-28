//! Diagnostic hook injected only into a disposable initial-sync build.
use super::Config;
use lake_replication_worker::wire::{Client, Error};
use std::cell::RefCell;
use std::time::Duration;

thread_local! {
    static CLIENT: RefCell<Option<Client>> = const { RefCell::new(None) };
}

pub fn admit(bytes: usize, config: &Config) -> Result<(), Error> {
    let database = std::env::var("LAKE_DIAGNOSTIC_STAGING_DATABASE")
        .map_err(|_| Error::Protocol("diagnostic budget database is required".into()))?;
    let delay = CLIENT.with(|cell| {
        let mut stored = cell.borrow_mut();
        if stored.is_none() {
            let mut client = Client::connect_named_secure(
                &config.destination_address,
                &config.destination_user,
                &database,
                false,
                "diagnostic_staging_budget",
                config.destination_password.as_deref(),
                config.destination_tls.as_ref(),
            )?;
            client.set_read_timeout(Some(Duration::from_secs(6)))?;
            client.simple_query("SET statement_timeout='5s'; SET lock_timeout='2s'")?;
            *stored = Some(client);
        }
        let result = stored
            .as_mut()
            .unwrap()
            .simple_query(&format!("SELECT staging_budget.reserve({bytes})::text"));
        match result {
            Ok(rows) => rows
                .first()
                .and_then(|r| r.first())
                .and_then(Option::as_ref)
                .ok_or_else(|| Error::Protocol("budget returned no wait time".into()))?
                .parse::<f64>()
                .map_err(|_| Error::Protocol("invalid budget wait time".into())),
            Err(error) => {
                *stored = None;
                Err(error)
            }
        }
    })?;
    if !delay.is_finite() || !(0.0..=2.0).contains(&delay) {
        return Err(Error::Protocol(
            "budget wait exceeds diagnostic horizon".into(),
        ));
    }
    // The coordinator committed and the destination append has not begun. The
    // source COPY snapshot remains open. The supervisor can kill this child
    // promptly; abandoned credit expires as time refills it.
    std::thread::sleep(Duration::from_secs_f64(delay));
    Ok(())
}
