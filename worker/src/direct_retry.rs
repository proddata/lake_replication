//! Replay only a server-confirmed deadlock abort, on a synchronized connection.
use super::Result;
use lake_replication_worker::{
    staging::format_lsn,
    wire::{Client, Error as WireError},
};
use std::{env, time::Duration};
pub fn limit() -> Result<u32> {
    let retries = env::var("LAKE_DIRECT_DEADLOCK_RETRIES")
        .unwrap_or("3".into())
        .parse()?;
    if retries > 8 {
        return Err("deadlock retries must be 0..8".into());
    }
    Ok(retries)
}
fn confirmed_deadlock(error: &(dyn std::error::Error + 'static), state: Option<&str>) -> bool {
    matches!(
        error.downcast_ref::<WireError>(),
        Some(WireError::Server(_))
    ) && state == Some("40P01")
}
pub fn transaction<F, T>(
    sink: &mut Client,
    source: &mut Client,
    safe_lsn: u64,
    mut publish: F,
) -> Result<T>
where
    F: FnMut(&mut Client, &mut Client) -> Result<T>,
{
    let retries = limit()?;
    for attempt in 0..=retries {
        match publish(sink, source) {
            Ok(value) => return Ok(value),
            Err(error) => {
                if !confirmed_deadlock(error.as_ref(), sink.last_query_sqlstate())
                    || attempt == retries
                {
                    return Err(error);
                }
                // ErrorResponse is drained to ReadyForQuery. ROLLBACK must
                // succeed before any replay, including a spill rewind.
                sink.simple_query("ROLLBACK")?;
                source.standby_status(safe_lsn)?;
                let delay = 100u64 * (1u64 << attempt.min(4));
                eprintln!(
                    "direct_retry sqlstate=40P01 retry={} limit={retries} safe_lsn={} backoff_ms={delay}",
                    attempt + 1,
                    format_lsn(safe_lsn)
                );
                std::thread::sleep(Duration::from_millis(delay));
            }
        }
    }
    unreachable!()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_outcomes_and_message_spoofs_do_not_authorize_replay() {
        assert!(confirmed_deadlock(
            &WireError::Server("localized arbitrary text".into()),
            Some("40P01")
        ));
        assert!(!confirmed_deadlock(
            &WireError::Server("C=40P01".into()),
            Some("23505")
        ));
        assert!(!confirmed_deadlock(
            &WireError::Server("C=40P01".into()),
            None
        ));
        assert!(!confirmed_deadlock(
            &WireError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
            Some("40P01")
        ));
        assert!(!confirmed_deadlock(
            &WireError::Protocol("unknown COMMIT outcome".into()),
            Some("40P01")
        ));
    }
}
