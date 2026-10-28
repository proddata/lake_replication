//! Diagnostic-only receiver snapshots. Never feeds WAL acknowledgement.
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq)]
struct Header {
    kind: &'static str,
    wal_end: u64,
    sent_pg_us: i64,
}

fn header(data: &[u8]) -> Option<Header> {
    let (kind, end, sent, size) = match data.first()? {
        b'w' => ("data", 9, 17, 25),
        b'k' => ("keepalive", 1, 9, 18),
        _ => return None,
    };
    if data.len() < size {
        return None;
    }
    Some(Header {
        kind,
        wal_end: u64::from_be_bytes(data[end..end + 8].try_into().ok()?),
        sent_pg_us: i64::from_be_bytes(data[sent..sent + 8].try_into().ok()?),
    })
}

pub struct Progress {
    database: String,
    subscription: i64,
    started: Instant,
    started_epoch: f64,
    last_emit: Option<Instant>,
    last_message: Option<Instant>,
    message: Option<Header>,
    durable_lsn: u64,
    observed_commit: Option<(u64, i64)>,
    messages: u64,
    timeouts: u64,
    path: PathBuf,
}

impl Progress {
    pub fn new(database: &str, subscription: i64, durable_lsn: u64) -> io::Result<Self> {
        let mut value = Self {
            database: database.to_owned(),
            subscription,
            started: Instant::now(),
            started_epoch: epoch(),
            last_emit: None,
            last_message: None,
            message: None,
            durable_lsn,
            observed_commit: None,
            messages: 0,
            timeouts: 0,
            path: snapshot_path(database, subscription),
        };
        value.emit("starting", true)?;
        Ok(value)
    }

    pub fn observe(&mut self, data: &[u8]) -> io::Result<()> {
        if let Some(message) = header(data) {
            self.message = Some(message);
            self.last_message = Some(Instant::now());
            self.messages += 1;
        }
        self.emit("before_decode", false)
    }

    pub fn committing(&mut self, end: u64, commit_pg_us: i64) -> io::Result<()> {
        self.observed_commit = Some((end, commit_pg_us));
        self.emit("before_staging", true)
    }

    pub fn timeout(&mut self) -> io::Result<()> {
        self.timeouts += 1;
        self.emit("read_timeout", false)
    }

    pub fn wait_for_journal(
        &mut self,
        destination: &mut lake_replication_worker::wire::Client,
        maximum: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let began = Instant::now();
        loop {
            // Native source-slot ownership supplies the single producer in
            // this prototype. This is not an admission API for other writers.
            let rows = destination.simple_query(&format!(
                "SELECT count(*)::text FROM lake_replication.transactions WHERE subscription_id={} AND state<>'published'",
                self.subscription
            ))?;
            let count: u64 = rows
                .first()
                .and_then(|r| r.first())
                .and_then(Option::as_deref)
                .ok_or("journal count missing")?
                .parse()?;
            if count < maximum {
                return Ok(());
            }
            self.emit("journal_backpressure", false)?;
            if began.elapsed() > Duration::from_secs(30) {
                return Err("diagnostic journal backpressure exceeded 30 seconds".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn staged(&mut self, lsn: u64) -> io::Result<()> {
        self.durable_lsn = lsn;
        self.emit("durably_staged", false)
    }

    fn emit(&mut self, phase: &str, force: bool) -> io::Result<()> {
        if !force
            && self
                .last_emit
                .is_some_and(|last| last.elapsed() < Duration::from_millis(250))
        {
            return Ok(());
        }
        let message = match &self.message {
            Some(h) => format!(
                "{{\"kind\":\"{}\",\"wal_end\":\"{}\",\"sent_pg_us\":{}}}",
                h.kind,
                super::format_lsn(h.wal_end),
                h.sent_pg_us
            ),
            None => "null".to_owned(),
        };
        let age = self
            .last_message
            .map(|last| last.elapsed().as_secs_f64().to_string())
            .unwrap_or("null".into());
        let commit = self
            .observed_commit
            .map(|(end, timestamp)| {
                format!(
                    "{{\"end_lsn\":\"{}\",\"commit_pg_us\":{timestamp}}}",
                    super::format_lsn(end)
                )
            })
            .unwrap_or("null".into());
        let data = format!(
            "{{\"database\":{},\"subscription_id\":{},\"pid\":{},\"started_epoch\":{},\"snapshot_epoch\":{},\"elapsed_seconds\":{},\"phase\":\"{}\",\"last_message_age_at_snapshot_seconds\":{},\"last_message\":{},\"durably_staged_lsn\":\"{}\",\"observed_commit\":{},\"messages\":{},\"read_timeouts\":{}}}\n",
            super::json_string(&self.database),
            self.subscription,
            std::process::id(),
            self.started_epoch,
            epoch(),
            self.started.elapsed().as_secs_f64(),
            phase,
            age,
            message,
            super::format_lsn(self.durable_lsn),
            commit,
            self.messages,
            self.timeouts
        );
        let temp = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(data.as_bytes())?;
        drop(file);
        fs::rename(temp, &self.path)?;
        self.last_emit = Some(Instant::now());
        Ok(())
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        // A destructor write failure must not replace the original worker outcome.
        // A killed process leaves its prior record, which readers must age.
        let _ = self.emit("exited", true);
    }
}

fn snapshot_path(database: &str, subscription: i64) -> PathBuf {
    let key: String = database
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    PathBuf::from(format!(
        "/worker-logs/receiver-progress-{key}-{subscription}.json"
    ))
}

fn epoch() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_subscription_path_survives_pid_churn_and_preserves_database_identity() {
        assert_ne!(snapshot_path("a", 1), snapshot_path("a", 2));
        assert_ne!(snapshot_path("a-b", 1), snapshot_path("a", 1));
        assert!(!snapshot_path("../é", 1).to_str().unwrap().contains(".."));
    }

    #[test]
    fn parses_both_wire_headers_without_treating_wal_end_as_durable() {
        for (tag, prefix, size) in [(b'w', 9, 25), (b'k', 1, 18)] {
            let mut bytes = vec![0; size];
            bytes[0] = tag;
            bytes[prefix..prefix + 8].copy_from_slice(&1234u64.to_be_bytes());
            bytes[prefix + 8..prefix + 16].copy_from_slice(&(-7i64).to_be_bytes());
            let h = header(&bytes).unwrap();
            assert_eq!(h.wal_end, 1234);
            assert_eq!(h.sent_pg_us, -7);
            assert!(header(&bytes[..size - 1]).is_none());
        }
        assert!(header(&[]).is_none());
        assert!(header(b"x").is_none());
    }
}
