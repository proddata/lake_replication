//! Opt-in timing metadata. Never accepts SQL text, credentials or row payloads.
use crate::logging::event;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static CONNECTIONS: AtomicU64 = AtomicU64::new(0);

pub(crate) struct QueryTrace {
    connection: u64,
    database: String,
    role: &'static str,
    binding: Option<(i64, i64)>,
    started: Instant,
    pub(crate) backend_pid: u32,
    pub(crate) sequence: u64,
}

impl QueryTrace {
    pub(crate) fn from_env(database: &str, application: &str) -> Option<Self> {
        if std::env::var("LAKE_TRACE_TRANSACTIONS").as_deref() != Ok("1") {
            return None;
        }
        let executable = std::env::current_exe().ok()?;
        let name = executable.file_name()?.to_str()?;
        let role = if name.starts_with("lake-replication-publisher") {
            "publisher"
        } else if name.starts_with("lake-replication-initial-sync") {
            "initial_sync"
        } else if name.starts_with("lake-replication-receiver") {
            "receiver"
        } else if name.starts_with("lake-replication-supervisor") {
            "supervisor"
        } else {
            "other"
        };
        Some(Self {
            connection: CONNECTIONS.fetch_add(1, Ordering::Relaxed),
            database: database.to_owned(),
            role,
            binding: application
                .strip_prefix("lake_replication_worker/")
                .and_then(|value| {
                    let (subscription, generation) = value.split_once('/')?;
                    Some((subscription.parse().ok()?, generation.parse().ok()?))
                }),
            started: Instant::now(),
            backend_pid: 0,
            sequence: 0,
        })
    }

    pub(crate) fn emit(
        &self,
        operation: &'static str,
        phase: &'static str,
        elapsed_us: u128,
        status: &str,
    ) {
        event(
            "wire_trace",
            "INFO",
            "query_timing",
            "Opt-in connection/query timing; not CPU time",
            &[
                ("connection", self.connection.to_string()),
                ("database", self.database.clone()),
                ("role", self.role.to_owned()),
                (
                    "subscription_id",
                    self.binding.map(|v| v.0.to_string()).unwrap_or_default(),
                ),
                (
                    "generation",
                    self.binding.map(|v| v.1.to_string()).unwrap_or_default(),
                ),
                ("backend_pid", self.backend_pid.to_string()),
                ("sequence", self.sequence.to_string()),
                ("operation", operation.to_owned()),
                ("phase", phase.to_owned()),
                ("elapsed_us", elapsed_us.to_string()),
                ("status", status.to_owned()),
            ],
        );
    }

    pub(crate) fn connected(&self) {
        self.emit(
            "connect",
            "startup",
            self.started.elapsed().as_micros(),
            "ok",
        );
    }
}
