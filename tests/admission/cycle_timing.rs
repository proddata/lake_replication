//! Supervisor phase timing for an isolated diagnostic build.
use lake_replication_worker::logging::event;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT: AtomicU64 = AtomicU64::new(1);

pub struct Cycle {
    database: String,
    subscription: i64,
    number: u64,
    started: Instant,
}

impl Cycle {
    pub fn new(database: &str, subscription: i64) -> Self {
        let value = Self {
            database: database.into(),
            subscription,
            number: NEXT.fetch_add(1, Ordering::Relaxed),
            started: Instant::now(),
        };
        value.mark("cycle_started");
        value
    }

    pub fn mark(&self, phase: &str) {
        event(
            "diagnostic_cycle",
            "INFO",
            phase,
            "isolated supervisor phase",
            &[
                ("database", self.database.clone()),
                ("subscription_id", self.subscription.to_string()),
                ("cycle", self.number.to_string()),
                ("elapsed_us", self.started.elapsed().as_micros().to_string()),
            ],
        );
    }
}

impl Drop for Cycle {
    fn drop(&mut self) {
        self.mark("cycle_finished");
    }
}
