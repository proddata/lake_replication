// Experimental contention budget. Identity/WAL validation runs before this check.
struct SlotContention {
    first_busy: std::collections::BTreeMap<String, std::time::Instant>,
    budget: Duration,
}

impl SlotContention {
    fn new(budget: Duration) -> Self {
        Self {
            first_busy: std::collections::BTreeMap::new(),
            budget,
        }
    }

    fn observe(
        &mut self,
        address: &str,
        active: bool,
        now: std::time::Instant,
    ) -> Result<(), CycleError> {
        if !active {
            // A validated inactive endpoint is selected immediately; a healthy
            // cycle clears stale contention on alternative endpoints as well.
            self.first_busy.clear();
            return Ok(());
        }
        let first = *self.first_busy.entry(address.to_owned()).or_insert(now);
        let elapsed = now.saturating_duration_since(first);
        let message = format!(
            "source slot contention at {address}: slot is still active; refusing consumption ({}ms of {}ms budget)",
            elapsed.as_millis(),
            self.budget.as_millis()
        );
        if elapsed < self.budget {
            Err(CycleError::Retry(message))
        } else {
            Err(CycleError::Fatal(message))
        }
    }

    fn remaining(&self) -> Option<Duration> {
        self.first_busy
            .values()
            .map(|first| self.budget.saturating_sub(first.elapsed()))
            .min()
    }
}

#[cfg(test)]
mod contention_tests {
    use super::*;

    #[test]
    fn transient_contention_refuses_consumption_then_clears() {
        let now = std::time::Instant::now();
        let mut busy = SlotContention::new(Duration::from_secs(5));
        assert!(matches!(
            busy.observe("source", true, now),
            Err(CycleError::Retry(_))
        ));
        assert!(matches!(
            busy.observe("alternative", true, now),
            Err(CycleError::Retry(_))
        ));
        assert!(
            busy.observe("source", false, now + Duration::from_secs(1))
                .is_ok()
        );
        assert!(busy.first_busy.is_empty());
        assert!(matches!(
            busy.observe("source", true, now + Duration::from_secs(10)),
            Err(CycleError::Retry(_))
        ));
    }

    #[test]
    fn persistent_contention_expires_without_resetting_on_repeated_observations() {
        let now = std::time::Instant::now();
        let mut busy = SlotContention::new(Duration::from_secs(5));
        for seconds in [0, 1, 4] {
            assert!(matches!(
                busy.observe("source", true, now + Duration::from_secs(seconds)),
                Err(CycleError::Retry(_))
            ));
        }
        assert!(matches!(
            busy.observe("source", true, now + Duration::from_secs(5)),
            Err(CycleError::Fatal(_))
        ));
        assert!(matches!(
            busy.observe("source", true, now + Duration::from_secs(6)),
            Err(CycleError::Fatal(_))
        ));
    }

    #[test]
    fn unsafe_binding_is_fatal_even_when_slot_is_active() {
        let expected = tests::expected_source();
        let mut observed = tests::source_observation();
        observed.active = true;
        observed.system_identifier = "wrong".into();
        assert!(
            validate_source_binding(&expected, &observed, None)
                .unwrap_err()
                .contains("identity mismatch")
        );
        observed.system_identifier = expected.system_identifier.clone();
        observed.wal_status = Some("lost".into());
        assert!(validate_source_binding(&expected, &observed, None).is_err());
        observed.wal_status = Some("reserved".into());
        observed.confirmed_lsn = expected.staged_lsn + 1;
        assert!(
            validate_source_binding(&expected, &observed, None)
                .unwrap_err()
                .contains("ahead")
        );
    }
}
