//! Manually bootstrapped, same-database WAL-backed publication experiment.
//! The default receiver and release bundles do not invoke this binary.
mod direct_retry;
mod direct_spill;
mod direct_stream;
use lake_replication_worker::journal::TransactionAssembler;
use lake_replication_worker::pgoutput::{self, Message};
use lake_replication_worker::sql::sql_literal;
use lake_replication_worker::staging::{
    changes_json, format_lsn, parse_lsn, relation_columns_json, single_value,
};
use lake_replication_worker::wire::Error as WireError;
use lake_replication_worker::wire::{Client, address_is_loopback};
use std::{
    collections::HashMap,
    env,
    io::ErrorKind,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn required(name: &str) -> Result<String> {
    let value = env::var(name)?;
    if value.is_empty() {
        return Err(format!("{name} must be nonempty").into());
    }
    Ok(value)
}
fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn main() {
    let outcome = run();
    // Lifetime diagnostics also capture short apply bursts between samples.
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
        let usage = unsafe { usage.assume_init() };
        let peak_kib = if cfg!(target_os = "macos") {
            usage.ru_maxrss / 1024
        } else {
            usage.ru_maxrss
        };
        let cpu = usage.ru_utime.tv_sec as f64
            + usage.ru_stime.tv_sec as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0;
        eprintln!(
            "direct_resources lifetime_peak_rss_kib={peak_kib} lifetime_cpu_seconds={cpu:.6}"
        );
    }
    if let Err(error) = outcome {
        eprintln!("direct publication failed (unpublished WAL remains unacknowledged): {error}");
        std::process::exit(1);
    }
}

#[derive(Default)]
struct PendingBatch {
    parts: Vec<String>,
    rows: usize,
    charged_bytes: usize,
    commit_lsn: u64,
    end_lsn: u64,
    started: Option<Instant>,
}
impl PendingBatch {
    fn len(&self) -> usize {
        self.parts.len()
    }
    fn encoded_bytes(&self) -> usize {
        let nonempty = self.parts.iter().filter(|p| p.len() > 2).count();
        2 + self.parts.iter().map(|p| p.len() - 2).sum::<usize>() + nonempty.saturating_sub(1)
    }
    fn fits(
        &self,
        payload: &str,
        rows: usize,
        charged: usize,
        max_rows: usize,
        max_bytes: usize,
    ) -> bool {
        self.rows + rows <= max_rows
            && self.charged_bytes + charged <= max_bytes
            && self.encoded_bytes() + payload.len() - 2
                + usize::from(payload.len() > 2 && self.parts.iter().any(|p| p.len() > 2))
                <= max_bytes
    }
    fn payload(&self) -> String {
        let mut out = String::with_capacity(self.encoded_bytes());
        out.push('[');
        for part in self.parts.iter().filter(|p| p.len() > 2) {
            if out.len() > 1 {
                out.push(',');
            }
            out.push_str(&part[1..part.len() - 1]);
        }
        out.push(']');
        out
    }
    fn expired(&self, interval: Duration) -> bool {
        self.started
            .is_some_and(|start| start.elapsed() >= interval)
    }
}
fn flush(
    sink: &mut Client,
    source: &mut Client,
    batch: &mut PendingBatch,
    published: &mut u64,
    reason: &str,
) -> Result<()> {
    if batch.len() == 0 {
        return Ok(());
    }
    let payload = batch.payload();
    let query = format!(
        "SELECT lake_direct.publish({}::pg_lsn,{}::pg_lsn,{}::pg_lsn,{}::jsonb,{})::text",
        sql_literal(&format_lsn(*published)),
        sql_literal(&format_lsn(batch.commit_lsn)),
        sql_literal(&format_lsn(batch.end_lsn)),
        sql_literal(&payload),
        batch.len()
    );
    // Never acknowledge buffered input or an unknown COMMIT outcome.
    direct_retry::transaction(sink, source, *published, |sink, _source| {
        if env::var("LAKE_DIRECT_TRACE_PHASES").as_deref() == Ok("1") {
            // Diagnostic mode adds round trips; it is not a throughput benchmark.
            for (phase, statement) in [
                ("begin", "BEGIN"),
                ("apply", query.as_str()),
                ("commit", "COMMIT"),
            ] {
                let started = Instant::now();
                sink.simple_query(statement)?;
                eprintln!(
                    "direct_phase end_lsn={} phase={phase} elapsed_us={}",
                    format_lsn(batch.end_lsn),
                    started.elapsed().as_micros()
                );
            }
        } else {
            sink.simple_query(&format!("BEGIN; {query}; COMMIT"))?;
        }
        Ok(())
    })?;
    *published = batch.end_lsn;
    if env::var("LAKE_DIRECT_EXIT_BEFORE_FEEDBACK").as_deref() == Ok("1") {
        return Err("injected exit after publication before feedback".into());
    }
    source.standby_status(*published)?;
    eprintln!(
        "direct_published end_lsn={} transactions={} rows={} bytes={} reason={reason}",
        format_lsn(*published),
        batch.len(),
        batch.rows,
        payload.len()
    );
    *batch = PendingBatch::default();
    Ok(())
}

fn run() -> Result<()> {
    if required("LAKE_DIRECT_EXPERIMENT")? != "1" {
        return Err("explicit experiment opt-in required".into());
    }
    direct_retry::limit()?;
    let address = required("LAKE_DIRECT_ADDRESS")?;
    if !address_is_loopback(&address) {
        return Err("prototype accepts loopback connections only".into());
    }
    let database = required("LAKE_DIRECT_DATABASE")?;
    let user = required("LAKE_DIRECT_USER")?;
    let password = env::var("LAKE_DIRECT_PASSWORD").ok();
    let max_commits: u64 = env::var("LAKE_DIRECT_MAX_COMMITS")
        .unwrap_or("1".into())
        .parse()?;
    let max_rows: usize = env::var("LAKE_DIRECT_MAX_ROWS")
        .unwrap_or("4096".into())
        .parse()?;
    let max_bytes: usize = env::var("LAKE_DIRECT_MAX_BYTES")
        .unwrap_or("8388608".into())
        .parse()?;
    if max_commits == 0 || !(1..=4096).contains(&max_rows) || !(1..=8388608).contains(&max_bytes) {
        return Err("invalid prototype bounds".into());
    }
    let interval_ms: u64 = env::var("LAKE_DIRECT_PUBLISH_INTERVAL_MS")
        .unwrap_or("0".into())
        .parse()?;
    let max_batch_transactions: usize = env::var("LAKE_DIRECT_BATCH_MAX_TRANSACTIONS")
        .unwrap_or("32".into())
        .parse()?;
    if interval_ms > 60000 || !(1..=32).contains(&max_batch_transactions) {
        return Err("invalid coalescing bounds".into());
    }
    let interval = Duration::from_millis(interval_ms);
    let mut sink =
        Client::connect_secure(&address, &user, &database, false, password.as_deref(), None)?;
    // Session ownership survives commits, and is released on process/connection loss.
    let locked = sink.simple_query("SELECT pg_try_advisory_lock(1818321765, 1)::text")?;
    if single_value(&locked, "direct stream lock")? != "true" {
        return Err("another direct worker owns this database".into());
    }
    let state = sink.simple_query("SELECT s.slot_name::text,s.publication::text,s.published_lsn::text,s.system_identifier,s.timeline::text FROM lake_direct.stream s JOIN pg_replication_slots r ON r.slot_name=s.slot_name WHERE s.id=1 AND r.plugin='pgoutput' AND r.database=current_database() AND NOT r.active AND r.invalidation_reason IS NULL AND r.restart_lsn<=s.published_lsn AND r.confirmed_flush_lsn<=s.published_lsn AND s.system_identifier=(pg_control_system()).system_identifier::text AND s.timeline=(pg_control_checkpoint()).timeline_id AND s.database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())")?;
    let row = state
        .first()
        .ok_or("slot/history/identity cannot resume from direct checkpoint")?;
    let field = |n: usize| -> Result<&str> {
        row.get(n)
            .and_then(Option::as_deref)
            .ok_or("missing direct state field".into())
    };
    let slot = field(0)?.to_owned();
    let publication = field(1)?.to_owned();
    let mut published = parse_lsn(field(2)?)?;
    let mut source =
        Client::connect_secure(&address, &user, &database, true, password.as_deref(), None)?;
    let identity = source.simple_query("IDENTIFY_SYSTEM")?;
    let identity = identity.first().ok_or("missing source identity")?;
    if identity.first().and_then(Option::as_deref) != Some(field(3)?)
        || identity.get(1).and_then(Option::as_deref) != Some(field(4)?)
    {
        return Err("replication connection identity differs from sink".into());
    }
    let publication_valid = sink.simple_query("SELECT (NOT p.puballtables AND p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate AND NOT p.pubviaroot AND NOT EXISTS(SELECT FROM pg_publication_rel r WHERE r.prpubid=p.oid AND (r.prqual IS NOT NULL OR r.prattrs IS NOT NULL)) AND NOT EXISTS((SELECT prrelid FROM pg_publication_rel WHERE prpubid=p.oid EXCEPT SELECT source_relation::oid FROM lake_direct.mapping) UNION ALL (SELECT source_relation::oid FROM lake_direct.mapping EXCEPT SELECT prrelid FROM pg_publication_rel WHERE prpubid=p.oid)) AND EXISTS(SELECT FROM lake_direct.mapping))::text FROM pg_publication p JOIN lake_direct.stream s ON s.publication=p.pubname")?;
    if single_value(&publication_valid, "explicit unfiltered publication")? != "true" {
        return Err("prototype requires an exact, unfiltered application-table publication".into());
    }
    if let Ok(point) = env::var("LAKE_DIRECT_CRASH_POINT") {
        sink.simple_query(&format!(
            "SET pg_lake_table.lab_crash_point={}",
            sql_literal(&point)
        ))?;
    }
    if env::var("LAKE_DIRECT_STREAMING").as_deref() == Ok("1") {
        return direct_stream::run(
            sink,
            source,
            published,
            &slot,
            &publication,
            max_commits,
            max_rows,
            max_bytes,
        );
    }
    let replay = sink.simple_query("SELECT (replay_lsn IS NULL OR replay_lsn=published_lsn)::text FROM lake_direct.stream WHERE id=1")?;
    if single_value(&replay, "stream replay boundary")? != "true" {
        return Err("unfinished streamed transaction requires streaming recovery".into());
    }
    let mut versions = HashMap::new();
    let mut relation_sizes = HashMap::new();
    // No streaming into durable staging: oversized committed transactions fail
    // the buffer limits below. pgoutput retains WAL; no spill is acknowledged.
    source.start_replication(&format!("START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {}, streaming 'false')",ident(&slot),format_lsn(published),format!("'{}'", ident(&publication).replace('\'', "''"))))?;
    if interval_ms > 0 {
        source.set_read_timeout(Some(Duration::from_millis(interval_ms.min(100))))?;
    }
    let mut batch = PendingBatch::default();
    let mut last_feedback = Instant::now();
    eprintln!("direct_started pid={}", std::process::id());
    let mut assembler = TransactionAssembler::default();
    let mut bytes = 0usize;
    let mut rows = 0usize;
    let mut committed = 0u64;
    loop {
        if batch.expired(interval) {
            flush(
                &mut sink,
                &mut source,
                &mut batch,
                &mut published,
                "interval",
            )?;
        }
        let frame = match source.read_copy_data() {
            Ok(frame) => frame,
            Err(WireError::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                if last_feedback.elapsed() >= Duration::from_secs(1) {
                    source.standby_status(published)?;
                    last_feedback = Instant::now();
                }
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match frame.first() {
            Some(b'k') if frame.len() == 18 => {
                if frame[17] != 0 {
                    source.standby_status(published)?;
                }
            }
            Some(b'w') if frame.len() >= 25 => {
                let logical = &frame[25..];
                if logical.len() > max_bytes {
                    return Err("logical message exceeds prototype byte limit".into());
                }
                let message = pgoutput::decode(logical)?;
                match message {
                    Message::Begin(begin) => {
                        bytes = 0;
                        rows = 0;
                        assembler.begin(u64::from_be_bytes(frame[1..9].try_into()?), begin.xid)?;
                    }
                    Message::Relation(relation) => {
                        let descriptor = format!(
                            "jsonb_build_object('namespace',{},'name',{},'replica_identity',{},'columns',{}::jsonb)",
                            sql_literal(&relation.namespace),
                            sql_literal(&relation.name),
                            sql_literal(&(relation.replica_identity as char).to_string()),
                            sql_literal(&relation_columns_json(&relation))
                        );
                        let valid=sink.simple_query(&format!("SELECT (descriptor={descriptor})::text FROM lake_direct.mapping WHERE source_relation={}::oid",relation.id))?;
                        if single_value(&valid, "frozen relation descriptor")? != "true" {
                            return Err("frozen relation descriptor changed".into());
                        }
                        if versions.len() >= 64 && !versions.contains_key(&relation.id) {
                            return Err("prototype relation limit exceeded".into());
                        }
                        relation_sizes.insert(relation.id, logical.len());
                        versions.insert(relation.id, 1);
                        assembler.relation(relation);
                    }
                    Message::Commit(commit) => {
                        let transaction = assembler.commit(commit)?;
                        if transaction.end_lsn <= published {
                            source.standby_status(published)?;
                            continue;
                        }
                        if batch.len() > 0 && transaction.end_lsn <= batch.end_lsn {
                            return Err("source commit order regressed".into());
                        }
                        let payload = changes_json(&transaction, &versions)?;
                        if payload.len() > max_bytes {
                            return Err(
                                "serialized transaction exceeds prototype byte limit".into()
                            );
                        }
                        let charged = bytes.max(payload.len());
                        if !batch.fits(&payload, rows, charged, max_rows, max_bytes) {
                            flush(
                                &mut sink,
                                &mut source,
                                &mut batch,
                                &mut published,
                                "buffer_pressure",
                            )?;
                        }
                        if !batch.fits(&payload, rows, charged, max_rows, max_bytes) {
                            return Err("transaction exceeds prototype buffer limits".into());
                        }
                        if batch.started.is_none() {
                            batch.started = Some(Instant::now());
                        }
                        batch.rows += rows;
                        batch.charged_bytes += charged;
                        batch.commit_lsn = transaction.commit_lsn;
                        batch.end_lsn = transaction.end_lsn;
                        batch.parts.push(payload);
                        eprintln!(
                            "direct_buffered end_lsn={} transactions={}",
                            format_lsn(batch.end_lsn),
                            batch.len()
                        );
                        committed += 1;
                        if interval_ms == 0
                            || batch.len() >= max_batch_transactions
                            || committed >= max_commits
                        {
                            let reason = if interval_ms == 0 {
                                "immediate"
                            } else if committed >= max_commits {
                                "exit"
                            } else {
                                "transactions"
                            };
                            flush(&mut sink, &mut source, &mut batch, &mut published, reason)?;
                            last_feedback = Instant::now();
                        }
                        if committed >= max_commits {
                            return Ok(());
                        }
                    }
                    other => {
                        // Include cloned relation metadata in the conservative buffer charge.
                        let (relation_id, count) = match &other {
                            Message::Insert(v) => (v.relation_id, 1),
                            Message::Update(v) => (v.relation_id, 1),
                            Message::Delete(v) => (v.relation_id, 1),
                            Message::Truncate(v) => (0, v.relation_ids.len()),
                            _ => {
                                return Err(
                                    "streamed protocol is unsupported by the direct prototype"
                                        .into(),
                                );
                            }
                        };
                        rows = rows.checked_add(count).ok_or("row count overflow")?;
                        bytes = bytes
                            .checked_add(
                                logical.len()
                                    + relation_sizes.get(&relation_id).copied().unwrap_or(0),
                            )
                            .ok_or("byte count overflow")?;
                        // Flush only previously completed transactions under aggregate
                        // pressure; never include the in-progress transaction in feedback.
                        if batch.rows + rows > max_rows || batch.charged_bytes + bytes > max_bytes {
                            flush(
                                &mut sink,
                                &mut source,
                                &mut batch,
                                &mut published,
                                "buffer_pressure",
                            )?;
                        }
                        if rows > max_rows || bytes > max_bytes {
                            return Err(
                                "transaction exceeds prototype buffer limits; WAL not acknowledged"
                                    .into(),
                            );
                        }
                        match other {
                            Message::Insert(v) => assembler.insert(v)?,
                            Message::Update(v) => assembler.update(v)?,
                            Message::Delete(v) => assembler.delete(v)?,
                            Message::Truncate(v) => assembler.truncate(v)?,
                            _ => unreachable!(),
                        }
                    }
                }
            }
            _ => return Err("invalid replication frame".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flattening_keeps_transaction_and_change_order() {
        let batch = PendingBatch {
            parts: vec![
                "[]".into(),
                "[{\"id\":1}]".into(),
                "[{\"id\":2},{\"id\":3}]".into(),
            ],
            ..Default::default()
        };
        assert_eq!(batch.payload(), "[{\"id\":1},{\"id\":2},{\"id\":3}]");
        assert_eq!(batch.payload().len(), batch.encoded_bytes());
        assert_eq!(batch.len(), 3);
    }
    #[test]
    fn bounds_cover_pending_and_next_complete_transaction() {
        let batch = PendingBatch {
            parts: vec!["[{}]".into()],
            rows: 2,
            charged_bytes: 10,
            ..Default::default()
        };
        assert!(batch.fits("[{}]", 2, 10, 4, 20));
        assert!(!batch.fits("[{}]", 3, 10, 4, 20));
        assert!(!batch.fits("[{}]", 2, 11, 4, 20));
        assert!(!PendingBatch::default().fits("[{\"large\":123456789}]", 1, 1, 4, 10));
    }
    #[test]
    fn idle_deadline_starts_at_first_buffered_commit() {
        assert!(!PendingBatch::default().expired(Duration::ZERO));
        let batch = PendingBatch {
            started: Some(Instant::now() - Duration::from_secs(2)),
            ..Default::default()
        };
        assert!(batch.expired(Duration::from_secs(1)));
        assert!(!batch.expired(Duration::from_secs(10)));
    }
}
