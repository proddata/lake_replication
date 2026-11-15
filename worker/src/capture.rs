//! Optional source-side driver. Decoder callbacks never execute pg_lake SQL.
use lake_replication_worker::{
    credentials::Settings,
    logging::event,
    push_import::{hex, unhex},
    push_protocol::{
        Batch, Change, Control, Event, MAX_BATCH, MAX_EVENTS, Stream, Table, Transaction, Value,
    },
    push_schema::{Definition, State as SchemaState},
    sql::sql_literal,
    staging::{format_lsn, json_string, single_value},
    wire::Client,
};
use sha2::{Digest, Sha256};
use std::{
    env, thread,
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
struct LeaseBusy;
impl std::fmt::Display for LeaseBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("duplicate capture worker lease")
    }
}
impl std::error::Error for LeaseBusy {}

fn failure_sqlstate(error: &(dyn std::error::Error + 'static)) -> Option<String> {
    let lake_replication_worker::wire::Error::Server(message) =
        error.downcast_ref::<lake_replication_worker::wire::Error>()?
    else {
        return None;
    };
    // The wire client places native severity/code fields before the message.
    // A message or detail containing "C=..." is not a SQLSTATE field.
    message
        .split(" M=")
        .next()?
        .split_ascii_whitespace()
        .find_map(|field| field.strip_prefix("C="))
        .filter(|code| {
            code.len() == 5
                && code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        })
        .map(str::to_owned)
}

fn required(name: &str) -> Result<String> {
    Ok(env::var(name).map_err(|_| format!("required environment variable {name} is not set"))?)
}
fn field(row: &[Option<String>], index: usize) -> Result<&str> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| "missing capture configuration field".into())
}
fn uuid(text: &str) -> Result<[u8; 16]> {
    unhex(&text.replace('-', ""), 16)?
        .try_into()
        .map_err(|_| "invalid capture UUID".into())
}

struct Config {
    stream: Stream,
    tables: [Table; 2],
    table_oids: [u32; 2],
    progress_oid: u32,
    schema_oid: u32,
    schema_auditing: bool,
    names: [String; 2],
    slot: String,
    baseline: Option<String>,
    sequence: u64,
    end_lsn: u64,
    max_wal: u64,
    max_spill: u64,
    decode_work_mem_kb: u32,
    transactions_per_read: u32,
    publication_interval_ms: u64,
    bootstrap: String,
    snapshot_state: String,
}
fn config(source: &mut Client, id: &str, generation: i64) -> Result<Config> {
    let rows=source.simple_query(&format!("SELECT replace(stream_id::text,'-',''),origin_system_identifier::text,
        origin_database_oid::text,capture_generation::text,replace(table_a_id::text,'-',''),replace(table_b_id::text,'-',''),
        table_a::oid::text,table_b::oid::text,
        (SELECT format('%I.%I',n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=table_a),
        (SELECT format('%I.%I',n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=table_b),
        slot_name::text,baseline_lsn::text,
        captured_sequence::text,captured_end_lsn::text,max_wal_bytes::text,bootstrap_mode,snapshot_state,max_decode_spill_bytes::text,decode_work_mem_kb::text,transactions_per_read::text,publication_interval_ms::text,
        'lake_capture.progress_requests'::regclass::oid::text,schema_auditing::text,
        'lake_capture.schema_events'::regclass::oid::text FROM lake_capture.streams
        WHERE stream_id={}::uuid AND config_generation={generation} AND desired_state='running'
        AND owner=current_user::regrole AND bound_postmaster=pg_postmaster_start_time()
        AND origin_system_identifier=(SELECT system_identifier FROM pg_control_system())
        AND origin_database_oid=(SELECT oid FROM pg_database WHERE datname=current_database())",sql_literal(id)))?;
    if rows.len() != 1 {
        return Err("capture worker is stopped, copied or obsolete".into());
    }
    let r = &rows[0];
    Ok(Config {
        stream: Stream {
            id: uuid(field(r, 0)?)?,
            system_identifier: field(r, 1)?.parse()?,
            database_oid: field(r, 2)?.parse()?,
            generation: field(r, 3)?.parse()?,
        },
        tables: [
            Table {
                id: uuid(field(r, 4)?)?,
                schema_version: 1,
            },
            Table {
                id: uuid(field(r, 5)?)?,
                schema_version: 1,
            },
        ],
        table_oids: [field(r, 6)?.parse()?, field(r, 7)?.parse()?],
        progress_oid: field(r, 21)?.parse()?,
        schema_auditing: field(r, 22)? == "true",
        schema_oid: field(r, 23)?.parse()?,
        names: [field(r, 8)?.into(), field(r, 9)?.into()],
        slot: field(r, 10)?.into(),
        baseline: r[11].clone(),
        sequence: field(r, 12)?.parse()?,
        end_lsn: lake_replication_worker::staging::parse_lsn(field(r, 13)?)?,
        max_wal: field(r, 14)?.parse()?,
        max_spill: field(r, 17)?.parse()?,
        decode_work_mem_kb: field(r, 18)?.parse()?,
        transactions_per_read: field(r, 19)?.parse()?,
        publication_interval_ms: field(r, 20)?.parse()?,
        bootstrap: field(r, 15)?.into(),
        snapshot_state: field(r, 16)?.into(),
    })
}

fn capture_envelopes(rows: &[Vec<Option<String>>]) -> Result<Vec<Vec<u8>>> {
    let mut envelopes: Vec<Vec<u8>> = Vec::new();
    let mut next_part = 0;
    let mut bytes = 0;
    for row in rows {
        let message: usize = field(row, 0)?.parse()?;
        let part: usize = field(row, 1)?.parse()?;
        if message == envelopes.len() + 1 {
            if envelopes.len() == 32 || part != 0 {
                return Err("capture message window gap or count budget exceeded".into());
            }
            envelopes.push(Vec::new());
            next_part = 0;
        }
        if message != envelopes.len() || part != next_part || message == 0 {
            return Err("capture message/chunk ordering gap".into());
        }
        let chunk = unhex(field(row, 2)?, 4 * 1024 * 1024)?;
        if chunk.is_empty() || bytes + chunk.len() > MAX_BATCH {
            return Err("capture read envelope byte budget exceeded".into());
        }
        bytes += chunk.len();
        envelopes.last_mut().unwrap().extend(chunk);
        next_part += 1;
    }
    Ok(envelopes)
}

fn ensure_slot(source: &mut Client, id: &str, generation: i64, cfg: &Config) -> Result<()> {
    let rows = source.simple_query(&format!(
        "SELECT confirmed_flush_lsn::text,plugin, database::text,
        coalesce(invalidation_reason,'') FROM pg_replication_slots WHERE slot_name={}",
        sql_literal(&cfg.slot)
    ))?;
    if cfg.baseline.is_none() && matches!(cfg.bootstrap.as_str(), "paused_copy" | "online_copy") {
        if cfg.snapshot_state != "none" || !rows.is_empty() {
            return Err(
                "incomplete snapshot generation requires controlled reset and recopy".into(),
            );
        }
        return paused_snapshot(source, id, generation, cfg);
    }
    if cfg.baseline.is_some() && rows.is_empty() {
        return Err(
            "capture history lost: owned slot is missing; rebuild snapshot generation".into(),
        );
    }
    let baseline = if let Some(row) = rows.first() {
        if field(row, 1)? != "lake_replication_capture"
            || field(row, 2)? != required("LAKE_SOURCE_DATABASE")?
            || field(row, 3)? != ""
        {
            return Err("capture slot identity differs or history is invalidated".into());
        }
        field(row, 0)?.to_owned()
    } else {
        // No writes or row locks before logical-slot creation. The selected
        // tables stay locked through the consistency boundary. App writes
        // after this COMMIT are protected by the new persistent slot.
        source.simple_query("BEGIN")?;
        source.simple_query(&format!(
            "LOCK TABLE {},{} IN SHARE MODE",
            cfg.names[0], cfg.names[1]
        ))?;
        let count = source.simple_query(&format!(
            "SELECT (SELECT count(*) FROM {})+(SELECT count(*) FROM {})",
            cfg.names[0], cfg.names[1]
        ))?;
        if single_value(&count, "empty activation")? != "0" {
            source.simple_query("ROLLBACK")?;
            return Err("empty-start capture requires paused empty selected tables; existing rows require snapshot support".into());
        }
        let slot=source.simple_query(&format!("SELECT lsn::text FROM pg_create_logical_replication_slot({},'lake_replication_capture')",sql_literal(&cfg.slot)))?;
        let baseline = single_value(&slot, "capture slot creation")?.to_owned();
        source.simple_query("COMMIT")?;
        baseline
    };
    if cfg.baseline.is_none() {
        source.simple_query(&format!("BEGIN;SELECT lake_capture.assert_capture({}::uuid,{generation});
            UPDATE lake_capture.streams SET baseline_lsn={}::pg_lsn,captured_end_lsn={}::pg_lsn WHERE stream_id={}::uuid;COMMIT",
            sql_literal(id),sql_literal(&baseline),sql_literal(&baseline),sql_literal(id)))?;
    }
    Ok(())
}

fn upload_bytes(batch: &Batch) -> Result<usize> {
    let (header, records) = batch.log_parts()?;
    Ok(header.len() * 2
        + 64
        + 4096
        + records
            .iter()
            .map(|(_, _, event)| event.len() * 2)
            .sum::<usize>())
}

fn reserve_upload(source: &mut Client, id: &str, generation: i64, batch: &Batch) -> Result<String> {
    let bytes = upload_bytes(batch)?;
    let rows = source.simple_query(&format!(
        "SELECT lake_capture.reserve_upload({}::uuid,{generation},{}::pg_lsn,{},{} )::text",
        sql_literal(id),
        sql_literal(&format_lsn(batch.manifest.transaction.end_lsn)),
        sql_literal(&hex(&batch.manifest.digest)),
        bytes
    ))?;
    Ok(single_value(&rows, "durable upload reservation")?.into())
}

fn source_connection(id: &str, generation: i64, replication: bool) -> Result<Client> {
    let settings = Settings::from_env()?;
    let mut source = Client::connect_named_secure(
        &required("LAKE_SOURCE_ADDRESS")?,
        &settings.required("LAKE_SOURCE_USER")?,
        &required("LAKE_SOURCE_DATABASE")?,
        replication,
        &format!("lake_capture_budget/{id}/{generation}"),
        settings.optional("LAKE_SOURCE_PASSWORD").as_deref(),
        None,
    )?;
    source.simple_query("SET default_transaction_isolation='read committed';SET statement_timeout='30s';SET lock_timeout='10s'")?;
    Ok(source)
}

// The durable capture receipt can be ahead of confirmed_flush_lsn after a
// lost COMMIT response. Decode from the version state at that confirmed boundary,
// then replay every later immutable audit event in source order.
fn confirmed_schema_state(source: &mut Client, cfg: &Config) -> Result<SchemaState> {
    let rows = source.simple_query(&format!(
        "SELECT coalesce(r.table_a_version,1)::text,coalesce(r.table_b_version,1)::text,
            encode(a.definition_bytes,'hex'),encode(b.definition_bytes,'hex')
         FROM lake_capture.streams s JOIN pg_replication_slots slot ON slot.slot_name=s.slot_name
         LEFT JOIN LATERAL (SELECT table_a_version,table_b_version FROM lake_capture.receipts
            WHERE stream_id=s.stream_id AND end_lsn<=slot.confirmed_flush_lsn ORDER BY end_lsn DESC LIMIT 1) r ON true
         LEFT JOIN lake_capture.schema_definitions a ON a.stream_id=s.stream_id AND a.capture_generation=s.capture_generation
            AND a.table_id=s.table_a_id AND a.schema_version=coalesce(r.table_a_version,1)
         LEFT JOIN lake_capture.schema_definitions b ON b.stream_id=s.stream_id AND b.capture_generation=s.capture_generation
            AND b.table_id=s.table_b_id AND b.schema_version=coalesce(r.table_b_version,1)
         WHERE s.stream_id={}::uuid AND slot.confirmed_flush_lsn IS NOT NULL",sql_literal(&uuid_text(&cfg.stream.id))))?;
    if rows.len() != 1 {
        return Err("missing confirmed schema boundary".into());
    }
    let row = &rows[0];
    Ok(SchemaState::new([
        (
            cfg.tables[0].id,
            field(row, 0)?.parse()?,
            Definition::decode(&unhex(field(row, 2)?, 1024 * 1024)?)?,
        ),
        (
            cfg.tables[1].id,
            field(row, 1)?.parse()?,
            Definition::decode(&unhex(field(row, 3)?, 1024 * 1024)?)?,
        ),
    ])?)
}

fn schema_versions(state: Option<&SchemaState>, cfg: &Config) -> Result<[u32; 2]> {
    match state {
        None => Ok([1, 1]),
        Some(state) => Ok([
            state
                .definition(&cfg.tables[0].id)
                .ok_or("missing table a schema state")?
                .0,
            state
                .definition(&cfg.tables[1].id)
                .ok_or("missing table b schema state")?
                .0,
        ]),
    }
}

fn publication_json(
    batch: &Batch,
    raw_hash: &str,
    reservation: &str,
    versions: [u32; 2],
) -> Result<String> {
    let (header, records) = batch.log_parts()?;
    let events = records
        .iter()
        .map(|(table, ordinal, payload)| {
            let table = table
                .map(|table| uuid_text(&table.id))
                .unwrap_or("control".into());
            format!(
                "{{\"table\":{},\"ordinal\":{ordinal},\"hex\":{}}}",
                json_string(&table),
                json_string(&hex(payload))
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "{{\"sequence\":{},\"batch_id\":{},\"commit_lsn\":{},\"end_lsn\":{},\"raw_hash\":{},\"header\":{},\"digest\":{},\"events\":[{events}],\"reservation\":{},\"table_a_version\":{},\"table_b_version\":{}}}",
        batch.manifest.sequence,
        json_string(&uuid_text(&batch.manifest.batch_id)),
        json_string(&format_lsn(batch.manifest.transaction.commit_lsn)),
        json_string(&format_lsn(batch.manifest.transaction.end_lsn)),
        json_string(raw_hash),
        json_string(&hex(&header)),
        json_string(&hex(&batch.manifest.digest)),
        json_string(reservation),
        versions[0],
        versions[1]
    ))
}

fn publish_encoded_group(
    source: &mut Client,
    id: &str,
    generation: i64,
    members: &str,
) -> Result<()> {
    if let Ok(point) = env::var("LAKE_CAPTURE_TEST_CRASH") {
        source.simple_query(&format!(
            "SET pg_lake_table.lab_crash_point={}",
            sql_literal(&point)
        ))?;
    }
    source.simple_query(&format!(
        "SELECT lake_capture.publish_group({}::uuid,{generation},{}::jsonb)",
        sql_literal(id),
        sql_literal(&format!("[{members}]"))
    ))?;
    Ok(())
}

fn publish(
    source: &mut Client,
    id: &str,
    generation: i64,
    batch: &Batch,
    raw_hash: &str,
    reservation: &str,
) -> Result<()> {
    publish_encoded_group(
        source,
        id,
        generation,
        &publication_json(batch, raw_hash, reservation, [1, 1])?,
    )
}

fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn require_snapshot_slot(rows: &[Vec<Option<String>>], slot: &str) -> Result<()> {
    if rows.len() != 1
        || field(&rows[0], 0)? != slot
        || field(&rows[0], 3)? != "lake_replication_capture"
        || field(&rows[0], 2)?.is_empty()
    {
        return Err("slot creation did not export a consistent snapshot".into());
    }
    Ok(())
}

/// One bounded paused generation. SHARE locks are acquired before reading any
/// table rows and held through slot creation, log publication and activation.
fn paused_snapshot(source: &mut Client, id: &str, generation: i64, cfg: &Config) -> Result<()> {
    source.simple_query(&format!(
        "BEGIN;SELECT lake_capture.assert_capture({}::uuid,{generation});
        UPDATE lake_capture.streams SET snapshot_state='building' WHERE stream_id={}::uuid;COMMIT",
        sql_literal(id),
        sql_literal(id)
    ))?;
    let online = cfg.bootstrap == "online_copy";
    let mut online_baseline = None;
    if online {
        let mut exporter = source_connection(id, generation, true)?;
        let slot = exporter.simple_query(&format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL lake_replication_capture (SNAPSHOT 'export')",
            identifier(&cfg.slot)
        ))?;
        require_snapshot_slot(&slot, &cfg.slot)?;
        online_baseline = Some(lake_replication_worker::staging::parse_lsn(field(
            &slot[0], 1,
        )?)?);
        source.simple_query(&format!(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;SET TRANSACTION SNAPSHOT {}",
            sql_literal(field(&slot[0], 2)?)
        ))?;
        // Import completes before closing the exporting replication connection.
        // All baseline reads now share the slot's exact consistent snapshot.
        drop(exporter);
    } else {
        source.simple_query(
            "BEGIN; SET LOCAL lock_timeout='10s'; SET LOCAL statement_timeout='30s'",
        )?;
        source.simple_query(&format!(
            "LOCK TABLE {},{} IN SHARE MODE",
            cfg.names[0], cfg.names[1]
        ))?;
    }
    let mut plans = Vec::new();
    let mut total_rows = 0usize;
    let mut estimated_bytes = 0usize;
    for index in 0..2 {
        let attributes = source.simple_query(&format!(
            "SELECT a.attname,(a.attnum=ANY((i.indkey::smallint[])[0:i.indnkeyatts-1]))::text
            FROM pg_attribute a JOIN pg_index i ON i.indrelid=a.attrelid AND i.indisprimary
            WHERE a.attrelid={}::oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
            cfg.table_oids[index]
        ))?;
        let columns = attributes
            .iter()
            .map(|row| field(row, 0).map(identifier))
            .collect::<Result<Vec<_>>>()?;
        let keys = attributes
            .iter()
            .filter(|row| row.get(1).and_then(Option::as_deref) == Some("true"))
            .map(|row| field(row, 0).map(identifier))
            .collect::<Result<Vec<_>>>()?;
        if keys.is_empty() || columns.is_empty() {
            return Err("snapshot requires registered primary-key columns".into());
        }
        let sizes = columns
            .iter()
            .map(|name| format!("coalesce(octet_length({name}::text),0)"))
            .collect::<Vec<_>>()
            .join("+");
        let sizes = format!("64::numeric+{}+{sizes}", columns.len() * 64);
        let remaining = MAX_EVENTS - 4 - total_rows;
        let inventory = source.simple_query(&format!(
            "SELECT count(*)::text FROM (SELECT 1 FROM {} LIMIT {}) bounded_rows",
            cfg.names[index],
            remaining + 1
        ))?;
        let rows = single_value(&inventory, "bounded snapshot row inventory")?.parse::<usize>()?;
        if rows > remaining {
            return Err(
                "snapshot exceeds bounded generation row budget; no partial baseline is activated"
                    .into(),
            );
        }
        let inventory = source.simple_query(&format!(
            "SELECT coalesce(sum({sizes}),0)::text FROM {}",
            cfg.names[index]
        ))?;
        total_rows += rows;
        estimated_bytes += single_value(&inventory, "snapshot byte inventory")?.parse::<usize>()?;
        if estimated_bytes > MAX_BATCH - 2048 {
            return Err(
                "snapshot exceeds bounded generation byte budget; no partial baseline is activated"
                    .into(),
            );
        }
        plans.push((columns, keys));
    }
    let baseline = if let Some(baseline) = online_baseline {
        baseline
    } else {
        let slot = source.simple_query(&format!(
            "SELECT lsn::text FROM pg_create_logical_replication_slot({},'lake_replication_capture')",
            sql_literal(&cfg.slot)
        ))?;
        lake_replication_worker::staging::parse_lsn(single_value(
            &slot,
            "snapshot consistency boundary",
        )?)?
    };
    let mut token = cfg.stream.generation.to_be_bytes().to_vec();
    token.extend(baseline.to_be_bytes());
    let mut events = vec![Event {
        ordinal: 0,
        table: None,
        change: Change::Control(Control::SnapshotBegin, token.clone()),
    }];
    for (index, (columns, keys)) in plans.iter().enumerate() {
        let columns = columns
            .iter()
            .map(|name| format!("{name}::text"))
            .collect::<Vec<_>>()
            .join(",");
        let rows = source.simple_query(&format!(
            "SELECT {columns} FROM {} ORDER BY {} LIMIT {}",
            cfg.names[index],
            keys.join(","),
            MAX_EVENTS
        ))?;
        for row in rows {
            let values = row
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    (
                        (index + 1) as u32,
                        value.map(Value::Text).unwrap_or(Value::Null),
                    )
                })
                .collect();
            events.push(Event {
                ordinal: events.len() as u32,
                table: Some(cfg.tables[index]),
                change: Change::Insert(values),
            });
        }
    }
    if events.len() != total_rows + 1 {
        return Err("snapshot row inventory changed under table locks".into());
    }
    let mut inventory = token.clone();
    inventory.extend((total_rows as u32).to_be_bytes());
    for (kind, payload) in [
        (Control::SnapshotChunk, inventory),
        (Control::SnapshotComplete, token.clone()),
        (Control::SnapshotActivate, token),
    ] {
        events.push(Event {
            ordinal: events.len() as u32,
            table: None,
            change: Change::Control(kind, payload),
        });
    }
    let batch = Batch::new(
        cfg.stream.clone(),
        1,
        Transaction {
            xid: 0,
            begin_lsn: baseline - 1,
            commit_lsn: baseline - 1,
            end_lsn: baseline,
            commit_time_micros: 0,
        },
        events,
    )?;
    if env::var("LAKE_CAPTURE_TEST_SNAPSHOT_STOP").as_deref() == Ok("before-publication") {
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
    }
    if online {
        source.simple_query("COMMIT")?;
    }
    let raw_hash = hex(&Sha256::digest(batch.encode()?));
    // The snapshot connection holds table locks. A separate short transaction
    // durably reserves upload budget before any object-store publication.
    let reservation = reserve_upload(
        &mut source_connection(id, generation, false)?,
        id,
        generation,
        &batch,
    )?;
    if online {
        source.simple_query("BEGIN")?;
    }
    publish(source, id, generation, &batch, &raw_hash, &reservation)?;
    source.simple_query("COMMIT")?;
    event(
        "capture",
        "INFO",
        "snapshot_captured",
        "complete consistent baseline captured",
        &[("rows", total_rows.to_string())],
    );
    Ok(())
}

/// All importer reads share this database-local gate. Disable automatic
/// vacuum on private logs so expiry/deletion runs only under the exclusive
/// gate, including when a reader has not opened the Iceberg relations yet.
fn maintain(source: &mut Client, id: &str, generation: i64) -> Result<()> {
    let acquired = source.simple_query("SELECT pg_try_advisory_lock(1280593921,1)::text")?;
    if single_value(&acquired, "maintenance gate")? != "true" {
        return Ok(());
    }
    let result: Result<()> = (|| {
        let reserved = source.simple_query(&format!(
            "SELECT lake_capture.reserve_maintenance({}::uuid,{generation})::text",
            sql_literal(id)
        ))?;
        if single_value(&reserved, "maintenance budget")? != "true" {
            event(
                "capture",
                "WARN",
                "maintenance_budget_exhausted",
                "generation maintenance budget exhausted; configure or rebuild explicitly",
                &[],
            );
            return Ok(());
        }
        for table in ["events_a", "events_b", "controls", "manifests"] {
            source.simple_query(&format!("VACUUM lake_capture_log.{table}"))?;
        }
        event(
            "capture",
            "INFO",
            "logs_maintained",
            "private log compaction and delayed cleanup completed",
            &[],
        );
        Ok(())
    })();
    let unlocked = source.simple_query("SELECT pg_advisory_unlock(1280593921,1)");
    result?;
    unlocked?;
    Ok(())
}

fn run() -> Result<()> {
    let settings = Settings::from_env()?;
    let id = required("LAKE_CAPTURE_STREAM")?;
    uuid(&id)?;
    let generation: i64 = required("LAKE_CONFIG_GENERATION")?.parse()?;
    let chunk_bytes: usize = env::var("LAKE_CAPTURE_TEST_CHUNK_BYTES")
        .unwrap_or_else(|_| "4194304".into())
        .parse()?;
    if chunk_bytes == 0 || chunk_bytes > 4194304 {
        return Err("invalid capture transport chunk budget".into());
    }
    let read_events: u32 = env::var("LAKE_CAPTURE_TEST_READ_EVENTS")
        .unwrap_or_else(|_| MAX_EVENTS.to_string())
        .parse()?;
    if read_events == 0 || read_events > MAX_EVENTS as u32 {
        return Err("invalid capture read event budget".into());
    }
    let address = required("LAKE_SOURCE_ADDRESS")?;
    if !lake_replication_worker::wire::address_is_loopback(&address) {
        return Err("source capture must connect to its local source server".into());
    }
    let mut source = Client::connect_named_secure(
        &address,
        &settings.required("LAKE_SOURCE_USER")?,
        &required("LAKE_SOURCE_DATABASE")?,
        false,
        &format!("lake_capture/{id}/{generation}"),
        settings.optional("LAKE_SOURCE_PASSWORD").as_deref(),
        None,
    )?;
    source.simple_query("SET client_connection_check_interval='100ms';SET default_transaction_isolation='read committed';SET statement_timeout='30s';SET lock_timeout='10s'")?;
    let lock = source.simple_query(&format!(
        "SELECT pg_try_advisory_lock(hashtextextended({},0))::text",
        sql_literal(&format!("lake_capture/{id}"))
    ))?;
    if single_value(&lock, "capture lease")? != "true" {
        return Err(Box::new(LeaseBusy));
    }
    let result: Result<()> = (|| {
        source.simple_query("SELECT lake_capture.lock_private_logs(true)")?;
        let initial = config(&mut source, &id, generation)?;
        source.simple_query(&format!(
            "SELECT lake_capture.assert_capture({}::uuid,{generation})",
            sql_literal(&id)
        ))?;
        ensure_slot(&mut source, &id, generation, &initial)?;
        if env::var("LAKE_CAPTURE_MAINTENANCE_ONLY").as_deref() == Ok("1") {
            return maintain(&mut source, &id, generation);
        }
        let mut last_maintenance = Instant::now();
        loop {
            if last_maintenance.elapsed() >= Duration::from_secs(300) {
                // Failure stops this configuration rather than retrying object
                // work indefinitely. The reservation survives the failure.
                maintain(&mut source, &id, generation)?;
                last_maintenance = Instant::now();
            }
            let cfg = config(&mut source, &id, generation)?;
            let retained=source.simple_query(&format!("SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_insert_lsn(),restart_lsn),0)::text FROM pg_replication_slots WHERE slot_name={}",sql_literal(&cfg.slot)))?;
            if single_value(&retained, "retained source WAL")?.parse::<u64>()? > cfg.max_wal {
                return Err("capture source WAL budget exceeded; stop/rebuild before required history is lost".into());
            }
            let mut schemas = if cfg.schema_auditing {
                Some(confirmed_schema_state(&mut source, &cfg)?)
            } else {
                None
            };
            let initial_versions = schema_versions(schemas.as_ref(), &cfg)?;
            let mut read_limit = cfg.transactions_per_read;
            if cfg.publication_interval_ms > 0 {
                thread::sleep(Duration::from_millis(cfg.publication_interval_ms));
            }
            let rows = loop {
                let result = source.simple_query(&format!(
                    "SET logical_decoding_work_mem='{}kB';SELECT decoded.message_index::text,chunk.part::text,encode(substring(data FROM chunk.part*{chunk_bytes}+1 FOR {chunk_bytes}),'hex')
                    FROM pg_logical_slot_peek_binary_changes({},NULL,{read_limit},
                    'table-a-oid',{},'table-b-oid',{},'table-a-id',{},'table-b-id',{},'max-spill-bytes',{},'test-spill-stop',{},'max-read-events',{},
                    'progress-table-oid',{},'stream-id',{},'capture-generation',{},
                    'schema-table-oid',{},'table-a-version',{},'table-b-version',{})
                    WITH ORDINALITY AS decoded(lsn,xid,data,message_index)
                    CROSS JOIN LATERAL generate_series(0,(octet_length(data)-1)/{chunk_bytes}) AS chunk(part)
                    ORDER BY decoded.message_index,chunk.part",
                    cfg.decode_work_mem_kb,
                    sql_literal(&cfg.slot),
                    sql_literal(&cfg.table_oids[0].to_string()),
                    sql_literal(&cfg.table_oids[1].to_string()),
                    sql_literal(&hex(&cfg.tables[0].id)),
                    sql_literal(&hex(&cfg.tables[1].id)),
                    sql_literal(&cfg.max_spill.to_string()),
                    sql_literal(if env::var("LAKE_CAPTURE_TEST_SPILL_STOP").as_deref()==Ok("1") { "1" } else { "0" }),
                    sql_literal(&read_events.to_string()),
                    sql_literal(&cfg.progress_oid.to_string()),
                    sql_literal(&uuid_text(&cfg.stream.id)),
                    sql_literal(&cfg.stream.generation.to_string()),
                    sql_literal(&cfg.schema_oid.to_string()),
                    sql_literal(&initial_versions[0].to_string()),
                    sql_literal(&initial_versions[1].to_string())
                ));
                match result {
                    Ok(rows) => break rows,
                    Err(error)
                        if read_limit > 1
                            && error
                                .to_string()
                                .contains("capture read window budget exceeded") =>
                    {
                        read_limit = (read_limit / 2).max(1);
                        source.simple_query("SELECT lake_capture.lock_private_logs(true)")?;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            if rows.is_empty() {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            let mut pending = Vec::new();
            let mut sequence = cfg.sequence;
            let mut end = 0;
            for raw in capture_envelopes(&rows)? {
                let raw_hash = hex(&Sha256::digest(&raw));
                let batch =
                    Batch::from_capture(cfg.stream.clone(), sequence + 1, cfg.tables, &raw)?;
                if let Some(state) = &schemas {
                    schemas = Some(state.after_batch(&batch)?);
                } else if batch
                    .events
                    .iter()
                    .any(|event| matches!(event.change, Change::Control(Control::Schema, _)))
                {
                    return Err("schema event requires initialized source schema history".into());
                }
                let post_versions = schema_versions(schemas.as_ref(), &cfg)?;
                let next_end = batch.manifest.transaction.end_lsn;
                if next_end <= end {
                    return Err("capture read has nonincreasing commit boundaries".into());
                }
                end = next_end;
                if batch.events.is_empty() {
                    continue;
                }
                if end <= cfg.end_lsn {
                    let receipt=source.simple_query(&format!("SELECT raw_digest,table_a_version::text,table_b_version::text FROM lake_capture.receipts WHERE stream_id={}::uuid AND end_lsn={}::pg_lsn",sql_literal(&id),sql_literal(&format_lsn(end))))?;
                    if receipt.len() != 1
                        || field(&receipt[0], 0)? != raw_hash
                        || field(&receipt[0], 1)?.parse::<u32>()? != post_versions[0]
                        || field(&receipt[0], 2)?.parse::<u32>()? != post_versions[1]
                    {
                        return Err("conflicting decoded replay at captured source boundary".into());
                    }
                } else {
                    sequence += 1;
                    pending.push((batch, raw_hash, post_versions));
                }
            }
            if !pending.is_empty() {
                // Reserve the whole bounded group durably before object I/O.
                // Every selected transaction retains its own receipt/manifest;
                // one publication transaction makes the entire group durable.
                let ends = pending
                    .iter()
                    .map(|(batch, _, _)| {
                        format!(
                            "{}::pg_lsn",
                            sql_literal(&format_lsn(batch.manifest.transaction.end_lsn))
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let digests = pending
                    .iter()
                    .map(|(batch, _, _)| sql_literal(&hex(&batch.manifest.digest)))
                    .collect::<Vec<_>>()
                    .join(",");
                let sizes = pending
                    .iter()
                    .map(|(batch, _, _)| upload_bytes(batch).map(|bytes| bytes.to_string()))
                    .collect::<Result<Vec<_>>>()?
                    .join(",");
                let reserved=source.simple_query(&format!("SELECT reservation.ordinal::text,reservation.upload_id::text FROM lake_capture.reserve_upload_group({}::uuid,{generation},ARRAY[{ends}],ARRAY[{digests}],ARRAY[{sizes}]::bigint[]) AS reservation ORDER BY reservation.ordinal",sql_literal(&id)))?;
                if reserved.len() != pending.len() {
                    return Err("capture upload group reservation count mismatch".into());
                }
                let mut reservations = Vec::new();
                for (index, row) in reserved.iter().enumerate() {
                    if field(row, 0)?.parse::<usize>()? != index + 1 {
                        return Err("capture upload reservation order mismatch".into());
                    }
                    let reservation = field(row, 1)?;
                    uuid(reservation)?;
                    reservations.push(reservation.to_owned());
                }
                source.simple_query("BEGIN")?;
                let members = pending
                    .iter()
                    .zip(&reservations)
                    .map(|((batch, raw_hash, versions), reservation)| {
                        publication_json(batch, raw_hash, reservation, *versions)
                    })
                    .collect::<Result<Vec<_>>>()?
                    .join(",");
                publish_encoded_group(&mut source, &id, generation, &members)?;
                source.simple_query("COMMIT")?;
                for (batch, _, _) in &pending {
                    event(
                        "capture",
                        "INFO",
                        "batch_captured",
                        "source transaction captured",
                        &[("sequence", batch.manifest.sequence.to_string())],
                    );
                }
            }
            // An empty decoded commit contains no selected-table changes.
            // Advancing over it requires no capture-state write and therefore
            // cannot recursively capture a progress update forever.
            source.simple_query(&format!(
                "SELECT end_lsn::text FROM pg_replication_slot_advance({},{}::pg_lsn)",
                sql_literal(&cfg.slot),
                sql_literal(&format_lsn(end))
            ))?;
        }
    })();
    if let Err(error) = &result {
        let _ = source.simple_query("ROLLBACK");
        let code = failure_sqlstate(error.as_ref());
        let sqlstate = code.as_deref().map(sql_literal).unwrap_or("NULL".into());
        let conflict = if code.as_deref() == Some("23505") {
            1
        } else {
            0
        };
        let _ = source.simple_query(&format!(
            "UPDATE lake_capture.streams SET desired_state='stopped',last_error={},\
             capture_failure_count=least(capture_failure_count::numeric+1,9223372036854775807)::bigint,\
             capture_conflict_count=least(capture_conflict_count::numeric+{conflict},9223372036854775807)::bigint,\
             last_failure_at=clock_timestamp(),last_failure_sqlstate={sqlstate} \
             WHERE stream_id={}::uuid AND config_generation={generation}",
            sql_literal(&error.to_string()), sql_literal(&id)
        ));
    }
    result
}
fn uuid_text(id: &[u8; 16]) -> String {
    let h = hex(id);
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}
fn main() {
    if let Err(error) = run() {
        event(
            "capture",
            "ERROR",
            "process_failed",
            &error.to_string(),
            &[],
        );
        std::process::exit(if error.downcast_ref::<LeaseBusy>().is_some() {
            75
        } else {
            1
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{capture_envelopes, failure_sqlstate};

    #[test]
    fn failure_codes_ignore_diagnostic_text() {
        use lake_replication_worker::wire::Error;
        assert_eq!(
            failure_sqlstate(&Error::Server(
                "S=ERROR C=53100 M=out of space C=23505".into()
            )),
            Some("53100".into())
        );
        assert_eq!(
            failure_sqlstate(&Error::Server("S=ERROR M=C=23505".into())),
            None
        );
        assert_eq!(
            failure_sqlstate(&Error::Server("S=ERROR C=bad M=invalid".into())),
            None
        );
        assert_eq!(failure_sqlstate(&Error::Protocol("C=23505".into())), None);
    }

    fn row(message: usize, part: usize, hex: &str) -> Vec<Option<String>> {
        vec![
            Some(message.to_string()),
            Some(part.to_string()),
            Some(hex.into()),
        ]
    }

    #[test]
    fn message_and_chunk_boundaries_remain_distinct() {
        assert_eq!(
            capture_envelopes(&[row(1, 0, "01"), row(1, 1, "02"), row(2, 0, "0304")]).unwrap(),
            vec![vec![1, 2], vec![3, 4]]
        );
        for rows in [
            vec![row(0, 0, "01")],
            vec![row(2, 0, "01")],
            vec![row(1, 1, "01")],
            vec![row(1, 0, "01"), row(1, 2, "02")],
            vec![row(1, 0, "01"), row(3, 0, "02")],
            vec![row(1, 0, "")],
        ] {
            assert!(capture_envelopes(&rows).is_err());
        }
        let rows = (1..=33)
            .map(|message| row(message, 0, "01"))
            .collect::<Vec<_>>();
        assert!(capture_envelopes(&rows).is_err());
    }
}
