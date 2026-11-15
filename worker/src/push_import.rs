//! Harness-driven importer; production selection/installation remains disabled.
use crate::events::{Column, Relation};
use crate::managed::ManagedClient;
use crate::push_protocol::{
    Batch, Id, MAX_BATCH, MAX_EVENT, MAX_EVENTS, MAX_HEADER_BYTES, Stream, Table, normalize_batch,
};
use crate::sql::sql_literal;
use crate::staging::{changes_json, format_lsn, single_value};
use crate::wire::Client;
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
pub fn unhex(text: &str, limit: usize) -> Result<Vec<u8>> {
    if text.len() % 2 != 0 || text.len() / 2 > limit {
        return Err("invalid bounded hex payload".into());
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err("noncanonical hex"),
            };
            Ok((digit(pair[0])? << 4) | digit(pair[1])?)
        })
        .collect()
}
fn id(text: &str) -> Result<Id> {
    unhex(&text.replace('-', ""), 16)?
        .try_into()
        .map_err(|_| "invalid UUID length".into())
}
fn field(row: &[Option<String>], index: usize) -> Result<&str> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| "missing push registry field".into())
}

pub fn registry(
    destination: &mut ManagedClient,
    subscription: i64,
) -> Result<BTreeMap<Table, (Relation, i64)>> {
    let rows = destination.simple_query(&format!(
        "SELECT p.table_id::text,p.source_schema_version::text,
        r.remote_relation_id::text,r.schema_version::text,r.source_namespace,r.source_relation,
        r.replica_identity::text,c->>'flags',c->>'name',c->>'type_oid',c->>'type_modifier'
        FROM lake_replication.push_tables p JOIN lake_replication.relations r
        ON r.subscription_id=p.subscription_id AND r.remote_relation_id=p.remote_relation_id
        AND r.schema_version=p.relation_schema_version
        CROSS JOIN LATERAL jsonb_array_elements(r.columns) WITH ORDINALITY AS columns(c,n)
        WHERE p.subscription_id={subscription} ORDER BY p.table_id,p.source_schema_version,n"
    ))?;
    let mut registry = BTreeMap::<Table, (Relation, i64)>::new();
    for row in rows {
        let table = Table {
            id: id(field(&row, 0)?)?,
            schema_version: field(&row, 1)?.parse()?,
        };
        let relation = registry.entry(table).or_insert((
            Relation {
                id: field(&row, 2)?.parse()?,
                namespace: field(&row, 4)?.into(),
                name: field(&row, 5)?.into(),
                replica_identity: field(&row, 6)?
                    .as_bytes()
                    .first()
                    .copied()
                    .ok_or("missing replica identity")?,
                columns: Vec::new(),
            },
            field(&row, 3)?.parse()?,
        ));
        relation.0.columns.push(Column {
            flags: field(&row, 7)?.parse()?,
            name: field(&row, 8)?.into(),
            type_oid: field(&row, 9)?.parse()?,
            type_modifier: field(&row, 10)?.parse()?,
        });
    }
    if registry.len() != 2 {
        return Err("push prototype requires exactly two registered tables".into());
    }
    Ok(registry)
}

pub fn next_sequence(destination: &mut ManagedClient, subscription: i64) -> Result<u64> {
    let rows=destination.simple_query(&format!("SELECT (imported_sequence+1)::text FROM lake_replication.push_bindings WHERE subscription_id={subscription}"))?;
    Ok(single_value(&rows, "imported checkpoint")?.parse()?)
}

pub fn stream_binding(destination: &mut ManagedClient, subscription: i64) -> Result<Stream> {
    let rows=destination.simple_query(&format!("SELECT stream_id::text,system_identifier::text,database_oid::text,
        capture_generation::text FROM lake_replication.push_bindings WHERE subscription_id={subscription}"))?;
    if rows.len() != 1 {
        return Err("push stream binding is missing".into());
    }
    Ok(Stream {
        id: id(field(&rows[0], 0)?)?,
        system_identifier: field(&rows[0], 1)?.parse()?,
        database_oid: field(&rows[0], 2)?.parse()?,
        generation: field(&rows[0], 3)?.parse()?,
    })
}

pub struct ContinuityPoint {
    pub sequence: u64,
    pub batch: Id,
    pub end_lsn: u64,
    pub digest: Vec<u8>,
}
pub fn continuity_point(
    destination: &mut ManagedClient,
    subscription: i64,
) -> Result<Option<ContinuityPoint>> {
    let rows=destination.simple_query(&format!("SELECT b.imported_sequence::text,r.batch_id::text,r.end_lsn::text,r.digest
        FROM lake_replication.push_bindings b LEFT JOIN lake_replication.push_receipts r
        ON r.subscription_id=b.subscription_id AND r.sequence=b.imported_sequence WHERE b.subscription_id={subscription}"))?;
    let row = rows
        .first()
        .ok_or("push stream continuity binding is missing")?;
    let sequence = field(row, 0)?.parse()?;
    if sequence == 0 {
        return Ok(None);
    }
    Ok(Some(ContinuityPoint {
        sequence,
        batch: id(field(row, 1)?)?,
        end_lsn: crate::staging::parse_lsn(field(row, 2)?)?,
        digest: unhex(field(row, 3)?, 32)?,
    }))
}

/// Read a complete batch from one PostgreSQL/pg_lake snapshot. SQL preflight caps
/// the bytes returned even if the source fixture contains malformed log rows.
pub struct SourceRead {
    pub batch: Option<Batch>,
    pub captured_sequence: u64,
}

pub fn read_batch(
    source: &mut Client,
    sequence: u64,
    expected: &BTreeMap<Table, (Relation, i64)>,
    binding: &Stream,
    continuity: Option<&ContinuityPoint>,
) -> Result<SourceRead> {
    // Acquire before BEGIN: taking a repeatable-read snapshot before waiting
    // for cleanup could pin already-expired Iceberg file references.
    source.simple_query("SELECT pg_advisory_lock_shared(1280593921,1)")?;
    if let Err(error) = source.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY") {
        let _ = source.simple_query("SELECT pg_advisory_unlock_shared(1280593921,1)");
        return Err(error.into());
    }
    let result = (|| {
        let progress=source.simple_query("SELECT stream_id::text,system_identifier::text,database_oid::text,
            capture_generation::text,captured_sequence::text,pruned_through_sequence::text FROM lake_capture_log.progress LIMIT 2")?;
        if progress.len() != 1 {
            return Err("source capture stream is not registered".into());
        }
        let row = &progress[0];
        let actual = Stream {
            id: id(field(row, 0)?)?,
            system_identifier: field(row, 1)?.parse()?,
            database_oid: field(row, 2)?.parse()?,
            generation: field(row, 3)?.parse()?,
        };
        if &actual != binding {
            return Err("source stream identity or generation mismatch".into());
        }
        let captured: u64 = field(row, 4)?.parse()?;
        let pruned: u64 = field(row, 5)?.parse()?;
        if sequence <= pruned {
            return Err("source log retention gap; controlled snapshot rebuild required".into());
        }
        if let Some(point) = continuity {
            if captured < point.sequence {
                return Err(
                    "source capture frontier moved backwards; controlled rebuild required".into(),
                );
            }
            let receipt = source.simple_query(&format!(
                "SELECT batch_id::text,end_lsn::text,digest
                FROM lake_capture_log.receipt_state WHERE sequence={} LIMIT 2",
                point.sequence
            ))?;
            if receipt.len() != 1
                || id(field(&receipt[0], 0)?)? != point.batch
                || crate::staging::parse_lsn(field(&receipt[0], 1)?)? != point.end_lsn
                || unhex(field(&receipt[0], 2)?, 32)? != point.digest
            {
                return Err(
                    "conflicting push replay: source continuity differs from imported history"
                        .into(),
                );
            }
        }
        let rows=source.simple_query(&format!("SELECT sequence::text,header_hex,digest_hex FROM lake_capture_log.manifests WHERE sequence>={sequence} ORDER BY lake_capture_log.manifests.sequence LIMIT 2"))?;
        if rows.is_empty() {
            if sequence <= captured {
                return Err("source manifest missing below captured frontier".into());
            }
            return Ok(SourceRead {
                batch: None,
                captured_sequence: captured,
            });
        }
        if field(&rows[0], 0)?.parse::<u64>()? != sequence {
            return Err("source manifest sequence gap".into());
        }
        if rows.len() > 1 && field(&rows[1], 0)?.parse::<u64>()? == sequence {
            return Err("duplicate source manifest sequence".into());
        }
        check_registrations(source, expected)?;
        let mut bytes = unhex(field(&rows[0], 1)?, MAX_HEADER_BYTES)?;
        let digest = unhex(field(&rows[0], 2)?, 32)?;
        if digest.len() != 32 {
            return Err("invalid manifest digest length".into());
        }
        let bounds=source.simple_query(&format!("WITH events AS (
            SELECT ordinal,event_hex FROM lake_capture_log.events_a WHERE sequence={sequence}
            UNION ALL SELECT ordinal,event_hex FROM lake_capture_log.events_b WHERE sequence={sequence}
            UNION ALL SELECT ordinal,event_hex FROM lake_capture_log.controls WHERE sequence={sequence})
            SELECT count(*)::text,coalesce(sum(octet_length(event_hex)),0)::text,
            coalesce(max(octet_length(event_hex)),0)::text,count(*) FILTER (WHERE event_hex IS NULL)::text FROM events"))?;
        if bounds.len() != 1
            || field(&bounds[0], 0)?.parse::<u64>()? > MAX_EVENTS as u64
            || field(&bounds[0], 1)?.parse::<u64>()? > MAX_BATCH as u64 * 2
            || field(&bounds[0], 2)?.parse::<u64>()? > MAX_EVENT as u64 * 2
            || field(&bounds[0], 3)? != "0"
        {
            return Err("source log bounds exceeded or NULL payload".into());
        }
        let events=source.simple_query(&format!("SELECT ordinal::text,event_hex FROM (
            SELECT ordinal,event_hex FROM lake_capture_log.events_a WHERE sequence={sequence}
            UNION ALL SELECT ordinal,event_hex FROM lake_capture_log.events_b WHERE sequence={sequence}
            UNION ALL SELECT ordinal,event_hex FROM lake_capture_log.controls WHERE sequence={sequence}) events
            ORDER BY events.ordinal LIMIT {}",MAX_EVENTS+1))?;
        if events.len() > MAX_EVENTS {
            return Err("source event count limit exceeded".into());
        }
        for (index, row) in events.iter().enumerate() {
            if field(row, 0)?.parse::<usize>()? != index {
                return Err("source event ordinal gap or duplicate".into());
            }
            let event = unhex(field(row, 1)?, MAX_EVENT)?;
            if bytes.len() + 4 + event.len() + 32 > MAX_BATCH {
                return Err("source batch size limit exceeded".into());
            }
            bytes.extend((event.len() as u32).to_be_bytes());
            bytes.extend(event);
        }
        bytes.extend(digest);
        let batch = Batch::decode(&bytes)?;
        if batch.manifest.stream != actual {
            return Err("source header identity or generation mismatch".into());
        }
        if batch.manifest.sequence != sequence {
            return Err("source manifest sequence differs from header".into());
        }
        Ok(SourceRead {
            batch: Some(batch),
            captured_sequence: captured,
        })
    })();
    match result {
        Ok(batch) => {
            source.simple_query("COMMIT")?;
            source.simple_query("SELECT pg_advisory_unlock_shared(1280593921,1)")?;
            Ok(batch)
        }
        Err(error) => {
            let _ = source.simple_query("ROLLBACK");
            let _ = source.simple_query("SELECT pg_advisory_unlock_shared(1280593921,1)");
            Err(error)
        }
    }
}

pub fn observe_source(
    destination: &mut ManagedClient,
    subscription: i64,
    binding: &Stream,
    captured_sequence: u64,
) -> Result<()> {
    destination.simple_query(&format!(
        "SELECT lake_replication.observe_push_source({subscription},{}::uuid,{},{}::oid,{},{captured_sequence})",
        sql_literal(&hex(&binding.id)), binding.system_identifier, binding.database_oid, binding.generation
    ))?;
    Ok(())
}

fn check_registrations(
    source: &mut Client,
    expected: &BTreeMap<Table, (Relation, i64)>,
) -> Result<()> {
    let bounds = source.simple_query(
        "SELECT count(*)::text,coalesce(bool_and(
        octet_length(source_namespace)<=63 AND octet_length(source_relation)<=63
        AND octet_length(columns::text)<=1048576),false)::text FROM lake_capture_log.registrations",
    )?;
    if field(&bounds[0], 0)? != "2" || field(&bounds[0], 1)? != "true" {
        return Err("invalid bounded source table registration group".into());
    }
    let rows = source.simple_query(
        "SELECT table_id::text,schema_version::text,source_namespace,source_relation,
        replica_identity::text,c->>'flags',c->>'name',c->>'type_oid',c->>'type_modifier'
        FROM lake_capture_log.registrations CROSS JOIN LATERAL
        jsonb_array_elements(columns) WITH ORDINALITY AS fields(c,n)
        ORDER BY table_id,schema_version,n LIMIT 3201",
    )?;
    if rows.len() > 3200 {
        return Err("source schema column budget exceeded".into());
    }
    let mut actual = BTreeMap::<Table, Relation>::new();
    for row in rows {
        let table = Table {
            id: id(field(&row, 0)?)?,
            schema_version: field(&row, 1)?.parse()?,
        };
        let relation = actual.entry(table).or_insert(Relation {
            id: 0,
            namespace: field(&row, 2)?.into(),
            name: field(&row, 3)?.into(),
            replica_identity: field(&row, 4)?
                .as_bytes()
                .first()
                .copied()
                .ok_or("missing source identity descriptor")?,
            columns: Vec::new(),
        });
        relation.columns.push(Column {
            flags: field(&row, 5)?.parse()?,
            name: field(&row, 6)?.into(),
            type_oid: field(&row, 7)?.parse()?,
            type_modifier: field(&row, 8)?.parse()?,
        });
    }
    if actual.len() != expected.len() {
        return Err("source table registration differs from destination binding".into());
    }
    for (table, source) in actual {
        let Some((destination, _)) = expected.get(&table) else {
            return Err("source table registration differs from destination binding".into());
        };
        if source.namespace != destination.namespace
            || source.name != destination.name
            || source.replica_identity != destination.replica_identity
            || source.columns != destination.columns
        {
            return Err("source table registration differs from destination binding".into());
        }
    }
    Ok(())
}

pub fn admit(
    destination: &mut ManagedClient,
    subscription: i64,
    batch: &Batch,
    registry: &BTreeMap<Table, (Relation, i64)>,
) -> Result<String> {
    let (transaction, versions, snapshot) = normalize_batch(batch, registry)?;
    let changes = changes_json(&transaction, &versions)?;
    let m = &batch.manifest;
    let stream = &m.stream;
    let tx = &m.transaction;
    let statement=format!("SELECT lake_replication.import_push_batch({subscription},{}::uuid,{},{}::oid,{},{},{}::uuid,{}, {},{}::pg_lsn,{}::pg_lsn,{}::pg_lsn,
        TIMESTAMPTZ '2000-01-01 00:00:00+00' + {} * INTERVAL '1 microsecond',{}::jsonb,{snapshot})",
        sql_literal(&hex(&stream.id)),stream.system_identifier,stream.database_oid,stream.generation,m.sequence,
        sql_literal(&hex(&m.batch_id)),sql_literal(&hex(&m.digest)),tx.xid,
        sql_literal(&format_lsn(tx.begin_lsn)),sql_literal(&format_lsn(tx.commit_lsn)),sql_literal(&format_lsn(tx.end_lsn)),
        tx.commit_time_micros,sql_literal(&changes));
    let rows = destination.simple_query(&statement)?;
    let outcome = single_value(&rows, "push journal admission")?;
    if !matches!(outcome, "imported" | "replayed" | "full") {
        return Err("invalid push admission result".into());
    }
    Ok(outcome.into())
}

pub fn schema_enabled(destination: &mut ManagedClient, subscription: i64) -> Result<bool> {
    let rows=destination.simple_query(&format!("SELECT schema_enabled::text FROM lake_replication.push_bindings WHERE subscription_id={subscription}"))?;
    Ok(single_value(&rows, "push schema mode")? == "true")
}

fn uuid_text(value: &Id) -> String {
    let value = hex(value);
    format!(
        "{}-{}-{}-{}-{}",
        &value[..8],
        &value[8..12],
        &value[12..16],
        &value[16..20],
        &value[20..]
    )
}

fn ordered_json(batch: &Batch, actions: &[crate::push_protocol::OrderedAction]) -> Result<String> {
    use crate::push_protocol::OrderedAction;
    use crate::staging::{change_payload_fields, json_string};
    let mut result = String::from("[");
    for (index, action) in actions.iter().enumerate() {
        if index > 0 {
            result.push(',');
        }
        let value = match action {
            OrderedAction::Row {
                ordinal, change, ..
            } => {
                let table = batch
                    .events
                    .get(*ordinal as usize)
                    .and_then(|event| event.table)
                    .ok_or("ordered row has no source table identity")?;
                format!(
                    "{{\"source_ordinal\":{ordinal},\"table_id\":{},\"source_schema_version\":{},{}}}",
                    json_string(&uuid_text(&table.id)),
                    table.schema_version,
                    change_payload_fields(change)?
                )
            }
            OrderedAction::Schema { ordinal, change } => format!(
                "{{\"operation\":\"schema\",\"source_ordinal\":{ordinal},\"table_id\":{},\"before_version\":{},\"after_version\":{},\"before_definition\":{},\"after_definition\":{},\"payload_hex\":{}}}",
                json_string(&uuid_text(&change.table_id)),
                change.before_version,
                change.after_version,
                change.before.json()?,
                change.after.json()?,
                json_string(&hex(&change.encode()?))
            ),
            OrderedAction::Progress { ordinal, request } => format!(
                "{{\"operation\":\"progress\",\"source_ordinal\":{ordinal},\"request_id\":{}}}",
                json_string(&uuid_text(request))
            ),
        };
        result.push_str(&value);
        if result.len() + 1 > MAX_BATCH {
            return Err("ordered journal JSON exceeds 64 MiB normalization budget".into());
        }
    }
    result.push(']');
    Ok(result)
}

fn ordered_registry(
    destination: &mut ManagedClient,
    subscription: i64,
    batch: &Batch,
    initial: &BTreeMap<Table, (Relation, i64)>,
) -> Result<(BTreeMap<Table, (Relation, i64)>, crate::push_schema::State)> {
    use crate::push_schema::{Definition, State};
    let sequence = batch.manifest.sequence;
    let rows=destination.simple_query(&format!(
        "SELECT v.table_id::text,v.source_schema_version::text,v.remote_relation_id::text,v.relation_schema_version::text,
            encode(v.definition_bytes,'hex'),CASE WHEN {sequence}=1 THEN 1 ELSE previous.source_schema_version END::text
         FROM lake_replication.push_schema_versions v JOIN lake_replication.push_schema_tables h
            ON h.subscription_id=v.subscription_id AND h.table_id=v.table_id
         LEFT JOIN lake_replication.push_schema_receipt_versions previous ON previous.subscription_id=h.subscription_id
            AND previous.table_id=h.table_id AND previous.sequence={sequence}-1
         WHERE v.subscription_id={subscription} ORDER BY v.table_id,v.source_schema_version"))?;
    let mut registry = BTreeMap::new();
    let mut definitions = BTreeMap::new();
    let mut before = Vec::new();
    for row in rows {
        let table = Table {
            id: id(field(&row, 0)?)?,
            schema_version: field(&row, 1)?.parse()?,
        };
        let definition = Definition::decode(&unhex(field(&row, 4)?, 1024 * 1024)?)?;
        let base = initial
            .get(&Table {
                schema_version: 1,
                ..table
            })
            .ok_or("unregistered retained schema table")?;
        let remote: u32 = field(&row, 2)?.parse()?;
        let local: i64 = field(&row, 3)?.parse()?;
        if remote != base.0.id || local <= 0 {
            return Err("retained schema has conflicting remote relation identity".into());
        }
        let relation = definition.relation(remote)?;
        if table.schema_version == 1 && (relation != base.0 || local != base.1) {
            return Err("initial retained definition differs from immutable registration".into());
        }
        if table.schema_version == field(&row, 5)?.parse()? {
            before.push((table.id, table.schema_version, definition.clone()));
        }
        definitions.insert(table, definition);
        registry.insert(table, (relation, local));
    }
    let state = State::new(before)?;
    for event in &batch.events {
        if let crate::push_protocol::Change::Control(
            crate::push_protocol::Control::Schema,
            payload,
        ) = &event.change
        {
            let change = crate::push_schema::Change::decode(payload)?;
            let table = Table {
                id: change.table_id,
                schema_version: change.after_version,
            };
            let base = initial
                .get(&Table {
                    schema_version: 1,
                    ..table
                })
                .ok_or("unregistered source schema event")?;
            if let Some(registered) = definitions.get(&table) {
                if registered != &change.after {
                    return Err("source schema differs from retained destination version".into());
                }
            } else {
                // Local versions are assigned by the atomic SQL admission call.
                // This temporary normalization entry supplies only source row shape.
                registry.insert(
                    table,
                    (
                        change.after.relation(base.0.id)?,
                        i64::from(table.schema_version),
                    ),
                );
                definitions.insert(table, change.after);
            }
        }
    }
    Ok((registry, state))
}

pub fn admit_ordered(
    destination: &mut ManagedClient,
    subscription: i64,
    batch: &Batch,
    initial: &BTreeMap<Table, (Relation, i64)>,
) -> Result<String> {
    use crate::push_protocol::{Change, Control, normalize_ordered};
    use crate::staging::{change_payload_fields, json_string};
    let (registry, state) = ordered_registry(destination, subscription, batch, initial)?;
    let snapshot = batch.events.iter().any(|event| {
        matches!(
            event.change,
            Change::Control(
                Control::SnapshotBegin
                    | Control::SnapshotChunk
                    | Control::SnapshotComplete
                    | Control::SnapshotActivate,
                _
            )
        )
    });
    let actions = if snapshot {
        let (transaction, _, is_snapshot) = normalize_batch(batch, initial)?;
        if !is_snapshot {
            return Err("invalid schema-mode snapshot normalization".into());
        }
        state.after_batch(batch)?;
        let mut result = String::from("[");
        for (ordinal, change) in transaction.changes.iter().enumerate() {
            let table = initial
                .iter()
                .find(|(_, entry)| entry.0.id == change.relation_id)
                .map(|(table, _)| table)
                .ok_or("snapshot row has no registered source identity")?;
            if ordinal > 0 {
                result.push(',');
            }
            result.push_str(&format!(
                "{{\"source_ordinal\":{ordinal},\"table_id\":{},\"source_schema_version\":1,{}}}",
                json_string(&uuid_text(&table.id)),
                change_payload_fields(change)?
            ));
            if result.len() + 1 > MAX_BATCH {
                return Err("schema snapshot JSON exceeds 64 MiB normalization budget".into());
            }
        }
        result.push(']');
        result
    } else {
        let (actions, _) = normalize_ordered(batch, &registry, &state)?;
        ordered_json(batch, &actions)?
    };
    let m = &batch.manifest;
    let stream = &m.stream;
    let tx = &m.transaction;
    let sql=format!("SELECT lake_replication.import_push_schema_batch({subscription},{}::uuid,{},{}::oid,{},{},{}::uuid,{}, {},{}::pg_lsn,{}::pg_lsn,{}::pg_lsn,
        TIMESTAMPTZ '2000-01-01 00:00:00+00' + {} * INTERVAL '1 microsecond',{}::jsonb,{snapshot})",
        sql_literal(&hex(&stream.id)),stream.system_identifier,stream.database_oid,stream.generation,m.sequence,
        sql_literal(&hex(&m.batch_id)),sql_literal(&hex(&m.digest)),tx.xid,
        sql_literal(&format_lsn(tx.begin_lsn)),sql_literal(&format_lsn(tx.commit_lsn)),sql_literal(&format_lsn(tx.end_lsn)),
        tx.commit_time_micros,sql_literal(&actions));
    let rows = destination.simple_query(&sql)?;
    let outcome = single_value(&rows, "ordered push admission")?;
    if !matches!(outcome, "imported" | "replayed" | "full") {
        return Err("invalid ordered push admission result".into());
    }
    Ok(outcome.into())
}
