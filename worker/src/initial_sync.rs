use lake_replication_worker::wire::{Client, Error as WireError};
use std::env;
use std::thread;
use std::time::Duration;

fn main() {
    if let Err(error) = run() {
        eprintln!("lake-replication-initial-sync: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let mut slot_owner = Client::connect(
        config.source_address.as_str(),
        &config.user,
        &config.source_database,
        true,
    )?;
    let slot_rows = slot_owner.simple_query(&format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER {})",
        quote_identifier(&config.slot),
        config.slot_failover
    ))?;
    let slot_row = slot_rows.first().ok_or("slot creation returned no row")?;
    let consistent_lsn = field(slot_row, 1, "consistent_point")?.to_owned();
    let snapshot_name = field(slot_row, 2, "snapshot_name")?.to_owned();

    let mut source = Client::connect(
        config.source_address.as_str(),
        &config.user,
        &config.source_database,
        false,
    )?;
    source.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")?;
    source.simple_query(&format!(
        "SET TRANSACTION SNAPSHOT {}",
        sql_literal(&snapshot_name)
    ))?;
    let mut plans = discover_tables(&mut source, &config)?;
    if plans.is_empty() {
        return Err("publication contains no eligible tables".into());
    }

    let mut destination = Client::connect(
        config.destination_address.as_str(),
        &config.user,
        &config.destination_database,
        false,
    )?;
    for plan in &mut plans {
        let registered = destination.simple_query(&format!(
            "SELECT lake_replication.register_relation({}, {}, {}, {}, {}, {}::jsonb)::text",
            config.subscription_id,
            plan.relation_id,
            sql_literal(&plan.source.schema),
            sql_literal(&plan.source.relation),
            sql_literal(&plan.replica_identity),
            sql_literal(&plan.columns),
        ))?;
        plan.schema_version = single_value(&registered, "register_relation")?.parse()?;
        destination.simple_query(&format!(
            "SELECT lake_replication.register_type_contract({}, {}, {}, {}, {}::jsonb)",
            config.subscription_id,
            plan.relation_id,
            plan.schema_version,
            config.type_mapping_version,
            sql_literal(&plan.type_contract),
        ))?;
        destination.simple_query(&format!(
            "SELECT lake_replication.ensure_iceberg_destination({}, {}, {}, {}, {}, {})::text",
            config.subscription_id,
            plan.relation_id,
            plan.schema_version,
            sql_literal(&plan.destination.schema),
            sql_literal(&plan.destination.relation),
            config.create_missing_destinations,
        ))?;
        destination.simple_query(&format!(
            "SELECT lake_replication.begin_initial_copy({}, {}, {}, {}::regclass, {}::pg_lsn, {}, {})",
            config.subscription_id,
            plan.relation_id,
            plan.schema_version,
            sql_literal(&plan.destination.sql()),
            sql_literal(&consistent_lsn),
            sql_literal(&snapshot_name),
            config.overwrite_destination,
        ))?;
    }

    println!(
        "snapshot={} consistent_lsn={} tables={}",
        snapshot_name,
        consistent_lsn,
        plans.len()
    );
    if config.pause_after_snapshot_ms > 0 {
        thread::sleep(Duration::from_millis(config.pause_after_snapshot_ms));
    }

    let mut total_rows = 0_u64;
    for plan in &plans {
        match copy_table(&mut source, &mut destination, &config, plan) {
            Ok(copied) => total_rows += copied,
            Err(error) => {
                for failed_plan in &plans {
                    mark_copy_failed(
                        &mut destination,
                        &config,
                        failed_plan.relation_id,
                        failed_plan.schema_version,
                        &error.to_string(),
                    )?;
                }
                return Err(error.into());
            }
        }
    }

    let completed = destination.simple_query(&format!(
        "SELECT lake_replication.finish_initial_sync_group({}, {})::text",
        config.subscription_id,
        plans.len()
    ))?;
    let completed = single_value(&completed, "finish_initial_sync_group")?;
    source.simple_query("COMMIT")?;
    println!(
        "initial copy ready tables={} rows={}/{} consistent_lsn={}",
        plans.len(),
        completed,
        total_rows,
        consistent_lsn
    );
    Ok(())
}

fn copy_table(
    source: &mut Client,
    destination: &mut Client,
    config: &Config,
    plan: &TablePlan,
) -> Result<u64, WireError> {
    let mut decoder = HexCopyDecoder::default();
    let mut pending_rows = Vec::with_capacity(config.chunk_rows);
    let mut pending_bytes = 0_usize;
    let mut copied = 0_u64;
    let filter = if plan.row_filter.is_empty() {
        String::new()
    } else {
        format!(" WHERE ({})", plan.row_filter)
    };
    source.copy_out(
        &format!(
            "COPY (
                SELECT encode(convert_to((
                    SELECT jsonb_object_agg(entry.key, entry.value)
                      FROM jsonb_each(to_jsonb(source_row)) AS entry
                     WHERE entry.key = ANY({})
                )::text, 'UTF8'), 'hex')
                  FROM {} AS source_row{}
             ) TO STDOUT",
            plan.selected_names_sql,
            plan.source.sql(),
            filter,
        ),
        |data| {
            decoder.push(data, |row| {
                if !pending_rows.is_empty()
                    && pending_bytes.saturating_add(row.len()) > config.chunk_bytes
                {
                    stage_rows(
                        destination,
                        config,
                        plan.relation_id,
                        plan.schema_version,
                        &mut copied,
                        &mut pending_rows,
                        &mut pending_bytes,
                    )?;
                }
                pending_bytes = pending_bytes.saturating_add(row.len());
                pending_rows.push(row);
                if pending_rows.len() == config.chunk_rows || pending_bytes >= config.chunk_bytes {
                    stage_rows(
                        destination,
                        config,
                        plan.relation_id,
                        plan.schema_version,
                        &mut copied,
                        &mut pending_rows,
                        &mut pending_bytes,
                    )?;
                }
                Ok(())
            })
        },
    )?;
    decoder.finish()?;
    if !pending_rows.is_empty() {
        stage_rows(
            destination,
            config,
            plan.relation_id,
            plan.schema_version,
            &mut copied,
            &mut pending_rows,
            &mut pending_bytes,
        )?;
    }
    println!("copied table={} rows={copied}", plan.source.sql());
    Ok(copied)
}

fn discover_tables(
    source: &mut Client,
    config: &Config,
) -> Result<Vec<TablePlan>, Box<dyn std::error::Error>> {
    let (predicate, publication_join) = if config.discover_publication {
        (
            format!("pt.pubname = {}", sql_literal(&config.publication)),
            "JOIN pg_publication_tables pt
               ON pt.schemaname = n.nspname AND pt.tablename = c.relname
             JOIN pg_attribute a ON a.attrelid = c.oid
                                AND a.attnum > 0 AND NOT a.attisdropped
                                AND a.attname = ANY(pt.attnames)"
                .to_owned(),
        )
    } else {
        let table = QualifiedName::parse(
            config
                .source_table
                .as_deref()
                .ok_or("LAKE_SOURCE_TABLE is required outside discovery mode")?,
        )?;
        (
            format!(
                "n.nspname = {} AND c.relname = {}",
                sql_literal(&table.schema),
                sql_literal(&table.relation)
            ),
            "JOIN pg_attribute a ON a.attrelid = c.oid
                                AND a.attnum > 0 AND NOT a.attisdropped
             CROSS JOIN LATERAL (
                 SELECT n.nspname AS schemaname, c.relname AS tablename,
                        ARRAY(SELECT attname FROM pg_attribute
                               WHERE attrelid=c.oid AND attnum > 0 AND NOT attisdropped
                               ORDER BY attnum)::name[] AS attnames,
                        NULL::text AS rowfilter
             ) pt"
                .to_owned(),
        )
    };
    let rows = source.simple_query(&format!(
        "SELECT c.oid::text, c.relreplident::text,
                jsonb_agg(jsonb_build_object(
                    'flags', CASE WHEN EXISTS (
                        SELECT 1 FROM pg_index i
                         WHERE i.indrelid = c.oid
                           AND (i.indisprimary OR i.indisreplident)
                           AND a.attnum = ANY(i.indkey)
                    ) THEN 1 ELSE 0 END,
                    'name', a.attname,
                    'type_oid', a.atttypid::integer,
                    'type_modifier', a.atttypmod
                ) ORDER BY a.attnum)::text,
                n.nspname::text, c.relname::text,
                format('ARRAY[%s]::text[]',
                    string_agg(quote_literal(a.attname), ', ' ORDER BY a.attnum)),
                coalesce(pt.rowfilter, ''),
                jsonb_agg(jsonb_build_object(
                    'name', a.attname,
                    'source_type', format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'generated', a.attgenerated <> ''
                ) ORDER BY a.attnum)::text
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           {publication_join}
          WHERE c.relkind IN ('r', 'p') AND {predicate}
          GROUP BY c.oid, c.relreplident, n.nspname, c.relname, pt.rowfilter
          ORDER BY n.nspname, c.relname"
    ))?;

    let mut plans = Vec::with_capacity(rows.len());
    for row in rows {
        let source_name = QualifiedName {
            schema: field(&row, 3, "source schema")?.to_owned(),
            relation: field(&row, 4, "source relation")?.to_owned(),
        };
        let destination = if config.discover_publication {
            QualifiedName {
                schema: config
                    .destination_schema
                    .as_ref()
                    .ok_or("LAKE_DESTINATION_SCHEMA is required in discovery mode")?
                    .to_owned(),
                relation: format!(
                    "{}{}",
                    source_name.relation, config.destination_table_suffix
                ),
            }
        } else {
            QualifiedName::parse(
                config
                    .destination_table
                    .as_deref()
                    .ok_or("LAKE_DESTINATION_TABLE is required outside discovery mode")?,
            )?
        };
        plans.push(TablePlan {
            relation_id: field(&row, 0, "relation OID")?.parse()?,
            replica_identity: field(&row, 1, "replica identity")?.to_owned(),
            columns: field(&row, 2, "source columns")?.to_owned(),
            source: source_name,
            destination,
            selected_names_sql: field(&row, 5, "publication columns")?.to_owned(),
            row_filter: field(&row, 6, "publication row filter")?.to_owned(),
            type_contract: field(&row, 7, "source type contract")?.to_owned(),
            schema_version: 0,
        });
    }
    Ok(plans)
}

struct TablePlan {
    relation_id: u32,
    replica_identity: String,
    columns: String,
    source: QualifiedName,
    destination: QualifiedName,
    selected_names_sql: String,
    row_filter: String,
    type_contract: String,
    schema_version: i64,
}

fn stage_rows(
    destination: &mut Client,
    config: &Config,
    relation_id: u32,
    schema_version: i64,
    copied: &mut u64,
    rows: &mut Vec<String>,
    pending_bytes: &mut usize,
) -> Result<(), WireError> {
    let json_rows = json_array(rows);
    destination.simple_query(&format!(
        "SELECT lake_replication.append_initial_copy_rows({}, {}, {}, {}, {}::jsonb)::text",
        config.subscription_id,
        relation_id,
        schema_version,
        copied,
        sql_literal(&json_rows),
    ))?;
    *copied += rows.len() as u64;
    rows.clear();
    *pending_bytes = 0;
    println!("staged initial rows={copied}");
    if config.pause_after_chunk_ms > 0 {
        thread::sleep(Duration::from_millis(config.pause_after_chunk_ms));
    }
    if config.fail_after_rows > 0 && *copied >= config.fail_after_rows {
        return Err(WireError::Protocol(format!(
            "injected failure after {copied} rows"
        )));
    }
    Ok(())
}

fn mark_copy_failed(
    destination: &mut Client,
    config: &Config,
    relation_id: u32,
    schema_version: i64,
    error: &str,
) -> Result<(), WireError> {
    destination.simple_query(&format!(
        "SELECT lake_replication.fail_initial_copy({}, {}, {}, {})",
        config.subscription_id,
        relation_id,
        schema_version,
        sql_literal(error),
    ))?;
    Ok(())
}

struct Config {
    source_address: String,
    destination_address: String,
    user: String,
    source_database: String,
    destination_database: String,
    slot: String,
    publication: String,
    subscription_id: i64,
    source_table: Option<String>,
    destination_table: Option<String>,
    destination_schema: Option<String>,
    discover_publication: bool,
    create_missing_destinations: bool,
    type_mapping_version: i32,
    destination_table_suffix: String,
    overwrite_destination: bool,
    chunk_rows: usize,
    chunk_bytes: usize,
    fail_after_rows: u64,
    pause_after_snapshot_ms: u64,
    pause_after_chunk_ms: u64,
    slot_failover: bool,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let config = Self {
            source_address: value("LAKE_SOURCE_ADDRESS", "127.0.0.1:55434"),
            destination_address: value("LAKE_DESTINATION_ADDRESS", "127.0.0.1:55434"),
            user: value("LAKE_USER", "postgres"),
            source_database: value("LAKE_SOURCE_DATABASE", "application"),
            destination_database: value("LAKE_DESTINATION_DATABASE", "analytics"),
            slot: required("LAKE_SLOT")?,
            publication: required("LAKE_PUBLICATION")?,
            subscription_id: required("LAKE_SUBSCRIPTION_ID")?.parse()?,
            source_table: env::var("LAKE_SOURCE_TABLE").ok(),
            destination_table: env::var("LAKE_DESTINATION_TABLE").ok(),
            destination_schema: env::var("LAKE_DESTINATION_SCHEMA").ok(),
            discover_publication: value("LAKE_DISCOVER_PUBLICATION", "0") == "1",
            create_missing_destinations: value("LAKE_CREATE_TABLES_USING_ICEBERG", "0") == "1",
            type_mapping_version: value("LAKE_TYPE_MAPPING_VERSION", "1").parse()?,
            destination_table_suffix: value("LAKE_DESTINATION_TABLE_SUFFIX", ""),
            overwrite_destination: value("LAKE_OVERWRITE_DESTINATION", "0") == "1",
            chunk_rows: value("LAKE_INITIAL_COPY_CHUNK_ROWS", "1000").parse()?,
            chunk_bytes: value("LAKE_INITIAL_COPY_CHUNK_BYTES", "8388608").parse()?,
            fail_after_rows: value("LAKE_INITIAL_COPY_FAIL_AFTER_ROWS", "0").parse()?,
            pause_after_snapshot_ms: value("LAKE_INITIAL_COPY_PAUSE_MS", "0").parse()?,
            pause_after_chunk_ms: value("LAKE_INITIAL_COPY_CHUNK_PAUSE_MS", "0").parse()?,
            slot_failover: value("LAKE_SLOT_FAILOVER", "1") == "1",
        };
        if config.chunk_rows == 0 || config.chunk_bytes == 0 || config.type_mapping_version <= 0 {
            return Err("initial-copy row and byte chunk limits must be positive".into());
        }
        Ok(config)
    }
}

struct QualifiedName {
    schema: String,
    relation: String,
}

impl QualifiedName {
    fn parse(value: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let (schema, relation) = value
            .split_once('.')
            .ok_or("table name must be schema-qualified")?;
        if schema.is_empty() || relation.is_empty() || relation.contains('.') {
            return Err("table name must contain exactly one non-empty qualifier".into());
        }
        Ok(Self {
            schema: schema.to_owned(),
            relation: relation.to_owned(),
        })
    }

    fn sql(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.relation)
        )
    }
}

fn json_array(rows: &[String]) -> String {
    let mut result = String::from("[");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            result.push(',');
        }
        result.push_str(row);
    }
    result.push(']');
    result
}

#[derive(Default)]
struct HexCopyDecoder {
    pending: Vec<u8>,
}

impl HexCopyDecoder {
    fn push<F>(&mut self, bytes: &[u8], mut consume: F) -> Result<(), WireError>
    where
        F: FnMut(String) -> Result<(), WireError>,
    {
        self.pending.extend_from_slice(bytes);
        while let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=newline).collect();
            let text = decode_hex(&line[..line.len() - 1])?;
            consume(text)?;
        }
        Ok(())
    }

    fn finish(&self) -> Result<(), WireError> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(WireError::Protocol(
                "COPY OUT ended with a partial row".into(),
            ))
        }
    }
}

fn decode_hex(bytes: &[u8]) -> Result<String, WireError> {
    if bytes.len() % 2 != 0 {
        return Err(WireError::Protocol("odd-length hex COPY row".into()));
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        decoded.push((high << 4) | low);
    }
    String::from_utf8(decoded).map_err(|_| WireError::Protocol("non-UTF-8 JSON row".into()))
}

fn hex_digit(byte: u8) -> Result<u8, WireError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(WireError::Protocol("invalid hex COPY row".into())),
    }
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("required environment variable {name} is not set").into())
}

fn value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn field<'a>(
    row: &'a [Option<String>],
    index: usize,
    name: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(|| format!("missing {name}").into())
}

fn single_value<'a>(
    rows: &'a [Vec<Option<String>>],
    operation: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    rows.first()
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .ok_or_else(|| format!("{operation} returned no value").into())
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('\"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_names_are_quoted() {
        let name = QualifiedName::parse("odd schema.a\"b").unwrap();
        assert_eq!(name.sql(), "\"odd schema\".\"a\"\"b\"");
    }

    #[test]
    fn malformed_qualified_names_are_rejected() {
        assert!(QualifiedName::parse("unqualified").is_err());
        assert!(QualifiedName::parse("a.b.c").is_err());
    }

    #[test]
    fn copy_rows_can_cross_wire_frames() {
        let mut decoder = HexCopyDecoder::default();
        let mut rows = Vec::new();
        decoder
            .push(b"7b2261223a", |row| {
                rows.push(row);
                Ok(())
            })
            .unwrap();
        decoder
            .push(b"317d\n7b2262223a226e5c5c6e227d\n", |row| {
                rows.push(row);
                Ok(())
            })
            .unwrap();
        decoder.finish().unwrap();
        assert_eq!(rows, [r#"{"a":1}"#, r#"{"b":"n\\n"}"#]);
    }
}
