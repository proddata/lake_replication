use lake_replication_worker::credentials::Settings;
use lake_replication_worker::journal::TransactionAssembler;
use lake_replication_worker::logging::event as log_event;
use lake_replication_worker::managed::ManagedClient;
use lake_replication_worker::pgoutput::{self, Message, Relation};
use lake_replication_worker::sql::sql_literal;
use lake_replication_worker::staging::{
    abort_stream, append_stream_segment, begin_stream, changes_json_slice, checkpoint,
    commit_stream, format_lsn, mark_acknowledged, register_relation, single_value,
    stage_transaction,
};
use lake_replication_worker::wire::{Client, Error as WireError, TlsConfig, address_is_loopback};
use std::collections::HashMap;
use std::env;
use std::io::ErrorKind;
use std::thread;
use std::time::Duration;

const WAL_HEADER_LENGTH: usize = 25;
const KEEPALIVE_LENGTH: usize = 18;

fn main() {
    if let Err(error) = run() {
        log_event(
            "receiver",
            "ERROR",
            "process_failed",
            &error.to_string(),
            &[],
        );
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let mut destination = ManagedClient::connect(
        config.destination_address.as_str(),
        &config.destination_user,
        &config.destination_database,
        config.destination_password.as_deref(),
        config.destination_tls.as_ref(),
        config.subscription_id,
        config.config_generation,
    )?;
    let start_lsn = checkpoint(&mut destination, config.subscription_id)?;
    let schema_contracts = schema_contracts_enabled(&mut destination, config.subscription_id)?;
    let mut metadata_source = if schema_contracts {
        Some(Client::connect_secure(
            config.source_address.as_str(),
            &config.source_user,
            &config.source_database,
            false,
            config.source_password.as_deref(),
            config.source_tls.as_ref(),
        )?)
    } else {
        None
    };
    let mut source = Client::connect_secure(
        config.source_address.as_str(),
        &config.source_user,
        &config.source_database,
        true,
        config.source_password.as_deref(),
        config.source_tls.as_ref(),
    )?;
    if config.idle_timeout_ms > 0 {
        source.set_read_timeout(Some(Duration::from_millis(config.idle_timeout_ms)))?;
    }
    if config.debug_streaming_immediate {
        source.simple_query("SET debug_logical_replication_streaming=immediate")?;
    }
    let command = format!(
        "START_REPLICATION SLOT {} LOGICAL {} (proto_version '4', publication_names '{}', streaming 'true')",
        quote_identifier(&config.slot),
        format_lsn(start_lsn),
        config.publication.replace('\'', "''"),
    );
    source.start_replication(&command)?;
    log_event(
        "receiver",
        "INFO",
        "replication_started",
        "logical replication stream started",
        &[
            ("subscription_id", config.subscription_id.to_string()),
            ("slot", config.slot.clone()),
            ("publication", config.publication.clone()),
            ("start_lsn", format_lsn(start_lsn)),
        ],
    );

    let mut assembler = TransactionAssembler::default();
    let mut streamed = HashMap::<u32, TransactionAssembler>::new();
    let mut active_stream = None;
    let mut schema_versions = HashMap::new();
    let mut committed = 0_u64;
    let mut feedback_lsn = start_lsn;
    loop {
        let copy_data = match source.read_copy_data() {
            Ok(copy_data) => copy_data,
            Err(WireError::Io(error))
                if config.idle_timeout_ms > 0
                    && matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                if config.persistent {
                    // A timeout is a supervision/feedback tick, not a reason to
                    // restart decoding. MessageReader retains partial frames.
                    source.standby_status(feedback_lsn)?;
                    continue;
                }
                log_event(
                    "receiver",
                    "INFO",
                    "idle_timeout",
                    &format!("receiver idle timeout after {}ms", config.idle_timeout_ms),
                    &[
                        ("subscription_id", config.subscription_id.to_string()),
                        ("idle_timeout_ms", config.idle_timeout_ms.to_string()),
                    ],
                );
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        match copy_data.first().copied() {
            Some(b'w') if copy_data.len() >= WAL_HEADER_LENGTH => {
                let wal_start = u64::from_be_bytes(copy_data[1..9].try_into().unwrap());
                let logical = &copy_data[WAL_HEADER_LENGTH..];
                if let (Some(active_xid), true) = (
                    active_stream,
                    matches!(logical.first(), Some(b'R' | b'I' | b'U' | b'D' | b'T')),
                ) {
                    let (message_xid, message) = pgoutput::decode_streamed(logical)?;
                    if message_xid != active_xid {
                        return Err(format!(
                            "stream segment XID {active_xid} contained message for {message_xid}"
                        )
                        .into());
                    }
                    let stream_assembler = streamed
                        .get_mut(&active_xid)
                        .ok_or("stream segment has no transaction assembler")?;
                    match message {
                        Message::Relation(relation) => {
                            let version = register_and_reconcile_relation(
                                &mut destination,
                                metadata_source.as_mut(),
                                config.subscription_id,
                                config.type_mapping_version,
                                &relation,
                            )?;
                            schema_versions.insert(relation.id, version);
                            stream_assembler.relation(relation);
                        }
                        Message::Insert(insert) => stream_assembler.insert(insert)?,
                        Message::Update(update) => stream_assembler.update(update)?,
                        Message::Delete(delete) => stream_assembler.delete(delete)?,
                        Message::Truncate(truncate) => stream_assembler.truncate(truncate)?,
                        _ => return Err("invalid message inside stream segment".into()),
                    }
                    continue;
                }
                let message = pgoutput::decode(logical)?;
                match message {
                    Message::Relation(relation) => {
                        let version = register_and_reconcile_relation(
                            &mut destination,
                            metadata_source.as_mut(),
                            config.subscription_id,
                            config.type_mapping_version,
                            &relation,
                        )?;
                        schema_versions.insert(relation.id, version);
                        assembler.relation(relation);
                    }
                    Message::Begin(begin) => assembler.begin(wal_start, begin.xid)?,
                    Message::Insert(insert) => assembler.insert(insert)?,
                    Message::Update(update) => assembler.update(update)?,
                    Message::Delete(delete) => assembler.delete(delete)?,
                    Message::Truncate(truncate) => assembler.truncate(truncate)?,
                    Message::StreamStart(start) => {
                        if active_stream.is_some() {
                            return Err("nested stream segment".into());
                        }
                        if start.first_segment {
                            if streamed.contains_key(&start.xid) {
                                return Err(format!(
                                    "duplicate first segment for XID {}",
                                    start.xid
                                )
                                .into());
                            }
                            let mut stream_assembler = TransactionAssembler::default();
                            stream_assembler.begin(wal_start, start.xid)?;
                            begin_stream(
                                &mut destination,
                                config.subscription_id,
                                start.xid,
                                wal_start,
                            )?;
                            streamed.insert(start.xid, stream_assembler);
                        } else if !streamed.contains_key(&start.xid) {
                            return Err(
                                format!("continuation for unknown XID {}", start.xid).into()
                            );
                        }
                        active_stream = Some(start.xid);
                        log_event(
                            "receiver",
                            "INFO",
                            "stream_segment_started",
                            &format!(
                                "stream segment xid={} first={}",
                                start.xid, start.first_segment
                            ),
                            &[
                                ("subscription_id", config.subscription_id.to_string()),
                                ("xid", start.xid.to_string()),
                                ("first_segment", start.first_segment.to_string()),
                            ],
                        );
                    }
                    Message::StreamStop => {
                        let xid = active_stream
                            .take()
                            .ok_or("STREAM STOP outside a segment")?;
                        let changes = streamed
                            .get_mut(&xid)
                            .ok_or("stream segment has no transaction assembler")?
                            .drain_changes()?;
                        if !changes.is_empty() {
                            let payload = changes_json_slice(&changes, &schema_versions)?;
                            append_stream_segment(
                                &mut destination,
                                config.subscription_id,
                                xid,
                                &payload,
                            )?;
                        }
                    }
                    Message::StreamAbort(abort) => {
                        if active_stream == Some(abort.xid) {
                            active_stream = None;
                        }
                        if streamed.remove(&abort.xid).is_none() {
                            return Err(format!("abort for unknown XID {}", abort.xid).into());
                        }
                        abort_stream(&mut destination, config.subscription_id, abort.xid)?;
                        log_event(
                            "receiver",
                            "INFO",
                            "stream_transaction_aborted",
                            &format!("discarded streamed xid={}", abort.xid),
                            &[
                                ("subscription_id", config.subscription_id.to_string()),
                                ("xid", abort.xid.to_string()),
                            ],
                        );
                    }
                    Message::StreamCommit(stream_commit) => {
                        if active_stream == Some(stream_commit.xid) {
                            return Err("STREAM COMMIT arrived before STREAM STOP".into());
                        }
                        let mut stream_assembler = streamed
                            .remove(&stream_commit.xid)
                            .ok_or("commit for unknown streamed transaction")?;
                        let transaction = stream_assembler.commit(stream_commit.commit)?;
                        let change_count = commit_stream(
                            &mut destination,
                            &mut || {
                                wait_for_journal(&mut source, feedback_lsn)?;
                                Ok(())
                            },
                            config.subscription_id,
                            &transaction,
                        )?;
                        source.standby_status(transaction.end_lsn)?;
                        feedback_lsn = transaction.end_lsn;
                        mark_acknowledged(
                            &mut destination,
                            config.subscription_id,
                            transaction.end_lsn,
                        )?;
                        committed += 1;
                        log_event(
                            "receiver",
                            "INFO",
                            "transaction_staged",
                            &format!(
                                "staged streamed xid={} end_lsn={} changes={}",
                                transaction.xid,
                                format_lsn(transaction.end_lsn),
                                change_count
                            ),
                            &[
                                ("subscription_id", config.subscription_id.to_string()),
                                ("xid", transaction.xid.to_string()),
                                ("end_lsn", format_lsn(transaction.end_lsn)),
                                ("changes", change_count.to_string()),
                                ("streamed", "true".to_owned()),
                            ],
                        );
                        if committed >= config.max_transactions {
                            return Ok(());
                        }
                    }
                    Message::Commit(commit) => {
                        let transaction = assembler.commit(commit)?;
                        stage_transaction(
                            &mut destination,
                            &mut || {
                                wait_for_journal(&mut source, feedback_lsn)?;
                                Ok(())
                            },
                            config.subscription_id,
                            &transaction,
                            &schema_versions,
                        )?;
                        // WAL feedback is deliberately ordered after the staging COMMIT.
                        source.standby_status(transaction.end_lsn)?;
                        feedback_lsn = transaction.end_lsn;
                        mark_acknowledged(
                            &mut destination,
                            config.subscription_id,
                            transaction.end_lsn,
                        )?;
                        committed += 1;
                        log_event(
                            "receiver",
                            "INFO",
                            "transaction_staged",
                            &format!(
                                "staged xid={} end_lsn={} changes={}",
                                transaction.xid,
                                format_lsn(transaction.end_lsn),
                                transaction.changes.len()
                            ),
                            &[
                                ("subscription_id", config.subscription_id.to_string()),
                                ("xid", transaction.xid.to_string()),
                                ("end_lsn", format_lsn(transaction.end_lsn)),
                                ("changes", transaction.changes.len().to_string()),
                                ("streamed", "false".to_owned()),
                            ],
                        );
                        if committed >= config.max_transactions {
                            return Ok(());
                        }
                    }
                }
            }
            Some(b'k') if copy_data.len() >= KEEPALIVE_LENGTH => {
                if copy_data[17] != 0 {
                    source.standby_status(feedback_lsn)?;
                }
            }
            Some(tag) => return Err(format!("unsupported CopyData tag {tag:#x}").into()),
            None => return Err("empty CopyData message".into()),
        }
    }
}

struct Config {
    source_address: String,
    destination_address: String,
    source_user: String,
    source_password: Option<String>,
    source_tls: Option<TlsConfig>,
    destination_user: String,
    destination_password: Option<String>,
    destination_tls: Option<TlsConfig>,
    source_database: String,
    destination_database: String,
    slot: String,
    publication: String,
    subscription_id: i64,
    config_generation: i64,
    max_transactions: u64,
    debug_streaming_immediate: bool,
    type_mapping_version: i32,
    idle_timeout_ms: u64,
    persistent: bool,
}

impl Config {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let mut settings = Settings::from_env()?;
        let legacy_user = value("LAKE_USER", "postgres");
        let source_address = value("LAKE_SOURCE_ADDRESS", "127.0.0.1:55434");
        let destination_address = value("LAKE_DESTINATION_ADDRESS", "127.0.0.1:55434");
        let destination_user = settings.destination_user()?;
        let destination_password = settings.optional("LAKE_DESTINATION_PASSWORD");
        let destination_tls = tls_config(&settings, "LAKE_DESTINATION", &[&destination_address])?;
        let destination_database = value("LAKE_DESTINATION_DATABASE", "analytics");
        let subscription_id: i64 = required("LAKE_SUBSCRIPTION_ID")?.parse()?;
        settings.load_subscription_source_credentials(
            subscription_id,
            &destination_address,
            &destination_user,
            &destination_database,
            destination_password.as_deref(),
            destination_tls.as_ref(),
        )?;
        Ok(Self {
            source_tls: tls_config(&settings, "LAKE_SOURCE", &[&source_address])?,
            destination_tls,
            source_address,
            destination_address,
            source_user: settings.value("LAKE_SOURCE_USER", &legacy_user),
            source_password: settings.optional("LAKE_SOURCE_PASSWORD"),
            destination_user,
            destination_password,
            source_database: value("LAKE_SOURCE_DATABASE", "application"),
            destination_database,
            slot: required("LAKE_SLOT")?,
            publication: required("LAKE_PUBLICATION")?,
            subscription_id,
            config_generation: value("LAKE_CONFIG_GENERATION", "0").parse()?,
            max_transactions: value("LAKE_MAX_TRANSACTIONS", "1").parse()?,
            debug_streaming_immediate: value("LAKE_DEBUG_STREAMING_IMMEDIATE", "0") == "1",
            type_mapping_version: value("LAKE_TYPE_MAPPING_VERSION", "1").parse()?,
            persistent: value("LAKE_RECEIVER_PERSISTENT", "0") == "1",
            idle_timeout_ms: value("LAKE_RECEIVER_IDLE_TIMEOUT_MS", "0").parse()?,
        })
    }
}

fn tls_config(
    settings: &Settings,
    prefix: &str,
    addresses: &[&str],
) -> Result<Option<TlsConfig>, Box<dyn std::error::Error>> {
    let default_mode = if addresses.iter().all(|address| address_is_loopback(address)) {
        "disable"
    } else {
        "verify-full"
    };
    match settings
        .value(&format!("{prefix}_TLS_MODE"), default_mode)
        .as_str()
    {
        "disable" => Ok(None),
        "verify-full" => Ok(Some(TlsConfig::verify_full(
            settings.required(&format!("{prefix}_TLS_ROOT_CERT"))?,
            settings.optional(&format!("{prefix}_TLS_SERVER_NAME")),
        ))),
        mode => Err(format!(
            "unsupported {prefix}_TLS_MODE {mode}; expected disable or verify-full"
        )
        .into()),
    }
}

fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("required environment variable {name} is not set").into())
}

fn value(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn schema_contracts_enabled(
    client: &mut ManagedClient,
    subscription_id: i64,
) -> Result<bool, Box<dyn std::error::Error>> {
    let rows = client.simple_query(&format!(
        "SELECT EXISTS(SELECT 1 FROM lake_replication.relation_type_contracts WHERE subscription_id={subscription_id})::text"
    ))?;
    Ok(single_value(&rows, "schema contract check")? == "true")
}

fn register_and_reconcile_relation(
    destination: &mut ManagedClient,
    metadata_source: Option<&mut Client>,
    subscription_id: i64,
    mapping_version: i32,
    relation: &Relation,
) -> Result<i64, Box<dyn std::error::Error>> {
    let version = register_relation(destination, subscription_id, relation)?;
    let Some(metadata_source) = metadata_source else {
        return Ok(version);
    };
    let contract = source_type_contract(metadata_source, relation)?;
    if let Err(error) = destination.simple_query(&format!(
        "SELECT lake_replication.register_type_contract({subscription_id}, {}, {version}, {mapping_version}, {}::jsonb)",
        relation.id,
        sql_literal(&contract),
    )) {
        let message = format!("source type contract rejected: {error}");
        record_schema_error(destination, subscription_id, relation.id, version, &message)?;
        return Err(message.into());
    }
    let rows = destination.simple_query(&format!(
        "SELECT lake_replication.reconcile_relation_schema({subscription_id}, {}, {version})",
        relation.id,
    ))?;
    let state = single_value(&rows, "schema reconciliation")?;
    if state != "ready" {
        return Err(state.to_owned().into());
    }
    Ok(version)
}

fn source_type_contract(
    source: &mut Client,
    relation: &Relation,
) -> Result<String, Box<dyn std::error::Error>> {
    let names = relation
        .columns
        .iter()
        .map(|column| sql_literal(&column.name))
        .collect::<Vec<_>>()
        .join(",");
    let rows = source.simple_query(&format!(
        "SELECT jsonb_agg(jsonb_build_object(
                    'name', a.attname,
                    'source_type', format_type(a.atttypid, a.atttypmod),
                    'not_null', a.attnotnull,
                    'generated', a.attgenerated <> '')
                    ORDER BY array_position(ARRAY[{names}]::text[], a.attname))::text
           FROM pg_attribute a
          WHERE a.attrelid={} AND a.attnum > 0 AND NOT a.attisdropped
            AND a.attname = ANY(ARRAY[{names}]::text[])",
        relation.id,
    ))?;
    let contract = single_value(&rows, "source type contract")?.to_owned();
    if contract == "null" {
        return Err(format!("source relation {} has no published columns", relation.id).into());
    }
    Ok(contract)
}

fn record_schema_error(
    destination: &mut ManagedClient,
    subscription_id: i64,
    relation_id: u32,
    version: i64,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    destination.simple_query(&format!(
        "SELECT lake_replication.record_schema_error({subscription_id}, {relation_id}, {version}, {})",
        sql_literal(message),
    ))?;
    Ok(())
}

fn wait_for_journal(source: &mut Client, feedback_lsn: u64) -> Result<(), WireError> {
    // Keep the sender alive without acknowledging the unadmitted commit.
    source.standby_status(feedback_lsn)?;
    thread::sleep(Duration::from_millis(100));
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('\"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lake_replication_worker::staging::{json_string, parse_lsn};

    #[test]
    fn lsn_round_trip() {
        let lsn = parse_lsn("16/B374D848").unwrap();
        assert_eq!(format_lsn(lsn), "16/B374D848");
    }

    #[test]
    fn escaping_is_valid_for_json_and_sql() {
        assert_eq!(json_string("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
        assert_eq!(sql_literal("O'Reilly"), "E'O''Reilly'");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }
}
