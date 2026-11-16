//! Streamed transactions use disposable files; a durable WAL floor owns replay.
use super::{Result, ident};
use crate::direct_spill::Spill;
use lake_replication_worker::{
    journal::TransactionAssembler,
    pgoutput::{self, Commit, Message},
    sql::sql_literal,
    staging::{changes_json_slice, format_lsn, parse_lsn, relation_columns_json, single_value},
    wire::{Client, Error as WireError},
};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    env,
    io::ErrorKind,
    path::PathBuf,
    time::{Duration, Instant},
};
fn setting(name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let n = env::var(name).unwrap_or(default.to_string()).parse()?;
    if !(min..=max).contains(&n) {
        return Err(format!("invalid {name}").into());
    }
    Ok(n)
}
fn advance(
    sink: &mut Client,
    source: &mut Client,
    store: &Spill,
    published: u64,
    floor: &mut u64,
) -> Result<()> {
    if store.transactions.is_empty() && *floor != published {
        sink.simple_query(&format!(
            "UPDATE lake_direct.stream SET replay_lsn={}::pg_lsn WHERE id=1",
            sql_literal(&format_lsn(published))
        ))?;
        *floor = published;
    }
    source.standby_status(*floor)?;
    Ok(())
}
fn apply(
    sink: &mut Client,
    source: &mut Client,
    store: &mut Spill,
    xid: u32,
    commit: Commit,
    published: &mut u64,
    floor: u64,
    chunk_limit: usize,
    row_limit: usize,
) -> Result<bool> {
    if commit.end_lsn <= *published
        || store
            .transactions
            .get(&xid)
            .ok_or("unknown commit transaction")?
            .rows
            == 0
    {
        store.remove(xid)?;
        return Ok(false);
    }
    let (total, bytes, chunks) = crate::direct_retry::transaction(
        sink,
        source,
        floor,
        |sink, source| {
            sink.simple_query("BEGIN; SET LOCAL statement_timeout='30s'; SELECT id FROM lake_direct.stream WHERE id=1 FOR UPDATE")?;
            let mut replay = store.replay(xid)?;
            let mut chunk = String::from("[");
            let mut rows = 0usize;
            let mut total = 0u64;
            let mut bytes = 0u64;
            let mut chunks = 0u64;
            let mut digest = Sha256::new();
            while let Some((payload, count)) = replay.next()? {
                if chunk.len() > 1
                    && (chunk.len() + payload.len() > chunk_limit
                        || rows + count as usize > row_limit)
                {
                    chunk.push(']');
                    sink.simple_query(&format!(
                        "SELECT lake_direct.apply_payload({}::jsonb)",
                        sql_literal(&chunk)
                    ))?;
                    source.standby_status(floor)?;
                    chunks += 1;
                    chunk = String::from("[");
                    rows = 0;
                }
                digest.update((payload.len() as u64).to_be_bytes());
                digest.update(payload.as_bytes());
                bytes += payload.len() as u64;
                total += u64::from(count);
                if payload.len() > 2 {
                    if chunk.len() > 1 {
                        chunk.push(',')
                    }
                    chunk.push_str(&payload[1..payload.len() - 1]);
                    rows += count as usize;
                }
            }
            if chunk.len() > 1 {
                chunk.push(']');
                sink.simple_query(&format!(
                    "SELECT lake_direct.apply_payload({}::jsonb)",
                    sql_literal(&chunk)
                ))?;
                chunks += 1;
            }
            sink.simple_query(&format!("SELECT lake_direct.finish_large({}::pg_lsn,{}::pg_lsn,{}::pg_lsn,{total},{bytes},{chunks},{}); COMMIT",sql_literal(&format_lsn(*published)),sql_literal(&format_lsn(commit.commit_lsn)),sql_literal(&format_lsn(commit.end_lsn)),sql_literal(&digest.finalize().iter().map(|b|format!("{b:02x}")).collect::<String>())))?;
            Ok((total, bytes, chunks))
        },
    )?;
    *published = commit.end_lsn;
    if env::var("LAKE_DIRECT_EXIT_BEFORE_FEEDBACK").as_deref() == Ok("1") {
        return Err("injected exit after publication before feedback".into());
    }
    store.remove(xid)?;
    eprintln!(
        "direct_large_published xid={xid} rows={total} bytes={bytes} chunks={chunks} end_lsn={}",
        format_lsn(*published)
    );
    Ok(true)
}
pub fn run(
    mut sink: Client,
    mut source: Client,
    mut published: u64,
    slot: &str,
    publication: &str,
    max_commits: u64,
    max_rows: usize,
    max_bytes: usize,
) -> Result<()> {
    let chunk_limit =
        setting("LAKE_DIRECT_CHUNK_BYTES", 1048576, 256, 8388608)?.min(max_bytes as u64) as usize;
    let limit = setting("LAKE_DIRECT_SPILL_BYTES", 268435456, 1, 1073741824)?;
    let active_limit = setting("LAKE_DIRECT_ACTIVE_TRANSACTIONS", 16, 1, 64)? as usize;
    let base = PathBuf::from(
        env::var("LAKE_DIRECT_SPILL_DIRECTORY").unwrap_or("/tmp/lake-direct-spill".into()),
    );
    let identity=single_value(&sink.simple_query("SELECT system_identifier||':'||database_oid::text||':'||slot_name FROM lake_direct.stream WHERE id=1")?,"spill identity")?.to_owned();
    let mut store = Spill::open(&base, &identity, limit, active_limit, chunk_limit)?;
    let state=sink.simple_query("UPDATE lake_direct.stream SET replay_lsn=coalesce(replay_lsn,published_lsn) WHERE id=1 RETURNING replay_lsn::text")?;
    let mut floor = parse_lsn(single_value(&state, "replay floor")?)?;
    let valid=sink.simple_query("SELECT (r.confirmed_flush_lsn<=s.replay_lsn AND r.restart_lsn<=s.replay_lsn AND s.replay_lsn<=s.published_lsn)::text FROM lake_direct.stream s JOIN pg_replication_slots r ON r.slot_name=s.slot_name")?;
    if single_value(&valid, "replay history")? != "true" {
        return Err("slot cannot reconstruct unfinished streamed transactions".into());
    }
    if env::var("LAKE_DIRECT_DEBUG_STREAMING").as_deref() == Ok("1") {
        source
            .simple_query("SET debug_logical_replication_streaming=immediate")
            .map_err(|e| format!("debug streaming setting: {e}"))?;
        source
            .simple_query("SET logical_decoding_work_mem='64kB'")
            .map_err(|e| format!("decoding memory setting: {e}"))?;
    }
    source.start_replication(&format!("START_REPLICATION SLOT {} LOGICAL {} (proto_version '4', publication_names {}, streaming 'true')",ident(slot),format_lsn(floor),format!("'{}'",ident(publication).replace('\'', "''")))).map_err(|e|format!("start streamed replication: {e}"))?;
    source.set_read_timeout(Some(Duration::from_millis(100)))?;
    eprintln!(
        "direct_started pid={} streaming=1 replay_lsn={}",
        std::process::id(),
        format_lsn(floor)
    );
    let mut assembler = TransactionAssembler::default();
    assembler.begin(0, 1)?;
    let mut versions = HashMap::new();
    let mut streamed = None;
    let mut ordinary = None;
    let mut committed = 0;
    let mut feedback = Instant::now();
    loop {
        if feedback.elapsed() >= Duration::from_secs(1) {
            source.standby_status(floor)?;
            feedback = Instant::now();
        }
        let frame = match source.read_copy_data() {
            Ok(v) => v,
            Err(WireError::Io(e))
                if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        if frame.first() == Some(&b'k') && frame.len() == 18 {
            if frame[17] != 0 {
                source.standby_status(floor)?
            }
            continue;
        }
        if frame.first() != Some(&b'w') || frame.len() < 25 {
            return Err("invalid replication frame".into());
        }
        let logical = &frame[25..];
        if logical.len() > 8388608 {
            return Err("logical message exceeds byte limit".into());
        }
        let message = if let Some(top) = streamed {
            if matches!(logical.first(), Some(b'R' | b'I' | b'U' | b'D' | b'T')) {
                let (sub, msg) = pgoutput::decode_streamed(logical)?;
                store.observe_sub(top, sub)?;
                msg
            } else {
                pgoutput::decode(logical)?
            }
        } else {
            pgoutput::decode(logical)?
        };
        match message {
            Message::StreamStart(v) => {
                if streamed.is_some() || ordinary.is_some() {
                    return Err("nested stream segment".into());
                }
                if v.first_segment {
                    store.start(v.xid)?
                } else if !store.transactions.contains_key(&v.xid) {
                    return Err("unknown stream continuation".into());
                }
                streamed = Some(v.xid);
            }
            Message::StreamStop => {
                let xid = streamed.take().ok_or("unexpected stream stop")?;
                let tx = &store.transactions[&xid];
                eprintln!(
                    "direct_spilled xid={xid} rows={} bytes={}",
                    tx.rows, tx.bytes
                );
            }
            Message::Begin(v) => {
                if streamed.is_some() || ordinary.is_some() {
                    return Err("nested ordinary transaction".into());
                }
                store.start(v.xid)?;
                ordinary = Some(v.xid);
            }
            Message::Commit(v) => {
                let xid = ordinary.take().ok_or("commit without begin")?;
                if apply(
                    &mut sink,
                    &mut source,
                    &mut store,
                    xid,
                    v,
                    &mut published,
                    floor,
                    chunk_limit,
                    max_rows,
                )? {
                    committed += 1;
                }
                advance(&mut sink, &mut source, &store, published, &mut floor)?;
            }
            Message::StreamCommit(v) => {
                if streamed.is_some() || ordinary.is_some() {
                    return Err("commit inside segment".into());
                }
                if apply(
                    &mut sink,
                    &mut source,
                    &mut store,
                    v.xid,
                    v.commit,
                    &mut published,
                    floor,
                    chunk_limit,
                    max_rows,
                )? {
                    committed += 1;
                }
                advance(&mut sink, &mut source, &store, published, &mut floor)?;
            }
            Message::StreamAbort(v) => {
                if streamed.is_some() || ordinary.is_some() {
                    return Err("abort inside segment".into());
                }
                if v.xid == v.subxid {
                    store.remove(v.xid)?
                } else {
                    store.abort_sub(v.xid, v.subxid)?
                }
                advance(&mut sink, &mut source, &store, published, &mut floor)?;
            }
            Message::Relation(r) => {
                let descriptor = format!(
                    "jsonb_build_object('namespace',{},'name',{},'replica_identity',{},'columns',{}::jsonb)",
                    sql_literal(&r.namespace),
                    sql_literal(&r.name),
                    sql_literal(&(r.replica_identity as char).to_string()),
                    sql_literal(&relation_columns_json(&r))
                );
                let valid=sink.simple_query(&format!("SELECT (descriptor={descriptor})::text FROM lake_direct.mapping WHERE source_relation={}::oid",r.id))?;
                if single_value(&valid, "frozen descriptor")? != "true" {
                    return Err("frozen relation descriptor changed".into());
                }
                if versions.len() >= 64 && !versions.contains_key(&r.id) {
                    return Err("relation limit exceeded".into());
                }
                versions.insert(r.id, 1);
                assembler.relation(r);
            }
            other => {
                let top = streamed.or(ordinary).ok_or("change outside transaction")?;
                match other {
                    Message::Insert(v) => assembler.insert(v)?,
                    Message::Update(v) => assembler.update(v)?,
                    Message::Delete(v) => assembler.delete(v)?,
                    Message::Truncate(v) => assembler.truncate(v)?,
                    _ => return Err("unexpected streaming message".into()),
                }
                let changes = assembler.drain_changes()?;
                if changes.len() > max_rows {
                    return Err("single message exceeds chunk row bound".into());
                }
                let payload = changes_json_slice(&changes, &versions)?;
                if payload.len() + 1 > chunk_limit {
                    return Err("single change exceeds chunk byte bound".into());
                }
                store.append(top, &payload, changes.len() as u32)?;
            }
        }
        if committed >= max_commits {
            return Ok(());
        }
    }
}
