//! Bounded same-instance snapshot bootstrap for the optional direct experiment.
use lake_replication_worker::sql::sql_literal;
use lake_replication_worker::staging::single_value;
use lake_replication_worker::wire::{Client, address_is_loopback};
use std::{env, thread, time::Duration};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
fn required(name: &str) -> Result<String> {
    let value = env::var(name)?;
    if value.is_empty() {
        return Err(format!("{name} must be nonempty").into());
    }
    Ok(value)
}
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn main() {
    if let Err(error) = run() {
        eprintln!("direct snapshot failed; reconcile committed stream state before retry: {error}");
        std::process::exit(1)
    }
}
fn run() -> Result<()> {
    if required("LAKE_DIRECT_EXPERIMENT")? != "1" {
        return Err("explicit experiment opt-in required".into());
    }
    let address = required("LAKE_DIRECT_ADDRESS")?;
    if !address_is_loopback(&address) {
        return Err("prototype requires loopback".into());
    }
    let database = required("LAKE_DIRECT_DATABASE")?;
    let user = required("LAKE_DIRECT_USER")?;
    let password = env::var("LAKE_DIRECT_PASSWORD").ok();
    let slot = required("LAKE_DIRECT_SLOT")?;
    let publication = required("LAKE_DIRECT_PUBLICATION")?;
    let schema = required("LAKE_DIRECT_TARGET_SCHEMA")?;
    if publication.len() > 63 || schema.len() > 63 {
        return Err("publication and schema identifiers must fit PostgreSQL name limits".into());
    }
    if slot.len() > 63
        || !slot
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err("invalid logical slot name".into());
    }
    let rows: u64 = env::var("LAKE_DIRECT_SNAPSHOT_MAX_ROWS")
        .unwrap_or("100000".into())
        .parse()?;
    let bytes: u64 = env::var("LAKE_DIRECT_SNAPSHOT_MAX_BYTES")
        .unwrap_or("67108864".into())
        .parse()?;
    let pause: u64 = env::var("LAKE_DIRECT_SNAPSHOT_PAUSE_MS")
        .unwrap_or("0".into())
        .parse()?;
    let table_pause: u64 = env::var("LAKE_DIRECT_SNAPSHOT_TABLE_PAUSE_MS")
        .unwrap_or("0".into())
        .parse()?;
    if !(1..=100000).contains(&rows)
        || !(1..=67108864).contains(&bytes)
        || pause > 60000
        || table_pause > 60000
    {
        return Err("invalid snapshot bounds".into());
    }
    let mut sink = Client::connect_named_secure(
        &address,
        &user,
        &database,
        false,
        "lake_direct_snapshot_control",
        password.as_deref(),
        None,
    )?;
    sink.simple_query("SET client_connection_check_interval='100ms'")?;
    let locked = sink.simple_query("SELECT pg_try_advisory_lock(1818321765,1)::text")?;
    if single_value(&locked, "snapshot ownership")? != "true" {
        return Err("another direct worker owns this database".into());
    }
    let ready=sink.simple_query("SELECT published_lsn::text,slot_name::text,publication::text,system_identifier,timeline::text,database_oid::text FROM lake_direct.stream WHERE id=1")?;
    if let Some(row) = ready.first() {
        let state=sink.simple_query(&format!("SELECT (s.slot_name={}::name AND s.publication={}::name AND s.system_identifier=(pg_control_system()).system_identifier::text AND s.timeline=(pg_control_checkpoint()).timeline_id AND s.database_oid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND EXISTS(SELECT FROM pg_replication_slots r WHERE r.slot_name=s.slot_name AND r.plugin='pgoutput' AND r.database=current_database() AND r.invalidation_reason IS NULL AND r.restart_lsn<=s.published_lsn AND r.confirmed_flush_lsn<=s.published_lsn) AND NOT EXISTS(SELECT FROM lake_direct.mapping m JOIN pg_class c ON c.oid=m.destination_relation JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname<>{}))::text FROM lake_direct.stream s WHERE id=1",sql_literal(&slot),sql_literal(&publication),sql_literal(&schema)))?;
        if single_value(&state, "existing bootstrap identity")? != "true" {
            return Err(
                "existing direct stream differs or lacks required history; refusing replacement"
                    .into(),
            );
        }
        eprintln!(
            "direct_snapshot_already_ready lsn={}",
            row[0].as_deref().ok_or("missing checkpoint")?
        );
        return Ok(());
    }
    let existing = sink.simple_query(&format!(
        "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name={}",
        sql_literal(&slot)
    ))?;
    let exists = single_value(&existing, "persistent slot lookup")? != "0";
    if exists {
        let valid=sink.simple_query(&format!("SELECT (plugin='pgoutput' AND database=current_database() AND NOT active AND NOT temporary AND invalidation_reason IS NULL AND restart_lsn IS NOT NULL)::text FROM pg_replication_slots WHERE slot_name={}",sql_literal(&slot)))?;
        if single_value(&valid, "retained snapshot slot")? != "true" {
            return Err("existing slot is busy, invalid or belongs to another source".into());
        }
    }
    // Pin source row types before exporting the snapshot. Taking these locks
    // after export leaves a gap for non-MVCC-safe table rewrites.
    let mut schema_guard = Client::connect_named_secure(
        &address,
        &user,
        &database,
        false,
        "lake_direct_snapshot_schema_guard",
        password.as_deref(),
        None,
    )?;
    schema_guard.simple_query("BEGIN READ ONLY; SET LOCAL lock_timeout='10s'; SET LOCAL client_connection_check_interval='100ms'")?;
    let locks=schema_guard.simple_query(&format!("SELECT format('LOCK TABLE ONLY %I.%I IN ACCESS SHARE MODE',n.nspname,c.relname) FROM pg_publication_rel pr JOIN pg_publication p ON p.oid=pr.prpubid JOIN pg_class c ON c.oid=pr.prrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE p.pubname={} ORDER BY c.oid",sql_literal(&publication)))?;
    if locks.is_empty() || locks.len() > 64 {
        return Err("invalid explicit snapshot table set".into());
    }
    for row in locks {
        schema_guard.simple_query(
            row.first()
                .and_then(Option::as_deref)
                .ok_or("missing source lock")?,
        )?;
    }
    let mut owner = Client::connect_named_secure(
        &address,
        &user,
        &database,
        true,
        "lake_direct_snapshot_owner",
        password.as_deref(),
        None,
    )?;
    let identity = owner.simple_query("IDENTIFY_SYSTEM")?;
    let origin = identity.first().ok_or("missing source identity")?;
    let system = origin[0].as_deref().ok_or("missing system identifier")?;
    let timeline = origin[1].as_deref().ok_or("missing timeline")?;
    let create = if exists {
        format!(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')",
            ident(&format!("lake_direct_snapshot_{}", std::process::id()))
        )
    } else {
        format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export')",
            ident(&slot)
        )
    };
    let result = owner.simple_query(&create)?;
    let row = result.first().ok_or("missing exported snapshot")?;
    let lsn = row[1].as_deref().ok_or("missing consistent LSN")?;
    let snapshot = row[2].as_deref().ok_or("missing snapshot name")?;
    sink.simple_query(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT {}; SET LOCAL statement_timeout='180s'",sql_literal(snapshot)))?;
    eprintln!(
        "direct_snapshot_acquired pid={} lsn={lsn} snapshot={snapshot}",
        std::process::id()
    );
    if pause > 0 {
        thread::sleep(Duration::from_millis(pause))
    }
    if let Ok(point) = env::var("LAKE_DIRECT_CRASH_POINT") {
        sink.simple_query(&format!(
            "SET LOCAL pg_lake_table.lab_crash_point={}",
            sql_literal(&point)
        ))?;
    }
    let copied=sink.simple_query(&format!("SELECT lake_direct.bootstrap({}::name,{}::name,{}::name,{}::pg_lsn,{},{},{}::bigint,{rows},{bytes},{table_pause})::text",sql_literal(&slot),sql_literal(&publication),sql_literal(&schema),sql_literal(lsn),sql_literal(snapshot),sql_literal(system),sql_literal(timeline)))?;
    let copied = single_value(&copied, "copied snapshot rows")?.to_owned();
    // If the pre-export source lock connection was lost before this
    // transaction acquired its own locks, a rewrite could invalidate the
    // imported row view. Never activate after losing that independent guard.
    // After this check our own source locks remain held through COMMIT.
    schema_guard.simple_query("SELECT 1")?;
    sink.simple_query("COMMIT")?;
    if env::var("LAKE_DIRECT_EXIT_AFTER_SNAPSHOT_COMMIT").as_deref() == Ok("1") {
        return Err("injected exit after snapshot COMMIT".into());
    }
    eprintln!("direct_snapshot_ready lsn={lsn} rows={copied}");
    // Closing the independent owner releases the exported snapshot, after COMMIT.
    drop(owner);
    schema_guard.simple_query("COMMIT")?;
    Ok(())
}
