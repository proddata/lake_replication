//! Shared destination journal admission and serialization.
//! Callers own transport keepalives; all SQL runs through ManagedClient fences.
use crate::events::{
    Cell, IdentityKind, IdentityTuple, Operation, Relation, StagedTransaction, TupleFormat,
};
use crate::managed::ManagedClient;
use crate::sql::sql_literal;
use std::collections::HashMap;

pub fn checkpoint(
    client: &mut ManagedClient,
    subscription_id: i64,
) -> Result<u64, Box<dyn std::error::Error>> {
    let rows = client.simple_query(&format!(
        "SELECT staged_lsn::text FROM lake_replication.checkpoints WHERE subscription_id = {subscription_id}"
    ))?;
    let value = rows
        .first()
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .ok_or("subscription checkpoint not found")?;
    parse_lsn(value)
}

pub fn register_relation(
    client: &mut ManagedClient,
    subscription_id: i64,
    relation: &Relation,
) -> Result<i64, Box<dyn std::error::Error>> {
    let columns = relation_columns_json(relation);
    let rows = client.simple_query(&format!(
        "SELECT lake_replication.register_relation({subscription_id}, {}, {}, {}, {}, {}::jsonb)",
        relation.id,
        sql_literal(&relation.namespace),
        sql_literal(&relation.name),
        sql_literal(&(relation.replica_identity as char).to_string()),
        sql_literal(&columns),
    ))?;
    Ok(single_value(&rows, "register_relation")?.parse()?)
}

pub fn stage_transaction(
    client: &mut ManagedClient,
    wait_for_admission: &mut impl FnMut() -> Result<(), Box<dyn std::error::Error>>,
    subscription_id: i64,
    transaction: &StagedTransaction,
    schema_versions: &HashMap<u32, i64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let changes = changes_json(transaction, schema_versions)?;
    let sql = format!(
        "SELECT lake_replication.try_stage_transaction({subscription_id}, {}, {}::pg_lsn, {}::pg_lsn, {}::pg_lsn, TIMESTAMPTZ '2000-01-01 00:00:00+00' + {} * INTERVAL '1 microsecond', {}::jsonb)::text",
        transaction.xid,
        sql_literal(&format_lsn(transaction.begin_lsn)),
        sql_literal(&format_lsn(transaction.commit_lsn)),
        sql_literal(&format_lsn(transaction.end_lsn)),
        transaction.commit_time_micros,
        sql_literal(&changes),
    );
    loop {
        let rows = client.simple_query(&sql)?;
        match single_value(&rows, "journal admission")? {
            "true" => return Ok(()),
            "false" => wait_for_admission()?,
            _ => return Err("invalid journal admission response".into()),
        }
    }
}

pub fn begin_stream(
    client: &mut ManagedClient,
    subscription_id: i64,
    xid: u32,
    begin_lsn: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    client.simple_query(&format!(
        "SELECT lake_replication.begin_stream({subscription_id}, {xid}, {}::pg_lsn)",
        sql_literal(&format_lsn(begin_lsn))
    ))?;
    Ok(())
}

pub fn append_stream_segment(
    client: &mut ManagedClient,
    subscription_id: i64,
    xid: u32,
    changes: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client.simple_query(&format!(
        "SELECT lake_replication.append_stream_segment({subscription_id}, {xid}, {}::jsonb)",
        sql_literal(changes)
    ))?;
    Ok(())
}

pub fn abort_stream(
    client: &mut ManagedClient,
    subscription_id: i64,
    xid: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    client.simple_query(&format!(
        "SELECT lake_replication.abort_stream({subscription_id}, {xid})"
    ))?;
    Ok(())
}

pub fn commit_stream(
    client: &mut ManagedClient,
    wait_for_admission: &mut impl FnMut() -> Result<(), Box<dyn std::error::Error>>,
    subscription_id: i64,
    transaction: &StagedTransaction,
) -> Result<u64, Box<dyn std::error::Error>> {
    let sql = format!(
        "SELECT lake_replication.try_commit_stream({subscription_id}, {}, {}::pg_lsn, {}::pg_lsn, TIMESTAMPTZ '2000-01-01 00:00:00+00' + {} * INTERVAL '1 microsecond')::text",
        transaction.xid,
        sql_literal(&format_lsn(transaction.commit_lsn)),
        sql_literal(&format_lsn(transaction.end_lsn)),
        transaction.commit_time_micros,
    );
    loop {
        let rows = client.simple_query(&sql)?;
        if let Some(value) = rows
            .first()
            .and_then(|r| r.first())
            .and_then(Option::as_ref)
        {
            return Ok(value.parse()?);
        }
        wait_for_admission()?;
    }
}

pub fn mark_acknowledged(
    client: &mut ManagedClient,
    subscription_id: i64,
    lsn: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    client.simple_query(&format!(
        "SELECT lake_replication.mark_acknowledged({subscription_id}, {}::pg_lsn)",
        sql_literal(&format_lsn(lsn)),
    ))?;
    Ok(())
}

pub fn relation_columns_json(relation: &Relation) -> String {
    let mut json = String::from("[");
    for (index, column) in relation.columns.iter().enumerate() {
        if index != 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"flags\":{},\"name\":{},\"type_oid\":{},\"type_modifier\":{}}}",
            column.flags,
            json_string(&column.name),
            column.type_oid,
            column.type_modifier
        ));
    }
    json.push(']');
    json
}

pub fn changes_json(
    transaction: &StagedTransaction,
    schema_versions: &HashMap<u32, i64>,
) -> Result<String, Box<dyn std::error::Error>> {
    changes_json_slice(&transaction.changes, schema_versions)
}

pub(crate) fn change_payload_fields(
    change: &crate::events::StagedChange,
) -> Result<String, Box<dyn std::error::Error>> {
    let (operation, old_tuple, new_tuple) = match &change.operation {
        Operation::Insert { new_values } => (
            "insert",
            None,
            Some(tuple_object(&change.relation, new_values, false)?),
        ),
        Operation::Update {
            old_tuple,
            new_values,
        } => {
            let identity = match old_tuple {
                Some(tuple) => identity_object(&change.relation, tuple)?,
                None => key_object(&change.relation, new_values)?,
            };
            (
                "update",
                Some(identity),
                Some(tuple_object(&change.relation, new_values, true)?),
            )
        }
        Operation::Delete { old_tuple } => (
            "delete",
            Some(identity_object(&change.relation, old_tuple)?),
            None,
        ),
        Operation::Truncate { options: _ } => ("truncate", None, None),
    };
    let mut json = format!("\"operation\":{}", json_string(operation));
    if let Some(old_tuple) = old_tuple {
        json.push_str(",\"old_tuple\":");
        json.push_str(&old_tuple);
    }
    if let Some(new_tuple) = new_tuple {
        json.push_str(",\"new_tuple\":");
        json.push_str(&new_tuple);
    }
    Ok(json)
}

pub fn changes_json_slice(
    changes: &[crate::events::StagedChange],
    schema_versions: &HashMap<u32, i64>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut json = String::from("[");
    for (index, change) in changes.iter().enumerate() {
        let version = schema_versions
            .get(&change.relation_id)
            .ok_or("relation has no registered schema version")?;
        if index != 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"remote_relation_id\":{},\"relation_schema_version\":{},{}}}",
            change.relation_id,
            version,
            change_payload_fields(change)?
        ));
    }
    json.push(']');
    Ok(json)
}

pub fn tuple_object(
    relation: &Relation,
    values: &[Cell],
    omit_unchanged: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    if values.len() != relation.columns.len() {
        return Err(format!("tuple width mismatch for relation {}", relation.id).into());
    }
    object_from_pairs(relation.columns.iter().zip(values), omit_unchanged)
}

pub fn identity_object(
    relation: &Relation,
    identity: &IdentityTuple,
) -> Result<String, Box<dyn std::error::Error>> {
    let key_columns: Vec<_> = relation
        .columns
        .iter()
        .filter(|column| column.flags & 1 != 0)
        .collect();
    match identity.kind {
        IdentityKind::Key if identity.values.len() == key_columns.len() => {
            object_from_pairs(key_columns.into_iter().zip(&identity.values), true)
        }
        IdentityKind::Key if identity.values.len() == relation.columns.len() => object_from_pairs(
            relation
                .columns
                .iter()
                .zip(&identity.values)
                .filter(|(column, _)| column.flags & 1 != 0),
            true,
        ),
        IdentityKind::Full if identity.values.len() == relation.columns.len() => {
            object_from_pairs(relation.columns.iter().zip(&identity.values), true)
        }
        _ => Err(format!("identity tuple width mismatch for relation {}", relation.id).into()),
    }
}

pub fn key_object(
    relation: &Relation,
    values: &[Cell],
) -> Result<String, Box<dyn std::error::Error>> {
    if values.len() != relation.columns.len() {
        return Err(format!("tuple width mismatch for relation {}", relation.id).into());
    }
    object_from_pairs(
        relation
            .columns
            .iter()
            .zip(values)
            .filter(|(column, _)| column.flags & 1 != 0),
        true,
    )
}

pub fn object_from_pairs<'a>(
    pairs: impl Iterator<Item = (&'a crate::events::Column, &'a Cell)>,
    omit_unchanged: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut json = String::from("{");
    let mut count = 0;
    for (column, cell) in pairs {
        if omit_unchanged && matches!(cell, Cell::UnchangedToast) {
            continue;
        }
        if count != 0 {
            json.push(',');
        }
        json.push_str(&json_string(&column.name));
        json.push(':');
        push_cell_json(&mut json, cell)?;
        count += 1;
    }
    if count == 0 {
        return Err("tuple contains no usable identity or values".into());
    }
    json.push('}');
    Ok(json)
}

pub fn push_cell_json(json: &mut String, cell: &Cell) -> Result<(), Box<dyn std::error::Error>> {
    match cell {
        Cell::Null => json.push_str("null"),
        Cell::UnchangedToast => json.push_str("{\"unchanged_toast\":true}"),
        Cell::Value {
            format: TupleFormat::Text,
            bytes,
        } => json.push_str(&json_string(std::str::from_utf8(bytes)?)),
        Cell::Value {
            format: TupleFormat::Binary,
            bytes,
        } => {
            json.push_str("{\"binary_hex\":\"");
            for byte in bytes {
                json.push_str(&format!("{byte:02x}"));
            }
            json.push_str("\"}");
        }
    }
    Ok(())
}

pub fn json_string(value: &str) -> String {
    let mut output = String::from("\"");
    for character in value.chars() {
        match character {
            '\"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character < ' ' => {
                output.push_str(&format!("\\u{:04x}", character as u32))
            }
            character => output.push(character),
        }
    }
    output.push('\"');
    output
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

pub fn parse_lsn(value: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let (high, low) = value.split_once('/').ok_or("invalid LSN")?;
    Ok((u64::from_str_radix(high, 16)? << 32) | u64::from_str_radix(low, 16)?)
}

pub fn single_value<'a>(
    rows: &'a [Vec<Option<String>>],
    operation: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    rows.first()
        .and_then(|row| row.first())
        .and_then(Option::as_deref)
        .ok_or_else(|| format!("{operation} returned no value").into())
}
