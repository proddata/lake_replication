//! Canonical, bounded schema definitions retained at the source DDL boundary.
//! These codecs do not enable schema controls in the current importer.
use std::collections::BTreeSet;

pub const MAX_COLUMNS: usize = 1600;
pub const MAX_DEFINITION_BYTES: usize = 1024 * 1024;
pub const MAX_SCHEMA_VERSION: u32 = 33;
const MAX_CHANGE_BYTES: usize = 2 * MAX_DEFINITION_BYTES + 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub &'static str);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
fn require(value: bool, message: &'static str) -> Result<()> {
    if value { Ok(()) } else { Err(Error(message)) }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub id: u32,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub key: bool,
    pub nullable: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Definition {
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    AddNullable { column_id: u32 },
    Rename { column_id: u32 },
}
impl Operation {
    fn tag(self) -> u8 {
        match self {
            Self::AddNullable { .. } => 0,
            Self::Rename { .. } => 1,
        }
    }
}
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && !name.contains('\0')
}
fn blob(output: &mut Vec<u8>, value: &[u8]) {
    output.extend((value.len() as u32).to_be_bytes());
    output.extend(value);
}

impl Definition {
    pub fn json(&self) -> Result<String> {
        use crate::staging::json_string;
        self.validate()?;
        let columns = self.columns.iter().map(|column| format!(
            "{{\"id\":{},\"name\":{},\"type_oid\":{},\"type_modifier\":{},\"key\":{},\"nullable\":{}}}",
            column.id,json_string(&column.name),column.type_oid,column.type_modifier,column.key,column.nullable
        )).collect::<Vec<_>>().join(",");
        Ok(format!(
            "{{\"namespace\":{},\"name\":{},\"replica_identity\":{},\"columns\":[{columns}]}}",
            json_string(&self.namespace),
            json_string(&self.name),
            json_string(&(self.replica_identity as char).to_string())
        ))
    }

    fn validate_row(&self, row: &crate::push_protocol::Row, key: bool, update: bool) -> Result<()> {
        use crate::push_protocol::Value;
        let columns = self.columns.iter().filter(|column| !key || column.key);
        require(
            row.len() == columns.clone().count(),
            "row width differs from retained schema",
        )?;
        for ((id, value), column) in row.iter().zip(columns) {
            require(
                *id == column.id,
                "row column identity differs from retained schema",
            )?;
            let valid = match value {
                Value::Null => column.nullable && !column.key,
                Value::Unchanged => update,
                Value::Absent => false,
                Value::Text(value) => {
                    !value.contains('\0')
                        && match column.type_oid {
                            16 => matches!(value.as_str(), "t" | "f" | "true" | "false"),
                            20 => value.parse::<i64>().is_ok(),
                            23 => value.parse::<i32>().is_ok(),
                            25 => true,
                            _ => false,
                        }
                }
            };
            require(valid, "row value differs from retained schema")?;
        }
        Ok(())
    }

    pub fn relation(&self, remote_id: u32) -> Result<crate::events::Relation> {
        self.validate()?;
        require(remote_id > 0, "invalid remote relation identity")?;
        Ok(crate::events::Relation {
            id: remote_id,
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            replica_identity: self.replica_identity,
            columns: self
                .columns
                .iter()
                .map(|column| crate::events::Column {
                    flags: u8::from(column.key),
                    name: column.name.clone(),
                    type_oid: column.type_oid,
                    type_modifier: column.type_modifier,
                })
                .collect(),
        })
    }

    pub fn validate(&self) -> Result<()> {
        require(
            valid_name(&self.namespace) && valid_name(&self.name),
            "invalid schema identifier",
        )?;
        require(
            matches!(self.replica_identity, b'd' | b'i'),
            "unsupported schema replica identity",
        )?;
        require(
            !self.columns.is_empty() && self.columns.len() <= MAX_COLUMNS,
            "schema column count exceeded",
        )?;
        let mut names = BTreeSet::new();
        let mut key = false;
        for (index, column) in self.columns.iter().enumerate() {
            require(
                column.id as usize == index + 1,
                "schema column identities must be contiguous and stable",
            )?;
            require(
                valid_name(&column.name) && names.insert(&column.name),
                "invalid or duplicate schema column name",
            )?;
            require(
                matches!(column.type_oid, 16 | 20 | 23 | 25) && column.type_modifier == -1,
                "unsupported schema scalar",
            )?;
            require(
                !column.key || !column.nullable,
                "schema key must be non-null",
            )?;
            key |= column.key;
        }
        require(key, "schema requires a primary-key identity")
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let mut bytes = b"LKDSC1".to_vec();
        blob(&mut bytes, self.namespace.as_bytes());
        blob(&mut bytes, self.name.as_bytes());
        bytes.push(self.replica_identity);
        bytes.extend((self.columns.len() as u32).to_be_bytes());
        for c in &self.columns {
            bytes.extend(c.id.to_be_bytes());
            blob(&mut bytes, c.name.as_bytes());
            bytes.extend(c.type_oid.to_be_bytes());
            bytes.extend(c.type_modifier.to_be_bytes());
            bytes.push(u8::from(c.key));
            bytes.push(u8::from(c.nullable));
        }
        require(
            bytes.len() <= MAX_DEFINITION_BYTES,
            "schema definition byte limit exceeded",
        )?;
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        require(
            bytes.len() <= MAX_DEFINITION_BYTES,
            "schema definition byte limit exceeded",
        )?;
        let mut reader = Reader(bytes);
        require(
            reader.take(6)? == b"LKDSC1",
            "unsupported schema definition version",
        )?;
        let namespace = reader.name()?;
        let name = reader.name()?;
        let replica_identity = reader.byte()?;
        let count = reader.u32()? as usize;
        require(
            (1..=MAX_COLUMNS).contains(&count),
            "schema column count exceeded",
        )?;
        let mut columns = Vec::with_capacity(count);
        for _ in 0..count {
            columns.push(Column {
                id: reader.u32()?,
                name: reader.name()?,
                type_oid: reader.u32()?,
                type_modifier: i32::from_be_bytes(reader.take(4)?.try_into().unwrap()),
                key: reader.boolean()?,
                nullable: reader.boolean()?,
            });
        }
        require(reader.0.is_empty(), "trailing schema definition bytes")?;
        let result = Self {
            namespace,
            name,
            replica_identity,
            columns,
        };
        result.validate()?;
        Ok(result)
    }
    pub fn transition(&self, after: &Self) -> Result<Operation> {
        self.validate()?;
        after.validate()?;
        require(
            self.namespace == after.namespace
                && self.name == after.name
                && self.replica_identity == after.replica_identity,
            "table identity or replica identity changed",
        )?;
        if after.columns.len() == self.columns.len() + 1
            && after.columns[..self.columns.len()] == self.columns
        {
            let added = after.columns.last().unwrap();
            require(
                !added.key && added.nullable,
                "new schema column must be nullable and non-key",
            )?;
            return Ok(Operation::AddNullable {
                column_id: added.id,
            });
        }
        require(
            self.columns.len() == after.columns.len(),
            "unsupported schema column lifecycle",
        )?;
        let mut renamed = None;
        for (old, new) in self.columns.iter().zip(&after.columns) {
            if old == new {
                continue;
            }
            require(
                renamed.is_none()
                    && old.id == new.id
                    && old.type_oid == new.type_oid
                    && old.type_modifier == new.type_modifier
                    && old.key == new.key
                    && old.nullable == new.nullable
                    && old.name != new.name,
                "unsupported combined schema change",
            )?;
            renamed = Some(new.id);
        }
        renamed
            .map(|column_id| Operation::Rename { column_id })
            .ok_or(Error("schema change is a no-op"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub table_id: [u8; 16],
    pub before_version: u32,
    pub after_version: u32,
    pub before: Definition,
    pub after: Definition,
}
impl Change {
    pub fn operation(&self) -> Result<Operation> {
        require(
            self.table_id != [0; 16]
                && self.before_version > 0
                && self.before_version.checked_add(1) == Some(self.after_version)
                && self.after_version <= MAX_SCHEMA_VERSION,
            "invalid schema identity or version transition",
        )?;
        self.before.transition(&self.after)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        let operation = self.operation()?;
        let mut bytes = b"LKDDL1".to_vec();
        bytes.extend(self.table_id);
        bytes.extend(self.before_version.to_be_bytes());
        bytes.extend(self.after_version.to_be_bytes());
        bytes.push(operation.tag());
        blob(&mut bytes, &self.before.encode()?);
        blob(&mut bytes, &self.after.encode()?);
        require(
            bytes.len() <= MAX_CHANGE_BYTES,
            "schema change byte limit exceeded",
        )?;
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        require(
            bytes.len() <= MAX_CHANGE_BYTES,
            "schema change byte limit exceeded",
        )?;
        let mut reader = Reader(bytes);
        require(
            reader.take(6)? == b"LKDDL1",
            "unsupported schema change version",
        )?;
        let table_id = reader.take(16)?.try_into().unwrap();
        let before_version = reader.u32()?;
        let after_version = reader.u32()?;
        let operation = reader.byte()?;
        let before = Definition::decode(reader.blob(MAX_DEFINITION_BYTES)?)?;
        let after = Definition::decode(reader.blob(MAX_DEFINITION_BYTES)?)?;
        require(reader.0.is_empty(), "trailing schema change bytes")?;
        let result = Self {
            table_id,
            before_version,
            after_version,
            before,
            after,
        };
        require(
            result.operation()?.tag() == operation,
            "schema operation disagrees with its definitions",
        )?;
        Ok(result)
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        require(n <= self.0.len(), "truncated schema data")?;
        let (value, remaining) = self.0.split_at(n);
        self.0 = remaining;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn blob(&mut self, limit: usize) -> Result<&'a [u8]> {
        let length = self.u32()? as usize;
        require(length <= limit, "schema field byte limit exceeded")?;
        self.take(length)
    }
    fn name(&mut self) -> Result<String> {
        let bytes = self.blob(63)?;
        let name =
            std::str::from_utf8(bytes).map_err(|_| Error("schema identifier is not UTF8"))?;
        require(valid_name(name), "invalid schema identifier")?;
        Ok(name.to_owned())
    }
    fn boolean(&mut self) -> Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error("invalid schema boolean")),
        }
    }
}

/// Retained definitions at one complete source transaction boundary. This is
/// separate from the latest catalog and from destination publication progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State(std::collections::BTreeMap<[u8; 16], (u32, Definition)>);

impl State {
    pub fn new(tables: impl IntoIterator<Item = ([u8; 16], u32, Definition)>) -> Result<Self> {
        let mut state = std::collections::BTreeMap::new();
        for (id, version, definition) in tables {
            require(
                id != [0; 16] && (1..=MAX_SCHEMA_VERSION).contains(&version),
                "invalid retained schema identity or version",
            )?;
            definition.validate()?;
            require(
                state.insert(id, (version, definition)).is_none(),
                "duplicate retained schema table",
            )?;
        }
        require(
            state.len() == 2,
            "schema state requires two registered tables",
        )?;
        Ok(Self(state))
    }

    pub fn definition(&self, id: &[u8; 16]) -> Option<&(u32, Definition)> {
        self.0.get(id)
    }

    /// Validate every event at its own ordinal, returning the post-commit state
    /// only after the entire batch succeeds. The original remains replayable
    /// after malformed controls or a late old-version row.
    pub fn after_batch(&self, batch: &crate::push_protocol::Batch) -> Result<Self> {
        use crate::push_protocol::{Change as EventChange, Control};
        batch
            .encode()
            .map_err(|_| Error("invalid canonical schema batch"))?;
        let mut next = self.clone();
        for event in &batch.events {
            match &event.change {
                EventChange::Control(kind, payload) => {
                    require(
                        event.table.is_none(),
                        "schema/control event must be tableless",
                    )?;
                    if *kind != Control::Schema {
                        continue;
                    }
                    let change = Change::decode(payload)?;
                    let current = next
                        .0
                        .get_mut(&change.table_id)
                        .ok_or(Error("unregistered schema event table"))?;
                    require(
                        current.0 == change.before_version && current.1 == change.before,
                        "schema event does not follow retained history",
                    )?;
                    *current = (change.after_version, change.after);
                }
                _ => {
                    let table = event
                        .table
                        .ok_or(Error("row event has no schema identity"))?;
                    let current = next
                        .0
                        .get(&table.id)
                        .ok_or(Error("unregistered row event table"))?;
                    require(
                        current.0 == table.schema_version,
                        "row schema version is out of source order",
                    )?;
                    let old_row = |old: &crate::push_protocol::OldRow| match old {
                        crate::push_protocol::OldRow::Key(row) => {
                            current.1.validate_row(row, true, false)
                        }
                        crate::push_protocol::OldRow::Full(row) => {
                            current.1.validate_row(row, false, false)
                        }
                    };
                    match &event.change {
                        EventChange::Insert(row) => current.1.validate_row(row, false, false)?,
                        EventChange::Update { old, new } => {
                            if let Some(old) = old {
                                old_row(old)?;
                            }
                            current.1.validate_row(new, false, true)?;
                        }
                        EventChange::Delete(old) => old_row(old)?,
                        _ => (),
                    }
                }
            }
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn definition() -> Definition {
        Definition {
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    id: 1,
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    key: true,
                    nullable: false,
                },
                Column {
                    id: 2,
                    name: "note".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    key: false,
                    nullable: true,
                },
            ],
        }
    }
    fn change() -> Change {
        let before = definition();
        let mut after = before.clone();
        after.columns.push(Column {
            id: 3,
            name: "extra".into(),
            type_oid: 16,
            type_modifier: -1,
            key: false,
            nullable: true,
        });
        Change {
            table_id: [7; 16],
            before_version: 1,
            after_version: 2,
            before,
            after,
        }
    }
    #[test]
    fn canonical_descriptor_matches_the_native_format() {
        let value = definition();
        let bytes = value.encode().unwrap();
        assert_eq!(
            crate::push_import::hex(&bytes),
            "4c4b44534331000000067075626c6963000000056974656d7364000000020000000100000002696400000014ffffffff010000000002000000046e6f746500000019ffffffff0001"
        );
        assert_eq!(Definition::decode(&bytes).unwrap(), value);
        for length in 0..bytes.len() {
            assert!(Definition::decode(&bytes[..length]).is_err());
        }
        let mut invalid = bytes.clone();
        invalid[26..30].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(Definition::decode(&invalid).is_err());
        let mut invalid = bytes.clone();
        invalid[6..10].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(Definition::decode(&invalid).is_err());
        let mut invalid = bytes.clone();
        invalid[48] = 2;
        assert!(Definition::decode(&invalid).is_err());
        let mut invalid = bytes;
        invalid.push(0);
        assert!(Definition::decode(&invalid).is_err());
    }
    #[test]
    fn only_single_safe_additions_and_renames_are_representable() {
        let value = change();
        assert_eq!(
            value.operation().unwrap(),
            Operation::AddNullable { column_id: 3 }
        );
        assert_eq!(Change::decode(&value.encode().unwrap()).unwrap(), value);
        let mut renamed = definition();
        renamed.columns[0].name = "key 世界".into();
        assert_eq!(
            definition().transition(&renamed).unwrap(),
            Operation::Rename { column_id: 1 }
        );
        let mut rejected = vec![definition()];
        let mut altered = definition();
        altered.columns[1].type_oid = 23;
        rejected.push(altered);
        let mut altered = definition();
        altered.columns.pop();
        rejected.push(altered);
        let mut altered = value.after.clone();
        altered.columns[2].nullable = false;
        rejected.push(altered);
        let mut altered = renamed;
        altered.columns[1].name = "also renamed".into();
        rejected.push(altered);
        let mut altered = definition();
        altered.namespace = "other".into();
        rejected.push(altered);
        for after in rejected {
            assert!(definition().transition(&after).is_err());
        }
    }
    #[test]
    fn change_payloads_cannot_hide_truncations_or_different_operations() {
        let value = change();
        let bytes = value.encode().unwrap();
        for length in 0..bytes.len() {
            assert!(Change::decode(&bytes[..length]).is_err());
        }
        let mut invalid = bytes.clone();
        invalid[30] = 1;
        assert!(Change::decode(&invalid).is_err());
        let mut invalid = bytes;
        invalid.push(0);
        assert!(Change::decode(&invalid).is_err());
        let mut invalid = value.clone();
        invalid.after_version = 3;
        assert!(invalid.encode().is_err());
        let mut invalid = value;
        invalid.table_id = [0; 16];
        assert!(invalid.encode().is_err());
    }
    #[test]
    fn descriptor_names_and_column_identities_are_bounded() {
        for name in [String::new(), "a".repeat(64), "nul\0name".into()] {
            let mut invalid = definition();
            invalid.columns[1].name = name;
            assert!(invalid.encode().is_err());
        }
        let mut invalid = definition();
        invalid.columns[1].name = "id".into();
        assert!(invalid.encode().is_err());
        let mut invalid = definition();
        invalid.columns[1].id = 3;
        assert!(invalid.encode().is_err());
        let mut invalid = definition();
        invalid.columns[0].nullable = true;
        assert!(invalid.encode().is_err());
        let mut invalid = definition();
        invalid.columns[1].type_oid = 1700;
        assert!(invalid.encode().is_err());
    }
    fn schema_batch(events: Vec<crate::push_protocol::Event>) -> crate::push_protocol::Batch {
        use crate::push_protocol::{Batch, Stream, Transaction};
        Batch::new(
            Stream {
                id: [1; 16],
                system_identifier: 42,
                database_oid: 123,
                generation: 1,
            },
            1,
            Transaction {
                xid: 99,
                begin_lsn: 10,
                commit_lsn: 20,
                end_lsn: 30,
                commit_time_micros: 0,
            },
            events,
        )
        .unwrap()
    }
    fn row_event(ordinal: u32, version: u32) -> crate::push_protocol::Event {
        use crate::push_protocol::{Change as EventChange, Event, Table};
        Event {
            ordinal,
            table: Some(Table {
                id: [7; 16],
                schema_version: version,
            }),
            change: EventChange::Truncate(0),
        }
    }
    fn schema_event(ordinal: u32, change: &Change) -> crate::push_protocol::Event {
        use crate::push_protocol::{Change as EventChange, Control, Event};
        Event {
            ordinal,
            table: None,
            change: EventChange::Control(Control::Schema, change.encode().unwrap()),
        }
    }
    #[test]
    fn retained_history_follows_multiple_ddl_and_dml_ordinals_atomically() {
        let added = change();
        let mut renamed = added.clone();
        renamed.before_version = 2;
        renamed.after_version = 3;
        renamed.before = added.after.clone();
        renamed.after.columns[1].name = "renamed".into();
        let state = State::new([([7; 16], 1, definition()), ([8; 16], 1, definition())]).unwrap();
        let events = vec![
            row_event(0, 1),
            schema_event(1, &added),
            row_event(2, 2),
            schema_event(3, &renamed),
            row_event(4, 3),
        ];
        let batch = schema_batch(events.clone());
        let next = state.after_batch(&batch).unwrap();
        assert_eq!(next.definition(&[7; 16]), Some(&(3, renamed.after.clone())));
        assert_eq!(next.definition(&[8; 16]), state.definition(&[8; 16]));
        assert_eq!(state.definition(&[7; 16]), Some(&(1, definition())));
        assert_eq!(state.after_batch(&batch).unwrap(), next);
        let mut broken = events;
        broken[4] = row_event(4, 1);
        assert!(state.after_batch(&schema_batch(broken)).is_err());
        assert_eq!(state.after_batch(&batch).unwrap(), next);
        assert!(next.after_batch(&batch).is_err());
    }
    #[test]
    fn ddl_only_history_rejects_stale_definitions_and_unregistered_identities() {
        let state = State::new([([7; 16], 1, definition()), ([8; 16], 1, definition())]).unwrap();
        let added = change();
        let next = state
            .after_batch(&schema_batch(vec![schema_event(0, &added)]))
            .unwrap();
        assert_eq!(next.definition(&[7; 16]), Some(&(2, added.after.clone())));
        let mut unknown = added.clone();
        unknown.table_id = [9; 16];
        assert!(
            state
                .after_batch(&schema_batch(vec![schema_event(0, &unknown)]))
                .is_err()
        );
        let mut divergent = added;
        divergent.before.columns[1].name = "different".into();
        divergent.after.columns[1].name = "different".into();
        assert!(
            state
                .after_batch(&schema_batch(vec![schema_event(0, &divergent)]))
                .is_err()
        );
        assert!(
            state
                .after_batch(&schema_batch(vec![row_event(0, 2)]))
                .is_err()
        );
    }
    #[test]
    fn ordered_normalization_keeps_each_rows_version_and_schema_barrier() {
        use crate::push_protocol::{
            Change as EventChange, Control, Event, OrderedAction, Table, Value, normalize_batch,
            normalize_ordered,
        };
        use std::collections::BTreeMap;
        let change = change();
        let state = State::new([([7; 16], 1, definition()), ([8; 16], 1, definition())]).unwrap();
        let first = Table {
            id: [7; 16],
            schema_version: 1,
        };
        let second = Table {
            schema_version: 2,
            ..first
        };
        let registry = BTreeMap::from([
            (first, (change.before.relation(42).unwrap(), 11)),
            (second, (change.after.relation(42).unwrap(), 12)),
        ]);
        let before = Event {
            ordinal: 0,
            table: Some(first),
            change: EventChange::Insert(vec![
                (1, Value::Text("1".into())),
                (2, Value::Text("before".into())),
            ]),
        };
        let after = Event {
            ordinal: 3,
            table: Some(second),
            change: EventChange::Insert(vec![
                (1, Value::Text("2".into())),
                (2, Value::Text("after".into())),
                (3, Value::Text("t".into())),
            ]),
        };
        let batch = schema_batch(vec![
            before,
            schema_event(1, &change),
            Event {
                ordinal: 2,
                table: None,
                change: EventChange::Control(Control::Progress, vec![9; 16]),
            },
            after,
        ]);
        let (actions, next) = normalize_ordered(&batch, &registry, &state).unwrap();
        assert_eq!(actions.len(), 4);
        for (index, expected_version, expected_columns) in [(0, 11, 2), (3, 12, 3)] {
            let OrderedAction::Row {
                ordinal,
                change: row,
                local_version,
            } = &actions[index]
            else {
                panic!("missing row");
            };
            assert_eq!(*ordinal, index as u32);
            assert_eq!(*local_version, expected_version);
            assert_eq!(row.relation.columns.len(), expected_columns);
            let json = crate::staging::changes_json_slice(
                std::slice::from_ref(row),
                &std::collections::HashMap::from([(42, *local_version)]),
            )
            .unwrap();
            assert!(json.contains(&format!("\"relation_schema_version\":{expected_version}")));
        }
        assert!(
            matches!(&actions[1],OrderedAction::Schema { ordinal: 1,change: captured } if captured == &change)
        );
        assert!(
            matches!(&actions[2],OrderedAction::Progress { ordinal: 2,request } if request == &[9;16])
        );
        assert_eq!(next.definition(&[7; 16]), Some(&(2, change.after.clone())));
        assert!(normalize_batch(&batch, &registry).is_err());
        let mut divergent = registry.clone();
        divergent.get_mut(&second).unwrap().0.columns[2].name = "wrong".into();
        assert!(normalize_ordered(&batch, &divergent, &state).is_err());
        let mut broken = batch.events.clone();
        broken[3].change = EventChange::Insert(vec![
            (1, Value::Text("2".into())),
            (2, Value::Text("after".into())),
            (3, Value::Text("invalid bool".into())),
        ]);
        assert!(normalize_ordered(&schema_batch(broken), &registry, &state).is_err());
        assert_eq!(
            normalize_ordered(&batch, &registry, &state).unwrap().1,
            next
        );
        let (only_ddl, _) = normalize_ordered(
            &schema_batch(vec![schema_event(0, &change)]),
            &registry,
            &state,
        )
        .unwrap();
        assert!(matches!(
            only_ddl.as_slice(),
            [OrderedAction::Schema { ordinal: 0, .. }]
        ));
    }
}
