//! Bounded canonical source-push protocol. See docs/push-protocol.md.
use crate::events::{
    Cell, IdentityKind, IdentityTuple, Operation, Relation, StagedChange, StagedTransaction,
    TupleFormat,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

pub type Id = [u8; 16];
pub type Hash = [u8; 32];
pub const MAX_BATCH: usize = 64 * 1024 * 1024;
pub const MAX_EVENT: usize = 8 * 1024 * 1024;
pub const MAX_EVENTS: usize = 100_000;
pub const MAX_TABLE_VERSIONS: usize = 66;
pub const MAX_HEADER_BYTES: usize = 4096;
const MAGIC: &[u8; 6] = b"LKPUSH";
const VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    pub id: Id,
    pub system_identifier: u64,
    pub database_oid: u32,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Table {
    pub id: Id,
    pub schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub xid: u32,
    pub begin_lsn: u64,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_time_micros: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Absent,
    Null,
    Unchanged,
    Text(String),
}
pub type Row = Vec<(u32, Value)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OldRow {
    Key(Row),
    Full(Row),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    SnapshotBegin,
    SnapshotChunk,
    SnapshotComplete,
    SnapshotActivate,
    Schema,
    Progress,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Insert(Row),
    Update { old: Option<OldRow>, new: Row },
    Delete(OldRow),
    Truncate(u8),
    Control(Control, Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub ordinal: u32,
    pub table: Option<Table>,
    pub change: Change,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub stream: Stream,
    pub sequence: u64,
    pub batch_id: Id,
    pub transaction: Transaction,
    pub event_count: u32,
    pub tables: Vec<(Table, u32)>,
    pub digest: Hash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    pub manifest: Manifest,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub &'static str);
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
fn require(ok: bool, message: &'static str) -> Result<()> {
    if ok { Ok(()) } else { Err(Error(message)) }
}

fn stream_bytes(stream: &Stream, output: &mut Vec<u8>) {
    output.extend(stream.id);
    output.extend(stream.system_identifier.to_be_bytes());
    output.extend(stream.database_oid.to_be_bytes());
    output.extend(stream.generation.to_be_bytes());
}
fn table_bytes(table: Table, output: &mut Vec<u8>) {
    output.extend(table.id);
    output.extend(table.schema_version.to_be_bytes());
}
fn batch_id(stream: &Stream, end_lsn: u64) -> Id {
    let mut bytes = b"lake-push-batch-v1".to_vec();
    stream_bytes(stream, &mut bytes);
    bytes.extend(end_lsn.to_be_bytes());
    Sha256::digest(bytes)[..16].try_into().unwrap()
}

fn row_bytes(row: &Row, output: &mut Vec<u8>) -> Result<()> {
    require(row.len() <= 1600, "row field limit exceeded")?;
    output.extend((row.len() as u32).to_be_bytes());
    let mut previous = 0;
    for (id, value) in row {
        require(*id > previous, "column IDs must be positive and sorted")?;
        previous = *id;
        output.extend(id.to_be_bytes());
        match value {
            Value::Absent => output.push(0),
            Value::Null => output.push(1),
            Value::Unchanged => output.push(2),
            Value::Text(value) => {
                output.push(3);
                blob(value.as_bytes(), output)?;
            }
        }
        require(output.len() <= MAX_EVENT, "event size limit exceeded")?;
    }
    Ok(())
}
fn blob(value: &[u8], output: &mut Vec<u8>) -> Result<()> {
    require(value.len() <= MAX_EVENT, "value size limit exceeded")?;
    output.extend((value.len() as u32).to_be_bytes());
    output.extend(value);
    Ok(())
}
fn old_bytes(old: Option<&OldRow>, output: &mut Vec<u8>) -> Result<()> {
    match old {
        None => output.push(0),
        Some(OldRow::Key(row)) => {
            output.push(1);
            row_bytes(row, output)?;
        }
        Some(OldRow::Full(row)) => {
            output.push(2);
            row_bytes(row, output)?;
        }
    }
    Ok(())
}
fn event_bytes(event: &Event) -> Result<Vec<u8>> {
    let mut bytes = event.ordinal.to_be_bytes().to_vec();
    match event.table {
        None => bytes.push(0),
        Some(table) => {
            bytes.push(1);
            table_bytes(table, &mut bytes);
        }
    }
    require(
        event.table.is_some() || matches!(event.change, Change::Control(..)),
        "row event requires a table",
    )?;
    match &event.change {
        Change::Insert(row) => {
            bytes.push(0);
            row_bytes(row, &mut bytes)?;
        }
        Change::Update { old, new } => {
            bytes.push(1);
            old_bytes(old.as_ref(), &mut bytes)?;
            row_bytes(new, &mut bytes)?;
        }
        Change::Delete(old) => {
            bytes.push(2);
            old_bytes(Some(old), &mut bytes)?;
        }
        Change::Truncate(options) => {
            bytes.push(3);
            bytes.push(*options);
        }
        Change::Control(kind, payload) => {
            bytes.push(match kind {
                Control::SnapshotBegin => 4,
                Control::SnapshotChunk => 5,
                Control::SnapshotComplete => 6,
                Control::SnapshotActivate => 7,
                Control::Schema => 8,
                Control::Progress => 9,
            });
            blob(payload, &mut bytes)?;
        }
    }
    require(bytes.len() <= MAX_EVENT, "event size limit exceeded")?;
    Ok(bytes)
}

fn header(manifest: &Manifest) -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(VERSION.to_be_bytes());
    stream_bytes(&manifest.stream, &mut bytes);
    bytes.extend(manifest.sequence.to_be_bytes());
    bytes.extend(manifest.batch_id);
    let tx = &manifest.transaction;
    bytes.extend(tx.xid.to_be_bytes());
    bytes.extend(tx.begin_lsn.to_be_bytes());
    bytes.extend(tx.commit_lsn.to_be_bytes());
    bytes.extend(tx.end_lsn.to_be_bytes());
    bytes.extend(tx.commit_time_micros.to_be_bytes());
    bytes.extend(manifest.event_count.to_be_bytes());
    bytes.extend((manifest.tables.len() as u32).to_be_bytes());
    for (table, count) in &manifest.tables {
        table_bytes(*table, &mut bytes);
        bytes.extend(count.to_be_bytes());
    }
    bytes
}

impl Batch {
    /// Native output plugin envelope: complete committed transaction metadata
    /// followed by canonical event frames. No transport chunks are committed.
    pub fn from_capture(
        stream: Stream,
        sequence: u64,
        tables: [Table; 2],
        raw: &[u8],
    ) -> Result<Self> {
        require(raw.len() <= MAX_BATCH, "capture byte budget exceeded")?;
        let mut reader = Reader(raw);
        require(reader.take(6)? == b"LKCAP1", "unknown capture envelope")?;
        let transaction = Transaction {
            xid: reader.u32()?,
            begin_lsn: reader.u64()?,
            commit_lsn: reader.u64()?,
            end_lsn: reader.u64()?,
            commit_time_micros: reader.u64()? as i64,
        };
        let event_count = reader.u32()?;
        require(
            event_count as usize <= MAX_EVENTS,
            "event count limit exceeded",
        )?;
        require(
            tables[0].id != [0; 16] && tables[1].id != [0; 16] && tables[0].id != tables[1].id,
            "capture requires two distinct table identities",
        )?;
        let mut native_counts = BTreeMap::new();
        for table in tables {
            let count = reader.u32()?;
            if count > 0 {
                native_counts.insert(table.id, count);
            }
        }
        let event_bytes = reader.0;
        let mut counts = BTreeMap::<Table, u32>::new();
        let mut actual_counts = BTreeMap::<Id, u32>::new();
        for _ in 0..event_count {
            let mut event = Reader(reader.blob()?);
            event.u32()?;
            match event.byte()? {
                0 => (),
                1 => {
                    let table = event.table()?;
                    require(
                        tables.iter().any(|expected| expected.id == table.id),
                        "capture event has an unregistered table",
                    )?;
                    *counts.entry(table).or_default() += 1;
                    *actual_counts.entry(table.id).or_default() += 1;
                    require(
                        counts.len() <= MAX_TABLE_VERSIONS,
                        "table version count limit exceeded",
                    )?;
                }
                _ => return Err(Error("invalid table presence")),
            }
        }
        require(reader.0.is_empty(), "trailing capture bytes")?;
        require(
            native_counts == actual_counts,
            "native capture table count mismatch",
        )?;
        let manifest = Manifest {
            batch_id: batch_id(&stream, transaction.end_lsn),
            stream,
            sequence,
            transaction,
            event_count,
            tables: counts.into_iter().collect(),
            digest: [0; 32],
        };
        let mut bytes = header(&manifest);
        bytes.extend(event_bytes);
        let digest = Sha256::digest(&bytes);
        bytes.extend(digest);
        Self::decode(&bytes)
    }

    pub fn log_parts(&self) -> Result<(Vec<u8>, Vec<(Option<Table>, u32, Vec<u8>)>)> {
        self.encode()?;
        let mut events = Vec::new();
        for event in &self.events {
            events.push((event.table, event.ordinal, event_bytes(event)?));
        }
        Ok((header(&self.manifest), events))
    }

    pub fn new(
        stream: Stream,
        sequence: u64,
        transaction: Transaction,
        events: Vec<Event>,
    ) -> Result<Self> {
        let mut tables = BTreeMap::<Table, u32>::new();
        require(events.len() <= MAX_EVENTS, "event count limit exceeded")?;
        for event in &events {
            if let Some(table) = event.table {
                *tables.entry(table).or_default() += 1;
            }
        }
        let manifest = Manifest {
            batch_id: batch_id(&stream, transaction.end_lsn),
            stream,
            sequence,
            transaction,
            event_count: events.len() as u32,
            tables: tables.into_iter().collect(),
            digest: [0; 32],
        };
        let mut batch = Self { manifest, events };
        let bytes = batch.canonical()?;
        batch.manifest.digest = Sha256::digest(bytes).into();
        Ok(batch)
    }

    fn canonical(&self) -> Result<Vec<u8>> {
        let m = &self.manifest;
        require(
            m.stream.id != [0; 16]
                && m.stream.system_identifier != 0
                && m.stream.database_oid != 0
                && m.stream.generation > 0
                && m.sequence > 0,
            "invalid stream identity",
        )?;
        require(
            m.transaction.begin_lsn <= m.transaction.commit_lsn
                && m.transaction.commit_lsn < m.transaction.end_lsn,
            "invalid source coordinates",
        )?;
        require(
            m.batch_id == batch_id(&m.stream, m.transaction.end_lsn),
            "invalid batch identity",
        )?;
        require(
            self.events.len() <= MAX_EVENTS && m.event_count as usize == self.events.len(),
            "event count mismatch",
        )?;
        require(
            m.tables.len() <= MAX_TABLE_VERSIONS,
            "table version count limit exceeded",
        )?;
        require(
            m.tables
                .iter()
                .map(|(table, _)| table.id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                <= 2,
            "table group limit exceeded",
        )?;
        let mut counts = BTreeMap::<Table, u32>::new();
        let mut bytes = header(m);
        for (index, event) in self.events.iter().enumerate() {
            require(event.ordinal as usize == index, "event ordinal gap")?;
            if let Some(table) = event.table {
                require(
                    table.id != [0; 16]
                        && (1..=crate::push_schema::MAX_SCHEMA_VERSION)
                            .contains(&table.schema_version),
                    "invalid table identity",
                )?;
                *counts.entry(table).or_default() += 1;
            }
            let encoded = event_bytes(event)?;
            bytes.extend((encoded.len() as u32).to_be_bytes());
            bytes.extend(encoded);
            require(bytes.len() + 32 <= MAX_BATCH, "batch size limit exceeded")?;
        }
        require(
            counts.into_iter().collect::<Vec<_>>() == m.tables,
            "table count mismatch",
        )?;
        Ok(bytes)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut bytes = self.canonical()?;
        require(
            Hash::from(Sha256::digest(&bytes)) == self.manifest.digest,
            "batch digest mismatch",
        )?;
        // Digest is a trailing trailer over exactly the preceding canonical bytes.
        bytes.extend(self.manifest.digest);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        require(
            bytes.len() <= MAX_BATCH && bytes.len() >= 32,
            "invalid batch size",
        )?;
        let (body, trailer) = bytes.split_at(bytes.len() - 32);
        require(
            Sha256::digest(body).as_slice() == trailer,
            "batch digest mismatch",
        )?;
        let mut r = Reader(body);
        require(r.take(6)? == MAGIC, "invalid protocol magic")?;
        require(r.u16()? == VERSION, "unsupported protocol version")?;
        let stream = Stream {
            id: r.id()?,
            system_identifier: r.u64()?,
            database_oid: r.u32()?,
            generation: r.u64()?,
        };
        let sequence = r.u64()?;
        let batch_id = r.id()?;
        let transaction = Transaction {
            xid: r.u32()?,
            begin_lsn: r.u64()?,
            commit_lsn: r.u64()?,
            end_lsn: r.u64()?,
            commit_time_micros: r.u64()? as i64,
        };
        let event_count = r.u32()?;
        require(
            event_count as usize <= MAX_EVENTS,
            "event count limit exceeded",
        )?;
        let table_count = r.u32()?;
        require(
            table_count as usize <= MAX_TABLE_VERSIONS,
            "table version count limit exceeded",
        )?;
        let mut tables = Vec::new();
        for _ in 0..table_count {
            tables.push((r.table()?, r.u32()?));
        }
        let mut events = Vec::new();
        for _ in 0..event_count {
            let event = r.blob()?;
            let mut er = Reader(event);
            let ordinal = er.u32()?;
            let table = match er.byte()? {
                0 => None,
                1 => Some(er.table()?),
                _ => return Err(Error("invalid table presence")),
            };
            let change = match er.byte()? {
                0 => Change::Insert(er.row()?),
                1 => Change::Update {
                    old: er.old()?,
                    new: er.row()?,
                },
                2 => Change::Delete(er.old()?.ok_or(Error("DELETE requires old identity"))?),
                3 => Change::Truncate(er.byte()?),
                tag @ 4..=9 => {
                    let kind = match tag {
                        4 => Control::SnapshotBegin,
                        5 => Control::SnapshotChunk,
                        6 => Control::SnapshotComplete,
                        7 => Control::SnapshotActivate,
                        8 => Control::Schema,
                        _ => Control::Progress,
                    };
                    Change::Control(kind, er.blob()?.to_vec())
                }
                _ => return Err(Error("unsupported event kind")),
            };
            require(er.0.is_empty(), "trailing event bytes")?;
            events.push(Event {
                ordinal,
                table,
                change,
            });
        }
        require(r.0.is_empty(), "trailing batch bytes")?;
        let batch = Self {
            manifest: Manifest {
                stream,
                sequence,
                batch_id,
                transaction,
                event_count,
                tables,
                digest: trailer.try_into().unwrap(),
            },
            events,
        };
        batch.canonical()?;
        Ok(batch)
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        require(length <= self.0.len(), "truncated protocol data")?;
        let (value, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn id(&mut self) -> Result<Id> {
        Ok(self.take(16)?.try_into().unwrap())
    }
    fn table(&mut self) -> Result<Table> {
        Ok(Table {
            id: self.id()?,
            schema_version: self.u32()?,
        })
    }
    fn blob(&mut self) -> Result<&'a [u8]> {
        let length = self.u32()? as usize;
        require(length <= MAX_EVENT, "event size limit exceeded")?;
        self.take(length)
    }
    fn row(&mut self) -> Result<Row> {
        let count = self.u32()?;
        require(count <= 1600, "row field limit exceeded")?;
        let mut row = Vec::new();
        for _ in 0..count {
            let id = self.u32()?;
            let value = match self.byte()? {
                0 => Value::Absent,
                1 => Value::Null,
                2 => Value::Unchanged,
                3 => Value::Text(
                    std::str::from_utf8(self.blob()?)
                        .map_err(|_| Error("invalid UTF-8 value"))?
                        .into(),
                ),
                _ => return Err(Error("unknown field value tag")),
            };
            row.push((id, value));
        }
        Ok(row)
    }
    fn old(&mut self) -> Result<Option<OldRow>> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(OldRow::Key(self.row()?))),
            2 => Ok(Some(OldRow::Full(self.row()?))),
            _ => Err(Error("invalid old identity tag")),
        }
    }
}

/// Convert supported INSERT events into the unchanged destination journal model.
/// Column IDs are one-based frozen-schema positions in this first prototype.
pub fn normalize_inserts(
    batch: &Batch,
    registry: &BTreeMap<Table, (Relation, i64)>,
) -> Result<(StagedTransaction, HashMap<u32, i64>)> {
    require(
        batch
            .events
            .iter()
            .all(|event| matches!(event.change, Change::Insert(_))),
        "snapshot requires INSERT events",
    )?;
    normalize_changes(batch, registry)
}

fn normalized_row(row: &Row, relation: &Relation, key: bool, update: bool) -> Result<Vec<Cell>> {
    let columns: Vec<_> = relation
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !key || column.flags & 1 != 0)
        .collect();
    require(row.len() == columns.len(), "row width mismatch")?;
    row.iter()
        .zip(columns)
        .map(|((id, value), (index, column))| {
            require(*id as usize == index + 1, "unregistered column ID")?;
            match value {
                Value::Null => {
                    require(column.flags & 1 == 0, "NULL primary key")?;
                    Ok(Cell::Null)
                }
                Value::Text(text) => {
                    require(!text.contains('\0'), "NUL in PostgreSQL text value")?;
                    require(
                        match column.type_oid {
                            16 => matches!(text.as_str(), "t" | "f" | "true" | "false"),
                            20 => text.parse::<i64>().is_ok(),
                            23 => text.parse::<i32>().is_ok(),
                            25 => true,
                            _ => false,
                        },
                        "invalid supported scalar value",
                    )?;
                    Ok(Cell::Value {
                        format: TupleFormat::Text,
                        bytes: text.as_bytes().to_vec(),
                    })
                }
                Value::Unchanged if update => Ok(Cell::UnchangedToast),
                _ => Err(Error("row requires present identity/INSERT values")),
            }
        })
        .collect()
}

fn normalized_old(old: &OldRow, relation: &Relation) -> Result<IdentityTuple> {
    let (row, key, kind) = match old {
        OldRow::Key(row) => (row, true, IdentityKind::Key),
        OldRow::Full(row) => (row, false, IdentityKind::Full),
    };
    Ok(IdentityTuple {
        kind,
        values: normalized_row(row, relation, key, false)?,
    })
}

fn normalize_row_event(
    event: &Event,
    registry: &BTreeMap<Table, (Relation, i64)>,
) -> Result<(StagedChange, i64)> {
    let table = event.table.ok_or(Error("unexpected control event"))?;
    let (relation, version) = registry
        .get(&table)
        .ok_or(Error("unregistered table or schema"))?;
    require(
        *version > 0 && relation.id > 0 && relation.columns.iter().any(|c| c.flags & 1 != 0),
        "invalid registered relation",
    )?;
    require(
        relation
            .columns
            .iter()
            .all(|c| matches!(c.type_oid, 16 | 20 | 23 | 25)),
        "unsupported scalar type",
    )?;
    let operation = match &event.change {
        Change::Insert(row) => Operation::Insert {
            new_values: normalized_row(row, relation, false, false)?,
        },
        Change::Update { old, new } => {
            let old_tuple = old
                .as_ref()
                .map(|old| normalized_old(old, relation))
                .transpose()?;
            let mut new_values = normalized_row(new, relation, false, true)?;
            let mut key_index = 0;
            for (index, column) in relation.columns.iter().enumerate() {
                if column.flags & 1 == 0 {
                    continue;
                }
                if matches!(new_values[index], Cell::UnchangedToast) {
                    let identity = old_tuple
                        .as_ref()
                        .ok_or(Error("unchanged UPDATE key requires historical identity"))?;
                    // Native WAL may leave the unchanged external key out
                    // of the new tuple. Its supplied old identity contains
                    // the exact historical value; no source-row read is needed.
                    new_values[index] = identity.values[match identity.kind {
                        IdentityKind::Key => key_index,
                        IdentityKind::Full => index,
                    }]
                    .clone();
                }
                key_index += 1;
            }
            Operation::Update {
                old_tuple,
                new_values,
            }
        }
        Change::Delete(old) => Operation::Delete {
            old_tuple: normalized_old(old, relation)?,
        },
        Change::Truncate(options) => {
            require(*options & !3 == 0, "unknown truncate option")?;
            Operation::Truncate { options: *options }
        }
        Change::Control(..) => return Err(Error("unsupported control event")),
    };
    Ok((
        StagedChange {
            relation_id: relation.id,
            relation: relation.clone(),
            operation,
        },
        *version,
    ))
}

pub fn normalize_changes(
    batch: &Batch,
    registry: &BTreeMap<Table, (Relation, i64)>,
) -> Result<(StagedTransaction, HashMap<u32, i64>)> {
    batch.encode()?;
    let mut changes = Vec::new();
    let mut versions = HashMap::new();
    let mut progress_requests = std::collections::BTreeSet::new();
    for event in &batch.events {
        if let Change::Control(Control::Progress, request) = &event.change {
            require(
                event.table.is_none() && request.len() == 16 && request.iter().any(|b| *b != 0),
                "invalid progress request control",
            )?;
            require(
                progress_requests.insert(request.clone()),
                "duplicate progress request control",
            )?;
            continue;
        }
        let (change, version) = normalize_row_event(event, registry)?;
        if let Some(previous) = versions.insert(change.relation_id, version) {
            require(previous == version, "conflicting relation version")?;
        }
        changes.push(change);
    }
    let tx = &batch.manifest.transaction;
    Ok((
        StagedTransaction {
            xid: tx.xid,
            begin_lsn: tx.begin_lsn,
            commit_lsn: tx.commit_lsn,
            end_lsn: tx.end_lsn,
            commit_time_micros: tx.commit_time_micros,
            changes,
        },
        versions,
    ))
}

/// Complete source-ordered actions for future schema-aware journal admission.
/// Each row retains its own local version, even when the same remote relation
/// appears on both sides of DDL in one transaction. Frozen admission still uses
/// normalize_batch and rejects Schema controls.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderedAction {
    Row {
        ordinal: u32,
        change: StagedChange,
        local_version: i64,
    },
    Schema {
        ordinal: u32,
        change: crate::push_schema::Change,
    },
    Progress {
        ordinal: u32,
        request: Id,
    },
}

pub fn normalize_ordered(
    batch: &Batch,
    registry: &BTreeMap<Table, (Relation, i64)>,
    initial: &crate::push_schema::State,
) -> Result<(Vec<OrderedAction>, crate::push_schema::State)> {
    let next = initial
        .after_batch(batch)
        .map_err(|_| Error("invalid ordered schema history"))?;
    let mut definitions = BTreeMap::new();
    for table in registry.keys() {
        if let Some((version, definition)) = initial.definition(&table.id) {
            if *version == table.schema_version {
                definitions.insert(*table, definition.clone());
            }
        }
    }
    let mut actions = Vec::with_capacity(batch.events.len());
    let mut requests = std::collections::BTreeSet::new();
    for event in &batch.events {
        let action = match &event.change {
            Change::Control(Control::Schema, payload) => {
                let change = crate::push_schema::Change::decode(payload)
                    .map_err(|_| Error("invalid schema payload"))?;
                definitions.insert(
                    Table {
                        id: change.table_id,
                        schema_version: change.after_version,
                    },
                    change.after.clone(),
                );
                OrderedAction::Schema {
                    ordinal: event.ordinal,
                    change,
                }
            }
            Change::Control(Control::Progress, payload) => {
                let request: Id = payload
                    .as_slice()
                    .try_into()
                    .map_err(|_| Error("invalid progress request control"))?;
                require(
                    request != [0; 16] && requests.insert(request),
                    "invalid or duplicate progress request control",
                )?;
                OrderedAction::Progress {
                    ordinal: event.ordinal,
                    request,
                }
            }
            Change::Control(..) => {
                return Err(Error(
                    "snapshot controls cannot be mixed with schema history",
                ));
            }
            _ => {
                let (change, local_version) = normalize_row_event(event, registry)?;
                let definition = definitions
                    .get(&event.table.unwrap())
                    .ok_or(Error("missing ordered row definition"))?;
                require(
                    definition
                        .relation(change.relation_id)
                        .map_err(|_| Error("invalid ordered row definition"))?
                        == change.relation,
                    "registered row definition differs from retained history",
                )?;
                OrderedAction::Row {
                    ordinal: event.ordinal,
                    change,
                    local_version,
                }
            }
        };
        actions.push(action);
    }
    Ok((actions, next))
}

/// The first snapshot implementation is one bounded, complete generation.
/// Controls authorize no partial journal admission and no arbitrary DDL.
pub fn normalize_batch(
    batch: &Batch,
    registry: &BTreeMap<Table, (Relation, i64)>,
) -> Result<(StagedTransaction, HashMap<u32, i64>, bool)> {
    batch.encode()?;
    if !batch
        .events
        .iter()
        .any(|event| matches!(event.change, Change::Control(kind,_) if kind!=Control::Progress))
    {
        let (transaction, versions) = normalize_changes(batch, registry)?;
        return Ok((transaction, versions, false));
    }
    require(
        batch.manifest.sequence == 1 && batch.events.len() >= 4,
        "snapshot must be the first complete generation",
    )?;
    let transaction = &batch.manifest.transaction;
    require(
        transaction.xid == 0
            && transaction.end_lsn > 0
            && transaction.commit_lsn == transaction.end_lsn - 1
            && transaction.begin_lsn == transaction.commit_lsn
            && transaction.commit_time_micros == 0,
        "snapshot requires its explicit synthetic baseline boundary",
    )?;
    let mut token = batch.manifest.stream.generation.to_be_bytes().to_vec();
    token.extend(batch.manifest.transaction.end_lsn.to_be_bytes());
    let count = batch.events.len();
    let control = |event: &Event, kind: Control, payload: &[u8]| {
        event.table.is_none() && event.change == Change::Control(kind, payload.to_vec())
    };
    require(
        control(&batch.events[0], Control::SnapshotBegin, &token),
        "snapshot begin is missing or inconsistent",
    )?;
    require(
        control(&batch.events[count - 2], Control::SnapshotComplete, &token)
            && control(&batch.events[count - 1], Control::SnapshotActivate, &token),
        "snapshot completion/activation is missing",
    )?;
    let mut inventory = token.clone();
    inventory.extend(((count - 4) as u32).to_be_bytes());
    require(
        control(&batch.events[count - 3], Control::SnapshotChunk, &inventory),
        "snapshot chunk inventory mismatch",
    )?;
    let mut rows = batch.events[1..count - 3].to_vec();
    for (index, event) in rows.iter_mut().enumerate() {
        require(
            matches!(event.change, Change::Insert(_)),
            "snapshot contains unsupported event",
        )?;
        event.ordinal = index as u32;
    }
    let rows = Batch::new(
        batch.manifest.stream.clone(),
        batch.manifest.sequence,
        batch.manifest.transaction.clone(),
        rows,
    )?;
    let (transaction, versions) = normalize_inserts(&rows, registry)?;
    Ok((transaction, versions, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Column;
    use crate::staging::changes_json;

    fn table() -> Table {
        Table {
            id: [2; 16],
            schema_version: 1,
        }
    }
    fn batch(change: Change) -> Batch {
        Batch::new(
            Stream {
                id: [1; 16],
                system_identifier: 42,
                database_oid: 7,
                generation: 1,
            },
            1,
            Transaction {
                xid: 9,
                begin_lsn: 100,
                commit_lsn: 110,
                end_lsn: 120,
                commit_time_micros: -1,
            },
            vec![Event {
                ordinal: 0,
                table: Some(table()),
                change,
            }],
        )
        .unwrap()
    }
    fn insert() -> Change {
        Change::Insert(vec![(1, Value::Text("7".into())), (2, Value::Null)])
    }
    fn registry() -> BTreeMap<Table, (Relation, i64)> {
        BTreeMap::from([(
            table(),
            (
                Relation {
                    id: 42,
                    namespace: "public".into(),
                    name: "items".into(),
                    replica_identity: b'd',
                    columns: vec![
                        Column {
                            flags: 1,
                            name: "id".into(),
                            type_oid: 20,
                            type_modifier: -1,
                        },
                        Column {
                            flags: 0,
                            name: "note".into(),
                            type_oid: 25,
                            type_modifier: -1,
                        },
                    ],
                },
                3,
            ),
        )])
    }
    fn resign(bytes: &mut Vec<u8>) {
        bytes.truncate(bytes.len() - 32);
        let digest = Sha256::digest(&*bytes);
        bytes.extend(digest);
    }

    #[test]
    fn multiple_schema_versions_preserve_two_table_group_and_native_counts() {
        let source = batch(insert());
        let second = Table {
            id: [3; 16],
            schema_version: 1,
        };
        let newer = Table {
            id: table().id,
            schema_version: 2,
        };
        let events = vec![
            source.events[0].clone(),
            Event {
                ordinal: 1,
                table: Some(newer),
                change: insert(),
            },
            Event {
                ordinal: 2,
                table: Some(second),
                change: insert(),
            },
        ];
        let expected = Batch::new(
            source.manifest.stream.clone(),
            1,
            source.manifest.transaction.clone(),
            events.clone(),
        )
        .unwrap();
        assert_eq!(expected.manifest.tables.len(), 3);
        assert_eq!(
            Batch::decode(&expected.encode().unwrap()).unwrap(),
            expected
        );
        let tx = &source.manifest.transaction;
        let mut raw = b"LKCAP1".to_vec();
        raw.extend(tx.xid.to_be_bytes());
        for value in [
            tx.begin_lsn,
            tx.commit_lsn,
            tx.end_lsn,
            tx.commit_time_micros as u64,
        ] {
            raw.extend(value.to_be_bytes());
        }
        raw.extend(3_u32.to_be_bytes());
        raw.extend(2_u32.to_be_bytes());
        raw.extend(1_u32.to_be_bytes());
        for event in &events {
            let bytes = event_bytes(event).unwrap();
            raw.extend((bytes.len() as u32).to_be_bytes());
            raw.extend(bytes);
        }
        assert_eq!(
            Batch::from_capture(source.manifest.stream.clone(), 1, [table(), second], &raw)
                .unwrap(),
            expected
        );
        raw[46..50].copy_from_slice(&1_u32.to_be_bytes());
        assert!(
            Batch::from_capture(source.manifest.stream.clone(), 1, [table(), second], &raw)
                .is_err()
        );
        let mut third = events;
        third.push(Event {
            ordinal: 3,
            table: Some(Table {
                id: [4; 16],
                schema_version: 1,
            }),
            change: insert(),
        });
        assert!(
            Batch::new(
                source.manifest.stream.clone(),
                1,
                source.manifest.transaction.clone(),
                third
            )
            .is_err()
        );
        let excessive = (0..=MAX_TABLE_VERSIONS)
            .map(|index| Event {
                ordinal: index as u32,
                table: Some(Table {
                    id: table().id,
                    schema_version: index as u32 + 1,
                }),
                change: insert(),
            })
            .collect();
        assert!(
            Batch::new(
                source.manifest.stream,
                1,
                source.manifest.transaction,
                excessive
            )
            .is_err()
        );
    }

    #[test]
    fn progress_controls_preserve_boundaries_without_row_mutations() {
        let source = batch(insert());
        let progress = Event {
            ordinal: 0,
            table: None,
            change: Change::Control(Control::Progress, vec![7; 16]),
        };
        let pure = Batch::new(
            source.manifest.stream.clone(),
            2,
            source.manifest.transaction.clone(),
            vec![progress.clone()],
        )
        .unwrap();
        let (transaction, versions, snapshot) = normalize_batch(&pure, &registry()).unwrap();
        assert!(transaction.changes.is_empty() && versions.is_empty() && !snapshot);
        assert_eq!(transaction.end_lsn, source.manifest.transaction.end_lsn);
        let mut mixed = source.events.clone();
        let mut marker = progress.clone();
        marker.ordinal = 1;
        mixed.push(marker);
        let mixed = Batch::new(
            source.manifest.stream.clone(),
            2,
            source.manifest.transaction.clone(),
            mixed,
        )
        .unwrap();
        assert_eq!(
            normalize_batch(&mixed, &registry())
                .unwrap()
                .0
                .changes
                .len(),
            1
        );
        for payload in [vec![], vec![0; 16], vec![1; 15], vec![1; 17]] {
            let invalid = Batch::new(
                source.manifest.stream.clone(),
                2,
                source.manifest.transaction.clone(),
                vec![Event {
                    ordinal: 0,
                    table: None,
                    change: Change::Control(Control::Progress, payload),
                }],
            )
            .unwrap();
            assert!(normalize_batch(&invalid, &registry()).is_err());
        }
        let mut duplicate = progress.clone();
        duplicate.ordinal = 1;
        let duplicate = Batch::new(
            source.manifest.stream,
            2,
            source.manifest.transaction,
            vec![progress, duplicate],
        )
        .unwrap();
        assert!(normalize_batch(&duplicate, &registry()).is_err());
    }

    #[test]
    fn roundtrip_preserves_row_states_old_identity_and_controls() {
        let row = vec![
            (1, Value::Absent),
            (2, Value::Null),
            (3, Value::Unchanged),
            (4, Value::Text("".into())),
            (5, Value::Text("世界".into())),
        ];
        let mut kinds = vec![
            Change::Insert(row.clone()),
            Change::Update {
                old: None,
                new: row.clone(),
            },
            Change::Update {
                old: Some(OldRow::Key(row.clone())),
                new: row.clone(),
            },
            Change::Update {
                old: Some(OldRow::Full(row.clone())),
                new: row.clone(),
            },
            Change::Delete(OldRow::Key(row.clone())),
            Change::Delete(OldRow::Full(row)),
            Change::Truncate(3),
        ];
        for kind in [
            Control::SnapshotBegin,
            Control::SnapshotChunk,
            Control::SnapshotComplete,
            Control::SnapshotActivate,
            Control::Schema,
            Control::Progress,
        ] {
            kinds.push(Change::Control(kind, vec![0, 255]));
        }
        for kind in kinds {
            let original = batch(kind);
            assert_eq!(
                Batch::decode(&original.encode().unwrap()).unwrap(),
                original
            );
        }
    }

    #[test]
    fn identity_and_digest_ignore_transport_chunk_boundaries() {
        let original = batch(insert());
        let wire = original.encode().unwrap();
        for size in [1, 7, 31, 64, 1000] {
            let reconstructed = wire.chunks(size).flatten().copied().collect::<Vec<_>>();
            let decoded = Batch::decode(&reconstructed).unwrap();
            assert_eq!(decoded.manifest, original.manifest);
            let (tx, versions) = normalize_inserts(&decoded, &registry()).unwrap();
            assert_eq!(
                changes_json(&tx, &versions).unwrap(),
                "[{\"remote_relation_id\":42,\"relation_schema_version\":3,\"operation\":\"insert\",\"new_tuple\":{\"id\":\"7\",\"note\":null}}]"
            );
        }
    }

    #[test]
    fn same_identity_changed_content_has_different_digest() {
        let original = batch(insert());
        let changed = batch(Change::Insert(vec![
            (1, Value::Text("8".into())),
            (2, Value::Null),
        ]));
        assert_eq!(original.manifest.batch_id, changed.manifest.batch_id);
        assert_ne!(original.manifest.digest, changed.manifest.digest);
    }

    #[test]
    fn rejects_every_truncation_corruption_and_unknown_version() {
        let wire = batch(insert()).encode().unwrap();
        for end in 0..wire.len() {
            assert!(Batch::decode(&wire[..end]).is_err());
        }
        for index in 0..wire.len() {
            let mut broken = wire.clone();
            broken[index] ^= 1;
            assert!(Batch::decode(&broken).is_err());
        }
        let mut unknown = wire.clone();
        unknown[7] = 2;
        resign(&mut unknown);
        assert_eq!(
            Batch::decode(&unknown),
            Err(Error("unsupported protocol version"))
        );
        let mut trailing = wire.clone();
        trailing.insert(trailing.len() - 32, 0);
        resign(&mut trailing);
        assert_eq!(Batch::decode(&trailing), Err(Error("trailing batch bytes")));
    }

    #[test]
    fn rejects_malicious_counts_ordinals_and_noncanonical_columns() {
        let original = batch(insert());
        let wire = original.encode().unwrap();
        let mut too_many = wire.clone();
        too_many[104..108].copy_from_slice(&100_001u32.to_be_bytes());
        resign(&mut too_many);
        assert!(Batch::decode(&too_many).is_err());
        let mut ordinal = wire;
        let offset = header(&original.manifest).len() + 4;
        ordinal[offset..offset + 4].copy_from_slice(&1u32.to_be_bytes());
        resign(&mut ordinal);
        assert_eq!(Batch::decode(&ordinal), Err(Error("event ordinal gap")));
        let mut changed = original.clone();
        changed.events[0].change = Change::Insert(vec![(1, Value::Null), (1, Value::Null)]);
        assert!(changed.encode().is_err());
        changed = original;
        changed.manifest.tables[0].1 += 1;
        assert!(changed.encode().is_err());
    }

    #[test]
    fn mutation_normalization_preserves_identity_and_unchanged_values() {
        let update = Change::Update {
            old: Some(OldRow::Key(vec![(1, Value::Text("1".into()))])),
            new: vec![(1, Value::Text("2".into())), (2, Value::Unchanged)],
        };
        let (tx, versions, snapshot) = normalize_batch(&batch(update), &registry()).unwrap();
        assert!(!snapshot);
        let json = changes_json(&tx, &versions).unwrap();
        assert!(json.contains("\"old_tuple\":{\"id\":\"1\"}"));
        assert!(json.contains("\"new_tuple\":{\"id\":\"2\"}"));
        for old in [
            vec![],
            vec![(2, Value::Text("1".into()))],
            vec![(1, Value::Null)],
            vec![(1, Value::Unchanged)],
            vec![(1, Value::Absent)],
        ] {
            assert!(
                normalize_batch(&batch(Change::Delete(OldRow::Key(old))), &registry()).is_err()
            );
        }
        for options in [0, 1, 2, 3] {
            let (tx, _, _) =
                normalize_batch(&batch(Change::Truncate(options)), &registry()).unwrap();
            assert_eq!(tx.changes[0].operation, Operation::Truncate { options });
        }
        assert!(normalize_batch(&batch(Change::Truncate(4)), &registry()).is_err());
        assert!(
            normalize_batch(
                &batch(Change::Update {
                    old: None,
                    new: vec![(1, Value::Unchanged), (2, Value::Null)]
                }),
                &registry()
            )
            .is_err()
        );
    }

    #[test]
    fn unchanged_key_uses_only_the_supplied_historical_identity() {
        for old in [
            OldRow::Key(vec![(1, Value::Text("7".into()))]),
            OldRow::Full(vec![(1, Value::Text("7".into())), (2, Value::Null)]),
        ] {
            let (tx, versions, _) = normalize_batch(
                &batch(Change::Update {
                    old: Some(old),
                    new: vec![(1, Value::Unchanged), (2, Value::Text("updated".into()))],
                }),
                &registry(),
            )
            .unwrap();
            let json = changes_json(&tx, &versions).unwrap();
            assert!(json.contains("\"new_tuple\":{\"id\":\"7\",\"note\":\"updated\"}"));
        }
        assert!(
            normalize_batch(
                &batch(Change::Update {
                    old: None,
                    new: vec![(1, Value::Unchanged), (2, Value::Null)]
                }),
                &registry()
            )
            .is_err()
        );
    }

    #[test]
    fn scalar_validation_rejects_overflow_before_journal_admission() {
        for value in [
            "9223372036854775808",
            "-9223372036854775809",
            "",
            "1.0",
            "NaN",
        ] {
            assert!(
                normalize_batch(
                    &batch(Change::Insert(vec![
                        (1, Value::Text(value.into())),
                        (2, Value::Null)
                    ])),
                    &registry()
                )
                .is_err()
            );
        }
        let mut typed = registry();
        typed.get_mut(&table()).unwrap().0.columns[1].type_oid = 23;
        assert!(
            normalize_batch(
                &batch(Change::Insert(vec![
                    (1, Value::Text("1".into())),
                    (2, Value::Text("2147483648".into()))
                ])),
                &typed
            )
            .is_err()
        );
        typed.get_mut(&table()).unwrap().0.columns[1].type_oid = 16;
        for value in ["t", "f", "true", "false"] {
            assert!(
                normalize_batch(
                    &batch(Change::Insert(vec![
                        (1, Value::Text("1".into())),
                        (2, Value::Text(value.into()))
                    ])),
                    &typed
                )
                .is_ok()
            );
        }
        assert!(
            normalize_batch(
                &batch(Change::Insert(vec![
                    (1, Value::Text("1".into())),
                    (2, Value::Text("unknown".into()))
                ])),
                &typed
            )
            .is_err()
        );
    }

    #[test]
    fn normalizer_rejects_unsupported_events_types_and_missing_values() {
        assert!(normalize_inserts(&batch(Change::Truncate(0)), &registry()).is_err());
        assert!(normalize_inserts(&batch(insert()), &BTreeMap::new()).is_err());
        for value in [
            Value::Absent,
            Value::Unchanged,
            Value::Null,
            Value::Text("a\0b".into()),
        ] {
            assert!(
                normalize_inserts(
                    &batch(Change::Insert(vec![(1, value), (2, Value::Null)])),
                    &registry()
                )
                .is_err()
            );
        }
        let mut unsupported = registry();
        unsupported.get_mut(&table()).unwrap().0.columns[1].type_oid = 17;
        assert!(normalize_inserts(&batch(insert()), &unsupported).is_err());
    }

    #[test]
    fn rejects_oversized_events_and_invalid_source_boundaries() {
        assert!(
            Batch::new(
                Stream {
                    id: [1; 16],
                    system_identifier: 1,
                    database_oid: 1,
                    generation: 1
                },
                1,
                Transaction {
                    xid: 1,
                    begin_lsn: 1,
                    commit_lsn: 2,
                    end_lsn: 3,
                    commit_time_micros: 0
                },
                vec![Event {
                    ordinal: 0,
                    table: None,
                    change: Change::Control(Control::Progress, vec![0; MAX_EVENT + 1])
                }]
            )
            .is_err()
        );
        let mut invalid = batch(insert());
        invalid.manifest.transaction.commit_lsn = invalid.manifest.transaction.end_lsn;
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn only_complete_consistent_snapshot_generations_reach_the_journal() {
        let original = batch(insert());
        let mut token = original.manifest.stream.generation.to_be_bytes().to_vec();
        token.extend(120u64.to_be_bytes());
        let mut inventory = token.clone();
        inventory.extend(1u32.to_be_bytes());
        let changes = vec![
            Change::Control(Control::SnapshotBegin, token.clone()),
            insert(),
            Change::Control(Control::SnapshotChunk, inventory),
            Change::Control(Control::SnapshotComplete, token.clone()),
            Change::Control(Control::SnapshotActivate, token),
        ];
        let events = changes
            .into_iter()
            .enumerate()
            .map(|(index, change)| Event {
                ordinal: index as u32,
                table: if matches!(change, Change::Insert(_)) {
                    Some(table())
                } else {
                    None
                },
                change,
            })
            .collect::<Vec<_>>();
        let transaction = Transaction {
            xid: 0,
            begin_lsn: 119,
            commit_lsn: 119,
            end_lsn: 120,
            commit_time_micros: 0,
        };
        let snapshot = Batch::new(
            original.manifest.stream.clone(),
            1,
            transaction.clone(),
            events.clone(),
        )
        .unwrap();
        let (staged, _, is_snapshot) = normalize_batch(&snapshot, &registry()).unwrap();
        assert!(is_snapshot);
        assert_eq!(staged.changes.len(), 1);
        for index in [0, 2, 3, 4] {
            let mut broken = events.clone();
            broken[index].change = Change::Control(Control::Progress, Vec::new());
            let incomplete = Batch::new(
                original.manifest.stream.clone(),
                1,
                transaction.clone(),
                broken,
            )
            .unwrap();
            assert!(normalize_batch(&incomplete, &registry()).is_err());
        }
        let late = Batch::new(original.manifest.stream, 2, transaction, events).unwrap();
        assert!(normalize_batch(&late, &registry()).is_err());
        assert!(!normalize_batch(&batch(insert()), &registry()).unwrap().2);
    }
}
