//! Source-neutral relation, row and transaction values admitted to the journal.
//! PostgreSQL wire messages remain in pgoutput; transport identity is separate.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityTuple {
    pub kind: IdentityKind,
    pub values: Vec<Cell>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityKind {
    Key,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TupleFormat {
    Text,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    UnchangedToast,
    Value { format: TupleFormat, bytes: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedChange {
    pub relation_id: u32,
    pub relation: Relation,
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Insert {
        new_values: Vec<Cell>,
    },
    Update {
        old_tuple: Option<IdentityTuple>,
        new_values: Vec<Cell>,
    },
    Delete {
        old_tuple: IdentityTuple,
    },
    Truncate {
        options: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedTransaction {
    pub xid: u32,
    pub begin_lsn: u64,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_time_micros: i64,
    pub changes: Vec<StagedChange>,
}
