use crate::pgoutput::{Cell, Commit, Delete, IdentityTuple, Insert, Relation, Truncate, Update};
use std::collections::HashMap;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    NestedBegin,
    InsertOutsideTransaction,
    CommitOutsideTransaction,
    UnknownRelation(u32),
    IdentityUnavailable(u32),
}

impl std::fmt::Display for AssembleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NestedBegin => write!(f, "received BEGIN inside a transaction"),
            Self::InsertOutsideTransaction => write!(f, "received INSERT outside a transaction"),
            Self::CommitOutsideTransaction => write!(f, "received COMMIT outside a transaction"),
            Self::UnknownRelation(id) => write!(f, "received INSERT for unknown relation {id}"),
            Self::IdentityUnavailable(id) => {
                write!(f, "relation {id} has no usable replica identity")
            }
        }
    }
}

impl std::error::Error for AssembleError {}

#[derive(Debug, Default)]
pub struct TransactionAssembler {
    relations: HashMap<u32, Relation>,
    current: Option<PendingTransaction>,
}

#[derive(Debug)]
struct PendingTransaction {
    xid: u32,
    begin_lsn: u64,
    changes: Vec<StagedChange>,
}

impl TransactionAssembler {
    pub fn relation(&mut self, relation: Relation) {
        self.relations.insert(relation.id, relation);
    }

    pub fn begin(&mut self, begin_lsn: u64, xid: u32) -> Result<(), AssembleError> {
        if self.current.is_some() {
            return Err(AssembleError::NestedBegin);
        }
        self.current = Some(PendingTransaction {
            xid,
            begin_lsn,
            changes: Vec::new(),
        });
        Ok(())
    }

    pub fn insert(&mut self, insert: Insert) -> Result<(), AssembleError> {
        let relation = self
            .relations
            .get(&insert.relation_id)
            .cloned()
            .ok_or(AssembleError::UnknownRelation(insert.relation_id))?;
        let current = self
            .current
            .as_mut()
            .ok_or(AssembleError::InsertOutsideTransaction)?;
        current.changes.push(StagedChange {
            relation_id: insert.relation_id,
            relation,
            operation: Operation::Insert {
                new_values: insert.values,
            },
        });
        Ok(())
    }

    pub fn update(&mut self, update: Update) -> Result<(), AssembleError> {
        let relation = self.relation_for(update.relation_id)?;
        if update.old_tuple.is_none()
            && !relation.columns.iter().any(|column| column.flags & 1 != 0)
        {
            return Err(AssembleError::IdentityUnavailable(update.relation_id));
        }
        self.push(StagedChange {
            relation_id: update.relation_id,
            relation,
            operation: Operation::Update {
                old_tuple: update.old_tuple,
                new_values: update.new_values,
            },
        })
    }

    pub fn delete(&mut self, delete: Delete) -> Result<(), AssembleError> {
        let relation = self.relation_for(delete.relation_id)?;
        self.push(StagedChange {
            relation_id: delete.relation_id,
            relation,
            operation: Operation::Delete {
                old_tuple: delete.old_tuple,
            },
        })
    }

    pub fn truncate(&mut self, truncate: Truncate) -> Result<(), AssembleError> {
        for relation_id in truncate.relation_ids {
            let relation = self.relation_for(relation_id)?;
            self.push(StagedChange {
                relation_id,
                relation,
                operation: Operation::Truncate {
                    options: truncate.options,
                },
            })?;
        }
        Ok(())
    }

    fn relation_for(&self, relation_id: u32) -> Result<Relation, AssembleError> {
        self.relations
            .get(&relation_id)
            .cloned()
            .ok_or(AssembleError::UnknownRelation(relation_id))
    }

    fn push(&mut self, change: StagedChange) -> Result<(), AssembleError> {
        self.current
            .as_mut()
            .ok_or(AssembleError::InsertOutsideTransaction)?
            .changes
            .push(change);
        Ok(())
    }

    pub fn commit(&mut self, commit: Commit) -> Result<StagedTransaction, AssembleError> {
        let current = self
            .current
            .take()
            .ok_or(AssembleError::CommitOutsideTransaction)?;
        Ok(StagedTransaction {
            xid: current.xid,
            begin_lsn: current.begin_lsn,
            commit_lsn: commit.commit_lsn,
            end_lsn: commit.end_lsn,
            commit_time_micros: commit.commit_time_micros,
            changes: current.changes,
        })
    }

    pub fn drain_changes(&mut self) -> Result<Vec<StagedChange>, AssembleError> {
        let current = self
            .current
            .as_mut()
            .ok_or(AssembleError::InsertOutsideTransaction)?;
        Ok(std::mem::take(&mut current.changes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::{Column, TupleFormat};

    fn relation() -> Relation {
        Relation {
            id: 42,
            namespace: "public".into(),
            name: "events".into(),
            replica_identity: b'd',
            columns: vec![Column {
                flags: 1,
                name: "id".into(),
                type_oid: 23,
                type_modifier: -1,
            }],
        }
    }

    #[test]
    fn assembles_only_complete_transactions() {
        let mut assembler = TransactionAssembler::default();
        assembler.relation(relation());
        assembler.begin(100, 7).unwrap();
        assembler
            .insert(Insert {
                relation_id: 42,
                values: vec![Cell::Value {
                    format: TupleFormat::Text,
                    bytes: b"1".to_vec(),
                }],
            })
            .unwrap();
        let staged = assembler
            .commit(Commit {
                flags: 0,
                commit_lsn: 110,
                end_lsn: 120,
                commit_time_micros: 5,
            })
            .unwrap();
        assert_eq!(staged.xid, 7);
        assert_eq!(staged.changes.len(), 1);
        assert_eq!(staged.changes[0].relation.name, "events");
    }

    #[test]
    fn refuses_an_insert_without_begin() {
        let mut assembler = TransactionAssembler::default();
        assembler.relation(relation());
        assert_eq!(
            assembler.insert(Insert {
                relation_id: 42,
                values: vec![],
            }),
            Err(AssembleError::InsertOutsideTransaction)
        );
    }
}
