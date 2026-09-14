//! Decoder for the insert-only subset of PostgreSQL's `pgoutput` protocol.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Begin(Begin),
    Commit(Commit),
    Relation(Relation),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate(Truncate),
    StreamStart(StreamStart),
    StreamStop,
    StreamCommit(StreamCommit),
    StreamAbort(StreamAbort),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    pub final_lsn: u64,
    pub commit_time_micros: i64,
    pub xid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_time_micros: i64,
}

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
pub struct Insert {
    pub relation_id: u32,
    pub values: Vec<Cell>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub relation_id: u32,
    pub old_tuple: Option<IdentityTuple>,
    pub new_values: Vec<Cell>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub relation_id: u32,
    pub old_tuple: IdentityTuple,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub options: u8,
    pub relation_ids: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamStart {
    pub xid: u32,
    pub first_segment: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCommit {
    pub xid: u32,
    pub commit: Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamAbort {
    pub xid: u32,
    pub subxid: u32,
    pub abort_lsn: Option<u64>,
    pub abort_time_micros: Option<i64>,
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
pub enum DecodeError {
    Truncated,
    InvalidUtf8,
    UnknownMessage(u8),
    InvalidTupleMarker(u8),
    InvalidTupleKind(u8),
    InvalidIdentityMarker(u8),
    MissingNewTuple,
    NegativeLength(i32),
    TrailingBytes(usize),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated pgoutput message"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in pgoutput identifier"),
            Self::UnknownMessage(tag) => write!(f, "unsupported pgoutput message {tag:#x}"),
            Self::InvalidTupleMarker(tag) => write!(f, "expected new-tuple marker, got {tag:#x}"),
            Self::InvalidTupleKind(tag) => write!(f, "invalid tuple-data kind {tag:#x}"),
            Self::InvalidIdentityMarker(tag) => write!(f, "invalid identity marker {tag:#x}"),
            Self::MissingNewTuple => write!(f, "UPDATE is missing its new tuple"),
            Self::NegativeLength(length) => write!(f, "negative tuple-data length {length}"),
            Self::TrailingBytes(count) => write!(f, "{count} trailing bytes in pgoutput message"),
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn decode(payload: &[u8]) -> Result<Message, DecodeError> {
    let mut cursor = Cursor::new(payload);
    let tag = cursor.byte()?;
    let message = match tag {
        b'B' => Message::Begin(Begin {
            final_lsn: cursor.u64()?,
            commit_time_micros: cursor.i64()?,
            xid: cursor.u32()?,
        }),
        b'C' => Message::Commit(Commit {
            flags: cursor.byte()?,
            commit_lsn: cursor.u64()?,
            end_lsn: cursor.u64()?,
            commit_time_micros: cursor.i64()?,
        }),
        b'R' => Message::Relation(decode_relation(&mut cursor)?),
        b'I' => Message::Insert(decode_insert(&mut cursor)?),
        b'U' => Message::Update(decode_update(&mut cursor)?),
        b'D' => Message::Delete(decode_delete(&mut cursor)?),
        b'T' => Message::Truncate(decode_truncate(&mut cursor)?),
        b'S' => Message::StreamStart(StreamStart {
            xid: cursor.u32()?,
            first_segment: cursor.byte()? != 0,
        }),
        b'E' => Message::StreamStop,
        b'c' => Message::StreamCommit(StreamCommit {
            xid: cursor.u32()?,
            commit: Commit {
                flags: cursor.byte()?,
                commit_lsn: cursor.u64()?,
                end_lsn: cursor.u64()?,
                commit_time_micros: cursor.i64()?,
            },
        }),
        b'A' => {
            let xid = cursor.u32()?;
            let subxid = cursor.u32()?;
            let (abort_lsn, abort_time_micros) = if cursor.remaining() == 0 {
                (None, None)
            } else {
                (Some(cursor.u64()?), Some(cursor.i64()?))
            };
            Message::StreamAbort(StreamAbort {
                xid,
                subxid,
                abort_lsn,
                abort_time_micros,
            })
        }
        other => return Err(DecodeError::UnknownMessage(other)),
    };
    if cursor.remaining() != 0 {
        return Err(DecodeError::TrailingBytes(cursor.remaining()));
    }
    Ok(message)
}

pub fn decode_streamed(payload: &[u8]) -> Result<(u32, Message), DecodeError> {
    let tag = *payload.first().ok_or(DecodeError::Truncated)?;
    if !matches!(tag, b'R' | b'I' | b'U' | b'D' | b'T') {
        return Err(DecodeError::UnknownMessage(tag));
    }
    let xid = u32::from_be_bytes(
        payload
            .get(1..5)
            .ok_or(DecodeError::Truncated)?
            .try_into()
            .unwrap(),
    );
    let mut ordinary = Vec::with_capacity(payload.len() - 4);
    ordinary.push(tag);
    ordinary.extend_from_slice(payload.get(5..).ok_or(DecodeError::Truncated)?);
    Ok((xid, decode(&ordinary)?))
}

fn decode_update(cursor: &mut Cursor<'_>) -> Result<Update, DecodeError> {
    let relation_id = cursor.u32()?;
    let old_tuple = match cursor.peek()? {
        b'K' | b'O' => Some(decode_identity(cursor)?),
        b'N' => None,
        _ => return Err(DecodeError::MissingNewTuple),
    };
    if cursor.byte()? != b'N' {
        return Err(DecodeError::MissingNewTuple);
    }
    Ok(Update {
        relation_id,
        old_tuple,
        new_values: decode_tuple(cursor)?,
    })
}

fn decode_delete(cursor: &mut Cursor<'_>) -> Result<Delete, DecodeError> {
    Ok(Delete {
        relation_id: cursor.u32()?,
        old_tuple: decode_identity(cursor)?,
    })
}

fn decode_identity(cursor: &mut Cursor<'_>) -> Result<IdentityTuple, DecodeError> {
    let kind = match cursor.byte()? {
        b'K' => IdentityKind::Key,
        b'O' => IdentityKind::Full,
        other => return Err(DecodeError::InvalidIdentityMarker(other)),
    };
    Ok(IdentityTuple {
        kind,
        values: decode_tuple(cursor)?,
    })
}

fn decode_truncate(cursor: &mut Cursor<'_>) -> Result<Truncate, DecodeError> {
    let count = cursor.u32()? as usize;
    let options = cursor.byte()?;
    let mut relation_ids = Vec::with_capacity(count);
    for _ in 0..count {
        relation_ids.push(cursor.u32()?);
    }
    Ok(Truncate {
        options,
        relation_ids,
    })
}

fn decode_relation(cursor: &mut Cursor<'_>) -> Result<Relation, DecodeError> {
    let id = cursor.u32()?;
    let namespace = cursor.cstring()?;
    let name = cursor.cstring()?;
    let replica_identity = cursor.byte()?;
    let count = cursor.u16()? as usize;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        columns.push(Column {
            flags: cursor.byte()?,
            name: cursor.cstring()?,
            type_oid: cursor.u32()?,
            type_modifier: cursor.i32()?,
        });
    }
    Ok(Relation {
        id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_insert(cursor: &mut Cursor<'_>) -> Result<Insert, DecodeError> {
    let relation_id = cursor.u32()?;
    let marker = cursor.byte()?;
    if marker != b'N' {
        return Err(DecodeError::InvalidTupleMarker(marker));
    }
    Ok(Insert {
        relation_id,
        values: decode_tuple(cursor)?,
    })
}

fn decode_tuple(cursor: &mut Cursor<'_>) -> Result<Vec<Cell>, DecodeError> {
    let count = cursor.u16()? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(match cursor.byte()? {
            b'n' => Cell::Null,
            b'u' => Cell::UnchangedToast,
            format @ (b't' | b'b') => {
                let length = cursor.i32()?;
                if length < 0 {
                    return Err(DecodeError::NegativeLength(length));
                }
                Cell::Value {
                    format: if format == b't' {
                        TupleFormat::Text
                    } else {
                        TupleFormat::Binary
                    },
                    bytes: cursor.take(length as usize)?.to_vec(),
                }
            }
            other => return Err(DecodeError::InvalidTupleKind(other)),
        });
    }
    Ok(values)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], DecodeError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(DecodeError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(DecodeError::Truncated)?;
        self.position = end;
        Ok(bytes)
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn peek(&self) -> Result<u8, DecodeError> {
        self.bytes
            .get(self.position)
            .copied()
            .ok_or(DecodeError::Truncated)
    }

    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn cstring(&mut self) -> Result<String, DecodeError> {
        let tail = self
            .bytes
            .get(self.position..)
            .ok_or(DecodeError::Truncated)?;
        let length = tail
            .iter()
            .position(|byte| *byte == 0)
            .ok_or(DecodeError::Truncated)?;
        let value = std::str::from_utf8(&tail[..length]).map_err(|_| DecodeError::InvalidUtf8)?;
        self.position += length + 1;
        Ok(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_cstring(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend(value.as_bytes());
        bytes.push(0);
    }

    #[test]
    fn decodes_begin_and_commit() {
        let mut begin = vec![b'B'];
        begin.extend(10_u64.to_be_bytes());
        begin.extend(20_i64.to_be_bytes());
        begin.extend(30_u32.to_be_bytes());
        assert_eq!(
            decode(&begin).unwrap(),
            Message::Begin(Begin {
                final_lsn: 10,
                commit_time_micros: 20,
                xid: 30,
            })
        );

        let mut commit = vec![b'C', 0];
        commit.extend(40_u64.to_be_bytes());
        commit.extend(50_u64.to_be_bytes());
        commit.extend(60_i64.to_be_bytes());
        assert_eq!(
            decode(&commit).unwrap(),
            Message::Commit(Commit {
                flags: 0,
                commit_lsn: 40,
                end_lsn: 50,
                commit_time_micros: 60,
            })
        );
    }

    #[test]
    fn decodes_relation() {
        let mut bytes = vec![b'R'];
        bytes.extend(42_u32.to_be_bytes());
        push_cstring(&mut bytes, "public");
        push_cstring(&mut bytes, "events");
        bytes.push(b'd');
        bytes.extend(1_u16.to_be_bytes());
        bytes.push(1);
        push_cstring(&mut bytes, "id");
        bytes.extend(23_u32.to_be_bytes());
        bytes.extend((-1_i32).to_be_bytes());
        let Message::Relation(relation) = decode(&bytes).unwrap() else {
            panic!("expected relation");
        };
        assert_eq!(relation.id, 42);
        assert_eq!(relation.columns[0].type_oid, 23);
    }

    #[test]
    fn decodes_insert_tuple_formats() {
        let mut bytes = vec![b'I'];
        bytes.extend(42_u32.to_be_bytes());
        bytes.push(b'N');
        bytes.extend(4_u16.to_be_bytes());
        bytes.push(b't');
        bytes.extend(1_i32.to_be_bytes());
        bytes.push(b'7');
        bytes.push(b'n');
        bytes.push(b'u');
        bytes.push(b'b');
        bytes.extend(2_i32.to_be_bytes());
        bytes.extend([0, 1]);
        let Message::Insert(insert) = decode(&bytes).unwrap() else {
            panic!("expected insert");
        };
        assert_eq!(
            insert.values,
            vec![
                Cell::Value {
                    format: TupleFormat::Text,
                    bytes: b"7".to_vec()
                },
                Cell::Null,
                Cell::UnchangedToast,
                Cell::Value {
                    format: TupleFormat::Binary,
                    bytes: vec![0, 1]
                },
            ]
        );
    }

    #[test]
    fn rejects_truncation_and_unknown_messages() {
        assert_eq!(decode(b"B"), Err(DecodeError::Truncated));
        assert_eq!(decode(b"X"), Err(DecodeError::UnknownMessage(b'X')));
    }

    #[test]
    fn decodes_update_delete_and_truncate() {
        let tuple = |value: u8| {
            let mut bytes = Vec::from(1_u16.to_be_bytes());
            bytes.push(b't');
            bytes.extend(1_i32.to_be_bytes());
            bytes.push(value);
            bytes
        };

        let mut update = vec![b'U'];
        update.extend(42_u32.to_be_bytes());
        update.push(b'K');
        update.extend(tuple(b'1'));
        update.push(b'N');
        update.extend(tuple(b'2'));
        let Message::Update(update) = decode(&update).unwrap() else {
            panic!("expected update");
        };
        assert_eq!(update.relation_id, 42);
        assert_eq!(update.old_tuple.unwrap().kind, IdentityKind::Key);

        let mut delete = vec![b'D'];
        delete.extend(42_u32.to_be_bytes());
        delete.push(b'O');
        delete.extend(tuple(b'2'));
        let Message::Delete(delete) = decode(&delete).unwrap() else {
            panic!("expected delete");
        };
        assert_eq!(delete.old_tuple.kind, IdentityKind::Full);

        let mut truncate = vec![b'T'];
        truncate.extend(2_u32.to_be_bytes());
        truncate.push(3);
        truncate.extend(42_u32.to_be_bytes());
        truncate.extend(43_u32.to_be_bytes());
        assert_eq!(
            decode(&truncate).unwrap(),
            Message::Truncate(Truncate {
                options: 3,
                relation_ids: vec![42, 43],
            })
        );
    }

    #[test]
    fn decodes_stream_control_and_xid_prefixed_change() {
        let mut start = vec![b'S'];
        start.extend(91_u32.to_be_bytes());
        start.push(1);
        assert_eq!(
            decode(&start).unwrap(),
            Message::StreamStart(StreamStart {
                xid: 91,
                first_segment: true,
            })
        );
        assert_eq!(decode(b"E").unwrap(), Message::StreamStop);
        start[5] = 0;
        assert_eq!(
            decode(&start).unwrap(),
            Message::StreamStart(StreamStart {
                xid: 91,
                first_segment: false,
            })
        );

        let mut insert = vec![b'I'];
        insert.extend(91_u32.to_be_bytes());
        insert.extend(42_u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(1_u16.to_be_bytes());
        insert.push(b't');
        insert.extend(1_i32.to_be_bytes());
        insert.push(b'7');
        let (xid, Message::Insert(insert)) = decode_streamed(&insert).unwrap() else {
            panic!("expected streamed insert");
        };
        assert_eq!(xid, 91);
        assert_eq!(insert.relation_id, 42);

        let mut commit = vec![b'c'];
        commit.extend(91_u32.to_be_bytes());
        commit.push(0);
        commit.extend(100_u64.to_be_bytes());
        commit.extend(110_u64.to_be_bytes());
        commit.extend(120_i64.to_be_bytes());
        let Message::StreamCommit(commit) = decode(&commit).unwrap() else {
            panic!("expected stream commit");
        };
        assert_eq!(commit.xid, 91);
        assert_eq!(commit.commit.end_lsn, 110);

        let mut abort = vec![b'A'];
        abort.extend(91_u32.to_be_bytes());
        abort.extend(92_u32.to_be_bytes());
        abort.extend(130_u64.to_be_bytes());
        abort.extend(140_i64.to_be_bytes());
        let Message::StreamAbort(abort) = decode(&abort).unwrap() else {
            panic!("expected stream abort");
        };
        assert_eq!(abort.subxid, 92);
        assert_eq!(abort.abort_lsn, Some(130));

        let mut short_abort = vec![b'A'];
        short_abort.extend(91_u32.to_be_bytes());
        short_abort.extend(91_u32.to_be_bytes());
        let Message::StreamAbort(short_abort) = decode(&short_abort).unwrap() else {
            panic!("expected short stream abort");
        };
        assert_eq!(short_abort.abort_lsn, None);
    }
}
