//! Minimal PostgreSQL v3 wire client for simple queries and logical replication.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(String),
    Server(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::Server(message) => write!(f, "PostgreSQL error: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub fn connect(
        address: impl ToSocketAddrs,
        user: &str,
        database: &str,
        replication: bool,
    ) -> Result<Self, Error> {
        let mut stream = TcpStream::connect(address)?;
        stream.set_nodelay(true)?;
        let mut body = Vec::new();
        body.extend(196_608_u32.to_be_bytes());
        parameter(&mut body, "user", user);
        parameter(&mut body, "database", database);
        parameter(&mut body, "application_name", "lake-replication-worker");
        if replication {
            parameter(&mut body, "replication", "database");
        }
        body.push(0);
        let length = u32::try_from(body.len() + 4)
            .map_err(|_| Error::Protocol("startup packet is too large".into()))?;
        stream.write_all(&length.to_be_bytes())?;
        stream.write_all(&body)?;

        let mut client = Self { stream };
        loop {
            let (tag, body) = client.read_message()?;
            match tag {
                b'R' if read_u32(&body, 0)? == 0 => {}
                b'R' => {
                    return Err(Error::Protocol(
                        "only trust authentication is supported by the milestone receiver".into(),
                    ));
                }
                b'S' | b'K' | b'N' => {}
                b'E' => return Err(Error::Server(error_message(&body))),
                b'Z' => break,
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected startup message {other:#x}"
                    )));
                }
            }
        }
        Ok(client)
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), Error> {
        self.stream.set_read_timeout(timeout)?;
        Ok(())
    }

    pub fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>, Error> {
        self.send_message(b'Q', nul_terminated(sql).as_slice())?;
        let mut rows = Vec::new();
        let mut server_error = None;
        loop {
            let (tag, body) = self.read_message()?;
            if server_error.is_some() {
                match tag {
                    b'N' | b'C' | b'I' => continue,
                    b'Z' => return Err(server_error.take().unwrap()),
                    _ => continue,
                }
            }
            match tag {
                b'D' => rows.push(data_row(&body)?),
                b'T' | b'C' | b'I' | b'N' => {}
                b'E' => server_error = Some(Error::Server(error_message(&body))),
                b'Z' => return Ok(rows),
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected query message {other:#x}"
                    )));
                }
            }
        }
    }

    pub fn copy_out<F>(&mut self, sql: &str, mut consume: F) -> Result<(), Error>
    where
        F: FnMut(&[u8]) -> Result<(), Error>,
    {
        self.send_message(b'Q', nul_terminated(sql).as_slice())?;
        let mut copying = false;
        loop {
            let (tag, body) = self.read_message()?;
            match tag {
                b'H' if !copying => copying = true,
                b'd' if copying => consume(&body)?,
                b'c' if copying => copying = false,
                b'C' | b'N' => {}
                b'E' => return Err(Error::Server(error_message(&body))),
                b'Z' if !copying => return Ok(()),
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected COPY OUT message {other:#x}"
                    )));
                }
            }
        }
    }

    pub fn start_replication(&mut self, command: &str) -> Result<(), Error> {
        self.send_message(b'Q', nul_terminated(command).as_slice())?;
        loop {
            let (tag, body) = self.read_message()?;
            match tag {
                b'W' => return Ok(()),
                b'N' => {}
                b'E' => return Err(Error::Server(error_message(&body))),
                other => {
                    return Err(Error::Protocol(format!(
                        "expected CopyBothResponse, got {other:#x}"
                    )));
                }
            }
        }
    }

    pub fn read_copy_data(&mut self) -> Result<Vec<u8>, Error> {
        loop {
            let (tag, body) = self.read_message()?;
            match tag {
                b'd' => return Ok(body),
                b'N' => {}
                b'E' => return Err(Error::Server(error_message(&body))),
                other => {
                    return Err(Error::Protocol(format!(
                        "unexpected replication message {other:#x}"
                    )));
                }
            }
        }
    }

    pub fn standby_status(&mut self, lsn: u64) -> Result<(), Error> {
        let mut body = Vec::with_capacity(34);
        body.push(b'r');
        body.extend(lsn.to_be_bytes());
        body.extend(lsn.to_be_bytes());
        body.extend(lsn.to_be_bytes());
        body.extend(postgres_now_micros().to_be_bytes());
        body.push(0);
        self.send_message(b'd', &body)
    }

    fn send_message(&mut self, tag: u8, body: &[u8]) -> Result<(), Error> {
        let length = u32::try_from(body.len() + 4)
            .map_err(|_| Error::Protocol("message is too large".into()))?;
        self.stream.write_all(&[tag])?;
        self.stream.write_all(&length.to_be_bytes())?;
        self.stream.write_all(body)?;
        self.stream.flush()?;
        Ok(())
    }

    fn read_message(&mut self) -> Result<(u8, Vec<u8>), Error> {
        let mut header = [0_u8; 5];
        self.stream.read_exact(&mut header)?;
        let length = u32::from_be_bytes(header[1..5].try_into().unwrap());
        if !(4..=64 * 1024 * 1024).contains(&length) {
            return Err(Error::Protocol(format!("invalid message length {length}")));
        }
        let mut body = vec![0; length as usize - 4];
        self.stream.read_exact(&mut body)?;
        Ok((header[0], body))
    }
}

fn parameter(buffer: &mut Vec<u8>, key: &str, value: &str) {
    buffer.extend(key.as_bytes());
    buffer.push(0);
    buffer.extend(value.as_bytes());
    buffer.push(0);
}

fn nul_terminated(value: &str) -> Vec<u8> {
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

fn data_row(body: &[u8]) -> Result<Vec<Option<String>>, Error> {
    let count = read_u16(body, 0)? as usize;
    let mut position = 2;
    let mut row = Vec::with_capacity(count);
    for _ in 0..count {
        let length = read_i32(body, position)?;
        position += 4;
        if length == -1 {
            row.push(None);
            continue;
        }
        if length < 0 {
            return Err(Error::Protocol("invalid DataRow field length".into()));
        }
        let end = position
            .checked_add(length as usize)
            .ok_or_else(|| Error::Protocol("DataRow overflow".into()))?;
        let bytes = body
            .get(position..end)
            .ok_or_else(|| Error::Protocol("truncated DataRow".into()))?;
        row.push(Some(
            std::str::from_utf8(bytes)
                .map_err(|_| Error::Protocol("non-UTF-8 DataRow".into()))?
                .to_owned(),
        ));
        position = end;
    }
    if position != body.len() {
        return Err(Error::Protocol("trailing DataRow bytes".into()));
    }
    Ok(row)
}

fn error_message(body: &[u8]) -> String {
    let mut position = 0;
    let mut fields = Vec::new();
    while let Some(code) = body.get(position).copied() {
        position += 1;
        if code == 0 {
            break;
        }
        let Some(end) = body[position..].iter().position(|byte| *byte == 0) else {
            break;
        };
        let value = String::from_utf8_lossy(&body[position..position + end]);
        if matches!(code, b'S' | b'C' | b'M' | b'D') {
            fields.push(format!("{}={value}", code as char));
        }
        position += end + 1;
    }
    fields.join(" ")
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, Error> {
    Ok(u16::from_be_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or_else(|| Error::Protocol("truncated u16".into()))?
            .try_into()
            .unwrap(),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    Ok(u32::from_be_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or_else(|| Error::Protocol("truncated u32".into()))?
            .try_into()
            .unwrap(),
    ))
}

fn read_i32(bytes: &[u8], offset: usize) -> Result<i32, Error> {
    Ok(i32::from_be_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or_else(|| Error::Protocol("truncated i32".into()))?
            .try_into()
            .unwrap(),
    ))
}

fn postgres_now_micros() -> i64 {
    const POSTGRES_EPOCH_UNIX_SECONDS: u64 = 946_684_800;
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = unix.as_secs().saturating_sub(POSTGRES_EPOCH_UNIX_SECONDS);
    i64::try_from(seconds)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(unix.subsec_micros()))
}
