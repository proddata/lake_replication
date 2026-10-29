//! Destination transactions owned by a managed worker generation.
use crate::wire::{Client, Error, TlsConfig};

pub type Rows = Vec<Vec<Option<String>>>;

pub struct ManagedClient {
    client: Client,
    subscription: i64,
    generation: i64,
    poisoned: bool,
}

impl ManagedClient {
    pub fn connect(
        address: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
        tls: Option<&TlsConfig>,
        subscription: i64,
        generation: i64,
    ) -> Result<Self, Error> {
        if subscription <= 0 || generation < 0 {
            return Err(Error::Protocol("invalid managed worker identity".into()));
        }
        let mut client = Client::connect_named_secure(
            address,
            user,
            database,
            false,
            &format!("lake_replication_worker/{subscription}/{generation}"),
            password,
            tls,
        )?;
        client.simple_query(&format!("SET client_connection_check_interval='100ms'; SET lake_replication.worker_generation='{generation}'; SET lake_replication.worker_subscription='{subscription}'"))?;
        Ok(Self {
            client,
            subscription,
            generation,
            poisoned: false,
        })
    }

    fn fence(&self) -> String {
        if self.generation == 0 {
            return String::new();
        }
        format!(
            "DO $lake_worker$ BEGIN PERFORM lake_replication.assert_worker({},{}); END $lake_worker$; ",
            self.subscription, self.generation
        )
    }

    fn transaction(&mut self, before_fence: &str, sql: &str) -> Result<Rows, Error> {
        if self.poisoned {
            return Err(Error::Protocol(
                "managed connection is unusable; reconnect required".into(),
            ));
        }
        let result = self.client.simple_query(&format!(
            "BEGIN; {before_fence}{}{sql}; COMMIT",
            self.fence()
        ));
        match &result {
            Err(Error::Server(_)) => {
                // Server errors were drained through ReadyForQuery. Only that
                // synchronized connection can safely receive a rollback.
                if self.client.simple_query("ROLLBACK").is_err() {
                    self.poisoned = true;
                    self.client.abort();
                }
            }
            Err(_) => {
                self.poisoned = true;
                self.client.abort();
            }
            Ok(_) => {}
        }
        result
    }

    pub fn simple_query(&mut self, sql: &str) -> Result<Rows, Error> {
        self.transaction("", sql)
    }

    pub fn staging_query(&mut self, sql: &str) -> Result<Rows, Error> {
        // Wait for the instance permit before taking the generation fence.
        // Stop can cancel this waiter without waiting for its config-row lock.
        self.transaction(
            "SET LOCAL lock_timeout='10s'; SELECT lake_replication.admit_staging(); ",
            sql,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::Duration;

    fn message(stream: &mut TcpStream, tag: u8, body: &[u8]) {
        stream.write_all(&[tag]).unwrap();
        stream
            .write_all(&((body.len() + 4) as u32).to_be_bytes())
            .unwrap();
        stream.write_all(body).unwrap();
    }
    fn packet(stream: &mut TcpStream) -> Vec<u8> {
        let mut length = [0; 4];
        stream.read_exact(&mut length).unwrap();
        let mut body = vec![0; u32::from_be_bytes(length) as usize - 4];
        stream.read_exact(&mut body).unwrap();
        body
    }
    fn query(stream: &mut TcpStream) -> String {
        let mut tag = [0];
        stream.read_exact(&mut tag).unwrap();
        assert_eq!(tag[0], b'Q');
        let mut body = packet(stream);
        assert_eq!(body.pop(), Some(0));
        String::from_utf8(body).unwrap()
    }
    fn handshake(stream: &mut TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        packet(stream);
        message(stream, b'R', &0u32.to_be_bytes());
        message(stream, b'Z', b"I");
        query(stream);
        message(stream, b'C', b"SET\0");
        message(stream, b'Z', b"I");
    }
    #[test]
    fn server_error_rolls_back_before_reusing_a_synchronized_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            handshake(&mut stream);
            query(&mut stream);
            message(&mut stream, b'E', b"SERROR\0C55000\0Mstopped worker\0\0");
            message(&mut stream, b'Z', b"E");
            assert_eq!(query(&mut stream), "ROLLBACK");
            message(&mut stream, b'C', b"ROLLBACK\0");
            message(&mut stream, b'Z', b"I");
            query(&mut stream);
            message(&mut stream, b'D', b"\0\x01\0\0\0\x012");
            message(&mut stream, b'C', b"SELECT 1\0");
            message(&mut stream, b'Z', b"I");
        });
        let mut client =
            ManagedClient::connect(&address, "postgres", "analytics", None, None, 1, 1).unwrap();
        assert!(matches!(
            client.simple_query("SELECT 1"),
            Err(Error::Server(_))
        ));
        assert_eq!(
            client.simple_query("SELECT 2").unwrap()[0][0].as_deref(),
            Some("2")
        );
        server.join().unwrap();
    }
    #[test]
    fn protocol_failure_closes_the_socket_and_forbids_rollback_or_reuse() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            handshake(&mut stream);
            query(&mut stream);
            message(&mut stream, b'?', b"");
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let mut client =
            ManagedClient::connect(&address, "postgres", "analytics", None, None, 1, 1).unwrap();
        assert!(matches!(
            client.simple_query("SELECT 1"),
            Err(Error::Protocol(_))
        ));
        assert!(
            matches!(client.simple_query("SELECT 2"),Err(Error::Protocol(message)) if message.contains("unusable"))
        );
        server.join().unwrap();
    }
}
