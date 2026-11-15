//! Validate identity selection through all four real executable entry points.
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::OpenOptionsExt;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const BINARIES: &[&str] = &[
    env!("CARGO_BIN_EXE_lake-replication-receiver"),
    env!("CARGO_BIN_EXE_lake-replication-publisher"),
    env!("CARGO_BIN_EXE_lake-replication-initial-sync"),
    env!("CARGO_BIN_EXE_lake-replication-supervisor"),
];
const IDENTITY_ERROR: &str = "managed workers require a non-empty LAKE_DESTINATION_USER";
static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

struct CredentialFile(std::path::PathBuf);
impl CredentialFile {
    fn new(contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "lake-worker-identity-{}-{}",
            std::process::id(),
            NEXT_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        Self(path)
    }
}
impl Drop for CredentialFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn command(binary: &str, address: &str) -> Command {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .env("LAKE_LOAD_SOURCE_CREDENTIALS", "1")
        .env("LAKE_SUBSCRIPTION_ID", "1")
        .env("LAKE_DESTINATION_ADDRESS", address)
        .env("LAKE_DESTINATION_DATABASE", "analytics")
        .env("LAKE_SLOT", "identity_probe")
        .env("LAKE_PUBLICATION", "identity_probe");
    command
}

fn diagnostic(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn output(command: &mut Command) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "worker did not finish identity validation: {}",
                diagnostic(&output)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn managed_binaries_reject_absent_blank_and_legacy_only_identity_without_connecting() {
    let valid = CredentialFile::new("LAKE_DESTINATION_USER=from_file\n");
    let missing = CredentialFile::new("LAKE_DESTINATION_PASSWORD=do-not-log-this\n");
    for binary in BINARIES {
        for case in 0..6 {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let mut child = command(binary, &listener.local_addr().unwrap().to_string());
            match case {
                0 => &mut child,
                1 => child.env("LAKE_USER", "postgres"),
                2 => child.env("LAKE_CREDENTIAL_FILE", &missing.0),
                3 => child.env("LAKE_DESTINATION_USER", ""),
                4 => child.env("LAKE_DESTINATION_USER", " \t"),
                _ => child
                    .env("LAKE_CREDENTIAL_FILE", &valid.0)
                    .env("LAKE_DESTINATION_USER", ""),
            };
            let output = output(&mut child);
            assert!(!output.status.success(), "{binary} accepted case {case}");
            let log = diagnostic(&output);
            assert!(log.contains(IDENTITY_ERROR), "{binary} case {case}: {log}");
            assert!(!log.contains("do-not-log-this"));
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "{binary} opened a destination connection without an identity"
            );
        }
    }
}

fn startup_user(mut stream: TcpStream) -> String {
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut length = [0; 4];
    stream.read_exact(&mut length).unwrap();
    let length = u32::from_be_bytes(length) as usize;
    assert!((8..=16_384).contains(&length));
    let mut startup = vec![0; length - 4];
    stream.read_exact(&mut startup).unwrap();
    assert_eq!(&startup[..4], &196608_u32.to_be_bytes());
    let fields: Vec<_> = startup[4..].split(|byte| *byte == 0).collect();
    let user = fields
        .chunks_exact(2)
        .find(|pair| pair[0] == b"user")
        .map(|pair| String::from_utf8(pair[1].to_vec()).unwrap())
        .unwrap();
    // Reject authentication after observing the actual protocol identity.
    let message = b"SFATAL\0C28000\0Midentity fixture refused login\0\0";
    stream.write_all(b"E").unwrap();
    stream
        .write_all(&((message.len() + 4) as u32).to_be_bytes())
        .unwrap();
    stream.write_all(message).unwrap();
    user
}

#[test]
fn managed_binaries_send_the_explicit_file_or_environment_identity() {
    let file = CredentialFile::new("LAKE_DESTINATION_USER=worker '雪\n");
    for binary in BINARIES {
        for override_user in [None, Some("explicit_override")] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let server = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    match listener.accept() {
                        Ok((stream, _)) => return startup_user(stream),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline, "worker did not connect");
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => panic!("{error}"),
                    }
                }
            });
            let mut child = command(binary, &address);
            child.env("LAKE_CREDENTIAL_FILE", &file.0);
            if let Some(user) = override_user {
                child.env("LAKE_DESTINATION_USER", user);
            }
            let output = output(&mut child);
            assert!(!output.status.success());
            assert!(
                diagnostic(&output).contains("identity fixture refused login"),
                "{binary}: {}",
                diagnostic(&output)
            );
            assert_eq!(
                server.join().unwrap(),
                override_user.unwrap_or("worker '雪")
            );
        }
    }
}
