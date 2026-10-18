//! Restricted connection settings shared by launcher-managed workers.

use crate::wire::{Client, TlsConfig};
use std::collections::HashMap;
use std::env;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

const MAX_CREDENTIAL_FILE_BYTES: u64 = 64 * 1024;
const ALLOWED_KEYS: &[&str] = &[
    "LAKE_SOURCE_USER",
    "LAKE_SOURCE_PASSWORD",
    "LAKE_SOURCE_TLS_MODE",
    "LAKE_SOURCE_TLS_ROOT_CERT",
    "LAKE_SOURCE_TLS_SERVER_NAME",
    "LAKE_DESTINATION_USER",
    "LAKE_DESTINATION_PASSWORD",
    "LAKE_DESTINATION_TLS_MODE",
    "LAKE_DESTINATION_TLS_ROOT_CERT",
    "LAKE_DESTINATION_TLS_SERVER_NAME",
];

#[derive(Default)]
pub struct Settings {
    file_values: HashMap<String, String>,
}

impl Settings {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let Some(path) = env::var_os("LAKE_CREDENTIAL_FILE") else {
            return Ok(Self::default());
        };
        Self::from_file(Path::new(&path))
    }

    pub fn load_subscription_source_credentials(
        &mut self,
        subscription_id: i64,
        destination_address: &str,
        destination_user: &str,
        destination_database: &str,
        destination_password: Option<&str>,
        destination_tls: Option<&TlsConfig>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if env::var("LAKE_LOAD_SOURCE_CREDENTIALS").as_deref() != Ok("1") {
            return Ok(());
        }
        let mut destination = Client::connect_secure(
            destination_address,
            destination_user,
            destination_database,
            false,
            destination_password,
            destination_tls,
        )?;
        let rows = destination.simple_query(&format!(
            "SELECT source_user, source_password FROM lake_replication.source_credentials({subscription_id})"
        ))?;
        let row = rows
            .first()
            .ok_or("subscription source credentials are unavailable")?;
        if rows.len() != 1 || row.len() != 2 {
            return Err("subscription source credentials have an invalid shape".into());
        }
        // Rows created before migration 0019 have no parsed source auth yet.
        // Keep their existing file-based credentials until CONNECTION is reset.
        let Some(user) = row[0].as_deref().filter(|value| !value.is_empty()) else {
            return Ok(());
        };
        self.file_values
            .insert("LAKE_SOURCE_USER".to_owned(), user.to_owned());
        if let Some(password) = &row[1] {
            self.file_values
                .insert("LAKE_SOURCE_PASSWORD".to_owned(), password.to_owned());
        }
        Ok(())
    }

    fn from_file(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(format!(
                "credential file {} must be a regular file, not a symlink",
                path.display()
            )
            .into());
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            return Err(format!(
                "credential file {} must be owned by the worker operating-system user",
                path.display()
            )
            .into());
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "credential file {} must not grant group or other permissions",
                path.display()
            )
            .into());
        }
        if metadata.len() > MAX_CREDENTIAL_FILE_BYTES {
            return Err(format!(
                "credential file {} exceeds {} bytes",
                path.display(),
                MAX_CREDENTIAL_FILE_BYTES
            )
            .into());
        }

        let mut contents = String::new();
        file.take(MAX_CREDENTIAL_FILE_BYTES + 1)
            .read_to_string(&mut contents)?;
        if contents.len() as u64 > MAX_CREDENTIAL_FILE_BYTES {
            return Err(format!(
                "credential file {} exceeds {} bytes",
                path.display(),
                MAX_CREDENTIAL_FILE_BYTES
            )
            .into());
        }
        let mut file_values = HashMap::new();
        for (index, raw_line) in contents.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, raw_value) = line.split_once('=').ok_or_else(|| {
                format!(
                    "credential file {} line {} must use KEY=value",
                    path.display(),
                    index + 1
                )
            })?;
            let key = key.trim();
            if !ALLOWED_KEYS.contains(&key) {
                return Err(format!(
                    "credential file {} line {} contains unsupported key {}",
                    path.display(),
                    index + 1,
                    key
                )
                .into());
            }
            if file_values
                .insert(key.to_owned(), raw_value.to_owned())
                .is_some()
            {
                return Err(format!(
                    "credential file {} contains duplicate key {}",
                    path.display(),
                    key
                )
                .into());
            }
        }
        Ok(Self { file_values })
    }

    pub fn optional(&self, name: &str) -> Option<String> {
        env::var(name)
            .ok()
            .or_else(|| self.file_values.get(name).cloned())
    }

    pub fn value(&self, name: &str, default: &str) -> String {
        self.optional(name).unwrap_or_else(|| default.to_owned())
    }

    pub fn required(&self, name: &str) -> Result<String, Box<dyn std::error::Error>> {
        self.optional(name)
            .ok_or_else(|| format!("required connection setting {name} is not set").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);

    fn credential_file(mode: u32, contents: &str) -> std::path::PathBuf {
        let path = env::temp_dir().join(format!(
            "pg-lake-credentials-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&path)
            .unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn reads_only_supported_connection_settings() {
        let path = credential_file(
            0o600,
            "# worker credentials\nLAKE_SOURCE_USER=replicator\nLAKE_SOURCE_PASSWORD=a b#c\n",
        );
        let settings = Settings::from_file(&path).unwrap();
        assert_eq!(
            settings.optional("LAKE_SOURCE_USER").as_deref(),
            Some("replicator")
        );
        assert_eq!(
            settings.optional("LAKE_SOURCE_PASSWORD").as_deref(),
            Some("a b#c")
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn rejects_group_readable_files_and_unknown_keys() {
        let open_path = credential_file(0o640, "LAKE_SOURCE_PASSWORD=secret\n");
        assert!(Settings::from_file(&open_path).is_err());
        fs::remove_file(open_path).unwrap();

        let unknown_path = credential_file(0o600, "LAKE_SLOT=must-not-be-overridden\n");
        assert!(Settings::from_file(&unknown_path).is_err());
        fs::remove_file(unknown_path).unwrap();
    }

    #[test]
    fn rejects_symbolic_links() {
        use std::os::unix::fs::symlink;

        let target = credential_file(0o600, "LAKE_SOURCE_PASSWORD=secret\n");
        let link = target.with_extension("link");
        symlink(&target, &link).unwrap();
        assert!(Settings::from_file(&link).is_err());
        fs::remove_file(link).unwrap();
        fs::remove_file(target).unwrap();
    }
}
