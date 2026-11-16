//! Disposable, quota-controlled files. Retained WAL, never these files, owns replay.
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const HEADER: usize = 52;
#[derive(Clone, Copy)]
struct Mark {
    offset: u64,
    rows: u64,
    records: u64,
}
pub struct Transaction {
    pub xid: u32,
    file: File,
    path: PathBuf,
    pub rows: u64,
    pub records: u64,
    pub bytes: u64,
    sub: HashMap<u32, Mark>,
}
pub struct Spill {
    directory: PathBuf,
    _lock: File,
    pub transactions: HashMap<u32, Transaction>,
    pub used: u64,
    pub limit: u64,
    pub peak: u64,
    max_active: usize,
    record_limit: usize,
}
fn private_dir(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(m) => {
            if !m.is_dir() || m.uid() != unsafe { libc::geteuid() } || m.mode() & 0o077 != 0 {
                return Err("spill directory must be an owned private directory".into());
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            DirBuilder::new().mode(0o700).create(path)?;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}
impl Spill {
    pub fn open(
        base: &Path,
        identity: &str,
        limit: u64,
        max_active: usize,
        record_limit: usize,
    ) -> Result<Self> {
        private_dir(base)?;
        let token = Sha256::digest(identity.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let directory = base.join(token);
        private_dir(&directory)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(directory.join(".owner"))?;
        let meta = lock.metadata()?;
        if !meta.is_file() || meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o077 != 0 {
            return Err("unsafe spill owner file".into());
        }
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err("spill namespace is owned by another process".into());
        }
        let mut removed = 0;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or("invalid spill filename")?;
            if name == ".owner" {
                continue;
            }
            let digits = name
                .strip_prefix("tx-")
                .and_then(|s| s.strip_suffix(".bin"));
            let meta = fs::symlink_metadata(entry.path())?;
            if !digits.is_some_and(|s| s.len() == 10 && s.bytes().all(|b| b.is_ascii_digit()))
                || !meta.is_file()
                || meta.uid() != unsafe { libc::geteuid() }
                || meta.mode() & 0o077 != 0
            {
                return Err("unrecognized or unsafe spill artifact; refusing cleanup".into());
            }
            fs::remove_file(entry.path())?;
            removed += 1;
        }
        eprintln!(
            "direct_spill_open directory={} stale_files_removed={removed}",
            directory.display()
        );
        Ok(Self {
            directory,
            _lock: lock,
            transactions: HashMap::new(),
            used: 0,
            limit,
            peak: 0,
            max_active,
            record_limit,
        })
    }
    pub fn start(&mut self, xid: u32) -> Result<()> {
        if xid == 0
            || self.transactions.contains_key(&xid)
            || self.transactions.len() >= self.max_active
        {
            return Err("invalid or excessive active transaction count".into());
        }
        let path = self.directory.join(format!("tx-{xid:010}.bin"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)?;
        self.transactions.insert(
            xid,
            Transaction {
                xid,
                file,
                path,
                rows: 0,
                records: 0,
                bytes: 0,
                sub: HashMap::new(),
            },
        );
        Ok(())
    }
    pub fn observe_sub(&mut self, top: u32, sub: u32) -> Result<()> {
        let tx = self
            .transactions
            .get_mut(&top)
            .ok_or("unknown transaction")?;
        if sub != top && !tx.sub.contains_key(&sub) {
            if tx.sub.len() >= 4096 {
                return Err("too many subtransactions".into());
            }
            tx.sub.insert(
                sub,
                Mark {
                    offset: tx.bytes,
                    rows: tx.rows,
                    records: tx.records,
                },
            );
        }
        Ok(())
    }
    pub fn append(&mut self, top: u32, payload: &str, rows: u32) -> Result<()> {
        if payload.len() > self.record_limit {
            return Err("single change exceeds spill record limit".into());
        }
        let extra = (HEADER + payload.len()) as u64;
        if self.used.checked_add(extra).is_none_or(|n| n > self.limit) {
            return Err("disposable spill byte budget exceeded; WAL not acknowledged".into());
        }
        let tx = self
            .transactions
            .get_mut(&top)
            .ok_or("unknown spill transaction")?;
        let mut header = Vec::with_capacity(HEADER);
        header.extend((payload.len() as u32).to_be_bytes());
        header.extend(rows.to_be_bytes());
        header.extend(tx.records.to_be_bytes());
        header.extend(top.to_be_bytes());
        let mut hash = Sha256::new();
        hash.update(&header);
        hash.update(payload.as_bytes());
        header.extend(hash.finalize());
        tx.file.write_all(&header)?;
        tx.file.write_all(payload.as_bytes())?;
        tx.bytes += extra;
        tx.rows += u64::from(rows);
        tx.records += 1;
        self.used += extra;
        self.peak = self.peak.max(self.used);
        Ok(())
    }
    pub fn abort_sub(&mut self, top: u32, sub: u32) -> Result<()> {
        let tx = self
            .transactions
            .get_mut(&top)
            .ok_or("unknown abort transaction")?;
        if let Some(mark) = tx.sub.get(&sub).copied() {
            tx.file.set_len(mark.offset)?;
            tx.file.seek(SeekFrom::Start(mark.offset))?;
            self.used -= tx.bytes - mark.offset;
            tx.bytes = mark.offset;
            tx.rows = mark.rows;
            tx.records = mark.records;
            tx.sub.retain(|_, m| m.offset < mark.offset);
        }
        Ok(())
    }
    pub fn remove(&mut self, xid: u32) -> Result<()> {
        let tx = self
            .transactions
            .remove(&xid)
            .ok_or("unknown spill transaction")?;
        self.used -= tx.bytes;
        fs::remove_file(&tx.path)?;
        Ok(())
    }
    pub fn replay(&mut self, xid: u32) -> Result<Replay<'_>> {
        let tx = self
            .transactions
            .get_mut(&xid)
            .ok_or("unknown commit transaction")?;
        if tx.file.metadata()?.len() != tx.bytes {
            return Err("spill file length changed".into());
        }
        tx.file.seek(SeekFrom::Start(0))?;
        Ok(Replay {
            tx,
            index: 0,
            rows: 0,
            limit: self.record_limit,
        })
    }
}
impl Drop for Spill {
    fn drop(&mut self) {
        for tx in self.transactions.values() {
            let _ = fs::remove_file(&tx.path);
        }
    }
}
pub struct Replay<'a> {
    tx: &'a mut Transaction,
    index: u64,
    rows: u64,
    limit: usize,
}
impl Replay<'_> {
    pub fn next(&mut self) -> Result<Option<(String, u32)>> {
        if self.index == self.tx.records {
            if self.rows != self.tx.rows || self.tx.file.stream_position()? != self.tx.bytes {
                return Err("spill record totals changed".into());
            }
            return Ok(None);
        }
        let mut h = [0u8; HEADER];
        self.tx.file.read_exact(&mut h)?;
        let length = u32::from_be_bytes(h[0..4].try_into()?) as usize;
        let rows = u32::from_be_bytes(h[4..8].try_into()?);
        if length > self.limit
            || u64::from_be_bytes(h[8..16].try_into()?) != self.index
            || u32::from_be_bytes(h[16..20].try_into()?) != self.tx.xid
        {
            return Err("invalid spill record header".into());
        }
        let mut body = vec![0; length];
        self.tx.file.read_exact(&mut body)?;
        let mut digest = Sha256::new();
        digest.update(&h[..20]);
        digest.update(&body);
        if digest.finalize().as_slice() != &h[20..52] {
            return Err("spill checksum mismatch".into());
        }
        self.index += 1;
        self.rows += u64::from(rows);
        Ok(Some((String::from_utf8(body)?, rows)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    fn base() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "direct-spill-test-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        private_dir(&p).unwrap();
        p
    }
    #[test]
    fn quota_and_subabort_preserve_prefix() {
        let base = base();
        {
            let mut s = Spill::open(&base, "test", 200, 2, 100).unwrap();
            s.start(10).unwrap();
            s.append(10, "[1]", 1).unwrap();
            s.observe_sub(10, 11).unwrap();
            s.append(10, "[2]", 1).unwrap();
            s.observe_sub(10, 12).unwrap();
            s.append(10, "[3]", 1).unwrap();
            assert!(s.append(10, "[4]", 1).is_err());
            s.abort_sub(10, 11).unwrap();
            assert_eq!(s.used, 55);
            s.append(10, "[5]", 1).unwrap();
            let mut replay = s.replay(10).unwrap();
            assert_eq!(replay.next().unwrap(), Some(("[1]".into(), 1)));
            assert_eq!(replay.next().unwrap(), Some(("[5]".into(), 1)));
            assert_eq!(replay.next().unwrap(), None);
            s.remove(10).unwrap();
            assert_eq!(s.used, 0);
        }
        fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn corruption_is_rejected() {
        let base = base();
        {
            let mut s = Spill::open(&base, "test", 1000, 2, 100).unwrap();
            s.start(10).unwrap();
            s.append(10, "[1]", 1).unwrap();
            let tx = s.transactions.get_mut(&10).unwrap();
            tx.file.seek(SeekFrom::Start(53)).unwrap();
            tx.file.write_all(b"2").unwrap();
            assert!(s.replay(10).unwrap().next().is_err());
        }
        fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn ownership_stale_cleanup_and_symlinks() {
        let base = base();
        {
            let mut s = Spill::open(&base, "test", 1000, 1, 100).unwrap();
            assert!(Spill::open(&base, "test", 1000, 1, 100).is_err());
            s.start(10).unwrap();
            assert!(s.start(11).is_err());
            let directory = s.directory.clone();
            drop(s);
            fs::write(directory.join("tx-0000000011.bin"), b"stale").unwrap();
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                directory.join("tx-0000000011.bin"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
            let s = Spill::open(&base, "test", 1000, 1, 100).unwrap();
            assert!(!directory.join("tx-0000000011.bin").exists());
            drop(s);
            std::os::unix::fs::symlink("/tmp", directory.join("tx-0000000012.bin")).unwrap();
            assert!(Spill::open(&base, "test", 1000, 1, 100).is_err());
        }
        fs::remove_dir_all(base).unwrap();
    }
}
