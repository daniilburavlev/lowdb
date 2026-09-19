//! Wal wrapper, allowing to restore state after fall, generate new sequenced writer
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

use common::{DbResult, error::DbError};
use memtable::MemTable;
use tokio::fs;
use wal::{WalCmd, WalReader, WalWriter};

const WAL_DIR: &str = "wal";

/// Restored state
pub struct Restored {
    /// Restored memory tables
    pub tables: Vec<MemTable>,
    /// Latest sequence found in WAL
    pub max_seq: u64,
    /// Latest commited transaction's id
    pub max_tx_id: u64,
}

/// Main WAL manager
pub struct Wal {
    path: PathBuf,
    id: AtomicU64,
}

impl Wal {
    /// Create or open given '${dir}/wal' path to existign WAL files, get latest sequence number
    pub async fn new(dir: impl Into<PathBuf>) -> DbResult<Self> {
        let path = dir.into();
        let path = path.join(WAL_DIR);
        fs::create_dir_all(&path).await?;
        let id = latest_id(&path).await? + 1;
        Ok(Self {
            path,
            id: AtomicU64::new(id),
        })
    }

    /// Restore all memtables
    pub async fn restore(&self) -> DbResult<Restored> {
        let wals = load_wals(&self.path).await?;
        restore_tables(wals).await
    }

    /// Create new WAL file, returns writer
    pub async fn new_writer(&self) -> DbResult<WalWriter> {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.path.join(format!("{}", id));
        let writer = WalWriter::open(&path).await?;
        Ok(writer)
    }

    /// Remove WAL file by id
    pub async fn remove(&self, id: u64) -> DbResult<()> {
        match fs::remove_file(self.path.join(format!("{}", id))).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

async fn wal_ids<P: AsRef<Path>>(dir: P) -> DbResult<Vec<u64>> {
    let mut ids = vec![];
    let mut dir = fs::read_dir(dir).await?;

    while let Some(entry) = dir.next_entry().await? {
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();

        if let Ok(id) = filename.parse::<u64>() {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

async fn latest_id<P: AsRef<Path>>(path: P) -> DbResult<u64> {
    let ids = wal_ids(path).await?;
    Ok(ids.last().copied().unwrap_or(0))
}

async fn load_wals<P: AsRef<Path>>(dir: P) -> DbResult<Vec<WalReader>> {
    let dir = dir.as_ref();
    let mut wals = vec![];

    for id in wal_ids(dir).await?.into_iter().rev() {
        let wal = WalReader::open(dir.join(format!("{}", id))).await?;
        wals.push(wal);
    }

    Ok(wals)
}

async fn restore_tables(mut wals: Vec<WalReader>) -> DbResult<Restored> {
    let mut tables = vec![];
    let mut max_seq = 0;
    let mut max_tx_id = 0;

    for wal in wals.iter_mut() {
        let table = MemTable::new(wal.id().parse::<u64>()?);
        let mut txs = HashMap::<u64, MemTable>::new();
        while let Some(cmd) = wal.next().await? {
            match cmd {
                WalCmd::TxBegin(tx_id) => {
                    max_tx_id = max_tx_id.max(tx_id);
                    if txs.insert(tx_id, MemTable::new(0)).is_some() {
                        return Err(DbError::InvalidState(format!(
                            "corrupted WAL; duplicate transaction id: '{tx_id}'"
                        )));
                    }
                }
                WalCmd::TxCommit(tx_id) => {
                    let Some(values) = txs.remove(&tx_id) else {
                        return Err(DbError::InvalidState(format!(
                            "corrupted WAL; unknown transaction commit: '{tx_id}'"
                        )));
                    };
                    for (k, v) in values.iter() {
                        table.put(k.clone(), v.clone());
                    }
                }
                WalCmd::Op(tx_id, key, value) => {
                    if let Some(values) = txs.get_mut(&tx_id) {
                        max_seq = max_seq.max(key.1);
                        values.put(key, value);
                    }
                }
                WalCmd::Batch(tx_id, batch) => {
                    if let Some(values) = txs.get_mut(&tx_id) {
                        for (key, value) in batch {
                            max_seq = max_seq.max(key.1);
                            values.put(key, value);
                        }
                    }
                }
            }
        }
        tables.push(table);
    }
    Ok(Restored {
        tables,
        max_seq,
        max_tx_id,
    })
}

#[cfg(test)]
mod tests {
    use common::{key::Key, lookup::Lookup, value::Value};
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn restore_tables() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf().join(WAL_DIR);
        fs::create_dir_all(&path).await.unwrap();

        for i in 0..10 {
            let path = path.join(format!("{}", i));
            let writer = WalWriter::open(&path).await.unwrap();
            writer.append(WalCmd::TxBegin(1)).await.unwrap();
            for j in 0..100 {
                let key = Key(format!("{}", i * j), i * j);
                let value = Value::Set(format!("{}", i * j));
                writer.append_kv(1, &key, &value).await.unwrap();
            }
            writer.append(WalCmd::TxCommit(1)).await.unwrap();
        }
        let wal = Wal::new(&dir.path()).await.unwrap();
        let restored = wal.restore().await.unwrap();
        assert_eq!(restored.tables.len(), 10);
        assert_eq!(restored.max_seq, 9 * 99);

        let id = wal.id.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(id, 10);
    }

    #[tokio::test]
    async fn restore_is_newest_log_first() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf().join(WAL_DIR);
        fs::create_dir_all(&path).await.unwrap();

        for id in 0..3u64 {
            let writer = WalWriter::open(path.join(format!("{}", id))).await.unwrap();
            writer.append(WalCmd::TxBegin(1)).await.unwrap();
            let key = Key::new("k", id + 1);
            writer
                .append_kv(1, &key, &Value::set(&format!("v{id}")))
                .await
                .unwrap();
            writer.append(WalCmd::TxCommit(1)).await.unwrap();
        }
        fs::write(path.join("3.tmp"), b"junk").await.unwrap();

        let wal = Wal::new(&dir.path()).await.unwrap();
        let restored = wal.restore().await.unwrap();

        assert_eq!(restored.tables.len(), 3);
        assert_eq!(restored.max_seq, 3);

        let Lookup::Found(value) = restored.tables[0].get("k") else {
            panic!("newest log must come first");
        };
        assert_eq!(value, "v2");
        assert_eq!(wal.id.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn creates_missing_dir() {
        let dir = tempdir().unwrap();
        let wal = Wal::new(dir.path().join("fresh")).await.unwrap();
        assert!(wal.restore().await.unwrap().tables.is_empty());
        wal.new_writer().await.unwrap();
    }
}
