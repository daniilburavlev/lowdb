use std::{
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

use common::DbResult;
use memtable::MemTable;
use tokio::fs;
use wal::wal::{WalReader, WalWriter};

const WAL_DIR: &str = "wal";

pub(crate) struct Restored {
    pub(crate) tables: Vec<MemTable>,
    pub(crate) max_seq: u64,
}

pub(crate) struct Wal {
    path: PathBuf,
    id: AtomicU64,
}

impl Wal {
    pub(crate) async fn new(dir: impl Into<PathBuf>) -> DbResult<Self> {
        let path = dir.into();
        let path = path.join(WAL_DIR);
        fs::create_dir_all(&path).await?;
        let id = latest_id(&path).await? + 1;
        Ok(Self {
            path,
            id: AtomicU64::new(id),
        })
    }

    pub(crate) async fn restore(&self) -> DbResult<Restored> {
        let wals = load_wals(&self.path).await?;
        restore_tables(wals).await
    }

    pub(crate) async fn new_writer(&self) -> DbResult<WalWriter> {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.path.join(format!("{}", id));
        let writer = WalWriter::open(&path).await?;
        Ok(writer)
    }

    /// Drop the log whose memtable has reached disk.
    ///
    /// Only safe once the corresponding SSTable is durable: until then this log
    /// is the only copy of those writes. A crash between the table rename and
    /// this unlink just leaves a redundant log — replaying it re-inserts entries
    /// that carry their original sequence numbers, which shadow the identical
    /// copies already in the table.
    pub(crate) async fn remove(&self, id: u64) -> DbResult<()> {
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

    for wal in wals.iter_mut() {
        let table = MemTable::new(wal.id().parse::<u64>()?);
        while let Some((key, value)) = wal.next().await? {
            max_seq = max_seq.max(key.1);
            table.set(key, value);
        }
        tables.push(table);
    }
    Ok(Restored { tables, max_seq })
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
            for j in 0..100 {
                let key = Key(format!("{}", i * j), i * j);
                let value = Value::Set(format!("{}", i * j));
                writer.append(&key, &value).await.unwrap();
            }
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
            let key = Key::new("k", id + 1);
            writer
                .append(&key, &Value::set(&format!("v{id}")))
                .await
                .unwrap();
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
