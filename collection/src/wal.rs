use std::{
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
};

use common::DbResult;
use memtable::MemTable;
use tokio::fs;
use wal::wal::{WalReader, WalWriter};

const WAL_DIR: &str = "wal";

pub(crate) struct Wal {
    path: PathBuf,
    id: AtomicU64,
}

impl Wal {
    pub(crate) async fn new(dir: impl Into<PathBuf>) -> DbResult<Self> {
        let path = dir.into();
        let path = path.join(WAL_DIR);
        let id = latest_id(&path).await? + 1;
        Ok(Self {
            path,
            id: AtomicU64::new(id),
        })
    }

    pub(crate) async fn restore(&self) -> DbResult<Vec<MemTable>> {
        let wals = load_wals(&self.path).await?;
        let tables = restore_tables(wals).await?;
        Ok(tables)
    }

    pub(crate) async fn new_writer(&self) -> DbResult<WalWriter> {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.path.join(format!("{}", id));
        let writer = WalWriter::open(&path).await?;
        Ok(writer)
    }
}

async fn latest_id<P: AsRef<Path>>(dir: P) -> DbResult<u64> {
    let mut id = 0u64;

    let mut dir = fs::read_dir(dir).await?;

    while let Some(entry) = dir.next_entry().await? {
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();
        let curr_id: u64 = filename.parse()?;

        id = id.max(curr_id);
    }
    Ok(id)
}

async fn load_wals<P: AsRef<Path>>(dir: P) -> DbResult<Vec<WalReader>> {
    let mut dir = fs::read_dir(dir).await?;
    let mut wals = vec![];

    while let Some(entry) = dir.next_entry().await? {
        let wal = WalReader::open(entry.path()).await?;
        wals.push(wal);
    }

    Ok(wals)
}

async fn restore_tables(mut wals: Vec<WalReader>) -> DbResult<Vec<MemTable>> {
    let mut tables = vec![];
    for wal in wals.iter_mut() {
        let table = MemTable::default();
        while let Some((key, value)) = wal.next().await? {
            table.set(key, value);
        }
        tables.push(table);
    }
    Ok(tables)
}

#[cfg(test)]
mod tests {
    use common::{key::Key, value::Value};
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
        let tables = wal.restore().await.unwrap();
        assert_eq!(tables.len(), 10);

        let id = wal.id.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(id, 10);
    }
}
