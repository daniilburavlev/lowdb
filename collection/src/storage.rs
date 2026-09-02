use std::{
    path::Path,
    sync::{Arc, atomic::AtomicU64},
};

use common::{DbResult, lookup::Lookup};
use dashmap::DashMap;
use memtable::MemTable;
use sstable::{meta::SSTableMeta, table::SSTable, writer::SSTableWriter};
use tokio::{
    fs::{self},
    sync::Mutex,
};

const TABLES_DIR: &str = "ss";

pub(crate) struct Storage {
    id: AtomicU64,
    tables: DashMap<u32, Vec<SSTable>>,
    lock: Mutex<()>,
}

impl Storage {
    pub(crate) async fn load<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let dir = dir.as_ref().join(TABLES_DIR);
        let mut dir = fs::read_dir(dir).await?;

        let mut id = 0u64;
        let metas = DashMap::new();

        while let Some(entry) = dir.next_entry().await? {
            let filename = entry.file_name();
            let filename = filename.to_string_lossy();
            let (level, curr_id) = get_level(&filename)?;

            let mut tables = metas.entry(level).or_insert(vec![]);
            let meta = SSTableMeta::read(entry.path()).await?;
            let table = SSTable::new(meta)?;

            tables.push(table);
            id = id.max(curr_id);
        }
        Ok(Self {
            id: AtomicU64::new(id),
            tables: metas,
            lock: Mutex::new(()),
        })
    }

    pub(crate) async fn l0(&self, mt: Arc<MemTable>) -> DbResult<()> {
        let path = format!(
            "0_{}",
            self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let mut writer = SSTableWriter::create(&path).await?;
        for (k, v) in mt.iter() {
            writer.add(k, v).await?;
        }
        if let Some(meta) = writer.finish().await? {
            let table = SSTable::new(meta)?;
            self.tables.entry(0).or_insert(vec![]).push(table);
        }
        Ok(())
    }

    pub(crate) async fn get(&self, key: &str) -> DbResult<Lookup> {
        for entry in self.tables.iter() {
            let tables = entry.value();
            for table in tables {
                match table.get(key, u64::MAX).await? {
                    Lookup::Absent => {}
                    lookup => return Ok(lookup),
                }
            }
        }
        Ok(Lookup::Absent)
    }
}

fn get_level(filename: &str) -> DbResult<(u32, u64)> {
    let elements: Vec<&str> = filename.split('_').collect();
    let level: u32 = elements[0].parse()?;
    let id: u64 = elements[1].parse()?;
    Ok((level, id))
}

#[cfg(test)]
mod tests {
    use common::{key::Key, value::Value};
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn load_files_by_levels() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(TABLES_DIR);
        fs::create_dir_all(&path).await.unwrap();
        let mut count = 0;
        for i in 0..3 {
            for _ in 0..10 {
                let path = path.join(format!("{}_{}", i, count));
                let mut writer = SSTableWriter::create(&path).await.unwrap();
                writer
                    .add(&Key::new("key", 1), &Value::Delete)
                    .await
                    .unwrap();
                writer.finish().await.unwrap();
                count += 1;
            }
        }
        let levels = Storage::load(&dir).await.unwrap();

        let id = levels.id;
        let levels = levels.tables;

        assert_eq!(levels.len(), 3);
        for entry in levels.iter() {
            let files = entry.value();
            assert_eq!(files.len(), 10);
        }
        assert_eq!(id.load(std::sync::atomic::Ordering::Relaxed), count - 1);
    }
}
