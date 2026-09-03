use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
};

use common::{DbResult, lookup::Lookup};
use dashmap::DashMap;
use memtable::MemTable;
use sstable::{table::SSTable, writer::SSTableWriter};
use tokio::{
    fs::{self},
    sync::Mutex,
};

const TABLES_DIR: &str = "ss";

pub(crate) struct Storage {
    dir: PathBuf,
    id: AtomicU64,
    /// Tables per level. Within a level they are kept **newest first**, so an L0 lookup
    /// sees the most recent version of a key before any older table that still holds it.
    tables: DashMap<u32, Vec<Arc<SSTable>>>,
    lock: Mutex<()>,
}

impl Storage {
    pub(crate) async fn load<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let dir = dir.as_ref().join(TABLES_DIR);
        fs::create_dir_all(&dir).await?;
        let mut read_dir = fs::read_dir(&dir).await?;

        let mut files = vec![];
        while let Some(entry) = read_dir.next_entry().await? {
            let filename = entry.file_name();
            let filename = filename.to_string_lossy();
            // Skips anything that is not `<level>_<id>`, e.g. a `.sst.tmp` left by a crash.
            if let Some((level, id)) = parse_name(&filename) {
                files.push((level, id, entry.path()));
            }
        }
        // Level ascending, id descending: the iteration order lookups rely on.
        files.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

        let max_id = files.iter().map(|(_, id, _)| *id).max().unwrap_or(0);
        let tables = DashMap::new();
        for (level, _, path) in files {
            let table = SSTable::open(path).await?;
            tables
                .entry(level)
                .or_insert_with(Vec::new)
                .push(Arc::new(table));
        }

        Ok(Self {
            dir,
            // `max_id` names an existing file; the next table must not reuse it.
            id: AtomicU64::new(max_id + 1),
            tables,
            lock: Mutex::new(()),
        })
    }

    pub(crate) async fn l0(&self, mt: Arc<MemTable>) -> DbResult<()> {
        // Serialises flushes so ids and the newest-first level order stay consistent.
        let _guard = self.lock.lock().await;
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = self.dir.join(format!("0_{}", id));

        let mut writer = SSTableWriter::create(&path).await?;
        for (k, v) in mt.iter() {
            writer.add(k, v).await?;
        }
        if let Some(meta) = writer.finish().await? {
            let table = SSTable::new(meta)?;
            self.tables
                .entry(0)
                .or_insert_with(Vec::new)
                .insert(0, Arc::new(table));
        }
        Ok(())
    }

    pub(crate) async fn get(&self, key: &str) -> DbResult<Lookup> {
        let mut levels: Vec<u32> = self.tables.iter().map(|e| *e.key()).collect();
        levels.sort_unstable();

        for level in levels {
            // Clone the handles out before awaiting: holding a DashMap guard across an
            // await would block any concurrent flush touching the same shard.
            let Some(tables) = self.tables.get(&level).map(|e| e.value().clone()) else {
                continue;
            };
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

fn parse_name(filename: &str) -> Option<(u32, u64)> {
    let (level, id) = filename.split_once('_')?;
    Some((level.parse().ok()?, id.parse().ok()?))
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
        // The next table gets a fresh id instead of overwriting the newest one on disk.
        assert_eq!(id.load(std::sync::atomic::Ordering::Relaxed), count);
    }

    #[tokio::test]
    async fn newest_l0_table_wins() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(TABLES_DIR);
        fs::create_dir_all(&path).await.unwrap();

        // Same user key in two L0 tables plus a stale copy at L1.
        for (name, seq, value) in [
            ("0_1", 1u64, "old"),
            ("0_2", 2, "new"),
            ("1_0", 0, "compacted"),
        ] {
            let mut writer = SSTableWriter::create(path.join(name)).await.unwrap();
            writer
                .add(&Key::new("k", seq), &Value::set(value))
                .await
                .unwrap();
            writer.finish().await.unwrap();
        }
        // A crash leftover must not break loading.
        fs::write(path.join("0_9.sst.tmp"), b"junk").await.unwrap();

        let storage = Storage::load(&dir).await.unwrap();
        let Lookup::Found(value) = storage.get("k").await.unwrap() else {
            panic!("key must be found");
        };
        assert_eq!(value, "new");
        assert!(matches!(
            storage.get("missing").await.unwrap(),
            Lookup::Absent
        ));
    }
}
