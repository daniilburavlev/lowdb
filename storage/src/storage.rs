//! Accessing persisted files
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use common::{DbResult, key::Key, lookup::Lookup};
use dashmap::DashMap;
use memtable::MemTable;
use sstable::{
    scan::compact,
    table::SSTable,
    writer::{SSTableWriter, tmp_path},
};
use tokio::{
    fs::{self},
    sync::Mutex,
};

const TABLES_DIR: &str = "ss";
const TMP_EXT: &str = "tmp";
const MIN_TABLES: usize = 4;

/// Structure for accessing files on disk
pub struct DiskStorage {
    dir: PathBuf,
    id: AtomicU64,
    max_seq: u64,
    tables: DashMap<u32, Vec<Arc<SSTable>>>,
    lock: Mutex<()>,
}

impl DiskStorage {
    /// Load files from `${dir}/ss` dir
    pub async fn load<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let dir = dir.as_ref().join(TABLES_DIR);
        fs::create_dir_all(&dir).await?;
        let mut read_dir = fs::read_dir(&dir).await?;

        let mut files = vec![];
        let mut stray = vec![];
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            let filename = entry.file_name();
            let filename = filename.to_string_lossy();
            if let Some((level, id)) = parse_name(&filename) {
                files.push((level, id, path))
            } else if path.extension().is_some_and(|e| e == TMP_EXT) {
                stray.push(path);
            }
        }
        for path in stray {
            let _ = fs::remove_file(path).await;
        }
        files.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

        let max_id = files.iter().map(|(_, id, _)| *id).max().unwrap_or(0);
        let tables = DashMap::new();
        let mut max_seq = 0;

        for (level, _, path) in files {
            let table = SSTable::open(path).await?;
            max_seq = max_seq.max(table.meta.largest_seq());
            tables
                .entry(level)
                .or_insert_with(Vec::new)
                .push(Arc::new(table));
        }

        Ok(Self {
            dir,
            id: AtomicU64::new(max_id + 1),
            max_seq,
            tables,
            lock: Mutex::new(()),
        })
    }

    /// Get latest persisted sequence
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Persist memtable to level 0 SSTable
    pub async fn l0(&self, mt: &MemTable, read_ts_watermark: u64) -> DbResult<bool> {
        let _guard = self.lock.lock().await;
        let id = self.id.fetch_add(1, Relaxed);
        let path = self.dir.join(format!("0_{}", id));

        let result = self.write_l0(&path, mt, read_ts_watermark).await;
        if result.is_err() {
            let _ = fs::remove_file(tmp_path(&path)).await;
        }
        result
    }

    async fn write_l0(&self, path: &Path, mt: &MemTable, read_ts_watermark: u64) -> DbResult<bool> {
        let mut writer = SSTableWriter::create(path).await?;
        let mut previous = None::<&Key>;
        for (k, v) in mt.iter() {
            if matches!(previous, Some(key) if key.0 == k.0 && key.1 <= read_ts_watermark) {
                continue;
            }
            writer.add(k, v).await?;
            previous = Some(k);
        }
        let Some(meta) = writer.finish().await? else {
            return Ok(false);
        };
        let table = SSTable::new(meta)?;
        self.tables.entry(0).or_default().insert(0, Arc::new(table));
        Ok(true)
    }

    /// Search value by key
    pub async fn get(&self, key: &str) -> DbResult<Lookup> {
        self.get_snap(key, u64::MAX).await
    }

    /// Search value by key where given seq is greater or equal
    pub async fn get_snap(&self, key: &str, seq: u64) -> DbResult<Lookup> {
        let levels = self.get_levels();
        for level in levels {
            let Some(tables) = self.tables.get(&level).map(|e| e.clone()) else {
                continue;
            };
            for table in tables.iter() {
                match table.get(key, seq).await? {
                    Lookup::Absent => {}
                    lookup => return Ok(lookup),
                }
            }
        }
        Ok(Lookup::Absent)
    }

    /// Merge tables by level
    pub async fn merge(&self, read_ts_watermark: u64) -> DbResult<()> {
        let _lock = self.lock.lock().await;
        let levels = self.get_levels();
        let Some(last) = levels.last() else {
            return Ok(());
        };
        let last = *last;
        for level in levels {
            let Some(tables) = self.tables.get(&level).map(|e| e.clone()) else {
                continue;
            };
            if tables.len() < MIN_TABLES {
                continue;
            }
            let mut scans = Vec::with_capacity(tables.len());
            for table in tables {
                scans.push(table.scan().await?);
            }
            let id = self.id.fetch_add(1, Relaxed);
            let path = format!("{}_{}", level + 1, id);
            let drop_tombstone = level == last;
            compact(path, scans, drop_tombstone, read_ts_watermark).await?;
            self.tables.remove(&level);
        }
        Ok(())
    }

    fn get_levels(&self) -> Vec<u32> {
        let mut levels: Vec<u32> = self.tables.iter().map(|e| *e.key()).collect();
        levels.sort_unstable();
        levels
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
    async fn l0_writes_one_entry_per_user_key() {
        let dir = tempdir().unwrap();
        let storage = DiskStorage::load(dir.path()).await.unwrap();

        let mt = MemTable::new(0);
        mt.put(Key::new("a", 1), Value::set("old"));
        mt.put(Key::new("a", 2), Value::set("new"));
        mt.put(Key::new("b", 3), Value::Delete);

        assert!(storage.l0(&mt, 3).await.unwrap());

        let path = dir.path().join(TABLES_DIR).join("0_1");
        let meta = sstable::meta::SSTableMeta::read(&path).await.unwrap();
        assert_eq!(meta.entries(), 2, "superseded version must be dropped");
        assert_eq!(meta.largest_seq(), 3);
        assert_eq!(meta.smallest_seq(), 2);

        let Lookup::Found(value) = storage.get("a").await.unwrap() else {
            panic!("newest version must survive");
        };
        assert_eq!(value, "new");
        assert!(matches!(storage.get("b").await.unwrap(), Lookup::Deleted));

        // The sequence high-water mark survives a reload without the WAL.
        let reloaded = DiskStorage::load(dir.path()).await.unwrap();
        assert_eq!(reloaded.max_seq(), 3);
    }

    #[tokio::test]
    async fn l0_of_an_empty_memtable_writes_nothing() {
        let dir = tempdir().unwrap();
        let storage = DiskStorage::load(dir.path()).await.unwrap();

        assert!(!storage.l0(&MemTable::new(0), 0).await.unwrap());

        let mut entries = fs::read_dir(dir.path().join(TABLES_DIR)).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "no file, not even a temp one, may be left behind"
        );
    }

    #[tokio::test]
    async fn load_sweeps_stray_temp_files() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(TABLES_DIR);
        fs::create_dir_all(&path).await.unwrap();
        fs::write(path.join("0_7.sst.tmp"), b"half written")
            .await
            .unwrap();

        DiskStorage::load(dir.path()).await.unwrap();

        assert!(!fs::try_exists(path.join("0_7.sst.tmp")).await.unwrap());
    }

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
        let levels = DiskStorage::load(&dir).await.unwrap();

        let id = levels.id;
        let levels = levels.tables;

        assert_eq!(levels.len(), 3);
        for entry in levels.iter() {
            let files = entry.value();
            assert_eq!(files.len(), 10);
        }
        assert_eq!(id.load(std::sync::atomic::Ordering::Relaxed), count);
    }

    #[tokio::test]
    async fn newest_l0_table_wins() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(TABLES_DIR);
        fs::create_dir_all(&path).await.unwrap();

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
        fs::write(path.join("0_9.sst.tmp"), b"junk").await.unwrap();

        let storage = DiskStorage::load(&dir).await.unwrap();
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
