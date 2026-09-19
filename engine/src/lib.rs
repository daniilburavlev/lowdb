#![deny(unreachable_pub)]
#![warn(missing_docs)]
//! Simple LSM key-value storage engine
use std::{collections::BTreeMap, path::Path, sync::Arc};

use common::{DbResult, value::Value};
use storage::{Storage, flush_loop, storage::DiskStorage, wal::Wal};
use tokio::{sync::Mutex, task::JoinHandle};
use transaction::{Snapshot, Transaction, oracle::Oracle};

/// DB instance type with the concurrent access to creation/deletion
#[derive(Clone)]
pub struct DB {
    inner: Arc<Storage>,
    flusher: Arc<Mutex<Option<JoinHandle<()>>>>,
    oracle: Arc<Oracle>,
}

impl DB {
    /// Open database with default options
    pub async fn open<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let wal = Wal::new(dir.as_ref()).await?;
        let storage = DiskStorage::load(dir.as_ref()).await?;
        let restored = wal.restore().await?;
        let max_seq = storage.max_seq().max(restored.max_seq);
        let inner = Arc::new(Storage::new(wal, storage, restored.tables).await?);
        let flusher = tokio::spawn(flush_loop(Arc::downgrade(&inner)));
        inner.flush_notify_one();
        Ok(Self {
            inner,
            flusher: Arc::new(Mutex::new(Some(flusher))),
            oracle: Arc::new(Oracle::new(max_seq + 1, restored.max_tx_id + 1)),
        })
    }

    /// Create new transaction
    pub async fn transaction(&self) -> Transaction {
        let oracle = Arc::clone(&self.oracle);
        let storage = Arc::clone(&self.inner);
        Transaction::new(oracle, storage).await
    }

    /// Take a read-only snapshot; every read through it sees the same state
    pub async fn snapshot(&self) -> Snapshot {
        Snapshot::new(Arc::clone(&self.oracle), &self.inner).await
    }

    /// Insert/update new key-value pair
    pub async fn set(&self, key: &str, value: &str) -> DbResult<()> {
        let value = BTreeMap::from([(key.to_string(), Value::set(value))]);
        let tx_id = self.oracle.next_tx_id();
        self.oracle.commit(tx_id, &self.inner, None, value).await?;
        Ok(())
    }

    /// Get latest committed value by key
    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        self.inner.get_snap(key, self.oracle.read_ts()).await
    }

    /// Flush memory values stored in memory to disk
    pub async fn flush(&self) -> DbResult<()> {
        self.oracle.freeze(&self.inner).await?;
        self.inner.flush_frozen().await
    }

    /// Close current instance of database
    pub async fn close(&self) -> DbResult<()> {
        let result = self.flush().await;
        self.inner.begin_shutdown();
        if let Some(handle) = self.flusher.lock().await.take() {
            let _ = handle.await;
        }
        result
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.inner.begin_shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{
        AtomicBool,
        Ordering::{Acquire, Release},
    };

    use ::wal::WalWriter;
    use common::key::Key;
    use sstable::meta::SSTableMeta;
    use storage::testing::{drain, names};
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn flush_moves_the_memtable_to_a_table_and_drops_its_wal() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path()).await.unwrap();

        for i in 0..3 {
            db.set(&format!("k{i}"), &format!("v{i}")).await.unwrap();
        }
        db.flush().await.unwrap();

        assert_eq!(names(&dir, "ss").await.len(), 1, "one table per flush");
        assert_eq!(
            names(&dir, "wal").await.len(),
            1,
            "only the active log survives"
        );

        for i in 0..3 {
            assert_eq!(
                db.get(&format!("k{i}")).await.unwrap(),
                Some(format!("v{i}"))
            );
        }
    }

    #[tokio::test]
    async fn flush_keeps_only_the_newest_version_of_a_key() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path()).await.unwrap();

        for value in ["a", "b", "c"] {
            db.set("k", value).await.unwrap();
        }
        db.set("other", "x").await.unwrap();
        db.close().await.unwrap();

        let tables = names(&dir, "ss").await;
        assert_eq!(tables.len(), 1);
        let meta = SSTableMeta::read(dir.path().join("ss").join(&tables[0]))
            .await
            .unwrap();
        assert_eq!(meta.entries(), 2, "one entry per user key, not per write");

        let db = DB::open(dir.path()).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), Some("c".to_string()));
        assert_eq!(db.get("other").await.unwrap(), Some("x".to_string()));
    }

    #[tokio::test]
    async fn reopen_resumes_the_sequence_above_the_flushed_tables() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path()).await.unwrap();

        db.set("k", "first").await.unwrap();
        db.close().await.unwrap();

        let db = DB::open(dir.path()).await.unwrap();

        db.set("k", "second").await.unwrap();
        db.close().await.unwrap();

        let db = DB::open(dir.path()).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), Some("second".to_string()));
    }

    #[tokio::test]
    async fn a_redundant_wal_after_a_crash_replays_harmlessly() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path()).await.unwrap();
        db.set("k", "value").await.unwrap();
        db.flush().await.unwrap();

        let wal = WalWriter::open(dir.path().join("wal").join("1"))
            .await
            .unwrap();
        wal.append_kv(1, &Key::new("k", 1), &Value::set("value"))
            .await
            .unwrap();
        drop(wal);
        db.close().await.unwrap();

        let db = DB::open(dir.path()).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), Some("value".to_string()));
        drain(&db.inner).await;
        assert_eq!(db.get("k").await.unwrap(), Some("value".to_string()));
    }

    #[tokio::test]
    async fn snapshot_keeps_seeing_old_versions_across_writes_and_flush() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path()).await.unwrap();

        db.set("k", "old").await.unwrap();
        let snap = db.snapshot().await;

        db.set("k", "new").await.unwrap();
        db.set("added", "x").await.unwrap();
        // The flush keeps only `k@new`; the snapshot must still see `k@old`.
        db.flush().await.unwrap();

        assert_eq!(snap.get("k").await.unwrap(), Some("old".to_string()));
        assert_eq!(snap.get("added").await.unwrap(), None);
        assert_eq!(db.get("k").await.unwrap(), Some("new".to_string()));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn keys_stay_readable_across_a_flush() {
        const KEYS: usize = 100;

        let dir = tempdir().unwrap();
        let db = Arc::new(DB::open(dir.path()).await.unwrap());
        for i in 0..KEYS {
            db.set(&format!("k{i:03}"), &format!("v{i}")).await.unwrap();
        }

        let done = Arc::new(AtomicBool::new(false));
        let reader = tokio::spawn({
            let (db, done) = (db.clone(), done.clone());
            async move {
                while !done.load(Acquire) {
                    for i in 0..KEYS {
                        let got = db.get(&format!("k{i:03}")).await.unwrap();
                        assert_eq!(
                            got,
                            Some(format!("v{i}")),
                            "k{i:03} was invisible during a flush"
                        );
                    }
                }
            }
        });

        for _ in 0..3 {
            for i in 0..KEYS {
                db.set(&format!("k{i:03}"), &format!("v{i}")).await.unwrap();
            }
            db.flush().await.unwrap();
        }
        done.store(true, Release);
        reader.await.unwrap();
    }
}
