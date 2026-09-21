#![deny(unreachable_pub)]
#![warn(missing_docs)]
//! Simple LSM key-value storage engine
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Weak},
    time::Duration,
};

use common::{DbResult, value::Value};
use storage::{Storage, storage::DiskStorage, wal::Wal};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use transaction::{Snapshot, Transaction, oracle::Oracle};

const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// DB instance type with the concurrent access to creation/deletion
///
/// # Example
/// ```rust
/// use tempfile::tempdir;
/// use engine::DB;
///
/// #[tokio::main]
/// async fn main() {
///     let dir = tempdir().unwrap();
///     let db = DB::open(dir.path()).await.unwrap();
/// }
/// ```
#[derive(Clone)]
pub struct DB {
    inner: Arc<Storage>,
    oracle: Arc<Oracle>,
    flusher: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl DB {
    /// Open database with default options
    ///
    /// 1. Load WAL files from `${dir}/wal` directory, restore max_seq, max_tx_id, mem_tables
    /// 2. Load sstables' metadata from `${dir}/ss` directory, restore max_seq
    /// 3. Run background flusher loop
    pub async fn open<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let wal = Wal::new(dir.as_ref()).await?;
        let restored = wal.restore().await?;

        let storage = DiskStorage::load(dir.as_ref()).await?;

        let max_seq = storage.max_seq().max(restored.max_seq);
        let flush_notify = Arc::new(Notify::new());
        let merge_notify = Arc::new(Notify::new());
        let shutdown_notify = Arc::new(Notify::new());
        let inner = Arc::new(
            Storage::new(
                wal,
                storage,
                restored.tables,
                Arc::clone(&flush_notify),
                Arc::clone(&merge_notify),
                Arc::clone(&shutdown_notify),
            )
            .await?,
        );
        let oracle = Arc::new(Oracle::new(max_seq + 1, restored.max_tx_id + 1));

        let flusher = tokio::spawn(flush_loop(
            Arc::downgrade(&inner),
            Arc::clone(&oracle),
            flush_notify,
            shutdown_notify,
        ));
        inner.flush_notify_one();

        Ok(Self {
            inner,
            flusher: Arc::new(Mutex::new(Some(flusher))),
            oracle,
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
    pub async fn flush(&self) -> DbResult<bool> {
        let read_ts_watermark = self.oracle.watermark();
        self.oracle.freeze(&self.inner).await?;
        self.inner.flush_oldest(read_ts_watermark).await
    }

    /// Close current instance of database
    pub async fn close(&self) -> DbResult<bool> {
        let result = self.flush().await;
        self.inner.begin_shutdown();
        if let Some(handle) = self.flusher.lock().await.take() {
            let _ = handle.await;
        }
        result
    }
}

async fn flush_loop(
    storage: Weak<Storage>,
    oracle: Arc<Oracle>,
    flush_notify: Arc<Notify>,
    shutdown_notify: Arc<Notify>,
) {
    let mut retry = RETRY_MIN;
    loop {
        let Some(storage) = storage.upgrade() else {
            return;
        };
        let read_ts_watermark = oracle.watermark();
        match storage.flush_oldest(read_ts_watermark).await {
            Ok(true) => {
                retry = RETRY_MIN;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::error!("flush failed: {e}");
                if storage.is_shutdown() {
                    return;
                }
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(RETRY_MAX);
                continue;
            }
        }
        if storage.is_shutdown() {
            return;
        }
        tokio::select! {
            _ = flush_notify.notified() => {}
            _ = shutdown_notify.notified() => {}
        }
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
