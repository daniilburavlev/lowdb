#![deny(unreachable_pub)]
#![warn(missing_docs)]
//! Simple LSM key-value storage engine
use std::sync::{
    Arc,
    atomic::{
        AtomicBool,
        Ordering::{Acquire, Release},
    },
};

use ::wal::WalCmd;
use common::{DbResult, key::Key, value::Value};
use memtable::MemTable;
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock};

pub mod state;
pub mod storage;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod wal;

use crate::{state::State, storage::DiskStorage, wal::Wal};

/// Inner database storage, allowing make insertions/deletions, flush data to disk
pub struct Storage {
    state: RwLock<Arc<State>>,
    // Used in freeze locks
    state_lock: Mutex<()>,
    flush_lock: Mutex<()>,
    flush_notify: Arc<Notify>,
    merge_notify: Arc<Notify>,
    shutdown: AtomicBool,
    shutdown_notify: Arc<Notify>,
    wal: Wal,
}

impl Storage {
    /// Create new storage instance
    pub async fn new(
        wal: Wal,
        storage: DiskStorage,
        tables: Vec<MemTable>,
        flush_notify: Arc<Notify>,
        merge_notify: Arc<Notify>,
        shutdown_notify: Arc<Notify>,
    ) -> DbResult<Self> {
        let writer = wal.new_writer().await?;
        let state = State::new(writer, tables, storage)?;
        Ok(Self {
            state: RwLock::new(Arc::new(state)),
            state_lock: Mutex::new(()),
            flush_lock: Mutex::new(()),
            flush_notify,
            merge_notify,
            shutdown: AtomicBool::new(false),
            shutdown_notify,
            wal,
        })
    }

    /// Set multiply values at one time
    pub async fn set_batch(&self, tx_id: u64, batch: Vec<(Key, Value)>) -> DbResult<()> {
        let guard = self.state.read().await;
        guard.wal.append(WalCmd::TxBegin(tx_id)).await?;
        guard.wal.append_batch(tx_id, &batch).await?;
        for (k, v) in batch {
            guard.mem_table.put(k, v);
        }
        guard.wal.append(WalCmd::TxCommit(tx_id)).await?;
        Ok(())
    }

    /// Flush one waiter file is notified
    pub fn flush_notify_one(&self) {
        self.flush_notify.notify_one();
    }

    /// Write transaction bigining in WAL file
    pub async fn begin(&self, tx_id: u64) -> DbResult<()> {
        let snapshot = self.snapshot().await;
        snapshot.begin(tx_id).await
    }

    /// Write transaction commitment in WAL file
    pub async fn commit(&self, tx_id: u64) -> DbResult<()> {
        let snapshot = self.snapshot().await;
        snapshot.commit(tx_id).await
    }

    /// Freeze the active memtable if it is full.
    ///
    /// Callers must make sure no write is between its memtable insert and its
    /// publish, or a flush could drop the older, still-visible version.
    pub async fn freeze_if_full(&self) -> DbResult<()> {
        if self.state.read().await.mem_table.is_full() {
            self.try_freeze().await?;
        }
        Ok(())
    }

    /// Freeze the active memtable if it holds anything. Same caller
    /// requirement as [`Storage::freeze_if_full`].
    pub async fn freeze_active(&self) -> DbResult<()> {
        let guard = self.state_lock.lock().await;
        if !self.state.read().await.mem_table.is_empty() {
            self.force_freeze(&guard).await?;
        }
        Ok(())
    }

    /// Get current snapshot
    pub async fn snapshot(&self) -> Arc<State> {
        self.state.read().await.clone()
    }

    /// Get value by key
    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        self.get_snap(key, u64::MAX).await
    }

    /// Get value by key
    pub async fn get_snap(&self, key: &str, seq: u64) -> DbResult<Option<String>> {
        let snapshot = self.snapshot().await;
        snapshot.get_snap(key, seq).await
    }

    pub async fn flush_oldest(&self, read_ts_watermark: u64) -> DbResult<bool> {
        let _guard = self.flush_lock.lock().await;

        let (mt, storage) = {
            let state = self.state.read().await;
            let Some(mt) = state.frozen.last().cloned() else {
                return Ok(false);
            };
            (mt, state.storage.clone())
        };
        storage.l0(&mt, read_ts_watermark).await?;
        self.retire(&mt).await;
        self.wal.remove(mt.id()).await?;

        self.flush_notify.notify_waiters();
        Ok(true)
    }

    pub async fn merge(&self, read_ts_watermark: u64) -> DbResult<()> {
        let storage = self.state.read().await.storage.clone();
        storage.merge(read_ts_watermark).await?;
        Ok(())
    }

    async fn retire(&self, mt: &Arc<MemTable>) {
        let _guard = self.state_lock.lock().await;
        let mut guard = self.state.write().await;
        let mut snapshot = guard.as_ref().clone();

        snapshot.frozen.retain(|frozen| !Arc::ptr_eq(frozen, mt));
        *guard = Arc::new(snapshot);
    }

    /// Shutdown process marker
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Acquire)
    }

    /// Send shutdown event
    pub fn begin_shutdown(&self) {
        self.shutdown.store(true, Release);
        self.shutdown_notify.notify_one();
    }

    async fn try_freeze(&self) -> DbResult<()> {
        let guard = self.state_lock.lock().await;
        let still_full = {
            let s = self.state.read().await;
            s.mem_table.is_full()
        };
        if !still_full {
            return Ok(());
        }
        self.force_freeze(&guard).await
    }

    async fn force_freeze(&self, _g: &MutexGuard<'_, ()>) -> DbResult<()> {
        let new_wal = Arc::new(self.wal.new_writer().await?);
        let id: u64 = new_wal.id().parse()?;
        let new_mt = Arc::new(MemTable::new(id));
        {
            let mut guard = self.state.write().await;
            let mut snapshot = guard.as_ref().clone();

            let _ = std::mem::replace(&mut snapshot.wal, new_wal);
            let old = std::mem::replace(&mut snapshot.mem_table, new_mt);

            snapshot.frozen.insert(0, old);
            *guard = Arc::new(snapshot);
        }
        self.flush_notify.notify_one();
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::testing::{create_storage, drain, names};

    use super::*;

    #[tokio::test]
    async fn background_flusher_drains_frozen_memtables() {
        let (dir, storage) = create_storage().await;
        let storage = Arc::new(storage);

        for i in 0..3 {
            let key = Key(format!("k{i}"), i);
            let value = Value::Set(format!("v{i}"));
            storage.set_batch(1, vec![(key, value)]).await.unwrap();
            let guard = storage.state_lock.lock().await;
            storage.force_freeze(&guard).await.unwrap();
        }
        storage.flush_oldest(0).await.unwrap();
        storage.flush_oldest(0).await.unwrap();
        storage.flush_oldest(0).await.unwrap();
        drain(&storage).await;

        assert_eq!(names(&dir, "ss").await.len(), 3);
        assert_eq!(names(&dir, "wal").await.len(), 1);
        for i in 0..3 {
            assert_eq!(
                storage.get(&format!("k{i}")).await.unwrap(),
                Some(format!("v{i}"))
            );
        }
    }
}
