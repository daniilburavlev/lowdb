#![deny(unreachable_pub)]
#![warn(missing_docs)]
//! Simple LSM key-value storage engine
use std::{
    sync::{
        Arc, Weak,
        atomic::{
            AtomicBool,
            Ordering::{Acquire, Release},
        },
    },
    time::Duration,
};

use common::{DbResult, key::Key, value::Value};
use memtable::MemTable;
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock};

pub mod state;
pub mod storage;
mod tables;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod wal;

use crate::{state::State, storage::DiskStorage, wal::Wal};

const MAX_FROZEN: usize = 4;
const BACKPRESSURE_POLL: Duration = Duration::from_millis(50);
const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(5);

/// Inner database storage, allowing make insertions/deletions, flush data to disk
pub struct Storage {
    state: RwLock<Arc<State>>,
    state_lock: Mutex<()>,
    flush_lock: Mutex<()>,
    flush_notify: Notify,
    flushed_notify: Notify,
    shutdown: AtomicBool,
    shutdown_notify: Notify,
    wal: Wal,
}

impl Storage {
    /// Create new storage instance
    pub async fn new(wal: Wal, storage: DiskStorage, tables: Vec<MemTable>) -> DbResult<Self> {
        let writer = wal.new_writer().await?;
        let state = State::new(writer, tables, storage)?;
        Ok(Self {
            state: RwLock::new(Arc::new(state)),
            state_lock: Mutex::new(()),
            flush_lock: Mutex::new(()),
            flush_notify: Notify::default(),
            flushed_notify: Notify::default(),
            shutdown: AtomicBool::new(false),
            shutdown_notify: Notify::default(),
            wal,
        })
    }

    /// Set multiply values at one time
    pub async fn set_batch(&self, batch: Vec<(Key, Value)>) -> DbResult<()> {
        let guard = self.state.read().await;
        guard.wal.append_batch(&batch).await?;
        for (k, v) in batch {
            guard.mem_table.put(k, v);
        }
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

    /// Check current mem_table is full and replace with new one
    pub async fn maybe_freeze(&self) -> DbResult<()> {
        if self.state.read().await.mem_table.is_full() {
            self.try_freeze().await?;
            self.await_flush_capacity().await;
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

    /// Flush all frozen tables to disk
    pub async fn flush_all(&self) -> DbResult<()> {
        {
            let guard = self.state_lock.lock().await;
            if !self.state.read().await.mem_table.is_empty() {
                self.force_freeze(&guard).await?;
            }
        }
        while self.flush_oldest().await? {}
        Ok(())
    }

    async fn flush_oldest(&self) -> DbResult<bool> {
        let _guard = self.flush_lock.lock().await;

        let (mt, storage) = {
            let state = self.state.read().await;
            let Some(mt) = state.frozen.last().cloned() else {
                return Ok(false);
            };
            (mt, state.storage.clone())
        };

        storage.l0(&mt).await?;
        self.retire(&mt).await;
        self.wal.remove(mt.id()).await?;

        self.flush_notify.notify_waiters();
        Ok(true)
    }

    async fn retire(&self, mt: &Arc<MemTable>) {
        let _guard = self.state_lock.lock().await;
        let mut guard = self.state.write().await;
        let mut snapshot = guard.as_ref().clone();

        snapshot.frozen.retain(|frozen| !Arc::ptr_eq(frozen, mt));
        *guard = Arc::new(snapshot);
    }

    async fn await_flush_capacity(&self) {
        loop {
            if self.state.read().await.frozen.len() <= MAX_FROZEN {
                return;
            }
            self.flush_notify.notify_one();
            let _ = tokio::time::timeout(BACKPRESSURE_POLL, self.flushed_notify.notified()).await;
        }
    }

    fn is_shutdown(&self) -> bool {
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

/// Run flush loop
pub async fn flush_loop(inner: Weak<Storage>) {
    let mut retry = RETRY_MIN;
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };

        match inner.flush_oldest().await {
            Ok(true) => {
                retry = RETRY_MIN;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::error!("flush failed: {e}");
                if inner.is_shutdown() {
                    return;
                }
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(RETRY_MAX);
                continue;
            }
        }
        if inner.is_shutdown() {
            return;
        }
        tokio::select! {
            _ = inner.flush_notify.notified() => {}
            _ = inner.shutdown_notify.notified() => {}
        }
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
        tokio::spawn(flush_loop(Arc::downgrade(&storage)));

        for i in 0..3 {
            let key = Key(format!("k{i}"), i);
            let value = Value::Set(format!("v{i}"));
            storage.set_batch(vec![(key, value)]).await.unwrap();
            let guard = storage.state_lock.lock().await;
            storage.force_freeze(&guard).await.unwrap();
        }
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
