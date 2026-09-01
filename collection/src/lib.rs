use std::{
    path::Path,
    sync::{Arc, atomic::AtomicU64},
};

use chrono::Utc;
use common::{DbResult, key::Key, value::Value};
use memtable::MemTable;
use tokio::sync::{Mutex, MutexGuard, RwLock};

use crate::{state::State, storage::Storage, wal::Wal};

mod state;
mod storage;
mod wal;

pub struct Collection {
    seq: AtomicU64,
    state: RwLock<Arc<State>>,
    state_lock: Mutex<()>,
    flush_notify: tokio::sync::Notify,
    wal: Wal,
}

impl Collection {
    pub async fn open<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let timestamp = Utc::now().timestamp() as u64;
        let wal = Wal::new(dir.as_ref()).await?;
        let tables = wal.restore().await?;
        let writer = wal.new_writer().await?;
        let storage = Storage::load(dir.as_ref()).await?;
        let state = State::new(writer, tables, storage);
        Ok(Self {
            seq: AtomicU64::new(timestamp),
            state: RwLock::new(Arc::new(state)),
            state_lock: Mutex::new(()),
            flush_notify: tokio::sync::Notify::default(),
            wal,
        })
    }

    pub async fn set(&self, key: &str, value: &str) -> DbResult<()> {
        let seq = self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let key = Key::new(key, seq);
        let value = Value::set(value);

        let is_full = {
            let guard = self.state.read().await;
            guard.wal.append(&key, &value).await?;
            guard.mem_table.set(key, value);
            guard.mem_table.is_full()
        };
        if is_full {
            self.try_freeze().await?;
        }
        Ok(())
    }

    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        let snapshot = self.state.read().await.clone();
        if let Some(v) = snapshot.mem_table.get(key) {
            return Ok(Some(v));
        }
        for mt in &snapshot.frozen {
            if let Some(v) = mt.get(key) {
                return Ok(Some(v));
            }
        }
        snapshot.storage.get(key).await
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
        let new_mt = Arc::new(MemTable::default());
        let new_wal = Arc::new(self.wal.new_writer().await?);
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
