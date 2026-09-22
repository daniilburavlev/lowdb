use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Mutex as SyncMutex,
        atomic::{
            AtomicU64,
            Ordering::{Acquire, Relaxed, Release},
        },
    },
};

use common::{DbResult, error::DbError, key::Key, value::Value};
use storage::Storage;
use tokio::sync::Mutex;

/// Shared by the `DB` and every transactions.
pub struct Oracle {
    // Globaly used sequence number
    next_seq: AtomicU64,
    // Current commited visible sequence
    visible: AtomicU64,
    // Global incremental tx_id
    tx_id: AtomicU64,
    commit_lock: Mutex<HashMap<String, u64>>,
    // Counter by visibility sequence
    active: SyncMutex<BTreeMap<u64, u64>>,
}

impl Oracle {
    pub fn new(max_seq: u64, tx_id: u64) -> Self {
        Self {
            tx_id: AtomicU64::new(tx_id),
            next_seq: AtomicU64::new(max_seq + 1),
            visible: AtomicU64::new(max_seq),
            commit_lock: Mutex::new(HashMap::new()),
            active: SyncMutex::new(BTreeMap::new()),
        }
    }

    pub fn next_tx_id(&self) -> u64 {
        self.tx_id.fetch_add(1, Relaxed)
    }

    pub fn read_ts(&self) -> u64 {
        self.visible.load(Acquire)
    }

    pub fn watermark(&self) -> u64 {
        let active = self.active.lock().unwrap();
        let commit_ts = self.visible.load(Relaxed);
        active.first_key_value().map_or(commit_ts, |(&ts, _)| ts)
    }

    /// Increment current visibility sequnce counter
    pub(crate) fn begin(&self) -> u64 {
        let mut active = self.active.lock().unwrap();
        let read_ts = self.visible.load(Acquire);
        *active.entry(read_ts).or_default() += 1;
        read_ts
    }

    /// Decrement transaction's visibility sequence
    pub(crate) fn end(&self, read_ts: u64) {
        let mut active = self.active.lock().unwrap();
        if let Some(n) = active.get_mut(&read_ts) {
            *n -= 1;
            if *n == 0 {
                active.remove(&read_ts);
            }
        }
    }

    /// If `read_ts == None` = plain write, no conflict check
    pub async fn commit(
        &self,
        tx_id: u64,
        storage: &Storage,
        read_ts: Option<u64>,
        buffer: BTreeMap<String, Value>,
    ) -> DbResult<u64> {
        let mut recent = self.commit_lock.lock().await;
        // 1. Conflict check: someone committed k after snapshot.
        if let Some(read_ts) = read_ts
            && buffer
                .keys()
                .any(|k| recent.get(k).is_some_and(|&ts| ts > read_ts))
        {
            return Err(DbError::CommitConflict);
        }
        // 2. One timestamp for the whole write set.
        let commit_ts = self.next_seq.fetch_add(1, Relaxed);
        let batch: Vec<(Key, Value)> = buffer
            .iter()
            .map(|(k, v)| (Key::new(k, commit_ts), v.clone()))
            .collect();

        // 3. One WAL record + memtable insert from the same State.
        storage.set_batch(tx_id, batch).await?;

        // 4. Record for conflict checks, the publish.
        for k in buffer.into_keys() {
            recent.insert(k, commit_ts);
        }
        self.visible.store(commit_ts, Release);

        // Prune after publishing: a tx that begins later gets read_ts >= commit_ts.
        let watermark = {
            let active = self.active.lock().unwrap();
            active.first_key_value().map_or(commit_ts, |(&ts, _)| ts)
        };
        recent.retain(|_, ts| *ts > watermark);

        // Freeze under the commit lock so a frozen memtable never holds an
        // unpublished write; the backpressure wait happens outside it.
        storage.freeze_if_full().await?;
        drop(recent);
        Ok(commit_ts)
    }

    /// Freeze the active memtable while no commit is in flight, so a flush
    /// cannot drop a visible version in favour of an unpublished one.
    pub async fn freeze(&self, storage: &Storage) -> DbResult<()> {
        let _recent = self.commit_lock.lock().await;
        storage.freeze_active().await
    }
}
