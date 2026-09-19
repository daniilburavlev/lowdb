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
    next_seq: AtomicU64,
    visible: AtomicU64,
    commit_lock: Mutex<HashMap<String, u64>>,
    active: SyncMutex<BTreeMap<u64, u64>>,
}

impl Oracle {
    pub fn new(max_seq: u64) -> Self {
        Self {
            next_seq: AtomicU64::new(max_seq + 1),
            visible: AtomicU64::new(max_seq),
            commit_lock: Mutex::new(HashMap::new()),
            active: SyncMutex::new(BTreeMap::new()),
        }
    }

    pub fn read_ts(&self) -> u64 {
        self.visible.load(Acquire)
    }

    pub(crate) fn begin(&self) -> u64 {
        let mut active = self.active.lock().unwrap();
        let read_ts = self.visible.load(Acquire);
        *active.entry(read_ts).or_default() += 1;
        read_ts
    }

    pub(crate) fn end(&self, read_ts: u64) {
        let mut active = self.active.lock().unwrap();
        if let Some(n) = active.get_mut(&read_ts) {
            *n -= 1;
            if *n == 0 {
                active.remove(&read_ts);
            }
        }
    }

    /// `read_ts == Non` = plain write, no conflict check
    pub async fn commit(
        &self,
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
        storage.apply_batch(batch).await?;

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

        drop(recent);
        // Freeze / backpressure wait goes here, outside the commit lock.
        storage.maybe_freeze().await?;
        Ok(commit_ts)
    }
}
