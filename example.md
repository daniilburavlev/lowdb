# `Transaction` structure: example

This shows how `Transaction` could look under the redesign in `fix.md` (see also `explanation.md`).
It follows the names in the current `transaction` crate. The shared state lives in a new `Oracle`,
which replaces `Lock` and the raw `seq` counter.

> Not compiled or tested. It assumes two `Storage` methods that don't exist yet (see
> [Assumed storage API](#assumed-storage-api)).

```rust
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex as SyncMutex,
        atomic::{AtomicU64, Ordering::{Acquire, Release, Relaxed}},
    },
};

use common::{DbResult, error::DbError, key::Key, value::Value};
use storage::{Storage, state::State};
use tokio::sync::Mutex;

/// Shared by the `DB` and every transaction. Replaces `Lock` + the raw `seq`.
pub struct Oracle {
    /// Next sequence number to hand out. Only touched under `commit_lock`.
    next_seq: AtomicU64,
    /// Highest seq whose writes are fully applied. Snapshots read at this.
    visible: AtomicU64,
    /// Serializes commits; holds key -> last commit_ts for conflict checks.
    commit_lock: Mutex<HashMap<String, u64>>,
    /// Active snapshots: read_ts -> count. Sync mutex so `Drop` can use it.
    active: SyncMutex<BTreeMap<u64, usize>>,
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

    /// Plain `DB::get` reads at this instead of `u64::MAX`.
    pub fn read_ts(&self) -> u64 {
        self.visible.load(Acquire)
    }

    /// Take a snapshot and register it, atomically w.r.t. pruning.
    fn begin(&self) -> u64 {
        let mut active = self.active.lock().unwrap();
        let read_ts = self.visible.load(Acquire);
        *active.entry(read_ts).or_default() += 1;
        read_ts
    }

    fn end(&self, read_ts: u64) {
        let mut active = self.active.lock().unwrap();
        if let Some(n) = active.get_mut(&read_ts) {
            *n -= 1;
            if *n == 0 {
                active.remove(&read_ts);
            }
        }
    }

    /// The one write path. `read_ts == None` = plain write, no conflict check.
    pub async fn commit(
        &self,
        storage: &Storage,
        read_ts: Option<u64>,
        writes: BTreeMap<String, Value>,
    ) -> DbResult<u64> {
        let mut recent = self.commit_lock.lock().await;

        // 1. Conflict check: someone committed k after our snapshot.
        if let Some(read_ts) = read_ts
            && writes.keys().any(|k| recent.get(k).is_some_and(|&ts| ts > read_ts))
        {
            return Err(DbError::CommitConflict);
        }

        // 2. One timestamp for the whole write set.
        let commit_ts = self.next_seq.fetch_add(1, Relaxed);
        let batch: Vec<(Key, Value)> = writes
            .iter()
            .map(|(k, v)| (Key::new(k, commit_ts), v.clone()))
            .collect();

        // 3. One WAL record + memtable insert from the same State.
        //    If this fails nothing is visible: `visible` hasn't moved.
        storage.apply_batch(batch).await?;

        // 4. Record for conflict checks, then publish.
        for k in writes.into_keys() {
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

pub struct Transaction {
    read_ts: u64,
    writes: BTreeMap<String, Value>,
    oracle: Arc<Oracle>,
    storage: Arc<Storage>,
    state: Arc<State>,
    done: bool,
}

impl Transaction {
    pub async fn new(oracle: Arc<Oracle>, storage: Arc<Storage>) -> Self {
        // read_ts first, then the State: every write <= read_ts is already
        // in this snapshot (or flushed to disk).
        let read_ts = oracle.begin();
        let state = storage.snapshot().await;
        Self { read_ts, writes: BTreeMap::new(), oracle, storage, state, done: false }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.writes.insert(key.to_string(), Value::set(value));
    }

    pub fn delete(&mut self, key: &str) {
        self.writes.insert(key.to_string(), Value::Delete);
    }

    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        match self.writes.get(key) {
            Some(Value::Set(v)) => return Ok(Some(v.clone())),
            Some(Value::Delete) => return Ok(None),
            None => {}
        }
        self.state.get_snap(key, self.read_ts).await
    }

    /// Consumes the tx, so a second commit doesn't compile (bug 10).
    pub async fn commit(mut self) -> DbResult<()> {
        let writes = std::mem::take(&mut self.writes);
        if writes.is_empty() {
            return Ok(()); // Drop unregisters
        }
        self.oracle
            .commit(&self.storage, Some(self.read_ts), writes)
            .await
            .map(|_| ())
        // Drop runs on every path (Ok, conflict, IO error) and unregisters (bug 3).
    }

    pub fn rollback(self) {}
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.oracle.end(self.read_ts);
        }
    }
}
```

## Plain writes and reads in `DB`

`DB::set` and `DB::delete` go through the same commit path as a one-key transaction with no
conflict check:

```rust
pub async fn set(&self, key: &str, value: &str) -> DbResult<()> {
    let writes = BTreeMap::from([(key.to_string(), Value::set(value))]);
    self.oracle.commit(&self.storage, None, writes).await.map(|_| ())
}

pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
    self.storage.get_seq(key, self.oracle.read_ts()).await
}
```

## Details that are easy to get wrong

- **`begin` loads `visible` while holding the `active` mutex, and `commit` stores `visible` before
  it reads the minimum from `active`.** Together, these rule out a race where a transaction loads
  `read_ts`, pruning runs before the transaction is registered, and the pruning removes `recent`
  entries that the transaction still needs to check (bug 2).
- **Take `read_ts` before `storage.snapshot()`.** In the other order, a write at or below
  `read_ts` could land in a memtable that was frozen after your `State` snapshot was taken, and
  you'd miss it.
- **The `done` flag isn't strictly needed.** `commit(self)` means `Drop` runs exactly once. It's
  kept so it's easy to add an explicit `end` call later.

## Assumed storage API

- `Storage::apply_batch(Vec<(Key, Value)>)`: writes one `WalCmd::Batch` record and fsyncs, then
  inserts into the memtable from the same `State`.
- `Storage::maybe_freeze()`: the freeze and backpressure step that `Storage::set` does today,
  split out so it can run after the commit lock is released.
