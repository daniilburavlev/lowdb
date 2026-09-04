use std::{
    path::Path,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use common::{DbResult, key::Key, lookup::Lookup, value::Value};
use memtable::MemTable;
use tokio::{
    sync::{Mutex, MutexGuard, Notify, RwLock},
    task::JoinHandle,
};

use crate::{state::State, storage::Storage, wal::Wal};

mod state;
mod storage;
mod tables;
mod wal;

/// Frozen memtables tolerated before writers are held back. Without a cap a
/// writer faster than the disk turns `State::frozen` into an unbounded leak.
const MAX_FROZEN: usize = 4;
/// How long a held-back writer waits before re-checking. Polling rather than
/// waiting purely on the notify keeps a flusher that is backing off after an
/// I/O error from wedging writers on a missed wake-up.
const BACKPRESSURE_POLL: Duration = Duration::from_millis(50);
/// Backoff bounds for a failing flush.
const RETRY_MIN: Duration = Duration::from_millis(50);
const RETRY_MAX: Duration = Duration::from_secs(5);

pub struct Collection {
    inner: Arc<Inner>,
    flusher: Mutex<Option<JoinHandle<()>>>,
}

impl Collection {
    pub async fn open<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let inner = Arc::new(Inner::open(dir).await?);
        let flusher = tokio::spawn(flush_loop(Arc::downgrade(&inner)));
        // Everything replayed from the WAL is frozen already; get it to disk.
        inner.flush_notify.notify_one();
        Ok(Self {
            inner,
            flusher: Mutex::new(Some(flusher)),
        })
    }

    pub async fn set(&self, key: &str, value: &str) -> DbResult<()> {
        self.inner.set(key, value).await
    }

    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        self.inner.get(key).await
    }

    /// Freeze the active memtable and wait until every frozen memtable is on
    /// disk and its WAL is gone.
    pub async fn flush(&self) -> DbResult<()> {
        self.inner.flush_all().await
    }

    /// Flush what is pending, then stop the background flusher and wait for it.
    ///
    /// Dropping a `Collection` without calling this also stops the flusher, but
    /// cannot wait for the memtable it is in the middle of writing.
    pub async fn close(&self) -> DbResult<()> {
        let result = self.inner.flush_all().await;
        self.inner.begin_shutdown();
        if let Some(handle) = self.flusher.lock().await.take() {
            let _ = handle.await;
        }
        result
    }
}

impl Drop for Collection {
    fn drop(&mut self) {
        // The flush task holds an `Arc<Inner>` while it works, so it — not this
        // `Drop` — releases the state. Signalling is all that is needed.
        self.inner.begin_shutdown();
    }
}

struct Inner {
    seq: AtomicU64,
    state: RwLock<Arc<State>>,
    /// Serializes freezes and retirements, i.e. every `State` mutation.
    state_lock: Mutex<()>,
    /// Serializes flushes, so two flushers cannot pick the same memtable.
    flush_lock: Mutex<()>,
    flush_notify: Notify,
    /// Signalled after each completed flush, to release held-back writers.
    flushed_notify: Notify,
    shutdown: AtomicBool,
    shutdown_notify: Notify,
    wal: Wal,
}

impl Inner {
    async fn open<P: AsRef<Path>>(dir: P) -> DbResult<Self> {
        let wal = Wal::new(dir.as_ref()).await?;
        let restored = wal.restore().await?;
        let writer = wal.new_writer().await?;
        let storage = Storage::load(dir.as_ref()).await?;
        // A flushed WAL file is deleted, so the log alone no longer holds the
        // sequence high-water mark — the tables carry the rest of it. Reissuing
        // a sequence that already exists on disk would make new writes sort
        // *older* than the data they replace.
        let max_seq = restored.max_seq.max(storage.max_seq());
        let state = State::new(writer, restored.tables, storage)?;
        Ok(Self {
            seq: AtomicU64::new(max_seq + 1),
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

    async fn set(&self, key: &str, value: &str) -> DbResult<()> {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
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
            self.await_flush_capacity().await;
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> DbResult<Option<String>> {
        let snapshot = self.state.read().await.clone();
        match snapshot.mem_table.get(key) {
            Lookup::Found(value) => return Ok(Some(value)),
            Lookup::Deleted => return Ok(None),
            _ => {}
        }
        for mt in &snapshot.frozen {
            match mt.get(key) {
                Lookup::Found(value) => return Ok(Some(value)),
                Lookup::Deleted => return Ok(None),
                _ => {}
            }
        }
        match snapshot.storage.get(key).await? {
            Lookup::Found(value) => Ok(Some(value)),
            _ => Ok(None),
        }
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

            // Index 0 is the newest frozen memtable, the last element the
            // oldest. Reads walk from the head, flushes consume from the tail.
            snapshot.frozen.insert(0, old);
            *guard = Arc::new(snapshot);
        }
        self.flush_notify.notify_one();
        Ok(())
    }

    /// Freeze whatever is in the active memtable and drain the frozen list.
    async fn flush_all(&self) -> DbResult<()> {
        {
            let guard = self.state_lock.lock().await;
            if !self.state.read().await.mem_table.is_empty() {
                self.force_freeze(&guard).await?;
            }
        }
        while self.flush_oldest().await? {}
        Ok(())
    }

    /// Write the oldest frozen memtable out as a level-0 table, then retire the
    /// memtable and delete the WAL it came from.
    ///
    /// Returns `false` when nothing is frozen. On error the memtable stays in
    /// `frozen` and its WAL stays on disk, so no write is ever lost — the flush
    /// can simply be retried.
    async fn flush_oldest(&self) -> DbResult<bool> {
        let _guard = self.flush_lock.lock().await;

        // Oldest first: WAL files may only be removed in id order, and the
        // oldest frozen memtable owns the oldest surviving log.
        let (mt, storage) = {
            let state = self.state.read().await;
            let Some(mt) = state.frozen.last().cloned() else {
                return Ok(false);
            };
            (mt, state.storage.clone())
        };

        // The state lock is deliberately not held across this: writing a table
        // is seconds-scale I/O and `state_lock` serializes every freeze.
        storage.l0(&mt).await?;
        // Only now, with the table durable and published, may the memtable go.
        // During the overlap the key is found twice and the memtable copy wins,
        // which is correct — it is the same data. The reverse order would leave
        // the key invisible for the length of that window.
        self.retire(&mt).await;
        self.wal.remove(mt.id()).await?;

        self.flushed_notify.notify_waiters();
        Ok(true)
    }

    /// Drop a flushed memtable from `frozen`, copy-on-write.
    async fn retire(&self, mt: &Arc<MemTable>) {
        let _guard = self.state_lock.lock().await;
        let mut guard = self.state.write().await;
        let mut snapshot = guard.as_ref().clone();
        // By identity, never by index: `force_freeze` may have pushed new
        // entries at the head while this one was being written out.
        snapshot.frozen.retain(|frozen| !Arc::ptr_eq(frozen, mt));
        *guard = Arc::new(snapshot);
        // Readers holding the previous `Arc<State>` keep seeing the memtable
        // until they drop it. That is safe: it is the data just installed.
    }

    /// Hold a writer back while the flusher is behind.
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
        self.shutdown.load(Ordering::Acquire)
    }

    fn begin_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // `notify_one` leaves a permit if the flusher is mid-flush, so the
        // signal cannot be missed.
        self.shutdown_notify.notify_one();
    }
}

/// Background flusher: one frozen memtable at a time, oldest first.
async fn flush_loop(inner: Weak<Inner>) {
    let mut retry = RETRY_MIN;
    loop {
        let Some(inner) = inner.upgrade() else {
            return;
        };

        match inner.flush_oldest().await {
            // `notify_one` coalesces, so one wake-up does not mean one
            // memtable: keep going until the list is actually empty.
            Ok(true) => {
                retry = RETRY_MIN;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                // Nothing was dropped, so the data is still safe on the WAL.
                eprintln!("lowdb: flush failed: {e}");
                if inner.is_shutdown() {
                    return;
                }
                tokio::time::sleep(retry).await;
                retry = (retry * 2).min(RETRY_MAX);
                continue;
            }
        }

        // Nothing left to do — shut down here, having drained the list.
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
    use std::sync::atomic::AtomicBool;

    use sstable::meta::SSTableMeta;
    use tempfile::{TempDir, tempdir};
    use tokio::fs;

    use super::*;

    #[tokio::test]
    async fn flush_moves_the_memtable_to_a_table_and_drops_its_wal() {
        let dir = tempdir().unwrap();
        let db = Collection::open(dir.path()).await.unwrap();

        for i in 0..3 {
            db.set(&format!("k{i}"), &format!("v{i}")).await.unwrap();
        }
        db.flush().await.unwrap();

        assert_eq!(names(&dir, "ss").await.len(), 1, "one table per flush");
        assert!(db.inner.state.read().await.frozen.is_empty());
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
    async fn background_flusher_drains_frozen_memtables() {
        let dir = tempdir().unwrap();
        let db = Collection::open(dir.path()).await.unwrap();

        for i in 0..3 {
            db.set(&format!("k{i}"), &format!("v{i}")).await.unwrap();
            let guard = db.inner.state_lock.lock().await;
            db.inner.force_freeze(&guard).await.unwrap();
        }
        drain(&db).await;

        assert_eq!(names(&dir, "ss").await.len(), 3);
        assert_eq!(names(&dir, "wal").await.len(), 1);
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
        let db = Collection::open(dir.path()).await.unwrap();

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

        let db = Collection::open(dir.path()).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), Some("c".to_string()));
        assert_eq!(db.get("other").await.unwrap(), Some("x".to_string()));
    }

    #[tokio::test]
    async fn reopen_resumes_the_sequence_above_the_flushed_tables() {
        let dir = tempdir().unwrap();
        let db = Collection::open(dir.path()).await.unwrap();
        db.set("k", "first").await.unwrap();
        db.close().await.unwrap();

        // The log that held `k` is gone, so the sequence can only come from the
        // table footer. Reissuing 1 would make later writes sort *older*.
        let db = Collection::open(dir.path()).await.unwrap();
        assert_eq!(
            db.inner.seq.load(Ordering::Relaxed),
            2,
            "next sequence must clear the largest one on disk"
        );

        db.set("k", "second").await.unwrap();
        db.close().await.unwrap();

        let db = Collection::open(dir.path()).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), Some("second".to_string()));
    }

    #[tokio::test]
    async fn a_redundant_wal_after_a_crash_replays_harmlessly() {
        let dir = tempdir().unwrap();
        let db = Collection::open(dir.path()).await.unwrap();
        db.set("k", "value").await.unwrap();
        db.flush().await.unwrap();

        // Recreate the crash window between the table rename and the unlink by
        // writing the log back; its entries keep their original sequences.
        let wal = ::wal::wal::WalWriter::open(dir.path().join("wal").join("1"))
            .await
            .unwrap();
        wal.append(&Key::new("k", 1), &Value::set("value"))
            .await
            .unwrap();
        drop(wal);
        db.close().await.unwrap();

        let db = Collection::open(dir.path()).await.unwrap();
        assert_eq!(db.get("k").await.unwrap(), Some("value".to_string()));
        drain(&db).await;
        assert_eq!(db.get("k").await.unwrap(), Some("value".to_string()));
    }

    #[tokio::test]
    async fn keys_stay_readable_across_a_flush() {
        const KEYS: usize = 100;

        let dir = tempdir().unwrap();
        let db = Arc::new(Collection::open(dir.path()).await.unwrap());
        for i in 0..KEYS {
            db.set(&format!("k{i:03}"), &format!("v{i}")).await.unwrap();
        }

        let done = Arc::new(AtomicBool::new(false));
        let reader = tokio::spawn({
            let (db, done) = (db.clone(), done.clone());
            async move {
                while !done.load(Ordering::Acquire) {
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
        done.store(true, Ordering::Release);
        reader.await.unwrap();
    }

    /// Wait for the background flusher to empty the frozen list.
    async fn drain(db: &Collection) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !db.inner.state.read().await.frozen.is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("flusher should drain the frozen memtables");
    }

    async fn names(dir: &TempDir, sub: &str) -> Vec<String> {
        let mut entries = fs::read_dir(dir.path().join(sub)).await.unwrap();
        let mut names = vec![];
        while let Some(entry) = entries.next_entry().await.unwrap() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        names
    }
}
