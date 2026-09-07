use std::{
    path::Path,
    sync::{
        Arc, Weak,
        atomic::{
            AtomicBool, AtomicU64,
            Ordering::{Acquire, Release},
        },
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

const MAX_FROZEN: usize = 4;
const BACKPRESSURE_POLL: Duration = Duration::from_millis(50);
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

    pub async fn flush(&self) -> DbResult<()> {
        self.inner.flush_all().await
    }

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
        self.inner.begin_shutdown();
    }
}

struct Inner {
    seq: AtomicU64,
    state: RwLock<Arc<State>>,
    state_lock: Mutex<()>,
    flush_lock: Mutex<()>,
    flush_notify: Notify,
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

    fn begin_shutdown(&self) {
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

async fn flush_loop(inner: Weak<Inner>) {
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
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    use ::wal::WalWriter;
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

        let db = Collection::open(dir.path()).await.unwrap();
        assert_eq!(
            db.inner.seq.load(Relaxed),
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

        let wal = WalWriter::open(dir.path().join("wal").join("1"))
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
