use std::{collections::BTreeMap, sync::Arc};

use common::{DbResult, value::Value};
use storage::{Storage, state::State};

use crate::oracle::Oracle;

pub mod oracle;

pub struct Transaction {
    read_ts: u64,
    buffer: BTreeMap<String, Value>,
    oracle: Arc<Oracle>,
    storage: Arc<Storage>,
    state: Arc<State>,
    done: bool,
}

impl Transaction {
    pub async fn new(oracle: Arc<Oracle>, storage: Arc<Storage>) -> Self {
        let read_ts = oracle.begin();
        let state = storage.snapshot().await;
        Self {
            read_ts,
            buffer: BTreeMap::new(),
            oracle,
            storage,
            state,
            done: false,
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.buffer.insert(key.to_string(), Value::set(value));
    }

    pub fn delete(&mut self, key: &str) {
        self.buffer.insert(key.to_string(), Value::Delete);
    }

    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        match self.buffer.get(key) {
            Some(Value::Set(value)) => return Ok(Some(value.clone())),
            Some(Value::Delete) => return Ok(None),
            None => {}
        }
        self.state.get_snap(key, self.read_ts).await
    }

    pub async fn commit(mut self) -> DbResult<()> {
        let buffer = std::mem::take(&mut self.buffer);
        if buffer.is_empty() {
            return Ok(());
        }
        self.oracle
            .commit(&self.storage, Some(self.read_ts), buffer)
            .await
            .map(|_| ())
    }

    pub fn rollback(&self) {}
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.done {
            self.done = true;
            self.oracle.end(self.read_ts);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn tx_visibility() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let storage = Arc::new(storage);

        let seq = Arc::new(AtomicU64::new(1));

        let key1 = Key::new("k1", seq.fetch_add(1, Relaxed));
        let value1 = Value::set("v1");
        storage.set(key1, value1).await.unwrap();

        let lock = Lock::default();
        let tx = Transaction::new(seq.clone(), Arc::clone(&storage), lock).await;

        let key2 = Key::new("k1", seq.fetch_add(1, Relaxed));
        let value2 = Value::set("v2");
        storage.set(key2, value2).await.unwrap();

        assert_eq!(tx.get("k1").await.unwrap(), Some("v1".to_string()));
    }

    #[tokio::test]
    async fn concurrent_txs() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let storage = Arc::new(storage);

        let seq = Arc::new(AtomicU64::new(1));
        let storage = Arc::clone(&storage);
        let lock = Lock::default();

        let tx = Transaction::new(Arc::clone(&seq), Arc::clone(&storage), lock.clone()).await;
        {
            let lock = lock.lock().await;
            assert!(lock.0.txs.contains(&1));
        }

        let tx2 = Transaction::new(seq, storage, lock.clone()).await;
        {
            let lock = lock.lock().await;
            assert!(lock.0.txs.contains(&2));
        }
        let tx1 = tokio::spawn(async move {
            tx.set("k1", "v2").await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            tx.commit().await
        });
        tx2.set("k1", "v1").await.unwrap();
        tx2.commit().await.unwrap();

        let Err(DbError::CommitConflict) = tx1.await.unwrap() else {
            panic!("first tx does not rollbacked");
        };
    }

    // ---- Review findings: each test asserts correct behaviour and fails today ----

    /// Bug: `commit` returns early on conflict without `remove_tx`, and there is
    /// no rollback/Drop, so aborted or abandoned txs stay in `txs` forever. Since
    /// `remove_tx` only prunes when the committing tx is the minimum, one leaked
    /// id blocks pruning of `recent` for the rest of the process.
    #[tokio::test]
    async fn aborted_and_dropped_txs_leave_active_set() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let storage = Arc::new(storage);
        let seq = Arc::new(AtomicU64::new(1));
        let lock = Lock::default();

        let dropped = Transaction::new(seq.clone(), storage.clone(), lock.clone()).await;
        let dropped_id = dropped.id;
        drop(dropped);

        let t1 = Transaction::new(seq.clone(), storage.clone(), lock.clone()).await;
        let t2 = Transaction::new(seq.clone(), storage.clone(), lock.clone()).await;
        t1.set("k", "a").await.unwrap();
        t2.set("k", "b").await.unwrap();
        t1.commit().await.unwrap();
        assert!(matches!(t2.commit().await, Err(DbError::CommitConflict)));

        let guard = lock.lock().await;
        assert!(
            !guard.0.txs.contains(&dropped_id),
            "dropped tx still registered as active"
        );
        assert!(
            !guard.0.txs.contains(&t2.id),
            "aborted tx still registered as active"
        );
    }

    /// Bug: the snapshot seq is simply the next counter value, not a
    /// "committed up to" watermark. A plain writer that allocated a lower seq
    /// before the tx began but applies it after is visible mid-transaction.
    /// (Simulates the interleaving `fetch_add` → tx begin → `Storage::set`.)
    #[tokio::test]
    async fn in_flight_lower_seq_write_is_not_visible_to_later_snapshot() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let storage = Arc::new(storage);
        let seq = Arc::new(AtomicU64::new(1));

        let in_flight = seq.fetch_add(1, Relaxed); // DB::set allocated, not yet applied
        let tx = Transaction::new(seq.clone(), storage.clone(), Lock::default()).await;
        assert_eq!(tx.get("k").await.unwrap(), None);

        storage
            .set(Key::new("k", in_flight), Value::set("late"))
            .await
            .unwrap();

        assert_eq!(
            tx.get("k").await.unwrap(),
            None,
            "snapshot changed after the tx began"
        );
    }

    /// Bug: `commit(&self)` can be called again (e.g. after success), re-applying
    /// the write set with stale seqs and re-appending TxBegin/TxCommit. After
    /// another tx committed in between, the re-commit is not rejected.
    #[tokio::test]
    async fn committed_tx_cannot_commit_again() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let storage = Arc::new(storage);
        let seq = Arc::new(AtomicU64::new(1));
        let lock = Lock::default();

        let tx = Transaction::new(seq.clone(), storage.clone(), lock.clone()).await;
        tx.set("k", "v").await.unwrap();
        tx.commit().await.unwrap();

        assert!(tx.commit().await.is_err(), "second commit must be rejected");
    }
}
