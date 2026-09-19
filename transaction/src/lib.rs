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

    use common::error::DbError;

    use super::*;

    #[tokio::test]
    async fn tx_visibility() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let max_seq = 0;
        let oracle = Arc::new(Oracle::new(max_seq));
        let storage = Arc::new(storage);

        oracle
            .commit(
                &storage,
                None,
                BTreeMap::from([("k1".to_string(), Value::set("v1"))]),
            )
            .await
            .unwrap();

        let tx = Transaction::new(oracle.clone(), storage.clone()).await;

        oracle
            .commit(
                &storage,
                None,
                BTreeMap::from([("k1".to_string(), Value::set("v1"))]),
            )
            .await
            .unwrap();

        assert_eq!(tx.get("k1").await.unwrap(), Some("v1".to_string()));
    }

    #[tokio::test]
    async fn concurrent_txs() {
        let (_dir, storage) = storage::testing::create_storage().await;
        let storage = Arc::new(storage);

        let storage = Arc::clone(&storage);
        let oracle = Arc::new(Oracle::new(1));

        let mut tx = Transaction::new(oracle.clone(), storage.clone()).await;

        let mut tx2 = Transaction::new(oracle.clone(), storage.clone()).await;

        let tx1 = tokio::spawn(async move {
            tx.set("k1", "v2");
            tokio::time::sleep(Duration::from_secs(1)).await;
            tx.commit().await
        });
        tx2.set("k1", "v1");
        tx2.commit().await.unwrap();

        let Err(DbError::CommitConflict) = tx1.await.unwrap() else {
            panic!("first tx does not rollbacked");
        };
    }

    #[tokio::test]
    async fn conflict_does_not_depend_on_tx_creation_order() {
        let (_dir, storage) = storage::testing::create_storage().await;

        let oracle = Arc::new(Oracle::new(1));
        let storage = Arc::new(storage);

        let dropped = Transaction::new(oracle.clone(), storage.clone()).await;
        drop(dropped);

        let mut t1 = Transaction::new(oracle.clone(), storage.clone()).await;
        let mut t2 = Transaction::new(oracle.clone(), storage.clone()).await;
        t1.set("k", "a");
        t2.set("k", "b");
        t1.commit().await.unwrap();
        assert!(matches!(t2.commit().await, Err(DbError::CommitConflict)));
    }

    /// Bug: the snapshot seq is simply the next counter value, not a
    /// "committed up to" watermark. A plain writer that allocated a lower seq
    /// before the tx began but applies it after is visible mid-transaction.
    /// (Simulates the interleaving `fetch_add` → tx begin → `Storage::set`.)
    #[tokio::test]
    async fn in_flight_lower_seq_write_is_not_visible_to_later_snapshot() {
        let (_dir, storage) = storage::testing::create_storage().await;

        let oracle = Arc::new(Oracle::new(1));
        let storage = Arc::new(storage);

        let tx = Transaction::new(oracle.clone(), storage.clone()).await;
        assert_eq!(tx.get("k").await.unwrap(), None);

        oracle
            .commit(
                &storage,
                None,
                BTreeMap::from([("k".to_string(), Value::set("late"))]),
            )
            .await
            .unwrap();

        assert_eq!(
            tx.get("k").await.unwrap(),
            None,
            "snapshot changed after the tx began"
        );
    }

    #[tokio::test]
    async fn committed_tx_cannot_commit_again() {
        let (_dir, storage) = storage::testing::create_storage().await;

        let oracle = Arc::new(Oracle::new(1));
        let storage = Arc::new(storage);

        let mut tx = Transaction::new(oracle, storage).await;
        tx.set("k", "v");
        tx.commit().await.unwrap();
    }
}
