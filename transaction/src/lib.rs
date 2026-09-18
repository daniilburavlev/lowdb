use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering::Relaxed},
};

use common::{DbResult, error::DbError, key::Key, lookup::Lookup, value::Value};
use memtable::MemTable;
use storage::{Storage, state::State};

use crate::locks::Lock;

pub mod locks;

pub struct Transaction {
    id: u64,
    seq: Arc<AtomicU64>,
    buffer: MemTable,
    storage: Arc<Storage>,
    state: Arc<State>,
    lock: Lock,
}

impl Transaction {
    pub async fn new(seq: Arc<AtomicU64>, storage: Arc<Storage>, lock: Lock) -> Self {
        let id = seq.fetch_add(1, Relaxed);
        let state = storage.snapshot().await;
        {
            let mut lock = lock.lock().await;
            lock.add_tx(id);
        }
        Self {
            id,
            seq,
            buffer: MemTable::new(0),
            state,
            storage,
            lock,
        }
    }

    pub async fn set(&self, key: &str, value: &str) -> DbResult<()> {
        let seq = self.seq.fetch_add(1, Relaxed);
        self.buffer.put(Key::new(key, seq), Value::set(value));
        Ok(())
    }

    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        match self.buffer.get(key) {
            Lookup::Found(value) => return Ok(Some(value)),
            Lookup::Deleted => return Ok(None),
            Lookup::Absent => {}
        }
        self.state.get_snap(key, self.id).await
    }

    pub async fn commit(&self) -> DbResult<()> {
        let mut lock = self.lock.lock().await;
        for (k, _) in self.buffer.iter() {
            if lock.last_id(&k.0).is_some_and(|id| id != self.id) {
                return Err(DbError::CommitConflict);
            }
        }
        self.storage.begin(self.id).await?;
        for (k, v) in self.buffer.iter() {
            self.storage.set(k.clone(), v.clone()).await?;
        }
        for (k, _) in self.buffer.iter() {
            lock.update(k.0.clone(), self.id);
        }
        lock.remove_tx(self.id);
        self.storage.commit(self.id).await?;
        Ok(())
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
}
