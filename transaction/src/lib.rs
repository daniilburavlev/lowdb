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
            if lock.last_seq(&k.0).is_some_and(|seq| seq > k.1) {
                return Err(DbError::CommitConflict);
            }
        }
        self.storage.begin(self.id).await?;
        for (k, v) in self.buffer.iter() {
            self.storage.set(k.clone(), v.clone()).await?;
        }
        for (k, _) in self.buffer.iter() {
            lock.update(k.0.clone(), k.1);
        }
        self.storage.commit(self.id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
}
