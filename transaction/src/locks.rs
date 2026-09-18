use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use tokio::sync::{Mutex, MutexGuard};

#[derive(Clone)]
pub struct Lock(Arc<Mutex<InnerLock>>);

impl Lock {
    pub async fn lock(&self) -> LockGuard<'_> {
        let write = self.0.lock().await;
        LockGuard(write)
    }
}

impl Default for Lock {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(InnerLock::default())))
    }
}

#[derive(Debug, Default)]
pub(crate) struct InnerLock {
    pub(crate) recent: HashMap<String, u64>,
    pub(crate) txs: BTreeSet<u64>,
}

#[derive(Debug)]
pub struct LockGuard<'a>(pub(crate) MutexGuard<'a, InnerLock>);

impl<'a> LockGuard<'a> {
    pub fn add_tx(&mut self, id: u64) {
        self.0.txs.insert(id);
    }

    pub fn remove_tx(&mut self, id: u64) {
        if let Some(min) = self.0.txs.first()
            && *min == id
        {
            self.0.recent.retain(|_, value| *value == id);
        }
        self.0.txs.remove(&id);
    }

    pub fn last_id(&self, key: &String) -> Option<u64> {
        self.0.recent.get(key).cloned()
    }

    pub fn update(&mut self, key: String, tx_id: u64) {
        self.0.recent.insert(key, tx_id);
    }
}
