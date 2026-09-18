use std::{collections::HashMap, sync::Arc};

use tokio::sync::{Mutex, MutexGuard};

#[derive(Clone)]
pub struct Lock {
    recent: Arc<Mutex<HashMap<String, u64>>>,
}

impl Lock {
    pub async fn lock(&self) -> LockGuard<'_> {
        let write = self.recent.lock().await;
        LockGuard(write)
    }
}

impl Default for Lock {
    fn default() -> Self {
        Self {
            recent: Arc::new(Mutex::new(HashMap::default())),
        }
    }
}

pub struct LockGuard<'a>(MutexGuard<'a, HashMap<String, u64>>);

impl<'a> LockGuard<'a> {
    pub fn last_seq(&self, key: &String) -> Option<u64> {
        self.0.get(key).cloned()
    }

    pub fn update(&mut self, key: String, seq: u64) {
        self.0.insert(key, seq);
    }
}
