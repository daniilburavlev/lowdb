use std::sync::Arc;

use common::DbResult;
use storage::{Storage, state::State};

use crate::oracle::Oracle;

/// Read-only, consistent view of the database as of its creation.
///
/// Holds the `State` it was taken from, so versions that a later level-0 flush
/// drops stay readable through the pinned memtables.
pub struct Snapshot {
    read_ts: u64,
    state: Arc<State>,
    oracle: Arc<Oracle>,
}

impl Snapshot {
    /// Take a snapshot at the currently visible sequence number.
    pub async fn new(oracle: Arc<Oracle>, storage: &Storage) -> Self {
        // `read_ts` before `state`: every write at or below `read_ts` is then
        // either in the captured memtables or already on disk.
        let read_ts = oracle.begin();
        let state = storage.snapshot().await;
        Self {
            read_ts,
            state,
            oracle,
        }
    }

    /// Sequence number this snapshot reads at.
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    /// Get the value of `key` as of this snapshot.
    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        self.state.get_snap(key, self.read_ts).await
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.oracle.end(self.read_ts);
    }
}
