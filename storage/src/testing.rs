//! Utility testing functions
#[cfg(any(test, feature = "testing"))]
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(test, feature = "testing"))]
use tempfile::{TempDir, tempdir};
#[cfg(any(test, feature = "testing"))]
use tokio::{fs, sync::Notify};

#[cfg(any(test, feature = "testing"))]
use crate::{Storage, storage::DiskStorage, wal::Wal};

/// Wait until all frozen memtables will be flushed
pub async fn drain(db: &Storage) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !db.state.read().await.frozen.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("flusher should drain the frozen memtables");
}

/// Get all filenames in given folder
#[cfg(any(test, feature = "testing"))]
pub async fn names(dir: &TempDir, sub: &str) -> Vec<String> {
    let mut entries = fs::read_dir(dir.path().join(sub)).await.unwrap();
    let mut names = vec![];
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    names
}

/// Create tempdir and Storage instance
#[cfg(any(test, feature = "testing"))]
pub async fn create_storage() -> (TempDir, Storage) {
    let dir = tempdir().unwrap();
    let wal = Wal::new(dir.path()).await.unwrap();
    let storage = DiskStorage::load(dir.path()).await.unwrap();
    let storage = Storage::new(
        wal,
        storage,
        vec![],
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    )
    .await
    .unwrap();
    (dir, storage)
}
