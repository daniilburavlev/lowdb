use std::{path::PathBuf, sync::atomic::AtomicUsize};

use common::DbResult;
use sstable::table::SSTable;
use tokio::fs;

pub struct Collection {
    #[allow(dead_code)]
    seq: AtomicUsize,
}

impl Collection {
    pub async fn open(dir: impl Into<PathBuf>) -> DbResult<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        Ok(Self {
            seq: AtomicUsize::new(0),
        })
    }

    pub async fn set(&self, _: &str, _: &str) -> DbResult<()> {
        Ok(())
    }

    pub async fn get(&self, _: &str) -> DbResult<Option<String>> {
        Ok(None)
    }

    async fn load_sstables() -> DbResult<Vec<SSTable>> {
        todo!()
    }
}
