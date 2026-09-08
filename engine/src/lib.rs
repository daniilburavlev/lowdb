use std::path::Path;

use common::DbResult;

pub struct DB {}

impl DB {
    pub async fn open<P: AsRef<Path>>(path: P) -> DbResult<Self> {
        todo!()
    }

    pub async fn put(&self, key: &str, value: &str) -> DbResult<()> {
        Ok(())
    }

    pub async fn get(&self, key: &str) -> DbResult<Option<String>> {
        Ok(None)
    }
}
