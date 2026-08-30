use std::{path::Path, sync::atomic::AtomicUsize};

use common::{DbResult, key::Key, value::Value};
use wal::wal::WalWriter;

use crate::list::SkipList;

pub(crate) mod list;

const MAX_SIZE: usize = 1024 * 1024;

pub struct MemTable {
    skip_list: SkipList,
    wal: WalWriter,
    size: AtomicUsize,
    max_size: usize,
}

impl MemTable {
    pub async fn new<P: AsRef<Path>>(path: P) -> DbResult<Self> {
        let wal = WalWriter::open(path).await?;
        let skip_list = SkipList::new();
        Ok(Self {
            skip_list,
            wal,
            size: AtomicUsize::new(0),
            max_size: MAX_SIZE,
        })
    }

    pub async fn set(&self, key: Key, value: Value) -> DbResult<()> {
        self.wal.append(&key, &value).await?;
        self.skip_list.insert(key, value);
        self.size.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        self.skip_list.get(key).cloned()
    }

    pub fn is_full(&self) -> bool {
        let curr = self.skip_list.mem_usage();
        curr >= self.max_size
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn get_set() {
        let file = NamedTempFile::new().unwrap();
        let mem_table = MemTable::new(file.path()).await.unwrap();
        for i in 0..100 {
            let key = Key(format!("{}", i), 1);
            let value = Value::Set(format!("{}", i * 2));
            mem_table.set(key, value).await.unwrap();
        }
        for i in 0..100 {
            let key = format!("{}", i);
            let expected = format!("{}", i * 2);
            let value = mem_table.get(&key).await.unwrap();
            assert!(matches!(value, Value::Set(v) if v == expected));
        }
    }
}
