use std::{fs::File, path::PathBuf, sync::Arc};

use common::{DbResult, error::DbError, key::Key};

use crate::{
    cursor::{Cursor, ValueRef},
    index::IndexedKey,
    meta::SSTableMeta,
};

pub enum Lookup {
    Found(String),
    Deleted,
    Absent,
}

pub struct BlockHandle {
    pub last_key: Key,
    pub offset: u64,
    pub len: u32,
}

pub struct SSTable {
    file: Arc<File>,
    pub meta: SSTableMeta,
}

impl SSTable {
    pub async fn open(path: impl Into<PathBuf>) -> DbResult<Self> {
        let path = path.into();
        let meta = SSTableMeta::read(&path).await?;
        let file = File::open(&path)?;
        Ok(Self {
            file: Arc::new(file),
            meta,
        })
    }

    pub async fn get(&self, user: &str, snapshot: u64) -> DbResult<Lookup> {
        if user < &*self.meta.first_key.0 || user > &*self.meta.last_key.0 {
            return Ok(Lookup::Absent);
        }
        if !self.meta.bloom.may_contain(user) {
            return Ok(Lookup::Absent);
        }
        let target = Key::new(user, snapshot);
        let i = self.meta.index.partition_point(|h| h.key < target);
        let Some(handle) = self.meta.index.get(i) else {
            return Ok(Lookup::Absent);
        };

        let block = self.read_block(handle).await?;
        let mut c = Cursor::new(&block);
        while let Some(entry) = c.next_entry()? {
            if entry < target {
                continue;
            };
            return Ok(if entry.key == user {
                match entry.value {
                    ValueRef::Set(b) => Lookup::Found(b.to_string()),
                    ValueRef::Delete => Lookup::Deleted,
                }
            } else {
                Lookup::Absent
            });
        }
        Ok(Lookup::Absent)
    }

    async fn read_block(&self, h: &IndexedKey) -> DbResult<Vec<u8>> {
        let file = self.file.clone();
        let (off, len) = (h.block_off, h.block_len as usize);
        tokio::task::spawn_blocking(move || {
            let mut buf = vec![0u8; len];
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                file.read_exact_at(&mut buf, off)?;
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::FileExt;
                let mut n = 0;
                while n < len {
                    n += file.seek_reed(&mut buf[n..], off + n as u64)?;
                }
            }
            Ok(buf)
        })
        .await
        .map_err(|e| DbError::InvalidState(e.to_string()))?
    }
}
