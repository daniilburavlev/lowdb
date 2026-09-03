use std::{io::SeekFrom, path::Path};

use common::{DbResult, error::DbError, key::Key, value::Value};
use tokio::{
    fs::File,
    io::{self, AsyncReadExt, AsyncSeekExt, BufReader},
};

use crate::{
    footer::{FOOTER_LEN, Footer},
    read::ReadFrom,
};

const BLOCK_HEADER: u64 = 4 + 2;

pub struct TableScan {
    r: BufReader<File>,
    block_remaining: u16,
    remaining: u64,
}

impl TableScan {
    pub async fn open(path: &Path) -> DbResult<Self> {
        let mut f = File::open(path).await?;
        let size = f.metadata().await?.len();
        if (size as usize) < FOOTER_LEN {
            return Err(DbError::invalid_state("sstable is empty"));
        }
        f.seek(io::SeekFrom::Start(size - FOOTER_LEN as u64))
            .await?;
        let footer = Footer::read(&mut f).await?;
        f.seek(io::SeekFrom::Start(0)).await?;
        Ok(Self {
            r: BufReader::with_capacity(256 * 1024, f),
            block_remaining: 0,
            remaining: footer.bloom_off,
        })
    }

    pub(crate) async fn next(&mut self) -> DbResult<Option<(Key, Value)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if self.block_remaining == 0 {
            self.check_block().await?;
            if self.block_remaining == 0 {
                return match self.remaining {
                    0 => Ok(None),
                    _ => Err(DbError::invalid_state(
                        "empty block before end of the table",
                    )),
                };
            }
        }
        let key = Key::read(&mut self.r).await?;
        let mut used = key.disk_size();

        let value = Value::read(&mut self.r).await?;
        used += value.disk_size();

        self.block_remaining = (self.block_remaining as usize)
            .checked_sub(used)
            .and_then(|n| u16::try_from(n).ok())
            .ok_or(DbError::invalid_state("entry crosses the block boundary"))?;
        self.remaining = self
            .remaining
            .checked_sub(used as u64)
            .ok_or(DbError::invalid_state("entry runs past the last block"))?;
        Ok(Some((key, value)))
    }

    async fn check_block(&mut self) -> DbResult<()> {
        let sum = self.r.read_u32().await?;
        let len = self.r.read_u16().await?;
        self.remaining = self
            .remaining
            .checked_sub(BLOCK_HEADER)
            .ok_or(DbError::invalid_state("truncated block header"))?;

        let pos = self.r.seek(SeekFrom::Current(0)).await?;
        let mut buf = vec![0u8; len as usize];
        self.r.read_exact(&mut buf).await?;
        let hash = crc32fast::hash(&buf);
        if hash != sum {
            return Err(DbError::invalid_state("block corrupted"));
        }
        self.r.seek(SeekFrom::Start(pos)).await?;

        self.block_remaining = len;
        Ok(())
    }
}
