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

pub struct TableScan {
    r: BufReader<File>,
    block_remaining: u16,
    remaining: u64,
}

impl TableScan {
    pub async fn open(path: &Path) -> DbResult<Self> {
        let mut f = File::open(path).await?;
        let size = f.metadata().await?.len();
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
        }
        let key = Key::read(&mut self.r).await?;
        let mut used = key.disk_size();

        let value = Value::read(&mut self.r).await?;
        used += value.disk_size();

        self.block_remaining -= used as u16;
        self.remaining -= used as u64;
        Ok(Some((key, value)))
    }

    async fn check_block(&mut self) -> DbResult<()> {
        let sum = self.r.read_u32().await?;
        let len = self.r.read_u16().await?;
        self.remaining -= 6;

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
