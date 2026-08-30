use common::{DbResult, key::Key};
use tokio::io::AsyncReadExt;

use crate::{
    encode::{encode_key, put_u16, put_u64},
    read::ReadFrom,
    write::WriteToBuf,
};

#[derive(Clone, Debug)]
pub struct IndexedKey {
    pub key: Key,
    pub block_off: u64,
    pub block_len: u16,
}

impl IndexedKey {
    pub(crate) fn new(key: Key, block_off: u64, block_len: u16) -> Self {
        Self {
            key,
            block_off,
            block_len,
        }
    }

    pub(crate) fn disk_size(&self) -> usize {
        self.key.disk_size() + 8 + 2
    }
}

impl WriteToBuf for IndexedKey {
    fn write_to_buf(&self, buf: &mut Vec<u8>) -> DbResult<()> {
        encode_key(buf, &self.key)?;
        put_u64(buf, self.block_off);
        put_u16(buf, self.block_len);
        Ok(())
    }
}

impl ReadFrom for IndexedKey {
    async fn read<R>(read: &mut R) -> DbResult<Self>
    where
        R: AsyncReadExt + Unpin,
    {
        let key = Key::read(read).await?;
        let block_off = read.read_u64().await?;
        let block_len = read.read_u16().await?;
        Ok(Self {
            key,
            block_off,
            block_len,
        })
    }
}
