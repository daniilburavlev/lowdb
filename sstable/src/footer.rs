use common::{DbResult, error::DbError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    encode::{put_u32, put_u64},
    read::ReadFrom,
    write::WriteTo,
};

pub(crate) const FOOTER_LEN: usize = 8 + 8 + 4 + 8 + 4 + 4 + 8 + 8 + 4;
pub(crate) const MAGIC: u32 = 0x5354_424C;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Footer {
    pub(crate) entries: u64,
    pub(crate) bloom_off: u64,
    pub(crate) bloom_len: u32,
    pub(crate) index_off: u64,
    pub(crate) index_len: u32,
    pub(crate) index_count: u32,
    /// Lowest sequence number stored in the table.
    pub(crate) smallest_seq: u64,
    /// Highest sequence number stored in the table. Recovery seeds the next
    /// sequence from the maximum over every table and every surviving WAL, so a
    /// flushed WAL can be deleted without losing the sequence counter.
    pub(crate) largest_seq: u64,
    pub(crate) magic: u32,
}

impl Footer {
    pub fn write_to_buf(&self, buffer: &mut Vec<u8>) {
        put_u64(buffer, self.entries);
        put_u64(buffer, self.bloom_off);
        put_u32(buffer, self.bloom_len);
        put_u64(buffer, self.index_off);
        put_u32(buffer, self.index_len);
        put_u32(buffer, self.index_count);
        put_u64(buffer, self.smallest_seq);
        put_u64(buffer, self.largest_seq);
        put_u32(buffer, self.magic);
    }
}

impl WriteTo for Footer {
    async fn write<W>(&self, w: &mut W) -> DbResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        w.write_u64(self.entries).await?;
        w.write_u64(self.bloom_off).await?;
        w.write_u32(self.bloom_len).await?;
        w.write_u64(self.index_off).await?;
        w.write_u32(self.index_len).await?;
        w.write_u32(self.index_count).await?;
        w.write_u64(self.smallest_seq).await?;
        w.write_u64(self.largest_seq).await?;
        w.write_u32(self.magic).await?;
        Ok(())
    }
}

impl From<Footer> for Vec<u8> {
    fn from(value: Footer) -> Self {
        let mut buffer = Vec::with_capacity(FOOTER_LEN);
        value.write_to_buf(&mut buffer);
        buffer
    }
}

impl ReadFrom for Footer {
    async fn read<R>(r: &mut R) -> DbResult<Self>
    where
        R: AsyncReadExt + Unpin,
    {
        let entries = r.read_u64().await?;
        let bloom_off = r.read_u64().await?;
        let bloom_len = r.read_u32().await?;
        let index_off = r.read_u64().await?;
        let index_len = r.read_u32().await?;
        let index_count = r.read_u32().await?;
        let smallest_seq = r.read_u64().await?;
        let largest_seq = r.read_u64().await?;
        let magic = r.read_u32().await?;

        if magic != MAGIC {
            return Err(DbError::invalid_state("bad sstable magic"));
        }

        Ok(Self {
            entries,
            bloom_off,
            bloom_len,
            index_off,
            index_len,
            index_count,
            smallest_seq,
            largest_seq,
            magic,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tokio::io::BufReader;

    use super::*;

    #[tokio::test]
    async fn footer_write_read() {
        let footer = Footer {
            entries: 1,
            bloom_off: 1,
            bloom_len: 1,
            index_off: 1234,
            index_len: 1000000,
            index_count: 100,
            smallest_seq: 7,
            largest_seq: 42,
            magic: MAGIC,
        };

        let mut buffer = vec![];
        footer.write_to_buf(&mut buffer);

        let cursor = Cursor::new(buffer);
        let mut reader = BufReader::new(cursor);
        let restored = Footer::read(&mut reader).await.unwrap();

        assert_eq!(footer, restored);
    }
}
