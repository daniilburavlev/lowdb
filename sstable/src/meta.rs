use std::{io::SeekFrom, path::PathBuf};

use common::{DbResult, error::DbError, key::Key};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
};

use crate::{
    bloom::BloomFilter,
    footer::{FOOTER_LEN, Footer},
    index::IndexedKey,
    read::ReadFrom,
};

#[derive(Clone, Debug)]
pub struct SSTableMeta {
    pub path: PathBuf,
    pub file_size: u64,
    pub bloom: BloomFilter,
    pub first_key: Key,
    pub last_key: Key,
    pub footer: Footer,
    pub index: Vec<IndexedKey>,
}

impl SSTableMeta {
    pub async fn read(path: impl Into<PathBuf>) -> DbResult<Self> {
        let path = path.into();
        let file = File::open(&path).await?;
        let metadata = file.metadata().await?;
        let file_size = metadata.len();
        if (file_size as usize) < FOOTER_LEN {
            return Err(DbError::invalid_state("cannot read metadata: file empty"));
        }
        let mut reader = BufReader::new(file);
        let footer = Self::footer(file_size, &mut reader).await?;
        let bloom = Self::bloom(&mut reader, footer.bloom_off, footer.bloom_len as usize).await?;
        let first_key = Self::first_key(&mut reader).await?;

        let index = Self::index(
            &mut reader,
            footer.index_off,
            footer.index_len as usize,
            footer.index_count as usize,
        )
        .await?;

        let last_key = index
            .last()
            .cloned()
            .ok_or(DbError::invalid_state("cannot load last table's key"))?
            .key;

        Ok(Self {
            path,
            file_size,
            bloom,
            first_key,
            last_key,
            footer,
            index,
        })
    }

    pub fn smallest_seq(&self) -> u64 {
        self.footer.smallest_seq
    }

    pub fn largest_seq(&self) -> u64 {
        self.footer.largest_seq
    }

    pub fn entries(&self) -> u64 {
        self.footer.entries
    }

    async fn footer(size: u64, read: &mut BufReader<File>) -> DbResult<Footer> {
        read.seek(SeekFrom::Start(size - FOOTER_LEN as u64)).await?;
        Footer::read(read).await
    }

    async fn bloom(read: &mut BufReader<File>, offset: u64, len: usize) -> DbResult<BloomFilter> {
        read.seek(SeekFrom::Start(offset)).await?;
        BloomFilter::read(read, len).await
    }

    async fn index(
        read: &mut BufReader<File>,
        offset: u64,
        mut len: usize,
        count: usize,
    ) -> DbResult<Vec<IndexedKey>> {
        let mut index = Vec::with_capacity(count);
        read.seek(SeekFrom::Start(offset)).await?;
        while len > 0 {
            let key = IndexedKey::read(read).await?;
            len = len
                .checked_sub(key.disk_size())
                .ok_or(DbError::invalid_state(
                    "index length does not match entries",
                ))?;
            index.push(key);
        }
        Ok(index)
    }

    async fn first_key(read: &mut BufReader<File>) -> DbResult<Key> {
        read.seek(SeekFrom::Start(0)).await?;
        read.read_u32().await?;
        read.read_u16().await?;
        Key::read(read).await
    }
}

#[cfg(test)]
mod tests {
    use common::{key::Key, value::Value};
    use tempfile::NamedTempFile;

    use crate::writer::SSTableWriter;

    use super::*;

    #[tokio::test]
    async fn meta_read() {
        let file = NamedTempFile::new().unwrap();
        let mut writer = SSTableWriter::create(file.path()).await.unwrap();

        let key = Key::new("key", 2);
        let value = Value::set("value");

        writer.add(&key, &value).await.unwrap();
        writer.finish().await.unwrap();

        let f = File::open(file.path()).await.unwrap();
        let file_size = f.metadata().await.unwrap().len();

        let meta = SSTableMeta::read(file.path()).await.unwrap();
        assert_eq!(meta.file_size, file_size);
    }
}
