use std::path::{Path, PathBuf};

use common::{DbResult, error::DbError, key::Key, value::Value};
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
};

use crate::{
    BLOCK_HEADER,
    bloom::BloomFilter,
    encode::{encode, put_u16, put_u32, set_u16, set_u32},
    footer::{FOOTER_LEN, Footer, MAGIC},
    index::IndexedKey,
    meta::SSTableMeta,
    write::{WriteTo, WriteToBuf},
};

const BLOCK_SIZE: usize = 4 * 1024;
const BITS_PER_KEY: usize = 10;
const WRITE_BUF: usize = 1024 * 1024;

pub struct SSTableWriter {
    file: BufWriter<File>,
    path: PathBuf,
    tmp: PathBuf,

    block: Vec<u8>,
    offset: u64,

    index_bytes: Vec<u8>,
    index: Vec<IndexedKey>,

    hashes: Vec<u64>,
    entries: u64,

    first_key: Option<Key>,
    last_key: Option<Key>,
    smallest_seq: u64,
    largest_seq: u64,
}

/// Path a writer for `path` streams into before the atomic publish. Exposed so
/// a caller that aborts a half-written table can clean the file up.
pub fn tmp_path(path: &Path) -> PathBuf {
    path.with_extension("sst.tmp")
}

impl SSTableWriter {
    pub async fn create(path: impl Into<PathBuf>) -> DbResult<Self> {
        let path = path.into();
        let tmp = tmp_path(&path);
        let f = File::create(&tmp).await?;
        // Create empty block with start space for checksum: 4 bytes + payload's len: 2 bytes
        let mut block = Vec::with_capacity(BLOCK_SIZE + 1024);
        reset_block(&mut block);

        Ok(Self {
            file: BufWriter::with_capacity(WRITE_BUF, f),
            path,
            tmp,
            block,
            offset: 0,
            index: Vec::new(),
            index_bytes: Vec::new(),
            hashes: Vec::new(),
            first_key: None,
            last_key: None,
            entries: 0,
            smallest_seq: 0,
            largest_seq: 0,
        })
    }

    pub async fn add(&mut self, key: &Key, value: &Value) -> DbResult<()> {
        if let Some(last) = &self.last_key
            && last > key
        {
            return Err(DbError::InvalidValue(format!(
                "key: {} < last: {}",
                key, last
            )));
        }
        let new_key = self.last_key.as_ref().is_none_or(|lk| lk.0 != key.0);
        if new_key {
            self.hashes.push(BloomFilter::hash64(&key.0));
        }
        encode(&mut self.block, key, value)?;
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
            self.smallest_seq = key.1;
            self.largest_seq = key.1;
        } else {
            self.smallest_seq = self.smallest_seq.min(key.1);
            self.largest_seq = self.largest_seq.max(key.1);
        }
        self.last_key = Some(key.clone());
        self.entries += 1;

        if self.block.len() > BLOCK_SIZE {
            self.flush_block().await?;
        }
        Ok(())
    }

    async fn flush_block(&mut self) -> DbResult<()> {
        if self.block.len() <= BLOCK_HEADER {
            return Ok(());
        }
        let last = self
            .last_key
            .clone()
            .ok_or(DbError::invalid_state("last block is none"))?;

        let crc = crc32fast::hash(&self.block[BLOCK_HEADER..]);
        set_u32(&mut self.block, 0, crc);

        let len: u16 = self.block.len().try_into()?;
        let payload_len = len - BLOCK_HEADER as u16;
        set_u16(&mut self.block, 4, payload_len);

        self.file.write_all(&self.block).await?;

        let index_key = IndexedKey::new(last, self.offset, len);
        index_key.write_to_buf(&mut self.index_bytes)?;
        self.index.push(index_key);

        self.offset += len as u64;
        self.block.clear();
        // Realloc space for block header
        reset_block(&mut self.block);
        Ok(())
    }

    pub async fn finish(mut self) -> DbResult<Option<SSTableMeta>> {
        if self.entries == 0 {
            drop(self.file);
            let _ = fs::remove_file(&self.tmp).await;
            return Ok(None);
        }
        self.flush_block().await?;

        let bloom = BloomFilter::build(&self.hashes, BITS_PER_KEY);
        let bloom_off = self.offset;
        bloom.write(&mut self.file).await?;
        self.offset += bloom.disk_size() as u64;

        let index_off = self.offset;
        self.file.write_all(&self.index_bytes).await?;
        self.offset += IndexedKey::list_disk_size(&self.index) as u64;

        let mut index_len = 0u32;
        for i in &self.index {
            index_len += i.disk_size() as u32;
        }

        let footer = Footer {
            entries: self.entries,
            bloom_off,
            bloom_len: bloom.disk_size() as u32,
            index_off,
            index_len,
            index_count: self.index.len() as u32,
            smallest_seq: self.smallest_seq,
            largest_seq: self.largest_seq,
            magic: MAGIC,
        };
        footer.write(&mut self.file).await?;
        self.offset += FOOTER_LEN as u64;

        self.file.flush().await?;
        let f = self.file.into_inner();
        f.sync_all().await?;
        drop(f);

        fs::rename(&self.tmp, &self.path).await?;
        sync_dir(self.path.parent().unwrap()).await?;

        Ok(Some(SSTableMeta {
            path: self.path,
            file_size: self.offset,
            first_key: self.first_key.unwrap(),
            last_key: self.last_key.unwrap(),
            bloom,
            index: self.index,
            footer,
        }))
    }
}

fn reset_block(block: &mut Vec<u8>) {
    put_u32(block, 0);
    put_u16(block, 0);
}

async fn sync_dir(dir: &Path) -> DbResult<()> {
    Ok(File::open(dir).await?.sync_all().await?)
}

#[cfg(test)]
mod tests {
    use std::io::SeekFrom;

    use tempfile::NamedTempFile;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

    use crate::read::ReadFrom;

    use super::*;

    #[tokio::test]
    async fn write_block_to_file() {
        let file = NamedTempFile::new().unwrap();
        let mut writer = SSTableWriter::create(file.path()).await.unwrap();

        for i in 0..10 {
            writer
                .add(&Key(format!("{}", i), 1), &Value::Set(format!("{}", i * 2)))
                .await
                .unwrap();
        }
        writer.finish().await.unwrap();

        let f = File::open(file.path()).await.unwrap();
        let file_size = f.metadata().await.unwrap().len();
        let mut r = BufReader::new(f);

        let sum = r.read_u32().await.unwrap();
        assert_eq!(sum, 1471837825);

        let mut len: u16 = r.read_u16().await.unwrap();
        assert_eq!(len, 145);

        let mut i = 0;

        while len > 0 {
            let key = Key::read(&mut r).await.unwrap();
            assert_eq!(key.0, format!("{}", i));
            let value = Value::read(&mut r).await.unwrap();
            len -= key.disk_size() as u16;
            len -= value.disk_size() as u16;
            i += 1;
        }
        assert_eq!(10, i);

        r.seek(SeekFrom::Start(file_size - FOOTER_LEN as u64))
            .await
            .unwrap();

        let footer = Footer::read(&mut r).await.unwrap();
        assert_eq!(10, footer.entries);
        assert_eq!(151, footer.bloom_off);
        assert_eq!(14, footer.bloom_len);
        assert_eq!(165, footer.index_off);
        assert_eq!(21, footer.index_len);
        assert_eq!(1, footer.index_count);
        assert_eq!(MAGIC, footer.magic);

        r.seek(SeekFrom::Start(footer.bloom_off)).await.unwrap();
        let mut bloom = vec![0u8; footer.bloom_len as usize];
        r.read_exact(&mut bloom).await.unwrap();

        for _ in 0..footer.index_count {
            IndexedKey::read(&mut r).await.unwrap();
        }
    }
}
