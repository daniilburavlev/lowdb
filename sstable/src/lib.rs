#[deny(unreachable_pub)]
#[warn(missing_docs)]
use std::path::PathBuf;

use common::{DbResult, key::Key, value::Value};

use crate::{meta::SSTableMeta, writer::SSTableWriter};

pub(crate) mod bloom;
pub(crate) mod cursor;
pub(crate) mod encode;
pub(crate) mod footer;
pub(crate) mod index;
pub(crate) mod key;
pub mod meta;
pub(crate) mod read;
pub mod scan;
pub mod table;
pub(crate) mod write;
pub mod writer;

pub async fn build_from_iter<I>(path: impl Into<PathBuf>, iter: I) -> DbResult<Option<SSTableMeta>>
where
    I: IntoIterator<Item = (Key, Value)>,
{
    let mut w = SSTableWriter::create(path).await?;
    for (k, v) in iter {
        w.add(&k, &v).await?;
    }
    w.finish().await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::scan::table::TableScan;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn table_scan() {
        let (file, values) = write_to_disk().await;
        let mut table_scan = TableScan::open(file.path()).await.unwrap();
        let mut result = Vec::with_capacity(values.len());
        while let Some((k, v)) = table_scan.next().await.unwrap() {
            result.push((k, v));
        }
        assert_eq!(values.len(), result.len());
        for ((k1, v1), (k2, v2)) in values.iter().zip(&values) {
            assert_eq!(k1, k2);
            assert_eq!(v1, v2);
        }
    }

    /// Every entry is 64 bytes on disk (2 + 12 key + 8 seq + 2 + 40 value), so the 64th
    /// `add` takes the block to 6 + 4096 bytes and flushes it: the writer must not then
    /// emit a second, payload-less block from `finish`.
    #[tokio::test]
    async fn last_add_fills_block_exactly() {
        const ENTRIES: usize = 64;

        let file = NamedTempFile::new().unwrap();
        let mut writer = SSTableWriter::create(file.path()).await.unwrap();
        for i in 0..ENTRIES {
            let key = Key(format!("key{:09}", i), i as u64);
            writer.add(&key, &Value::Set("v".repeat(40))).await.unwrap();
        }
        let meta = writer.finish().await.unwrap().unwrap();

        assert_eq!(meta.index.len(), 1, "no empty trailing block");
        assert_eq!(
            meta.file_size,
            tokio::fs::metadata(file.path()).await.unwrap().len(),
            "reported size matches the bytes on disk"
        );

        let mut scan = TableScan::open(file.path()).await.unwrap();
        let mut read = 0;
        while let Some((k, _)) = scan.next().await.unwrap() {
            assert_eq!(k.0, format!("key{:09}", read));
            read += 1;
        }
        assert_eq!(read, ENTRIES);
    }

    async fn write_to_disk() -> (NamedTempFile, BTreeMap<Key, Value>) {
        let file = NamedTempFile::new().unwrap();
        let mut writer = SSTableWriter::create(file.path()).await.unwrap();
        let mut sorted = BTreeMap::new();
        for i in 0..1000 {
            let key = Key(format!("{}", i), i);
            let value = Value::Set(format!("{}", i * 2));
            sorted.insert(key, value);
        }
        for (k, v) in sorted.iter() {
            writer.add(k, v).await.unwrap();
        }
        writer.finish().await.unwrap();
        (file, sorted)
    }
}
