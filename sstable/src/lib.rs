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
