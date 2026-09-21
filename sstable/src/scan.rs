use std::path::PathBuf;

use common::DbResult;

use crate::{
    meta::SSTableMeta,
    scan::{compaction::CompactionScan, merge::MergeScan, table::TableScan},
    writer::SSTableWriter,
};

pub(crate) mod compaction;
pub(crate) mod merge;
pub(crate) mod table;

pub async fn compact(
    out: impl Into<PathBuf>,
    inputs: Vec<TableScan>,
    drop_tombstone: bool,
    read_ts_watermark: u64,
) -> DbResult<Option<SSTableMeta>> {
    let m = MergeScan::new(inputs).await?;
    let mut c = CompactionScan::new(m, drop_tombstone, read_ts_watermark).await?;
    let mut w = SSTableWriter::create(out).await?;
    while let Some((k, v)) = c.next().await? {
        w.add(&k, &v).await?;
    }
    w.finish().await
}

#[cfg(test)]
mod tests {
    use common::{key::Key, value::Value};
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn compaction_with_bottom_read_ts_line() {
        let dir = tempdir().unwrap();
        let mut tables = vec![];

        for i in 0..10 {
            let path = dir.path().join(format!("{}", i));
            let mut writer = SSTableWriter::create(&path).await.unwrap();

            let key = Key::new("key", i);
            let value = Value::Set(format!("{}", i * i));
            writer.add(&key, &value).await.unwrap();
            writer.finish().await.unwrap();

            tables.push(TableScan::open(&path).await.unwrap());
        }
        let path = dir.path().join("final");
        compact(&path, tables, false, 7).await.unwrap();

        let mut scan = TableScan::open(&path).await.unwrap();
        assert_eq!(
            Some((Key::new("key", 9), Value::set("81"))),
            scan.next().await.unwrap()
        );
        assert_eq!(
            Some((Key::new("key", 8), Value::set("64"))),
            scan.next().await.unwrap()
        );
        assert_eq!(
            Some((Key::new("key", 7), Value::set("49"))),
            scan.next().await.unwrap()
        );
        assert!(scan.next().await.unwrap().is_none());
    }
}
