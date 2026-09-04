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
) -> DbResult<Option<SSTableMeta>> {
    let m = MergeScan::new(inputs).await?;
    let mut c = CompactionScan::new(m).await?;
    let mut w = SSTableWriter::create(out).await?;
    while let Some((k, v)) = c.next().await? {
        println!("store: {:?} {:?}", k, v);
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
    async fn compact_10_tables() {
        let dir = tempdir().unwrap();
        let mut tables = vec![];

        for i in 0..10 {
            let path = dir.path().join(format!("{}", i));
            let mut writer = SSTableWriter::create(&path).await.unwrap();

            let key = Key(format!("{}", 1), i);
            let value = Value::Set(format!("{}", i * i));
            writer.add(&key, &value).await.unwrap();
            writer.finish().await.unwrap().unwrap();

            tables.push(TableScan::open(&path).await.unwrap());
        }
        let path = dir.path().join("final");
        compact(&path, tables).await.unwrap();

        let mut scan = TableScan::open(&path).await.unwrap();
        let (key, value) = scan.next().await.unwrap().unwrap();
        assert!(scan.next().await.unwrap().is_none());
        assert_eq!(key.0, "1");
        assert_eq!(key.1, 9);
        assert_eq!(value, Value::set("81"));
    }
}
