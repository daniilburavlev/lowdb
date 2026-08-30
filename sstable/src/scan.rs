use std::path::PathBuf;

use common::DbResult;

use crate::{
    meta::SSTableMeta,
    scan::{merge::MergeScan, table::TableScan},
    writer::SSTableWriter,
};

pub(crate) mod merge;
pub(crate) mod table;

pub async fn compact(
    out: impl Into<PathBuf>,
    inputs: Vec<TableScan>,
) -> DbResult<Option<SSTableMeta>> {
    let mut m = MergeScan::new(inputs).await?;
    let mut w = SSTableWriter::create(out).await?;
    while let Some((k, v)) = m.next().await? {
        w.add(&k, &v).await?;
    }
    w.finish().await
}
