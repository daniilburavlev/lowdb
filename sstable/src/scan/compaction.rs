use common::{DbResult, key::Key, value::Value};

use crate::scan::merge::MergeScan;

pub(crate) struct CompactionScan {
    scan: MergeScan,
    curr: Option<(Key, Value)>,
    drop_tombstone: bool,
    read_ts_watermark: u64,
}

impl CompactionScan {
    pub(crate) async fn new(
        mut scan: MergeScan,
        drop_tombstone: bool,
        read_ts_watermark: u64,
    ) -> DbResult<Self> {
        let curr = scan.next().await?;
        Ok(Self {
            scan,
            curr,
            drop_tombstone,
            read_ts_watermark,
        })
    }

    pub(crate) async fn next(&mut self) -> DbResult<Option<(Key, Value)>> {
        if self.curr.is_none() {
            return Ok(None);
        }
        while let Some((key, value)) = self.scan.next().await? {
            let next = matches!(&self.curr, Some((k, _)) 
                if key.0 != k.0 || k.1 > self.read_ts_watermark || k.1 > self.read_ts_watermark && key.1 <= self.read_ts_watermark);
            if next {
                let curr = self.curr.take();
                self.curr = Some((key, value));
                if !self.drop_tombstone {
                    return Ok(curr);
                }
                if !matches!(curr, Some((_, Value::Delete))) {
                    return Ok(curr);
                }
            }
        }
        match self.curr.take() {
            Some((_, Value::Delete)) if self.drop_tombstone => Ok(None),
            o => Ok(o),
        }
    }
}
