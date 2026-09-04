use common::{DbResult, key::Key, value::Value};

use crate::scan::merge::MergeScan;

pub(crate) struct CompactionScan {
    curr: Option<(Key, Value)>,
    scan: MergeScan,
}

impl CompactionScan {
    pub(crate) async fn new(mut scan: MergeScan) -> DbResult<Self> {
        let curr = scan.next().await?;
        Ok(Self { curr, scan })
    }

    pub(crate) async fn next(&mut self) -> DbResult<Option<(Key, Value)>> {
        if self.curr.is_none() {
            return Ok(None);
        }
        while let Some((k, v)) = self.scan.next().await? {
            let next = matches!(&self.curr, Some((key, _)) if key.0 != k.0);
            if next {
                let curr = self.curr.take();
                self.curr = Some((k, v));
                if !matches!(curr, Some((_, Value::Delete))) {
                    return Ok(curr);
                }
            }
        }
        Ok(self.curr.take())
    }
}
