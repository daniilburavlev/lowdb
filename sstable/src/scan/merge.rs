use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use common::{DbResult, error::DbError, key::Key, value::Value};

use crate::scan::table::TableScan;

struct HeapKey {
    key: Key,
    idx: usize,
}

impl Ord for HeapKey {
    fn cmp(&self, o: &Self) -> Ordering {
        self.key.cmp(&o.key).then(self.idx.cmp(&o.idx))
    }
}

impl PartialOrd for HeapKey {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl PartialEq for HeapKey {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}

impl Eq for HeapKey {}

pub(crate) struct MergeScan {
    scans: Vec<TableScan>,
    pending: Vec<Option<Value>>,
    heap: BinaryHeap<Reverse<HeapKey>>,
}

impl MergeScan {
    pub(crate) async fn new(scans: Vec<TableScan>) -> DbResult<Self> {
        let n = scans.len();
        let mut me = Self {
            scans,
            pending: vec![None; n],
            heap: BinaryHeap::new(),
        };
        for i in 0..n {
            me.advance(i).await?;
        }
        Ok(me)
    }

    async fn advance(&mut self, i: usize) -> DbResult<()> {
        if let Some((k, v)) = self.scans[i].next().await? {
            self.pending[i] = Some(v);
            self.heap.push(Reverse(HeapKey { key: k, idx: i }));
        } else {
            self.pending[i] = None;
        }
        Ok(())
    }

    pub(crate) async fn next(&mut self) -> DbResult<Option<(Key, Value)>> {
        let Some(Reverse(HeapKey { key, idx })) = self.heap.pop() else {
            return Ok(None);
        };
        let v = self.pending[idx]
            .take()
            .ok_or(DbError::invalid_state("pending set"))?;
        self.advance(idx).await?;
        Ok(Some((key, v)))
    }
}
