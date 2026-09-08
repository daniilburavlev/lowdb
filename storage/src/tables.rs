use std::sync::Arc;

use sstable::table::SSTable;

#[derive(Debug, Clone)]
pub(crate) struct SSTables(Vec<Arc<SSTable>>);

impl SSTables {
    pub(crate) fn new() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn push(&mut self, table: Arc<SSTable>) {
        self.0.push(table);
    }

    pub(crate) fn insert(&mut self, at: usize, table: Arc<SSTable>) {
        self.0.insert(at, table);
    }

    pub(crate) fn search(self) -> std::vec::IntoIter<Arc<SSTable>> {
        self.0.into_iter()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}
