use std::{iter::Rev, sync::Arc};

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

    /// Iterate through the elements to burn to disk: Oldest -> Newest
    pub(crate) fn flush(self) -> Rev<std::vec::IntoIter<Arc<SSTable>>> {
        self.0.into_iter().rev()
    }

    /// Iterate through the elements to search: Newest -> Oldest
    pub(crate) fn search(self) -> std::vec::IntoIter<Arc<SSTable>> {
        self.0.into_iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}
