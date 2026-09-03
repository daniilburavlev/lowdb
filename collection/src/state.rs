use std::sync::Arc;

use common::DbResult;
use memtable::MemTable;
use wal::wal::WalWriter;

use crate::storage::Storage;

#[derive(Clone)]
pub(crate) struct State {
    pub(crate) wal: Arc<WalWriter>,
    pub(crate) mem_table: Arc<MemTable>,
    pub(crate) frozen: Vec<Arc<MemTable>>,
    pub(crate) storage: Arc<Storage>,
}

impl State {
    pub(crate) fn new(wal: WalWriter, frozen: Vec<MemTable>, storage: Storage) -> DbResult<Self> {
        let frozen = frozen.into_iter().map(Arc::new).collect();
        let id: u64 = wal.id().parse()?;
        Ok(Self {
            wal: Arc::new(wal),
            mem_table: Arc::new(MemTable::new(id)),
            frozen,
            storage: Arc::new(storage),
        })
    }
}
