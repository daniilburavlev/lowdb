//! Storage's state
use std::sync::Arc;

use common::{DbResult, lookup::Lookup};
use memtable::MemTable;
use wal::{WalCmd, WalWriter};

use crate::storage::DiskStorage;

/// WAL, mem_table, frozen tables and storage pointers wrapper
#[derive(Clone)]
pub struct State {
    pub(crate) wal: Arc<WalWriter>,
    pub(crate) mem_table: Arc<MemTable>,
    pub(crate) frozen: Vec<Arc<MemTable>>,
    pub(crate) storage: Arc<DiskStorage>,
}

impl State {
    pub(crate) fn new(
        wal: WalWriter,
        frozen: Vec<MemTable>,
        storage: DiskStorage,
    ) -> DbResult<Self> {
        let frozen = frozen.into_iter().map(Arc::new).collect();
        let id: u64 = wal.id().parse()?;
        Ok(Self {
            wal: Arc::new(wal),
            mem_table: Arc::new(MemTable::new(id)),
            frozen,
            storage: Arc::new(storage),
        })
    }

    /// Write transaction begining in WAL file
    pub async fn begin(&self, tx_id: u64) -> DbResult<()> {
        self.wal.append(WalCmd::TxBegin(tx_id)).await
    }

    /// Write transaction commit in WAL file
    pub async fn commit(&self, tx_id: u64) -> DbResult<()> {
        self.wal.append(WalCmd::TxCommit(tx_id)).await
    }

    /// Get snapshot value by key, version is less or equal given seq
    pub async fn get_snap(&self, key: &str, seq: u64) -> DbResult<Option<String>> {
        match self.mem_table.get_snap(key, seq) {
            Lookup::Found(value) => return Ok(Some(value)),
            Lookup::Deleted => return Ok(None),
            _ => {}
        }
        for mt in &self.frozen {
            match mt.get_snap(key, seq) {
                Lookup::Found(value) => return Ok(Some(value)),
                Lookup::Deleted => return Ok(None),
                _ => {}
            }
        }
        match self.storage.get_snap(key, seq).await? {
            Lookup::Found(value) => Ok(Some(value)),
            _ => Ok(None),
        }
    }
}
