//! The `wal` crate provides structs for writing and reading write-ahead log to/from disk

use std::path::Path;

use common::{DbResult, key::Key, value::Value};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::{Mutex, MutexGuard},
};

use crate::{BATCH_CMD, OP_CMD, TX_BEGIN_CMD, TX_COMMIT_CMD, WalCmd, wal_id};

/// Main structure for writing write-ahead log to disk.
///
/// # Example
/// ```rust
/// use wal::WalWriter;
/// use wal::WalCmd;
/// use common::{key::Key, value::Value};
///
/// #[tokio::main]
/// async fn main() {
///     let mut writer = WalWriter::open(".example_write").await.unwrap();
///     writer.append_kv(&Key::new("key", 1), &Value::Delete).await.unwrap();
///     writer.append(WalCmd::TxBegin(100)).await.unwrap();
/// }
/// ```
pub struct WalWriter {
    id: String,
    file: Mutex<BufWriter<File>>,
}

impl WalWriter {
    /// Open existing or create new writer from file's path. File name used as WAL's id
    ///
    /// File is opened with `append` flag, for most filesystems, the operating system guarantees that all writes are
    /// atomic: no writes get mangled because another process writes at the same time.
    pub async fn open<P: AsRef<Path>>(path: P) -> DbResult<Self> {
        let id = wal_id(path.as_ref())?;
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        let writer = BufWriter::new(file);
        Ok(Self {
            id,
            file: Mutex::new(writer),
        })
    }

    /// Append new command to the end of the WAL file.
    ///
    /// # Record structure:
    /// [...[op u8 u64, op u8 key_len u16, key_bytes, seq u64, value_len u16, value_bytes]...]
    ///
    /// After each `append` call, file_sync called
    pub async fn append(&self, op: WalCmd) -> DbResult<()> {
        let mut writer = self.file.lock().await;
        match op {
            WalCmd::TxBegin(tx_id) => append_tx_begin(&mut writer, tx_id).await?,
            WalCmd::TxCommit(tx_id) => append_tx_commit(&mut writer, tx_id).await?,
            WalCmd::Op(key, value) => append_op(&mut writer, &key, &value).await?,
            WalCmd::Batch(batch) => append_batch(&mut writer, &batch).await?,
        }
        flush(&mut writer).await
    }

    pub async fn append_batch(&self, batch: &[(Key, Value)]) -> DbResult<()> {
        let mut writer = self.file.lock().await;
        append_batch(&mut writer, batch).await?;
        flush(&mut writer).await
    }

    /// Append new key-value pair to the end of the WAL file.
    ///
    /// # Record structure:
    /// [...[op u8, key_len u16, key_bytes, seq u64, value_len u16, value_bytes]...]
    ///
    /// After each `append_kv` call, file_sync called
    pub async fn append_kv(&self, key: &Key, value: &Value) -> DbResult<()> {
        let mut writer = self.file.lock().await;
        append_op(&mut writer, key, value).await?;
        flush(&mut writer).await
    }

    /// WAL's id, used for managing existed wals
    pub fn id(&self) -> &str {
        &self.id
    }
}

async fn append_tx_begin(writer: &mut MutexGuard<'_, BufWriter<File>>, tx_id: u64) -> DbResult<()> {
    writer.write_u8(TX_BEGIN_CMD).await?;
    writer.write_u64(tx_id).await?;
    Ok(())
}

async fn append_tx_commit(
    writer: &mut MutexGuard<'_, BufWriter<File>>,
    tx_id: u64,
) -> DbResult<()> {
    writer.write_u8(TX_COMMIT_CMD).await?;
    writer.write_u64(tx_id).await?;
    Ok(())
}

async fn append_op(
    writer: &mut MutexGuard<'_, BufWriter<File>>,
    key: &Key,
    value: &Value,
) -> DbResult<()> {
    writer.write_u8(OP_CMD).await?;

    let len: u16 = key.0.len().try_into()?;
    writer.write_u16(len).await?;
    writer.write_all(key.0.as_bytes()).await?;
    writer.write_u64(key.1).await?;

    match value {
        Value::Set(value) => {
            let len: u16 = value.len().try_into()?;
            writer.write_u16(len).await?;
            writer.write_all(value.as_bytes()).await?;
        }
        Value::Delete => writer.write_u16(0).await?,
    }

    Ok(())
}

async fn append_batch(
    writer: &mut MutexGuard<'_, BufWriter<File>>,
    batch: &[(Key, Value)],
) -> DbResult<()> {
    writer.write_u8(BATCH_CMD).await?;

    for (k, v) in batch {
        append_op(writer, k, v).await?;
    }

    Ok(())
}

async fn flush(writer: &mut BufWriter<File>) -> DbResult<()> {
    writer.flush().await?;
    let file = writer.get_mut();
    file.sync_all().await?;
    Ok(())
}
