#![deny(unreachable_pub)]
#![warn(missing_docs)]
//! The `wal` crate provides structs for writing and reading write-ahead log to/from disk

use std::{io::ErrorKind, path::Path};

use common::{DbResult, error::DbError, key::Key, value::Value};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, BufReader},
};

use crate::{BATCH_CMD, OP_CMD, TX_BEGIN_CMD, TX_COMMIT_CMD, WalCmd, wal_id};

/// Main WAL reader structure, used for reading key-value pairs from existing file
///
/// # Example
/// ```rust
/// use tempfile::NamedTempFile;
/// use wal::WalReader;
/// use wal::WalCmd;
///
/// #[tokio::main]
/// async fn main() {
///     let file = NamedTempFile::new().unwrap();
///     let mut reader = WalReader::open(file.path()).await.unwrap();
///     if let Some(WalCmd::Op(tx_id, k, v)) = reader.next().await.unwrap() {
///         println!("tx_id: {tx_id} key: {:?} value: {:?}", k, v);
///     }
/// }
/// ```
pub struct WalReader {
    id: String,
    reader: BufReader<File>,
}

impl WalReader {
    /// Open existing file by path
    pub async fn open<P: AsRef<Path>>(path: P) -> DbResult<Self> {
        let id = wal_id(path.as_ref())?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;
        let reader = BufReader::new(file);
        Ok(Self { id, reader })
    }

    /// Gets next key-value pair from WAL file
    pub async fn next(&mut self) -> DbResult<Option<WalCmd>> {
        let cmd = match self.reader.read_u8().await {
            Ok(cmd) => cmd,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(DbError::IO(e)),
        };
        match cmd {
            TX_BEGIN_CMD => {
                let tx_id = self.reader.read_u64().await?;
                Ok(Some(WalCmd::TxBegin(tx_id)))
            }
            TX_COMMIT_CMD => {
                let tx_id = self.reader.read_u64().await?;
                Ok(Some(WalCmd::TxCommit(tx_id)))
            }
            OP_CMD => {
                let tx_id = self.reader.read_u64().await?;
                let (key, value) = self.read_key_value().await?;
                Ok(Some(WalCmd::Op(tx_id, key, value)))
            }
            BATCH_CMD => {
                let len = self.reader.read_u16().await?;
                let tx_id = self.reader.read_u64().await?;
                let mut batch = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let (key, value) = self.read_key_value().await?;
                    batch.push((key, value));
                }
                Ok(Some(WalCmd::Batch(tx_id, batch)))
            }
            e => Err(DbError::InvalidState(format!(
                "unexpected WAL command: {}",
                e
            ))),
        }
    }

    /// WAL's id, used for managing existed wals
    pub fn id(&self) -> &str {
        &self.id
    }

    async fn read_key_value(&mut self) -> DbResult<(Key, Value)> {
        let key = self.read_key().await?;
        let value = self.read_value().await?;
        Ok((key, value))
    }

    async fn read_key(&mut self) -> DbResult<Key> {
        let len = match self.reader.read_u16().await {
            Ok(len) => len,
            Err(e) => return Err(DbError::IO(e)),
        };
        let key = self.read_str(len).await?;
        let seq = self.reader.read_u64().await?;
        Ok(Key(key, seq))
    }

    async fn read_value(&mut self) -> DbResult<Value> {
        let len = self.reader.read_u16().await?;
        if len == 0 {
            return Ok(Value::Delete);
        }
        let value = self.read_str(len).await?;
        Ok(Value::Set(value))
    }

    async fn read_str(&mut self, len: u16) -> DbResult<String> {
        let mut value = vec![0u8; len as usize];
        self.reader.read_exact(&mut value).await?;
        Ok(String::from_utf8_lossy(&value).to_string())
    }
}
