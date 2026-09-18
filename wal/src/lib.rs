use std::path::{Path, PathBuf};

use common::{DbResult, error::DbError, key::Key, value::Value};

mod reader;
mod writer;

pub(crate) const TX_BEGIN_CMD: u8 = 1;
pub(crate) const TX_COMMIT_CMD: u8 = 2;
pub(crate) const OP_CMD: u8 = 3;

pub use reader::WalReader;
pub use writer::WalWriter;

/// WAL command
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalCmd {
    TxBegin(u64),
    TxCommit(u64),
    Op(Key, Value),
}

pub(crate) fn wal_id<P: AsRef<Path>>(path: P) -> DbResult<String> {
    let id = PathBuf::from(path.as_ref())
        .file_name()
        .ok_or(DbError::invalid_state("cannot get wal filename"))?
        .to_string_lossy()
        .to_string();
    Ok(id)
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::{reader::WalReader, writer::WalWriter};

    use super::*;

    #[tokio::test]
    async fn wal_operation_write_read() {
        let file = NamedTempFile::new().unwrap();
        let writer = WalWriter::open(file.path()).await.unwrap();

        let key = Key::new("key", 1);
        let value = Value::set("value");
        writer
            .append(WalCmd::Op(key.clone(), value.clone()))
            .await
            .unwrap();
        writer
            .append(WalCmd::Op(key.clone(), Value::Delete))
            .await
            .unwrap();

        let writer_id = writer.id().to_owned();
        drop(writer);

        let mut reader = WalReader::open(file.path()).await.unwrap();

        let result = reader.next().await.unwrap().unwrap();
        assert_eq!(WalCmd::Op(key.clone(), value), result);

        let result = reader.next().await.unwrap().unwrap();
        assert_eq!(WalCmd::Op(key, Value::Delete), result);

        assert_eq!(reader.id(), writer_id);
    }

    #[tokio::test]
    async fn wal_key_value() {
        let file = NamedTempFile::new().unwrap();
        let writer = WalWriter::open(file.path()).await.unwrap();

        let key = Key::new("long_enough_key", 1);
        let value = Value::set("short");
        writer
            .append(WalCmd::Op(key.clone(), value.clone()))
            .await
            .unwrap();
        drop(writer);

        let mut reader = WalReader::open(file.path()).await.unwrap();
        let result = reader.next().await.unwrap().unwrap();

        assert_eq!(WalCmd::Op(key, value), result);
    }

    #[tokio::test]
    async fn long_key() {
        let file = NamedTempFile::new().unwrap();
        let writer = WalWriter::open(file.path()).await.unwrap();

        let key = Key("k".repeat((u16::MAX as usize) + 5), 1);
        let value = Value::set("value");

        assert!(
            matches!(
                writer.append(WalCmd::Op(key, value)).await.err().unwrap(),
                DbError::InvalidInt(_)
            ),
            "should validate u16 overflow"
        );
    }

    #[tokio::test]
    async fn read_empty_file() {
        let file = NamedTempFile::new().unwrap();

        let mut reader = WalReader::open(file.path()).await.unwrap();
        assert!(reader.next().await.unwrap().is_none());
    }
}
