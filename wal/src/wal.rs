use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
};

use common::{DbResult, error::DbError, key::Key, value::Value};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::Mutex,
};

pub struct WalWriter {
    id: String,
    file: Mutex<BufWriter<File>>,
}

impl WalWriter {
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

    pub async fn append(&self, key: &Key, value: &Value) -> DbResult<()> {
        let mut writer = self.file.lock().await;

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

        writer.flush().await?;
        let file = writer.get_mut();
        file.sync_all().await?;
        Ok(())
    }

    pub fn id(&self) -> &str {
        &self.id
    }
}

pub struct WalReader {
    id: String,
    reader: BufReader<File>,
}

impl WalReader {
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

    pub async fn next(&mut self) -> DbResult<Option<(Key, Value)>> {
        let key = match self.read_key().await? {
            Some(key) => key,
            None => return Ok(None),
        };
        let value = self.read_value().await?;
        Ok(Some((key, value)))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    async fn read_key(&mut self) -> DbResult<Option<Key>> {
        let len = match self.reader.read_u16().await {
            Ok(len) => len,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(DbError::IO(e)),
        };
        let key = self.read_str(len).await?;
        let seq = self.reader.read_u64().await?;
        Ok(Some(Key(key, seq)))
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

fn wal_id(path: &Path) -> DbResult<String> {
    let id = PathBuf::from(path)
        .file_name()
        .ok_or(DbError::invalid_state("cannot get wal filename"))?
        .to_string_lossy()
        .to_string();
    Ok(id)
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn wal_operation_write_read() {
        let file = NamedTempFile::new().unwrap();
        let writer = WalWriter::open(file.path()).await.unwrap();

        let key = Key::new("key", 1);
        let value = Value::set("value");
        writer.append(&key, &value).await.unwrap();
        drop(writer);

        let mut reader = WalReader::open(file.path()).await.unwrap();
        let result = reader.next().await.unwrap().unwrap();

        assert_eq!((key, value), result);
    }

    #[tokio::test]
    async fn wal_key_value() {
        let file = NamedTempFile::new().unwrap();
        let writer = WalWriter::open(file.path()).await.unwrap();

        let key = Key::new("long_enough_key", 1);
        let value = Value::set("short");
        writer.append(&key, &value).await.unwrap();
        drop(writer);

        let mut reader = WalReader::open(file.path()).await.unwrap();
        let result = reader.next().await.unwrap().unwrap();

        assert_eq!((key, value), result);
    }

    #[tokio::test]
    async fn long_key() {
        let file = NamedTempFile::new().unwrap();
        let writer = WalWriter::open(file.path()).await.unwrap();

        let key = Key("k".repeat((u16::MAX as usize) + 5), 1);
        let value = Value::set("value");

        assert!(
            matches!(
                writer.append(&key, &value).await.err().unwrap(),
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
