use common::{DbResult, key::Key, value::Value};
use tokio::io::AsyncReadExt;

pub(crate) fn set_u16(b: &mut [u8], offset: usize, v: u16) {
    b[offset..offset + 2].copy_from_slice(&v.to_be_bytes());
}

pub(crate) fn set_u32(b: &mut [u8], offset: usize, v: u32) {
    b[offset..offset + 4].copy_from_slice(&v.to_be_bytes());
}

pub(crate) fn put_u16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_be_bytes());
}

pub(crate) fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}

pub(crate) fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}

pub(crate) fn encode_key(buf: &mut Vec<u8>, key: &Key) -> DbResult<()> {
    let len: u16 = key.0.len().try_into()?;
    put_u16(buf, len);
    buf.extend_from_slice(key.0.as_bytes());
    put_u64(buf, key.1);
    Ok(())
}

pub(crate) fn encode_value(buf: &mut Vec<u8>, value: &Value) -> DbResult<()> {
    match value {
        Value::Set(value) => {
            let len: u16 = value.len().try_into()?;
            put_u16(buf, len);
            buf.extend_from_slice(value.as_bytes());
        }
        Value::Delete => put_u16(buf, 0),
    }
    Ok(())
}

pub(crate) fn encode(buf: &mut Vec<u8>, key: &Key, value: &Value) -> DbResult<()> {
    encode_key(buf, key)?;
    encode_value(buf, value)?;
    Ok(())
}

pub(crate) async fn read_value<R>(r: &mut R) -> DbResult<Value>
where
    R: AsyncReadExt + Unpin,
{
    match r.read_u16().await? {
        0 => Ok(Value::Delete),
        l => {
            let mut value = vec![0u8; l as usize];
            r.read_exact(&mut value).await?;
            let value = String::from_utf8_lossy(&value).to_string();
            Ok(Value::Set(value))
        }
    }
}
