use common::key::Key;

use crate::{
    encode::{put_u16, put_u64},
    read::ReadFrom,
    write::WriteToBuf,
};

impl ReadFrom for Key {
    async fn read<R>(r: &mut R) -> common::DbResult<Self>
    where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        let len = r.read_u16().await?;
        let mut buf = vec![0u8; len as usize];
        r.read_exact(&mut buf).await?;
        let seq = r.read_u64().await?;
        let key = String::from_utf8_lossy(&buf).to_string();
        Ok(Key(key, seq))
    }
}

impl WriteToBuf for Key {
    fn write_to_buf(&self, buf: &mut Vec<u8>) -> common::DbResult<()> {
        let len: u16 = self.0.len().try_into()?;
        put_u16(buf, len);
        buf.extend_from_slice(self.0.as_bytes());
        put_u64(buf, self.1);
        Ok(())
    }
}
