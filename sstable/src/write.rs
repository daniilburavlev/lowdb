use common::DbResult;
use tokio::io::AsyncWriteExt;

pub(crate) trait ToWriter {
    async fn write<W>(&self, w: &mut W) -> DbResult<()>
    where
        W: AsyncWriteExt + Unpin;
}

pub(crate) trait ToBuffer {
    fn write_to_buf(&self, buf: &mut Vec<u8>) -> DbResult<()>;
}
