use common::DbResult;
use tokio::io::AsyncReadExt;

pub(crate) trait FromReader: Sized {
    async fn read<R>(r: &mut R) -> DbResult<Self>
    where
        R: AsyncReadExt + Unpin;
}
