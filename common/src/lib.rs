use crate::error::DbError;

pub mod error;
pub mod key;
pub mod value;

pub type DbResult<T> = Result<T, DbError>;
