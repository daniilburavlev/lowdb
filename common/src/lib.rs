#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! Common types, errors, functions used in other crates

use crate::error::DbError;

pub mod error;
pub mod key;
pub mod lookup;
pub mod value;

/// Result wrapper returned by all funcation
pub type DbResult<T> = Result<T, DbError>;
