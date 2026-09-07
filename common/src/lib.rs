#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! Contains common types, errors, functions for usage in other crates

use crate::error::DbError;

pub mod error;
pub mod key;
pub mod lookup;
pub mod value;

/// Main database result type used in all other crates
pub type DbResult<T> = Result<T, DbError>;
