//! Error module
use std::{
    array::TryFromSliceError,
    num::{ParseIntError, TryFromIntError},
};

use thiserror::Error;

/// Errors returned by database engine
///
/// Variants fall into three groups:
///
/// - **Caller mistakes**: [`InvalidInt`](Self::InvalidInt),[`InvalidValue`](Self::InvalidValue).Fix the input; retrying won't help.
/// - **Environment/storage problems**: [`IO`](Self::IO),[`InvalidState`](Self::InvalidState).
/// - **Concurrency**: [`CommitConflict`](Self::CommitConflict). Safe to retry.
#[derive(Debug, Error)]
pub enum DbError {
    /// A string could not be parsed as an integer.
    ///
    /// The payload is a human-readable description of the offending input.
    #[error("invalid integer: {0}")]
    InvalidInt(String),

    /// An underlying I/O operation falied (e.g. reading or writing the data file).
    ///
    /// The original [`std::io::Error`] is available via [`source()`](std::error::Error::source).
    #[error("I/O error:{0}")]
    IO(#[from] std::io::Error),

    /// The input wal well-formed but rejected, e.g. a key that is empty or a value that exceeds the
    /// size limit.
    ///
    /// The payload describes which value was rejected and why.
    #[error("invalid value :{0}")]
    InvalidValue(String),

    /// Stored data or internal state is inconsistent, for example a truncated or corrupted file.
    ///
    /// This is usually indicates corrupted or a bag rather than a caller mistake, so relying is
    /// unikely to help.
    #[error("invalid state: {0}")]
    InvalidState(String),

    /// The transaction could not be commited because another transaction modified the same data
    /// first.
    ///
    /// Nothing was written. Is is safe to retry the whole transaction.
    #[error("commit conflict")]
    CommitConflict,
}

impl DbError {
    /// Crates `DbError::InvalidState` fro string reference
    pub fn invalid_state(msg: &str) -> Self {
        Self::InvalidState(msg.to_string())
    }
}

impl From<TryFromIntError> for DbError {
    fn from(err: TryFromIntError) -> Self {
        Self::InvalidInt(err.to_string())
    }
}

impl From<TryFromSliceError> for DbError {
    fn from(err: TryFromSliceError) -> Self {
        Self::InvalidValue(err.to_string())
    }
}

impl From<ParseIntError> for DbError {
    fn from(err: ParseIntError) -> Self {
        Self::InvalidValue(err.to_string())
    }
}
