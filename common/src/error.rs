use std::{
    array::TryFromSliceError,
    num::{ParseIntError, TryFromIntError},
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("{0}")]
    InvalidInt(String),
    #[error("IO :{0}")]
    IO(#[from] std::io::Error),
    #[error("{0}")]
    Unexpected(String),
    #[error("{0}")]
    InvalidValue(String),
    #[error("{0}")]
    InvalidState(String),
}

impl DbError {
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
