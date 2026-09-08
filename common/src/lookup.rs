//! Current state of the key

/// State enum
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// Key existed and has value
    Found(String),
    /// Key was deleted
    Deleted,
    /// Key is not found
    Absent,
}
