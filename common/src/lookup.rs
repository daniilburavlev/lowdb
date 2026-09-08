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

impl Lookup {
    /// Create `Lookup::Found` from string pointer
    pub fn found(value: &str) -> Self {
        Self::Found(value.to_owned())
    }
}
