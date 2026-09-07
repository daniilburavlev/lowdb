//! A value module

/// Represents operation with the key
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// Store value
    Set(String),
    /// Mark key deleted
    Delete,
}

impl Value {
    /// Create `Value::Set` from string pointer
    pub fn set(value: &str) -> Self {
        Self::Set(value.to_string())
    }

    /// Is deleted
    pub fn is_delete(&self) -> bool {
        matches!(self, Value::Delete)
    }

    /// in-memory size in bytes
    pub fn heap_size(&self) -> usize {
        match self {
            Self::Set(value) => value.len(),
            Self::Delete => 0,
        }
    }

    /// on-disk size in bytes
    pub fn disk_size(&self) -> usize {
        2 + self.heap_size()
    }
}
