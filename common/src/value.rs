#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Set(String),
    Delete,
}

impl Value {
    pub fn set(value: &str) -> Self {
        Self::Set(value.to_string())
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Value::Delete)
    }

    pub fn heap_size(&self) -> usize {
        match self {
            Self::Set(value) => value.len(),
            Self::Delete => 0,
        }
    }

    pub fn disk_size(&self) -> usize {
        2 + self.heap_size()
    }
}
