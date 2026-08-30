#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Key(pub String, pub u64);

impl Key {
    pub fn new(key: &str, seq: u64) -> Self {
        Self(key.to_owned(), seq)
    }

    pub fn heap_size(&self) -> usize {
        self.0.len() + 8
    }

    pub fn disk_size(&self) -> usize {
        2 + self.heap_size()
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0).then_with(|| other.1.cmp(&self.1))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}
