#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! Memory table implementation based on skip list.

use common::{key::Key, lookup::Lookup, value::Value};

use crate::list::SkipList;

pub(crate) mod list;

pub use list::Iter;

// Max size of table in heap memory
const MAX_SIZE: usize = 64 * 1024 * 1024;

/// Skip list wrapper, counting size in heap
///
/// # Example
/// ```rust
/// use memtable::MemTable;
/// use common::lookup::Lookup;
/// use common::key::Key;
/// use common::value::Value;
///
/// let table = MemTable::new(0);
/// table.put(Key::new("key", 1), Value::set("value"));
/// assert_eq!(table.get("key"), Lookup::found("value"));
/// ```
pub struct MemTable {
    id: u64,
    skip_list: SkipList,
    max_size: usize,
}

impl MemTable {
    /// Create new memory table with 64Mb max size
    pub fn new(id: u64) -> Self {
        Self::with_max_size(id, MAX_SIZE)
    }

    /// Create new memory table with custom max size
    pub fn with_max_size(id: u64, max_size: usize) -> Self {
        let skip_list = SkipList::new();
        Self {
            id,
            skip_list,
            max_size,
        }
    }

    /// Put key-value pair in skip list
    pub fn put(&self, key: Key, value: Value) {
        self.skip_list.insert(key, value);
    }

    /// Get latest stored value version by key
    pub fn get(&self, key: &str) -> Lookup {
        self.get_seq(key, u64::MAX)
    }

    /// Get stored value with max seq less than received
    pub fn get_seq(&self, key: &str, seq: u64) -> Lookup {
        match self.skip_list.get_at(key, seq).cloned() {
            Some(Value::Set(value)) => Lookup::Found(value),
            Some(Value::Delete) => Lookup::Deleted,
            None => Lookup::Absent,
        }
    }

    /// Check is available heap space is full
    pub fn is_full(&self) -> bool {
        let curr = self.skip_list.mem_usage();
        curr >= self.max_size
    }

    /// Get key-value iterator
    pub fn iter(&self) -> Iter<'_> {
        self.skip_list.iter()
    }

    /// Amount of elements
    pub fn len(&self) -> usize {
        self.skip_list.len()
    }

    /// Check is table is empty
    pub fn is_empty(&self) -> bool {
        self.skip_list.is_empty()
    }

    /// Table's id
    pub fn id(&self) -> u64 {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn get_set() {
        let mem_table = MemTable::new(0);
        for i in 0..100 {
            let key = Key(format!("{}", i), 1);
            let value = Value::Set(format!("{}", i * 2));
            mem_table.put(key, value);
        }
        for i in 0..100 {
            let key = format!("{}", i);
            let expected = format!("{}", i * 2);
            let Lookup::Found(value) = mem_table.get(&key) else {
                panic!("value not found");
            };
            assert_eq!(expected, value);
        }
    }

    #[test]
    fn mark_deleted() {
        let mem_table = MemTable::new(0);
        mem_table.put(Key::new("1", 0), Value::Delete);
        assert_eq!(mem_table.id(), 0);
        assert_eq!(mem_table.get("1"), Lookup::Deleted);
    }

    #[test]
    fn get_empty() {
        let mem_table = MemTable::new(0);
        assert!(mem_table.is_empty());
        assert!(!mem_table.is_full());
        assert_eq!(mem_table.len(), 0);
        assert_eq!(mem_table.get("empty"), Lookup::Absent);
    }

    #[test]
    fn iter() {
        let mem_table = MemTable::new(0);
        let mut existed = HashSet::new();
        for i in 0..100 {
            let value = format!("{i}");
            mem_table.put(Key::new(&value, i), Value::set(&value));
            existed.insert(value);
        }
        for (k, _) in mem_table.iter() {
            existed.remove(&k.0);
        }
        assert!(existed.is_empty());
    }
}
