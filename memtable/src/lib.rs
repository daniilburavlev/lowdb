use std::sync::atomic::AtomicUsize;

use common::{key::Key, lookup::Lookup, value::Value};

use crate::list::SkipList;

pub(crate) mod list;

pub use list::Iter;

const MAX_SIZE: usize = 1024 * 1024;

pub struct MemTable {
    skip_list: SkipList,
    size: AtomicUsize,
    max_size: usize,
}

impl MemTable {
    pub fn with_max_size(max_size: usize) -> Self {
        let skip_list = SkipList::new();
        Self {
            skip_list,
            size: AtomicUsize::new(0),
            max_size,
        }
    }

    pub fn set(&self, key: Key, value: Value) {
        let size = key.heap_size();
        self.skip_list.insert(key, value);
        self.size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get(&self, key: &str) -> Lookup {
        match self.skip_list.get(key).cloned() {
            Some(Value::Set(value)) => Lookup::Found(value),
            Some(Value::Delete) => Lookup::Deleted,
            None => Lookup::Absent,
        }
    }

    pub fn is_full(&self) -> bool {
        let curr = self.skip_list.mem_usage();
        curr >= self.max_size
    }

    pub fn iter(&self) -> Iter<'_> {
        self.skip_list.iter()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::with_max_size(MAX_SIZE)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn get_set() {
        let mem_table = MemTable::default();
        for i in 0..100 {
            let key = Key(format!("{}", i), 1);
            let value = Value::Set(format!("{}", i * 2));
            mem_table.set(key, value);
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
}
