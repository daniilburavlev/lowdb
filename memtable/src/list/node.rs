use std::{cmp::Ordering, ptr, sync::atomic::AtomicPtr};

use common::{key::Key, value::Value};

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) key: Key,
    pub(crate) value: Value,
    // Tower of nodes by levels 0..height
    pub(crate) tower: Box<[AtomicPtr<Node>]>,
}

impl Node {
    pub(crate) fn alloc(key: Key, value: Value, height: usize) -> *mut Node {
        let tower = (0..height)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Box::into_raw(Box::new(Node { key, value, tower }))
    }

    pub(crate) fn heap_size(&self) -> usize {
        std::mem::size_of::<Node>()
            + self.key.heap_size()
            + self.value.heap_size()
            + self.tower.len() * std::mem::size_of::<AtomicPtr<Node>>()
    }
}

pub(crate) fn cmp_key(stored: &Key, key: &str, seq: u64) -> Ordering {
    stored.0.as_str().cmp(key).then_with(|| seq.cmp(&stored.1))
}
