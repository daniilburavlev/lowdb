//! Partially implemented (inserts only) non-blocking skip list
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::{cell::Cell, sync::atomic::AtomicU64};

use common::key::Key;
use common::value::Value;

use Ordering::{Acquire, Relaxed, Release};

use crate::list::node::{Node, cmp_key};
use std::cmp::Ordering as Cmp;

pub(crate) mod node;

/// Maximum number of layers (levels) stacked on top of each other
pub(crate) const MAX_HEIGHT: usize = 16;
/// Chance to grow height is `1/BRANCHING` (25%)
const BRANCHING: u64 = 4;

/// Stores sorted nodes
/// [0]-------->[3]
/// [0]-->[1]-->[3]
pub(crate) struct SkipList {
    // First element's tower
    head: [AtomicPtr<Node>; MAX_HEIGHT],
    max_height: AtomicUsize,
    len: AtomicUsize,
    mem_usage: AtomicUsize,
}

unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl SkipList {
    pub(crate) fn new() -> Self {
        Self {
            head: std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            max_height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            mem_usage: AtomicUsize::new(0),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len.load(Relaxed)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn mem_usage(&self) -> usize {
        self.mem_usage.load(Relaxed)
    }

    // Get node by level from `pred` if not empty, otherwise get by level from head
    #[inline]
    fn slot(&self, pred: *mut Node, level: usize) -> &AtomicPtr<Node> {
        match unsafe { pred.as_ref() } {
            Some(node) => &node.tower[level],
            None => &self.head[level],
        }
    }

    // Generate random height
    // Compare height with existing max height in CAS loop
    //   if height is greater than current, try to swap
    //
    // Allocate new node by key value and tower of current height
    //
    // Create preds array of length 16
    // Create succs array of length 16
    //
    // Try find current key seq pair; if pair exists, drop allocated node and return false
    //
    // If found value equal source => drop allocated node and return false
    //
    // Make new node current level of tower point to succ level
    // Replace pred level tower to point to new node in CAS loop
    pub(crate) fn insert(&self, key: Key, value: Value) -> bool {
        let height = random_height();
        let mut observed = self.max_height.load(Relaxed);
        while height > observed {
            match self
                .max_height
                .compare_exchange_weak(observed, height, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(actual) => observed = actual,
            }
        }

        let node = Node::alloc(key, value, height);
        let (key, seq) = unsafe { ((*node).key.0.clone(), (*node).key.1) };
        let mut preds = [ptr::null_mut::<Node>(); MAX_HEIGHT];
        let mut succs = [ptr::null_mut::<Node>(); MAX_HEIGHT];
        unsafe {
            loop {
                if self.find(&key, seq, &mut preds, &mut succs) {
                    drop(Box::from_raw(node));
                    return false;
                }
                (*node).tower[0].store(succs[0], Relaxed);
                if self
                    .slot(preds[0], 0)
                    .compare_exchange(succs[0], node, Release, Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
            let size = (*node).heap_size();
            self.len.fetch_add(1, Relaxed);
            self.mem_usage.fetch_add(size, Relaxed);

            for level in 1..height {
                loop {
                    (*node).tower[level].store(succs[level], Relaxed);
                    if self
                        .slot(preds[level], level)
                        .compare_exchange(succs[level], node, Release, Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                    self.find(&key, seq, &mut preds, &mut succs);
                }
            }
        }
        true
    }

    // For each level from [n..0]
    // If pred is not empty, get as current; otherwise, get head
    //
    // Iterate through Node pointers of current level
    //
    // If value less, use current as pred, get new as current node
    // If >= stop loop
    // Store found values in arrays
    fn find(
        &self,
        key: &str,
        seq: u64,
        preds: &mut [*mut Node; MAX_HEIGHT],
        succs: &mut [*mut Node; MAX_HEIGHT],
    ) -> bool {
        let max_h = self.max_height.load(Relaxed);
        let mut pred: *mut Node = ptr::null_mut();

        for level in (0..max_h).rev() {
            let mut curr = self.slot(pred, level).load(Acquire);
            while let Some(node) = unsafe { curr.as_ref() } {
                match cmp_key(&node.key, key, seq) {
                    Cmp::Less => {
                        pred = curr;
                        curr = node.tower[level].load(Acquire);
                    }
                    _ => break,
                }
            }
            preds[level] = pred;
            succs[level] = curr;
        }
        match unsafe { succs[0].as_ref() } {
            Some(n) => cmp_key(&n.key, key, seq) == Cmp::Equal,
            None => false,
        }
    }

    pub(crate) fn get_at(&self, key: &str, snapshot: u64) -> Option<&Value> {
        self.get_entry_at(key, snapshot).map(|(_, v)| v)
    }

    #[cfg(test)]
    fn get(&self, key: &str) -> Option<&Value> {
        self.get_at(key, u64::MAX)
    }

    pub(crate) fn get_entry_at(&self, key: &str, snapshot: u64) -> Option<(&Key, &Value)> {
        let node = unsafe { self.seek(key, snapshot).as_ref()? };
        if node.key.0 == key {
            Some((&node.key, &node.value))
        } else {
            None
        }
    }

    // Get current list's max height
    // Iterator other heights 0..max
    // In each height:
    //  get pred || head's node by level
    //  find node with nearest or equal key
    fn seek(&self, key: &str, seq: u64) -> *mut Node {
        let max_h = self.max_height.load(Relaxed);
        let mut pred: *mut Node = ptr::null_mut();
        let mut curr = ptr::null_mut();
        for level in (0..max_h).rev() {
            curr = self.slot(pred, level).load(Acquire);
            while let Some(node) = unsafe { curr.as_ref() } {
                if cmp_key(&node.key, key, seq) == Cmp::Less {
                    pred = curr;
                    curr = node.tower[level].load(Acquire);
                } else {
                    break;
                }
            }
        }
        curr
    }

    pub(crate) fn iter(&self) -> Iter<'_> {
        Iter {
            _list: self,
            curr: self.head[0].load(Acquire),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn iter_from(&self, key: &str, seq: u64) -> Iter<'_> {
        Iter {
            _list: self,
            curr: self.seek(key, seq),
        }
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        let mut curr = *self.head[0].get_mut();
        while !curr.is_null() {
            let owned = unsafe { Box::from_raw(curr) };
            curr = owned.tower[0].load(Relaxed);
        }
    }
}

/// Key-value iterator
pub struct Iter<'a> {
    _list: &'a SkipList,
    curr: *mut Node,
}

unsafe impl Send for Iter<'_> {}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Key, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        let node = unsafe { self.curr.as_ref()? };
        self.curr = node.tower[0].load(Acquire);
        Some((&node.key, &node.value))
    }
}

fn random_height() -> usize {
    // Thread local RNG state:
    //
    // - each thread maintains its own 64-bit random state
    // - `Cell` allowes mutable access without synchronization, initialized with 0
    thread_local! {
        static RNG: Cell<u64> = const { Cell::new(0) };
    }
    static SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

    RNG.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            x = SEED.fetch_add(0x9E37_79B9_7F4A_7C15, Relaxed) | 1;
        }
        let mut height = 1;
        loop {
            // xorshift64
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Height = 1: 75% probability (3/4)
            // Height = 2: 18.75% (1/4 × 3/4)
            // Height = 3: 4.6875% (1/4² × 3/4)
            // Height ≥ n: (1/4)^(n-1)
            if height < MAX_HEIGHT && x % BRANCHING == 0 {
                height += 1;
            } else {
                break;
            }
        }
        cell.set(x);
        height
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn insert_and_get() {
        let list = SkipList::new();
        assert!(list.insert(Key::new("apple", 1), Value::set("a1")));
        assert!(list.insert(Key::new("banana", 2), Value::set("b2")));
        assert!(list.insert(Key::new("apple", 3), Value::set("a3")));

        assert_eq!(list.get("apple"), Some(&Value::set("a3")));
        assert_eq!(list.get("banana"), Some(&Value::set("b2")));
        assert_eq!(list.get("cherry"), None);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn insert_greater_to_lower() {
        let list = SkipList::new();
        assert!(list.insert(Key::new("b", 1), Value::Delete));
        assert!(list.insert(Key::new("a", 2), Value::Delete));
    }

    #[test]
    fn tombstone_is_a_hit() {
        let list = SkipList::new();
        list.insert(Key::new("k", 1), Value::set("v"));
        list.insert(Key::new("k", 5), Value::Delete);

        assert_eq!(list.get("k"), Some(&Value::Delete));
        assert!(list.get("k").unwrap().is_delete());
        // Reading below the tombstone still sees the old value.
        assert_eq!(list.get_at("k", 4), Some(&Value::set("v")));
    }

    #[test]
    fn snapshot_reads() {
        let list = SkipList::new();
        for seq in [10u64, 20, 30] {
            list.insert(Key::new("k", seq), Value::set(&format!("v{seq}")));
        }
        assert_eq!(list.get_at("k", 5), None);
        assert_eq!(list.get_at("k", 10), Some(&Value::set("v10")));
        assert_eq!(list.get_at("k", 25), Some(&Value::set("v20")));
        assert_eq!(list.get_at("k", u64::MAX), Some(&Value::set("v30")));

        let (key, _) = list.get_entry_at("k", 25).unwrap();
        assert_eq!(key.1, 20);
    }

    #[test]
    fn duplicate_is_rejected() {
        let list = SkipList::new();
        assert!(list.insert(Key::new("k", 7), Value::set("first")));
        assert!(!list.insert(Key::new("k", 7), Value::set("second")));
        assert_eq!(list.get("k"), Some(&Value::set("first")));
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn iteration_is_sorted_newest_first() {
        let list = SkipList::new();
        list.insert(Key::new("b", 1), Value::set("b1"));
        list.insert(Key::new("a", 2), Value::set("a2"));
        list.insert(Key::new("b", 9), Value::Delete);
        list.insert(Key::new("a", 1), Value::set("a1"));

        let got: Vec<_> = list.iter().map(|(k, _)| (k.0.clone(), k.1)).collect();
        assert_eq!(
            got,
            vec![
                ("a".into(), 2u64),
                ("a".into(), 1),
                ("b".into(), 9),
                ("b".into(), 1)
            ]
        );

        // What a flush would emit: first entry of each user-key run.
        let mut flushed: Vec<(&str, &Value)> = Vec::new();
        for (k, v) in list.iter() {
            if flushed.last().map(|(u, _)| *u) != Some(k.0.as_str()) {
                flushed.push((&k.0, v));
            }
        }
        assert_eq!(
            flushed,
            vec![("a", &Value::set("a2")), ("b", &Value::Delete)]
        );
    }

    #[test]
    fn iter_from_seeks() {
        let list = SkipList::new();
        for k in ["a", "c", "e", "g"] {
            list.insert(Key::new(k, 1), Value::set(k));
        }
        let got: Vec<_> = list
            .iter_from("c", u64::MAX)
            .map(|(k, _)| k.0.clone())
            .collect();
        assert_eq!(got, vec!["c", "e", "g"]);
    }

    #[test]
    fn concurrent_inserts() {
        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 2_000;

        let list = Arc::new(SkipList::new());
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let list = Arc::clone(&list);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        let seq = t * PER_THREAD + i;
                        // Interleave key spaces so threads collide in the middle.
                        let key = format!("key{:06}", i * THREADS + t);
                        assert!(list.insert(Key(key, seq), Value::set(&format!("v{seq}"))));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let total = (THREADS * PER_THREAD) as usize;
        assert_eq!(list.len(), total);

        // Every key is readable.
        for i in 0..total as u64 {
            let key = format!("key{i:06}");
            assert!(list.get(&key).is_some(), "missing {key}");
        }

        // Level 0 is fully linked and sorted.
        let mut count = 0;
        let mut prev: Option<Key> = None;
        for (k, _) in list.iter() {
            if let Some(p) = &prev {
                assert!(p < k, "out of order: {p:?} then {k:?}");
            }
            prev = Some(k.clone());
            count += 1;
        }
        assert_eq!(count, total);
        assert!(list.mem_usage() > 0);
    }

    #[test]
    fn concurrent_reads_during_writes() {
        let list = Arc::new(SkipList::new());
        let writer = {
            let list = Arc::clone(&list);
            std::thread::spawn(move || {
                for i in 0..5_000u64 {
                    list.insert(Key(format!("key{i:06}"), i), Value::set("v"));
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let list = Arc::clone(&list);
                std::thread::spawn(move || {
                    let mut seen = 0;
                    for _ in 0..200 {
                        // Must never observe a torn node or a broken chain.
                        let mut prev: Option<Key> = None;
                        for (k, v) in list.iter() {
                            if let Some(p) = &prev {
                                assert!(p < k);
                            }
                            assert!(matches!(v, Value::Set(_)));
                            prev = Some(k.clone());
                            seen += 1;
                        }
                    }
                    seen
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(list.len(), 5_000);
    }
}
