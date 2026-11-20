// Open file handles. Every create, open and release changes the table and every read and write
// looks a handle up, from all vCPUs at once, so it is split into shards that lock on their own:
// handles are numbered in order, and concurrent streams land in different shards.

use std::array;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

const SHARDS: usize = 64;

/// Handles are sequence numbers the guest cannot choose, so one multiply spreads them well
/// enough: SipHash cost every READ and WRITE more than the rest of the lookup.
#[derive(Default)]
struct HandleHasher(u64);

impl Hasher for HandleHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 << 8) | u64::from(b);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

type Shard<T> = HashMap<u64, Arc<T>, BuildHasherDefault<HandleHasher>>;

pub(super) struct HandleTable<T> {
    shards: [RwLock<Shard<T>>; SHARDS],
    next: AtomicU64,
}

impl<T> HandleTable<T> {
    pub fn new() -> Self {
        HandleTable {
            shards: array::from_fn(|_| RwLock::new(Shard::default())),
            next: AtomicU64::new(1),
        }
    }

    fn shard(&self, handle: u64) -> &RwLock<Shard<T>> {
        &self.shards[handle as usize % SHARDS]
    }

    /// Adds `data` under a new handle and returns the handle.
    pub fn insert(&self, data: Arc<T>) -> u64 {
        let handle = self.next.fetch_add(1, Ordering::Relaxed);
        self.shard(handle).write().unwrap().insert(handle, data);
        handle
    }

    pub fn get(&self, handle: u64) -> Option<Arc<T>> {
        self.shard(handle).read().unwrap().get(&handle).cloned()
    }

    /// Removes the handle if `matches` accepts its data; returns whether it did.
    pub fn remove_if(&self, handle: u64, matches: impl FnOnce(&T) -> bool) -> bool {
        let mut shard = self.shard(handle).write().unwrap();
        if shard.get(&handle).is_some_and(|data| matches(data)) {
            shard.remove(&handle);
            return true;
        }
        false
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().unwrap().clear();
        }
    }

    /// Every handle's data, for the rare operations that look across handles.
    pub fn all(&self) -> Vec<Arc<T>> {
        self.shards
            .iter()
            .flat_map(|shard| shard.read().unwrap().values().cloned().collect::<Vec<_>>())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn handles_are_unique_and_found_until_removed() {
        let table = HandleTable::new();
        let a = table.insert(Arc::new("a"));
        let b = table.insert(Arc::new("b"));
        assert_ne!(a, b);
        assert_eq!(*table.get(a).unwrap(), "a");
        assert!(!table.remove_if(a, |d| *d == "b"));
        assert!(table.get(a).is_some());
        assert!(table.remove_if(a, |d| *d == "a"));
        assert!(table.get(a).is_none());
        assert!(!table.remove_if(a, |_| true));
        assert_eq!(table.all().len(), 1);
        table.clear();
        assert!(table.get(b).is_none());
    }

    #[test]
    fn concurrent_inserts_and_removes_keep_every_handle() {
        let table = Arc::new(HandleTable::new());
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let table = table.clone();
                thread::spawn(move || {
                    let mut kept = Vec::new();
                    for i in 0..2000u64 {
                        let h = table.insert(Arc::new((t, i)));
                        assert_eq!(*table.get(h).unwrap(), (t, i));
                        if i % 2 == 0 {
                            assert!(table.remove_if(h, |d| *d == (t, i)));
                        } else {
                            kept.push(h);
                        }
                    }
                    kept
                })
            })
            .collect();
        let kept: Vec<u64> = threads
            .into_iter()
            .flat_map(|t| t.join().unwrap())
            .collect();
        assert_eq!(kept.len(), 8 * 1000);
        assert_eq!(table.all().len(), kept.len());
        assert!(kept.iter().all(|&h| table.get(h).is_some()));
    }
}
