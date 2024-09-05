use bytes::Bytes;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SharedMap {
    inner: Arc<Vec<Mutex<SharedMapInner>>>,
}
struct SharedMapInner {
    data: HashMap<String, Bytes>,
}

impl SharedMap {
    pub fn new(num_shards: usize) -> Self {
        let mut db = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            db.push(Mutex::new(SharedMapInner {
                data: HashMap::new(),
            }));
        }

        Self {
            inner: Arc::new(db),
        }
    }

    pub fn insert(&self, key: String, value: Bytes) {
        // let mut lock = self.inner.lock().unwrap();
        let mut lock = self.inner[self.hash(&key) % self.inner.len()]
            .lock()
            .unwrap();
        lock.data.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<Bytes> {
        let lock = self.inner[self.hash(&key) % self.inner.len()]
            .lock()
            .unwrap();
        lock.data.get(&key).cloned()
    }

    fn hash<H: Hash>(&self, h: &H) -> usize {
        let mut hasher = DefaultHasher::new();
        h.hash(&mut hasher);
        hasher.finish() as usize
    }
}
