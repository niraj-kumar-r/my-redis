use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SharedMap {
    inner: Arc<Mutex<SharedMapInner>>,
}
struct SharedMapInner {
    data: HashMap<String, Bytes>,
}

impl SharedMap {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedMapInner {
                data: HashMap::new(),
            })),
        }
    }

    pub fn insert(&self, key: String, value: Bytes) {
        let mut lock = self.inner.lock().unwrap();
        lock.data.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<Bytes> {
        let lock = self.inner.lock().unwrap();
        lock.data.get(&key).cloned()
    }
}
