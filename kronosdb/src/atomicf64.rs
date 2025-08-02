use std::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicF64 {
    storage: AtomicU64
}

impl AtomicF64 {
    pub fn new(value: f64) -> Self {
        Self {
            storage: AtomicU64::new(value.to_bits())
        }
    }

    pub fn store(&self, value: f64, ordering: Ordering) {
        self.storage.store(value.to_bits(), ordering)
    }

    pub fn load(&self, ordering: Ordering) -> f64 {
        f64::from_bits(self.storage.load(ordering))
    }

    pub fn fetch_add(&self, value: f64, ordering: Ordering) -> f64 {
        f64::from_bits(self.storage.fetch_add(value.to_bits(), ordering))
    }
}