use std::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicF64 {
    storage: AtomicU64
}

impl AtomicF64 {
    pub fn new(amount: f64) -> Self {
        Self {
            storage: AtomicU64::new(amount.to_bits())
        }
    }

    pub fn load(&self, ordering: Ordering) -> f64 {
        let as_u64 = self.storage.load(ordering);
        f64::from_bits(as_u64)
    }

    pub fn store(&self, value: f64, ordering: Ordering) {
        let as_u64 = value.to_bits();
        self.storage.store(as_u64, ordering);
    }
}