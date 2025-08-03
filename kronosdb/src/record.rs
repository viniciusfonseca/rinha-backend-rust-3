use std::sync::{atomic::AtomicI64, Arc};

use crate::atomicf64::AtomicF64;

#[derive(Clone)]
pub struct Record {
    pub amount: f64,
    pub sum: Arc<AtomicF64>,
    pub count: Arc<AtomicI64>
}

impl Record {
    pub fn new(amount: f64) -> Self {
        Self {
            amount,
            sum: Arc::new(AtomicF64::new(0.0)),
            count: Arc::new(AtomicI64::new(0))
        }
    }
}