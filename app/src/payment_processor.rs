use std::sync::{atomic::{AtomicBool, Ordering}, Arc};

use crate::atomicf64::AtomicF64;

#[derive(PartialEq, Eq, Debug)]
pub enum PaymentProcessorIdentifier {
    Default,
    Fallback,
}
impl ToString for PaymentProcessorIdentifier {
    fn to_string(&self) -> String {
        match self {
            PaymentProcessorIdentifier::Default => "D".to_string(),
            PaymentProcessorIdentifier::Fallback => "F".to_string(),
        }
    }
}

pub struct PaymentProcessor {
    pub id: PaymentProcessorIdentifier,
    pub url: String,
    pub failing: Arc<AtomicBool>,
    pub min_response_time: Arc<AtomicF64>,
    pub tax: f64,
    pub efficiency: Arc<AtomicF64>,
}

impl PaymentProcessor {
    pub fn failing(&self) -> bool { self.failing.load(Ordering::Relaxed) }
    pub fn min_response_time(&self) -> f64 { self.min_response_time.load(Ordering::Relaxed) }
    pub fn efficiency(&self) -> f64 { self.efficiency.load(Ordering::Relaxed) }

    pub fn update_failing(&self, failing: bool) { self.failing.store(failing, Ordering::Relaxed); }
    pub fn update_min_response_time(&self, min_response_time: f64) { self.min_response_time.store(min_response_time, Ordering::Relaxed); }
    pub fn update_efficiency(&self, efficiency: f64) { self.efficiency.store(efficiency, Ordering::Relaxed); }
}