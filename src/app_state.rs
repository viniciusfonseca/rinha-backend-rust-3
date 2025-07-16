use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, Ordering};

use crate::{payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}, storage::PaymentsStorage, worker::QueueEvent};

pub struct AppState {
    pub tx: tokio::sync::mpsc::Sender<QueueEvent>,
    pub reqwest_client: reqwest::Client,
    pub default_payment_processor: PaymentProcessor,
    pub fallback_payment_processor: PaymentProcessor,
    pub preferred_payment_processor: AtomicU16,
    pub signal_tx: tokio::sync::mpsc::Sender<()>,
    pub queue_len: AtomicI32,
    pub consuming_payments: AtomicBool,
    pub storage: PaymentsStorage
}

impl AppState {
    pub fn consuming_payments(&self) -> bool { self.consuming_payments.load(Ordering::Relaxed) }
    pub fn update_consuming_payments(&self, consuming_payments: bool) { self.consuming_payments.store(consuming_payments, Ordering::Relaxed); }

    pub fn preferred_payment_processor(&self) -> &PaymentProcessor {
        match self.preferred_payment_processor.load(Ordering::Relaxed) {
            0 => &self.default_payment_processor,
            1 => &self.fallback_payment_processor,
            _ => panic!("Invalid preferred payment processor"),
        }
    }
    pub fn set_preferred_payment_processor(&self, preferred_payment_processor: PaymentProcessorIdentifier) {
        match preferred_payment_processor {
            PaymentProcessorIdentifier::Default => self.preferred_payment_processor.store(0, Ordering::Relaxed),
            PaymentProcessorIdentifier::Fallback => self.preferred_payment_processor.store(1, Ordering::Relaxed),
        }
    }
    pub async fn send_event(&self, event: &QueueEvent) {
        _ = self.tx.send(event.clone()).await;
        self.queue_len.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update_payment_processor_state(
        &self,
        payment_processor_id: &PaymentProcessorIdentifier,
        failing: Option<bool>,
        min_response_time: Option<f64>
    ) {
    
        let payment_processor = match payment_processor_id {
            PaymentProcessorIdentifier::Default => &self.default_payment_processor,
            PaymentProcessorIdentifier::Fallback => &self.fallback_payment_processor
        };
    
        if let Some(failing) = failing {
            payment_processor.update_failing(failing);
        }
        if let Some(min_response_time) = min_response_time {
            payment_processor.update_min_response_time(min_response_time);
        }
    
    }

    pub fn update_preferred_payment_processor(&self) -> anyhow::Result<()> {
    
        let new_preferred_payment_processor = {
            if self.default_payment_processor.failing() && self.fallback_payment_processor.failing() {
                self.update_consuming_payments(false);
                return Err(anyhow::Error::msg("Both payment processors are failing"));
            }
            else if self.fallback_payment_processor.failing() {
                PaymentProcessorIdentifier::Default
            }
            else if self.default_payment_processor.failing() {
                PaymentProcessorIdentifier::Fallback
            }
            else if self.default_payment_processor.min_response_time() < 10.0 {
                PaymentProcessorIdentifier::Default
            }
            else if self.default_payment_processor.efficiency() >= self.fallback_payment_processor.efficiency() {
                PaymentProcessorIdentifier::Default
            }
            else {
                PaymentProcessorIdentifier::Fallback
            }
        };

        let preferred_payment_processor = &self.preferred_payment_processor();
        if preferred_payment_processor.id != new_preferred_payment_processor {
            println!("Preferred payment processor changing from {:?} to {:?}", preferred_payment_processor.id, new_preferred_payment_processor);
            self.set_preferred_payment_processor(new_preferred_payment_processor);
        }

        Ok(())
    }
}
