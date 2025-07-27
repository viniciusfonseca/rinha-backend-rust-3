use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Instant};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{atomicf64::AtomicF64, WorkerState};

#[derive(Clone, PartialEq, Eq, Debug)]
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

#[derive(Clone)]
pub struct PaymentProcessor {
    pub id: PaymentProcessorIdentifier,
    pub url: String,
    pub failing: Arc<AtomicBool>,
    pub min_response_time: Arc<AtomicF64>,
    pub tax: f64,
    pub efficiency: Arc<AtomicF64>,
}

impl PaymentProcessor {

    pub fn new(id: PaymentProcessorIdentifier, url: String, tax: f64) -> Self {
        Self {
            id,
            url,
            failing: Arc::new(AtomicBool::new(false)),
            min_response_time: Arc::new(AtomicF64::new(0.0)),
            tax,
            efficiency: Arc::new(AtomicF64::new(0.0)),
        }
    }

    pub fn failing(&self) -> bool { self.failing.load(std::sync::atomic::Ordering::Relaxed) }
    pub fn min_response_time(&self) -> f64 { self.min_response_time.load(std::sync::atomic::Ordering::Relaxed) }
    pub fn efficiency(&self) -> f64 { self.efficiency.load(std::sync::atomic::Ordering::Relaxed) }

    pub fn update_failing(&self, failing: bool) { self.failing.store(failing, std::sync::atomic::Ordering::Relaxed); }
    pub fn update_min_response_time(&self, min_response_time: f64) { self.min_response_time.store(min_response_time, std::sync::atomic::Ordering::Relaxed); }
    pub fn update_efficiency(&self, efficiency: f64) { self.efficiency.store(efficiency, std::sync::atomic::Ordering::Relaxed); }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorRequest {
    pub correlation_id: String,
    pub amount: f64,
    pub requested_at: chrono::DateTime<Utc>
}

impl WorkerState {

    pub fn set_preferred_payment_processor(&self, preferred_payment_processor: PaymentProcessorIdentifier) {
        match preferred_payment_processor {
            PaymentProcessorIdentifier::Default => self.preferred_payment_processor.store(0, Ordering::Relaxed),
            PaymentProcessorIdentifier::Fallback => self.preferred_payment_processor.store(1, Ordering::Relaxed),
        }
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

    pub fn preferred_payment_processor(&self) -> &PaymentProcessor {
        if self.default_payment_processor.efficiency() > self.fallback_payment_processor.efficiency() {
            &self.default_payment_processor
        } else {
            &self.fallback_payment_processor
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

    pub async fn process_payment<'a>(&'a self, (correlation_id, amount): &(String, f64)) -> anyhow::Result<(&'a PaymentProcessorIdentifier, DateTime<Utc>)> {

        let payment_processor = self.preferred_payment_processor();

        if payment_processor.failing() {
            _ = &self.update_preferred_payment_processor();
            return Err(anyhow::Error::msg("Payment processor is failing"));
        }

        let id = &self.preferred_payment_processor().id;
        let url = &payment_processor.url;
        let requested_at = Utc::now();

        let body = PaymentProcessorRequest {
            correlation_id: correlation_id.to_string(),
            amount: *amount,
            requested_at,
        };

        let start = Instant::now();
        let response = self.reqwest_client
                .post(format!("{url}/payments"))
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&body)?)
                .send()
                .await?;
        let elapsed = start.elapsed();

        let response_status = response.status().as_u16();

        if response_status >= 400 && response_status < 500 {
            eprintln!("Got 4xx error: {response_status} - {}", response.text().await?);
            return Err(anyhow::Error::msg("Payment processor returned error"));
        }

        if response.status().as_u16() >= 500 {
            self.update_payment_processor_state(id, Some(true), Some(elapsed.as_secs_f64()));
            _ = &self.update_preferred_payment_processor();
            return Err(anyhow::Error::msg("Payment processor returned error"));
        }

        self.update_payment_processor_state(id, Some(false), Some(elapsed.as_secs_f64()));
        _ = &self.update_preferred_payment_processor();

        Ok((id, requested_at))
    }
}