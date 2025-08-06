use chrono::{DateTime, Utc};

use crate::{payment_processor::PaymentProcessorIdentifier, WorkerState};

impl WorkerState {
    pub async fn save_payment(&self, amount: f64, payment_processor_id: &PaymentProcessorIdentifier, requested_at: DateTime<Utc>) -> anyhow::Result<()> {
        match payment_processor_id {
            PaymentProcessorIdentifier::Default => self.default_storage.insert_data(requested_at, amount).await,
            PaymentProcessorIdentifier::Fallback => self.fallback_storage.insert_data(requested_at, amount).await
        }
    }
}