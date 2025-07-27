use serde::Deserialize;

use crate::{payment_processor::PaymentProcessorIdentifier, WorkerState};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorHealthResponse {
    pub failing: bool,
    pub min_response_time: f64
}

impl WorkerState {

    pub fn consuming_payments(&self) -> bool {
        self.consuming_payments.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn update_consuming_payments(&self, consuming: bool) {
        self.consuming_payments.store(consuming, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn health_check(&self, payment_processor_id: PaymentProcessorIdentifier) -> anyhow::Result<()> {
        let payment_processor = match payment_processor_id {
            PaymentProcessorIdentifier::Default => &self.default_payment_processor,
            PaymentProcessorIdentifier::Fallback => &self.fallback_payment_processor
        };

        let url = &payment_processor.url;

        let body = self.reqwest_client.get(format!("{url}/payments/service-health"))
            .send()
            .await?
            .bytes()
            .await?;

        let response = serde_json::from_slice::<PaymentProcessorHealthResponse>(&body)?;

        payment_processor.update_failing(response.failing);
        payment_processor.update_min_response_time(response.min_response_time);
        payment_processor.update_efficiency((1000.0 / f64::max(response.min_response_time, 1.0)) / payment_processor.tax);

        Ok(())
    }
}