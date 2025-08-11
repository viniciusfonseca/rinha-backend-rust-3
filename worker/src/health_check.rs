use chrono::Utc;
use serde::Deserialize;

use crate::{payment_processor::PaymentProcessorIdentifier, WorkerState};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorHealthResponse {
    pub failing: bool,
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

        let res = self.reqwest_client.get(format!("{url}/payments/service-health"))
            .send()
            .await?;

        if res.status().as_u16() != 200 {
            return Ok(());
        }

        let body = res.bytes().await?;

        let response = match serde_json::from_slice::<PaymentProcessorHealthResponse>(&body) {
            Ok(response) => response,
            Err(_) => { return Ok(()); }
        };

        match payment_processor_id {
            PaymentProcessorIdentifier::Fallback => {
                let failing = response.failing || self.fallback_payment_processor.failing_period.load(std::sync::atomic::Ordering::Relaxed) <= 22;
                payment_processor.update_failing(failing);
            },
            PaymentProcessorIdentifier::Default => {
                let now = Utc::now().timestamp();
                let last_health_check = payment_processor.last_health_check.load(std::sync::atomic::Ordering::Relaxed);
                if response.failing {
                    payment_processor.failing_period.fetch_add(now - last_health_check, std::sync::atomic::Ordering::Relaxed);
                    let failing_period = payment_processor.failing_period.load(std::sync::atomic::Ordering::Relaxed);
                    println!("Default payment processor is failing for {failing_period} seconds");
                }
                payment_processor.last_health_check.store(now, std::sync::atomic::Ordering::Relaxed);
                payment_processor.update_failing(response.failing);
            },
        }

        Ok(())
    }
}