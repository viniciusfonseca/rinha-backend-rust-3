use std::{sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{app_state::AppState, payment_processor::PaymentProcessorIdentifier, storage::PAYMENTS_STORAGE_PATH_HEALTH};

pub type QueueEvent = (String, Decimal);

pub async fn process_queue_event(state: &Arc<AppState>, event: &QueueEvent) -> anyhow::Result<()> {
    while state.consuming_payments() {
        if let Ok((payment_processor_id, requested_at)) = call_payment_processor(&state, &event).await {
            state.batch_tx.send((event.1, payment_processor_id.to_string(), requested_at)).await?;
            return Ok(());
        }
    }
    Err(anyhow::Error::msg("Payments are not being consumed"))
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorHealthResponse {
    pub failing: bool,
    pub min_response_time: f64
}

#[derive(Deserialize, Serialize)]
pub struct PaymentProcessorHealthStatuses {
    default: PaymentProcessorHealthResponse,
    fallback: PaymentProcessorHealthResponse
}

pub async fn update_payment_processor_health(state: &Arc<AppState>, payment_processor_id: PaymentProcessorIdentifier) -> anyhow::Result<()> {

    let payment_processor = match payment_processor_id {
        PaymentProcessorIdentifier::Default => &state.default_payment_processor,
        PaymentProcessorIdentifier::Fallback => &state.fallback_payment_processor
    };

    let url = &payment_processor.url;

    let body = state.reqwest_client.get(format!("{url}/payments/service-health"))
        .send()
        .await?
        .bytes()
        .await?;

    let response = serde_json::from_slice::<PaymentProcessorHealthResponse>(&body)?;

    payment_processor.update_failing(response.failing);
    payment_processor.update_min_response_time(response.min_response_time);
    payment_processor.update_efficiency((1000.0 / f64::max(response.min_response_time, 1.0)) / payment_processor.tax);

    write_payment_processor_health_statuses_to_file(&state).await?;

    Ok(())
}

pub async fn update_payment_processor_health_statuses_from_file(state: &Arc<AppState>) -> anyhow::Result<PaymentProcessorHealthStatuses> {
    let data = tokio::fs::read(PAYMENTS_STORAGE_PATH_HEALTH).await?;
    let statuses: PaymentProcessorHealthStatuses = serde_json::from_slice(&data)?;

    state.update_payment_processor_state(&PaymentProcessorIdentifier::Default, Some(statuses.default.failing), Some(statuses.default.min_response_time));
    state.update_payment_processor_state(&PaymentProcessorIdentifier::Fallback, Some(statuses.fallback.failing), Some(statuses.fallback.min_response_time));

    Ok(statuses)
}

pub async fn write_payment_processor_health_statuses_to_file(state: &Arc<AppState>) -> anyhow::Result<()> {
    let statuses = PaymentProcessorHealthStatuses {
        default: PaymentProcessorHealthResponse {
            failing: state.default_payment_processor.failing(),
            min_response_time: state.default_payment_processor.min_response_time()
        },
        fallback: PaymentProcessorHealthResponse {
            failing: state.fallback_payment_processor.failing(),
            min_response_time: state.fallback_payment_processor.min_response_time()
        }
    };

    let json = serde_json::to_string(&statuses)?;
    tokio::fs::write(PAYMENTS_STORAGE_PATH_HEALTH, json).await?;
    
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorRequest {
    pub correlation_id: String,
    pub amount: Decimal,
    pub requested_at: chrono::DateTime<Utc>
}

async fn call_payment_processor<'a>(state: &'a Arc<AppState>, (correlation_id, amount): &QueueEvent) -> anyhow::Result<(&'a PaymentProcessorIdentifier, DateTime<Utc>)> {

    let payment_processor = state.preferred_payment_processor();

    if payment_processor.failing() {
        _ = &state.update_preferred_payment_processor();
        return Err(anyhow::Error::msg("Payment processor is failing"));
    }

    let id = &state.preferred_payment_processor().id;
    let url = &payment_processor.url;
    let requested_at = Utc::now();

    let body = PaymentProcessorRequest {
        correlation_id: correlation_id.to_string(),
        amount: *amount,
        requested_at,
    };

    let start = Instant::now();
    let response = state.reqwest_client
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
        state.update_payment_processor_state(id, Some(true), Some(elapsed.as_secs_f64()));
        _ = &state.update_preferred_payment_processor();
        return Err(anyhow::Error::msg("Payment processor returned error"));
    }

    state.update_payment_processor_state(id, Some(false), Some(elapsed.as_secs_f64()));
    _ = &state.update_preferred_payment_processor();

    Ok((id, requested_at))
}