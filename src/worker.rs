use std::{sync::Arc, time::Instant};

use axum::{extract::State, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{app_state::AppState, payment_processor::PaymentProcessorIdentifier};

pub type QueueEvent = (String, Decimal);

pub async fn events(State(state): State<Arc<AppState>>, Json(payload): Json<QueueEvent>) -> axum::http::StatusCode {
    state.send_event(&payload.into()).await;
    axum::http::StatusCode::ACCEPTED
}

pub async fn process_queue_event(state: &Arc<AppState>, event: &QueueEvent) -> anyhow::Result<()> {
    while state.consuming_payments() {
        if let Ok((payment_processor_id, requested_at)) = call_payment_processor(&state, &event).await {
            state.storage.insert_payment(&state, &event, payment_processor_id, requested_at).await?;
            return Ok(())
        }
    }
    Err(anyhow::Error::msg("Payments are not being consumed"))
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorHealthResponse {
    pub failing: bool,
    pub min_response_time: f64
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