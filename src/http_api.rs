use std::{collections::HashMap, sync::Arc};

use axum::{extract::{Query, State}, response::IntoResponse, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{app_state::AppState, worker::QueueEvent};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

pub async fn payments(State(state): State<Arc<AppState>>, Json(payload): Json<PaymentPayload>) -> axum::http::StatusCode {
    tokio::spawn(async move {
        let event: QueueEvent = (payload.correlation_id, payload.amount);
        if let Ok(worker_url) = &state.worker_url {
            if let Err(e) = state.reqwest_client.post(worker_url)
                .header("Content-Type", "application/json")
                .body(serde_json::to_string(&event).unwrap())
                .send()
                .await {
                    println!("Failed to send event to worker: {e}")
                }
        }
    });
    axum::http::StatusCode::ACCEPTED
}

pub async fn purge_payments(State(state): State<Arc<AppState>>) -> axum::http::StatusCode {
    _ = sqlx::query("DELETE FROM payments").execute(&state.pg_pool).await;
    axum::http::StatusCode::ACCEPTED
}

pub async fn payments_summary(State(state): State<Arc<AppState>>, Query(params): Query<HashMap<String, DateTime<Utc>>>) -> impl IntoResponse {

    match state.storage.get_summary(&state, &params.get("from"), &params.get("to")).await {
        Ok(summary) => (
            axum::http::StatusCode::OK,
            serde_json::to_vec(&summary).unwrap()
        ),
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, serde_json::to_vec(&e.to_string()).unwrap())
    }
}