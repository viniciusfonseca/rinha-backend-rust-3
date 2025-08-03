use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;

use crate::ApiState;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: f64,
}

pub async fn enqueue_payment(State(state): State<ApiState>, Json(payload): Json<PaymentPayload>) -> StatusCode {
    _ = state.tx.send((payload.correlation_id, payload.amount)).await;
    StatusCode::ACCEPTED
}

pub async fn purge_payments() -> StatusCode {
    _ = tokio::join!(
        tokio::fs::remove_file("/tmp/storage/default/partitions"),
        tokio::fs::remove_file("/tmp/storage/fallback/partitions"),
    );
    StatusCode::OK
}