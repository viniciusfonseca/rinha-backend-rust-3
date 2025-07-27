use axum::{extract::State, http::StatusCode, Json};
use serde::Deserialize;

use crate::AppState;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: f64,
}

pub async fn enqueue_payment(State(state): State<AppState>, Json(payload): Json<PaymentPayload>) -> StatusCode {
    _ = state.tx.send((payload.correlation_id, payload.amount));
    StatusCode::ACCEPTED
}

pub async fn purge_payments() -> StatusCode {
    StatusCode::OK
}