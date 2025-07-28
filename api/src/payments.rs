use axum::{extract::State, http::StatusCode, Json};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::ApiState;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

pub async fn enqueue_payment(State(state): State<ApiState>, Json(payload): Json<PaymentPayload>) -> StatusCode {
    _ = state.tx.send_blocking((payload.correlation_id, payload.amount));
    StatusCode::ACCEPTED
}

pub async fn purge_payments(State(state): State<ApiState>) -> StatusCode {
    if let Err(e) = state.psql_client.execute(&state.purge_statement, &[]).await {
        eprintln!("Error purging payments: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}