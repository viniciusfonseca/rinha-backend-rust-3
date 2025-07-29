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

pub async fn purge_payments(State(state): State<ApiState>) -> StatusCode {
    // if let Err(e) = state.psql_client.execute(&state.purge_statement, &[]).await {
    //     eprintln!("Error purging payments: {}", e);
    //     return StatusCode::INTERNAL_SERVER_ERROR;
    // }
    StatusCode::OK
}