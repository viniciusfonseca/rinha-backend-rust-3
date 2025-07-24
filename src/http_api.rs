use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{extract::{Query, State}, response::IntoResponse, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{app_state::AppState, storage::{self, PaymentsSummary}};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

pub async fn payments(State(state): State<Arc<AppState>>, Json(payload): Json<PaymentPayload>) -> axum::http::StatusCode {
    _ = state.tx.send((payload.correlation_id, payload.amount)).await;
    axum::http::StatusCode::ACCEPTED
}

pub async fn purge_payments(State(_): State<Arc<AppState>>) -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

pub async fn payments_summary(Query(params): Query<HashMap<String, DateTime<Utc>>>) -> impl IntoResponse {

    let start = Instant::now();
    match storage::get_summary(
        &params.get("from").unwrap_or(&Utc::now()),
        &params.get("to").unwrap_or(&Utc::now())
    ).await {
        Ok(summary) => {
            println!("Got summary in {}ms", start.elapsed().as_millis());
            Json(summary)
        },
        Err(e) => {
            println!("Error at payments_summary: {e}");
            Json(PaymentsSummary::default())
        }
    }
}