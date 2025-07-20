use std::{collections::HashMap, sync::Arc};

use axum::{extract::{Query, State}, response::IntoResponse, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{app_state::AppState, storage::PaymentsSummary};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

pub async fn payments(State(state): State<Arc<AppState>>, Json(payload): Json<PaymentPayload>) -> axum::http::StatusCode {
    tokio::spawn(async move {
        state.send_event(&(payload.correlation_id, payload.amount)).await;
    });
    // let fd_count = procfs::process::Process::myself()
    //     .and_then(|p| p.fd_count())
    //     .unwrap_or(0);
    // println!("open files: {fd_count}");
    axum::http::StatusCode::ACCEPTED
}

pub async fn purge_payments(State(_): State<Arc<AppState>>) -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

pub async fn payments_summary(State(state): State<Arc<AppState>>, Query(params): Query<HashMap<String, DateTime<Utc>>>) -> impl IntoResponse {

    state.storage.get_summary(&params.get("from").unwrap_or(&Utc::now()), &params.get("to").unwrap_or(&Utc::now())).await
        .map(|summary| Json(summary))
        .unwrap_or_else(|_| Json(PaymentsSummary::default()))
}