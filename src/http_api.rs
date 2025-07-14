use std::{collections::HashMap, sync::Arc};

use axum::{extract::{Query, State}, response::IntoResponse, Json};
use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

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

#[derive(FromRow, Default, Clone, Debug)]
struct PaymentsSummaryDetailsRow {
    pub total_requests: i64,
    pub total_amount: Decimal,
    pub payment_processor: String
}

impl Into<PaymentsSummaryDetails> for PaymentsSummaryDetailsRow {
    fn into(self) -> PaymentsSummaryDetails {
        PaymentsSummaryDetails {
            total_requests: self.total_requests,
            total_amount: self.total_amount
        }
    }
}

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentsSummaryDetails {
    pub total_requests: i64,
    pub total_amount: Decimal
}

#[derive(Serialize, Default, Debug)]
struct PaymentsSummary {
    pub default: PaymentsSummaryDetails,
    pub fallback: PaymentsSummaryDetails
}

impl PaymentsSummary {
    fn get(query_result: Vec<PaymentsSummaryDetailsRow>, payment_processor: String) -> PaymentsSummaryDetails {
        match query_result.iter().find(|row| row.payment_processor == payment_processor) {
            Some(element) => element.clone().into(),
            None => PaymentsSummaryDetails::default()
        }
    }
}

const PAYMENTS_SUMMARY_QUERY: &'static str = "
SELECT COUNT(correlation_id) as total_requests, COALESCE(SUM(amount), 0) as total_amount, payment_processor
FROM PAYMENTS
WHERE requested_at BETWEEN $1 AND $2
GROUP BY payment_processor";

fn date_str_to_datetime(value: &str) -> Option<DateTime<Utc>> {
    value.parse().ok()
        .or_else(|| value.parse().ok().map(|value| Utc.from_local_datetime(&value).unwrap()))
}

pub async fn payments_summary(State(state): State<Arc<AppState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {

    let from = date_str_to_datetime(params.get("from").unwrap());
    let to = date_str_to_datetime(params.get("to").unwrap());

    match sqlx::query_as::<_, PaymentsSummaryDetailsRow>(PAYMENTS_SUMMARY_QUERY)
        .bind(from)
        .bind(to)
        .fetch_all(&state.pg_pool)
        .await {
            Ok(summary) => {
                let default = PaymentsSummary::get(summary.clone(), "D".to_string());
                let fallback = PaymentsSummary::get(summary.clone(), "F".to_string());
                let summary = PaymentsSummary { default, fallback };
                println!("Summary: {summary:?}");
                (axum::http::StatusCode::OK, serde_json::to_vec(&summary).unwrap())
            },
            Err(err) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {err}").as_bytes().to_vec())
        }
}