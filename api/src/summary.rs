use std::{collections::HashMap, time::Instant};

use axum::{extract::{Query, State}, Json};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::ApiState;

pub const PAYMENTS_SUMMARY_QUERY: &'static str = "
SELECT COUNT(requested_at) as total_requests, COALESCE(SUM(amount), 0) as total_amount, payment_processor_id
FROM PAYMENTS
WHERE requested_at BETWEEN $1 AND $2
GROUP BY payment_processor_id";

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PaymentsSummaryDetails {
    pub total_requests: i64,
    pub total_amount: Decimal
}

#[derive(Serialize, Default, Debug)]
pub struct PaymentsSummary {
    pub default: PaymentsSummaryDetails,
    pub fallback: PaymentsSummaryDetails
}

pub async fn summary(State(state): State<ApiState>, Query(params): Query<HashMap<String, DateTime<Utc>>>) -> Json<PaymentsSummary> {

    let start = Instant::now();
    let rows = state.psql_client.query(&state.summary_statement, &[
        &params.get("from").unwrap_or(&Utc::now()),
        &params.get("to").unwrap_or(&Utc::now()),
    ]).await.expect("Failed to execute summary query");

    let mut default = PaymentsSummaryDetails::default();
    let mut fallback = PaymentsSummaryDetails::default();

    for row in rows.iter() {
        let payment_processor: &str = row.get("payment_processor_id");
        let total_requests: i64 = row.get("total_requests");
        let total_amount: Decimal = row.get("total_amount");

        match payment_processor {
            "D" => {
                default.total_requests = total_requests;
                default.total_amount = total_amount;
            }
            "F" => {
                fallback.total_requests = total_requests;
                fallback.total_amount = total_amount;
            }
            _ => continue
        }
    }

    println!("Got summary in {}ms", start.elapsed().as_millis());

    Json(PaymentsSummary {
        default,
        fallback,
    })
}