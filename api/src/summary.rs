use std::{collections::HashMap, time::Instant};

use axum::{extract::{Query, State}, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::ApiState;

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PaymentsSummaryDetails {
    pub total_requests: i64,
    pub total_amount: f64
}

#[derive(Serialize, Default, Debug)]
pub struct PaymentsSummary {
    pub default: PaymentsSummaryDetails,
    pub fallback: PaymentsSummaryDetails
}

#[axum::debug_handler]
pub async fn summary(State(state): State<ApiState>, Query(params): Query<HashMap<String, DateTime<Utc>>>) -> Json<PaymentsSummary> {

    let from = params.get("from").unwrap();
    let to = params.get("to").unwrap();

    let start = Instant::now();
    let (default_query_result, fallback_query_result) = tokio::join!(
        state.default_storage.query_diff_from_fs(&from, &to),
        state.fallback_storage.query_diff_from_fs(&from, &to),
    );
    println!("query diff in {}ms", start.elapsed().as_millis());
    
    let default = match default_query_result {
        Ok((sum, count)) => PaymentsSummaryDetails {
            total_requests: count,
            total_amount: sum / 100.0 * 100.0
        },
        Err(e) => {
            eprintln!("Error querying default storage: {}", e);
            PaymentsSummaryDetails::default()
        }
    };

    let fallback = match fallback_query_result {
        Ok((sum, count)) => PaymentsSummaryDetails {
            total_requests: count,
            total_amount: sum / 100.0 * 100.0
        },
        Err(e) => {
            eprintln!("Error querying fallback storage: {}", e);
            PaymentsSummaryDetails::default()
        }
    };

    Json(PaymentsSummary {
        default,
        fallback,
    })
}