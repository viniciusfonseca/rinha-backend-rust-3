use std::collections::HashMap;

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

    if let (Ok((default_sum, default_count)), Ok((fallback_sum, fallback_count))) = tokio::join!(
        state.default_storage.query_diff_from_fs(params.get("from").unwrap().clone(), params.get("to").unwrap().clone()),
        state.fallback_storage.query_diff_from_fs(params.get("from").unwrap().clone(), params.get("to").unwrap().clone()),
    ) {
        Json(PaymentsSummary {
            default: PaymentsSummaryDetails {
                total_requests: default_count,
                total_amount: default_sum
            },
            fallback: PaymentsSummaryDetails {
                total_requests: fallback_count,
                total_amount: fallback_sum
            }
        })
    }
    else {
        Json(PaymentsSummary::default())
    }

}