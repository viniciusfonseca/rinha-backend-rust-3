use std::collections::HashMap;

use axum::{extract::Query, Json};
use chrono::{DateTime, Utc};
use kronosdb::ReadOnlyStorage;
use serde::Serialize;

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
pub async fn summary(Query(params): Query<HashMap<String, DateTime<Utc>>>) -> Json<PaymentsSummary> {

    let default_storage = ReadOnlyStorage::connect("/tmp/storage/default".to_string());
    let fallback_storage = ReadOnlyStorage::connect("/tmp/storage/fallback".to_string());
    
    if let (Ok((default_sum, default_count)), Ok((fallback_sum, fallback_count))) = tokio::join!(
        default_storage.query_diff_from_fs(params.get("from").unwrap().clone(), params.get("to").unwrap().clone()),
        fallback_storage.query_diff_from_fs(params.get("from").unwrap().clone(), params.get("to").unwrap().clone()),
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