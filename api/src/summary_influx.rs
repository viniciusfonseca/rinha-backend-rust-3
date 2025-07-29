use std::{collections::HashMap, time::Instant};

use axum::{extract::{Query, State}, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::ApiState;

pub const PAYMENTS_SUMMARY_QUERY: &'static str = "
SELECT COUNT(*) as total_requests, COALESCE(SUM(amount), 0) as total_amount, payment_processor_id
FROM PAYMENTS
WHERE requested_at BETWEEN $1 AND $2
GROUP BY payment_processor_id";

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

pub async fn summary(State(state): State<ApiState>, Query(params): Query<HashMap<String, DateTime<Utc>>>) -> Json<PaymentsSummary> {

    let start = Instant::now();

    let url = format!("{db_url}/query", db_url = state.db_url);
    let mut form_data = HashMap::new();
    form_data.insert("db", "db");
    form_data.insert("q", PAYMENTS_SUMMARY_QUERY);

    let bytes = state.reqwest_client.get(url)
        .form(&form_data)
        .send()
        .await
        .expect("error querying summary")
        .bytes()
        .await
        .expect("error getting summary bytes");

    let json = serde_json::from_slice::<serde_json::Value>(&bytes).expect("error deserializing summary");

    let values = json.get("results").unwrap()
        .as_array().unwrap()
        .get(0).unwrap()
        .as_object().unwrap()
        .get("values").unwrap()
        .as_array().unwrap();

    let mut default = PaymentsSummaryDetails::default();
    let mut fallback = PaymentsSummaryDetails::default();
    
    for value in values {
        let row = value.as_array().unwrap();
        let total_requests = row.get(0).unwrap().as_i64().unwrap();
        let total_amount = row.get(1).unwrap().as_str().unwrap();
        let payment_processor_id = row.get(2).unwrap().as_str().unwrap();

        match payment_processor_id {
            "D" => {
                default.total_requests = total_requests;
                default.total_amount = total_amount.parse().unwrap();
            }
            "F" => {
                fallback.total_requests = total_requests;
                fallback.total_amount = total_amount.parse().unwrap();
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