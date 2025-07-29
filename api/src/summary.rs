use std::{collections::HashMap, time::Instant};

use axum::{extract::{Query, State}, Json};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;

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

pub async fn summary(State(state): State<ApiState>, Query(params): Query<HashMap<String, DateTime<Utc>>>) -> Json<PaymentsSummary> {

    let start = Instant::now();

    let from = params.get("from").unwrap().to_rfc3339();
    let to = params.get("to").unwrap().to_rfc3339();

    let query = format!(r#"
        from(bucket: "db")
        |> range(start: {from}, stop: {to})
        |> filter(fn: (r) => r._measurement == "payments")
        |> group(columns: ["payment_processor_id"])
        |> aggregateWindow(
            every: v.windowPeriod,
            fn: (tables) => tables
                |> map(fn: (r) => ({{
                    r with
                    total_requests: 1.0,
                    total_amount: r._value
                }}))
                |> sum(columns: ["total_requests", "total_amount"])
            )
        |> yield(name: "result")
    "#);

    let url = format!("{db_url}/api/v2/query?org=myorg", db_url = state.db_url);

    let res = state.reqwest_client.post(url)
        .header("Authorization", "Token sec-token")
        .header("Accept", "application/json")
        .header("Content-Type", "application/vnd.flux")
        .body(query)
        .send()
        .await
        .expect("error querying summary");

    let res_status = res.status().as_u16();
    let res_text = res.text().await.unwrap();

    println!("Got influxql response: {res_status} - {res_text}");

    let json = serde_json::from_str::<serde_json::Value>(&res_text).expect("error deserializing summary");
    let empty_array = &json!([]);
    let empty_vec = &Vec::new();
    let empty_json = &json!({});
    let empty_map = &serde_json::Map::new();

    let values = json.get("results").unwrap_or_else(|| empty_array)
        .as_array().unwrap_or_else(|| empty_vec)
        .get(0).unwrap_or_else(|| empty_json)
        .as_object().unwrap_or_else(|| empty_map)
        .get("values").unwrap_or_else(|| empty_array)
        .as_array().unwrap_or_else(|| empty_vec);

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