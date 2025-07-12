
use std::{collections::HashMap, sync::{atomic::{AtomicBool, AtomicI32, AtomicU16, AtomicU64, Ordering}, Arc}};
use axum::{extract::{Query, State}, http::{request, HeaderMap}, response::IntoResponse, routing, Json, Router};
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgQueryResult, prelude::FromRow, types::Decimal};
use tokio::{sync::Semaphore, time::Instant};

pub struct AtomicF64 {
    storage: AtomicU64,
}
impl AtomicF64 {
    pub fn new(value: f64) -> Self {
        let as_u64 = value.to_bits();
        Self { storage: AtomicU64::new(as_u64) }
    }
    pub fn store(&self, value: f64, ordering: Ordering) {
        let as_u64 = value.to_bits();
        self.storage.store(as_u64, ordering)
    }
    pub fn load(&self, ordering: Ordering) -> f64 {
        let as_u64 = self.storage.load(ordering);
        f64::from_bits(as_u64)
    }
}

type QueueEvent = (String, Decimal);

#[derive(Clone, PartialEq, Eq, Debug)]
enum PaymentProcessorIdentifier {
    Default,
    Fallback,
}

#[derive(Clone)]
struct PaymentProcessor {
    pub id: PaymentProcessorIdentifier,
    pub url: String,
    failing: Arc<AtomicBool>,
    min_response_time: Arc<AtomicF64>,
    pub tax: f64,
    efficiency: Arc<AtomicF64>,
}

impl PaymentProcessor {
    pub fn failing(&self) -> bool { self.failing.load(Ordering::Relaxed) }
    pub fn min_response_time(&self) -> f64 { self.min_response_time.load(Ordering::Relaxed) }
    pub fn efficiency(&self) -> f64 { self.efficiency.load(Ordering::Relaxed) }

    pub fn update_failing(&self, failing: bool) { self.failing.store(failing, Ordering::Relaxed); }
    pub fn update_min_response_time(&self, min_response_time: f64) { self.min_response_time.store(min_response_time, Ordering::Relaxed); }
    pub fn update_efficiency(&self, efficiency: f64) { self.efficiency.store(efficiency, Ordering::Relaxed); }
}

struct AppState {
    pub pg_pool: sqlx::PgPool,
    pub tx: tokio::sync::mpsc::Sender<QueueEvent>,
    pub reqwest_client: reqwest::Client,
    pub default_payment_processor: PaymentProcessor,
    pub fallback_payment_processor: PaymentProcessor,
    preferred_payment_processor: AtomicU16,
    pub worker_url: Result<String, std::env::VarError>,
    pub signal_tx: tokio::sync::mpsc::Sender<()>,
    pub queue_len: AtomicI32,
    pub consuming_payments: AtomicBool
}

impl AppState {
    pub fn consuming_payments(&self) -> bool { self.consuming_payments.load(Ordering::Relaxed) }
    pub fn update_consuming_payments(&self, consuming_payments: bool) { self.consuming_payments.store(consuming_payments, Ordering::Relaxed); }

    pub fn preferred_payment_processor(&self) -> &PaymentProcessor {
        match self.preferred_payment_processor.load(Ordering::Relaxed) {
            0 => &self.default_payment_processor,
            1 => &self.fallback_payment_processor,
            _ => panic!("Invalid preferred payment processor"),
        }
    }
    pub fn update_preferred_payment_processor(&self, preferred_payment_processor: PaymentProcessorIdentifier) {
        match preferred_payment_processor {
            PaymentProcessorIdentifier::Default => self.preferred_payment_processor.store(0, Ordering::Relaxed),
            PaymentProcessorIdentifier::Fallback => self.preferred_payment_processor.store(1, Ordering::Relaxed),
        }
    }
}

impl AppState {
    async fn send_event(&self, event: &QueueEvent) {
        _ = self.tx.send(event.clone()).await;
        self.queue_len.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let node = std::env::var("NODE")?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(50000);

    let (signal_tx, mut signal_rx) = tokio::sync::mpsc::channel(1);

    let db_conn_str = std::env::var("DB_CONNECTION_STRING")?;
    let pg_pool = sqlx::PgPool::connect(&db_conn_str).await?;
    let reqwest_client = reqwest::Client::new();

    let default_payment_processor = PaymentProcessor {
        id: PaymentProcessorIdentifier::Default,
        url: std::env::var("PAYMENT_PROCESSOR_URL_DEFAULT")?,
        failing: Arc::new(AtomicBool::new(false)),
        min_response_time: Arc::new(AtomicF64::new(0.0)),
        tax: 0.05,
        efficiency: Arc::new(AtomicF64::new(0.0)),
    };

    let fallback_payment_processor = PaymentProcessor {
        id: PaymentProcessorIdentifier::Fallback,
        url: std::env::var("PAYMENT_PROCESSOR_URL_FALLBACK")?,
        failing: Arc::new(AtomicBool::new(false)),
        min_response_time: Arc::new(AtomicF64::new(0.0)),
        tax: 0.15,
        efficiency: Arc::new(AtomicF64::new(0.0)),
    };

    let preferred_payment_processor = AtomicU16::new(0);

    let worker_url = std::env::var("WORKER_URL");

    let queue_len = AtomicI32::new(0);

    let consuming_payments = AtomicBool::new(true);

    let state = Arc::new(AppState {
        pg_pool,
        tx,
        reqwest_client,
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor,
        worker_url,
        signal_tx,
        queue_len,
        consuming_payments
    });

    let state_async_0 = state.clone();
    let state_async_1 = state.clone();

    match node.as_str() {
        "1" | "2" => {
            let app = Router::new()
                .route("/payments", routing::post(payments))
                .route("/payments-summary", routing::get(payments_summary))
                .route("/purge-payments", routing::post(purge_payments))
                .with_state(state);

            let tcp_listen = std::env::var("TCP_LISTEN")?;
            let listener = tokio::net::TcpListener::bind(tcp_listen).await?;

            axum::serve(listener, app).await?;
        },
        "3" => {
            tokio::spawn(async move {
                println!("Starting queue processor");
                loop {
                    let worker_max_threads = std::env::var("WORKER_MAX_THREADS").unwrap_or("500".to_string()).parse().unwrap();
                    let semaphore = Arc::new(Semaphore::new(worker_max_threads));
                    while let Some(event) = rx.recv().await {
                        state_async_0.queue_len.fetch_add(-1, Ordering::Relaxed);
                        if !state_async_0.consuming_payments() {
                            state_async_0.send_event(&event).await;
                            println!("Both payment processors are failing. Break event consumer loop");
                            break
                        }
                        let state_async_0 = state_async_0.clone();
                        let permit = semaphore.clone().acquire_owned().await;
                        tokio::spawn(async move {
                            let _p = permit;
                            if let Err(e) = process_queue_event(&state_async_0, &event).await {
                                eprintln!("Failed to process queue event: {e}");
                                state_async_0.send_event(&event).await;
                            }
                        });
                    }
                    signal_rx.recv().await;
                }
            });
            tokio::spawn(async move {
                println!("Starting health updater");
                loop {
                    _ = tokio::join! {
                        update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Default),
                        update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Fallback)
                    };
                    let update_result = update_preferred_payment_processor(&state_async_1);
                    if !state_async_1.consuming_payments() && update_result.is_ok() {
                        _ = state_async_1.signal_tx.send(()).await;
                        println!("One of the payment processors is healthy. Start consuming payments");
                        state_async_1.update_consuming_payments(true);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5005)).await;
                }
            });

            let app = Router::new()
                .route("/", routing::post(events))
                .with_state(state);

            let tcp_listen = std::env::var("TCP_LISTEN")?;
            let listener = tokio::net::TcpListener::bind(tcp_listen).await?;

            axum::serve(listener, app).await?;
        },
        _ => ()
    }

    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

async fn payments(State(state): State<Arc<AppState>>, Json(payload): Json<PaymentPayload>) -> axum::http::StatusCode {
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

async fn events(State(state): State<Arc<AppState>>, Json(payload): Json<QueueEvent>) -> axum::http::StatusCode {
    state.send_event(&payload.into()).await;
    axum::http::StatusCode::ACCEPTED
}

async fn purge_payments(State(state): State<Arc<AppState>>) -> axum::http::StatusCode {
    _ = sqlx::query("DELETE FROM payments").execute(&state.pg_pool).await;
    axum::http::StatusCode::ACCEPTED
}

const INSERT_STATEMENT: &'static str = "INSERT INTO payments (correlation_id, amount, payment_processor, requested_at) VALUES ($1, $2, $3, $4)";

async fn insert_payment(
    state: &Arc<AppState>,
    (correlation_id, amount): &QueueEvent,
    payment_processor_id: &PaymentProcessorIdentifier,
    requested_at: DateTime<Utc>
) -> Result<PgQueryResult, sqlx::Error> {

    let payment_processor = match payment_processor_id {
        PaymentProcessorIdentifier::Default => "D",
        PaymentProcessorIdentifier::Fallback => "F",
    };

    sqlx::query(INSERT_STATEMENT)
        .bind(correlation_id)
        .bind(amount)
        .bind(payment_processor)
        .bind(requested_at)
        .execute(&state.pg_pool)
        .await
}

async fn process_queue_event(state: &Arc<AppState>, event: &QueueEvent) -> anyhow::Result<()> {
    while state.consuming_payments() {
        if let Ok((payment_processor_id, requested_at)) = call_payment_processor(&state, &event).await {
            insert_payment(&state, &event, payment_processor_id, requested_at).await?;
            return Ok(())
        }
    }
    Err(anyhow::Error::msg("Payments are not being consumed"))
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

async fn payments_summary(State(state): State<Arc<AppState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {

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

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorHealthResponse {
    pub failing: bool,
    pub min_response_time: f64
}

async fn update_payment_processor_health(state: &Arc<AppState>, payment_processor_id: PaymentProcessorIdentifier) -> anyhow::Result<()> {

    let payment_processor = match payment_processor_id {
        PaymentProcessorIdentifier::Default => &state.default_payment_processor,
        PaymentProcessorIdentifier::Fallback => &state.fallback_payment_processor
    };

    let url = &payment_processor.url;

    let body = state.reqwest_client.get(format!("{url}/payments/service-health"))
        .send()
        .await?
        .bytes()
        .await?;

    let response = serde_json::from_slice::<PaymentProcessorHealthResponse>(&body)?;

    payment_processor.update_failing(response.failing);
    payment_processor.update_min_response_time(response.min_response_time);
    payment_processor.update_efficiency((1000.0 / f64::max(response.min_response_time, 1.0)) / payment_processor.tax);

    Ok(())
}

fn update_payment_processor_state(
    state: &Arc<AppState>,
    payment_processor_id: &PaymentProcessorIdentifier,
    failing: Option<bool>,
    min_response_time: Option<f64>
) {

    let payment_processor = match payment_processor_id {
        PaymentProcessorIdentifier::Default => &state.default_payment_processor,
        PaymentProcessorIdentifier::Fallback => &state.fallback_payment_processor
    };

    if let Some(failing) = failing {
        payment_processor.update_failing(failing);
    }
    if let Some(min_response_time) = min_response_time {
        payment_processor.update_min_response_time(min_response_time);
    }

}

fn update_preferred_payment_processor(state: &Arc<AppState>) -> anyhow::Result<()> {
    
    let new_preferred_payment_processor = {
        if state.default_payment_processor.failing() && state.fallback_payment_processor.failing() {
            state.update_consuming_payments(false);
            return Err(anyhow::Error::msg("Both payment processors are failing"));
        }
        else if state.fallback_payment_processor.failing() {
            PaymentProcessorIdentifier::Default
        }
        else if state.default_payment_processor.failing() {
            PaymentProcessorIdentifier::Fallback
        }
        else if state.default_payment_processor.min_response_time() < 10.0 {
            PaymentProcessorIdentifier::Default
        }
        else if state.default_payment_processor.efficiency() >= state.fallback_payment_processor.efficiency() {
            PaymentProcessorIdentifier::Default
        }
        else {
            PaymentProcessorIdentifier::Fallback
        }
    };

    let preferred_payment_processor = state.preferred_payment_processor();
    if preferred_payment_processor.id != new_preferred_payment_processor {
        println!("Preferred payment processor changing from {:?} to {:?}", preferred_payment_processor.id, new_preferred_payment_processor);
        state.update_preferred_payment_processor(new_preferred_payment_processor);
    }

    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorRequest {
    pub correlation_id: String,
    pub amount: Decimal,
    pub requested_at: chrono::DateTime<Utc>
}

async fn call_payment_processor<'a>(state: &'a Arc<AppState>, (correlation_id, amount): &QueueEvent) -> anyhow::Result<(&'a PaymentProcessorIdentifier, DateTime<Utc>)> {

    let payment_processor = state.preferred_payment_processor();

    if payment_processor.failing() {
        _ = update_preferred_payment_processor(&state);
        return Err(anyhow::Error::msg("Payment processor is failing"));
    }

    let id = &state.preferred_payment_processor().id;
    let url = &payment_processor.url;
    let requested_at = Utc::now();

    let body = PaymentProcessorRequest {
        correlation_id: correlation_id.to_string(),
        amount: *amount,
        requested_at,
    };
    
    let start = Instant::now();
    let response = state.reqwest_client
        .post(format!("{url}/payments"))
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&body)?)
        .send()
        .await?;
    let elapsed = start.elapsed();

    let response_status = response.status().as_u16();

    if response_status >= 400 && response_status < 500 {
        eprintln!("Got 4xx error: {response_status} - {}", response.text().await?);
        return Err(anyhow::Error::msg("Payment processor returned error"));
    }

    if response.status().as_u16() >= 500 {
        update_payment_processor_state(&state, id, Some(true), Some(elapsed.as_secs_f64()));
        _ = update_preferred_payment_processor(&state);
        return Err(anyhow::Error::msg("Payment processor returned error"));
    }

    update_payment_processor_state(&state, id, Some(false), Some(elapsed.as_secs_f64()));
    _ = update_preferred_payment_processor(&state);

    Ok((id, requested_at))
}