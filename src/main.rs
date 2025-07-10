
use std::{collections::HashMap, sync::{atomic::{AtomicI32, Ordering}, Arc}};
use axum::{extract::{Query, State}, response::IntoResponse, routing, Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgQueryResult, prelude::FromRow};
use tokio::{sync::RwLock, task::JoinHandle, time::Instant};

type QueueEvent = (String, f64, chrono::DateTime<chrono::Utc>);

#[derive(Clone, PartialEq, Eq)]
enum PaymentProcessorIdentifier {
    Default,
    Fallback,
}

#[derive(Clone)]
struct PaymentProcessor {
    pub id: PaymentProcessorIdentifier,
    pub url: String,
    pub failing: bool,
    pub min_response_time: f64,
    pub tax: f64,
    pub efficiency: f64
}

struct AppState {
    pub pg_pool: sqlx::PgPool,
    pub tx: tokio::sync::mpsc::Sender<QueueEvent>,
    pub reqwest_client: reqwest::Client,
    pub default_payment_processor: RwLock<PaymentProcessor>,
    pub fallback_payment_processor: RwLock<PaymentProcessor>,
    pub preferred_payment_processor: RwLock<PaymentProcessor>,
    pub processing_payments: dashmap::DashMap<String, JoinHandle<()>>,
    pub worker_url: Result<String, std::env::VarError>,
    pub signal_tx: tokio::sync::mpsc::Sender<()>,
    pub queue_len: AtomicI32,
    pub redirect_lock: Arc<tokio::sync::Mutex<()>>
}

impl AppState {
    async fn send_event(&self, event: QueueEvent) {
        _ = self.tx.send(event).await;
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

    let default_payment_processor = RwLock::new(PaymentProcessor {
        id: PaymentProcessorIdentifier::Default,
        url: std::env::var("PAYMENT_PROCESSOR_URL_DEFAULT")?,
        failing: false,
        min_response_time: 0.0,
        tax: 0.05,
        efficiency: 0.0
    });

    let fallback_payment_processor = RwLock::new(PaymentProcessor {
        id: PaymentProcessorIdentifier::Fallback,
        url: std::env::var("PAYMENT_PROCESSOR_URL_FALLBACK")?,
        failing: false,
        min_response_time: 0.0,
        tax: 0.15,
        efficiency: 0.0
    });

    let preferred_payment_processor = {
        let r = default_payment_processor.read().await.clone();
        RwLock::new(r)
    };

    let processing_payments = dashmap::DashMap::new();

    let worker_url = std::env::var("WORKER_URL");

    let queue_len = AtomicI32::new(0);

    let redirect_lock = Arc::new(tokio::sync::Mutex::new(()));

    let state = Arc::new(AppState {
        pg_pool,
        tx,
        reqwest_client,
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor,
        processing_payments,
        worker_url,
        signal_tx,
        queue_len,
        redirect_lock
    });

    let state_async_0 = state.clone();
    let state_async_1 = state.clone();

    match node.as_str() {
        "1" | "2" => {
            let app = Router::new()
                .route("/payments", routing::post(payments))
                .route("/payments-summary", routing::get(payments_summary))
                .with_state(state);

            let tcp_listen = std::env::var("TCP_LISTEN")?;
            let listener = tokio::net::TcpListener::bind(tcp_listen).await?;

            axum::serve(listener, app).await?;
        },
        "3" => {
            tokio::spawn(async move {
                println!("Starting queue processor");
                loop {
                    println!("Queue len: {}", state_async_0.queue_len.load(Ordering::Relaxed));
                    while let Some(event) = rx.recv().await {
                        println!("Processing queue event: {event:?}");
                        state_async_0.queue_len.fetch_add(-1, Ordering::Relaxed);
                        {
                            if state_async_0.default_payment_processor.read().await.failing && state_async_0.fallback_payment_processor.read().await.failing {
                                state_async_0.send_event(event).await;
                                println!("Both payment processors are failing. Break");
                                break
                            }
                        }
                        let event_key = serde_json::to_string(&event).unwrap();
                        if state_async_0.processing_payments.contains_key(&event_key) {
                            continue
                        }
                        let event_key_async = event_key.clone();
                        let state_async_1 = state_async_0.clone();
                        let handle = tokio::spawn(async move {
                            println!("Spawn handle -> {event:?}");
                            if let Err(e) = process_queue_event(state_async_1.clone(), event).await {
                                println!("Failed to process queue event: {e}");
                            };
                            state_async_1.processing_payments.remove(&event_key_async);
                        });
                        state_async_0.processing_payments.insert(event_key, handle);
                    }
                    signal_rx.recv().await;
                }
            });
            tokio::spawn(async move {
                println!("Starting health updater");
                loop {
                    let (default_payment_processor_failing, fallback_payment_processor_failing) = {(
                        state_async_1.default_payment_processor.read().await.failing,
                        state_async_1.fallback_payment_processor.read().await.failing
                    )};
                    _ = tokio::join! {
                        update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Default),
                        update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Fallback)
                    };
                    _ = update_preferred_payment_processor(&state_async_1).await;
                    if !default_payment_processor_failing || !fallback_payment_processor_failing {
                        _ = state_async_1.signal_tx.send(()).await;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });

            let app = Router::new()
                .route("/", routing::post(events))
                .route("/queue-length", routing::get(queue_length))
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
    amount: f64,
}

impl Into<QueueEvent> for PaymentPayload {
    fn into(self) -> QueueEvent {
        (self.correlation_id, self.amount, chrono::Utc::now())
    }
}

async fn payments(State(state): State<Arc<AppState>>, Json(payload): Json<PaymentPayload>) -> axum::http::StatusCode {
    tokio::spawn(async move {
        let event: QueueEvent = payload.into();
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
    state.send_event(payload.into()).await;
    axum::http::StatusCode::ACCEPTED
}

async fn queue_length(State(state): State<Arc<AppState>>) -> Vec<u8> {
    state.queue_len.load(Ordering::Relaxed).to_string().into_bytes()
}

async fn insert_payment(state: &Arc<AppState>, (correlation_id, amount, requested_at): QueueEvent, payment_processor_id: PaymentProcessorIdentifier) -> Result<PgQueryResult, sqlx::Error> {

    let payment_processor = match payment_processor_id {
        PaymentProcessorIdentifier::Default => "D",
        PaymentProcessorIdentifier::Fallback => "F",
    };

    sqlx::query("INSERT INTO payments (correlation_id, amount, payment_processor, requested_at) VALUES ($1, $2, $3, $4)")
        .bind(correlation_id)
        .bind(amount)
        .bind(payment_processor)
        .bind(requested_at)
        .execute(&state.pg_pool)
        .await
}

async fn process_queue_event(state: Arc<AppState>, event: QueueEvent) -> anyhow::Result<()> {
    let payment_processor_id = call_payment_processor(&state, &event).await?;
    if let Err(e) = insert_payment(&state, event, payment_processor_id).await {
        println!("Failed to insert payment: {e}");
    }
    Ok(())
}

#[derive(FromRow, Default, Clone)]
struct PaymentsSummaryDetailsRow {
    pub total_requests: i64,
    pub total_amount: f64,
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

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct PaymentsSummaryDetails {
    pub total_requests: i64,
    pub total_amount: f64
}

#[derive(Serialize, Default)]
struct PaymentsSummary {
    pub default: PaymentsSummaryDetails,
    pub fallback: PaymentsSummaryDetails
}

impl PaymentsSummary {
    pub fn new(query_result: &Vec<PaymentsSummaryDetailsRow>) -> PaymentsSummary {
        PaymentsSummary {
            default: Self::get_summary(&query_result, "D".to_string()),
            fallback: Self::get_summary(&query_result, "F".to_string())
        }
    }
    fn get_summary(query_result: &Vec<PaymentsSummaryDetailsRow>, payment_processor_id: String) -> PaymentsSummaryDetails {
        match query_result.iter().find(|row| row.payment_processor == payment_processor_id) {
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

async fn payments_summary(State(state): State<Arc<AppState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {

    let from = params.get("from").and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok());
    let to = params.get("to").and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok());

    match sqlx::query_as::<_, PaymentsSummaryDetailsRow>(PAYMENTS_SUMMARY_QUERY)
        .bind(from)
        .bind(to)
        .fetch_all(&state.pg_pool)
        .await {
            Ok(summary) => (axum::http::StatusCode::OK, serde_json::to_vec(&PaymentsSummary::new(&summary)).unwrap()),
            Err(err) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("Error: {err}").as_bytes().to_vec())
        }
}

#[derive(Deserialize)]
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

    let url = { &payment_processor.read().await.url };

    let body = state.reqwest_client.get(format!("{url}/payments/service-health"))
        .send()
        .await?
        .bytes()
        .await?;

    let response: PaymentProcessorHealthResponse = serde_json::from_slice(&body)?;

    {
        let mut payment_processor = payment_processor.write().await;
        payment_processor.failing = response.failing;
        payment_processor.min_response_time = response.min_response_time;
        payment_processor.efficiency = (1000.0 / f64::max(response.min_response_time, 1.0)) / payment_processor.tax;
    }

    Ok(())
}

async fn update_payment_processor_state(
    state: &Arc<AppState>,
    payment_processor_id: PaymentProcessorIdentifier,
    failing: Option<bool>,
    min_response_time: Option<f64>
) -> anyhow::Result<()> {

    let payment_processor = match payment_processor_id {
        PaymentProcessorIdentifier::Default => &state.default_payment_processor,
        PaymentProcessorIdentifier::Fallback => &state.fallback_payment_processor
    };

    {
        let mut payment_processor = payment_processor.write().await;
        if let Some(failing) = failing {
            payment_processor.failing = failing;
        }
        if let Some(min_response_time) = min_response_time {
            payment_processor.min_response_time = min_response_time;
        }
    }

    Ok(())
}

async fn update_preferred_payment_processor(state: &Arc<AppState>) -> anyhow::Result<()> {
    
    let default_payment_processor = &state.default_payment_processor.read().await;
    let fallback_payment_processor = &state.fallback_payment_processor.read().await;

    let new_preferred_payment_processor = PaymentProcessor::clone({
        if default_payment_processor.failing && fallback_payment_processor.failing {
            let state = state.clone();
            tokio::spawn(redirect_processing_payments(state));
            return Err(anyhow::Error::msg("Both payment processors are failing"));
        }
        else if fallback_payment_processor.failing {
            default_payment_processor
        }
        else if default_payment_processor.failing {
            fallback_payment_processor
        }
        else if default_payment_processor.min_response_time < 10.0 {
            default_payment_processor
        }
        else if default_payment_processor.efficiency >= fallback_payment_processor.efficiency {
            default_payment_processor
        }
        else {
            fallback_payment_processor
        }
    });

    {
        let preferred_payment_processor = state.preferred_payment_processor.read().await;
        if preferred_payment_processor.id != new_preferred_payment_processor.id {
            let mut preferred_payment_processor = state.preferred_payment_processor.write().await;
            *preferred_payment_processor = new_preferred_payment_processor.clone();
            let state = state.clone();
            tokio::spawn(redirect_processing_payments(state));
        }
    }

    Ok(())
}

async fn redirect_processing_payments(state: Arc<AppState>) {
    println!("Redirecting processing payments");
    if let Ok(_) = state.redirect_lock.try_lock() {
        for entry in &state.processing_payments {
            let event = serde_json::from_str(entry.key()).unwrap();
            entry.abort();
            state.send_event(event).await;
            _ = &state.processing_payments.remove(entry.key());
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorRequest {
    pub correlation_id: String,
    pub amount: f64,
    pub requested_at: String
}

async fn call_payment_processor(state: &Arc<AppState>, (correlation_id, amount, requested_at): &QueueEvent) -> anyhow::Result<PaymentProcessorIdentifier> {

    let (id, failing, url) = {
        let payment_processor = &state.preferred_payment_processor.read().await;
        (payment_processor.id.clone(), payment_processor.failing, payment_processor.url.clone())
    };

    if failing {
        let state = state.clone();
        let (correlation_id, amount, requested_at) = (correlation_id.clone(), *amount, requested_at.clone());
        tokio::spawn(async move {
            _ = update_preferred_payment_processor(&state).await;
            state.send_event((correlation_id, amount, requested_at)).await;
        });
        return Err(anyhow::Error::msg("Payment processor is failing"));
    } 

    let body = PaymentProcessorRequest {
        correlation_id: correlation_id.to_string(),
        amount: *amount,
        requested_at: requested_at.to_string(),
    };
    
    let start = Instant::now();
    let response = state.reqwest_client
        .post(url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&body)?)
        .send()
        .await?;
    let elapsed = start.elapsed();

    let state = state.clone();

    println!("payment processor status: {}", response.status());

    if response.status().as_u16() >= 500 {
        let (correlation_id, amount, requested_at) = (correlation_id.clone(), *amount, requested_at.clone());
        tokio::spawn(async move {
            _ = update_payment_processor_state(&state, id, Some(true), Some(elapsed.as_secs_f64())).await;
            _ = update_preferred_payment_processor(&state).await;
            state.send_event((correlation_id, amount, requested_at)).await;
        });
        return Err(anyhow::Error::msg("Payment processor returned error"));
    }

    let id_async = id.clone();
    tokio::spawn(async move {
        _ = update_payment_processor_state(&state, id_async, Some(false), Some(elapsed.as_secs_f64())).await;
        _ = update_preferred_payment_processor(&state).await;
    });

    Ok(id)
}