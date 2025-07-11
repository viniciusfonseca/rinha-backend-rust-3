
use std::{collections::HashMap, sync::{atomic::{AtomicBool, AtomicI32, Ordering}, Arc}};
use axum::{extract::{Query, State}, http::HeaderMap, response::IntoResponse, routing, Json, Router};
use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize, Serializer};
use sqlx::{postgres::PgQueryResult, prelude::FromRow};
use tokio::{sync::RwLock, time::Instant};

type QueueEvent = (String, f64, chrono::DateTime<chrono::Utc>);

#[derive(Clone, PartialEq, Eq, Debug)]
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
    pub worker_url: Result<String, std::env::VarError>,
    pub signal_tx: tokio::sync::mpsc::Sender<()>,
    pub queue_len: AtomicI32,
    pub consuming_payments: AtomicBool
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
    let worker_max_threads = std::env::var("WORKER_MAX_THREADS").unwrap_or("500".to_string()).parse()?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(worker_max_threads);

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
        let processor = { default_payment_processor.read().await }.clone();
        RwLock::new(processor)
    };

    let worker_url = std::env::var("WORKER_URL");

    let queue_len = AtomicI32::new(0);

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
        consuming_payments: AtomicBool::new(true)
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
                    while let Some(event) = rx.recv().await {
                        state_async_0.queue_len.fetch_add(-1, Ordering::Relaxed);
                        if !state_async_0.consuming_payments.load(Ordering::Relaxed) {
                            state_async_0.send_event(event).await;
                            println!("Both payment processors are failing. Break event consumer loop");
                            break
                        }
                        let state_async_0 = state_async_0.clone();
                        tokio::spawn(async move {
                            if let Err(e) = process_queue_event(state_async_0.clone(), event).await {
                                eprintln!("Failed to process queue event: {e}");
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
                    _ = update_preferred_payment_processor(&state_async_1).await;
                    let (default_payment_processor_failing, fallback_payment_processor_failing) = {(
                        state_async_1.default_payment_processor.read().await.failing,
                        state_async_1.fallback_payment_processor.read().await.failing
                    )};
                    if !state_async_1.consuming_payments.load(Ordering::Relaxed) && (!default_payment_processor_failing || !fallback_payment_processor_failing) {
                        _ = state_async_1.signal_tx.send(()).await;
                        println!("One of the payment processors is healthy. Start consuming payments");
                        state_async_1.consuming_payments.store(true, Ordering::Relaxed);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5005)).await;
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

async fn payments(State(state): State<Arc<AppState>>, headers: HeaderMap, Json(payload): Json<PaymentPayload>) -> axum::http::StatusCode {
    tokio::spawn(async move {
        let requested_at = headers.get(axum::http::header::DATE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(Utc::now);
        let event: QueueEvent = (payload.correlation_id, payload.amount, requested_at);
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

const INSERT_STATEMENT: &'static str = "INSERT INTO payments (correlation_id, amount, payment_processor, requested_at) VALUES ($1, $2, $3, $4)";

async fn insert_payment(state: &Arc<AppState>, (correlation_id, amount, requested_at): QueueEvent, payment_processor_id: PaymentProcessorIdentifier) -> Result<PgQueryResult, sqlx::Error> {

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

async fn process_queue_event(state: Arc<AppState>, event: QueueEvent) -> anyhow::Result<()> {
    let payment_processor_id = call_payment_processor(&state, &event).await?;
    insert_payment(&state, event, payment_processor_id).await?;
    Ok(())
}

#[derive(FromRow, Default, Clone, Debug)]
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

fn f64_ser2<S>(fv: &f64, se: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    se.serialize_f64((fv * 100.).round() / 100.)
}

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
struct PaymentsSummaryDetails {
    pub total_requests: i64,
    #[serde(serialize_with = "f64_ser2")]
    pub total_amount: f64
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

fn date_str_to_datetime(value: &str) -> Option<chrono::DateTime<Utc>> {
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

    let url = { payment_processor.read().await.url.clone() };

    let body = state.reqwest_client.get(format!("{url}/payments/service-health"))
        .send()
        .await?
        .bytes()
        .await?;

    let response = serde_json::from_slice::<PaymentProcessorHealthResponse>(&body)?;

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
    
    let default_payment_processor = &{ state.default_payment_processor.read().await.clone() };
    let fallback_payment_processor = &{ state.fallback_payment_processor.read().await.clone() };

    let new_preferred_payment_processor = PaymentProcessor::clone({
        if default_payment_processor.failing && fallback_payment_processor.failing {
            state.consuming_payments.store(false, Ordering::Relaxed);
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

    let preferred_payment_processor = { state.preferred_payment_processor.read().await.clone() };
    if preferred_payment_processor.id != new_preferred_payment_processor.id {
        println!("Preferred payment processor changing from {:?} to {:?}", preferred_payment_processor.id, new_preferred_payment_processor.id);
        let mut preferred_payment_processor = state.preferred_payment_processor.write().await;
        *preferred_payment_processor = new_preferred_payment_processor.clone();
    }

    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PaymentProcessorRequest {
    pub correlation_id: String,
    pub amount: f64,
    pub requested_at: chrono::DateTime<Utc>
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
        requested_at: *requested_at,
    };
    
    let start = Instant::now();
    let response = state.reqwest_client
        .post(format!("{url}/payments"))
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(&body)?)
        .send()
        .await?;
    let elapsed = start.elapsed();

    let state = state.clone();

    let response_status = response.status().as_u16();

    if response_status >= 400 && response_status < 500 {
        return Err(anyhow::Error::msg("Payment processor returned error"));
    }

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