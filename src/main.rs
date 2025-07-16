
use std::sync::{atomic::{AtomicBool, AtomicI32, AtomicU16, Ordering}, Arc};
use axum::{routing, Router};
use tokio::sync::Semaphore;

use crate::{app_state::AppState, atomicf64::AtomicF64, payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}};

mod app_state;
mod atomicf64;
mod payment_processor;
mod http_api;
mod storage;
mod worker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let node = std::env::var("NODE")?;
    let is_primary_node = node == "1";

    let (tx, mut rx) = tokio::sync::mpsc::channel(50000);

    let (signal_tx, mut signal_rx) = tokio::sync::mpsc::channel(1);

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

    let queue_len = AtomicI32::new(0);

    let consuming_payments = AtomicBool::new(true);

    let storage = storage::PaymentsStorage::new();

    let state = Arc::new(AppState {
        tx,
        reqwest_client,
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor,
        signal_tx,
        queue_len,
        consuming_payments,
        storage,
    });

    let state_async_0 = state.clone();
    let state_async_1 = state.clone();
    
    let app = Router::new()
        .route("/payments", routing::post(http_api::payments))
        .route("/payments-summary", routing::get(http_api::payments_summary))
        .route("/purge-payments", routing::post(http_api::purge_payments))
        .with_state(state);

    let tcp_listen = std::env::var("TCP_LISTEN")?;
    let listener = tokio::net::TcpListener::bind(tcp_listen).await?;

    axum::serve(listener, app).await?;

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
                    if let Err(e) = worker::process_queue_event(&state_async_0, &event).await {
                        eprintln!("Failed to process queue event: {e}");
                        state_async_0.send_event(&event).await;
                    }
                });
            }
            if is_primary_node {
                signal_rx.recv().await;
            }
            else {
                loop {
                    worker::update_payment_processor_health_statuses_from_file(&state_async_0).await.unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
    });
    if is_primary_node {
        tokio::spawn(async move {
            println!("Starting health updater");
            loop {
                _ = tokio::join! {
                    worker::update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Default),
                    worker::update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Fallback)
                };
                let update_result = &state_async_1.update_preferred_payment_processor();
                if !state_async_1.consuming_payments() && update_result.is_ok() {
                    _ = state_async_1.signal_tx.send(()).await;
                    println!("One of the payment processors is healthy. Start consuming payments");
                    state_async_1.update_consuming_payments(true);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5005)).await;
            }
        });
    }

    Ok(())
}