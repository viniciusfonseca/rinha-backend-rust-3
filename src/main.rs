
use std::sync::{atomic::{AtomicBool, AtomicI32, AtomicU16, Ordering}, Arc};
use axum::{routing, Router};
use tokio::{io::AsyncWriteExt, sync::Semaphore};
// use tokio::sync::Semaphore;

use crate::{app_state::AppState, atomicf64::AtomicF64, payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}, storage::{PAYMENTS_STORAGE_PATH, PAYMENTS_STORAGE_PATH_DATA}, worker::update_payment_processor_health_statuses_from_file};

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

    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(50000);

    let state = Arc::new(AppState {
        tx,
        reqwest_client,
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor,
        signal_tx,
        queue_len,
        consuming_payments,
        batch_tx,
    });

    let state_async_0 = state.clone();
    let state_async_1 = state.clone();

    tokio::spawn(async move {
        println!("Starting queue processor");
        loop {
            let worker_max_threads = std::env::var("WORKER_MAX_THREADS")
                .unwrap_or("500".to_string()).parse().unwrap();
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
                continue;
            }
            while !state_async_0.consuming_payments() {
                _ = update_payment_processor_health_statuses_from_file(&state_async_0).await;
                _ = state_async_0.update_preferred_payment_processor();
            }
        }
    });

    tokio::spawn(async move {
        println!("Starting batch processor");
        std::fs::create_dir_all(PAYMENTS_STORAGE_PATH).ok();

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&PAYMENTS_STORAGE_PATH_DATA)
            .await
            .expect("Failed to open or create partition file");

        let mut bufwriter = tokio::io::BufWriter::new(file);

        let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(30));

        loop {
            tokio::select! {
                Some((correlation_id, amount, payment_processor_id, requested_at)) = batch_rx.recv() => {
                    let bytes = format!("{},{},{},{}\n", correlation_id, amount, payment_processor_id, requested_at.to_rfc3339()).into_bytes();
                    bufwriter.write_all(&bytes).await.expect("Failed to write to file");
                },
                _ = ticker.tick() => {
                    bufwriter.flush().await.expect("Failed to flush file");
                },
            }
        }
    });

    tokio::spawn(async move {

        println!("Starting health updater");
        let health_status_update_period = if is_primary_node {
            tokio::time::Duration::from_millis(5005)
        } else {
            tokio::time::Duration::from_millis(500)
        };
        loop {
            if is_primary_node {
                _ = tokio::join! {
                    worker::update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Default),
                    worker::update_payment_processor_health(&state_async_1, PaymentProcessorIdentifier::Fallback)
                };
            }
            else {
                _ = worker::update_payment_processor_health_statuses_from_file(&state_async_1).await;
            }
            let update_result = &state_async_1.update_preferred_payment_processor();
            if !state_async_1.consuming_payments() && update_result.is_ok() {
                _ = state_async_1.signal_tx.send(()).await;
                println!("One of the payment processors is healthy. Start consuming payments");
                state_async_1.update_consuming_payments(true);
            }
            tokio::time::sleep(health_status_update_period).await;
        }
    });

    let app = Router::new()
        .route("/payments", routing::post(http_api::payments))
        .route("/payments-summary", routing::get(http_api::payments_summary))
        .route("/purge-payments", routing::post(http_api::purge_payments))
        .with_state(state);

    let tcp_listen = std::env::var("TCP_LISTEN")?;
    let listener = tokio::net::TcpListener::bind(tcp_listen).await?;

    axum::serve(listener, app).await?;

    Ok(())
}