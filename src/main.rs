
use std::sync::{atomic::{AtomicBool, AtomicU16}, Arc};
use axum::{routing, Router};
use tokio::{io::AsyncWriteExt, sync::Semaphore};

use crate::{app_state::AppState, atomicf64::AtomicF64, payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}, storage::{PAYMENTS_STORAGE_PATH, PAYMENTS_STORAGE_PATH_DATA}};

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

    let consuming_payments = AtomicBool::new(true);

    let (batch_tx, mut batch_rx) = tokio::sync::mpsc::channel(50000);

    let state = Arc::new(AppState {
        tx,
        reqwest_client,
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor,
        signal_tx,
        consuming_payments,
        batch_tx,
    });

    let state_async_0 = state.clone();
    let state_async_1 = state.clone();

    let worker_max_threads = std::env::var("WORKER_MAX_THREADS")
        .unwrap_or("500".to_string()).parse().unwrap();

    tokio::spawn(async move {
        println!("Starting queue processor");
        loop {
            let semaphore = Arc::new(Semaphore::new(worker_max_threads));
            while let Some(event) = rx.recv().await {
                if !state_async_0.consuming_payments() {
                    _ = state_async_0.tx.send(event).await;
                    println!("Both payment processors are failing. Break event consumer loop");
                    break
                }
                let state_async_0 = state_async_0.clone();
                let permit = semaphore.clone().acquire_owned().await;
                tokio::spawn(async move {
                    let _p = permit;
                    if worker::process_queue_event(&state_async_0, &event).await.is_err() {
                        _ = state_async_0.tx.send(event).await;
                    }
                });
            }
            signal_rx.recv().await;
        }
    });

    tokio::spawn(async move {
        println!("Starting batch processor");
        std::fs::create_dir_all(PAYMENTS_STORAGE_PATH).expect("Failed to create directory");

        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&PAYMENTS_STORAGE_PATH_DATA)
            .await
            .expect("Failed to open or create partition file");

        // let mut records = 0;
        let mut bufwriter = tokio::io::BufWriter::new(file);

        let mut flush_ticker = tokio::time::interval(tokio::time::Duration::from_millis(10));
        let mut health_update_ticker = tokio::time::interval(if is_primary_node {
            tokio::time::Duration::from_millis(5005)
        } else {
            tokio::time::Duration::from_millis(500)
        });

        loop {
            tokio::select! {
                Some((amount, payment_processor_id, requested_at)) = batch_rx.recv() => {
                    let bytes = format!("{},{},{}\n", amount, payment_processor_id, requested_at.to_rfc3339()).into_bytes();
                    bufwriter.write(&bytes).await.expect("Failed to write to file");
                    // records += 1;
                },
                _ = flush_ticker.tick() => {
                    //     if bufwriter.buffer().is_empty() {
                        //         continue;
                        //     }
                        // let start = Instant::now();
                        // let elapsed = start.elapsed().as_millis();
                        // println!("Flushed {records} records in {}ms at {}", elapsed, Utc::now().to_rfc3339());
                        // records = 0;
                    bufwriter.flush().await.expect("Failed to flush file");
                },
                _ = health_update_ticker.tick() => {
                    let state_async_1 = state_async_1.clone();
                    tokio::spawn(async move {
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
                    });
                }
            }
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