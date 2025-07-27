
use std::{os::unix::fs::PermissionsExt, sync::{atomic::{AtomicBool, AtomicU16}, Arc}};
use axum::{routing, Router};
use tokio::io::AsyncWriteExt;

use crate::{app_state::AppState, atomicf64::AtomicF64, payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}, storage::PAYMENTS_STORAGE_PATH};

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

    let (tx, rx) = tokio_mpmc::channel(50000);

    let worker_threads = std::env::var("WORKER_THREADS")
        .unwrap_or("500".to_string()).parse().unwrap();

    let (signal_tx, signal_rx) = tokio_mpmc::channel(worker_threads);

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

    let state = Arc::new(AppState {
        tx: tx.clone(),
        reqwest_client,
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor,
        signal_tx,
        consuming_payments,
    });
    
    let state_async_0 = state.clone();
    let state_async_1 = state.clone();

    std::fs::create_dir_all(PAYMENTS_STORAGE_PATH).expect("Failed to create directory");

    for _ in 1..=worker_threads {
        let state_async_0 = state_async_0.clone();
        let rx = rx.clone();
        let signal_rx = signal_rx.clone();

        tokio::spawn(async move {
            let file = storage::get_datafile().await.expect("Failed to get data file");
            let mut bufwriter = tokio::io::BufWriter::new(tokio::fs::File::from(file));
            loop {
                while let Ok(Some(ref event)) = rx.recv().await {
                    if !state_async_0.consuming_payments() {
                        println!("Both payment processors are failing. Break worker loop");
                        _ = state_async_0.tx.send(event.clone()).await;
                        break;
                    }
                    match worker::process_queue_event(&state_async_0, &event).await {
                        Ok((amount, payment_processor_id, requested_at)) => {
                            let bytes = format!("{},{},{}\n", amount, payment_processor_id, requested_at).into_bytes();
                            bufwriter.write(&bytes).await.expect("Failed to write to file");
                            bufwriter.flush().await.expect("Failed to flush file");
                        },
                        Err(e) => {
                            eprintln!("Error processing queue event: {}", e);
                            _ = state_async_0.tx.send(event.clone()).await;
                        }
                    }
                }
                _ = signal_rx.recv().await;
            }
        });
    }

    tokio::spawn(async move {
        println!("Starting batch processor");

        let health_check_interval = if is_primary_node {
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
                for _ in 1..=worker_threads {
                    _ = state_async_1.signal_tx.send(()).await;
                }
                println!("One of the payment processors is healthy. Start consuming payments");
                state_async_1.update_consuming_payments(true);
            }
            tokio::time::sleep(health_check_interval).await;
        }
    });

    let app = Router::new()
        .route("/payments", routing::post(http_api::payments))
        .route("/payments-summary", routing::get(http_api::payments_summary))
        .route("/purge-payments", routing::post(http_api::purge_payments))
        .with_state(tx.clone());

    let hostname = std::env::var("HOSTNAME").unwrap();

    let sockets_dir = "/tmp/sockets";
    std::fs::create_dir_all(std::path::Path::new(sockets_dir)).unwrap();
    let socket_path = format!("{sockets_dir}/{hostname}.sock");
    match tokio::fs::remove_file(&socket_path).await {
        Err(e) => println!("warn: unable to unlink path {socket_path}: {e}"),
        _ => ()
    };

    let listener = std::os::unix::net::UnixListener::bind(&socket_path)
        .expect(format!("error listening to socket {socket_path}").as_str());
    listener.set_nonblocking(true).unwrap();
    let mut permissions = std::fs::metadata(&socket_path).unwrap().permissions();
    permissions.set_mode(0o777);
    std::fs::set_permissions(&socket_path, permissions).unwrap();

    let listener = tokio::net::UnixListener::from_std(listener)
        .expect("error parsing std listener");

    axum::serve(listener, app).await?;

    Ok(())
}