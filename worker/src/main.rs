use std::sync::{atomic::{AtomicBool, AtomicU16}, Arc};

use rust_decimal::Decimal;

use crate::{payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}, storage::Storage};

mod atomicf64;
mod health_check;
mod payment_processor;
mod storage;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone)]
struct WorkerState {
    pub reqwest_client: reqwest::Client,
    pub default_payment_processor: PaymentProcessor,
    pub fallback_payment_processor: PaymentProcessor,
    pub preferred_payment_processor: Arc<AtomicU16>,
    pub signal_tx: async_channel::Sender<()>,
    pub consuming_payments: Arc<AtomicBool>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    
    let worker_threads = std::env::var("WORKER_THREADS")
        .unwrap_or("10".to_string())
        .parse()?;

    let default_payment_processor = PaymentProcessor::new(
        PaymentProcessorIdentifier::Default,
        std::env::var("PAYMENT_PROCESSOR_URL_DEFAULT")?,
        0.05
    );

    let fallback_payment_processor = PaymentProcessor::new(
        PaymentProcessorIdentifier::Fallback,
        std::env::var("PAYMENT_PROCESSOR_URL_FALLBACK")?,
        0.15
    );

    let (tx, rx) = async_channel::bounded(16000);
    let (signal_tx, signal_rx) = async_channel::bounded(worker_threads);

    let state = WorkerState {
        reqwest_client: reqwest::Client::new(),
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor: Arc::new(AtomicU16::new(0)),
        signal_tx,
        consuming_payments: Arc::new(AtomicBool::new(true)),
    };

    let sockets_dir = "/tmp/sockets";
    std::fs::create_dir_all(std::path::Path::new(sockets_dir))?;

    let worker_socket_path = "/tmp/sockets/worker.sock";

    let socket = uds::bind_unix_datagram_socket(worker_socket_path).await?;
    let socket = Arc::new(socket);

    let channel_threads = std::env::var("CHANNEL_THREADS")
        .unwrap_or("5".to_string())
        .parse()?;

    for _ in 0..channel_threads {
        let socket = socket.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut buf = [0; 64];
                match socket.recv(&mut buf).await {
                    Ok(size) => {
                        if size == 0 { continue }
                        let message = String::from_utf8_lossy(&buf[..size]);
                        // println!("Received message: {}", message);
                        let split = message.split(':').collect::<Vec<&str>>();
                        let correlation_id = split[0].to_string();
                        let amount: Decimal = split[1].parse().unwrap_or(Decimal::ZERO);
                        tx.send((correlation_id, amount)).await?;
                    }
                    Err(e) => break eprintln!("Error receiving from socket: {}", e),
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    for _ in 0..worker_threads {
        let state = state.clone();
        let rx = rx.clone();
        let tx = tx.clone();
        let signal_rx = signal_rx.clone();
        tokio::spawn(async move {
            'x: loop {
                let storage = Storage::init().await?;
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if !state.consuming_payments() {
                                println!("Both payment processors are failing. Break worker loop");
                                _ = tx.send(event).await?;
                                break;
                            }
                            match state.process_payment(&event).await {
                                Ok((payment_processor_id, requested_at)) => {
                                    storage.save_payment(event.1, payment_processor_id, requested_at).await?;
                                    // println!("Saved payment: {event:?}");
                                }
                                Err(e) => {
                                    eprintln!("Error processing payment: {}", e);
                                    tx.send(event).await?;
                                },
                            }
                        }
                        Err(e) => break 'x eprintln!("Error receiving from channel: {}", e),
                    }
                }
                loop {
                    match signal_rx.recv().await {
                        Ok(_) => break,
                        Err(e) => break 'x eprintln!("Error receiving from signal channel: {}", e),
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    let health_check_interval = tokio::time::Duration::from_secs(5);

    loop {
        let (default_health, fallback_health) = tokio::join!(
            state.health_check(PaymentProcessorIdentifier::Default),
            state.health_check(PaymentProcessorIdentifier::Fallback)
        );
        if default_health.is_err() || fallback_health.is_err() {
            break eprintln!("Error checking payment processor health: {:?}", default_health.err().or(fallback_health.err()));
        }
        let update_result = &state.update_preferred_payment_processor();
        if !state.consuming_payments() && update_result.is_ok() {
            println!("One of the payment processors is healthy. Start consuming payments");
            for _ in 0..worker_threads {
                _ = state.signal_tx.send(()).await;
            }
            state.update_consuming_payments(true);
        }
        tokio::time::sleep(health_check_interval).await;
    }

    Ok(())
}
