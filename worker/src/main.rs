use std::sync::{atomic::{AtomicBool, AtomicU16}, Arc};

use crossbeam::channel::TryRecvError;
use tokio::net::UnixDatagram;

use crate::{atomicf64::AtomicF64, payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}, storage::Storage};

mod atomicf64;
mod health_check;
mod payment_processor;
mod storage;

#[derive(Clone)]
struct WorkerState {
    pub reqwest_client: reqwest::Client,
    pub default_payment_processor: PaymentProcessor,
    pub fallback_payment_processor: PaymentProcessor,
    pub preferred_payment_processor: Arc<AtomicU16>,
    pub signal_tx: crossbeam::channel::Sender<()>,
    pub consuming_payments: Arc<AtomicBool>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    
    let channel_threads = std::env::var("CHANNEL_THREADS")
        .unwrap_or("5".to_string())
        .parse()?;

    let worker_threads = std::env::var("WORKER_THREADS")
        .unwrap_or("10".to_string())
        .parse()?;

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

    let (tx, rx) = crossbeam::channel::unbounded();
    let (signal_tx, signal_rx) = crossbeam::channel::bounded(worker_threads);

    let state = WorkerState {
        reqwest_client: reqwest::Client::new(),
        default_payment_processor,
        fallback_payment_processor,
        preferred_payment_processor: Arc::new(AtomicU16::new(0)),
        signal_tx,
        consuming_payments: Arc::new(AtomicBool::new(true)),
    };

    for _ in 0..channel_threads {
        let worker_socket = "/tmp/sockets/worker.sock";
        let socket = UnixDatagram::bind(worker_socket)?;
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut buf = [0; 64];
                match socket.recv(&mut buf).await {
                    Ok(size) => {
                        let message = String::from_utf8_lossy(&buf[..size]);
                        let split = message.split(':').collect::<Vec<&str>>();
                        let correlation_id = split[0].to_string();
                        let amount: f64 = split[1].parse().unwrap_or(0.0);
                        tx.send((correlation_id, amount)).unwrap();
                    }
                    Err(e) => eprintln!("Error receiving from socket: {}", e),
                }
            }
        });
    }

    for _ in 0..worker_threads {
        let state = state.clone();
        let rx = rx.clone();
        let tx = tx.clone();
        let signal_rx = signal_rx.clone();
        let storage = Storage::init().await;
        tokio::spawn(async move {
            loop {
                loop {
                    match rx.try_recv() {
                        Ok(event) => {
                            if !state.consuming_payments() {
                                println!("Both payment processors are failing. Break worker loop");
                                _ = tx.send(event);
                                break;
                            }
                            match state.process_payment(&event).await {
                                Ok((payment_processor_id, requested_at)) => {
                                    storage.save_payment(event.1, payment_processor_id, requested_at).await.unwrap();
                                }
                                Err(e) => {
                                    eprintln!("Error processing payment: {}", e);
                                    tx.send(event).unwrap();
                                },
                            }
                        }
                        Err(TryRecvError::Empty) => continue,
                        Err(e) => eprintln!("Error receiving from channel: {}", e),
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                loop {
                    match signal_rx.try_recv() {
                        Ok(_) => break,
                        Err(TryRecvError::Empty) => continue,
                        Err(e) => eprintln!("Error receiving from signal channel: {}", e),
                    }
                }
            }
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
            for _ in 0..worker_threads {
                _ = state.signal_tx.send(());
            }
            println!("One of the payment processors is healthy. Start consuming payments");
            state.update_consuming_payments(true);
        }
        tokio::time::sleep(health_check_interval).await;
    }

    Ok(())
}
