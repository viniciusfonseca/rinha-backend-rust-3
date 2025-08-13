use std::sync::{atomic::{AtomicBool, AtomicU16}, Arc};

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{payment_processor::{PaymentProcessor, PaymentProcessorIdentifier}};

mod health_check;
mod payment_processor;
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

async fn connect_pg() -> anyhow::Result<tokio_postgres::Client> {
    let psql_url = std::env::var("DATABASE_URL")?;
    loop {
        if let Ok((psql_client, psql_conn)) = tokio_postgres::connect(&psql_url, tokio_postgres::NoTls).await {
            tokio::spawn(async move {
                if let Err(e) = psql_conn.await {
                    eprintln!("Postgres connection error: {e}");
                }
            });
            return Ok(psql_client);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PaymentPayload {
    correlation_id: String,
    amount: Decimal,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    
    let worker_threads = std::env::var("WORKER_THREADS")
        .unwrap_or("10".to_string())
        .parse()?;

    let default_payment_processor = PaymentProcessor::new(
        PaymentProcessorIdentifier::Default,
        std::env::var("PAYMENT_PROCESSOR_URL_DEFAULT")?,
    );

    let fallback_payment_processor = PaymentProcessor::new(
        PaymentProcessorIdentifier::Fallback,
        std::env::var("PAYMENT_PROCESSOR_URL_FALLBACK")?,
    );

    let (tx, rx) = async_channel::unbounded();
    let (batch_tx, batch_rx) = async_channel::unbounded();
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
            let mut buffer = [0; 256];
            while let Ok(n) = socket.recv(&mut buffer).await {
                if n == 0 { continue }
                let mut headers = [httparse::EMPTY_HEADER; 64];
                let mut req = httparse::Request::new(&mut headers);
                if let Ok(httparse::Status::Complete(body_start)) = req.parse(&buffer[..n]) {
                    let payment_payload: PaymentPayload = serde_json::from_slice(&buffer[body_start..n])?;
                    tx.send((payment_payload.correlation_id, payment_payload.amount)).await?;
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
        let batch_tx = batch_tx.clone();
        tokio::spawn(async move {
            loop {
                while let Ok(event) = rx.recv().await {
                    if !state.consuming_payments() {
                        println!("Both payment processors are failing. Break worker loop");
                        _ = tx.send(event).await?;
                        break;
                    }
                    match state.process_payment(&event).await {
                        Ok((payment_processor_id, requested_at)) =>
                            batch_tx.send((event.1.clone(), payment_processor_id.to_owned(), requested_at)).await?,
                        Err(e) => {
                            eprintln!("Error processing payment: {}", e);
                            tx.send(event).await?
                        },
                    }
                }
                if let Err(e) = signal_rx.recv().await {
                    break eprintln!("Error receiving from signal channel: {}", e)
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    tokio::spawn(async move {
        let mut client = connect_pg().await?;
        loop {
            let mut statements_default = Vec::new();
            let mut statements_fallback = Vec::new();
            while statements_default.len() + statements_fallback.len() < 1000 &&
            let Ok((amount, payment_processor_id, requested_at)) = batch_rx.try_recv() {
                match payment_processor_id {
                    PaymentProcessorIdentifier::Default => statements_default.push(format!("({amount}, '{requested_at}')")),
                    PaymentProcessorIdentifier::Fallback => statements_fallback.push(format!("({amount}, '{requested_at}')")),
                }
            }
            let insert_default = if statements_default.is_empty() {
                String::new()
            } else {
                format!("INSERT INTO payments_default (amount, requested_at) VALUES {};", statements_default.join(","))
            };
            let insert_fallback = if statements_fallback.is_empty() {
                String::new()
            } else {
                format!("INSERT INTO payments_fallback (amount, requested_at) VALUES {};", statements_fallback.join(","))
            };
            let transaction = client.transaction().await.expect("Error opening transaction");
            transaction.batch_execute(&(insert_default + &insert_fallback)).await.expect("Error batch execute");
            if let Err(e) = transaction.commit().await {
                break eprintln!("Error saving payments: {}", e);
            };
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        Ok::<(), anyhow::Error>(())
    });

    let health_check_interval = tokio::time::Duration::from_secs(1);

    'x: loop {
        let (default_health, fallback_health) = tokio::join!(
            state.health_check(PaymentProcessorIdentifier::Default),
            state.health_check(PaymentProcessorIdentifier::Fallback)
        );
        if default_health.is_err() || fallback_health.is_err() {
            continue;
        }
        let update_result = &state.update_preferred_payment_processor();
        if !state.consuming_payments() && update_result.is_ok() {
            println!("One of the payment processors is healthy. Start consuming payments");
            for _ in 0..worker_threads {
                if let Err(e) = state.signal_tx.send(()).await {
                    break 'x eprintln!("Error sending to signal channel: {}", e)
                }
            }
            state.update_consuming_payments(true);
        }
        tokio::time::sleep(health_check_interval).await;
    }

    Ok(())
}
