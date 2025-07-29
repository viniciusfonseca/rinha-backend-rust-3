use std::sync::Arc;

use axum::{routing, Router};
use tokio::net::UnixDatagram;

mod payments;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone)]
struct ApiState {
    tx: async_channel::Sender<(String, f64)>,
    db_url: String,
    reqwest_client: reqwest::Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let (tx, rx) = async_channel::bounded(16000);

    let state = ApiState {
        tx,
        db_url: std::env::var("DATABASE_URL")?,
        reqwest_client: reqwest::Client::new(),
    };
    
    let channel_threads = std::env::var("CHANNEL_THREADS")
        .unwrap_or("5".to_string())
        .parse()?;

    println!("Starting with {channel_threads} channel threads");

    for _ in 0..channel_threads {
        let rx = rx.clone();
        tokio::spawn(async move {
            let worker_socket = "/tmp/sockets/worker.sock";
            let tx_worker = UnixDatagram::unbound()?;
            let backoff = std::env::var("BACKOFF_CHANNEL")
                .unwrap_or("0".to_string())
                .parse()?;
            loop {
                match rx.recv().await {
                    Ok((correlation_id, amount)) => {
                        tx_worker.send_to(format!("{correlation_id}:{amount}").as_bytes(), worker_socket).await?;
                    }
                    Err(e) => break eprintln!("Error receiving from channel: {e}"),
                }
                if backoff > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    let app = Router::new()
        .route("/payments", routing::post(payments::enqueue_payment))
        .route("/payments-summary", routing::get(summary::summary))
        .route("/purge-payments", routing::post(payments::purge_payments))
        .with_state(state);

    let sockets_dir = "/tmp/sockets";
    std::fs::create_dir_all(std::path::Path::new(sockets_dir))?;

    let hostname = std::env::var("HOSTNAME")?;

    let socket_path = format!("{sockets_dir}/{hostname}.sock");
    println!("Binding to socket: {socket_path}");
    let listener = uds::create_unix_socket(&socket_path).await?;

    axum::serve(listener, app).await?;

    Ok(())
}