use std::sync::Arc;

use axum::{routing, Router};
use rust_decimal::Decimal;
use tokio::net::UnixDatagram;

mod payments;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use crate::summary::PAYMENTS_SUMMARY_QUERY;

#[derive(Clone)]
struct ApiState {
    tx: async_channel::Sender<(String, Decimal)>,
    psql_client: Arc<tokio_postgres::Client>,
    summary_statement: tokio_postgres::Statement,
    purge_statement: tokio_postgres::Statement,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let psql_url = std::env::var("DATABASE_URL")?;

    let (psql_client, psql_conn) = tokio_postgres::connect(&psql_url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = psql_conn.await {
            eprintln!("Postgres connection error: {e}");
        }
    });

    let summary_statement = psql_client.prepare(PAYMENTS_SUMMARY_QUERY).await?;
    let purge_statement = psql_client.prepare("DELETE FROM PAYMENTS").await?;
    let (tx, rx) = async_channel::unbounded();

    let state = ApiState {
        tx,
        psql_client: Arc::new(psql_client),
        summary_statement,
        purge_statement,
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
            loop {
                match rx.recv().await {
                    Ok((correlation_id, amount)) => {
                        tx_worker.send_to(format!("{correlation_id}:{amount}").as_bytes(), worker_socket).await?;
                    }
                    Err(e) => break eprintln!("Error receiving from channel: {e}"),
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