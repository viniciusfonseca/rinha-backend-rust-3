use std::sync::Arc;

use axum::{routing, Router};
use crossbeam::channel::TryRecvError;
use tokio::net::UnixDatagram;

mod payments;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use crate::{summary::PAYMENTS_SUMMARY_QUERY, uds::set_socket_permissions};

#[derive(Clone)]
struct ApiState {
    tx: crossbeam::channel::Sender<(String, f64)>,
    psql_client: Arc<tokio_postgres::Client>,
    summary_statement: tokio_postgres::Statement,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let psql_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let (tx, rx) = crossbeam::channel::unbounded();
    let (psql_client, psql_conn) = tokio_postgres::connect(&psql_url, tokio_postgres::NoTls).await?;
    let summary_statement = psql_client.prepare(PAYMENTS_SUMMARY_QUERY).await?;

    tokio::spawn(async move {
        if let Err(e) = psql_conn.await {
            eprintln!("Postgres connection error: {e}");
        }
    });

    let state = ApiState {
        tx,
        psql_client: Arc::new(psql_client),
        summary_statement,
    };
    
    let channel_threads = std::env::var("CHANNEL_THREADS")
        .unwrap_or("5".to_string())
        .parse()?;

    for _ in 0..channel_threads {
        let rx = rx.clone();
        tokio::spawn(async move {
            let worker_socket = "/tmp/sockets/worker.sock";
            let tx_worker = UnixDatagram::bind(worker_socket)?;
            set_socket_permissions(worker_socket)?;
            loop {
                match rx.try_recv() {
                    Ok((correlation_id, amount)) => {
                        tx_worker.send(format!("{correlation_id}:{amount}").as_bytes()).await?;
                    }
                    Err(TryRecvError::Empty) => continue,
                    Err(e) => break eprintln!("Error receiving from channel: {e}"),
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    let listener = uds::create_unix_socket(&socket_path).await?;

    axum::serve(listener, app).await?;

    Ok(())
}