use std::sync::Arc;

use rust_decimal::Decimal;
use tokio::{io::AsyncWriteExt, net::UnixDatagram};

mod handler;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use crate::{handler::handler_loop, summary::PAYMENTS_SUMMARY_QUERY};

#[derive(Clone)]
struct ApiState {
    tx: async_channel::Sender<(String, Decimal)>,
    psql_client: Arc<tokio_postgres::Client>,
    summary_statement: tokio_postgres::Statement,
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let psql_client = connect_pg().await?;

    let summary_statement = psql_client.prepare(PAYMENTS_SUMMARY_QUERY).await?;
    let (tx, rx) = async_channel::unbounded();
    let (http_tx, http_rx) = async_channel::unbounded();

    let state = ApiState {
        tx,
        psql_client: Arc::new(psql_client),
        summary_statement,
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
            while let Ok((correlation_id, amount)) = rx.recv().await {
                tx_worker.send_to(format!("{correlation_id}:{amount}").as_bytes(), worker_socket).await?;
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    let http_workers = std::env::var("HTTP_WORKERS")
        .unwrap_or("5".to_string())
        .parse()?;

    for _ in 0..http_workers {
        let http_rx = http_rx.clone();
        let state = state.clone();
        tokio::spawn(async move {
            handler_loop(&state, http_rx).await
        });
    }

    let sockets_dir = "/tmp/sockets";
    std::fs::create_dir_all(std::path::Path::new(sockets_dir))?;

    let hostname = std::env::var("HOSTNAME")?;

    let socket_path = format!("{sockets_dir}/{hostname}.sock");
    println!("Binding to socket: {socket_path}");
    let listener = uds::create_unix_socket(&socket_path).await?;

    while let Ok((mut stream, _)) = listener.accept().await {
        stream.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await?;
        // http_tx.send(stream).await?;
    }

    Ok(())
}