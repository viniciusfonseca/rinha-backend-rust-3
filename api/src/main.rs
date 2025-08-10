use std::sync::Arc;

mod handler;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use rust_decimal::Decimal;
use tokio::{net::UnixDatagram, time::Instant};

use crate::{handler::handler_loop, summary::PAYMENTS_SUMMARY_QUERY};

#[derive(Clone)]
struct ApiState {
    tx: tokio::sync::mpsc::UnboundedSender<(String, Decimal)>,
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
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (http_tx, http_rx) = async_channel::unbounded();

    let state = ApiState {
        tx,
        psql_client: Arc::new(psql_client),
        summary_statement,
    };

    tokio::spawn(async move {
        let tx_worker = UnixDatagram::unbound()?;
        while let Some((correlation_id, amount)) = rx.recv().await {
            tx_worker.send_to(format!("{correlation_id}:{amount}").as_bytes(), "/tmp/sockets/worker.sock").await?;
        }
        Ok::<(), std::io::Error>(())
    });

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

    while let Ok((stream, _)) = listener.accept().await {
        http_tx.send((stream, Instant::now())).await?;
    }

    Ok(())
}