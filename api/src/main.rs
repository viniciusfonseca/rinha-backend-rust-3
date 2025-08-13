use std::sync::Arc;

mod handler;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use futures::StreamExt;
use tokio::net::UnixDatagram;

use crate::summary::PAYMENTS_SUMMARY_QUERY;

#[derive(Clone)]
struct ApiState {
    tx: tokio::sync::mpsc::UnboundedSender<(String, f64)>,
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

    let state = ApiState {
        tx,
        psql_client: Arc::new(psql_client),
        summary_statement,
    };

    let queue_backoff = std::env::var("QUEUE_BACKOFF")
        .unwrap_or("100".to_string())
        .parse()?;

    let queue_batch_size = std::env::var("QUEUE_BATCH_SIZE")
        .unwrap_or("100".to_string())
        .parse()?;

    tokio::spawn(async move {
        loop {
            let mut batch = Vec::new();
            let n = rx.recv_many(&mut batch, queue_batch_size).await;
            futures::stream::iter(batch).for_each_concurrent(n, |(correlation_id, amount)| async move {
                let tx_worker = UnixDatagram::unbound().unwrap();
                tx_worker.send_to(format!("{correlation_id}:{amount}").as_bytes(), "/tmp/sockets/worker.sock").await.unwrap();
            }).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(queue_backoff)).await;
        }
    });

    let sockets_dir = "/tmp/sockets";
    std::fs::create_dir_all(std::path::Path::new(sockets_dir))?;

    let hostname = std::env::var("HOSTNAME")?;

    let socket_path = format!("{sockets_dir}/{hostname}.sock");
    println!("Binding to socket: {socket_path}");

    let listener = uds::create_unix_socket(&socket_path).await?;

    while let Ok((stream, _)) = listener.accept().await {
        let state = Arc::new(state.clone());
        tokio::spawn(async move {
            if let Err(e) = handler::handler_loop_stream(&state, stream).await {
                eprintln!("[!] Error handling connection: {}", e);
            }
        });
    }

    Ok(())

}