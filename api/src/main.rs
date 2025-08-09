use std::sync::Arc;

use axum::{routing, Router};
use tokio::net::UnixDatagram;

mod payments;
mod summary;
mod uds;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use crate::summary::PAYMENTS_SUMMARY_QUERY;

#[derive(Clone)]
struct ApiState {
    datagram: Arc<UnixDatagram>,
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
    let datagram = UnixDatagram::unbound()?;

    let state = ApiState {
        datagram: Arc::new(datagram),
        psql_client: Arc::new(psql_client),
        summary_statement,
    };
    
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