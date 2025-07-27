use tokio::net::UnixDatagram;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    
    let worker_socket = "/tmp/sockets/worker.sock";
    let tx_worker = UnixDatagram::bind(worker_socket)?;

    Ok(())
}
