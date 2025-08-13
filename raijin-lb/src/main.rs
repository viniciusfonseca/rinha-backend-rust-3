use std::sync::atomic::{AtomicUsize, Ordering};
use deadpool::managed::{Object, Pool};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::uds_pool::UnixSocketConnectionManager;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod uds_pool;

static NEXT_BACKEND: AtomicUsize = AtomicUsize::new(0);

type Manager = UnixSocketConnectionManager;

async fn handle_connection(mut client: TcpStream, pools: &mut Vec<Pool<Manager>>) -> (anyhow::Result<()>, anyhow::Result<()>) {

    let pool_index = NEXT_BACKEND.fetch_add(1, Ordering::Relaxed) % pools.len();
    let upstream = &mut pools[pool_index].get().await.expect("Failed to get connection");

    let (mut rc, mut wc) = client.split();
    let (mut ru, mut wu) = upstream.split();

    tokio::join!(
        async move {

            let mut buffer = [0; 256];
            let n = rc.read(&mut buffer).await?;
            wu.write_all(&buffer[..n]).await?;

            wu.flush().await?;

            anyhow::Ok(())
        },
        async move {

            let mut buffer = [0; 256];
            let n = ru.read(&mut buffer).await?;
            wc.write_all(&buffer[..n]).await?;

            wc.flush().await?;

            anyhow::Ok(())
        }
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let listen_addr = std::env::var("LISTEN_ADDR")
        .unwrap_or("0.0.0.0:9999".to_string());

    let target_sockets = std::env::var("TARGET_SOCKETS")
        .expect("TARGET_SOCKETS not set")
        .split(",").map(|s| s.to_string())
        .collect::<Vec<String>>();

    let listener = TcpListener::bind(&listen_addr).await?;
    println!("[*] Load balancer listening on {}", &listen_addr);

    let (tx, rx) = async_channel::unbounded();

    let mut backend_pools = Vec::new();

    let min_persistent_connections = std::env::var("MIN_PERSISTENT_CONNECTIONS")
        .unwrap_or("30".to_string())
        .parse()?;

    let max_persistent_connections = std::env::var("MAX_PERSISTENT_CONNECTIONS")
        .unwrap_or("340".to_string())
        .parse()?;

    for backend in &target_sockets {
        let manager = Manager::new(backend.to_string());
        let pool = Pool::<Manager, Object<Manager>>::builder(manager)
            .max_size(max_persistent_connections)
            .build()?;
        let mut tasks = Vec::new();
        for _ in 0..min_persistent_connections {
            tasks.push(pool.get());
        }
        futures::future::join_all(tasks).await;
        backend_pools.push(pool);
    }

    let http_workers = std::env::var("HTTP_WORKER_THREADS")
        .unwrap_or("10".to_string())
        .parse()?;

    for _ in 0..http_workers {
        let rx = rx.clone();
        let mut backend_pools = backend_pools.clone();
        tokio::spawn(async move {
            while let Ok(stream) = rx.recv().await {
                match handle_connection(stream, &mut backend_pools).await {
                    (Err(e), Ok(_)) => eprintln!("Error handling connection: {e}"),
                    (Ok(_), Err(e)) => eprintln!("Error handling connection: {e}"),
                    (Err(e1), Err(e2)) => eprintln!("Error handling connection: {e1} {e2}"),
                    _ => {}
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    while let Ok((stream, _)) = listener.accept().await {
        tx.send(stream).await?;
    }

    Ok(())
}

