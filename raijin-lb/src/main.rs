use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Context;
use deadpool::managed::{Object, Pool};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};

use crate::uds_pool::UnixSocketConnectionManager;
use crate::waker::SocketWaker;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod uds_pool;
mod waker;

static NEXT_BACKEND: AtomicUsize = AtomicUsize::new(0);

type Manager = UnixSocketConnectionManager;

async fn handle_connection(mut client: TcpStream, pools: &mut Vec<Pool<Manager>>) -> anyhow::Result<()> {

    let pool_index = NEXT_BACKEND.fetch_add(1, Ordering::Relaxed) % pools.len();
    let upstream = &mut pools[pool_index].get().await.expect("Failed to get connection");

    let mut buffer = vec![0; 1024];
    let n = client.read(&mut buffer).await?;
    let request = &buffer[..n];

    // Forward to upstream
    upstream.write_all(request).await?;
    upstream.flush().await?;

    // Read response from upstream
    let mut response = Vec::new();
    upstream.read_to_end(&mut response).await?;

    // Send response to client
    client.write_all(&response).await?;
    client.flush().await?;

    Ok(())
}

async fn connect_backend(backend: &str) -> io::Result<UnixStream> {
    loop {
        match UnixStream::connect(backend).await {
            Ok(stream) => { println!("[*] Connected to backend {}", backend); return Ok(stream) },
            Err(e) => {
                eprintln!("[!] Error connecting to backend {}: {}", backend, e);
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
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
    for backend in &target_sockets {
        let manager = Manager::new(backend.to_string());
        let pool = Pool::<Manager, Object<Manager>>::builder(manager)
            .max_size(340)
            .build()?;
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
                if let Err(e) = handle_connection(stream, &mut backend_pools).await {
                    eprintln!("[!] Error handling connection: {}", e);
                }
            }
            Ok::<(), anyhow::Error>(())
        });
    }

    let waker = SocketWaker::new();
    let mut context = Context::from_waker(&waker);
    loop {
        let mut tasks = Vec::new();
        while let std::task::Poll::Ready(Ok((stream, _))) = listener.poll_accept(&mut context) {
            tasks.push(tx.send(stream));
        }
        futures::future::join_all(tasks).await;
        tokio::time::sleep(std::time::Duration::from_nanos(10)).await;
    }
}

