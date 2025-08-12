use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Context;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};

use crate::waker::SocketWaker;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod waker;

struct AppState {
    next_backend: AtomicUsize,
}

async fn handle_connection(mut inbound: TcpStream, state: Arc<AppState>, streams: &mut Vec<UnixStream>) -> io::Result<()> {

    let backend_index = state.next_backend.fetch_add(1, Ordering::Relaxed) % streams.len();
    let outbound = &mut streams[backend_index];

    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = outbound.split();

    let client_to_server = async {
        io::copy(&mut ri, &mut wo).await?;
        wo.shutdown().await
    };

    let server_to_client = async {
        io::copy(&mut ro, &mut wi).await?;
        wi.shutdown().await
    };

    tokio::try_join!(client_to_server, server_to_client)?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let listen_addr = std::env::var("LISTEN_ADDR")
        .unwrap_or("127.0.0.1:9999".to_string());

    let target_sockets = std::env::var("TARGET_SOCKETS")
        .expect("TARGET_SOCKETS not set")
        .split(",").map(|s| s.to_string())
        .collect::<Vec<String>>();

    let listener = TcpListener::bind(&listen_addr).await?;
    println!("[*] Load balancer listening on {}", &listen_addr);

    let state = Arc::new(AppState {
        next_backend: AtomicUsize::new(0) });

    let (tx, rx) = async_channel::unbounded();

    let http_workers = std::env::var("HTTP_WORKER_THREADS")
        .unwrap_or("10".to_string())
        .parse()?;

    for _ in 0..http_workers {
        let state_clone = Arc::clone(&state);
        let rx = rx.clone();

        let mut streams = Vec::with_capacity(target_sockets.len());
        for backend in &target_sockets {
            streams.push(UnixStream::connect(backend).await?);
        }
        tokio::spawn(async move {
            while let Ok(stream) = rx.recv().await {
                let state_clone = Arc::clone(&state_clone);
                if let Err(e) = handle_connection(stream, state_clone, &mut streams).await {
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

