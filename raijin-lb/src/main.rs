use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

struct AppState {
    backends: Vec<String>,
    next_backend: AtomicUsize,
    async_paths: Vec<String>,
}

async fn handle_connection(mut inbound: TcpStream, state: Arc<AppState>) -> io::Result<()> {

    let backend_index = state.next_backend.fetch_add(1, Ordering::Relaxed) % state.backends.len();
    let backend_address = &state.backends[backend_index];

    println!("[+] Forwarding to backend: {}", backend_address);
    
    if backend_address.starts_with("http") {

        let mut outbound = match TcpStream::connect(backend_address).await {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("[!] Failed to connect to backend {}: {}", backend_address, e);
                return Err(e);
            }
        };

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

    }
    else if backend_address.starts_with("unix:") {
        let mut outbound = match UnixStream::connect(backend_address).await {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("[!] Failed to connect to backend {}: {}", backend_address, e);
                return Err(e);
            }
        };

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
    }
    else {
        eprintln!("[!] Invalid backend address: {}", backend_address);
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "Invalid backend address"));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    let listen_addr = std::env::var("LISTEN_ADDR").unwrap_or("127.0.0.1:8080".to_string());
    let async_paths = std::env::var("ASYNC_PATHS").unwrap_or("".to_string()).split(",").map(|s| s.to_string()).collect();

    let backends = vec!["127.0.0.1:8081".to_string(), "127.0.0.1:8082".to_string()];

    let listener = TcpListener::bind(&listen_addr).await?;
    println!("[*] Load balancer listening on {}", &listen_addr);

    let state = Arc::new(AppState {
        backends, next_backend: AtomicUsize::new(0), async_paths });

    let (tx, rx) = async_channel::unbounded();
    // let (tx_async, rx_async) = async_channel::unbounded();

    let http_workers = std::env::var("HTTP_WORKER_THREADS")
        .unwrap_or("10".to_string())
        .parse()?;

    for _ in 0..http_workers {
        let state_clone = Arc::clone(&state);
        let rx = rx.clone();
        tokio::spawn(async move {
            while let Ok(stream) = rx.recv().await {
                let state_clone = Arc::clone(&state_clone);
                if let Err(e) = handle_connection(stream, state_clone).await {
                    eprintln!("[!] Error handling connection: {}", e);
                }
            }
        });
    }

    // let async_workers = std::env::var("ASYNC_WORKER_THREADS")
    //     .unwrap_or("10".to_string())
    //     .parse()?;

    // for _ in 0..async_workers {
    //     let state_clone = Arc::clone(&state);
    //     let rx = rx.clone();
    //     tokio::spawn(async move {
    //         while let Ok((stream, _)) = listener.accept().await {
    //             let state_clone = Arc::clone(&state_clone);
    //             if let Err(e) = handle_connection(stream, state_clone).await {
    //                 eprintln!("[!] Error handling connection: {}", e);
    //             }
    //         }
    //     });
    // }

    while let Ok((stream, _)) = listener.accept().await {
        _ = tx.send(stream).await;
    }

    Ok(())
}

