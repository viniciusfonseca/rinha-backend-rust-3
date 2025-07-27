pub async fn bind_unix_datagram_socket(socket_path: &str) -> anyhow::Result<tokio::net::UnixDatagram> {
    
    if tokio::fs::remove_file(socket_path).await.is_err() {
        println!("warn: unable to unlink path {socket_path}");
    }

    let listener = std::os::unix::net::UnixDatagram::bind(socket_path)?;
    listener.set_nonblocking(true)?;

    let socket = tokio::net::UnixDatagram::from_std(listener)?;

    Ok(socket)
}