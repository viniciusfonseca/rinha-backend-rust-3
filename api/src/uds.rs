use std::os::unix::fs::PermissionsExt;

pub async fn create_unix_socket(socket_path: &str) -> anyhow::Result<std::os::unix::net::UnixListener> {

    _ = tokio::fs::remove_file(&socket_path).await;

    let listener = std::os::unix::net::UnixListener::bind(&socket_path)?;
    listener.set_nonblocking(true)?;

    set_socket_permissions(&socket_path)?;

    Ok(listener)
}

pub fn set_socket_permissions(socket_path: &str) -> anyhow::Result<()> {
    let mut permissions = std::fs::metadata(socket_path)?.permissions();
    permissions.set_mode(0o777);
    std::fs::set_permissions(socket_path, permissions)?;
    Ok(())
}