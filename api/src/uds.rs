use std::{os::unix::fs::PermissionsExt, sync::Arc, task::{RawWaker, RawWakerVTable, Wake, Waker}};

pub async fn create_unix_socket(socket_path: &str) -> anyhow::Result<tokio::net::UnixListener> {

    _ = tokio::fs::remove_file(&socket_path).await;

    let listener = std::os::unix::net::UnixListener::bind(&socket_path)?;
    listener.set_nonblocking(true)?;

    set_socket_permissions(&socket_path)?;

    let listener = tokio::net::UnixListener::from_std(listener)?;

    Ok(listener)
}

pub fn set_socket_permissions(socket_path: &str) -> anyhow::Result<()> {
    let mut permissions = std::fs::metadata(socket_path)?.permissions();
    permissions.set_mode(0o777);
    std::fs::set_permissions(socket_path, permissions)?;
    Ok(())
}

pub struct SocketWaker;

impl Wake for SocketWaker {
    fn wake(self: Arc<Self>) {}
}

impl SocketWaker {

    pub fn new() -> Waker {
        unsafe {
            static VTABLE: RawWakerVTable = RawWakerVTable::new(
                |data| RawWaker::new(data, &VTABLE),
                |data| unsafe {
                    let waker = Arc::from_raw(data as *const SocketWaker);
                    waker.clone().wake();
                    std::mem::forget(waker); // Prevent double free
                },
                |data| unsafe {
                    let waker = Arc::from_raw(data as *const SocketWaker);
                    waker.clone().wake();
                    std::mem::forget(waker);
                },
                |data| unsafe {
                    let _ = Arc::from_raw(data as *const SocketWaker);
                },
            );
    
            let raw_waker = RawWaker::new(
                Arc::into_raw(Arc::new(SocketWaker)) as *const (),
                &VTABLE,
            );
            Waker::from_raw(raw_waker)
        }
    }
}
