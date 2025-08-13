use deadpool::managed::{Manager, Metrics, RecycleResult};
use tokio::{io, net::UnixStream};

#[derive(Debug)]
pub enum UnixSocketError {
    Io(()),
    ConnectionFailed(()),
}

impl From<io::Error> for UnixSocketError {
    fn from(_: io::Error) -> Self {
        UnixSocketError::Io(())
    }
}

pub struct UnixSocketConnectionManager {
    address: String,
}

impl UnixSocketConnectionManager {
    pub fn new(address: String) -> Self {
        Self { address }
    }
}

impl Manager for UnixSocketConnectionManager {

    type Type = UnixStream;
    type Error = UnixSocketError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        UnixStream::connect(&self.address)
            .await
            .map_err(|_| UnixSocketError::ConnectionFailed(()))
    }

    async fn recycle(&self, conn: &mut UnixStream, _: &Metrics) -> RecycleResult<Self::Error> {
        match conn.writable().await {
            Ok(()) => Ok(()),
            Err(_) => Err(deadpool::managed::RecycleError::Backend(UnixSocketError::Io(()))),
        }
    }
}