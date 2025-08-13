use deadpool::managed::{Manager, Metrics, RecycleResult};
use tokio::{io, net::UnixStream};

#[derive(Debug)]
pub enum UnixSocketError {
    Io(io::Error),
    ConnectionFailed(String),
}

impl From<io::Error> for UnixSocketError {
    fn from(err: io::Error) -> Self {
        UnixSocketError::Io(err)
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
            .map_err(|e| UnixSocketError::ConnectionFailed(format!("Failed to connect: {}", e)))
    }

    async fn recycle(&self, conn: &mut UnixStream, _: &Metrics) -> RecycleResult<Self::Error> {
        match conn.writable().await {
            Ok(()) => Ok(()),
            Err(e) => Err(deadpool::managed::RecycleError::Backend(UnixSocketError::Io(e))),
        }
    }
}