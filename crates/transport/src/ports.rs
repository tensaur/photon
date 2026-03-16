use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("request failed: {0}")]
    Request(String),

    #[error("stream closed: {0}")]
    StreamClosed(String),

    #[error(transparent)]
    Unknown(#[from] anyhow::Error),
}

/// Port for network communication, generic over message types.
pub trait Transport<S, R>: Send + Sync + 'static
where
    S: Send + Sync,
    R: Send,
{
    fn send(&self, msg: &S) -> impl Future<Output = Result<(), TransportError>> + Send;
    fn recv(&self) -> impl Future<Output = Result<R, TransportError>> + Send;
}
