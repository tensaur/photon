use std::future::Future;

use async_trait::async_trait;

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("request failed: {0}")]
    Request(String),

    #[error("stream closed: {0}")]
    StreamClosed(String),
}

#[cfg(not(target_arch = "wasm32"))]
impl TransportError {
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn from_io(e: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match e.kind() {
            ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe => Self::StreamClosed(e.to_string()),
            _ => Self::Connection(e.to_string()),
        }
    }
}

/// Low-level byte transport. Uses `async_trait` because it must be dyn-compatible
/// for runtime protocol selection via [`TransportKind`](crate::TransportKind).
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ByteTransport: Send + Sync + 'static {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError>;
    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ByteTransport for Box<dyn ByteTransport> {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        (**self).send_bytes(bytes).await
    }

    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError> {
        (**self).recv_bytes().await
    }
}

/// Typed transport over a codec. Uses RPIT (no heap allocation).
#[cfg(not(target_arch = "wasm32"))]
pub trait Transport<S, R>: Clone + Send + Sync + 'static {
    fn send(&self, msg: &S) -> impl Future<Output = Result<(), TransportError>> + Send;
    fn recv(&self) -> impl Future<Output = Result<R, TransportError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait Transport<S, R>: Clone + 'static {
    fn send(&self, msg: &S) -> impl Future<Output = Result<(), TransportError>>;
    fn recv(&self) -> impl Future<Output = Result<R, TransportError>>;
}
